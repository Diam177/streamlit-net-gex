
# services/net_gex.py (strict IV fix)

from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, List, Dict, Any, Tuple, Optional
import math
from logger import get_logger

LOG = get_logger(__name__)

def _in_seconds(ts: float | int | None) -> Optional[float]:
    if ts is None:
        return None
    x = float(ts)
    if x > 1e12:
        x = x / 1000.0
    return x

def time_to_expiry(snapshot_ts: float | int, expiry_ts: float | int) -> Tuple[float, float]:
    t0 = _in_seconds(snapshot_ts)
    te = _in_seconds(expiry_ts)
    if t0 is None or te is None:
        raise ValueError("snapshot_ts and expiry_ts must be provided")
    seconds = max(te - t0, 0.0)
    T_years = max(seconds / 31_536_000.0, 1e-6)
    T_days = seconds / 86_400.0
    return T_years, T_days

def _normalize_iv(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        return None
    if v <= 1e-4:
        return None
    if v > 3.0:
        v = v / 100.0  # likely given in %
    v = max(min(v, 3.0), 0.01)
    return v

def _phi(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)

def _gamma_bs(S: float, K: float, sigma: float, T: float, r: float = 0.0) -> float:
    if sigma <= 0.0 or T <= 0.0 or S <= 0.0 or K <= 0.0:
        return 0.0
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
        return _phi(d1) / (S * sigma * math.sqrt(T))
    except Exception as e:
        LOG.exception("gamma calc error")
        return 0.0

@dataclass
class NetGexResult:
    rows: List[Dict[str, Any]]
    k: float
    metrics: Dict[str, Any]

def calculate_net_gex(
    S: float,
    rows: Iterable[Dict[str, Any]],
    expiry_ts: float | int,
    snapshot_ts: float | int,
    *,
    M: int = 100,
    scale_divisor: float = 1000.0,
    core_K: int = 11,
) -> NetGexResult:
    T_years, T_days = time_to_expiry(snapshot_ts, expiry_ts)

    enriched: List[Dict[str, Any]] = []
    iv_candidates: List[float] = []
    for r in rows:
        strike = float(r.get("strike"))
        call_OI = float(r.get("call_OI", 0) or 0)
        put_OI = float(r.get("put_OI", 0) or 0)
        call_iv = _normalize_iv(r.get("call_iv"))
        put_iv = _normalize_iv(r.get("put_iv"))
        agg_iv = _normalize_iv(r.get("iv"))
        iv_used = None
        for v in (call_iv, put_iv, agg_iv):
            if v is not None:
                iv_used = v if iv_used is None else (iv_used + v) / 2.0
        if iv_used is not None:
            iv_candidates.append(iv_used)
        enriched.append({"strike": strike, "call_OI": call_OI, "put_OI": put_OI, "dOI": call_OI - put_OI, "iv_used": iv_used})

    # median IV across all strikes
    iv_median = 0.20
    if iv_candidates:
        iv_candidates.sort()
        iv_median = iv_candidates[len(iv_candidates)//2]

    # fill missing iv_used with median
    for row in enriched:
        if row["iv_used"] is None:
            row["iv_used"] = iv_median

    # core = K nearest to S
    core = sorted(enriched, key=lambda x: abs(x["strike"] - S))[:max(core_K,1)]
    w_sum = 0.0
    g_sum = 0.0
    for row in core:
        w = row["call_OI"] + row["put_OI"] + 1.0
        sigma = float(row["iv_used"])
        g = _gamma_bs(S, row["strike"], sigma, T_years, 0.0)
        w_sum += w
        g_sum += w * g
    gamma_avg = (g_sum / w_sum) if w_sum > 0 else 0.0

    k = gamma_avg * S * float(M) / float(scale_divisor)

    out_rows: List[Dict[str, Any]] = []
    for row in sorted(enriched, key=lambda x: x["strike"]):
        net_gex = k * row["dOI"]
        out_rows.append({
            "strike": row["strike"],
            "call_OI": row["call_OI"],
            "put_OI": row["put_OI"],
            "dOI": row["dOI"],
            "iv_used": row["iv_used"],
            "NetGEX": round(net_gex, 1)
        })

    metrics = {"S": S, "T_days": T_days, "iv_median": iv_median, "gamma_avg": gamma_avg, "k": k}
    return NetGexResult(rows=out_rows, k=k, metrics=metrics)
