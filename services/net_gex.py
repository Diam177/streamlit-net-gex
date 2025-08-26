
# services/net_gex.py
# Correct Net GEX calculation per shared methodology.
# - Robust IV normalization (fractions, not percents; ignore sentinels like 1e-5)
# - Time to expiry computed from *seconds* (convert ms -> s if needed)
# - ATM-core weighted gamma average
# - Stable k ~= 4.x on liquid weeklies (for SPY-like)
#
# Public API:
#   calculate_net_gex(S, rows, expiry_ts, snapshot_ts, *, M=100, scale_divisor=1000, core_K=11)
#       rows: iterable of dicts with keys: strike, call_OI, put_OI, [call_iv], [put_iv], [iv]
#   time_to_expiry(snapshot_ts, expiry_ts) -> T_years, T_days

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Dict, Any, Tuple, Optional
import math

try:
    import numpy as np
except Exception:  # numpy is in requirements; fallback if not
    np = None

from logger import get_logger  # logger.py in project root

LOG = get_logger(__name__)

_SENTINEL_IV_MIN = 1e-4  # anything below — treat as "missing"
_IV_MAX_FRACTION = 3.0   # clamp upper bound after normalization
_IV_MIN_FRACTION = 0.01  # clamp lower bound after normalization


def _in_seconds(ts: float | int | None) -> Optional[float]:
    if ts is None:
        return None
    x = float(ts)
    # If looks like milliseconds, convert to seconds
    if x > 1e12:
        x = x / 1000.0
    return x


def time_to_expiry(snapshot_ts: float | int, expiry_ts: float | int) -> Tuple[float, float]:
    """Return (T_years, T_days). Both inputs may be in seconds or milliseconds; we normalize to seconds.
    Uses 365-day year, protects against T -> 0 per methodology."""
    t0 = _in_seconds(snapshot_ts)
    te = _in_seconds(expiry_ts)
    if t0 is None or te is None:
        raise ValueError("snapshot_ts and expiry_ts must be provided")
    seconds = max(te - t0, 0.0)
    # 31_536_000 = 365 * 24 * 3600
    T_years = max(seconds / 31_536_000.0, 1e-6)
    T_days = seconds / 86_400.0
    return T_years, T_days


def _normalize_iv(x: Optional[float]) -> Optional[float]:
    """Normalize IV to fraction. Returns None if missing/invalid."""
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        return None
    if v <= _SENTINEL_IV_MIN:
        # sentinel/placeholder (e.g., 1e-05) -> treat as missing
        return None
    # If looks like percent (e.g., 25), convert to fraction (0.25)
    if v > _IV_MAX_FRACTION:
        v = v / 100.0
    # clip to sane bounds
    v = max(min(v, _IV_MAX_FRACTION), _IV_MIN_FRACTION)
    return v


def _choose_iv(call_iv: Optional[float], put_iv: Optional[float], agg_iv: Optional[float]) -> Optional[float]:
    """Pick the best available IV for a strike.
    Priority: a valid side IV (call or put), else aggregated IV, else None."""
    c = _normalize_iv(call_iv) if call_iv is not None else None
    p = _normalize_iv(put_iv) if put_iv is not None else None
    a = _normalize_iv(agg_iv) if agg_iv is not None else None
    # Prefer the valid side IVs; if both exist, take mean
    cand = [v for v in (c, p) if v is not None]
    if cand:
        return sum(cand) / len(cand)
    return a


def _phi(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _gamma_bs(S: float, K: float, sigma: float, T: float, r: float = 0.0) -> float:
    """Black–Scholes gamma. Same for calls/puts."""
    if sigma <= 0.0 or T <= 0.0 or S <= 0.0 or K <= 0.0:
        return 0.0
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
        return _phi(d1) / (S * sigma * math.sqrt(T))
    except Exception as e:
        LOG.exception("gamma calc error S=%s K=%s sigma=%s T=%s", S, K, sigma, T)
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
    use_regression_refine: bool = False,
) -> NetGexResult:
    """
    Compute Net GEX table and return rows + k + metrics.
    Each input row should include at least: strike, call_OI, put_OI; optional: call_iv, put_iv, iv.
    """
    T_years, T_days = time_to_expiry(snapshot_ts, expiry_ts)

    # Prepare enriched rows with iv_used and delta OI
    enriched: List[Dict[str, Any]] = []
    iv_candidates: List[float] = []
    for r in rows:
        strike = float(r.get("strike"))
        call_OI = float(r.get("call_OI", 0) or 0)
        put_OI = float(r.get("put_OI", 0) or 0)
        call_iv = r.get("call_iv")
        put_iv = r.get("put_iv")
        agg_iv = r.get("iv")  # sometimes provider already gives one IV per strike
        iv_used = _choose_iv(call_iv, put_iv, agg_iv)
        if iv_used is not None:
            iv_candidates.append(iv_used)

        enriched.append(
            {
                "strike": strike,
                "call_OI": call_OI,
                "put_OI": put_OI,
                "dOI": call_OI - put_OI,
                "call_iv": call_iv,
                "put_iv": put_iv,
                "iv_provider": agg_iv,
                "iv_used": iv_used,
            }
        )

    # Median IV across strikes (fallback for missing)
    if iv_candidates:
        iv_median = sorted(iv_candidates)[len(iv_candidates) // 2]
    else:
        iv_median = 0.20  # conservative fallback

    # Build ATM core for gamma averaging
    core = sorted(enriched, key=lambda x: abs(x["strike"] - S))
    core = core[: max(core_K, 1)]
    # If too few have iv_used, impute with iv_median
    for row in core:
        if row["iv_used"] is None:
            row["iv_used"] = iv_median

    # Weighted gamma average over the core
    w_sum = 0.0
    g_sum = 0.0
    for row in core:
        w = row["call_OI"] + row["put_OI"] + 1.0
        sigma = float(row["iv_used"])
        g = _gamma_bs(S, row["strike"], sigma, T_years, 0.0)
        w_sum += w
        g_sum += w * g
    gamma_avg = (g_sum / w_sum) if w_sum > 0 else 0.0

    # k baseline per methodology
    k_raw = gamma_avg * S * float(M) / float(scale_divisor)
    k = float(k_raw)

    # Optional refine via regression (usually very close to k_raw)
    if use_regression_refine:
        num = 0.0
        den = 0.0
        for row in enriched:
            dOI = row["dOI"]
            Gi_star = k_raw * dOI
            num += dOI * Gi_star
            den += dOI * dOI
        if den > 0:
            k = num / den

    # Compose output rows with Net GEX
    out_rows: List[Dict[str, Any]] = []
    for row in sorted(enriched, key=lambda x: x["strike"]):
        net_gex = k * row["dOI"]
        out_rows.append(
            {
                "strike": row["strike"],
                "call_OI": row["call_OI"],
                "put_OI": row["put_OI"],
                "dOI": row["dOI"],
                "iv_used": row["iv_used"] if row["iv_used"] is not None else iv_median,
                "NetGEX": round(net_gex, 1),
            }
        )

    metrics = {
        "S": S,
        "T_years": T_years,
        "T_days": T_days,
        "iv_median_core": iv_median,
        "gamma_avg": gamma_avg,
        "k_raw": k_raw,
        "k": k,
        "M": M,
        "scale_divisor": scale_divisor,
        "core_K": core_K,
        "core_strikes": [r["strike"] for r in core],
    }

    LOG.info(
        "NetGEX calc: S=%.4f T_days=%.2f iv_med=%.4f gamma_avg=%.6e k=%.6f core=%s",
        S, T_days, iv_median, gamma_avg, k, metrics["core_strikes"]
    )

    return NetGexResult(rows=out_rows, k=k, metrics=metrics)
