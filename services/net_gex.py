import math
from datetime import datetime, timezone
from typing import Any, Dict
import pandas as pd

def _norm_sigma(iv):
    if iv is None: return None
    try:
        v = float(iv)
    except Exception:
        return None
    if v > 1.5:
        v /= 100.0
    return max(v, 1e-6)

def _extract_chain(doc: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(doc, dict) and "optionChain" in doc:
        oc = doc["optionChain"]
        if isinstance(oc, dict) and "result" in oc and oc["result"]:
            return oc["result"][0]
    if isinstance(doc, dict) and "result" in doc and isinstance(doc["result"], list) and doc["result"]:
        return doc["result"][0]
    return doc

def _collect_entry(chain_root: Dict[str, Any], target_epoch: int) -> Dict[str, Any] | None:
    options_list = chain_root.get("options") or chain_root.get("chains") or []
    for opt_entry in options_list or []:
        if int(opt_entry.get("expirationDate", 0)) == int(target_epoch):
            return opt_entry
    if "chains[0]" in chain_root:
        n = chain_root["chains[0]"]
        if int(n.get("expiration", 0)) == int(target_epoch):
            return {"expirationDate": n.get("expiration"), "calls": n.get("calls", []), "puts": n.get("puts", [])}
    if isinstance(options_list, list) and len(options_list) == 1:
        return options_list[0]
    return None

def _black_scholes_gamma(S: float, K: float, T: float, sigma: float, r: float = 0.0) -> float:
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0: return 0.0
    from math import log, sqrt, exp, pi
    d1 = (log(S/K) + (r + 0.5*sigma*sigma)*T) / (sigma*sqrt(T))
    pdf = exp(-0.5*d1*d1) / (2*pi)**0.5
    return pdf / (S*sigma*sqrt(T))

def compute_net_gex_from_payload(payload: Dict[str, Any], expiration_epoch: int, scale_divisor: float = 1000.0, contract_multiplier: int = 100):
    chain_root = _extract_chain(payload)
    quote = chain_root.get("quote", {})
    S = float(quote.get("regularMarketPrice") or quote.get("lastPrice") or quote.get("price"))
    t0 = int(quote.get("regularMarketTime") or quote.get("tradeTime") or quote.get("time") or 0)
    entry = _collect_entry(chain_root, expiration_epoch)
    if entry is None:
        raise RuntimeError("Не найден блок опционов для запрошенной экспирации.")
    by_strike = {}
    for side in ("calls","puts"):
        for opt in entry.get(side, []) or []:
            K = float(opt.get("strike"))
            oi = int(opt.get("openInterest") or 0)
            vol = int(opt.get("volume") or 0)
            iv  = _norm_sigma(opt.get("impliedVolatility"))
            rec = by_strike.setdefault(K, {"call_oi":0,"put_oi":0,"call_vol":0,"put_vol":0,"call_iv":None,"put_iv":None})
            if side=="calls":
                rec["call_oi"]=oi; rec["call_vol"]=vol; rec["call_iv"]=iv
            else:
                rec["put_oi"]=oi; rec["put_vol"]=vol; rec["put_iv"]=iv

    T = max((int(entry.get("expirationDate")) - t0) / 31_536_000.0, 1e-6)
    rows = []
    iv_list = []
    for K, rec in sorted(by_strike.items()):
        ivs = [v for v in (rec["call_iv"], rec["put_iv"]) if v is not None]
        iv_avg = sum(ivs)/len(ivs) if ivs else None
        rows.append({"Strike": float(K), "Call OI": int(rec["call_oi"]), "Put OI": int(rec["put_oi"]), "Call Volume": int(rec["call_vol"]), "Put Volume": int(rec["put_vol"]), "IV": iv_avg})
        if iv_avg is not None: iv_list.append(iv_avg)
    iv_median = float(pd.Series(iv_list).median()) if iv_list else 0.2
    for r in rows:
        if r["IV"] is None: r["IV"] = iv_median

    for r in rows:
        gamma_i = _black_scholes_gamma(S, r["Strike"], T, r["IV"])
        r["classic_gex_i"] = gamma_i * S * contract_multiplier / scale_divisor
        r["ΔOI"] = r["Call OI"] - r["Put OI"]

    df = pd.DataFrame(rows)
    df["w"] = (df["ΔOI"].astype(float))**2
    if df["w"].sum() > 0:
        k = float((df["w"] * df["classic_gex_i"]).sum() / df["w"].sum())
    else:
        df["dist"] = (df["Strike"] - S).abs()
        core = df.nsmallest(11, "dist")
        k = float((core["classic_gex_i"] * (core["ΔOI"].abs()+1)).sum() / (core["ΔOI"].abs()+1).sum())

    df["Net GEX"] = (df["ΔOI"].astype(float) * k).round(1)
    table_iv = df[["Strike","Call OI","Put OI","Call Volume","Put Volume","IV","Net GEX"]].sort_values("Strike").reset_index(drop=True)
    table_basic = df[["Strike","Call OI","Put OI","Call Volume","Put Volume","Net GEX"]].sort_values("Strike").reset_index(drop=True)

    meta = {"S": round(S,4), "T_days": round(T*365.0,4), "k": round(k,6), "contract_multiplier": contract_multiplier, "scale_divisor": scale_divisor, "expiration_epoch": int(expiration_epoch)}
    return table_basic, table_iv, meta
