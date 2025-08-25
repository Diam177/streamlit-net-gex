import numpy as np
import pandas as pd
from math import sqrt, pi
from typing import Dict, Any

from logger import get_logger
from services.utils.debug import dump_json

logger = get_logger("net_gex")

def _gamma_bs(S: float, K: float, T: float, sigma: float) -> float:
    if sigma <= 0 or T <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (np.log(S / K) + 0.5 * sigma * sigma * T) / (sigma * sqrt(T))
    pdf = np.exp(-0.5 * d1 * d1) / sqrt(2 * pi)
    return float(pdf / (S * sigma * sqrt(T)))

def calculate_net_gex(df: pd.DataFrame, S: float, expiry_ts: int, snapshot_ts: int) -> Dict[str, Any]:
    # Подготовка
    df = df.copy()
    df["call_OI"] = df["call_OI"].fillna(0).clip(lower=0)
    df["put_OI"]  = df["put_OI"].fillna(0).clip(lower=0)
    df["ΔOI"] = df["call_OI"] - df["put_OI"]

    T = max((expiry_ts - snapshot_ts) / 31_536_000, 1e-6)
    iv_series = df.get("iv", pd.Series([np.nan]*len(df)))
    iv_median = float(np.nanmedian(iv_series)) if np.isfinite(np.nanmedian(iv_series)) else 0.25
    df["iv_used"] = iv_series.fillna(iv_median).replace(0, iv_median)

    # Ядро вокруг ATM
    core = df[(df["strike"] >= S * 0.99) & (df["strike"] <= S * 1.01)]
    if core.empty:
        core = df.iloc[(df["strike"] - S).abs().sort_values().index[:11]]

    weights = core["call_OI"] + core["put_OI"] + 1.0
    gammas = core.apply(lambda r: _gamma_bs(S, float(r.strike), T, float(r.iv_used)), axis=1)
    gamma_avg = float(np.sum(gammas * weights) / np.sum(weights))

    # Масштаб
    M = 100.0
    scale_divisor = 1000.0
    k_raw = gamma_avg * S * M / scale_divisor

    # Итоговый Net GEX
    df["NetGEX"] = k_raw * df["ΔOI"]
    df_out = df[["strike", "call_OI", "put_OI", "ΔOI", "iv_used", "NetGEX"]].sort_values("strike").reset_index(drop=True)

    # Debug snapshot
    dump_json("calc_snapshot", {
        "S": S, "T_years": T, "iv_median": iv_median,
        "gamma_avg": gamma_avg, "k_raw": k_raw,
        "rows": int(len(df_out))
    })
    logger.info(f"calc: S={S:.4f}, T={T:.6f}, iv_med={iv_median:.4f}, gamma_avg={gamma_avg:.6e}, k_raw={k_raw:.4f}, rows={len(df_out)}")
    return {"k": k_raw, "summary": {"S": S, "T": T, "iv_median": iv_median}, "table": df_out}
