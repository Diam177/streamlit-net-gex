from dataclasses import dataclass
from typing import Optional
import numpy as np
import pandas as pd
from math import log, sqrt, exp
from scipy.stats import norm

@dataclass
class VariantBConfig:
    iv_floor: float = 0.05          # 5% hard floor for IV used in k
    t_days_floor: float = 1.0       # at least 1 day
    contract_mult: float = 100.0
    scale: float = 1.0              # additional user scale to match external site if needed

def _d1(S,K,vol,T):
    if S<=0 or K<=0 or vol<=0 or T<=0:
        return 0.0
    return (np.log(S/K) + 0.5*vol*vol*T)/(vol*np.sqrt(T))

def bs_gamma(S: float, K: float, vol: float, T: float) -> float:
    """ Black–Scholes gamma per underlying unit. """
    if S<=0 or K<=0 or vol<=0 or T<=0:
        return 0.0
    d1 = (np.log(S/K) + 0.5*vol*vol*T)/(vol*np.sqrt(T))
    return norm.pdf(d1)/(S*vol*np.sqrt(T))

def compute_variant_b(df: pd.DataFrame, spot: float, days_to_exp: float, cfg: Optional[VariantBConfig]=None) -> pd.Series:
    """Compute Net GEX per strike using Variant B: NetGEX = ΔOI * k_classic,
    where k_classic is taken as BS gamma * S^2 * contract_mult * scale (classic choice).
    We DO NOT normalize IVs to tiny 1e-5 – we clamp with iv_floor instead to avoid zeros.
    """
    cfg = cfg or VariantBConfig()
    T = max(days_to_exp, cfg.t_days_floor)/365.0
    vol_used = df[["iv","call_iv","put_iv"]].max(axis=1).astype(float).clip(lower=cfg.iv_floor)

    delta_oi = (df["call_OI"].fillna(0) - df["put_OI"].fillna(0)).astype(float)

    k = []
    for K, vol in zip(df["strike"].astype(float), vol_used):
        g = bs_gamma(spot, K, vol, T)  # per unit
        k_classic = g * (spot**2) * cfg.contract_mult * cfg.scale
        k.append(k_classic)

    k = np.array(k)
    netgex = delta_oi.values * k
    return pd.Series(netgex, index=df.index, name="NetGEX")
