import os
import time
import json
from typing import Dict, Any, List, Tuple
import requests
import pandas as pd

DEFAULT_HOST = os.getenv("RAPIDAPI_HOST", "yahoo-finance15.p.rapidapi.com")
DEFAULT_KEY = os.getenv("RAPIDAPI_KEY", "")

def _headers(host: str = None, key: str = None) -> Dict[str, str]:
    return {
        "x-rapidapi-host": (host or DEFAULT_HOST),
        "x-rapidapi-key": (key or DEFAULT_KEY),
    }

def _request_json(url: str, host: str = None, key: str = None, timeout: int = 20) -> Dict[str, Any]:
    r = requests.get(url, headers=_headers(host, key), timeout=timeout)
    r.raise_for_status()
    return r.json()

def _extract_chain_payload(payload: Dict[str, Any]) -> Tuple[float, int, List[int], Dict[int, pd.DataFrame]]:
    """
    Normalize different Yahoo payloads into:
        (spot, snapshot_ts, expirations, frames_by_expiration)
    """
    # Two shapes: { "optionChain": { "result": [ { "quote": {...}, "options":[ {...} ] } ] } }
    # and more compact one under "result"
    oc = None
    if "optionChain" in payload:
        res = payload["optionChain"].get("result") or []
        oc = res[0] if res else None
    elif "result" in payload and isinstance(payload["result"], list):
        oc = payload["result"][0] if payload["result"] else None
    if not oc:
        raise ValueError("Unexpected payload shape from provider")
    quote = oc.get("quote") or {}
    spot = float(quote.get("regularMarketPrice") or quote.get("regularMarketPreviousClose") or 0.0)
    snapshot = int(time.time())
    exp_list = oc.get("expirationDates") or []
    frames: Dict[int, pd.DataFrame] = {}
    # Loop "options" array (one entry per expiration)
    for opt in oc.get("options", []):
        exp = int(opt.get("expirationDate") or 0)
        calls = opt.get("calls") or []
        puts = opt.get("puts") or []
        # Build frames
        def _to_df(arr: List[Dict[str, Any]]) -> pd.DataFrame:
            rows = []
            for el in arr:
                rows.append({
                    "strike": float(el.get("strike") or 0.0),
                    "OI": int(el.get("openInterest") or 0),
                    "volume": int(el.get("volume") or 0),
                    "iv": float(el.get("impliedVolatility") or 0.0),
                })
            return pd.DataFrame(rows)

        cdf = _to_df(calls)
        pdf = _to_df(puts)
        if cdf.empty and pdf.empty:
            continue
        df = pd.merge(cdf.rename(columns={"OI":"call_OI","volume":"call_volume","iv":"call_iv"}),
                      pdf.rename(columns={"OI":"put_OI","volume":"put_volume","iv":"put_iv"}),
                      on="strike", how="outer").fillna(0)
        # Blend IV: max(call_iv, put_iv)
        df["iv"] = df[["call_iv","put_iv"]].max(axis=1)
        # Integers
        for col in ["call_OI","put_OI","call_volume","put_volume"]:
            df[col] = df[col].astype(int)
        frames[exp] = df.sort_values("strike").reset_index(drop=True)
    expirations = sorted(frames.keys()) if frames else exp_list
    return spot, snapshot, expirations, frames

def fetch_chain(ticker: str, host: str = None, key: str = None) -> Tuple[float, int, List[int], Dict[int, pd.DataFrame]]:
    """
    Try two Yahoo endpoints: /options and /op/option
    """
    base = f"https://{host or DEFAULT_HOST}/api/yahoo"
    urls = [
        f"{base}/options/{ticker}",
        f"{base}/op/option/{ticker}",
    ]
    last_err = None
    for url in urls:
        try:
            payload = _request_json(url, host, key)
            return _extract_chain_payload(payload)
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"All provider endpoints failed for {ticker}: {last_err}")
