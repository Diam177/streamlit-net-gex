
# streamlit_app.py (metrics key fix + show T)
from __future__ import annotations

import json
from typing import Any, Dict, List
import pandas as pd
import streamlit as st

from logger import get_logger
from services.net_gex import calculate_net_gex
from services.utils.debug import dump_json, list_debug_files
from services.api_client import get_option_chain

LOG = get_logger(__name__)
st.set_page_config(page_title="Net GEX calculator", layout="wide")

def extract_chain(raw: Dict[str, Any]) -> Dict[str, Any]:
    c = None
    if isinstance(raw, dict):
        oc = raw.get("optionChain") or raw.get("option_chain")
        if oc and isinstance(oc, dict):
            res = oc.get("result")
            if res and isinstance(res, list) and res:
                c = res[0]
        if c is None and "body" in raw and isinstance(raw["body"], list) and raw["body"]:
            c = raw["body"][0]
    if not c:
        raise RuntimeError("Unexpected provider payload format")

    quote = c.get("quote") or {}
    S = float(quote.get("regularMarketPrice") or quote.get("regular_market_price") or quote.get("price") or quote.get("close") or 0.0)
    snapshot = quote.get("regularMarketTime") or quote.get("regular_market_time") or quote.get("time")
    if isinstance(snapshot, str):
        try:
            snapshot = int(float(snapshot))
        except Exception:
            snapshot = None
    options = c.get("options") or []
    norm_opts = []
    for block in options:
        exp = block.get("expirationDate") or block.get("expiration") or block.get("date")
        calls = block.get("calls") or []
        puts = block.get("puts") or []
        norm_opts.append({"expiration": exp, "calls": calls, "puts": puts})
    return {"S": float(S), "snapshot": snapshot, "options": norm_opts}


def normalize_rows(calls: List[Dict[str, Any]], puts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_strike: Dict[float, Dict[str, Any]] = {}

    def get_oi(x: Dict[str, Any]) -> float:
        # don't use truthiness; 0 is a valid OI
        keys = ("openInterest", "open_interest", "open_interest_contracts")
        val = None
        for k in keys:
            if k in x:
                val = x[k]
                break
        # cautious fallback: use 'oi' only if it's clearly not an IV placeholder
        if val is None and "oi" in x:
            try:
                cand = float(x["oi"])
                if cand >= 1.0:  # plausible contracts count
                    val = cand
            except Exception:
                pass
        try:
            return float(val) if val is not None else 0.0
        except Exception:
            return 0.0

    def get_iv(x: Dict[str, Any]) -> float | None:
        v = None
        for k in ("impliedVolatility", "implied_volatility", "iv"):
            if k in x and x[k] is not None:
                try:
                    vv = float(x[k])
                except Exception:
                    continue
                # reject obvious OI values accidentally mapped here
                if vv > 10.0 and vv % 1 == 0:
                    continue
                # normalize percent -> fraction if needed
                if vv > 3.0:
                    vv = vv / 100.0
                # clamp
                if vv <= 0.0001:
                    continue
                v = max(min(vv, 3.0), 0.01)
                break
        return v

    for row in calls or []:
        K = float(row.get("strike"))
        rec = by_strike.setdefault(K, {"strike": K})
        rec["call_OI"] = get_oi(row)
        rec["call_iv"] = get_iv(row)

    for row in puts or []:
        K = float(row.get("strike"))
        rec = by_strike.setdefault(K, {"strike": K})
        rec["put_OI"] = get_oi(row)
        rec["put_iv"] = get_iv(row)

    for rec in by_strike.values():
        rec.setdefault("call_OI", 0.0)
        rec.setdefault("put_OI", 0.0)
        rec.setdefault("call_iv", None)
        rec.setdefault("put_iv", None)
        rec["iv"] = None
    rows = list(by_strike.values())
    rows.sort(key=lambda r: r["strike"])
    return rows

st.title("Net GEX calculator")
ticker = st.text_input("Тикер", value="SPY")

raw = get_option_chain(ticker)
dump_json("provider_raw.json", raw)
chain = extract_chain(raw)

expirations = sorted(set(blk["expiration"] for blk in chain["options"] if blk["expiration"] is not None))
sel_exp = st.selectbox("Дата экспирации", options=expirations, index=0, format_func=lambda x: pd.to_datetime(int(x), unit="s").strftime("%Y-%m-%d"))

S = chain["S"]
snapshot = chain["snapshot"]
st.caption(f"Spot S = {S:.3f} | Snapshot = {pd.to_datetime(int(snapshot), unit='s', utc=True).strftime('%Y-%m-%d %H:%M')} UTC" if snapshot else f"Spot S = {S:.3f}")

opt_block = next((blk for blk in chain["options"] if blk["expiration"] == sel_exp), None)
if not opt_block:
    st.error("Нет данных по выбранной экспирации.")
    st.stop()

rows = normalize_rows(opt_block["calls"], opt_block["puts"])
df_provider = pd.DataFrame(rows)
st.subheader("Сырые данные провайдера (нормализованные)")
st.dataframe(df_provider, use_container_width=True)

res = calculate_net_gex(S, rows, expiry_ts=sel_exp, snapshot_ts=snapshot, core_K=11, M=100, scale_divisor=1000.0, use_regression_refine=False)

st.subheader("Net GEX по страйкам")
df_net = pd.DataFrame(res.rows)
st.dataframe(df_net, use_container_width=True)

st.subheader("Итоговая таблица (провайдер + Net GEX)")
df_merged = df_provider.merge(df_net[["strike", "NetGEX"]], on="strike", how="left")
st.dataframe(df_merged, use_container_width=True)

with st.expander("Параметры расчёта (контроль качества)"):
    m = res.metrics
    cols = st.columns(6)
    cols[0].metric("k", f"{m['k']:.4f}")
    cols[1].metric("Gamma avg", f"{m['gamma_avg']:.6e}")
    # FIX: correct key name
    cols[2].metric("IV median (exp)", f"{m.get('iv_median', 0.0):.4f}")
    cols[3].metric("T (days)", f"{m.get('T_days', 0.0):.2f}")
    cols[4].metric("S", f"{m['S']:.3f}" if 'S' in m else f"{S:.3f}")
    cols[5].metric("Core K", str(m.get("core_K", 11)))
    st.write({"core_strikes": m.get("core_strikes", []), "scale_divisor": 1000.0, "M": 100})

c1, c2 = st.columns(2)
with c1:
    st.download_button("Скачать итоговую таблицу (CSV)", data=df_merged.to_csv(index=False).encode("utf-8"), file_name=f"provider_plus_netgex_{ticker}_{pd.to_datetime(sel_exp, unit='s').date()}.csv", mime="text/csv")
with c2:
    files = list_debug_files()
    if files:
        st.download_button("Скачать последний debug-файл", data=open(files[-1], "rb").read(), file_name="last_debug.json", mime="application/json")

st.subheader("Debug / Logs")
st.caption("Логи: /tmp/streamlit_net_gex_logs/app.log")
