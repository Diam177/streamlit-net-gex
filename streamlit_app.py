
# streamlit_app.py
# UI for the Net GEX calculator with visibility of control metrics.
# Assumes services/api_client.get_option_chain exists and returns a raw provider payload.
# We *do not* change api_client endpoints; the fix is in IV handling + k and adding metrics.

from __future__ import annotations

import json
from typing import Any, Dict, List
import pandas as pd
import streamlit as st

from logger import get_logger
from services.net_gex import calculate_net_gex, time_to_expiry
from services.utils.debug import dump_json, dump_text, list_debug_files  # existing utils
from services.api_client import get_option_chain  # existing client (unchanged)

LOG = get_logger(__name__)

st.set_page_config(page_title="Net GEX calculator", layout="wide")


# ---------- helpers ----------

def extract_chain(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a normalized dict:
      {
        "S": float,
        "snapshot": int (seconds),
        "options": [
            {"expiration": int (seconds), "calls": [...], "puts": [...]},
            ...
        ]
      }
    """
    # Try typical Yahoo 'optionChain' result
    c = None
    if isinstance(raw, dict):
        oc = raw.get("optionChain") or raw.get("option_chain")
        if oc and isinstance(oc, dict):
            res = oc.get("result")
            if res and isinstance(res, list) and res:
                c = res[0]
        # Some providers return {'body': [{'options': [...], 'quote': {...}}]}
        if c is None and "body" in raw and isinstance(raw["body"], list) and raw["body"]:
            c = raw["body"][0]

    if not c:
        raise RuntimeError("Unexpected provider payload format")

    S = None
    snap = None
    quote = c.get("quote") or {}
    for key in ("regularMarketPrice", "regular_market_price", "price"):
        if key in quote:
            S = float(quote[key])
            break
    if S is None:
        # fallback for some payloads
        S = float(quote.get("close") or quote.get("last") or 0.0)

    # snapshot timestamp (seconds)
    snap = quote.get("regularMarketTime") or quote.get("regular_market_time") or quote.get("time")
    if isinstance(snap, str):
        try:
            snap = int(float(snap))
        except Exception:
            snap = None

    options = c.get("options") or []
    norm_opts = []
    for block in options:
        exp = block.get("expirationDate") or block.get("expiration") or block.get("date")
        calls = block.get("calls") or []
        puts = block.get("puts") or []
        norm_opts.append({"expiration": exp, "calls": calls, "puts": puts})

    return {"S": float(S), "snapshot": snap, "options": norm_opts}


def normalize_rows(calls: List[Dict[str, Any]], puts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Join calls & puts on strike. Extract OI, side IVs, and an aggregate 'iv' if provider supplies it.
    We DO NOT average with placeholders (1e-5) — that is handled inside net_gex.calculate_net_gex."""
    by_strike: Dict[float, Dict[str, Any]] = {}

    def take_iv(x: Dict[str, Any]) -> float | None:
        for k in ("impliedVolatility", "implied_volatility", "iv", "impliedVol"):
            if k in x and x[k] is not None:
                return x[k]
        return None

    for row in calls or []:
        K = float(row.get("strike"))
        rec = by_strike.setdefault(K, {"strike": K})
        rec["call_OI"] = float(row.get("openInterest") or row.get("oi") or 0)
        rec["call_iv"] = take_iv(row)

    for row in puts or []:
        K = float(row.get("strike"))
        rec = by_strike.setdefault(K, {"strike": K})
        rec["put_OI"] = float(row.get("openInterest") or row.get("oi") or 0)
        rec["put_iv"] = take_iv(row)

    # optional aggregated IV if provider offers per-strike blended IV
    for rec in by_strike.values():
        # some payloads include 'iv' at top level; keep placeholder None here
        rec.setdefault("call_OI", 0.0)
        rec.setdefault("put_OI", 0.0)
        rec.setdefault("call_iv", None)
        rec.setdefault("put_iv", None)
        # no aggregated 'iv' here; calculate_net_gex will decide based on sides + median
        rec["iv"] = None

    rows = list(by_strike.values())
    rows.sort(key=lambda r: r["strike"])
    return rows


# ---------- UI ----------

st.title("Net GEX calculator")
ticker = st.text_input("Тикер", value="SPY")

# Load provider payload (the client handles host/keys)
raw = get_option_chain(ticker)
dump_json("provider_raw.json", raw)

chain = extract_chain(raw)

# Expirations dropdown
expirations = [blk["expiration"] for blk in chain["options"] if blk["expiration"] is not None]
expirations = sorted(set(expirations))
exp_idx = 0
sel_exp = st.selectbox("Дата экспирации", options=expirations, index=exp_idx, format_func=lambda x: pd.to_datetime(int(x), unit="s").strftime("%Y-%m-%d"))

S = chain["S"]
snapshot = chain["snapshot"]

st.caption(f"Spot S = {S:.3f} | Snapshot = {pd.to_datetime(int(snapshot), unit='s', utc=True).strftime('%Y-%m-%d %H:%M')} UTC" if snapshot else f"Spot S = {S:.3f}")

# Find selected option block
opt_block = None
for blk in chain["options"]:
    if blk["expiration"] == sel_exp:
        opt_block = blk
        break

if not opt_block:
    st.error("Нет данных по выбранной экспирации.")
    st.stop()

# Provider table (normalized but *not* averaged IVs)
rows = normalize_rows(opt_block["calls"], opt_block["puts"])
df_provider = pd.DataFrame(rows)
st.subheader("Сырые данные провайдера (нормализованные)")
st.dataframe(df_provider, use_container_width=True)

# Calculate Net GEX
res = calculate_net_gex(S, rows, expiry_ts=sel_exp, snapshot_ts=snapshot, core_K=11, M=100, scale_divisor=1000.0, use_regression_refine=False)

# Net GEX table
st.subheader("Net GEX по страйкам")
df_net = pd.DataFrame(res.rows)
st.dataframe(df_net, use_container_width=True)

# Merge provider + Net GEX
st.subheader("Итоговая таблица (провайдер + Net GEX)")
df_merged = df_provider.merge(df_net[["strike", "NetGEX"]], on="strike", how="left")
st.dataframe(df_merged, use_container_width=True)

# Metrics block
with st.expander("Параметры расчёта (контроль качества)"):
    m = res.metrics
    cols = st.columns(6)
    cols[0].metric("k", f"{m['k']:.4f}")
    cols[1].metric("Gamma avg", f"{m['gamma_avg']:.6e}")
    cols[2].metric("IV median (core)", f"{m['iv_median_core']:.4f}")
    cols[3].metric("T (days)", f"{m['T_days']:.2f}")
    cols[4].metric("S", f"{m['S']:.3f}")
    cols[5].metric("Core K", str(m["core_K"]))
    st.write({"core_strikes": m["core_strikes"], "scale_divisor": m["scale_divisor"], "M": m["M"]})

# Downloads
c1, c2 = st.columns(2)
with c1:
    csv_bytes = df_merged.to_csv(index=False).encode("utf-8")
    st.download_button("Скачать итоговую таблицу (CSV)", data=csv_bytes, file_name=f"provider_plus_netgex_{ticker}_{pd.to_datetime(sel_exp, unit='s').date()}.csv", mime="text/csv")
with c2:
    dump_json("net_gex_result.json", {"metrics": res.metrics, "rows": res.rows})
    files = list_debug_files()
    if files:
        st.download_button("Скачать последний debug-файл", data=open(files[-1], "rb").read(), file_name="last_debug.json", mime="application/json")

# Logs (path is defined in logger.py)
st.subheader("Debug / Logs")
st.caption("Логи: /tmp/streamlit_net_gex_logs/app.log")
try:
    with open("/tmp/streamlit_net_gex_logs/app.log", "rb") as f:
        st.download_button("Скачать лог-файл", data=f.read(), file_name="app.log", mime="text/plain")
except Exception:
    st.caption("Лог-файл недоступен (появится после первого запуска на проде).")
