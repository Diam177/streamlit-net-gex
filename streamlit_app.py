import os
import time
import math
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

from api_client import fetch_chain
from net_gex import compute_variant_b, VariantBConfig

st.set_page_config(page_title="Net GEX calculator", layout="wide")

st.title("Net GEX calculator")

col1, col2 = st.columns([1,2])
with col1:
    ticker = st.text_input("Тикер", value="SPY").strip().upper()
with col2:
    st.caption("Секреты: RAPIDAPI_HOST, RAPIDAPI_KEY (App settings → Secrets).")

# Fetch chain
try:
    spot, snapshot, expirations, frames = fetch_chain(ticker)
except Exception as e:
    st.error(f"Не удалось получить данные: {e}")
    st.stop()

# Expiration select
def _label(ts: int) -> str:
    try:
        return time.strftime("%Y-%m-%d (%s)" % ts, time.gmtime(ts))
    except Exception:
        return str(ts)
default_exp = expirations[0] if expirations else 0
exp = st.selectbox("Экспирация", options=expirations, index=0 if expirations else None, format_func=lambda x: time.strftime("%Y-%m-%d", time.gmtime(int(x))) if x else "n/a")

df = frames.get(int(exp)) if frames else pd.DataFrame()
if df.empty:
    st.warning("Нет доступных страйков для выбранной экспирации")
    st.stop()

st.caption(f"Spot: {spot:.2f} | Snapshot: {time.strftime('%Y-%m-%d', time.gmtime(snapshot))}")

st.subheader("Сырые данные провайдера (нормализованные)")
st.dataframe(df, use_container_width=True)

# Compute Variant B net GEX
days_to_exp = max( (int(exp)-int(time.time())) / 86400.0, 0.0 )
cfg = VariantBConfig(
    iv_floor= float(os.getenv("NETGEX_IV_FLOOR", "0.05")),
    t_days_floor= float(os.getenv("NETGEX_T_DAYS_FLOOR", "1")),
    contract_mult= float(os.getenv("NETGEX_CONTRACT_MULT", "100")),
    scale= float(os.getenv("NETGEX_SCALE", "0.001"))  # small default to keep numbers reasonable
)
netgex_series = compute_variant_b(df, spot, days_to_exp, cfg)
table = df.copy()
table["NetGEX"] = netgex_series

st.subheader("Net GEX по страйкам (Variant B, k_classic·ΔOI)")
st.dataframe(pd.DataFrame({"strike": table["strike"], "NetGEX": table["NetGEX"].round(1)}), use_container_width=True)

st.subheader("Итоговая таблица (провайдер + Net GEX)")
st.dataframe(table, use_container_width=True)

# Bar chart
st.subheader("Net GEX по страйкам")
strikes = table["strike"].astype(float).values
values = table["NetGEX"].astype(float).values

colors = ["#ff4d4f" if v < 0 else "#22c55e" for v in values]
fig = go.Figure()
fig.add_trace(go.Bar(x=strikes, y=values, marker_color=colors, name="Net GEX"))
# price line
fig.add_shape(type="line",
              x0=spot, x1=spot, y0=min(0, values.min()), y1=max(values.max(), 0),
              line=dict(color="#f59e0b", width=2))
fig.add_annotation(x=spot, y=values.max()*0.9, text=f"Price: {spot:.2f}", showarrow=False, font=dict(color="#f59e0b"))
fig.update_layout(height=520,
                  showlegend=False,
                  xaxis_title="Strike",
                  yaxis_title="Net GEX")
st.plotly_chart(fig, use_container_width=True)
