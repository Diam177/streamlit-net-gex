import json
import time
import pandas as pd
import streamlit as st

from logger import get_logger, get_log_file_path
from services.api_client import get_option_chain
from services.net_gex import calculate_net_gex
from services.utils.debug import list_debug_files

st.set_page_config(page_title="Net GEX — Streamlit", layout="wide")
logger = get_logger("ui")

st.title("Net GEX calculator")

col_a, col_b, col_c = st.columns([1,1,2])
with col_a:
    ticker = st.text_input("Тикер", "SPY").strip().upper()
with col_b:
    debug_mode = st.toggle("Debug mode", value=True)
with col_c:
    st.caption("Секреты: RAPIDAPI_HOST, RAPIDAPI_KEY (App settings → Secrets).")

# 1) общий снимок (для списка дат и S)
try:
    raw_initial = get_option_chain(ticker)
    res0 = raw_initial["optionChain"]["result"][0]
    expirations = res0.get("expirationDates", []) or []
    snapshot_ts = res0["quote"]["regularMarketTime"]
    spot = res0["quote"]["regularMarketPrice"]
except Exception as e:
    st.error(f"Ошибка загрузки данных по {ticker}: {e}")
    st.stop()

if not expirations:
    st.error("Провайдер вернул пустой список дат экспирации.")
    st.stop()

def ts2str(ts: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ts))

expiry_ts = st.selectbox("Дата экспирации", options=expirations, index=0, format_func=ts2str)
st.write(f"Spot S = {spot} | Snapshot = {ts2str(snapshot_ts)} UTC")

if st.button("Рассчитать", type="primary"):
    try:
        # 2) конкретная экспирация
        raw = get_option_chain(ticker, expiry_ts=int(expiry_ts))
        res = raw["optionChain"]["result"][0]
        options_blocks = res.get("options", [])
        if not options_blocks:
            st.error("В ответе нет блока options.")
            st.stop()
        block = options_blocks[0]

        # 3) нормализация сырых данных
        calls = block.get("calls", [])
        puts  = block.get("puts", [])

        df_calls = pd.DataFrame([{
            "strike": c.get("strike"),
            "call_OI": c.get("openInterest", 0),
            "call_volume": c.get("volume", 0),
            "call_iv": c.get("impliedVolatility", None),
        } for c in calls])

        df_puts = pd.DataFrame([{
            "strike": p.get("strike"),
            "put_OI": p.get("openInterest", 0),
            "put_volume": p.get("volume", 0),
            "put_iv": p.get("impliedVolatility", None),
        } for p in puts])

        df_raw = pd.merge(df_calls, df_puts, on="strike", how="outer").sort_values("strike").reset_index(drop=True)
        df_raw["iv"] = pd.concat([df_raw["call_iv"], df_raw["put_iv"]], axis=1).mean(axis=1, skipna=True)

        st.subheader("Сырые данные провайдера (нормализованные)")
        st.dataframe(df_raw.fillna(0), use_container_width=True)

        # 4) расчёт Net GEX
        result = calculate_net_gex(
            df=df_raw[["strike", "call_OI", "put_OI", "iv"]],
            S=float(spot),
            expiry_ts=int(expiry_ts),
            snapshot_ts=int(snapshot_ts),
        )
        df_gex = result["table"]

        st.subheader("Net GEX по страйкам")
        st.dataframe(df_gex, use_container_width=True)

        # 5) итоговая таблица
        df_out = df_raw.merge(df_gex[["strike", "NetGEX"]], on="strike", how="left")
        df_out = df_out[["strike", "call_OI", "put_OI", "call_volume", "put_volume", "iv", "NetGEX"]]
        st.subheader("Итоговая таблица (провайдер + Net GEX)")
        st.dataframe(df_out.fillna(0), use_container_width=True)

        # 6) выгрузки
        c1, c2, c3 = st.columns(3)
        with c1:
            st.download_button(
                "Скачать итоговую таблицу (CSV)",
                data=df_out.to_csv(index=False).encode("utf-8"),
                file_name=f"net_gex_{ticker}_{ts2str(expiry_ts)}.csv",
                mime="text/csv",
            )
        with c2:
            st.download_button(
                "Скачать сырые данные (JSON)",
                data=json.dumps(df_raw.fillna(0).to_dict(orient="records"), ensure_ascii=False, indent=2),
                file_name=f"raw_{ticker}_{ts2str(expiry_ts)}.json",
                mime="application/json",
            )
        with c3:
            files = list_debug_files()
            if files:
                last_file = files[0]
                with open(last_file, "rb") as f:
                    st.download_button("Скачать последний debug-файл", data=f.read(), file_name=last_file.split('/')[-1])
            else:
                st.caption("Нет debug-файлов")

    except Exception as e:
        logger.exception("Calculation failed")
        st.exception(e)

st.divider()
st.subheader("Debug / Logs")
log_path = get_log_file_path()
st.caption(f"Логи: {log_path}")
try:
    with open(log_path, "rb") as f:
        st.download_button("Скачать лог-файл", data=f.read(), file_name="app.log")
except FileNotFoundError:
    st.caption("Лог-файл пока не создан")
