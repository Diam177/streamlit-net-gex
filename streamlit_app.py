import time
import json
import pandas as pd
import streamlit as st

from logger import get_logger, get_log_file_path
from services.api_client import get_option_chain
from services.net_gex import calculate_net_gex
from services.utils.debug import list_debug_files

st.set_page_config(page_title="Net GEX — Streamlit", layout="wide")
logger = get_logger("ui")

st.title("Net GEX calculator")

# Панель настроек / debug
col_a, col_b, col_c = st.columns([1,1,2])
with col_a:
    ticker = st.text_input("Тикер", "SPY").strip().upper()
with col_b:
    debug_mode = st.toggle("Debug mode", value=True, help="Сохранять сырые ответы и промежуточные расчёты")
with col_c:
    st.caption("Секреты должны быть заданы: RAPIDAPI_HOST и RAPIDAPI_KEY (App settings → Secrets).")

# Получаем цепочку для списка дат экспирации и цены
raw = None
expirations = []
snapshot_ts = None
spot = None

try:
    raw = get_option_chain(ticker)
    res = raw["optionChain"]["result"][0]
    expirations = res["expirationDates"]
    snapshot_ts = res["quote"]["regularMarketTime"]
    spot = res["quote"]["regularMarketPrice"]
except Exception as e:
    st.error(f"Ошибка загрузки данных по {ticker}: {e}")
    st.stop()

# Выбор экспирации (по умолчанию ближайшая)
exp_idx = 0
if not expirations:
    st.error("Провайдер вернул пустой список дат экспирации.")
    st.stop()

def _ts_to_str(ts: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ts))

expiry_ts = st.selectbox(
    "Дата экспирации",
    options=expirations,
    index=exp_idx,
    format_func=_ts_to_str
)

st.write(f"Spot S = {spot} | Snapshot = {_ts_to_str(snapshot_ts)} UTC")

# Кнопка расчёта
if st.button("Рассчитать", type="primary"):
    try:
        # Находим блок с нужной экспирацией
        options_blocks = res["options"]
        block = None
        for b in options_blocks:
            # у yahoo finance внутри options список с 1 элементом для выбранной expiry,
            # но подстрахуемся и фильтранем по «expirationDate», если он есть
            if "expirationDate" in b and int(b["expirationDate"]) != int(expiry_ts):
                continue
            block = b
            break
        if not block:
            st.error("Не найден блок options для выбранной даты экспирации.")
            st.stop()

        # Собираем таблицу с провайдера
        calls = block.get("calls", [])
        puts  = block.get("puts", [])

        # Индексируем по страйку для merge
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

        # Расчёт Net GEX
        result = calculate_net_gex(
            df=df_raw[["strike", "call_OI", "put_OI", "iv"]],
            S=float(spot),
            expiry_ts=int(expiry_ts),
            snapshot_ts=int(snapshot_ts),
        )
        df_gex = result["table"]

        st.subheader("Net GEX по страйкам")
        st.dataframe(df_gex, use_container_width=True)

        # Объединённая таблица: Strike, Call OI, Put OI, Call Volume, Put Volume, IV, Net GEX
        df_out = df_raw.merge(df_gex[["strike", "NetGEX"]], on="strike", how="left")
        df_out = df_out[["strike", "call_OI", "put_OI", "call_volume", "put_volume", "iv", "NetGEX"]]
        st.subheader("Итоговая таблица (провайдер + Net GEX)")
        st.dataframe(df_out.fillna(0), use_container_width=True)

        # Кнопки выгрузки
        c1, c2, c3 = st.columns(3)
        with c1:
            st.download_button(
                "Скачать итоговую таблицу (CSV)",
                data=df_out.to_csv(index=False).encode("utf-8"),
                file_name=f"net_gex_{ticker}_{_ts_to_str(expiry_ts)}.csv",
                mime="text/csv",
            )
        with c2:
            st.download_button(
                "Скачать сырые данные (JSON)",
                data=json.dumps(df_raw.fillna(0).to_dict(orient="records"), ensure_ascii=False, indent=2),
                file_name=f"raw_{ticker}_{_ts_to_str(expiry_ts)}.json",
                mime="application/json",
            )
        with c3:
            # Последние файлы дебага для быстрой выгрузки
            files = list_debug_files()
            if files:
                last_file = files[0]
                with open(last_file, "rb") as f:
                    st.download_button("Скачать последний debug-файл", data=f.read(), file_name=last_file.split("/")[-1])
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
