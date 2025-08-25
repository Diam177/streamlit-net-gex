import os, json
from datetime import datetime, timezone
import streamlit as st
from services.api_client import RapidYahooClient
from services.net_gex import compute_net_gex_from_payload
from utils.logger import get_logger

st.set_page_config(page_title="Net GEX (RapidAPI Yahoo Finance 15)", layout="wide")
logger = get_logger()

st.title("Net GEX — RapidAPI Yahoo Finance 15")

HOST = st.secrets.get("RAPIDAPI_HOST", os.environ.get("RAPIDAPI_HOST", "yahoo-finance15.p.rapidapi.com"))
KEY  = st.secrets.get("RAPIDAPI_KEY",  os.environ.get("RAPIDAPI_KEY", None))
if not KEY:
    st.error("RAPIDAPI_KEY не задан. Добавьте ключ в Streamlit Secrets.")
    st.stop()

client = RapidYahooClient(host=HOST, key=KEY)
st.sidebar.header("Параметры")
ticker = st.sidebar.text_input("Тикер базового актива", value="SPY").strip().upper()

exp_payload = None; exp_error = None
try:
    exp_payload = client.get_options_chain(ticker)
    nearest_epoch = client.pick_nearest_expiration(exp_payload)
    exp_dates = exp_payload.get("expirationDates") or []
    if not exp_dates and isinstance(exp_payload.get("chains[0]"), dict):
        exp_dates = [exp_payload["chains[0]"].get("expiration")]
    choices = []
    for e in exp_dates:
        try:
            e = int(e)
            label = datetime.fromtimestamp(e, tz=timezone.utc).strftime("%Y-%m-%d")
            choices.append((label, e))
        except Exception:
            continue
    if not choices and nearest_epoch:
        label = datetime.fromtimestamp(int(nearest_epoch), tz=timezone.utc).strftime("%Y-%m-%d")
        choices = [(label, int(nearest_epoch))]
except Exception as ex:
    logger.exception("Failed to fetch expirations: %s", ex)
    exp_error = str(ex)

if exp_error:
    st.error(f"Не удалось получить даты экспирации: {exp_error}")
    st.stop()
if not choices:
    st.warning("Для этого тикера не найдено дат экспирации.")
    st.stop()

default_index = 0
if len(choices) > 1 and nearest_epoch:
    for i, (_, e) in enumerate(choices):
        if int(e) == int(nearest_epoch):
            default_index = i
            break

exp_label = st.sidebar.selectbox("Дата экспирации", options=[c[0] for c in choices], index=default_index)
expiration_epoch = choices[[c[0] for c in choices].index(exp_label)][1]

calc = st.sidebar.button("Рассчитать", type="primary")

with st.expander("🔧 Debug / Raw"):
    st.caption("Сырые данные, метаданные запроса и полезные кнопки для диагностики.")
    if exp_payload:
        st.json(exp_payload if isinstance(exp_payload, dict) else {"payload": exp_payload})
        raw_bytes = json.dumps(exp_payload).encode("utf-8")
        st.download_button("Скачать raw.json", data=raw_bytes, file_name=f"{ticker}_raw.json")

if calc:
    try:
        payload = client.get_options_chain(ticker, expiration=int(expiration_epoch))
        table_basic, table_iv, meta = compute_net_gex_from_payload(payload, int(expiration_epoch))
        c1, c2 = st.columns([1,1])
        with c1:
            st.subheader("Таблица (OI/Volume + Net GEX)")
            st.dataframe(table_basic, use_container_width=True)
        with c2:
            st.subheader("Таблица (с IV + Net GEX)")
            st.dataframe(table_iv, use_container_width=True)

        st.markdown("---")
        st.subheader("Метаданные расчёта")
        meta_show = meta.copy()
        meta_show["expiration_date_utc"] = datetime.fromtimestamp(meta["expiration_epoch"], tz=timezone.utc).strftime("%Y-%m-%d")
        st.json(meta_show)

    except Exception as ex:
        logger.exception("Calculation error: %s", ex)
        st.error(f"Ошибка при расчёте: {ex}")
else:
    st.info("Выберите тикер и дату экспирации, затем нажмите **Рассчитать**.")
