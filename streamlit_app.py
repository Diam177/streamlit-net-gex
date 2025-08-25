import json
import time
import pandas as pd
import math
import streamlit as st
import numpy as np

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

def ts2str(ts: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ts))

def extract_chain(raw: dict) -> dict:
    """
    Возвращает объект result[0] вне зависимости от того, пришёл ли 'optionChain' или 'body'.
    """
    if "optionChain" in raw and raw["optionChain"].get("result"):
        return raw["optionChain"]["result"][0]

    if "body" in raw and isinstance(raw["body"], list) and raw["body"]:
        return raw["body"][0]

    # Если формат неизвестен — пробрасываем исключение для видимой ошибки + дебага
    raise KeyError("Unsupported response format: neither 'optionChain' nor 'body'")

# 1) общий снимок (для списка дат и S)
try:
    raw_initial = get_option_chain(ticker)
    res0 = extract_chain(raw_initial)
    expirations = res0.get("expirationDates", []) or []
    quote = res0.get("quote", {}) or {}
    snapshot_ts = int(quote.get("regularMarketTime") or quote.get("regularMarketTime", 0))
    spot = float(quote.get("regularMarketPrice"))
except Exception as e:
    st.error(f"Ошибка загрузки данных по {ticker}: {e}")
    st.stop()

if not expirations:
    st.error("Провайдер вернул пустой список дат экспирации.")
    st.stop()

expiry_ts = st.selectbox("Дата экспирации", options=expirations, index=0, format_func=ts2str)
st.write(f"Spot S = {spot} | Snapshot = {ts2str(snapshot_ts)} UTC")

if st.button("Рассчитать", type="primary"):
    try:
        # 2) конкретная экспирация — библиотека отдаст уже нормализованный ответ
        raw = get_option_chain(ticker, expiry_ts=int(expiry_ts))
        res = extract_chain(raw)

        options_blocks = res.get("options", [])
        if not options_blocks:
            st.error("В ответе нет блока options (calls/puts). Проверьте тариф/провайдера.")
            st.stop()
        block = options_blocks[0]

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

        result = calculate_net_gex(
            df=df_raw[["strike", "call_OI", "put_OI", "iv"]],
            S=float(spot),
            expiry_ts=int(expiry_ts),
            snapshot_ts=int(snapshot_ts),
        )
        df_gex = result["table"]

        # --- Classic k calibration (optional, aligns scale to Black–Scholes gamma*S*100) ---
        S = float(spot)
        T_years = max((int(expiry_ts) - int(snapshot_ts)) / 31536000.0, 1e-6)

        iv_ser = pd.to_numeric(df_raw.get("iv"), errors="coerce")
        try:
            if float(iv_ser.median(skipna=True)) > 1.0:
                iv_ser = iv_ser / 100.0
        except Exception:
            pass
        iv_ser = iv_ser.clip(0.01, 3.0)

        def _phi(x):
            return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)

        def _gamma_bs(Sval, K, Tval, sigma):
            sigma = float(max(sigma, 1e-6))
            Tval = float(max(Tval, 1e-6))
            try:
                d1 = (math.log(Sval / float(K)) + 0.5 * sigma * sigma * Tval) / (sigma * math.sqrt(Tval))
            except Exception:
                return 0.0
            return _phi(d1) / (Sval * sigma * math.sqrt(Tval))

        gamma_vals = [
            _gamma_bs(S, float(k), T_years, float(s)) if pd.notna(s) and pd.notna(k) else 0.0
            for k, s in zip(df_raw["strike"], iv_ser)
        ]
        gS = np.array(gamma_vals) * S * 100.0  # classic scale per strike

        delta_oi = (
            pd.to_numeric(df_raw["call_OI"], errors="coerce").fillna(0)
            - pd.to_numeric(df_raw["put_OI"], errors="coerce").fillna(0)
        )
        netgex_series = df_gex.set_index("strike")["NetGEX"].reindex(df_raw["strike"])

        mask = (delta_oi != 0) & netgex_series.notna() & np.isfinite(netgex_series.astype(float)) & np.isfinite(gS)
        try:
            k_current = float(np.median(np.abs(netgex_series[mask].astype(float)) / (np.abs(delta_oi[mask].astype(float)) + 1e-12)))
            k_classic = float(np.median(gS[mask]))
            if k_current > 0 and np.isfinite(k_current) and np.isfinite(k_classic) and k_classic > 0:
                scale = k_classic / k_current
                df_gex["NetGEX"] = pd.to_numeric(df_gex["NetGEX"], errors="coerce") * scale
        except Exception:
            pass
        # --- end calibration ---

        st.subheader("Net GEX по страйкам")
        st.dataframe(df_gex, use_container_width=True)

        df_out = df_raw.merge(df_gex[["strike", "NetGEX"]], on="strike", how="left")
        df_out = df_out[["strike", "call_OI", "put_OI", "call_volume", "put_volume", "iv", "NetGEX"]]
        st.subheader("Итоговая таблица (провайдер + Net GEX)")
        st.dataframe(df_out.fillna(0), use_container_width=True)

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
