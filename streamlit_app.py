# -*- coding: utf-8 -*-
import math
import json
import time
from typing import List, Dict, Any

import numpy as np
import pandas as pd
import streamlit as st

from logger import get_logger, get_log_file_path
from services.api_client import get_option_chain
from services.net_gex import calculate_net_gex
from services.utils.debug import list_debug_files
from netgex_chart import render_net_gex_bar_chart

st.set_page_config(page_title="Net GEX — Streamlit", layout="wide")
logger = get_logger("ui")

st.title("Net GEX calculator")

# ---------- helpers ----------

def _to_ts(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0

def _as_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _phi(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)

def _gamma_bs(S: float, K: float, T: float, sigma: float) -> float:
    sigma = float(max(sigma, 1e-6))
    T = float(max(T, 1e-6))
    try:
        d1 = (math.log(S / float(K)) + 0.5 * sigma * sigma * T) / (sigma * math.sqrt(T))
    except Exception:
        return 0.0
    return _phi(d1) / (S * sigma * math.sqrt(T))  # одинаково для call/put

def _format_date(ts: int) -> str:
    try:
        return time.strftime("%Y-%m-%d", time.gmtime(int(ts)))
    except Exception:
        return str(ts)


# ---------- robust extractors for multiple provider shapes ----------

def _get_first(d: dict, keys):
    for k in keys:
        v = d.get(k) if isinstance(d, dict) else None
        if v is not None:
            return v
    return None

def _extract_root(chain_obj: Any) -> Dict[str, Any]:
    """
    Support responses like:
    - {"body":{"result":[{...}]}}          (RapidAPI /op/option/*)
    - {"optionChain":{"result":[{...}]}}   (Yahoo query2 v7)
    - {"result":[{...}]}                   (some mirrors)
    - direct dict with "options"/"quote"
    - list-wrapped variants
    """
    if isinstance(chain_obj, dict):
        body = chain_obj.get("body")
        if isinstance(body, dict):
            res = body.get("result")
            if isinstance(res, list) and res:
                return res[0]
        oc = chain_obj.get("optionChain")
        if isinstance(oc, dict):
            res = oc.get("result")
            if isinstance(res, list) and res:
                return res[0]
        res = chain_obj.get("result")
        if isinstance(res, list) and res:
            return res[0]
        # as-is
        return chain_obj
    if isinstance(chain_obj, list):
        for item in chain_obj:
            if isinstance(item, dict):
                # try unwrap this dict
                return _extract_root(item)
        return {}
    return {}

def _extract_meta(chain_obj: Any) -> Dict[str, Any]:
    """
    Return tuple: (root, quote, expirations, options_blocks)
    """
    root = _extract_root(chain_obj) or {}
    # quote heuristic
    quote = _get_first(root, ["quote"]) or             _get_first(chain_obj if isinstance(chain_obj, dict) else {}, ["quote"]) or             _get_first(_get_first(chain_obj if isinstance(chain_obj, dict) else {}, ["meta"]) or {}, ["quote"]) or {}
    # expirations
    expirations = root.get("expirationDates") or root.get("expirations") or []
    # options
    options_blocks = root.get("options") or []
    return root, quote, expirations, options_blocks

# ---------- UI controls ----------

col1, col2, col3 = st.columns([1,1,1.2])
with col1:
    ticker = st.text_input("Тикер", value="SPY").strip().upper()

# Загружаем опционную цепочку
chain = None
quote = {}
expirations = []

if ticker:
    try:
        chain = get_option_chain(ticker)
    except Exception as e:
        st.error(f"Ошибка получения цепочки: {e}")
        st.stop()

# Универсальный парс разных провайдеров
def _extract_first_block(chain_obj: Any) -> Dict[str, Any]:  # legacy (kept for backward compat)

    # ожидаем либо список с элементами, либо словарь
    if isinstance(chain_obj, list):
        for item in chain_obj:
            if isinstance(item, dict) and ("options" in item or "optionChain" in item):
                return item
    elif isinstance(chain_obj, dict):
        return chain_obj
    return {}

root, quote, expirations, _legacy_options = _extract_meta(chain)
res0 = root
quote = quote or {}
spot = _as_float(quote.get("regularMarketPrice", quote.get("price", None)), default=np.nan)
snapshot_ts = _to_ts(quote.get("regularMarketTime", quote.get("timestamp", time.time())))

# expirations
expirations = expirations or []
expirations = [ _to_ts(x) for x in expirations if _to_ts(x) > 0 ]
expirations = sorted(set(expirations))

with col2:
    if expirations:
        labels = [f"{_format_date(ts)} ({ts})" for ts in expirations]
        idx = st.selectbox("Экспирация", options=list(range(len(expirations))), format_func=lambda i: labels[i])
        expiry_ts = expirations[idx]
    else:
        st.warning("Нет доступных дат экспирации")
        expiry_ts = 0

with col3:
    st.caption(f"Spot: {spot if np.isfinite(spot) else 'n/a'} | Snapshot: {_format_date(snapshot_ts)}")

# Кнопка расчёта
run = st.button("Рассчитать уровни")

if run:
    try:
        # --- достаём calls/puts для выбранной экспирации ---
        options_blocks = (res0.get("options") or _legacy_options or [])
        if not options_blocks:
            st.error("В ответе нет блока options (calls/puts). Проверьте тариф/провайдера.")
            st.stop()

        # Найдём блок с нужной датой (у Yahoo это options[{expirationDate, calls, puts}])
        block = None
        for b in options_blocks:
            if _to_ts(b.get("expirationDate")) == int(expiry_ts):
                block = b
                break
        if block is None:
            block = options_blocks[0]  # как минимум что-то возьмём

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

        # --- IV: не перезаписываем, если уже корректно, иначе фолбэк ---
        if "iv" not in df_raw.columns or df_raw["iv"].isna().all():
            df_raw["iv"] = pd.concat([df_raw["call_iv"], df_raw["put_iv"]], axis=1).mean(axis=1, skipna=True)
        # проценты -> доли, если нужно
        try:
            med_iv = float(pd.to_numeric(df_raw["iv"], errors="coerce").median(skipna=True))
            if med_iv > 1.0:
                df_raw["iv"] = pd.to_numeric(df_raw["iv"], errors="coerce") / 100.0
        except Exception:
            pass

        st.subheader("Сырые данные провайдера (нормализованные)")
        st.dataframe(df_raw.fillna(0), use_container_width=True)

        # --- базовый расчёт из services.net_gex (оставляем как есть) ---
        result = calculate_net_gex(
            df=df_raw[["strike", "call_OI", "put_OI", "iv"]],
            S=float(spot),
            expiry_ts=int(expiry_ts),
            snapshot_ts=int(snapshot_ts),
        )
        df_gex = result.get("table", pd.DataFrame())
        if df_gex is None or df_gex.empty:
            df_gex = pd.DataFrame({"strike": df_raw["strike"], "NetGEX": 0.0})

        # --- ВАРИАНТ B: калибровка к классике и NetGEX = k_classic * ΔOI ---
        S = float(spot) if np.isfinite(spot) else float(df_raw["strike"].median())
        T_years = max((int(expiry_ts) - int(snapshot_ts)) / 31536000.0, 1e-6)

        # выравниваем по страйкам и берём ATM-ядро (21 ближайший страйк)
        df_align = df_raw.merge(df_gex[["strike", "NetGEX"]], on="strike", how="inner").copy()
        df_align["strike"] = pd.to_numeric(df_align["strike"], errors="coerce")
        df_align = df_align.dropna(subset=["strike"])
        df_align["_dist"] = (df_align["strike"] - S).abs()
        df_align = df_align.sort_values("_dist").head(21)

        iv_align = pd.to_numeric(df_align.get("iv"), errors="coerce").clip(0.01, 3.0)
        try:
            if float(iv_align.median(skipna=True)) > 1.0:
                iv_align = iv_align / 100.0
        except Exception:
            pass

        gamma_vals = np.array([
            _gamma_bs(S, float(K), T_years, float(sig)) if pd.notna(sig) and pd.notna(K) else 0.0
            for K, sig in zip(df_align["strike"], iv_align)
        ], dtype=float)
        gS = gamma_vals * S * 100.0

        finite_gS = np.isfinite(gS)
        if finite_gS.any():
            k_classic = float(np.median(gS[finite_gS]))
        else:
            # очень защитный фолбэк
            k_classic = float(S * 100.0 / (S * max(float(iv_align.median(skipna=True)), 0.3) * math.sqrt(max(T_years, 1e-6)) * math.sqrt(2 * math.pi)))

        # ΔOI по всем страйкам
        dseries = (
            pd.to_numeric(df_raw["call_OI"], errors="coerce").fillna(0)
            - pd.to_numeric(df_raw["put_OI"], errors="coerce").fillna(0)
        )
        # переносим ΔOI на df_gex по strike
        df_map = pd.DataFrame({"strike": df_raw["strike"], "_d": dseries})
        df_gex = df_gex.merge(df_map, on="strike", how="left")
        df_gex["NetGEX"] = pd.to_numeric(df_gex["_d"], errors="coerce").fillna(0).astype(float) * float(k_classic)
        df_gex.drop(columns=["_d"], inplace=True, errors="ignore")

        st.subheader("Net GEX по страйкам (Variant B, k_classic·ΔOI)")
        st.dataframe(df_gex[["strike", "NetGEX"]].fillna(0), use_container_width=True)

        # --- Итоговая таблица ---
        df_out = df_raw.merge(df_gex[["strike", "NetGEX"]], on="strike", how="left")
        df_out = df_out[["strike", "call_OI", "put_OI", "call_volume", "put_volume", "iv", "NetGEX"]]
        st.subheader("Итоговая таблица (провайдер + Net GEX)")
        st.dataframe(df_out.fillna(0), use_container_width=True)

        # --- График ---
        st.markdown("---")
        render_net_gex_bar_chart(df_out, S, ticker)

        # --- Кнопки скачивания/отладка ---
        c1, c2, c3 = st.columns(3)
        with c1:
            st.download_button(
                "Скачать итог (CSV)",
                data=df_out.to_csv(index=False).encode("utf-8"),
                file_name=f"{ticker}_netgex_{_format_date(snapshot_ts)}.csv",
                mime="text/csv"
            )
        with c2:
            st.download_button(
                "Скачать итог (JSON)",
                data=df_out.to_json(orient="records", force_ascii=False).encode("utf-8"),
                file_name=f"{ticker}_netgex_{_format_date(snapshot_ts)}.json",
                mime="application/json"
            )
        with c3:
            # список debug-файлов из /tmp
            dbg = list_debug_files()
            if dbg:
                st.caption("Debug-файлы:")
                for pth in dbg:
                    with open(pth, "rb") as f:
                        st.download_button(f"Скачать {pth.split('/')[-1]}", data=f.read(), file_name=pth.split('/')[-1])
            else:
                st.caption("Нет debug-файлов")

    except Exception as e:
        logger.exception("Calculation failed")
        st.exception(e)

# --- Логи ---
st.divider()
st.subheader("Debug / Logs")
log_path = get_log_file_path()
st.caption(f"Логи: {log_path}")
try:
    with open(log_path, "rb") as f:
        st.download_button("Скачать лог-файл", data=f.read(), file_name="app.log")
except FileNotFoundError:
    st.caption("Лог-файл пока не создан")
