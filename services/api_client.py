import os
import time
import requests
from typing import Any, Dict, Optional, Tuple

from logger import get_logger
from services.utils.debug import dump_json, safe_redact

logger = get_logger("api")

RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "").strip()
RAPIDAPI_KEY  = os.getenv("RAPIDAPI_KEY", "").strip()

COMMON_HEADERS = {
    "x-rapidapi-host": RAPIDAPI_HOST,
    "x-rapidapi-key":  RAPIDAPI_KEY,
    # Некоторые провайдеры капризничают без UA
    "User-Agent": "net-gex/1.0 (+streamlit)"
}

class ApiError(RuntimeError):
    pass

def _require_secrets():
    missing = []
    if not RAPIDAPI_HOST: missing.append("RAPIDAPI_HOST")
    if not RAPIDAPI_KEY:  missing.append("RAPIDAPI_KEY")
    if missing:
        raise ApiError(f"Missing secrets: {', '.join(missing)} (Streamlit → App settings → Secrets)")

def _http_get(url: str, params: Optional[dict] = None) -> Dict[str, Any]:
    t0 = time.perf_counter()
    resp = requests.get(url, headers=COMMON_HEADERS, params=params, timeout=30)
    dt = time.perf_counter() - t0
    logger.info(f"GET {url} params={params} → {resp.status_code} in {dt:.3f}s")
    if resp.status_code >= 400:
        # пишем контекст для дебага
        dump_json("http_error", {"url": url, "params": params, "headers": safe_redact(COMMON_HEADERS), "status": resp.status_code, "text": resp.text[:800]})
        resp.raise_for_status()
    data = resp.json()
    dump_json("http_ok", {"url": url, "params": params, "headers": safe_redact(COMMON_HEADERS)})
    dump_json("http_response", data)
    return data

def _as_standard_option_chain(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Приводим ответ к виду:
    { "optionChain": { "result": [ { "quote": {...}, "expirationDates": [...], "options": [ {...} ] } ], "error": null } }
    Большинство RapidAPI-провайдеров уже отдают так; на всякий случай оставляем точку расширения.
    """
    if "optionChain" in raw and "result" in raw["optionChain"]:
        return raw  # уже стандарт

    # Некоторые редкие провайдеры кладут полезное прямо в корень
    # Здесь можно добавить маппинг при необходимости.
    return raw  # по умолчанию — без преобразования

def _host_is(h: str) -> bool:
    return RAPIDAPI_HOST.lower() == h.lower()

def get_option_chain(ticker: str, expiry_ts: Optional[int] = None) -> Dict[str, Any]:
    """
    Универсальный клиент:
    - Для yh-finance.p.rapidapi.com → /stock/v3/get-options
    - Для yahoo-finance15.p.rapidapi.com → пробуем несколько путей
    Возвращает JSON в «стандартном» формате (см. _as_standard_option_chain).
    """
    _require_secrets()
    base = f"https://{RAPIDAPI_HOST}"

    # 1) Apidojo (yh-finance)
    if _host_is("yh-finance.p.rapidapi.com"):
        url = f"{base}/stock/v3/get-options"
        params = {"symbol": ticker}
        if expiry_ts:
            params["date"] = str(expiry_ts)
        data = _http_get(url, params=params)
        return _as_standard_option_chain(data)

    # 2) Sparior (yahoo-finance15) — пробуем набор путей до первого успешного
    if _host_is("yahoo-finance15.p.rapidapi.com"):
        candidates: Tuple[Tuple[str, Optional[dict]], ...] = (
            (f"{base}/api/yahoo/options/{ticker}", None),
            (f"{base}/api/yahoo/options/{ticker}", {"date": str(expiry_ts)}) if expiry_ts else (f"{base}/api/yahoo/options/{ticker}", None),
            (f"{base}/api/yahoo/op/option/{ticker}", None),
            (f"{base}/api/yahoo/op/option-chain/{ticker}", {"date": str(expiry_ts)} if expiry_ts else None),
        )
        last_err = None
        for url, params in candidates:
            try:
                data = _http_get(url, params=params)
                return _as_standard_option_chain(data)
            except Exception as e:
                logger.warning(f"endpoint failed: {url} params={params} → {e}")
                last_err = e
                continue
        # если все варианты упали — бросаем последнюю ошибку
        raise last_err if last_err else ApiError("All endpoint candidates failed for yahoo-finance15")

    # 3) Любой другой RapidAPI-хост (на будущее)
    url = f"{base}/stock/v3/get-options"
    params = {"symbol": ticker}
    if expiry_ts:
        params["date"] = str(expiry_ts)
    data = _http_get(url, params=params)
    return _as_standard_option_chain(data)
