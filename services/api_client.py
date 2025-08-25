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
        dump_json("http_error", {
            "url": url, "params": params, "headers": safe_redact(COMMON_HEADERS),
            "status": resp.status_code, "text": resp.text[:800]
        })
        resp.raise_for_status()
    data = resp.json()
    dump_json("http_ok", {"url": url, "params": params, "headers": safe_redact(COMMON_HEADERS)})
    dump_json("http_response", data)
    return data

def _as_standard_option_chain(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Нормализует разные ответы к формату:
    {
      "optionChain": {
        "result": [ { "quote": {...}, "expirationDates": [...], "options": [ {...} ] } ],
        "error": null
      }
    }
    """
    # Уже стандартный формат Yahoo
    if isinstance(raw, dict) and "optionChain" in raw and "result" in raw["optionChain"]:
        return raw

    # Формат SteadyAPI: {"meta":..., "body":[{ underlyngSymbol, expirationDates, quote, options: [...] }]}
    if isinstance(raw, dict) and "body" in raw and isinstance(raw["body"], list) and raw["body"]:
        item = raw["body"][0]
        quote = item.get("quote", {}) or {}
        expirations = item.get("expirationDates", []) or []
        options = item.get("options", []) or []

        # Если пришёл straddle-режим — делаем пустые calls/puts, позже подкачаем по конкретной дате с display=list
        norm_options = []
        for opt in options:
            if ("calls" in opt) or ("puts" in opt):
                norm_options.append(opt)
            else:
                norm_options.append({
                    "expirationDate": opt.get("expirationDate"),
                    "hasMiniOptions": opt.get("hasMiniOptions", False),
                    "calls": opt.get("calls", []),
                    "puts":  opt.get("puts",  [])
                })

        return {
            "optionChain": {
                "result": [{
                    "quote": quote,
                    "expirationDates": expirations,
                    "options": norm_options
                }],
                "error": None
            }
        }

    # Ничего не знаем — возвращаем как есть (пусть упадёт наверху с понятной ошибкой и дампом)
    return raw

def _host_is(name: str) -> bool:
    return RAPIDAPI_HOST.lower() == name.lower()

def get_option_chain(ticker: str, expiry_ts: Optional[int] = None) -> Dict[str, Any]:
    """
    Универсальный клиент:
      • yh-finance.p.rapidapi.com → /stock/v3/get-options  или /v1/options
      • yahoo-finance15.p.rapidapi.com → /api/yahoo/...  и также пробуем /v1/options (некоторые маршрутизируют)
    Возвращает нормализованный JSON (см. _as_standard_option_chain).
    """
    _require_secrets()
    base = f"https://{RAPIDAPI_HOST}"

    candidates: Tuple[Tuple[str, Optional[dict]], ...] = ()

    # Apidojo (yh-finance) — классический путь
    if _host_is("yh-finance.p.rapidapi.com"):
        p = {"symbol": ticker}
        if expiry_ts: p["date"] = str(expiry_ts)
        candidates += ((f"{base}/stock/v3/get-options", p),)

        # Их же современный v1/options
        p2 = {"ticker": ticker, "display": "list"}
        if expiry_ts: p2["expiration"] = str(expiry_ts)
        candidates += ((f"{base}/v1/options", p2),)

    # Sparior (yahoo-finance15) — их набор путей + v1/options на всякий случай
    if _host_is("yahoo-finance15.p.rapidapi.com"):
        # их собственные подкапоты
        candidates += (
            (f"{base}/api/yahoo/options/{ticker}", None),
            (f"{base}/api/yahoo/options/{ticker}", {"date": str(expiry_ts)}) if expiry_ts else (f"{base}/api/yahoo/options/{ticker}", None),
            (f"{base}/api/yahoo/op/option/{ticker}", None),
            (f"{base}/api/yahoo/op/option-chain/{ticker}", {"date": str(expiry_ts)} if expiry_ts else None),
        )
        # и вариант v1/options в стиле SteadyAPI
        p3 = {"ticker": ticker, "display": "list"}
        if expiry_ts: p3["expiration"] = str(expiry_ts)
        candidates += ((f"{base}/v1/options", p3),)

    # Фолбэк для любых хостов — попробуем оба популярных
    if not candidates:
        p = {"symbol": ticker}
        if expiry_ts: p["date"] = str(expiry_ts)
        candidates += ((f"{base}/stock/v3/get-options", p),)
        p2 = {"ticker": ticker, "display": "list"}
        if expiry_ts: p2["expiration"] = str(expiry_ts)
        candidates += ((f"{base}/v1/options", p2),)

    last_err: Optional[Exception] = None
    for url, params in candidates:
        try:
            data = _http_get(url, params=params)
            return _as_standard_option_chain(data)
        except Exception as e:
            logger.warning(f"endpoint failed: {url} params={params} → {e}")
            last_err = e
            continue

    raise last_err if last_err else ApiError("All endpoint candidates failed")
