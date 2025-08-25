import os
import time
import requests
from typing import Any, Dict, Optional

from logger import get_logger
from services.utils.debug import dump_json, safe_redact

logger = get_logger("api")

RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST")
RAPIDAPI_KEY  = os.getenv("RAPIDAPI_KEY")

HEADERS = {
    "x-rapidapi-host": RAPIDAPI_HOST or "",
    "x-rapidapi-key":  RAPIDAPI_KEY  or "",
}
BASE_URL = f"https://{RAPIDAPI_HOST}" if RAPIDAPI_HOST else None

class ApiError(RuntimeError):
    pass

def _check_env():
    missing = []
    if not RAPIDAPI_HOST: missing.append("RAPIDAPI_HOST")
    if not RAPIDAPI_KEY:  missing.append("RAPIDAPI_KEY")
    if missing:
        raise ApiError(f"Missing secrets: {', '.join(missing)} (App settings → Secrets)")

def get_option_chain(ticker: str, expiry_ts: Optional[int] = None) -> Dict[str, Any]:
    """
    Если expiry_ts задан — запрашиваем конкретную экспирацию (?date=timestamp).
    Иначе — общий снимок (вернёт список expirationDates и блок ближайшей options).
    """
    _check_env()
    assert BASE_URL, "BASE_URL is not configured"

    url = f"{BASE_URL}/api/yahoo/options/{ticker}"
    params = {"date": str(expiry_ts)} if expiry_ts else None

    t0 = time.perf_counter()
    try:
        logger.info(f"GET {url} params={params}")
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        dt = time.perf_counter() - t0
        logger.info(f"Response {resp.status_code} in {dt:.3f}s")
        resp.raise_for_status()
        data = resp.json()

        dump_json(f"request_{ticker}", {"url": url, "params": params, "headers": safe_redact(HEADERS)})
        dump_json(f"response_{ticker}", data)
        return data
    except Exception as e:
        dump_json("error_context", {"url": url, "params": params, "headers": safe_redact(HEADERS), "error": str(e)})
        logger.exception("get_option_chain failed")
        raise
