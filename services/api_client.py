import os
import time
import requests
from typing import Any, Dict

from logger import get_logger
from .utils.debug import dump_json, safe_redact

logger = get_logger("api")

RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST")
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")

HEADERS = {
    "x-rapidapi-host": RAPIDAPI_HOST or "",
    "x-rapidapi-key": RAPIDAPI_KEY or "",
}

BASE_URL = f"https://{RAPIDAPI_HOST}" if RAPIDAPI_HOST else None

class ApiError(RuntimeError):
    pass

def _check_env():
    missing = []
    if not RAPIDAPI_HOST: missing.append("RAPIDAPI_HOST")
    if not RAPIDAPI_KEY:  missing.append("RAPIDAPI_KEY")
    if missing:
        msg = f"Missing secrets: {', '.join(missing)}. Add them in Streamlit -> App settings -> Secrets."
        logger.error(msg)
        raise ApiError(msg)

def get_option_chain(ticker: str) -> Dict[str, Any]:
    _check_env()
    assert BASE_URL, "BASE_URL is not configured"
    url = f"{BASE_URL}/api/yahoo/options/{ticker}"
    t0 = time.perf_counter()
    try:
        logger.info(f"GET {url}")
        resp = requests.get(url, headers=HEADERS, timeout=30)
        dt = time.perf_counter() - t0
        logger.info(f"Response {resp.status_code} in {dt:.3f}s")
        resp.raise_for_status()
        data = resp.json()
        # Сохраняем «сырой» ответ без ключа
        safe_req = {"url": url, "headers": safe_redact(HEADERS)}
        dump_json(f"request_{ticker}", safe_req)
        dump_json(f"response_{ticker}", data)
        return data
    except Exception as e:
        dump_json("error_context", {"url": url, "headers": safe_redact(HEADERS), "error": str(e)})
        logger.exception(f"Failed to fetch option chain for {ticker}")
        raise
