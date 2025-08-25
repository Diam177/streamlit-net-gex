# services/api_client.py
import os
import requests
from typing import Tuple, List, Dict, Any

RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "yahoo-finance15.p.rapidapi.com")
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")

BASE_URL = f"https://{RAPIDAPI_HOST}"


def _headers() -> Dict[str, str]:
    return {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
    }


def _get(url: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    resp = requests.get(url, headers=_headers(), params=params or {}, timeout=30)
    # Дадим больше дебага в логах Streamlit
    try:
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(
            f"HTTP error {resp.status_code} for {url} params={params}; body[:500]={resp.text[:500]}"
        ) from e
    try:
        return resp.json()
    except Exception as e:
        raise RuntimeError(f"JSON parse error for {url}: {resp.text[:500]}") from e


def get_option_overview(symbol: str, exp_ts: int | None = None) -> Dict[str, Any]:
    """
    Универсальный вызов:
    - без date -> получить список экспираций + первую цепочку
    - с date   -> получить конкретную цепочку на дату
    """
    url = f"{BASE_URL}/api/yahoo/option/{symbol.upper()}"
    params = {}
    if exp_ts:
        params["date"] = str(int(exp_ts))
    return _get(url, params=params)


def _extract_expirations(payload: Dict[str, Any]) -> List[int]:
    """
    Устойчиво выдёргиваем список экспираций из разных возможных схем:
    1) { meta: { expirationDates: [...] }, options: [...] }
    2) { optionChain: { result: [ { expirationDates: [...], options: [...] } ] } }
    3) { expirationDates: [...] }
    """
    exps: List[int] = []

    if not isinstance(payload, dict):
        return exps

    # Вариант 1
    meta = payload.get("meta")
    if isinstance(meta, dict) and "expirationDates" in meta:
        exps = meta.get("expirationDates") or []

    # Вариант 2
    if not exps and "optionChain" in payload:
        oc = payload.get("optionChain") or {}
        res = oc.get("result") or []
        if isinstance(res, list) and res:
            r0 = res[0] or {}
            if "expirationDates" in r0:
                exps = r0.get("expirationDates") or []
            if not exps and "options" in r0:
                opts = r0.get("options") or []
                # соберём expirationDate из options-массивов
                pool = []
                for o in opts:
                    ed = o.get("expirationDate")
                    if isinstance(ed, int):
                        pool.append(ed)
                exps = pool

    # Вариант 3
    if not exps and "expirationDates" in payload:
        exps = payload.get("expirationDates") or []

    # Нормализуем
    out: List[int] = []
    for x in exps:
        try:
            out.append(int(x))
        except Exception:
            continue
    return sorted(set(out))


def _extract_calls_puts(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Возвращаем (calls, puts) из разных схем:
    1) { options: [ { calls: [...], puts: [...] } ] }
    2) { optionChain: { result: [ { options: [ { calls: [...], puts: [...] } ] } ] } }
    """
    # Схема 1
    if "options" in payload:
        opts = payload.get("options") or []
        if isinstance(opts, list) and opts:
            first = opts[0] or {}
            return first.get("calls", []) or [], first.get("puts", []) or []

    # Схема 2
    if "optionChain" in payload:
        oc = payload.get("optionChain") or {}
        res = oc.get("result") or []
        if isinstance(res, list) and res:
            r0 = res[0] or {}
            opts = r0.get("options") or []
            if isinstance(opts, list) and opts:
                first = opts[0] or {}
                return first.get("calls", []) or [], first.get("puts", []) or []

    return [], []


def get_expiration_dates(symbol: str) -> Tuple[List[int], Dict[str, Any]]:
    data = get_option_overview(symbol)
    exps = _extract_expirations(data)
    return exps, data


def get_option_chain(symbol: str, exp_ts: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    data = get_option_overview(symbol, exp_ts=exp_ts)
    calls, puts = _extract_calls_puts(data)
    return calls, puts, data
