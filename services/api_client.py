# services/api_client.py
from __future__ import annotations

import os
import time
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

DEFAULT_BASE = "https://{host}/api/v1/markets/options"
ENV_HOST = "RAPIDAPI_HOST"
ENV_KEY = "RAPIDAPI_KEY"


class RapidYahooClient:
    """
    Клиент для yahoo-finance15.p.rapidapi.com (Yahoo Finance 15 на RapidAPI).

    ВАЖНО: у этого провайдера узел с цепочкой опционов лежит под ключом-строкой
    'chains[0]'. Поэтому обращаться нужно именно так: payload["chains[0]"].
    """

    def __init__(
        self,
        host: Optional[str] = None,
        api_key: Optional[str] = None,
        session: Optional[requests.Session] = None,
        timeout: float = 25.0,
    ) -> None:
        self.host = (host or os.getenv(ENV_HOST) or "").strip()
        self.api_key = (api_key or os.getenv(ENV_KEY) or "").strip()
        if not self.host or not self.api_key:
            raise RuntimeError(
                f"RapidYahooClient: отсутствуют секреты {ENV_HOST} / {ENV_KEY}"
            )

        self.base_url = DEFAULT_BASE.format(host=self.host)
        self.session = session or requests.Session()
        self.timeout = timeout

    # ---------- HTTP ----------

    def _headers(self) -> Dict[str, str]:
        return {
            "x-rapidapi-host": self.host,
            "x-rapidapi-key": self.api_key,
            "accept": "application/json",
        }

    def _get(self, params: Dict[str, Any]) -> Dict[str, Any]:
        LOG.info("GET %s params=%s", self.base_url, params)
        r = self.session.get(self.base_url, params=params, headers=self._headers(), timeout=self.timeout)
        if r.status_code != 200:
            # Логируем укороченный ответ для дебага
            txt = r.text
            LOG.error("RapidAPI error %s: %s", r.status_code, txt[:1000])
            raise RuntimeError(f"RapidAPI {r.status_code}: {txt[:200]}")
        data = r.json()
        LOG.info("OK: keys=%s", list(data.keys()))
        return data

    # ---------- Публичные методы, которые вызывает твой streamlit_app ----------

    def get_expiration_dates(self, ticker: str) -> List[int]:
        """
        Возвращает список доступных дат экспирации (Unix seconds, UTC).
        Провайдер отдаёт их в поле 'expirationDates'.
        """
        payload = self._get({"ticker": ticker})
        exps = payload.get("expirationDates") or []
        if not isinstance(exps, list):
            LOG.warning("Неожиданный формат expirationDates: %r", exps)
            return []
        return [int(x) for x in exps]

    def pick_nearest_expiration(self, expirations: List[int], t0: Optional[int] = None) -> Optional[int]:
        """
        Выбрать ближайшую дату экспирации >= t0 (по умолчанию — сейчас).
        """
        if not expirations:
            return None
        now = int(t0 if t0 is not None else time.time())
        future = [e for e in expirations if int(e) >= now]
        return int(min(future)) if future else int(max(expirations))

    def get_options_chain(self, ticker: str, expiration: Optional[int] = None) -> Dict[str, Any]:
        """
        Возвращает словарь:
        {
          "expiration": <unix>,
          "calls": [...],
          "puts": [...],
          "expirations": [...],           # список всех дат
          "quote": {...},                 # блок с ценой базового актива и временем снимка
          "raw": <исходный payload>       # для полного дебага
        }
        """
        params: Dict[str, Any] = {"ticker": ticker}
        if expiration is not None:
            params["expiration"] = int(expiration)

        payload = self._get(params)

        # Цена базового актива и время снимка
        quote = payload.get("quote") or {}
        # Список всех доступных экспираций
        expirations = payload.get("expirationDates") or []

        # ВАЖНО: у провайдера ключ именно 'chains[0]'
        chain_node = payload.get("chains[0]") or {}
        # Если разработчик случайно будет ожидать массив 'chains', попробуем graceful-fallback
        if not chain_node and isinstance(payload.get("chains"), list) and payload["chains"]:
            chain_node = payload["chains"][0]

        calls = chain_node.get("calls") or []
        puts = chain_node.get("puts") or []
        exp = chain_node.get("expiration") or payload.get("expiration")

        return {
            "expiration": int(exp) if exp is not None else None,
            "calls": calls,
            "puts": puts,
            "expirations": expirations,
            "quote": quote,
            "raw": payload,
        }

    # ---------- Утилиты (можешь вызывать из streamlit_app, если удобно) ----------

    @staticmethod
    def quote_price_and_time(quote: Dict[str, Any]) -> Tuple[Optional[float], Optional[int]]:
        """
        Достаёт S и t0 из блока quote.
        """
        s = quote.get("regularMarketPrice")
        t0 = quote.get("regularMarketTime")
        try:
            s = float(s) if s is not None else None
        except Exception:
            s = None
        try:
            t0 = int(t0) if t0 is not None else None
        except Exception:
            t0 = None
        return s, t0

    @staticmethod
    def aggregate_by_strike(
        calls: List[Dict[str, Any]],
        puts: List[Dict[str, Any]]
    ) -> Dict[float, Dict[str, Any]]:
        """
        Агрегирует по страйку:
          strike -> {call_oi, put_oi, call_vol, put_vol, iv_mid}
        Отсутствующие volume у провайдера считаем 0.
        IV усредняем между call/put, если есть обе.
        """
        acc: Dict[float, Dict[str, Any]] = {}

        def upd(strike: float, kind: str, item: Dict[str, Any]) -> None:
            node = acc.setdefault(
                strike,
                {"call_oi": 0, "put_oi": 0, "call_vol": 0, "put_vol": 0, "iv_mid": None, "iv_c": None, "iv_p": None},
            )
            if kind == "call":
                node["call_oi"] += int(item.get("openInterest") or 0)
                node["call_vol"] += int(item.get("volume") or 0)
                iv = item.get("impliedVolatility")
                if iv is not None:
                    node["iv_c"] = float(iv)
            else:
                node["put_oi"] += int(item.get("openInterest") or 0)
                node["put_vol"] += int(item.get("volume") or 0)
                iv = item.get("impliedVolatility")
                if iv is not None:
                    node["iv_p"] = float(iv)

        for it in calls or []:
            try:
                strike = float(it.get("strike"))
            except Exception:
                continue
            upd(strike, "call", it)

        for it in puts or []:
            try:
                strike = float(it.get("strike"))
            except Exception:
                continue
            upd(strike, "put", it)

        # посчитаем iv_mid
        for strike, node in acc.items():
            iv_c = node.pop("iv_c", None)
            iv_p = node.pop("iv_p", None)
            if iv_c is not None and iv_p is not None:
                node["iv_mid"] = (iv_c + iv_p) / 2.0
            elif iv_c is not None:
                node["iv_mid"] = iv_c
            elif iv_p is not None:
                node["iv_mid"] = iv_p
            else:
                node["iv_mid"] = None

        return acc
