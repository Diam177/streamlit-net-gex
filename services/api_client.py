# services/api_client.py
from __future__ import annotations

import os
import time
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

log = logging.getLogger(__name__)

DEFAULT_HOST = os.getenv("RAPIDAPI_HOST", "yahoo-finance15.p.rapidapi.com")
DEFAULT_KEY = os.getenv("RAPIDAPI_KEY", "")

# По реальным ответам провайдера (см. ваш raw compact):
# основной эндпоинт:
OPTIONS_PATH = "/api/v1/markets/options"  # возвращает quote, expirationDates и chains[calls/puts]


class RapidYahooClient:
    """
    Клиент RapidAPI (Yahoo Finance 15).
    Делает tolerant-парсинг под разные варианты ключей в ответах.
    """

    def __init__(self, host: Optional[str] = None, key: Optional[str] = None, api_key: Optional[str] = None) -> None:
        # совместимость: можно передать key=... или api_key=...
        self.host = (host or DEFAULT_HOST).strip()
        self.key = (key or api_key or DEFAULT_KEY).strip()

        if not self.host or not self.key:
            raise ValueError("RAPIDAPI_HOST / RAPIDAPI_KEY не заданы.")

        self.base_url = f"https://{self.host}".rstrip("/")
        self.sess = requests.Session()
        self.sess.headers.update({
            "x-rapidapi-host": self.host,
            "x-rapidapi-key": self.key,
        })
        self.timeout = 20

        log.info("RapidYahooClient inited host=%s", self.host)

    # ---------- публичные методы, которые вызывает streamlit_app ----------

    def get_expirations(self, symbol: str) -> List[int]:
        """
        Возвращает список expiration timestamps (UTC) для тикера.
        """
        data = self._fetch_options(symbol=symbol)
        expirations = (
            self._get_path(data, "expirationDates")
            or self._get_path(data, "meta.expirations")
            or self._get_path(data, "expirations")
            or []
        )

        # на всякий: если chains есть — заберём оттуда
        if not expirations:
            chains = self._extract_chains(data)
            expirations = sorted({int(c.get("expiration")) for c in chains if c.get("expiration")})

        expirations = [int(x) for x in expirations]
        log.debug("Expirations for %s: %s", symbol, expirations)
        return sorted(expirations)

    def pick_nearest_expiration(self, expirations: List[int], now_ts: Optional[int] = None) -> Optional[int]:
        """
        Берёт ближайшую будущую экспирацию; если все прошли — последнюю.
        """
        if not expirations:
            return None
        now_ts = now_ts or int(time.time())
        future = sorted([e for e in expirations if e >= now_ts])
        return (future[0] if future else sorted(expirations)[-1])

    def get_options_chain(self, symbol: str, expiration: Optional[int] = None) -> Dict[str, Any]:
        """
        Возвращает структуру с calls/puts + quote/meta.
        """
        data = self._fetch_options(symbol=symbol, expiration=expiration)

        chain = self._extract_chain_for_expiration(data, expiration)
        quote = self._get_path(data, "quote") or {}
        meta = {
            "endpoint": OPTIONS_PATH,
            "symbol": symbol,
            "expiration": expiration,
        }
        return {"quote": quote, "meta": meta, "chain": chain}

    def get_underlying_price(self, options_payload: Dict[str, Any]) -> Optional[float]:
        """
        Вынимает цену базового актива из payload с опционной цепочкой.
        """
        quote = options_payload.get("quote") or {}
        # Часто используется regularMarketPrice:
        for k in ("regularMarketPrice", "regularMarketPreviousClose", "price", "last", "close"):
            v = quote.get(k)
            if isinstance(v, (int, float)):
                return float(v)
        return None

    # ---------- внутреннее -----------

    def _fetch_options(self, symbol: str, expiration: Optional[int] = None) -> Dict[str, Any]:
        """
        Делает запрос к /api/v1/markets/options.
        Провайдер понимает и symbol, и ticker — передадим оба.
        """
        url = f"{self.base_url}{OPTIONS_PATH}"
        params = {"symbol": symbol, "ticker": symbol}
        if expiration:
            params.update({"expiration": expiration, "date": expiration})

        log.info("GET %s params=%s", url, params)
        r = self.sess.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        data = r.json()
        # Некоторые прокси возвращают data под ключом 'data'
        if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
            data = data["data"]

        log.debug("Provider raw keys: %s", list(data.keys()))
        return data

    def _extract_chains(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Возвращает список объектов цепей (каждый содержит expiration, calls, puts).
        Поддерживает разные варианты ключей.
        """
        # Нормальный случай: data["chains"] -> list[ {expiration, calls, puts}, ... ]
        chains = data.get("chains")
        if isinstance(chains, list):
            return chains

        # Иногда приходят просто fields "calls"/"puts" без вложенного списка:
        if "calls" in data or "puts" in data:
            exp = self._first(self._get_path(data, "expiration"), self._get_path(data, "meta.expiration"))
            return [{"expiration": exp, "calls": data.get("calls", []), "puts": data.get("puts", [])}]

        # Крайний случай — всё внутри data["options"] (или "optionChain") с похожей структурой
        for key in ("options", "optionChain"):
            node = data.get(key)
            if isinstance(node, list) and node and isinstance(node[0], dict):
                # ожидаем { 'expiration': ..., 'calls':[], 'puts':[] }
                if "calls" in node[0] or "puts" in node[0]:
                    return node

        return []

    def _extract_chain_for_expiration(self, data: Dict[str, Any], expiration: Optional[int]) -> Dict[str, Any]:
        chains = self._extract_chains(data)
        if not chains:
            return {"expiration": expiration, "calls": [], "puts": []}

        if expiration:
            for ch in chains:
                try:
                    if int(ch.get("expiration")) == int(expiration):
                        return {
                            "expiration": int(ch.get("expiration")),
                            "calls": ch.get("calls", []),
                            "puts": ch.get("puts", []),
                        }
                except Exception:
                    continue

        # иначе возьмём первую по списку
        ch = chains[0]
        return {
            "expiration": ch.get("expiration"),
            "calls": ch.get("calls", []),
            "puts": ch.get("puts", []),
        }

    @staticmethod
    def _get_path(obj: Dict[str, Any], dotted: str) -> Any:
        cur: Any = obj
        for p in dotted.split("."):
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                return None
        return cur

    @staticmethod
    def _first(*vals):
        for v in vals:
            if v is not None:
                return v
        return None
