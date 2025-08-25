# services/api_client.py
from __future__ import annotations

import os
import time
import logging
from typing import Any, Dict, Optional

import requests

log = logging.getLogger(__name__)


class RapidYahooClient:
    """
    Мини-клиент для Yahoo Finance 15 (RapidAPI).
    Использует секреты/переменные окружения:
      RAPIDAPI_HOST, RAPIDAPI_KEY
    """

    def __init__(
        self,
        host: Optional[str] = None,
        key: Optional[str] = None,
        timeout: int = 20,
    ) -> None:
        # host/ключ читаем из env или (в Streamlit Cloud) из st.secrets
        host_env = host or os.getenv("RAPIDAPI_HOST")
        key_env = key or os.getenv("RAPIDAPI_KEY")

        # на всякий случай уберём лишние кавычки, если их ввели в Secrets
        if host_env and host_env.startswith('"') and host_env.endswith('"'):
            host_env = host_env[1:-1]
        if key_env and key_env.startswith('"') and key_env.endswith('"'):
            key_env = key_env[1:-1]

        if not host_env or not key_env:
            raise RuntimeError(
                "RAPIDAPI_HOST / RAPIDAPI_KEY не заданы. "
                "Добавьте их в Secrets приложения."
            )

        self.base_url = f"https://{host_env.strip()}"
        self.headers = {
            "x-rapidapi-host": host_env.strip(),
            "x-rapidapi-key": key_env.strip(),
        }
        self.timeout = timeout

        log.debug("RapidYahooClient init: base_url=%s", self.base_url)

    # ---------------------------- internal helpers ----------------------------

    def _req(self, method: str, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        t0 = time.time()
        r = requests.request(method, url, headers=self.headers, params=params, timeout=self.timeout)
        try:
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            # логируем тело ответа для дебага
            log.exception("HTTP error on %s %s params=%s status=%s text=%s",
                          method, url, params, getattr(r, "status_code", None), getattr(r, "text", None))
            raise

        log.debug("HTTP %s %s %s -> %ss", method, url, params or "", round(time.time() - t0, 3))
        return data

    def _try_paths(self, paths: list[str], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Перебирает возможные пути API (у провайдера встречаются разные варианты).
        Возвращает первый успешный JSON.
        """
        last_exc = None
        for p in paths:
            url = self.base_url + p
            try:
                return self._req("GET", url, params)
            except Exception as e:
                last_exc = e
                log.debug("Path failed: %s (%s)", p, e)
                continue
        if last_exc:
            raise last_exc
        raise RuntimeError("No API path succeeded")

    # ------------------------------ public API --------------------------------

    def get_options_chain(self, symbol: str) -> Dict[str, Any]:
        """
        Возвращает структуру optionChain для тикера:
          - либо целиком raw payload провайдера,
          - либо dict вида result[0] (как в Yahoo).
        Этой структуры достаточно, чтобы вытащить expirationDates.
        """
        # На RapidAPI встречаются разные пути к одной и той же ручке
        paths = [
            f"/api/yahoo/op/option/{symbol}",
            f"/api/yahoo/options/{symbol}",
            f"/api/yahoo/finance/options/{symbol}",
        ]
        payload = self._try_paths(paths)

        # normalize к формату Yahoo: optionChain.result[0]
        if "optionChain" in payload and "result" in payload["optionChain"]:
            # уже «классический» формат
            return payload["optionChain"]["result"][0]
        return payload  # пусть верхний слой сам разберётся

    def get_options_for_expiry(self, symbol: str, expiration_unix: int) -> Dict[str, Any]:
        """
        Возвращает optionChain для конкретной даты экспирации (unix timestamp).
        """
        params = {"date": str(int(expiration_unix))}
        paths = [
            f"/api/yahoo/op/option/{symbol}",
            f"/api/yahoo/options/{symbol}",
            f"/api/yahoo/finance/options/{symbol}",
        ]
        payload = self._try_paths(paths, params=params)
        if "optionChain" in payload and "result" in payload["optionChain"]:
            return payload["optionChain"]["result"][0]
        return payload

    def get_quote(self, symbol: str) -> Dict[str, Any]:
        """
        Возвращает «квоту» по тикеру, где можно прочитать текущую цену S.
        Возвращаем словарь с полем 'price' (float) и raw.
        """
        # несколько вариантов путей для quote
        q_paths = [
            f"/api/yahoo/qu/quote/{symbol}",
            f"/api/yahoo/qs/quote",              # ?symbols=SPY
            f"/api/yahoo/market/quotes/{symbol}",
        ]

        # сначала варианты без params
        for p in [q_paths[0], q_paths[2]]:
            try:
                raw = self._try_paths([p])
                price = _extract_price_from_quote(raw)
                return {"price": price, "raw": raw}
            except Exception:
                pass

        # затем вариант с параметром symbols
        try:
            raw = self._try_paths([q_paths[1]], params={"symbols": symbol})
            price = _extract_price_from_quote(raw)
            return {"price": price, "raw": raw}
        except Exception:
            # отдаём хоть что-то для дебага
            raise


# ----------------------------- helpers (module) ------------------------------

def _extract_price_from_quote(raw: Dict[str, Any]) -> float:
    """
    Пытаемся достать цену из разных возможных форматов Yahoo.
    """
    # вариант quoteResponse.result[0].regularMarketPrice
    try:
        return float(raw["quoteResponse"]["result"][0]["regularMarketPrice"])
    except Exception:
        pass

    # вариант price.regularMarketPrice.raw
    try:
        return float(raw["price"]["regularMarketPrice"]["raw"])
    except Exception:
        pass

    # вариант просто regularMarketPrice в корне
    try:
        return float(raw["regularMarketPrice"])
    except Exception as e:
        log.exception("Не удалось извлечь цену из quote payload: %s", e)
        raise
