# services/api_client.py
# -*- coding: utf-8 -*-
"""
Клиент RapidAPI Yahoo Finance 15.
Нужны секреты (Streamlit Cloud → Settings → Secrets):
  RAPIDAPI_HOST = "yahoo-finance15.p.rapidapi.com"
  RAPIDAPI_KEY  = "<ваш ключ>"
"""

from __future__ import annotations
import os
import time
import math
import typing as T
import requests

# ---- логгер: используем ваш utils.logger, если есть; иначе стандартный ----
try:
    from utils.logger import get_logger  # type: ignore
    log = get_logger(__name__)
except Exception:  # pragma: no cover
    import logging
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)


def _as_int(x, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, bool):
            return int(x)
        return int(float(x))
    except Exception:
        return default


def _as_float(x, default: float = 0.0) -> float:
    try:
        if x is None or (isinstance(x, str) and x.strip() == ""):
            return default
        return float(x)
    except Exception:
        return default


class RapidYahooClient:
    """
    Минималистичный клиент под наши задачи:
      - список дат экспирации
      - чейн опционов на выбранную дату
      - котировка базового актива (S)
    """

    def __init__(
        self,
        host: str | None = None,
        key: str | None = None,
        session: requests.Session | None = None,
        timeout: int = 30,
    ) -> None:
        self.host = (host or os.getenv("RAPIDAPI_HOST") or "").strip()
        self.key = (key or os.getenv("RAPIDAPI_KEY") or "").strip()
        if not self.host or not self.key:
            raise RuntimeError(
                "RAPIDAPI_HOST / RAPIDAPI_KEY не заданы (проверьте Secrets)."
            )

        # На RapidAPI «host» и «base_url» разделены:
        self.base_url = f"https://{self.host}"
        self.timeout = timeout
        self.s = session or requests.Session()
        self.headers = {
            "x-rapidapi-host": self.host,
            "x-rapidapi-key": self.key,
        }

    # ------------------------ низкоуровневый вызов -------------------------

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        log.debug("GET %s params=%s", url, params)
        r = self.s.get(url, headers=self.headers, params=params, timeout=self.timeout)
        try:
            r.raise_for_status()
        except Exception as e:
            # В лог выводим текст; вверх даём компактное исключение
            log.exception("HTTP error: %s %s", url, getattr(r, "text", "")[:500])
            raise RuntimeError(f"HTTP {r.status_code} for {path}") from e
        try:
            data = r.json()
        except Exception as e:
            log.exception("JSON decode error: %s", r.text[:500])
            raise RuntimeError("Провайдер вернул не-JSON") from e

        # RapidAPI иногда отдаёт 200 с сообщением об ошибке внутри JSON
        if isinstance(data, dict) and data.get("message"):
            msg = data.get("message")
            log.error("API message: %s", msg)
            raise RuntimeError(f"API message: {msg}")
        return data

    # --------------------------- публичные методы --------------------------

    def get_expirations(self, symbol: str) -> list[int]:
        """
        Список дат экспирации (Unix timestamp, сек) для тикера.
        """
        data = self._get(f"/api/yahoo/v7/finance/options/{symbol}")
        try:
            return list(
                map(
                    int,
                    data["optionChain"]["result"][0]["expirationDates"],
                )
            )
        except Exception:
            log.debug("expirations raw: %s", str(data)[:800])
            return []

    def get_option_chain_raw(self, symbol: str, date_ts: int) -> dict:
        """
        Сырой чейн опционов на дату date_ts.
        """
        return self._get(
            f"/api/yahoo/v7/finance/options/{symbol}",
            params={"date": int(date_ts)},
        )

    def get_quote_raw(self, symbol: str) -> dict:
        """
        Сырая котировка базового актива. Пробуем hiresquotes, при неудаче – /qu/quote.
        """
        try:
            return self._get(f"/api/yahoo/hiresquotes/{symbol}")
        except Exception:
            return self._get(f"/api/yahoo/qu/quote/{symbol}")

    # --------------------- функции нормализации под расчёты ----------------

    @staticmethod
    def _extract_price_from_chain(chain_raw: dict) -> tuple[float, int]:
        """
        Пытаемся достать цену S и метку времени t0 прямо из ответа по опционам.
        Возвращаем (S, t0). Если не получилось – (0.0, now).
        """
        now = int(time.time())
        try:
            res0 = chain_raw["optionChain"]["result"][0]
            q = res0.get("quote", {}) or {}
            # разные эндпоинты/символы по-разному кладут цену
            s_candidates = [
                q.get("regularMarketPrice"),
                q.get("regularMarketPreviousClose"),
                q.get("postMarketPrice"),
                q.get("preMarketPrice"),
            ]
            S = next((float(x) for x in s_candidates if x is not None), 0.0)
            t0 = _as_int(q.get("regularMarketTime"), now)
            return (S, t0 if t0 > 0 else now)
        except Exception:
            return (0.0, now)

    @staticmethod
    def _norm_option_list(lst: list[dict] | None, opt_type: str) -> list[dict]:
        out: list[dict] = []
        for x in lst or []:
            out.append(
                {
                    "type": opt_type,  # "call" | "put"
                    "contractSymbol": x.get("contractSymbol"),
                    "strike": _as_float(x.get("strike")),
                    "oi": _as_int(x.get("openInterest")),
                    "volume": _as_int(x.get("volume")),
                    # impliedVolatility у Yahoo хранится как доля (0.25 = 25%)
                    "iv": _as_float(x.get("impliedVolatility")),
                }
            )
        return out

    def get_chain_normalized(
        self, symbol: str, expiration_ts: int, need_quote_fallback: bool = True
    ) -> dict:
        """
        Возвращает нормализованный объект:
        {
          "symbol": str,
          "snapshot_ts": int,
          "expiration_ts": int,
          "price": float,     # S
          "calls": [ {strike, oi, volume, iv, ...}, ... ],
          "puts":  [ ... ]
        }
        """
        raw = self.get_option_chain_raw(symbol, expiration_ts)
        try:
            res0 = raw["optionChain"]["result"][0]
            options0 = res0["options"][0]
        except Exception as e:
            log.debug("chain raw head: %s", str(raw)[:800])
            raise RuntimeError("Не удалось распарсить цепочку опционов") from e

        S, t0 = self._extract_price_from_chain(raw)

        # Если из чейна цену не вытащили — доберём из котировки
        if S <= 0 and need_quote_fallback:
            try:
                qraw = self.get_quote_raw(symbol)
                # hiresquotes → {"body":[{"regularMarketPrice":...}]}
                if isinstance(qraw, dict) and "body" in qraw:
                    body = qraw.get("body") or []
                    if body:
                        S = _as_float(body[0].get("regularMarketPrice"), 0.0)
                        t0 = _as_int(body[0].get("regularMarketTime"), t0)
                else:
                    # /qu/quote/{symbol}
                    S = (
                        _as_float(qraw.get("regularMarketPrice"), 0.0)
                        or _as_float(qraw.get("price", {}).get("regularMarketPrice", {}).get("raw"), 0.0)
                    )
                    t0 = _as_int(qraw.get("regularMarketTime"), t0)
            except Exception:
                log.warning("Не удалось получить котировку для %s", symbol)

        calls = self._norm_option_list(options0.get("calls"), "call")
        puts = self._norm_option_list(options0.get("puts"), "put")

        return {
            "symbol": symbol.upper(),
            "snapshot_ts": int(t0),
            "expiration_ts": int(expiration_ts),
            "price": float(S),
            "calls": calls,
            "puts": puts,
        }

    # Удобный хелпер: ближайшая экспирация (если нужна в UI по умолчанию)
    def get_nearest_expiration(self, symbol: str) -> int | None:
        exps = self.get_expirations(symbol)
        now_ts = int(time.time())
        future = [e for e in exps if e >= now_ts - 3600]  # плюс небольшой допуск
        return min(future) if future else (min(exps) if exps else None)


# Алиас на случай, если где-то использовалось другое имя
RapidApiYahooClient = RapidYahooClient
