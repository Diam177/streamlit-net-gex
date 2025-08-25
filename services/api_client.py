import os
import requests
from typing import Any, Dict, Optional

class RapidYahooClient:
    def __init__(self, host: Optional[str] = None, key: Optional[str] = None, timeout: int = 20):
        self.host = host or os.environ.get("RAPIDAPI_HOST") or "yahoo-finance15.p.rapidapi.com"
        self.key  = key  or os.environ.get("RAPIDAPI_KEY")
        if not self.key:
            raise RuntimeError("RAPIDAPI_KEY is not set. Provide it via Streamlit Secrets or env var.")
        self.base_url = f"https://{self.host}/api/v1/markets/options"
        self.timeout = timeout

    def _headers(self) -> Dict[str, str]:
        return {
            "x-rapidapi-host": self.host,
            "x-rapidapi-key": self.key,
        }

    def get_options_chain(self, ticker: str, expiration: Optional[int] = None) -> Dict[str, Any]:
        params = {"ticker": ticker}
        if expiration is not None:
            params["expiration"] = int(expiration)
        r = requests.get(self.base_url, headers=self._headers(), params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def pick_nearest_expiration(payload: Dict[str, Any]) -> Optional[int]:
        xs = payload.get("expirationDates") or []
        if not xs:
            node = payload.get("chains[0]")
            if isinstance(node, dict) and "expiration" in node:
                return int(node["expiration"])
            return None
        try:
            xs_int = sorted(int(x) for x in xs)
        except Exception:
            return None
        return xs_int[0]
