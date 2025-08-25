import json
import os
import time
from pathlib import Path
from typing import Any, Iterable

from logger import get_logger

logger = get_logger("debug")

DEBUG_DIR = Path(os.getenv("DEBUG_DIR", "/tmp/streamlit_net_gex_debug"))
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

def now_slug() -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.gmtime())

def safe_redact(obj: Any, keys_to_hide: Iterable[str] = ("x-rapidapi-key","RAPIDAPI_KEY")) -> Any:
    if isinstance(obj, dict):
        return {k: ("***" if k in keys_to_hide else safe_redact(v, keys_to_hide)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [safe_redact(v, keys_to_hide) for v in obj]
    return obj

def dump_json(name: str, data: Any) -> str:
    path = DEBUG_DIR / f"{now_slug()}_{name}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved debug JSON: {path}")
    return str(path)

def dump_text(name: str, text: str) -> str:
    path = DEBUG_DIR / f"{now_slug()}_{name}.log"
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    logger.info(f"Saved debug text: {path}")
    return str(path)

def list_debug_files(limit: int = 20) -> list[str]:
    files = sorted([str(p) for p in DEBUG_DIR.glob("*")], reverse=True)
    return files[:limit]

