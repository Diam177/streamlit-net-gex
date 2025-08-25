import os
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp/streamlit_net_gex_logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "app.log"

def _build_handler() -> RotatingFileHandler:
    handler = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    return handler

def get_logger(name: str = "app", level: str | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    level_name = level or os.getenv("LOG_LEVEL", "INFO")
    logger.setLevel(level_name.upper())
    logger.addHandler(_build_handler())
    stream = logging.StreamHandler()
    stream.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logger.addHandler(stream)
    logger.propagate = False
    logger.info(f"Logger initialized → file={LOG_FILE}")
    return logger

def get_log_file_path() -> str:
    return str(LOG_FILE)

