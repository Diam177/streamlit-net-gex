# services/__init__.py
# Без сайд-эффектов. Экспортируем только то, что реально есть.
from .api_client import get_option_chain, ApiError

__all__ = ["get_option_chain", "ApiError"]
