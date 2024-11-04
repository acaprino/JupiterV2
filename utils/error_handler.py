# utils/error_handler.py

import logging
import asyncio
from functools import wraps

def exception_handler(coroutine_func):
    """
    Decoratore per gestire le eccezioni nelle coroutine.
    """

    @wraps(coroutine_func)
    async def wrapper(*args, **kwargs):
        try:
            return await coroutine_func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Errore in {coroutine_func.__name__}: {e}", exc_info=True)
    return wrapper
