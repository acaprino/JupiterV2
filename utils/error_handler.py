# utils/error_handler.py

from functools import wraps
from typing import Callable, Awaitable

from utils.logger import log_error


def exception_handler(func: Callable[..., Awaitable[None]]) -> Callable[..., Awaitable[None]]:
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            await func(*args, **kwargs)
        except Exception as e:
            log_error(f"Exception in {func.__name__}: {e}")
            # Puoi aggiungere altre azioni, come notifiche
    return wrapper
