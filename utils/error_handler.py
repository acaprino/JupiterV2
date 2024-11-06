# utils/error_handler.py

from functools import wraps
from typing import Callable, Awaitable, TypeVar, Optional

from utils.logger import log_error

R = TypeVar('R')

def exception_handler(func: Callable[..., Awaitable[R]]) -> Callable[..., Awaitable[R]]:
    @wraps(func)
    async def wrapper(*args, **kwargs) -> Optional[R]:
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            log_error(f"Exception in {func.__name__}: {e}")
            return None
    return wrapper