from functools import wraps
from typing import Callable, Awaitable, TypeVar, Optional

from utils.logger import log_error

R = TypeVar('R')


def exception_handler(func: Callable[..., Awaitable[R]]) -> Callable[..., Awaitable[Optional[R]]]:
    """
    Decorator to handle exceptions in asynchronous functions. If an exception occurs,
    it logs the error and returns None instead of raising the exception.

    Parameters:
    - func: The asynchronous function to wrap with error handling.

    Returns:
    - A wrapper function that executes `func` and logs any exceptions encountered.
    """

    @wraps(func)
    async def wrapper(*args, **kwargs) -> Optional[R]:
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            log_error(f"Exception in {func.__name__}: {e}")
            return None

    return wrapper
