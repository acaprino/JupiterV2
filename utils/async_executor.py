# utils/async_executor.py

import asyncio
from concurrent.futures import ThreadPoolExecutor

from utils.error_handler import exception_handler

# Esecutore per le chiamate bloccanti del broker
executor = ThreadPoolExecutor(max_workers=5)


@exception_handler
async def execute_broker_call(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    try:
        # Run the function in the executor with provided args and kwargs
        return await loop.run_in_executor(executor, func, *args, **kwargs)
    except Exception as e:
        print(f"Error in execute_broker_call: {e}")
        return None
