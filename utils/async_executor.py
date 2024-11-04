# utils/async_executor.py

import asyncio
from concurrent.futures import ThreadPoolExecutor

# Esecutore per le chiamate bloccanti del broker
executor = ThreadPoolExecutor(max_workers=1)

async def execute_broker_call(func, *args, **kwargs):
    """
    Esegue una funzione del broker in modo asincrono utilizzando un ThreadPoolExecutor.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, func, *args, **kwargs)
