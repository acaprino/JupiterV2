import asyncio
from concurrent.futures import ThreadPoolExecutor

from utils.error_handler import exception_handler
from utils.logger import Logger

# Executor for handling blocking broker calls
executor = ThreadPoolExecutor(max_workers=1)


@exception_handler
async def execute_broker_call(bot_name, func, *args, **kwargs):
    """
    Executes a blocking broker call asynchronously by running it in a separate thread.

    Parameters:
    - func: The blocking function to execute.
    - *args, **kwargs: Arguments to pass to the function.

    Returns:
    - The result of the function call, or None if an error occurs.
    """
    loop = asyncio.get_running_loop()
    try:
        # Execute the function in the thread pool and wait for the result
        return await loop.run_in_executor(executor, func, *args, **kwargs)
    except Exception as e:
        Logger.get_logger(bot_name).critical(f"Error in execute_broker_call: {e}")
        return None
