import asyncio
import logging
from datetime import datetime
from typing import Callable, Awaitable, List

from utils.enums import Timeframe
from utils.error_handler import exception_handler
from utils.logger import log_info, log_debug, log_error
from utils.utils_functions import dt_to_unix, now_utc, unix_to_datetime


class TickNotifier:
    """
    Notifies registered callbacks at the start of each new tick based on the specified timeframe.
    """

    def __init__(self, timeframe: Timeframe, execution_lock: asyncio.Lock = None):
        self.timeframe = timeframe
        self.execution_lock = execution_lock

        self._running = False
        self._task = None
        self._on_new_tick_callbacks: List[Callable[[Timeframe, datetime], Awaitable[None]]] = []

    async def start(self):
        """Starts the tick notifier loop."""
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run())
            log_info(f"TickNotifier started for timeframe {self.timeframe}.")

    async def stop(self):
        """Stops the tick notifier loop."""
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            log_info(f"TickNotifier stopped for timeframe {self.timeframe}.")

    def register_on_new_tick(self, callback: Callable[[Timeframe, datetime], Awaitable[None]]):
        """Registers a callback to be notified at the start of each new tick."""
        if not callable(callback):
            raise ValueError("Callback must be callable")
        self._on_new_tick_callbacks.append(callback)
        log_info("Callback registered for new tick notifications.")

    @exception_handler
    async def _run(self):
        """Main loop to trigger registered callbacks at each new tick interval."""
        while self._running:
            try:
                timestamp = await self.wait_next_tick()
                log_info(f"New tick triggered for {self.timeframe} at {timestamp}.")

                # Trigger all registered callbacks
                tasks = [callback(self.timeframe, timestamp) for callback in self._on_new_tick_callbacks]
                await asyncio.gather(*tasks, return_exceptions=True)

            except Exception as e:
                log_error(f"Error in TickNotifier._run: {e}")

    async def wait_next_tick(self) -> datetime:
        """Calculates the time to wait until the next tick and waits for it."""
        timeframe_duration = self.timeframe.to_seconds()
        now = now_utc()
        current_time = dt_to_unix(now)

        # Calculate time until the next tick
        time_to_next_candle = timeframe_duration - (current_time % timeframe_duration)
        next_candle_time = unix_to_datetime(current_time + time_to_next_candle).replace(microsecond=0)

        log_debug(f"Waiting for next candle: current time {now}, duration until next tick: {time_to_next_candle} seconds.")
        log_info(f"Next tick expected at {next_candle_time}")

        await asyncio.sleep(time_to_next_candle)

        log_debug(f"Wait for next tick completed. Current time: {now_utc()}")
        return next_candle_time
