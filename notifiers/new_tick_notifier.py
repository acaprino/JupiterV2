# providers/tick_notifier.py

import asyncio
import logging
from datetime import datetime
from typing import Callable, Awaitable, List

from utils.enums import Timeframe
from utils.error_handler import exception_handler
from utils.logger import log_info
from utils.utils_functions import dt_to_unix, now_utc, unix_to_datetime


class TickNotifier:
    """
    Provides new candles at regular intervals and triggers registered callbacks.
    """

    def __init__(self, timeframe: Timeframe, execution_lock: asyncio.Lock = None):
        self.timeframe = timeframe
        self._running = False
        self._task = None
        self._on_new_tick_callbacks: List[Callable[[Timeframe, datetime], Awaitable[None]]] = []
        self.execution_lock = execution_lock

    async def start(self):
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run())
            log_info(f"Tick notifier started for timeframe {self.timeframe}.")

    async def stop(self):
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            log_info(f"Tick notifier stopped for timeframe {self.timeframe}.")

    def register_on_new_tick(self, callback: Callable[[Timeframe, datetime], Awaitable[None]]):
        if not callable(callback):
            raise ValueError("Callback must be callable")
        self._on_new_tick_callbacks.append(callback)

    @exception_handler
    async def _run(self):
        while self._running:
            try:
                timestamp = await self.wait_next_tick()
                log_info(f"New tick for timeframe {self.timeframe} at {timestamp}.")
                tasks = [callback(self.timeframe, timestamp) for callback in self._on_new_tick_callbacks]
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                logging.error(f"Error in TickNotifier._run: {e}", exc_info=True)

    async def wait_next_tick(self) -> datetime:
        timeframe_duration = self.timeframe.to_seconds()
        now = now_utc()
        current_time = dt_to_unix(now)
        time_to_next_candle = timeframe_duration - (current_time % timeframe_duration)
        next_candle_time = unix_to_datetime(current_time + time_to_next_candle).replace(microsecond=0)
        log_info(f"Waiting for the next candle: current time: {now}, next candle at: {next_candle_time}")
        await asyncio.sleep(time_to_next_candle)
        log_info(f"New candle started, current time: {now_utc()}")
        return next_candle_time
