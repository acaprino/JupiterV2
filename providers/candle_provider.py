# providers/candle_provider.py

import asyncio
import logging
from datetime import datetime
from typing import Callable, Awaitable, List

from time import sleep, time
from brokers.broker_interface import BrokerAPI
from utils.async_executor import execute_broker_call
from utils.enums import Timeframe
from utils.error_handler import exception_handler
from utils.logger import log_info


class CandleProvider:
    """
    Provides new candles at regular intervals and triggers registered callbacks.
    """

    def __init__(self, broker: BrokerAPI, symbol: str, timeframe: Timeframe, execution_lock: asyncio.Lock = None):
        self.broker = broker
        self.symbol = symbol
        self.timeframe = timeframe  # Timeframe in minutes
        self._running = False
        self._task = None
        self._on_new_candle_callbacks: List[Callable[[dict], Awaitable[None]]] = []
        self.execution_lock = execution_lock  # Lock to synchronize executions

    async def start(self):
        """
        Starts the candle provider.
        """
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run())
            log_info(f"Candle provider for {self.symbol} started.")

    async def stop(self):
        """
        Stops the candle provider.
        """
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            log_info(f"Candle provider for {self.symbol} stopped.")

    def register_on_new_candle(self, callback: Callable[[dict], Awaitable[None]]):
        """
        Registers a callback function to be called when a new candle is available.
        """
        if not callable(callback):
            raise ValueError("Callback must be callable")
        self._on_new_candle_callbacks.append(callback)

    @exception_handler
    async def _run(self):
        while self._running:
            try:
                await self.wait_next_tick()
                candle = await self._fetch_candle()
                log_info(f"New candle for {self.symbol}: {candle}")
                # Trigger callbacks in a synchronized manner
                tasks = [callback(candle) for callback in self._on_new_candle_callbacks]
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                logging.error(f"Error in CandleProvider._run: {e}", exc_info=True)
             # Wait 60 seconds before fetching the next candle

    async def _fetch_candle(self) -> dict:
        candles = await execute_broker_call(self.broker.get_last_candles, self.symbol, self.timeframe, 1)
        if candles:
            return candles[0]
        else:
            raise Exception("Error fetching the candle")

    async def wait_next_tick(self):
        timeframe_duration = self.timeframe.to_seconds()
        current_time = time()
        time_to_next_candle = timeframe_duration - (current_time % timeframe_duration)
        next_candle_time = datetime.fromtimestamp(current_time + time_to_next_candle)
        log_info(f"Waiting for the next candle. Current time: {datetime.now()}, Next candle at: {next_candle_time}")
        await asyncio.sleep(time_to_next_candle)
        log_info(f"New candle started. Current time: {datetime.now()}")