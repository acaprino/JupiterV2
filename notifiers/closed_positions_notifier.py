import asyncio
from datetime import timedelta
from typing import List, Callable, Awaitable

from brokers.broker_interface import BrokerAPI
from dto.Deal import Deal
from utils.async_executor import execute_broker_call
from utils.error_handler import exception_handler
from utils.bot_logger import BotLogger
from utils.utils_functions import now_utc


class ClosedPositionNotifier:
    """
    Monitors closed positions for a specific symbol and magic number, triggering registered callbacks when changes occur.
    """

    def __init__(self, worker_id: str, broker: BrokerAPI, symbol: str, magic_number: int, execution_lock: asyncio.Lock = None):
        self.worker_id = worker_id
        self.logger = BotLogger.get_logger(worker_id)
        self.broker = broker
        self.symbol = symbol
        self.magic_number = magic_number
        self.execution_lock = execution_lock
        self.interval_seconds = 60 * 5
        self._running = False
        self._task = None
        self.last_check_timestamp = None
        self.started_with_closed_marked = None
        self._on_deal_status_change_event_callbacks: List[Callable[[Deal], Awaitable[None]]] = []

    @exception_handler
    async def start(self):
        """Starts the closed position notifier loop, setting the initial timestamp for checking closed positions."""
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run())

            # Set the initial last check timestamp adjusted to broker's timezone
            timezone_offset = await execute_broker_call(self.worker_id, self.broker.get_broker_timezone_offset, self.symbol)
            if timezone_offset is not None:
                self.last_check_timestamp = now_utc() - timedelta(hours=timezone_offset)
                self.started_with_closed_marked = True

            self.logger.info(f"ClosedPositionNotifier started for symbol: {self.symbol}")

    def register_on_deal_status_notifier(self, callback: Callable[[Deal], Awaitable[None]]):
        """Registers a callback to be called when a closed position is detected."""
        if not callable(callback):
            raise ValueError("Callback must be callable")
        self._on_deal_status_change_event_callbacks.append(callback)
        self.logger.debug("Callback registered for closed position notifications.")

    def unregister_on_deal_status_notifier(self, callback: Callable[[Deal], Awaitable[None]]):
        """Unregisters a previously registered callback."""
        if callback in self._on_deal_status_change_event_callbacks:
            self._on_deal_status_change_event_callbacks.remove(callback)
            self.logger.debug("Callback unregistered from closed position notifications.")

    @exception_handler
    async def _run(self):
        """Main loop to periodically check for closed positions and trigger callbacks if any are found."""
        exception = False
        exception_retry_seconds = 10
        while self._running:
            try:
                await asyncio.sleep(self.interval_seconds)

                # Check if the market is open before proceeding
                if not await execute_broker_call(self.worker_id, self.broker.is_market_open, self.symbol):
                    self.logger.debug(f"Market for {self.symbol} is closed. Skipping closed position monitoring.")
                    continue

                # Adjust current time to broker's timezone and set check interval
                timezone_offset = await execute_broker_call(self.worker_id, self.broker.get_broker_timezone_offset, self.symbol)
                now = now_utc()
                prev_check_timestamp = now - timedelta(seconds=self.interval_seconds) - timedelta(hours=timezone_offset)
                if exception:
                    prev_check_timestamp = prev_check_timestamp - timedelta(seconds=exception_retry_seconds)
                    exception = False

                current_time_utc = now - timedelta(hours=timezone_offset)

                self.logger.debug(f"Checking for closed positions between {prev_check_timestamp} and {current_time_utc}.")

                # Retrieve closed positions within the time interval
                closed_positions = await execute_broker_call(
                    self.worker_id,
                    self.broker.get_historical_positions,
                    prev_check_timestamp,
                    current_time_utc,
                    self.symbol,
                    self.magic_number
                )

                if not closed_positions:
                    self.logger.debug("No closed positions found in this interval.")
                    continue

                # Notify all registered callbacks for each closed position found
                for position in closed_positions:
                    tasks = [callback(position) for callback in self._on_deal_status_change_event_callbacks]
                    await asyncio.gather(*tasks, return_exceptions=True)
                    self.logger.debug(f"Callbacks notified for closed position: {position}")

            except Exception as e:
                self.logger.error(f"Error in ClosedPositionNotifier loop: {e}")
                exception = True
                await asyncio.sleep(exception_retry_seconds)

    @exception_handler
    async def stop(self):
        """Stops the closed position notifier by canceling the monitoring task."""
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            self.logger.info("ClosedPositionNotifier stopped.")
