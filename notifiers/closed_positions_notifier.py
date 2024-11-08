import asyncio
from datetime import timedelta
from typing import List, Callable, Awaitable

from brokers.broker_interface import BrokerAPI
from datao.Deal import Deal
from utils.async_executor import execute_broker_call
from utils.error_handler import exception_handler
from utils.logger import log_info, log_debug, log_error
from utils.utils_functions import now_utc


class ClosedPositionNotifier:
    """
    Monitors closed positions for a specific symbol and magic number, triggering registered callbacks when changes occur.
    """

    def __init__(self, broker: BrokerAPI, symbol: str, magic_number: int, execution_lock: asyncio.Lock = None):
        self.broker = broker
        self.symbol = symbol
        self.magic_number = magic_number
        self.execution_lock = execution_lock

        self.interval_seconds = 60 * 5
        self.last_check_timestamp = None
        self._running = False
        self._task = None
        self._on_deal_status_change_event_callbacks: List[Callable[[Deal], Awaitable[None]]] = []

    async def start(self):
        """Starts the closed position notifier loop, setting the initial timestamp for checking closed positions."""
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run())

            # Set the initial last check timestamp adjusted to broker's timezone
            timezone_offset = await execute_broker_call(self.broker.get_broker_timezone_offset, self.symbol)
            self.last_check_timestamp = now_utc() - timedelta(hours=timezone_offset)

            log_info(f"ClosedPositionNotifier started for symbol: {self.symbol}")

    def register_on_deal_status_notifier(self, callback: Callable[[Deal], Awaitable[None]]):
        """Registers a callback to be called when a closed position is detected."""
        if not callable(callback):
            raise ValueError("Callback must be callable")
        self._on_deal_status_change_event_callbacks.append(callback)
        log_debug("Callback registered for closed position notifications.")

    def unregister_on_deal_status_notifier(self, callback: Callable[[Deal], Awaitable[None]]):
        """Unregisters a previously registered callback."""
        if callback in self._on_deal_status_change_event_callbacks:
            self._on_deal_status_change_event_callbacks.remove(callback)
            log_debug("Callback unregistered from closed position notifications.")

    @exception_handler
    async def _run(self):
        """Main loop to periodically check for closed positions and trigger callbacks if any are found."""
        while self._running:
            try:
                await asyncio.sleep(self.interval_seconds)

                # Check if the market is open before proceeding
                if not await execute_broker_call(self.broker.is_market_open, self.symbol):
                    log_debug(f"Market for {self.symbol} is closed. Skipping closed position monitoring.")
                    continue

                # Adjust current time to broker's timezone and set check interval
                timezone_offset = await execute_broker_call(self.broker.get_broker_timezone_offset, self.symbol)
                current_time_utc = now_utc() - timedelta(hours=timezone_offset)

                log_debug(f"Checking for closed positions between {self.last_check_timestamp} and {current_time_utc}.")

                # Retrieve closed positions within the time interval
                closed_positions = await execute_broker_call(
                    self.broker.get_historical_positions,
                    self.last_check_timestamp,
                    current_time_utc,
                    self.symbol,
                    self.magic_number
                )

                # Update last check timestamp to the current time
                self.last_check_timestamp = current_time_utc

                if not closed_positions:
                    log_debug("No closed positions found in this interval.")
                    continue

                # Notify all registered callbacks for each closed position found
                for position in closed_positions:
                    tasks = [callback(position) for callback in self._on_deal_status_change_event_callbacks]
                    await asyncio.gather(*tasks, return_exceptions=True)
                    log_debug(f"Callbacks notified for closed position: {position}")

            except Exception as e:
                log_error(f"Error in ClosedPositionNotifier loop: {e}")
                await asyncio.sleep(5)  # Short delay before retrying in case of error

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
            log_info("ClosedPositionNotifier stopped.")
