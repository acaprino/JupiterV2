import asyncio
from datetime import timedelta
from typing import List, Callable, Awaitable

from brokers.broker_interface import BrokerAPI
from datao.Position import Position
from utils.async_executor import execute_broker_call
from utils.error_handler import exception_handler
from utils.logger import log_info, log_debug, log_error
from utils.utils import now_utc


class ClosedDealsNotifier:

    def __init__(self, broker: BrokerAPI, symbol: str, execution_lock: asyncio.Lock = None):
        self.interval_seconds = None
        self.last_check_timestamp = None
        self.json_file_path = None
        self.sandbox_dir = None
        self.importance = None
        self.broker = broker
        self.symbol = symbol
        self.processed_events = {}  # Dictionary to track already processed events
        self._running = False
        self._task = None
        self._on_deal_status_change_event_callbacks: List[Callable[[Position], Awaitable[None]]] = []
        self.execution_lock = execution_lock  # Lock to synchronize executions

    async def start(self):
        if not self._running:
            # Configurations
            self.interval_seconds = 60
            self._running = True
            self._task = asyncio.create_task(self._run())

            timezone_offset = await execute_broker_call(self.broker.get_broker_timezone_offset, self.symbol)
            self.last_check_timestamp = now_utc() - timedelta(hours=timezone_offset)

            log_info(f"Deal status notifier started.")

    def register_on_deal_status_notifier(self, callback: Callable[[Position], Awaitable[None]]):
        if not callable(callback):
            raise ValueError("Callback must be callable")
        self._on_deal_status_change_event_callbacks.append(callback)
        log_debug(f"Callback registered: {callback}")

    def unregister_on_deal_status_notifier(self, callback: Callable[[Position], Awaitable[None]]):
        if callback in self._on_deal_status_change_event_callbacks:
            self._on_deal_status_change_event_callbacks.remove(callback)
            log_debug(f"Callback unregistered: {callback}")

    @exception_handler
    async def _run(self):
        while self._running:
            try:
                await asyncio.sleep(self.interval_seconds)  # Sleep asynchronously

                if not await execute_broker_call(self.broker.get_market_status, self.symbol):
                    log_debug(f"Market for {self.symbol} is closed. Skipping monitoring.")
                    continue

                timezone_offset = await execute_broker_call(self.broker.get_broker_timezone_offset, self.symbol)
                current_time_utc = now_utc() - timedelta(hours=timezone_offset)

                log_debug(f"Monitoring orders between {self.last_check_timestamp} and {current_time_utc}")

                deals = await execute_broker_call(self.broker.get_deals, self.last_check_timestamp, current_time_utc)

                self.last_check_timestamp = current_time_utc

                if not deals:
                    log_debug(f"No closed deals found in the interval.")
                    continue

                for position in deals:
                    tasks = [callback(position) for callback in self._on_deal_status_change_event_callbacks]
                    await asyncio.gather(*tasks, return_exceptions=True)
                    log_debug(f"Callbacks notified for position: {position}")

            except Exception as e:
                log_error(f"Error in ClosedDealsNotifier loop: {str(e)}")
                await asyncio.sleep(5)  # Prevenire un loop stretto in caso di errori persistenti

    async def stop(self):
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            log_info(f"Deal status notifier stopped.")
