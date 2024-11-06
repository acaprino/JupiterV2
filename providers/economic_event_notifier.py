# providers/economic_event_notifier.py

import asyncio
import json
import os
import time
from datetime import datetime, timedelta
from typing import Callable, Awaitable, List, Dict

from brokers.broker_interface import BrokerAPI
from utils.async_executor import execute_broker_call
from utils.error_handler import exception_handler
from utils.logger import log_debug, log_info, log_warning, log_error
from utils.utils import now_utc


class EconomicEventNotifier:
    """
    Monitors economic events and notifies registered callbacks when events of interest occur.
    """

    def __init__(self, broker: BrokerAPI, symbol: str, execution_lock: asyncio.Lock = None):
        self.interval_seconds = None
        self.json_file_path = None
        self.sandbox_dir = None
        self.importance = None
        self.broker = broker
        self.symbol = symbol
        self.processed_events = {}  # Dictionary to track already processed events
        self._running = False
        self._task = None
        self._on_economic_event_callbacks: List[Callable[[Dict], Awaitable[None]]] = []
        self.execution_lock = execution_lock  # Lock to synchronize executions

    async def start(self):
        if not self._running:
            # Configurations
            self.sandbox_dir = await execute_broker_call(self.broker.get_working_directory)
            self.json_file_path = os.path.join(self.sandbox_dir, 'economic_calendar.json')
            self.interval_seconds = 60 * 5
            self.importance = 3
            self._running = True
            self._task = asyncio.create_task(self._run())
            log_info(f"EconomicEventNotifier started.")

    def register_on_economic_event(self, callback: Callable[[Dict], Awaitable[None]]):
        if not callable(callback):
            raise ValueError("Callback must be callable")
        self._on_economic_event_callbacks.append(callback)
        log_debug(f"[EconomicEventNotifier] Callback registered: {callback}")

    def unregister_on_economic_event(self, callback: Callable[[Dict], Awaitable[None]]):
        if callback in self._on_economic_event_callbacks:
            self._on_economic_event_callbacks.remove(callback)
            log_debug(f"[EconomicEventNotifier] Callback unregistered: {callback}")

    def get_next_run_time(self, now: datetime) -> datetime:
        interval_minutes = self.interval_seconds / 60
        discard = timedelta(
            minutes=now.minute % interval_minutes,
            seconds=now.second,
            microseconds=now.microsecond
        )
        return now + timedelta(minutes=interval_minutes) - discard

    @exception_handler
    async def _run(self):
        while self._running:
            try:
                is_market_open = await execute_broker_call(self.broker.get_market_status, self.symbol)
                if not is_market_open:
                    log_info(f"[EconomicEventNotifier] Market closed for {self.symbol}. Waiting for {self.interval_seconds / 60} minutes.")
                    await asyncio.sleep(self.interval_seconds)
                    continue

                now = now_utc()

                # Calculate the next multiple of 5 minutes
                next_run = self.get_next_run_time(now)
                log_debug(f"[EconomicEventNotifier] Filtering events between {now} and {next_run}.")

                # Clean up expired events
                self._cleanup_processed_events(now)

                events = await self._load_events()
                if not events:
                    log_warning("[EconomicEventNotifier] No events loaded.")
                    # Calculate the time until the next multiple of 5 minutes
                    sleep_duration = (next_run - now).total_seconds()
                    await asyncio.sleep(max(sleep_duration, self.interval_seconds))
                    continue

                countries = self.get_symbol_countries_of_interest(self.symbol)
                log_debug(f"[EconomicEventNotifier] Countries of interest: {countries}")

                # Filter events based on countries, importance, and time
                filtered_events = [
                    event for event in events
                    if event.get('country_code') in countries
                       and event.get('event_importance') == self.importance
                       and now <= event.get('event_time') <= next_run
                       and event.get('event_id') not in self.processed_events
                ]
                log_debug(f"[EconomicEventNotifier] Filtered events: {filtered_events}")

                if not filtered_events:
                    log_debug("[EconomicEventNotifier] No events of interest found in this cycle.")
                else:
                    for event in filtered_events:
                        await self._handle_event(event)

            except Exception as e:
                log_error(f"[EconomicEventNotifier] Error while monitoring events: {str(e)}")

            # Calculate the time until the next multiple of 5 minutes
            now = now_utc()
            next_run = self.get_next_run_time(now)
            sleep_duration = (next_run - now).total_seconds()
            log_debug(f"[EconomicEventNotifier] Waiting for {sleep_duration} seconds until the next check.")
            await asyncio.sleep(max(sleep_duration, self.interval_seconds))

    def _cleanup_processed_events(self, current_time: datetime):
        expired_events = {event_id: event_time for event_id, event_time in self.processed_events.items()
                          if event_time <= current_time}
        if expired_events:
            for event_id in expired_events:
                del self.processed_events[event_id]
            log_debug(f"[EconomicEventNotifier] Expired events removed: {expired_events}")

    async def _load_events(self) -> List[Dict]:
        """
        Loads economic events from the JSON file.
        """
        log_debug(f"[EconomicEventNotifier] JSON file path for events: {self.json_file_path}")

        lock_file_path = os.path.join(self.sandbox_dir, 'lock.sem')
        self._wait_until_lock_file_removed(lock_file_path)

        if not os.path.exists(self.json_file_path):
            log_error(f"[EconomicEventNotifier] The JSON file for economic events does not exist: {self.json_file_path}")
            return []

        if os.path.getsize(self.json_file_path) == 0:
            log_error("[EconomicEventNotifier] The JSON file for economic events is empty.")
            return []

        try:
            timezone_offset = await execute_broker_call(self.broker.get_broker_timezone_offset, self.symbol)
            with open(self.json_file_path, 'r') as file:
                events = json.load(file)
                for event in events:
                    event['event_time'] = datetime.strptime(event['event_time'], '%Y.%m.%d %H:%M') - timedelta(hours=timezone_offset)
            log_debug(f"[EconomicEventNotifier] Events loaded successfully.")
            return events
        except json.JSONDecodeError as e:
            log_error("[EconomicEventNotifier] Error decoding the JSON file for economic events.")
            return []
        except Exception as e:
            log_error(f"[EconomicEventNotifier] Error loading economic events: {str(e)}")
            return []

    def _wait_until_lock_file_removed(self, lock_file_path: str, check_interval: int = 5, timeout: int = 300):
        """
        Waits until the lock file is removed.
        """
        start_time = time.time()
        while True:
            if not os.path.exists(lock_file_path):
                log_debug(f"[EconomicEventNotifier] Lock file removed: {lock_file_path}")
                break

            if time.time() - start_time > timeout:
                log_warning(f"[EconomicEventNotifier] Timeout reached. The lock file {lock_file_path} still exists.")
                break

            log_debug(f"[EconomicEventNotifier] Lock file still present: {lock_file_path}. Waiting for {check_interval} seconds.")
            time.sleep(check_interval)

    async def _handle_event(self, event: Dict):
        """
        Handles a single economic event.
        """
        event_id = event.get('event_id')
        event_name = event.get('event_name')
        event_time = event.get('event_time')

        log_info(f"[EconomicEventNotifier] Handling event: {event_name} (ID: {event_id}) at {event_time}.")

        seconds_until_event = (event_time - datetime.now()).total_seconds()

        self.processed_events[event_id] = event_time
        log_debug(f"[EconomicEventNotifier] Event {event_id} marked as processed.")

        # Notify registered callbacks
        await self._notify_callbacks(event)

    async def _notify_callbacks(self, notification: Dict):
        """
        Notifies all registered callbacks with the event information.
        """
        tasks = [callback(notification) for callback in self._on_economic_event_callbacks]
        await asyncio.gather(*tasks, return_exceptions=True)
        log_debug(f"[EconomicEventNotifier] Callbacks notified for event: {notification.get('event').get('event_id')}")

    def get_symbol_countries_of_interest(self, symbol):
        try:
            pair = self.get_pair(symbol)

            if pair is None:
                return []

            countries = pair["countries"]
            log_debug(f"Loaded countries of interest: {countries}")
            return countries

        except Exception as e:
            log_error(f"An error occurred: {e}")
            return []

    def get_pairs(self) -> List[Dict]:
        try:
            cur_script_directory = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.dirname(cur_script_directory) + '/pairs.json'
            # Read the JSON data from the file
            with open(file_path, 'r') as f:
                data = json.load(f)

            return data
        except Exception as e:
            log_error(f"An error occurred: {e}")
            return []

    def get_pair(self, symbol) -> dict | None:
        for pair in self.get_pairs():
            if pair["symbol"] == symbol:
                return pair
        log_error(f"Symbol '{symbol}' not found in pairs.json")
        return None

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