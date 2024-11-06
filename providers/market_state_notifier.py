# providers/market_state_notifier.py

import asyncio
import logging
import time
from typing import Callable, Awaitable, List, Optional, Tuple

from brokers.broker_interface import BrokerAPI
from utils.async_executor import execute_broker_call
from utils.logger import log_info


class MarketStateNotifier:
    """
    Monitors the market state (open/closed) for a specified symbol and triggers registered callbacks when the state changes.
    """

    def __init__(self, broker: BrokerAPI, symbol: str, execution_lock: asyncio.Lock = None):
        self.broker = broker
        self.symbol = symbol
        self._market_open: Optional[bool] = None  # Initialize as None to represent unknown state
        self._running = False
        self._task = None
        self._on_market_status_change_callbacks: List[Callable[[bool, Optional[float], Optional[float], Optional[bool]], Awaitable[None]]] = []
        self._market_closed_time: Optional[float] = None
        self._market_opened_time: Optional[float] = None
        self.execution_lock = execution_lock  # Lock to synchronize executions
        self._initialized = False  # Flag to track initial notification

    async def start(self):
        """
        Starts the market state notifier.
        """
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run())
            log_info(f"Market state notifier for {self.symbol} started.")

    async def stop(self):
        """
        Stops the market state notifier.
        """
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            log_info(f"Market state notifier for {self.symbol} stopped.")

    def register_on_market_status_change(
            self,
            callback: Callable[[bool, Optional[float], Optional[float], Optional[bool]], Awaitable[None]]
    ):
        """
        Registers a callback function to be called when the market state changes.
        """
        if not callable(callback):
            raise ValueError("Callback must be callable")
        self._on_market_status_change_callbacks.append(callback)

    async def _update_market_state(self, market_is_open: bool, initializing: bool = False):
        current_time = time.time()

        if initializing:
            if market_is_open:
                self._market_opened_time = current_time
                self._market_closed_time = None
                log_info(f"The market for {self.symbol} is currently OPEN.")
            else:
                self._market_closed_time = current_time
                self._market_opened_time = None
                log_info(f"The market for {self.symbol} is currently CLOSED.")
        else:
            if market_is_open:
                self._market_opened_time = current_time
                self._market_closed_time = None
                log_info(f"The market for {self.symbol} is now OPEN.")
            else:
                self._market_closed_time = current_time
                self._market_opened_time = None
                log_info(f"The market for {self.symbol} is now CLOSED.")

        self._market_open = market_is_open

        # Trigger callbacks with the current state
        tasks = [
            callback(self._market_open, self._market_closed_time, self._market_opened_time, initializing)
            for callback in self._on_market_status_change_callbacks
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run(self):
        while self._running:
            try:
                # Recupera lo stato attuale del mercato
                market_is_open = await execute_broker_call(self.broker.get_market_status, self.symbol)

                if not self._initialized:
                    # Aggiorna lo stato iniziale e invoca le callback
                    await self._update_market_state(market_is_open, initializing=True)
                    self._initialized = True
                else:
                    if market_is_open != self._market_open:
                        # Aggiorna lo stato e invoca le callback in caso di cambiamento
                        await self._update_market_state(market_is_open, initializing=False)

                # Attendi prima del prossimo controllo
                await asyncio.sleep(5)  # Controlla lo stato del mercato ogni 5 secondi
            except Exception as e:
                logging.error(f"Error in MarketStateNotifier._run: {e}", exc_info=True)
                await asyncio.sleep(5)  # Prevenire un loop stretto in caso di errori persistenti
