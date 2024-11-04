# providers/market_state_notifier.py

import asyncio
import logging
import time
from typing import Callable, Awaitable, List, Optional

from brokers.broker_interface import BrokerAPI
from utils.async_executor import execute_broker_call
from utils.error_handler import exception_handler
from utils.logger import log_info


class MarketStateNotifier:
    """
    Monitora lo stato del mercato (aperto/chiuso) per un simbolo specificato e attiva i callback registrati quando lo stato cambia.
    """

    def __init__(self, broker: BrokerAPI, symbol: str, execution_lock: asyncio.Lock = None):
        self.broker = broker
        self.symbol = symbol
        self._market_open = False
        self._running = False
        self._task = None
        self._on_market_status_change_callbacks: List[Callable[[bool, Optional[float], Optional[float]], Awaitable[None]]] = []
        self._market_closed_time = None
        self._market_opened_time = None
        self.execution_lock = execution_lock  # Lock per sincronizzare le esecuzioni

    async def start(self):
        """
        Avvia il notifier dello stato del mercato.
        """
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run())
            log_info(f"Market state notifier per {self.symbol} avviato.")

    async def stop(self):
        """
        Arresta il notifier dello stato del mercato.
        """
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            log_info(f"Market state notifier per {self.symbol} fermato.")

    def register_on_market_status_change(self, callback: Callable[[bool, Optional[float], Optional[float]], Awaitable[None]]):
        """
        Registra una funzione di callback da chiamare quando lo stato del mercato cambia.
        """
        if not callable(callback):
            raise ValueError("Il callback deve essere callable")
        self._on_market_status_change_callbacks.append(callback)

    async def _run(self):
        while self._running:
            try:
                market_is_open = await execute_broker_call(self.broker.get_market_status, self.symbol)
                if market_is_open != self._market_open:
                    previous_state = self._market_open
                    self._market_open = market_is_open

                    closing_time = None
                    opening_time = None

                    if self._market_open:
                        self._market_opened_time = time.time()
                        opening_time = self._market_opened_time
                        if self._market_closed_time is not None:
                            closing_time = self._market_closed_time
                            self._market_closed_time = None
                    else:
                        self._market_closed_time = time.time()
                        closing_time = self._market_closed_time
                        self._market_opened_time = None

                    state_str = "OPEN" if self._market_open else "CLOSED"
                    log_info(f"Il mercato per {self.symbol} è ora {state_str}.")

                    # Trigger dei callback in modo sincronizzato
                    tasks = [
                        callback(self._market_open, closing_time, opening_time)
                        for callback in self._on_market_status_change_callbacks
                    ]
                    await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                logging.error(f"Errore in MarketStateNotifier._run: {e}", exc_info=True)
            await asyncio.sleep(1)  # Controlla lo stato del mercato ogni secondo
