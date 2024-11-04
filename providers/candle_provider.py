# providers/candle_provider.py

import asyncio
import logging
from datetime import datetime
from typing import Callable, Awaitable, List

from time import sleep, time
from brokers.broker_interface import BrokerAPI
from utils.async_executor import execute_broker_call
from utils.enums import Timeframe
from utils.logger import log_info


class CandleProvider:
    """
    Fornisce le nuove candele a intervalli regolari e attiva i callback registrati.
    """

    def __init__(self, broker: BrokerAPI, symbol: str, timeframe: Timeframe, execution_lock: asyncio.Lock = None):
        self.broker = broker
        self.symbol = symbol
        self.timeframe = timeframe  # Timeframe in minuti
        self._running = False
        self._task = None
        self._on_new_candle_callbacks: List[Callable[[dict], Awaitable[None]]] = []
        self.execution_lock = execution_lock  # Lock per sincronizzare le esecuzioni

    async def start(self):
        """
        Avvia il provider di candele.
        """
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run())
            log_info(f"Candle provider per {self.symbol} avviato.")

    async def stop(self):
        """
        Arresta il provider di candele.
        """
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            log_info(f"Candle provider per {self.symbol} fermato.")

    def register_on_new_candle(self, callback: Callable[[dict], Awaitable[None]]):
        """
        Registra una funzione di callback da chiamare quando una nuova candela è disponibile.
        """
        if not callable(callback):
            raise ValueError("Il callback deve essere callable")
        self._on_new_candle_callbacks.append(callback)

    async def _run(self):
        while self._running:
            try:
                await self.wait_next_tick()
                candle = await self._fetch_candle()
                log_info(f"Nuova candela per {self.symbol}: {candle}")
                # Trigger dei callback in modo sincronizzato
                tasks = [callback(candle) for callback in self._on_new_candle_callbacks]
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                logging.error(f"Errore in CandleProvider._run: {e}", exc_info=True)
             # Attendi 60 secondi prima di recuperare la prossima candela

    async def _fetch_candle(self) -> dict:
        candles = await execute_broker_call(self.broker.get_last_candles, self.symbol, self.timeframe, 1)
        if candles:
            return candles[0]
        else:
            raise Exception("Errore nel recupero della candela")

    async def wait_next_tick(self):
        timeframe_duration = self.timeframe.to_seconds()
        current_time = time()
        time_to_next_candle = timeframe_duration - (current_time % timeframe_duration)
        next_candle_time = datetime.fromtimestamp(current_time + time_to_next_candle)
        log_info(f"Waiting for the next candle. Current time: {datetime.now()}, Next candle at: {next_candle_time}")
        await asyncio.sleep(time_to_next_candle)
        log_info(f"New candle started. Current time: {datetime.now()}")