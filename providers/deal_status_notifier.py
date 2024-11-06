import asyncio
from typing import List, Callable, Awaitable

from brokers.broker_interface import BrokerAPI
from datao.Position import Position
from utils.error_handler import exception_handler
from utils.logger import log_info, log_debug


class ClosedDealsNotifier:

    def __init__(self, broker: BrokerAPI, symbol: str, execution_lock: asyncio.Lock = None):
        self.interval_seconds = None
        self.json_file_path = None
        self.sandbox_dir = None
        self.importance = None
        self.broker = broker
        self.symbol = symbol
        self.processed_events = {}  # Dizionario per tracciare gli eventi già elaborati
        self._running = False
        self._task = None
        self._on_deal_status_change_event_callbacks: List[Callable[[Position], Awaitable[None]]] = []
        self.execution_lock = execution_lock  # Lock per sincronizzare le esecuzioni

    async def start(self):
        if not self._running:
            # Configurazioni
            self._running = True
            self._task = asyncio.create_task(self._run())
            log_info(f"Deal status notifier avviato.")

    def register_on_deal_status_notifier(self, callback: Callable[[Position], Awaitable[None]]):
        if not callable(callback):
            raise ValueError("Il callback deve essere callable")
        self._on_deal_status_change_event_callbacks.append(callback)
        log_debug(f"[ClosedDealsNotifier] Callback registrato: {callback}")

    def unregister_on_deal_status_notifier(self, callback: Callable[[Position], Awaitable[None]]):
        if callback in self._on_deal_status_change_event_callbacks:
            self._on_deal_status_change_event_callbacks.remove(callback)
            log_debug(f"[ClosedDealsNotifier] Callback annullato: {callback}")

    @exception_handler
    async def _run(self):
        while self._running:
            pass

    async def _notify_callbacks(self, notification: Position):
        tasks = [callback(notification) for callback in self._on_deal_status_change_event_callbacks]
        await asyncio.gather(*tasks, return_exceptions=True)
        log_debug(f"[EconomicEventNotifier] Callback notificati per la posizione: {notification}")

    async def stop(self):
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            log_info(f"Deal Status notifier fermato.")
