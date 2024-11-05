# providers/economic_event_notifier.py

import asyncio
import json
import os
import time
from datetime import datetime, timedelta
from typing import Callable, Awaitable, List, Dict

from brokers.broker_interface import BrokerAPI
from utils.async_executor import execute_broker_call
from utils.logger import log_debug, log_info, log_warning, log_error


class EconomicEventNotifier:
    """
    Monitora gli eventi economici e notifica i callback registrati quando si verificano eventi di interesse.
    """

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
        self._on_economic_event_callbacks: List[Callable[[Dict], Awaitable[None]]] = []
        self.execution_lock = execution_lock  # Lock per sincronizzare le esecuzioni

    async def start(self):
        if not self._running:
            # Configurazioni
            self.sandbox_dir = await execute_broker_call(self.broker.get_working_directory)
            self.json_file_path = os.path.join(self.sandbox_dir, 'economic_calendar.json')
            self.interval_seconds = 60 * 5
            self.importance = 3
            self._running = True
            self._task = asyncio.create_task(self._run())
            log_info(f"Candle provider per {self.symbol} avviato.")

    def register_on_economic_event(self, callback: Callable[[Dict], Awaitable[None]]):
        if not callable(callback):
            raise ValueError("Il callback deve essere callable")
        self._on_economic_event_callbacks.append(callback)
        log_debug(f"[EconomicEventNotifier] Callback registrato: {callback}")

    def unregister_on_economic_event(self, callback: Callable[[Dict], Awaitable[None]]):
        if callback in self._on_economic_event_callbacks:
            self._on_economic_event_callbacks.remove(callback)
            log_debug(f"[EconomicEventNotifier] Callback annullato: {callback}")

    async def _run(self):
        while self._running:
            try:
                is_market_open = await execute_broker_call(self.broker.get_market_status, self.symbol)
                if not is_market_open:
                    log_info(f"[EconomicEventNotifier] Mercato chiuso per {self.symbol}. Attesa di {self.interval_seconds / 60} minuti.")
                    await asyncio.sleep(self.interval_seconds)
                    continue

                now = datetime.now().replace(microsecond=0)

                # Calcola il momento in cui controllare gli eventi (esattamente nelle prossime 5 minuti)
                soon = now + timedelta(minutes=5)
                log_debug(f"[EconomicEventNotifier] Filtraggio degli eventi per le ore {soon}.")

                # Pulisci gli eventi scaduti
                self._cleanup_processed_events(now)

                events = self._load_events()
                if not events:
                    log_warning("[EconomicEventNotifier] Nessun evento caricato.")
                    await asyncio.sleep(self.interval_seconds)
                    continue

                countries = self.get_symbol_countries_of_interest(self.symbol)
                log_debug(f"[EconomicEventNotifier] Paesi di interesse: {countries}")

                # Filtra gli eventi in base a paesi, importanza e tempo
                filtered_events = [
                    event for event in events
                    if event.get('country_code') in countries
                       and event.get('event_importance') == self.importance
                       and event.get('event_time') == soon
                       and event.get('event_id') not in self.processed_events
                ]
                log_debug(f"[EconomicEventNotifier] Eventi filtrati: {filtered_events}")

                if not filtered_events:
                    log_debug("[EconomicEventNotifier] Nessun evento di interesse trovato in questo ciclo.")
                else:
                    for event in filtered_events:
                        await self._handle_event(event)

            except Exception as e:
                log_error(f"[EconomicEventNotifier] Errore durante il monitoraggio degli eventi: {str(e)}")

            await asyncio.sleep(self.interval_seconds)

    def _cleanup_processed_events(self, current_time: datetime):
        expired_events = {event_id: event_time for event_id, event_time in self.processed_events.items()
                          if event_time <= current_time}
        if expired_events:
            for event_id in expired_events:
                del self.processed_events[event_id]
            log_debug(f"[EconomicEventNotifier] Eventi scaduti rimossi: {expired_events}")

    def _load_events(self) -> List[Dict]:
        """
        Carica gli eventi economici dal file JSON.
        """
        log_debug(f"[EconomicEventNotifier] Percorso del file JSON degli eventi: {self.json_file_path}")

        lock_file_path = os.path.join(self.sandbox_dir, 'lock.sem')
        self._wait_until_lock_file_removed(lock_file_path)

        if not os.path.exists(self.json_file_path):
            log_error(f"[EconomicEventNotifier] Il file JSON degli eventi economici non esiste: {self.json_file_path}")
            return []

        if os.path.getsize(self.json_file_path) == 0:
            log_error("[EconomicEventNotifier] Il file JSON degli eventi economici è vuoto.")
            return []

        try:
            with open(self.json_file_path, 'r') as file:
                events = json.load(file)
                for event in events:
                    # Assicurati che 'event_time' sia un oggetto datetime
                    event['event_time'] = datetime.strptime(event['event_time'], '%Y.%m.%d %H:%M')
            log_debug(f"[EconomicEventNotifier] Eventi caricati con successo.")
            return events
        except json.JSONDecodeError as e:
            log_error("[EconomicEventNotifier] Errore nel decodificare il file JSON degli eventi economici.")
            return []
        except Exception as e:
            log_error(f"[EconomicEventNotifier] Errore nel caricamento degli eventi economici: {str(e)}")
            return []

    def _wait_until_lock_file_removed(self, lock_file_path: str, check_interval: int = 5, timeout: int = 300):
        """
        Attende fino a quando il file di lock non viene rimosso.
        """
        start_time = time.time()
        while True:
            if not os.path.exists(lock_file_path):
                log_debug(f"[EconomicEventNotifier] File di lock rimosso: {lock_file_path}")
                break

            if time.time() - start_time > timeout:
                log_warning(f"[EconomicEventNotifier] Timeout raggiunto. Il file di lock {lock_file_path} esiste ancora.")
                break

            log_debug(f"[EconomicEventNotifier] File di lock ancora presente: {lock_file_path}. Attesa di {check_interval} secondi.")
            time.sleep(check_interval)

    async def _handle_event(self, event: Dict):
        """
        Gestisce un singolo evento economico.
        """
        event_id = event.get('event_id')
        event_name = event.get('event_name')
        event_time = event.get('event_time')

        log_info(f"[EconomicEventNotifier] Gestione dell'evento: {event_name} (ID: {event_id}) alle {event_time}.")

        seconds_until_event = (event_time - datetime.now()).total_seconds()
        minutes_until_event = int(seconds_until_event // 60)

        self.processed_events[event_id] = event_time
        log_debug(f"[EconomicEventNotifier] Evento {event_id} marcato come processato.")

        # Notifica i callback registrati
        await self._notify_callbacks(event)

    async def _notify_callbacks(self, notification: Dict):
        """
        Notifica tutti i callback registrati con le informazioni dell'evento.
        """
        tasks = [callback(notification) for callback in self._on_economic_event_callbacks]
        await asyncio.gather(*tasks, return_exceptions=True)
        log_debug(f"[EconomicEventNotifier] Callback notificati per l'evento: {notification.get('event').get('event_id')}")

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
