import json
import threading
from typing import Dict, Any

from utils.enums import TradingDirection, Timeframe, NotificationLevel
from utils.utils_functions import string_to_enum

required_structure = {
    "enabled": bool,
    "mt5": {
        "timeout": int,
        "account": int,
        "password": str,
        "server": str,
        "mt5_path": str
    },
    "trading": {
        "symbol": str,
        "timeframe": str,
        "trading_direction": str,
        "risk_percent": float
    },
    "bot": {
        "version": float,
        "name": str,
        "magic_number": int,
        "symbols_db_sheet_id": str,
        "logging_level": str
    },
    "telegram": {
        "token": str,
        "chat_ids": list,
        "active": bool,
        "notification_level": str
    },
    "mongo": {
        "host": str,
        "port": str,
        "db_name": str
    }
}


class ConfigReader:
    """
    Classe ConfigReader che implementa il Pattern Factory.
    Gestisce istanze di configurazione basate sul nome del bot estratto dal file di configurazione.
    Converte valori stringa specifici in tipi Enum corrispondenti per una facile utilizzazione.
    """

    # Dizionario di classe per memorizzare le istanze ConfigReader per ogni bot_name
    _configs: Dict[str, 'ConfigReader'] = {}
    _lock = threading.Lock()

    def __init__(self, config_file_param: str):
        """
        Inizializza un'istanza di ConfigReader.

        Parametri:
        - config_file_param: Percorso al file di configurazione JSON.
        """
        self.config_file = config_file_param
        self.config = None
        self.metatrader5_config = None
        self.live_config = None
        self.bot_config = None
        self.telegram_config = None
        self.mongo_config = None

        self._initialize_config()

    @classmethod
    def load_config(cls, config_file_param: str) -> 'ConfigReader':
        """
        Metodo factory per creare e ottenere un'istanza di ConfigReader basata sul nome del bot.
        Se l'istanza per il bot specificato esiste già, la restituisce.
        Altrimenti, crea una nuova istanza, la memorizza e la restituisce.

        Parametri:
        - config_file_param: Percorso al file di configurazione JSON.

        Ritorna:
        - Istanza di ConfigReader associata al bot_name estratto dal file di configurazione.
        """
        # Tentativo di estrarre il bot_name dal file di configurazione
        try:
            with open(config_file_param, 'r') as f:
                temp_config = json.load(f)
            bot_name = temp_config.get('bot', {}).get('name')
            if not bot_name:
                raise ValueError(f"Il campo 'name' nella sezione 'bot' non è presente nel file {config_file_param}.")
        except FileNotFoundError:
            raise FileNotFoundError(f"Il file di configurazione {config_file_param} non è stato trovato.")
        except json.JSONDecodeError as e:
            raise ValueError(f"Errore nel parsing del file JSON {config_file_param}: {e}")

        bot_key = bot_name.lower()

        with cls._lock:
            if bot_key not in cls._configs:
                cls._configs[bot_key] = cls(config_file_param)
            return cls._configs[bot_key]

    @classmethod
    def get_config(cls, bot_name: str) -> 'ConfigReader':
        """
        Ottiene l'istanza di ConfigReader associata al bot_name.

        Parametri:
        - bot_name: Nome del bot per identificare la configurazione.

        Ritorna:
        - Istanza di ConfigReader associata al bot_name.

        Solleva:
        - KeyError se non esiste un'istanza per il bot_name.
        """
        bot_key = bot_name.lower()
        if bot_key in cls._configs:
            return cls._configs[bot_key]
        else:
            raise KeyError(f"Nessuna configurazione trovata per il bot '{bot_name}'.")

    def _initialize_config(self):
        """
        Inizializza le varie sezioni di configurazione e converte le stringhe in Enum dove necessario.
        """

        with open(self.config_file, 'r') as f:
            self.config = json.load(f)

        if not self.config:
            raise ValueError("Configurazione non caricata.")

        self.check_structure(self.config, required_structure)

        # Inizializza la configurazione MetaTrader5
        self.metatrader5_config = self.config.get("mt5", {})

        # Inizializza la configurazione Live e converte alcuni campi in Enum
        trading_config = self.config.get("trading", {})
        trading_config['timeframe'] = string_to_enum(Timeframe, trading_config.get('timeframe'))
        trading_config['trading_direction'] = string_to_enum(TradingDirection, trading_config.get('trading_direction'))
        trading_config['risk_percent'] = float(trading_config.get('risk_percent', 0.0))
        self.live_config = trading_config

        # Inizializza la configurazione Bot e converte il mode in Enum
        bot_config = self.config.get("bot", {})
        self.bot_config = bot_config

        # Inizializza la configurazione Telegram e converte il livello di notifica in Enum
        telegram_config = self.config.get("telegram", {})
        telegram_config['notification_level'] = string_to_enum(NotificationLevel, telegram_config.get('notification_level'))
        self.telegram_config = telegram_config

        # Inizializza la configurazione MongoDB
        self.mongo_config = self.config.get("mongo", {})

    def check_structure(self, data: Dict[str, Any], structure: Dict[str, Any], path=""):
        for key, expected_type in structure.items():
            full_path = f"{path}.{key}" if path else key
            if key not in data:
                raise ValueError(f"Missing key '{full_path}' in the configuration.")
            if isinstance(expected_type, dict):
                if not isinstance(data[key], dict):
                    raise TypeError(f"Key '{full_path}' should be a dictionary.")
                self.check_structure(data[key], expected_type, full_path)
            elif not isinstance(data[key], expected_type):
                raise TypeError(f"Key '{full_path}' should be of type {expected_type.__name__}.")

    # Metodi getter per ciascuna sezione di configurazione

    def get_metatrader5_config(self):
        self._ensure_config_loaded()
        return self.metatrader5_config

    def get_live_config(self):
        self._ensure_config_loaded()
        return self.live_config

    def get_bot_config(self):
        self._ensure_config_loaded()
        return self.bot_config

    def get_telegram_config(self):
        self._ensure_config_loaded()
        return self.telegram_config

    def get_mongo_config(self):
        self._ensure_config_loaded()
        return self.mongo_config

    # Metodi getter specifici per ogni sezione

    def get_mt5_timeout(self):
        return self.get_metatrader5_config().get("timeout")

    def get_mt5_account(self):
        return self.get_metatrader5_config().get("account")

    def get_mt5_password(self):
        return self.get_metatrader5_config().get("password")

    def get_mt5_server(self):
        return self.get_metatrader5_config().get("server")

    def get_mt5_path(self):
        return self.get_metatrader5_config().get("mt5_path")

    def get_symbol(self):
        return self.get_live_config().get("symbol")

    def get_timeframe(self) -> Timeframe:
        return self.get_live_config().get("timeframe")

    def get_trading_direction(self) -> TradingDirection:
        return self.get_live_config().get("trading_direction")

    def get_risk_percent(self) -> float:
        return self.get_live_config().get("risk_percent")

    def get_bot_version(self):
        return self.get_bot_config().get("version")

    def get_bot_name(self):
        return self.get_bot_config().get("name")

    def get_bot_magic_number(self):
        return self.get_bot_config().get("magic_number")

    def get_bot_logging_level(self):
        return self.get_bot_config().get("logging_level")

    def get_bot_symbols_db_sheet_id(self):
        return self.get_bot_config().get("symbols_db_sheet_id")

    def get_telegram_token(self):
        return self.get_telegram_config().get("token")

    def get_telegram_chat_ids(self):
        return self.get_telegram_config().get("chat_ids")

    def get_telegram_active(self):
        return self.get_telegram_config().get("active")

    def get_telegram_notification_level(self) -> NotificationLevel:
        return self.get_telegram_config().get("notification_level")

    def get_mongo_host(self):
        return self.get_mongo_config().get("host")

    def get_mongo_port(self) -> int:
        return int(self.get_mongo_config().get("port"))

    def get_mongo_db_name(self) -> str:
        return self.get_mongo_config().get("db_name")

    # Metodo privato per assicurarsi che la configurazione sia caricata
    def _ensure_config_loaded(self):
        if self.config is None:
            raise ValueError("Configurazione non caricata. Chiamare load_config(filepath) prima.")
