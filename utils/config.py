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
    ConfigReader class implementing the Factory Pattern.
    Manages configuration instances based on the bot name extracted from the configuration file.
    Converts specific string values to corresponding Enum types for easier usage.
    """

    # Class dictionary to store ConfigReader instances for each bot_name
    _configs: Dict[str, 'ConfigReader'] = {}
    _lock = threading.Lock()

    def __init__(self, config_file_param: str):
        """
        Initializes an instance of ConfigReader.

        Parameters:
        - config_file_param: Path to the JSON configuration file.
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
        Factory method to create and get a ConfigReader instance based on the bot name.
        If the instance for the specified bot already exists, returns it.
        Otherwise, creates a new instance, stores it, and returns it.

        Parameters:
        - config_file_param: Path to the JSON configuration file.

        Returns:
        - ConfigReader instance associated with the bot_name extracted from the configuration file.
        """
        # Attempt to extract bot_name from the configuration file
        try:
            with open(config_file_param, 'r') as f:
                temp_config = json.load(f)
            bot_name = temp_config.get('bot', {}).get('name')
            if not bot_name:
                raise ValueError(f"Field 'name' in section 'bot' is missing in file {config_file_param}.")
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file {config_file_param} not found.")
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing JSON file {config_file_param}: {e}")

        bot_key = bot_name.lower()

        with cls._lock:
            if bot_key not in cls._configs:
                cls._configs[bot_key] = cls(config_file_param)
            return cls._configs[bot_key]

    @classmethod
    def get_config(cls, bot_name: str) -> 'ConfigReader':
        """
        Gets the ConfigReader instance associated with the bot_name.

        Parameters:
        - bot_name: Bot name to identify the configuration.

        Returns:
        - ConfigReader instance associated with the bot_name.

        Raises:
        - KeyError if no instance exists for the bot_name.
        """
        bot_key = bot_name.lower()
        if bot_key in cls._configs:
            return cls._configs[bot_key]
        else:
            raise KeyError(f"No configuration found for bot '{bot_name}'.")

    def _initialize_config(self):
        """
        Initializes various configuration sections and converts strings to Enums where needed.
        """

        with open(self.config_file, 'r') as f:
            self.config = json.load(f)

        if not self.config:
            raise ValueError("Configuration not loaded.")

        self.check_structure(self.config, required_structure)

        # Initialize MetaTrader5 configuration
        self.metatrader5_config = self.config.get("mt5", {})

        # Initialize Live configuration and convert specific fields to Enums
        trading_config = self.config.get("trading", {})
        trading_config['timeframe'] = string_to_enum(Timeframe, trading_config.get('timeframe'))
        trading_config['trading_direction'] = string_to_enum(TradingDirection, trading_config.get('trading_direction'))
        trading_config['risk_percent'] = float(trading_config.get('risk_percent', 0.0))
        self.live_config = trading_config

        # Initialize Bot configuration
        bot_config = self.config.get("bot", {})
        self.bot_config = bot_config

        # Initialize Telegram configuration and convert notification level to Enum
        telegram_config = self.config.get("telegram", {})
        telegram_config['notification_level'] = string_to_enum(NotificationLevel, telegram_config.get('notification_level'))
        self.telegram_config = telegram_config

        # Initialize MongoDB configuration
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

    # Getter methods for each configuration section

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

    # Specific getter methods for each section

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

    # Private method to ensure the configuration is loaded
    def _ensure_config_loaded(self):
        if self.config is None:
            raise ValueError("Configuration not loaded. Call load_config(filepath) first.")
