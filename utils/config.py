import json
from utils.enums import BotMode, TradingDirection, Timeframe, NotificationLevel
from utils.utils_functions import string_to_enum


class ConfigReader:
    """
    Singleton class to read and provide configuration data from a JSON file.
    Converts specific string values to corresponding Enum types for ease of use.
    """
    metatrader5_config = None
    live_config = None
    bot_config = None
    telegram_config = None
    mongo_config = None
    _instance = None  # Singleton instance

    def __new__(cls, config_file_param: str = None):
        """Implements singleton pattern to ensure only one instance is created."""
        if cls._instance is None:
            cls._instance = super(ConfigReader, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_file_param: str = None):
        if not self._initialized:
            self.config = None
            self._initialized = True
            if config_file_param:
                self.load_config(config_file_param)

    def load_config(self, filepath: str):
        """Loads configuration from the specified JSON file."""
        with open(filepath, 'r') as file:
            self.config = json.load(file)
        self._initialize_config()

    def _initialize_config(self):
        """Initializes various configuration sections and converts strings to Enums where needed."""
        # Initialize MetaTrader5 config
        self.metatrader5_config = self.config.get("mt5", {})

        # Initialize Live config and convert certain fields to Enums
        trading_config = self.config.get("trading", {})
        trading_config['timeframe'] = string_to_enum(Timeframe, trading_config.get('timeframe'))
        trading_config['trading_direction'] = string_to_enum(TradingDirection, trading_config.get('trading_direction'))
        trading_config['risk_percent'] = float(trading_config.get('risk_percent', 0.0))
        self.live_config = trading_config

        # Initialize Bot config and convert mode to Enum
        bot_config = self.config.get("bot", {})
        bot_config['mode'] = string_to_enum(BotMode, bot_config.get('mode'))
        self.bot_config = bot_config

        # Initialize Telegram config and convert notification level to Enum
        telegram_config = self.config.get("telegram", {})
        telegram_config['notification_level'] = string_to_enum(NotificationLevel, telegram_config.get('notification_level'))
        self.telegram_config = telegram_config

        # Initialize MongoDB config
        self.mongo_config = self.config.get("mongo", {})

    # Getter methods for each configuration section

    def get_metatrader5_config(self):
        self._ensure_config_loaded()
        return self.metatrader5_config

    def get_config(self):
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

    # Specific configuration getters for each section

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
        return self.get_config().get("symbol")

    def get_timeframe(self) -> Timeframe:
        return self.get_config().get("timeframe")

    def get_trading_direction(self) -> TradingDirection:
        return self.get_config().get("trading_direction")

    def get_risk_percent(self) -> float:
        return self.get_config().get("risk_percent")

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

    # Private helper to ensure configuration is loaded
    def _ensure_config_loaded(self):
        if self.config is None:
            raise ValueError("Configuration not loaded. Call load_config(filepath) first.")
