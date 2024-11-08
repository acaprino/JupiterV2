import logging
import inspect
import os
from logging.handlers import RotatingFileHandler

# Warning: Initialize ConfigReader with config file path before using this logger
logger = None


def log_init(bot_name: str, bot_version: str, level: str):
    """
    Initializes the logger with a rotating file handler.

    Parameters:
    - bot_name: Name of the bot to include in the logger's name and filename.
    - bot_version: Version of the bot, used in the log filename.
    - level: Logging level as a string (e.g., 'DEBUG', 'INFO').
    """
    global logger
    logger = logging.getLogger(f"{bot_name}-logger")
    log_level_num = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(log_level_num)

    log_file_path = f"logs/{bot_name}_{bot_version}.log"
    handler = RotatingFileHandler(log_file_path, maxBytes=10 * 1024 * 1024, backupCount=50, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)  # Ensure log directory exists

    log_info(f"Logger initialized for {bot_name} version {bot_version} at level {level}")


def _log(level: str, msg: str, exc_info: bool = False):
    """
    Internal helper to log messages with file, function, and line details.

    Parameters:
    - level: Logging level as a string (e.g., 'debug', 'info').
    - msg: The log message.
    - exc_info: If True, includes exception info in the log.
    """
    frame = inspect.stack()[2]
    relative_path = os.path.relpath(frame.filename)
    func_name = frame.function
    line_no = frame.lineno

    log_message = f"{relative_path}:{line_no} - {func_name} - {msg}"
    getattr(logger, level)(log_message, exc_info=exc_info)


def log_debug(msg: str):
    """Logs a message at DEBUG level."""
    _log('debug', msg)


def log_info(msg: str):
    """Logs a message at INFO level."""
    _log('info', msg)


def log_warning(msg: str):
    """Logs a message at WARNING level."""
    _log('warning', msg)


def log_error(msg: str):
    """Logs a message at ERROR level with exception info if available."""
    _log('error', msg, exc_info=True)


def log_critical(msg: str):
    """Logs a message at CRITICAL level."""
    _log('critical', msg)
