import logging
import inspect
import os

from logging.handlers import RotatingFileHandler

# Warning: ConfigReader must be first initialized with config file path
logger = None


def log_init(bot_name, bot_version, level):
    global logger
    logger = logging.getLogger(f"{bot_name}-logger")
    log_level_num = getattr(logging, level)
    logger.setLevel(log_level_num)
    handler = RotatingFileHandler(f"logs/{bot_name}_{bot_version}.log", maxBytes=10 * 1024 * 1024, backupCount=50, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False


def _log(level, msg, exc_info=False):
    # Retrieve the frame two levels up in the stack
    frame = inspect.stack()[2]
    full_path = frame.filename
    relative_path = os.path.relpath(full_path)
    func_name = frame.function
    line_no = frame.lineno  # Get the line number

    # Construct the log message including the line number
    log_message = f'{relative_path}:{line_no} - {func_name} - {msg}'

    # Call the appropriate logging method
    getattr(logger, level)(log_message, exc_info=exc_info)

def warning(msg):
    _log('warning', msg)


def error(msg):
    _log('error', msg)


def critical(msg):
    _log('critical', msg)


def log_debug(msg):
    _log('debug', msg)


def log_info(msg):
    _log('info', msg)


def log_warning(msg):
    _log('warning', msg)


def log_error(msg):
    _log('error', msg, exc_info=True)
