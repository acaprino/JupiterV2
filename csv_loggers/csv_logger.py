import os
import io
import logging
import pandas as pd

from datetime import datetime

from logging.handlers import RotatingFileHandler

from utils.utils import create_directories


class CSVLogger:
    _instances = {}

    def __new__(cls, *args, **kwargs):
        # Construct a key from parameters that differentiate the instances
        key = (cls, args, frozenset(kwargs.items()))
        if key not in cls._instances:
            cls._instances[key] = super(CSVLogger, cls).__new__(cls)
        return cls._instances[key]

    def __init__(self, logger_name=None, output_path=None, real_time_logging=False, max_bytes=10 ** 6, backup_count=10, memory_buffer_size=0):
        if not hasattr(self, 'initialized'):  # Prevent reinitialization
            self.initialized = False
            self.logger = None
            self.real_time_logging = None
            self.csv_file_path = None
            self.log_buffer = None
            self.relative_output_path = None
            self.full_output_path = None
            self.file_name = None
            self.memory_buffer_size = None
            self.initialize_logger(logger_name, output_path, real_time_logging, max_bytes, backup_count, memory_buffer_size)

    def initialize_logger(self, logger_name, output_path, real_time_logging, max_bytes, backup_count, memory_buffer_size):
        create_directories(f"output/{output_path}")
        self.log_buffer = []
        self.full_output_path = f"output/{output_path}"
        self.relative_output_path = output_path
        current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.file_name = f"{logger_name}_{current_time}.csv"
        self.csv_file_path = f"{self.full_output_path}/{self.file_name}" if logger_name else None
        self.real_time_logging = real_time_logging
        self.logger = logging.getLogger(logger_name) if logger_name else None
        self.memory_buffer_size = memory_buffer_size
        if self.csv_file_path and self.logger:
            handler = RotatingFileHandler(self.csv_file_path, maxBytes=max_bytes, backupCount=backup_count)
            self.logger.setLevel(logging.INFO)
            self.logger.addHandler(handler)
        self.initialized = True

    def record(self, event):
        if self.real_time_logging:
            self.write_to_csv([event])
            self.log_buffer.append(event)
            if len(self.log_buffer) > self.memory_buffer_size:
                self.log_buffer.pop(0)
        else:
            self.log_buffer.append(event)

    def flush_to_csv(self):
        if not self.real_time_logging and self.log_buffer:
            self.write_to_csv(self.log_buffer)
            self.log_buffer = []

    def write_to_csv(self, rows):
        if not rows:
            return
        df = pd.DataFrame(rows)
        buffer = io.StringIO()
        header = not os.path.exists(self.csv_file_path) or os.path.getsize(self.csv_file_path) == 0
        df.to_csv(buffer, sep=';', index=False, header=header, date_format='%Y-%m-%d %H:%M:%S', decimal=',')
        buffer.seek(0)  # Move back to the start of the buffer
        for line in buffer:
            self.logger.info(line.strip())

    def reset_buffer(self):
        self.log_buffer = []

    def get_latest_log(self):
        return self.log_buffer[-1] if self.log_buffer and len(self.log_buffer) > 0 else None

    def get_file_name(self) -> str:
        return self.file_name

    def get_relative_output_path(self) -> str:
        return self.relative_output_path

    def get_full_file_path(self) -> str:
        return self.csv_file_path

    def get_full_output_path(self) -> str:
        return self.full_output_path