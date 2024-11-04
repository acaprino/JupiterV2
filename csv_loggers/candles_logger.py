from pandas import Series

from csv_loggers.csv_logger import CSVLogger


class CandlesLogger(CSVLogger):

    def __init__(self, symbol, timeframe, trading_direction, custom_name=''):
        timeframe = timeframe.name
        trading_direction = trading_direction.name
        output_path = f"{symbol}/{timeframe}/{trading_direction}"
        logger_name = f'candles_{symbol}_{timeframe}_{trading_direction}_{custom_name}'
        super().__init__(logger_name, output_path, real_time_logging=True, max_bytes=10 ** 6, backup_count=10, memory_buffer_size=0)

    def add_candle(self, candle: Series):
        dic = candle.to_dict()
        self.record(dic)


class BTOutcomeLogger(CSVLogger):

    def __init__(self, symbol, timeframe, trading_direction):
        timeframe = timeframe.name
        trading_direction = trading_direction.name
        output_path = f"{symbol}/{timeframe}/{trading_direction}"
        logger_name = f'bt_outcome_log_{symbol}_{timeframe}_{trading_direction}'
        super().__init__(logger_name, output_path, real_time_logging=True, max_bytes=10 ** 6, backup_count=10, memory_buffer_size=1)

    def add_outcome(self, timestamp, order_type, enter_price, exit_price, balance, profit, pl):
        outcome = {'Timestamp': timestamp,
                   'Op. type': order_type.value,
                   'Enter price': enter_price,
                   'Exit price': exit_price,
                   'Balance': balance,
                   'Profit': profit,
                   'P&L': pl}
        self.record(outcome)