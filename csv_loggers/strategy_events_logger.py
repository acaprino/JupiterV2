from csv_loggers.csv_logger import CSVLogger
from utils.utils_functions import now_utc


class StrategyEventsLogger(CSVLogger):

    def __init__(self, symbol, timeframe, trading_direction):
        timeframe = timeframe.name
        trading_direction = trading_direction.name
        output_path = f"{symbol}/{timeframe}/{trading_direction}"
        logger_name = f'strategy_signals_{symbol}_{timeframe}_{trading_direction}'
        super().__init__(logger_name, output_path, real_time_logging=True, max_bytes=10 ** 6, backup_count=10, memory_buffer_size=0)

    def add_event(self, time_open, time_close, close_price, state_pre, state_cur, message, supert_fast_prev, supert_slow_prev, supert_fast_cur, supert_slow_cur, stoch_k_cur, stoch_d_cur):
        event = {
            'Candle_time_open': time_open,
            'Candle_time_close': time_close,
            'Timestamp': now_utc().strftime("%d/%m/%Y %H:%M:%S"),
            'Event': message,
            'Close price:': close_price,
            'State prev.': state_pre,
            'State cur.': state_cur,
            'Supertrend Fast prev.': supert_fast_prev,
            'Supertrend Slow prev.': supert_slow_prev,
            'Supertrend Fast cur.': supert_fast_cur,
            'Supertrend Slow cur.': supert_slow_cur,
            'Stochastic K cur.': stoch_k_cur,
            'Stochastic D cur.': stoch_d_cur
        }
        self.record(event)
