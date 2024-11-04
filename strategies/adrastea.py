# strategies/my_strategy.py

import math

import numpy as np
import pandas as pd
from pandas import Series

from csv_loggers.candles_logger import CandlesLogger
from datao.SymbolInfo import SymbolInfo
from providers.candle_provider import CandleProvider
from providers.market_state_notifier import MarketStateNotifier
from strategies.base_strategy import TradingStrategy
from strategies.indicators import supertrend, stochastic, average_true_range
from utils.async_executor import execute_broker_call
from utils.config import ConfigReader
from utils.enums import Indicators, Timeframe, TradingDirection
from utils.error_handler import exception_handler
from brokers.broker_interface import BrokerAPI

from utils.logger import log_info, log_error, log_debug
from utils.utils import describe_candle

leverages = {
    "FOREX": [10, 30, 100],
    "METALS": [10],
    "OIL": [10],
    "CRYPTOS": [5]
}

# Indicator parameters
supertrend_fast_period = 10
supertrend_fast_multiplier = 1
supertrend_slow_period = 40
supertrend_slow_multiplier = 3

stoch_k_period = 24
stoch_d_period = 5
stoch_smooth_k = 3

# Series keys prefix
STOCHASTIC_K = Indicators.STOCHASTIC_K.name
STOCHASTIC_D = Indicators.STOCHASTIC_D.name
SUPERTREND = Indicators.SUPERTREND.name
MOVING_AVERAGE = Indicators.MOVING_AVERAGE.name
ATR = Indicators.ATR.name

# Indicators series keys
supertrend_fast_key = SUPERTREND + '_' + str(supertrend_fast_period) + '_' + str(supertrend_fast_multiplier)
supertrend_slow_key = SUPERTREND + '_' + str(supertrend_slow_period) + '_' + str(supertrend_slow_multiplier)
stoch_k_key = STOCHASTIC_K + '_' + str(stoch_k_period) + '_' + str(stoch_d_period) + '_' + str(stoch_smooth_k)
stoch_d_key = STOCHASTIC_D + '_' + str(stoch_k_period) + '_' + str(stoch_d_period) + '_' + str(stoch_smooth_k)


class Adrastea(TradingStrategy):
    """
    Implementazione concreta della strategia di trading.
    """

    def __init__(self, broker: BrokerAPI, config: ConfigReader, market_state_notifier: MarketStateNotifier, candle_provider: CandleProvider):
        self.broker = broker
        self.config = config
        self.market_state_notifier = market_state_notifier
        self.candle_provider = candle_provider
        self.execution_lock = market_state_notifier.execution_lock  # Utilizza lo stesso lock per sincronizzare
        # Internal state
        self.initialized = False
        self.prev_condition_candle = None
        self.cur_condition_candle = None
        self.prev_state = None
        self.cur_state = None
        self.should_enter = False

    @exception_handler
    async def bootstrap(self):
        """
        Metodo chiamato per inizializzare la strategia prima di processare le prime candele.
        Ad esempio, carica candele storiche per inizializzare indicatori.
        """
        async with self.execution_lock:
            log_info("Bootstrap: inizializzazione della strategia.")
            timeframe = self.config.get_timeframe()
            symbol = self.config.get_symbol()
            trading_direction = self.config.get_trading_direction()

            heikin_ashi_candles_buffer = int(1000 * timeframe.to_hours())
            bootstrap_rates_count = int(500 * (1 / timeframe.to_hours()))
            tot_candles_count = heikin_ashi_candles_buffer + bootstrap_rates_count + self.get_minimum_frames_count()

            try:

                bootstrap_candles_logger = CandlesLogger(symbol, timeframe, trading_direction, custom_name='bootstrap')

                # Recupera le ultime 100 candele storiche
                candles = await execute_broker_call(
                    self.broker.get_last_candles,
                    self.config.get_symbol(),
                    self.config.get_timeframe(),
                    tot_candles_count
                )

                await self.calculate_indicators(candles)

                first_index = heikin_ashi_candles_buffer + self.get_minimum_frames_count() - 1
                last_index = tot_candles_count - 1  # Exclude last closed candle because it will be analysed in the first live loop, range is exclusive

                for i in range(first_index, last_index):
                    log_info(f"Bootstrap frame {i + 1}")
                    log_info(f"Candle: {describe_candle(candles.iloc[i])}")

                    log_debug("Checking signals for bootstrap frame.")

                    bootstrap_candles_logger.add_candle(candles.iloc[i])
                    self.should_enter, self.prev_state, self.cur_state, self.prev_condition_candle, self.cur_condition_candle = self.check_signals(rates=candles,
                                                                                                                                                   i=i,
                                                                                                                                                   timeframe=timeframe,
                                                                                                                                                   symbol=symbol,
                                                                                                                                                   trading_direction=trading_direction,
                                                                                                                                                   state=self.cur_state,
                                                                                                                                                   last_condition_candle=self.cur_condition_candle)

                log_debug(f"Finished bootstrap process with state={self.cur_state} and last_condition_candle={describe_candle(self.cur_condition_candle)}")

                # Qui puoi aggiungere logica per inizializzare indicatori o altri componenti
                log_info(f"Recuperate {len(candles)} candele storiche per inizializzare la strategia.")
                self.initialized = True
            except Exception as e:
                log_error(f"Errore nel bootstrap della strategia: {e}")

                # Decidi come gestire l'errore: interrompere, continuare, ecc.
                # Per ora, imposta initialized a False
                self.initialized = False

    def get_minimum_frames_count(self):
        return max(supertrend_fast_period,
                   supertrend_fast_multiplier,
                   supertrend_slow_period,
                   supertrend_slow_multiplier) + 1

    @exception_handler
    async def on_new_candle(self, candle: dict):
        async with self.execution_lock:
            log_info(f"Nuova candela ricevuta: {candle}")

    @exception_handler
    async def on_market_status_change(self, is_open: bool, closing_time: float, opening_time: float):
        async with self.execution_lock:
            log_info(f"Stato del mercato cambiato: aperto={is_open}, chiusura={closing_time}, apertura={opening_time}")

    @exception_handler
    async def on_deal_closed(self, deal_info: dict):
        async with self.execution_lock:
            log_info(f"Deal chiuso: {deal_info}")

    @exception_handler
    async def on_economic_event(self, event_info: dict):
        async with self.execution_lock:
            log_info(f"Economic event occurred: {event_info}")

    async def calculate_indicators(self, rates):
        # Convert candlestick to Heikin Ashi
        await self.heikin_ashi_values(rates, self.config.get_symbol())

        # Calculate indicators
        supertrend(supertrend_fast_period, supertrend_fast_multiplier, rates)
        supertrend(supertrend_slow_period, supertrend_slow_multiplier, rates)
        stochastic(stoch_k_period, stoch_d_period, stoch_smooth_k, rates)
        average_true_range(5, rates)
        average_true_range(2, rates)

        return rates

    async def heikin_ashi_values(self, df, symbol):
        # Ensure df is a DataFrame with the necessary columns
        if not isinstance(df, pd.DataFrame) or not {'open', 'high', 'low', 'close'}.issubset(df.columns):
            raise ValueError("Input must be a DataFrame with 'open', 'high', 'low', and 'close' columns.")

        # Get the symbol's point precision (e.g., 0.01, 0.0001)
        symbol_info: SymbolInfo = await execute_broker_call(
            self.broker.get_market_info,
            self.config.get_symbol()
        )

        # Calculate HA_close without rounding
        df['HA_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4

        # Initialize the first HA_open as the average of the first open and close
        ha_open = [(df['open'][0] + df['close'][0]) / 2]

        # Calculate subsequent HA_open values without rounding
        for i in range(1, len(df)):
            ha_open.append((ha_open[i - 1] + df['HA_close'].iloc[i - 1]) / 2)

        # Add the calculated HA_open values to the DataFrame (without rounding)
        df['HA_open'] = pd.Series(ha_open, index=df.index)

        # Calculate HA_high and HA_low without rounding
        df['HA_high'] = df[['HA_open', 'HA_close', 'high']].max(axis=1)
        df['HA_low'] = df[['HA_open', 'HA_close', 'low']].min(axis=1)

        # Now, round all final Heikin-Ashi values to the appropriate precision
        df['HA_open'] = self.round_to_point(df['HA_open'], symbol_info.point)
        df['HA_close'] = self.round_to_point(df['HA_close'], symbol_info.point)
        df['HA_high'] = self.round_to_point(df['HA_high'], symbol_info.point)
        df['HA_low'] = self.round_to_point(df['HA_low'], symbol_info.point)

        return df

    def round_to_point(self, value, point) -> np.ndarray or float:
        # Calculate the number of decimal places to round based on point
        num_decimal_places = abs(int(math.log10(point)))

        # If the input is a Pandas Series or numpy array, apply rounding to each element
        if isinstance(value, pd.Series) or isinstance(value, np.ndarray):
            return value.round(decimals=num_decimal_places)
        else:  # Otherwise, assume it's a single number and round it directly
            return round(value, num_decimal_places)

    def check_signals(self, rates: Series, i: int, timeframe: Timeframe, symbol: str, trading_direction: TradingDirection, state=None, last_condition_candle=None) -> (bool, int, int, Series):
        """
        Analyzes market conditions to determine the appropriateness of entering a trade based on a set of predefined rules.

        Parameters:
        - rates (dict): A dictionary containing market data rates, with keys for time, close, and other indicator values.
        - i (int): The current index in the rates dictionary to check signals for. In live mode is always the last candle index.
        - params (dict): A dictionary containing parameters such as symbol, timeframe, and trading direction.
        - state (int, optional): The current state of the trading conditions, used for tracking across multiple calls. Defaults to None.
        - last_condition_candle (Dataframe, optional): The last candle where a trading condition was met. Defaults to None.
        - notifications (bool, optional): Indicates whether to send notifications when conditions are met. Defaults to False.

        Returns:
        - should_enter (bool): Indicates whether the conditions suggest entering a trade.
        - state_cur (int): The updated state after checking the current conditions.
        - last_condition_candle (Dataframe): The candle where a trading condition was met.
        - data (dict): A dictionary of the current rates and indicator values used for decision making.
        - events (list): A list of log entries detailing which conditions were met or unmet.

        The function evaluates a series of trading conditions based on market direction (long or short), price movements, and indicators like Supertrend and Stochastic. It progresses through states as conditions are met, logging each step, and ultimately determines whether the strategy's criteria for entering a trade are satisfied.
        """

        state_cur = 0 if state is None else state
        state_prev = state_cur
        prev_condition_candle = last_condition_candle
        should_enter = False

        cur_candle = rates.iloc[i]
        close = cur_candle['HA_close']

        supert_fast_prev = rates[supertrend_fast_key][i - 1]
        supert_slow_prev = rates[supertrend_slow_key][i - 1]
        supert_fast_cur = rates[supertrend_fast_key][i]
        stoch_k_cur = rates[stoch_k_key][i]
        stoch_d_cur = rates[stoch_d_key][i]

        is_long = trading_direction == TradingDirection.LONG
        is_short = trading_direction == TradingDirection.SHORT

        # The conversion of the timestamp to an integer must be done dynamically because it can change following a state transition.
        # Therefore, it is necessary that the comparison is always made with the updated timestamp variable to avoid incorrectly verifying certain conditions.
        def int_time_open(candle: Series):
            return -1 if candle is None else int(candle['time_open'].timestamp())

        def int_time_close(candle: Series):
            return -1 if candle is None else int(candle['time_close'].timestamp())

        # Condition 1 must always be checked in every iteration and must be valid. If it is no longer valid, the process should not continue.

        # Condition 1
        can_check_condition_1 = state_cur >= 0 and int_time_open(cur_candle) >= int_time_open(last_condition_candle)
        log_debug(f"Can check condition 1: {can_check_condition_1}")
        if can_check_condition_1:
            log_debug(f"Before evaluating condition 1: state_cur={state_cur}, last_condition_candle={describe_candle(last_condition_candle)}")
            condition_1_met = (is_long and close >= supert_slow_prev) or (is_short and close < supert_slow_prev)
            if condition_1_met:
                if state_cur == 0:  # log only if is the first time
                    state_prev, state_cur, prev_condition_candle, last_condition_candle = self.update_state(cur_candle, prev_condition_candle, last_condition_candle, 1, state_cur)
            else:
                if state_cur >= 1:  # regress only if the condition has already been met
                    state_prev, state_cur, prev_condition_candle, last_condition_candle = self.update_state(cur_candle, prev_condition_candle, last_condition_candle, 0, state_cur)
            log_debug(f"After evaluating condition 1: state_cur={state_cur}, last_condition_candle={describe_candle(last_condition_candle)}")

        # Condition 2
        can_check_condition_2 = state_cur >= 1 and int_time_open(cur_candle) > int_time_open(last_condition_candle)
        log_debug(f"Can check condition 2: {can_check_condition_2}")
        if can_check_condition_2:
            log_debug(f"Before evaluating condition 2: state_cur={state_cur}, last_condition_candle={describe_candle(last_condition_candle)}")
            condition_2_met = (is_long and close <= supert_fast_cur) or (is_short and close > supert_fast_cur)
            if condition_2_met:
                if state_cur == 1:  # notify only if the condition has already been met
                    state_prev, state_cur, prev_condition_candle, last_condition_candle = self.update_state(cur_candle, prev_condition_candle, last_condition_candle, 2, state_cur)
            log_debug(f"After evaluating condition 2: state_cur={state_cur}, last_condition_candle={describe_candle(last_condition_candle)}")

        # Condition 3
        can_check_condition_3 = state_cur >= 2 and int_time_open(cur_candle) >= int_time_open(last_condition_candle)
        log_debug(f"Can check condition 3: {can_check_condition_3}")
        if can_check_condition_3:
            log_debug(f"Before evaluating condition 3: state_cur={state_cur}, last_condition_candle={describe_candle(last_condition_candle)}")
            condition_3_met = (is_long and close >= supert_fast_prev) or (is_short and close < supert_fast_prev)
            if condition_3_met:
                if state_cur == 2:  # log only if is the first time
                    state_prev, state_cur, prev_condition_candle, last_condition_candle = self.update_state(cur_candle, prev_condition_candle, last_condition_candle, 3, state_cur)
            else:
                if state_cur >= 3:  # regress only if the condition has already been met
                    if is_long:
                        state_prev, state_cur, prev_condition_candle, last_condition_candle = self.update_state(cur_candle, prev_condition_candle, last_condition_candle, 2, state_cur)
            log_debug(f"After evaluating condition 3: state_cur={state_cur}, last_condition_candle={describe_candle(last_condition_candle)}")

        # Condition 4 (Stochastic)
        can_check_condition_4 = state_cur >= 3
        log_debug(f"Can check condition 4: {can_check_condition_4}")
        if can_check_condition_4:
            log_debug(f"Before evaluating condition 4: state_cur={state_cur}, last_condition_candle={describe_candle(last_condition_candle)}")
            condition_4_met = ((is_long and stoch_k_cur > stoch_d_cur and stoch_d_cur < 50) or (is_short and stoch_k_cur < stoch_d_cur and stoch_d_cur > 50))
            if condition_4_met:
                if state_cur == 3:  # notify only if the condition has already been met
                    state_prev, state_cur, prev_condition_candle, last_condition_candle = self.update_state(cur_candle, prev_condition_candle, last_condition_candle, 4, state_cur)
            log_debug(f"After evaluating condition 4: state_cur={state_cur}, last_condition_candle={describe_candle(last_condition_candle)}")

        # Condition 5 (Final condition for entry)
        time_tolerance = 30
        # Check if state is 4 and cur_candle is indeed the candle right after last_condition_candle
        can_check_condition_5 = (
                state_cur == 4 and
                int(cur_candle['time_open'].timestamp()) > int(last_condition_candle['time_open'].timestamp())
        )
        log_debug(f"Can check condition 5: {can_check_condition_5}")
        if can_check_condition_5:
            # Verify that the current candle is exactly the one after the last condition 4 candle, with a tolerance margin between the expected time and a small delay.
            lower_bound = int_time_close(last_condition_candle)
            upper_bound = lower_bound + time_tolerance
            condition_5_met = lower_bound <= int_time_open(cur_candle) <= upper_bound
            log_debug(f"Lower Bound: {lower_bound}, Upper Bound: {upper_bound}, Current Candle Time: {int_time_open(cur_candle)}")
            # For testing purposes: If you want to trigger an entry signal every time the bot is launched after condition 4 has been matched and is still active, simply uncomment the following line.
            # condition_5_met = to_int(cur_candle_time) >= lower_bound
            if condition_5_met:
                log_debug(f"Before evaluating condition 5: state_cur={state_cur}, last_condition_candle={describe_candle(last_condition_candle)}")
                state_prev, state_cur, prev_condition_candle, last_condition_candle = self.update_state(cur_candle, prev_condition_candle, last_condition_candle, 5, state_cur)
                should_enter = True
            log_debug(f"After evaluating condition 5: state_cur={state_cur}, last_condition_candle={describe_candle(last_condition_candle)}")

        log_debug(f"Returning: should_enter={should_enter}, state_cur={state_cur}, last_condition_candle={describe_candle(last_condition_candle)}")
        return should_enter, state_prev, state_cur, prev_condition_candle, last_condition_candle

    def update_state(self, cur_candle: Series, prev_condition_candle: Series, last_condition_candle: Series, new_state: int, old_state: int) -> (bool, int, Series):
        """
        Updates the state and the last condition-matching candle time based on the current time,
        the previous condition-matching candle time, the new state, and the old state.

        Args:
            cur_candle (Series): The current candle.
            last_condition_candle (Series): The last condition-matching candle.
            new_state (int): The new state to be updated.
            old_state (int): The previous state.

        Raises:
            ValueError: If cur_time is unexpectedly earlier than the last condition candle open time.

        Returns:
            tuple: A tuple containing the previous state (int), the updated state (int),
                   and the last condition-matching candle time (Timestamp).
        """

        # Retain the highest state as the most recent condition met.
        ret_state = old_state
        if new_state != old_state:
            # Log a state change if there's a transition.
            log_debug(f"State change from {old_state} -> {new_state}")
            ret_state = new_state

        cur_candle_time_int = int(cur_candle['time_open'].timestamp())
        last_condition_candle_time = None if last_condition_candle is None else last_condition_candle['time_open']
        last_condition_candle_time_int = -1 if last_condition_candle_time is None else int(last_condition_candle_time.timestamp())

        # Raise an exception if cur_candle is unexpectedly earlier than last_condition_candle.
        if last_condition_candle_time_int is not None and cur_candle_time_int < last_condition_candle_time_int:
            raise ValueError(f"Strategy current candle time {cur_candle['time_open']} cannot be prior to last condition-matching "
                             f"candle time {last_condition_candle['time_open']}.")

        # Only update last_condition_candle for first-time matches in new candles,
        # avoiding unnecessary updates when conditions reconfirm without new occurrences.
        ret_candle = last_condition_candle

        # Update last_condition_candle whenever there is a state change, regardless of whether it's on the same candle or a different candle.
        if new_state != old_state:
            # Log candle index update for new condition matches.
            log_debug(f"Strategy candle time change from {last_condition_candle_time} -> {cur_candle['time_open']}")
            ret_candle = cur_candle
            prev_condition_candle = last_condition_candle
        else:
            log_debug(
                f"update_state function called but no state change detected. Current state remains {new_state}. Called with candle time {cur_candle['time_open']}. Previous state was {old_state}.")

        # Returns the previous state, current updated state, and the last condition-matching candle index.
        return old_state, ret_state, prev_condition_candle, ret_candle
