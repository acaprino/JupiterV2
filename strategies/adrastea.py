# strategies/my_strategy.py

import math

import numpy as np
import pandas as pd
from pandas import Series

from csv_loggers.candles_logger import CandlesLogger
from csv_loggers.strategy_events_logger import StrategyEventsLogger
from datao import TradeOrder
from datao.SymbolInfo import SymbolInfo
from providers.candle_provider import CandleProvider
from providers.market_state_notifier import MarketStateNotifier
from strategies.base_strategy import TradingStrategy
from strategies.indicators import supertrend, stochastic, average_true_range
from utils.async_executor import execute_broker_call
from utils.config import ConfigReader
from utils.enums import Indicators, Timeframe, TradingDirection, OpType, NotificationLevel
from utils.error_handler import exception_handler
from brokers.broker_interface import BrokerAPI

from utils.logger import log_info, log_error, log_debug, log_warning
from utils.mongo_db import MongoDB
from utils.telegram_lib import TelegramBotWrapper
from utils.utils import describe_candle, now_utc, round_to_step, round_to_point

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

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
        self.heikin_ashi_candles_buffer = int(1000 * config.get_timeframe().to_hours())
        self.telegram = TelegramBotWrapper(config.get_telegram_token())

        self.telegram.start()
        self.telegram.add_command_callback_handler(self.signal_confirmation_handler)

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

            bootstrap_rates_count = int(500 * (1 / timeframe.to_hours()))
            tot_candles_count = self.heikin_ashi_candles_buffer + bootstrap_rates_count + self.get_minimum_frames_count()

            try:

                bootstrap_candles_logger = CandlesLogger(symbol, timeframe, trading_direction, custom_name='bootstrap')

                candles = await execute_broker_call(
                    self.broker.get_last_candles,
                    self.config.get_symbol(),
                    self.config.get_timeframe(),
                    tot_candles_count
                )

                await self.calculate_indicators(candles)

                first_index = self.heikin_ashi_candles_buffer + self.get_minimum_frames_count() - 1
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

                self.send_message_with_details(f"🔄 Bootstrapping Complete - <b>Bot Ready for Trading</b>")

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

    def notify_state_change(self, state_prev, state_cur, rates, i, params, last_condition_candle, should_enter):
        symbol, timeframe, trading_direction, bot_mode = (
            params['symbol'], params['timeframe'], params['trading_direction'], params['bot_mode']
        )

        events_logger = StrategyEventsLogger(symbol, timeframe, trading_direction, bot_mode)
        cur_candle = rates.iloc[i]
        close = cur_candle['HA_close']

        # Extract required indicator values from the candles
        supert_fast_prev = rates[supertrend_fast_key][i - 1]
        supert_slow_prev = rates[supertrend_slow_key][i - 1]
        supert_fast_cur = rates[supertrend_fast_key][i]
        supert_slow_cur = rates[supertrend_slow_key][i]
        stoch_k_cur = rates[stoch_k_key][i]
        stoch_d_cur = rates[stoch_d_key][i]

        is_long = trading_direction == TradingDirection.LONG
        is_short = trading_direction == TradingDirection.SHORT

        def notify_event(event):
            log_debug(event)
            events_logger.add_event(
                cur_candle['time_open'], cur_candle['time_close'], close,
                state_prev, state_cur, event,
                supert_fast_prev, supert_slow_prev, supert_fast_cur, supert_slow_cur,
                stoch_k_cur, stoch_d_cur
            )
            self.send_message_with_details(event)

        # Handle state transitions and trigger notifications
        if state_cur == 1 and state_prev == 0:
            if is_long:
                notify_event(f"1️⃣ ✅ <b>Condition 1 matched</b>: Price {close} is above the slow Supertrend level {supert_slow_prev}, validating long position.")
            elif is_short:
                notify_event(f"1️⃣ ✅ <b>Condition 1 matched</b>: Price {close} is below the slow Supertrend level {supert_slow_prev}, validating short position.")
        elif state_cur == 0 and state_prev == 1:
            if is_long:
                notify_event(f"1️⃣ ❌ <b>Condition 1 regressed</b>: Price {close} is now below the slow Supertrend level {supert_slow_prev}, invalidating the long position.")
            elif is_short:
                notify_event(f"1️⃣ ❌ <b>Condition 1 regressed</b>: Price {close} is now above the slow Supertrend level {supert_slow_prev}, invalidating the short position.")

        elif state_cur == 2 and state_prev == 1:
            if is_long:
                notify_event(f"2️⃣ ✅ <b>Condition 2 matched</b>: Price {close} is below the fast Supertrend level {supert_fast_cur}, valid for long trade.")
            elif is_short:
                notify_event(f"2️⃣ ✅ <b>Condition 2 matched</b>: Price {close} is above the fast Supertrend level {supert_fast_cur}, valid for short trade.")
        elif state_cur == 1 and state_prev == 2:
            if is_long:
                notify_event(f"2️⃣ ❌ <b>Condition 2 regressed</b>: Price {close} failed to remain below the fast Supertrend level {supert_fast_cur}.")
            elif is_short:
                notify_event(f"2️⃣ ❌ <b>Condition 2 regressed</b>: Price {close} failed to remain above the fast Supertrend level {supert_fast_cur}.")

        elif state_cur == 3 and state_prev == 2:
            if is_long:
                notify_event(f"3️⃣ ✅ <b>Condition 3 matched</b>: Price {close} remains above the fast Supertrend level {supert_fast_prev}, confirming long trade.")
            elif is_short:
                notify_event(f"3️⃣ ✅ <b>Condition 3 matched</b>: Price {close} remains below the fast Supertrend level {supert_fast_prev}, confirming short trade.")
        elif state_cur == 2 and state_prev == 3:
            if is_long:
                notify_event(f"3️⃣ ❌ <b>Condition 3 regressed</b>: Price {close} failed to maintain above the fast Supertrend level {supert_fast_prev}, invalidating the long trade.")
            elif is_short:
                notify_event(f"3️⃣ ❌ <b>Condition 3 regressed</b>: Price {close} failed to maintain below the fast Supertrend level {supert_fast_prev}, invalidating the short trade.")

        elif state_cur == 4 and state_prev == 3:
            if is_long:
                notify_event(f"4️⃣ ✅ <b>Condition 4 matched</b>: Stochastic K ({stoch_k_cur}) crossed above D ({stoch_d_cur}) and D is below 50, confirming bullish momentum.")
            elif is_short:
                notify_event(f"4️⃣ ✅ <b>Condition 4 matched</b>: Stochastic K ({stoch_k_cur}) crossed below D ({stoch_d_cur}) and D is above 50, confirming bearish momentum.")

        elif state_cur == 3 and state_prev == 4:
            if is_long:
                notify_event(f"4️⃣ ❌ <b>Condition 4 regressed</b>: Stochastic K ({stoch_k_cur}) is no longer above D ({stoch_d_cur}).")
            elif is_short:
                notify_event(f"4️⃣ ❌ <b>Condition 4 regressed</b>: Stochastic K ({stoch_k_cur}) is no longer below D ({stoch_d_cur}).")

        if should_enter:
            t_open = last_condition_candle['time_open'].strftime('%H:%M')
            t_close = last_condition_candle['time_close'].strftime('%H:%M')

            trading_opportunity_message = (f"🚀 <b>Alert!</b> A new trading opportunity has been identified on frame {t_open} - {t_close}.\n\n"
                                           f"🔔 Would you like to confirm the placement of this order?\n\n"
                                           "Select an option to place the order or ignore this signal (by default, the signal will be <b>ignored</b> if no selection is made).")
            reply_markup = self.get_signal_confirmation_dialog(last_condition_candle)

            self.send_message_with_details(trading_opportunity_message, reply_markup=reply_markup)

            dir_str = "long" if is_long else "short"
            cur_candle_time = f"{cur_candle['time_open'].strftime('%H:%M')} - {cur_candle['time_close'].strftime('%H:%M')}"
            last_condition_candle_time = f"{last_condition_candle['time_open'].strftime('%H:%M')} - {last_condition_candle['time_close'].strftime('%H:%M')}"
            notify_event(
                f"5️⃣ ✅ <b>Condition 5 matched</b>: Final signal generated for {dir_str} trade. The current candle time {cur_candle_time} is from the candle following the last condition candle: {last_condition_candle_time}")

    @exception_handler
    async def on_new_candle(self, candle: dict):
        async with self.execution_lock:
            log_info(f"Nuova candela ricevuta: {candle}")

            timeframe = self.config.get_timeframe()
            symbol = self.config.get_symbol()
            trading_direction = self.config.get_trading_direction()

            candles_count = self.heikin_ashi_candles_buffer + self.get_minimum_frames_count()

            candles = await execute_broker_call(
                self.broker.get_last_candles,
                self.config.get_symbol(),
                self.config.get_timeframe(),
                candles_count
            )
            await self.calculate_indicators(candles)

            last_candle = candles.iloc[-1]
            log_info(f"Candle: {describe_candle(last_candle)}")

            log_debug(f"Checking signals - Start: State={self.cur_state}, Number of Candles={len(candles)}, Timeframe={timeframe}, Symbol={symbol}, Trading Direction={trading_direction}")

            log_debug("Checking for trading signals.")
            self.should_enter, self.prev_state, self.cur_state, self.prev_condition_candle, self.cur_condition_candle = self.check_signals(rates=candles,
                                                                                                                                           i=len(candles) - 1,
                                                                                                                                           timeframe=timeframe,
                                                                                                                                           symbol=symbol,
                                                                                                                                           trading_direction=trading_direction,
                                                                                                                                           state=self.cur_state,
                                                                                                                                           last_condition_candle=self.cur_condition_candle)

            log_debug(f"Checking signals - Results: should_enter={self.should_enter}, State={self.cur_state}, last_condition_candle={describe_candle(self.cur_condition_candle)}")

            last_candle_time_str = f"{last_candle['time_open'].strftime('%Y-%m-%d %H:%M:%S')} - {last_candle['time_close'].strftime('%Y-%m-%d %H:%M:%S')}"
            # log_candle(last_candle)

            # notify_state_change(state_prev, state, candles, len(candles) - 1, params, last_condition_candle, should_enter)

            if self.should_enter:
                log_debug("Condition satisfied for placing an order. Sending entry signal notification.")
                order_type_enter = OpType.BUY if trading_direction == TradingDirection.LONG else OpType.SELL
                log_debug(f"Determined order type as {order_type_enter.label}.")
                # send_entry_signal_notification(datetime.now(), order_type_enter)
                log_info(f"Condition satisfied for candle {last_candle_time_str}, entry signal sent.")
                log_debug("Processing order placement due to trading signal.")

                if self.check_signal_confirmation_and_place_order(self.prev_condition_candle):
                    order = self.prepare_order_to_place(last_candle)
                    await self.place_order(order)
            else:
                log_info(f"No condition satisfied for candle {last_candle_time_str}")

            try:
                log_debug("Adding last candle to buffers and logging.")
                live_candles_logger = CandlesLogger(symbol, timeframe, trading_direction, custom_name='live')
                live_candles_logger.add_candle(last_candle)
            except Exception as e:
                log_error(f"Error while generating analysis data: {e}")

    @exception_handler
    async def prepare_order_to_place(self, cur_candle: Series) -> TradeOrder:
        log_debug("[place_order] Starting the function.")

        symbol = self.config.get_symbol()
        trading_direction = self.config.get_trading_direction()
        order_type_enter = OpType.BUY if trading_direction == TradingDirection.LONG else OpType.SELL
        timeframe = self.config.get_timeframe()
        magic_number = self.config.get_bot_magic_number()

        symbol_info = await execute_broker_call(
            self.broker.get_market_info,
            symbol
        )

        if symbol_info is None:
            log_error("[place_order] Symbol info not found.")
            self.send_message_with_details("🚫 Symbol info not found for placing the order.")
            raise Exception(f"Symbol info {symbol} not found.")

        point = symbol_info.point
        volume_min = symbol_info.volume_min

        price = self.get_order_price(cur_candle, point, trading_direction)
        sl = self.get_stop_loss(cur_candle, point, trading_direction)
        tp = self.get_take_profit(cur_candle, price, point, timeframe, trading_direction)

        account_balance = await execute_broker_call(
            self.broker.get_account_balance
        )
        leverage = await execute_broker_call(
            self.broker.get_account_leverage
        )

        volume = self.get_volume(account_balance, symbol, leverage, price)

        log_info(f"[place_order] Account balance retrieved: {account_balance}, Leverage obtained: {leverage}, Calculated volume for the order on {symbol} at price {price}: {volume}")

        if volume < volume_min:
            log_warning(f"[place_order] Volume of {volume} is less than minimum of {volume_min}")
            self.send_message_with_details(f"❗ Volume of {volume} is less than the minimum of {volume_min} for {symbol}.")
            return False

        return TradeOrder(order_type=order_type_enter,
                          symbol=symbol,
                          order_price=price,
                          volume=volume,
                          sl=sl,
                          tp=tp,
                          comment="bot-enter-signal",
                          filling_mode=None,
                          magic_number=magic_number)

    def get_stop_loss(self, cur_candle: Series, symbol_point, trading_direction):
        # Ensure 'supertrend_slow_key' is defined or passed to this function
        supertrend_slow = cur_candle[supertrend_slow_key]

        # Calculate stop loss adjustment factor
        adjustment_factor = 0.003 / 100

        # Adjust stop loss based on trading direction
        if trading_direction == TradingDirection.LONG:
            sl = supertrend_slow - (supertrend_slow * adjustment_factor)
        elif trading_direction == TradingDirection.SHORT:
            sl = supertrend_slow + (supertrend_slow * adjustment_factor)
        else:
            raise ValueError("Invalid trading direction")

        # Return the stop loss rounded to the symbol's point value
        return round_to_point(sl, symbol_point)

    def get_take_profit(self, cur_candle: Series, order_price, symbol_point, timeframe, trading_direction):
        atr_periods = 5 if trading_direction == TradingDirection.SHORT else 2
        atr_key = f'ATR_{atr_periods}'
        atr = cur_candle[atr_key]
        multiplier = 1 if timeframe == Timeframe.M30 else 2
        multiplier = multiplier * -1 if trading_direction == TradingDirection.SHORT else multiplier
        take_profit_price = order_price + (multiplier * atr)

        # Return the take profit price rounded to the symbol's point value
        return round_to_point(take_profit_price, symbol_point)

    def get_order_price(self, cur_candle: Series, symbol_point, trading_direction) -> float:
        """
        This function calculates the order price for a trade based on the trading direction and a small adjustment factor.

        Parameters:
        - candle (dict): A dictionary containing the OHLC (Open, High, Low, Close) values for a specific time period.
        - symbol_point (float): The smallest price change for the trading symbol.
        - trading_direction (TradingDirection): An enum value indicating the trading direction (LONG or SHORT).

        Returns:
        - float: The adjusted order price, rounded to the symbol's point value.

        The function first determines the base price based on the trading direction. If the direction is LONG, the base price is the high price of the Heikin Ashi candle; if the direction is SHORT, the base price is the low price of the Heikin Ashi candle.

        Then, it calculates a small adjustment to the base price. The adjustment is a fixed percentage (0.003%) of the base price. The adjustment is added to the base price for LONG trades and subtracted from the base price for SHORT trades.

        Finally, the function returns the adjusted price, rounded to the symbol's point value.
        """
        # Determine the base price based on trading direction.
        base_price_key = 'HA_high' if trading_direction == TradingDirection.LONG else 'HA_low'
        base_price = cur_candle[base_price_key]

        # Calculate the price adjustment.
        adjustment_factor = 0.003 / 100
        adjustment = adjustment_factor * base_price
        adjusted_price = base_price + adjustment if trading_direction == TradingDirection.LONG else base_price - adjustment

        # Return the price rounded to the symbol's point value.
        return self.round_to_point(adjusted_price, symbol_point)

    def get_volume(self, account_balance, symbol_info, leverage, entry_price):
        """
        Calculate the lot size based on a fixed percentage of the account balance, adjusted for leverage,
        and ensuring compliance with the broker's lot size constraints.
        """
        # Calculate the capital to be invested in the trade
        capital_to_invest = account_balance * 0.20 * leverage

        # Calculate the lot size directly (volume in lotti)
        lot_size = capital_to_invest / (entry_price * symbol_info.trade_contract_size)

        # Adjust the lot size to meet the broker's minimum and maximum requirements
        adjusted_lot_size = max(symbol_info.volume_min, min(symbol_info.volume_max, round_to_step(lot_size, symbol_info.volume_step)))

        # Log warnings if necessary
        if lot_size < symbol_info.volume_min:
            log_warning(f"Adjusted lot size to {adjusted_lot_size} to meet minimum requirement of {symbol_info.volume_min} for {symbol_info.name}.")
        if lot_size > symbol_info.volume_max:
            log_warning(f"Adjusted lot size to {adjusted_lot_size} to meet maximum requirement of {symbol_info.volume_max} for {symbol_info.name}.")

        return adjusted_lot_size

    async def place_order(self, order: TradeOrder) -> bool:

        log_info(f"[place_order] Placing order: {order}")

        response = self.broker.place_order(order)

        log_debug(f"[place_order] Result of order placement: {response.success}")

        log_message = f"{response.server_response_code} - {response.server_response_message}"

        if response.success:
            log_info(f"[place_order] Order successfully placed. Platform log: \"{log_message}\"")
            self.send_message_with_details(
                f"✅ <b>Order successfully placed with Deal ID {response.deal}:</b>\n\n"
                f"{order}"
            )
        else:
            log_error("[place_order] Error while placing the order.")
            self.send_message_with_details(f"🚫 <b>Error while placing the order:</b>\n\n"
                                           f"{order}\n"
                                           f"Platform log: \"{log_message}\"")

        return response.success

    def check_signal_confirmation_and_place_order(self, signal_candle):
        bot_name = ConfigReader().get_bot_name()
        magic = ConfigReader().get_bot_magic_number()
        open_dt = signal_candle['time_open'].strftime('%H:%M')
        close_dt = signal_candle['time_close'].strftime('%H:%M')

        signal_id = {
            "bot_name": bot_name,
            "magic_number": magic,
            "open_time": open_dt,
            "close_time": close_dt
        }

        # Retrieve the signal confirmation from MongoDB
        signal_confirmation = MongoDB().find_one("signals_confirmation", signal_id)

        if not signal_confirmation:
            log_error(f"No confirmation found for signal with open time {open_dt} and close time {close_dt}")
            self.send_message_with_details(f"🚫 No confirmation found for signal with open time {open_dt} and close time {close_dt}. Not placing order by default.")
            return False

        confirmed = signal_confirmation.get("confirmation", False)
        user_username = signal_confirmation.get("user_name", "Unknown User")

        if confirmed:
            log_info(f"Signal for candle {open_dt} - {close_dt} confirmed by {user_username}. Placing order.")
            self.send_message_with_details(f"✅ Signal for candle {open_dt} - {close_dt} confirmed by {user_username}. Placing order.")
        else:
            log_info(f"Signal for candle {open_dt} - {close_dt} not confirmed by {user_username}. Not placing order.")
            self.send_message_with_details(f"🚫 Signal for candle {open_dt} - {close_dt} not confirmed by {user_username}. Not placing order.")

        return confirmed

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

    def send_message(self, message, level=NotificationLevel.DEFAULT, reply_markup=None):
        config = ConfigReader()
        if not config.get_telegram_active():
            return

        # Check if the notification level is high enough to send
        send_notification = level.value >= config.get_telegram_notification_level().value
        if not send_notification:
            return

        t_token = config.get_telegram_token()
        t_chat_ids = config.get_telegram_chat_ids()
        for chat_id in t_chat_ids:
            TelegramBotWrapper(t_token).send_message(chat_id, message, reply_markup=reply_markup)

    def send_message_with_details(self, message, level=NotificationLevel.DEFAULT, reply_markup=None):
        symbol, bot_name, timeframe, trading_direction = (self.config.get_symbol(),
                                                          self.config.get_bot_name(),
                                                          self.config.get_timeframe(),
                                                          self.config.get_trading_direction())
        direction_emoji = "⬆️" if trading_direction.name == "LONG" else "⬇️"
        detailed_message = (
            f"{message}\n\n"
            "<b>Details:</b>\n"
            f"🤖 <b>Bot name:</b> {bot_name}\n"
            f"💱 <b>Symbol:</b> {symbol}\n"
            f"🕒 <b>Timeframe:</b> {timeframe.name}\n"
            f"{direction_emoji} <b>Direction:</b> {trading_direction.name}"
        )
        self.send_message(detailed_message, level, reply_markup)

    async def signal_confirmation_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        await query.answer()
        log_debug(f"Callback query answered: {query}")

        # Retrieve data from callback, now in CSV format
        data = query.data.split(',')
        log_debug(f"Data retrieved from callback: {data}")
        bot_name, magic, open_dt, close_dt, confirmed_flag = data
        confirmed = confirmed_flag == '1'
        user_username = query.from_user.username if query.from_user.username else "Unknown User"
        user_id = query.from_user.id
        chat_id = update.effective_chat.id
        chat_username = update.effective_chat.username
        log_debug(
            f"Parsed data - bot_name: {bot_name}, magic: {magic}, open_dt: {open_dt}, close_dt: {close_dt}, confirmed: {confirmed}, user_username: {user_username}, user_id: {user_id}, chat_id: {chat_id}, chat_username: {chat_username}")

        # Create CSV formatted confirmation and blocking data
        csv_confirm = f"{bot_name},{magic},{open_dt},{close_dt},1"
        csv_block = f"{bot_name},{magic},{open_dt},{close_dt},0"
        log_debug(f"CSV formatted data - confirm: {csv_confirm}, block: {csv_block}")

        current_bot_name = ConfigReader().get_bot_name()
        current_magic_number = ConfigReader().get_bot_magic_number()
        log_debug(f"Current bot configuration - bot_name: {current_bot_name}, magic_number: {current_magic_number}")
        if bot_name != current_bot_name or int(magic) != current_magic_number:
            log_info(f"Ignored update for bot_name '{bot_name}' and magic number '{magic}' as they do not match the current instance '{current_bot_name}' and magic number '{current_magic_number}'.")
            return

        # Set the keyboard buttons with updated callback data
        if confirmed:
            keyboard = [
                [
                    InlineKeyboardButton("Confirmed ✔️", callback_data=csv_confirm),
                    InlineKeyboardButton("Blocked", callback_data=csv_block)
                ]
            ]
        else:
            keyboard = [
                [
                    InlineKeyboardButton("Confirm", callback_data=csv_confirm),
                    InlineKeyboardButton("Ignored ✔️", callback_data=csv_block)
                ]
            ]
        log_debug(f"Keyboard set with updated callback data: {keyboard}")

        # Update the inline keyboard
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

        # Update the database
        obj = {
            "bot_name": bot_name,
            "magic_number": int(magic),
            "open_time": open_dt,
            "close_time": close_dt,
            "confirmation": confirmed,
            "last_update_tms": now_utc(),
            "user_name": user_username,
            "user_id": user_id,
            "chat_id": chat_id,
            "chat_username": chat_username
        }
        log_debug(f"Database object to upsert: {obj}")
        MongoDB().upsert("signals_confirmation", {"bot_name": bot_name,
                                                  "magic_number": int(magic),
                                                  "open_time": open_dt,
                                                  "close_time": close_dt}, obj)
        log_debug("Database updated with new signal confirmation")

        choice_text = "✅ Confirm" if confirmed else "🚫 Ignore"
        message = f"ℹ️ Your choice to <b>{choice_text}</b> the signal for the candle from {open_dt} to {close_dt} has been successfully saved."
        self.send_message(message)
        log_debug(f"Confirmation message sent: {message}")

    def get_signal_confirmation_dialog(self, candle) -> InlineKeyboardMarkup:
        log_debug("Starting signal confirmation dialog creation")
        bot_name = ConfigReader().get_bot_name()
        magic = ConfigReader().get_bot_magic_number()

        open_dt = candle['time_open'].strftime('%H:%M')
        close_dt = candle['time_close'].strftime('%H:%M')

        csv_confirm = f"{bot_name},{magic},{open_dt},{close_dt},1"
        csv_block = f"{bot_name},{magic},{open_dt},{close_dt},0"
        log_debug(f"CSV data - confirm: {csv_confirm}, block: {csv_block}")

        keyboard = [
            [
                InlineKeyboardButton("Confirm", callback_data=csv_confirm),
                InlineKeyboardButton("Ignore", callback_data=csv_block)
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        log_debug(f"Keyboard created: {keyboard}")

        return reply_markup
