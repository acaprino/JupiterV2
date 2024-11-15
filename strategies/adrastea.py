# strategies/my_strategy.py
import asyncio
import math
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
from pandas import Series

from csv_loggers.candles_logger import CandlesLogger
from csv_loggers.strategy_events_logger import StrategyEventsLogger
from dto.Deal import Deal
from dto.Position import Position
from dto.RequestResult import RequestResult
from dto.SymbolInfo import SymbolInfo
from dto.TradeOrder import TradeOrder
from strategies.base_strategy import TradingStrategy
from strategies.indicators import supertrend, stochastic, average_true_range
from utils.async_executor import execute_broker_call
from utils.config import ConfigReader, TradingConfiguration
from utils.enums import Indicators, Timeframe, TradingDirection, OpType, NotificationLevel, OrderSource
from utils.error_handler import exception_handler
from brokers.broker_interface import BrokerAPI
from utils.bot_logger import BotLogger

from utils.mongo_db import MongoDB
from utils.telegram_lib import TelegramBotWrapper
from utils.utils_functions import describe_candle, now_utc, round_to_step, round_to_point, dt_to_unix, unix_to_datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

leverages = {
    "FOREX": [10, 30, 100],
    "METALS": [10],
    "OIL": [10],
    "CRYPTOS": [5]
}

# Indicator parameters
super_trend_fast_period = 10
super_trend_fast_multiplier = 1
super_trend_slow_period = 40
super_trend_slow_multiplier = 3

stoch_k_period = 24
stoch_d_period = 5
stoch_smooth_k = 3

# Series keys prefix
STOCHASTIC_K = Indicators.STOCHASTIC_K.name
STOCHASTIC_D = Indicators.STOCHASTIC_D.name
SUPER_TREND = Indicators.SUPERTREND.name
MOVING_AVERAGE = Indicators.MOVING_AVERAGE.name
ATR = Indicators.ATR.name

# Indicators series keys
supertrend_fast_key = SUPER_TREND + '_' + str(super_trend_fast_period) + '_' + str(super_trend_fast_multiplier)
supertrend_slow_key = SUPER_TREND + '_' + str(super_trend_slow_period) + '_' + str(super_trend_slow_multiplier)
stoch_k_key = STOCHASTIC_K + '_' + str(stoch_k_period) + '_' + str(stoch_d_period) + '_' + str(stoch_smooth_k)
stoch_d_key = STOCHASTIC_D + '_' + str(stoch_k_period) + '_' + str(stoch_d_period) + '_' + str(stoch_smooth_k)


class Adrastea(TradingStrategy):
    """
    Implementazione concreta della strategia di trading.
    """

    def __init__(self, worker_id: str, broker: BrokerAPI, database: MongoDB, config: ConfigReader, trading_config: TradingConfiguration, execution_lock: asyncio.Lock):
        self.broker = broker
        self.config = config
        self.worker_id = worker_id
        self.trading_config = trading_config
        self.logger = BotLogger.get_logger(worker_id)
        self.execution_lock = execution_lock
        # Internal state
        self.initialized = False
        self.prev_condition_candle = None
        self.cur_condition_candle = None
        self.prev_state = None
        self.cur_state = None
        self.should_enter = False
        self.database = database
        self.heikin_ashi_candles_buffer = int(1000 * trading_config.get_timeframe().to_hours())
        self.telegram = TelegramBotWrapper(token=trading_config.get_telegram_config().get_token(), worker_id=worker_id)
        self.allow_last_tick = False
        self.market_open_event = asyncio.Event()
        self.bootstrap_completed_event = asyncio.Event()
        self.live_candles_logger = CandlesLogger(trading_config.get_symbol(), trading_config.get_timeframe(), trading_config.get_trading_direction())

    @exception_handler
    async def start(self):
        self.logger.info("Starting the strategy.")
        self.telegram.start()
        self.telegram.add_callback_query_handler(handler=self.signal_confirmation_handler)

    def get_minimum_frames_count(self):
        return max(super_trend_fast_period,
                   super_trend_slow_period,
                   stoch_k_period,
                   stoch_d_period,
                   stoch_smooth_k) + 1

    def notify_state_change(self, rates, i):
        symbol, timeframe, trading_direction = (
            self.trading_config.get_symbol(), self.trading_config.get_timeframe(), self.trading_config.get_trading_direction()
        )

        events_logger = StrategyEventsLogger(symbol, timeframe, trading_direction)
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
            self.logger.debug(event)
            events_logger.add_event(
                time_open=cur_candle['time_open'],
                time_close=cur_candle['time_close'],
                close_price=close,
                state_pre=self.prev_state,
                state_cur=self.cur_state,
                message=event,
                supert_fast_prev=supert_fast_prev,
                supert_slow_prev=supert_slow_prev,
                supert_fast_cur=supert_fast_cur,
                supert_slow_cur=supert_slow_cur,
                stoch_k_cur=stoch_k_cur,
                stoch_d_cur=stoch_d_cur
            )
            self.send_message_with_details(event)

        # Handle state transitions and trigger notifications
        if self.cur_state == 1 and self.prev_state == 0:
            if is_long:
                notify_event(f"1️⃣ ✅ <b>Condition 1 matched</b>: Price {close} is above the slow Supertrend level {supert_slow_prev}, validating long position.")
            elif is_short:
                notify_event(f"1️⃣ ✅ <b>Condition 1 matched</b>: Price {close} is below the slow Supertrend level {supert_slow_prev}, validating short position.")
        elif self.cur_state == 0 and self.prev_state == 1:
            if is_long:
                notify_event(f"1️⃣ ❌ <b>Condition 1 regressed</b>: Price {close} is now below the slow Supertrend level {supert_slow_prev}, invalidating the long position.")
            elif is_short:
                notify_event(f"1️⃣ ❌ <b>Condition 1 regressed</b>: Price {close} is now above the slow Supertrend level {supert_slow_prev}, invalidating the short position.")

        elif self.cur_state == 2 and self.prev_state == 1:
            if is_long:
                notify_event(f"2️⃣ ✅ <b>Condition 2 matched</b>: Price {close} is below the fast Supertrend level {supert_fast_cur}, valid for long trade.")
            elif is_short:
                notify_event(f"2️⃣ ✅ <b>Condition 2 matched</b>: Price {close} is above the fast Supertrend level {supert_fast_cur}, valid for short trade.")
        elif self.cur_state == 1 and self.prev_state == 2:
            if is_long:
                notify_event(f"2️⃣ ❌ <b>Condition 2 regressed</b>: Price {close} failed to remain below the fast Supertrend level {supert_fast_cur}.")
            elif is_short:
                notify_event(f"2️⃣ ❌ <b>Condition 2 regressed</b>: Price {close} failed to remain above the fast Supertrend level {supert_fast_cur}.")

        elif self.cur_state == 3 and self.prev_state == 2:
            if is_long:
                notify_event(f"3️⃣ ✅ <b>Condition 3 matched</b>: Price {close} remains above the fast Supertrend level {supert_fast_prev}, confirming long trade.")
            elif is_short:
                notify_event(f"3️⃣ ✅ <b>Condition 3 matched</b>: Price {close} remains below the fast Supertrend level {supert_fast_prev}, confirming short trade.")
        elif self.cur_state == 2 and self.prev_state == 3:
            if is_long:
                notify_event(f"3️⃣ ❌ <b>Condition 3 regressed</b>: Price {close} failed to maintain above the fast Supertrend level {supert_fast_prev}, invalidating the long trade.")
            elif is_short:
                notify_event(f"3️⃣ ❌ <b>Condition 3 regressed</b>: Price {close} failed to maintain below the fast Supertrend level {supert_fast_prev}, invalidating the short trade.")

        elif self.cur_state == 4 and self.prev_state == 3:
            if is_long:
                notify_event(f"4️⃣ ✅ <b>Condition 4 matched</b>: Stochastic K ({stoch_k_cur}) crossed above D ({stoch_d_cur}) and D is below 50, confirming bullish momentum.")
            elif is_short:
                notify_event(f"4️⃣ ✅ <b>Condition 4 matched</b>: Stochastic K ({stoch_k_cur}) crossed below D ({stoch_d_cur}) and D is above 50, confirming bearish momentum.")

            t_open = self.cur_condition_candle['time_open'].strftime('%H:%M')
            t_close = self.cur_condition_candle['time_close'].strftime('%H:%M')

            trading_opportunity_message = (f"🚀 <b>Alert!</b> A new trading opportunity has been identified on frame {t_open} - {t_close}.\n\n"
                                           f"🔔 Would you like to confirm the placement of this order?\n\n"
                                           "Select an option to place the order or ignore this signal (by default, the signal will be <b>ignored</b> if no selection is made).")
            reply_markup = self.get_signal_confirmation_dialog(self.cur_condition_candle)

            self.send_message_with_details(trading_opportunity_message, reply_markup=reply_markup)

        elif self.cur_state == 3 and self.prev_state == 4:
            if is_long:
                notify_event(f"4️⃣ ❌ <b>Condition 4 regressed</b>: Stochastic K ({stoch_k_cur}) is no longer above D ({stoch_d_cur}).")
            elif is_short:
                notify_event(f"4️⃣ ❌ <b>Condition 4 regressed</b>: Stochastic K ({stoch_k_cur}) is no longer below D ({stoch_d_cur}).")

        if self.should_enter:
            dir_str = "long" if is_long else "short"
            cur_candle_time = f"{cur_candle['time_open'].strftime('%H:%M')} - {cur_candle['time_close'].strftime('%H:%M')}"
            last_condition_candle_time = f"{self.cur_condition_candle['time_open'].strftime('%H:%M')} - {self.cur_condition_candle['time_close'].strftime('%H:%M')}"
            notify_event(
                f"5️⃣ ✅ <b>Condition 5 matched</b>: Final signal generated for {dir_str} trade. The current candle time {cur_candle_time} is from the candle following the last condition candle: {last_condition_candle_time}")

    @exception_handler
    async def initialize(self):
        self.logger.info("Initializing the strategy.")

        market_is_open = await execute_broker_call(self.config.get_bot_name(), self.broker.is_market_open, self.trading_config.get_symbol())
        async with self.execution_lock:
            if not market_is_open:
                self.logger.info("Market is closed, waiting for it to open.")

        await self.market_open_event.wait()
        self.logger.info("Market is open, proceeding with strategy bootstrap.")

        async with self.execution_lock:
            timeframe = self.trading_config.get_timeframe()
            symbol = self.trading_config.get_symbol()
            trading_direction = self.trading_config.get_trading_direction()

            self.logger.debug(f"Config - Symbol: {symbol}, Timeframe: {timeframe}, Direction: {trading_direction}")

            bootstrap_rates_count = int(500 * (1 / timeframe.to_hours()))
            tot_candles_count = self.heikin_ashi_candles_buffer + bootstrap_rates_count + self.get_minimum_frames_count()

            try:
                bootstrap_candles_logger = CandlesLogger(symbol, timeframe, trading_direction, custom_name='bootstrap')

                candles = await execute_broker_call(
                    self.config.get_bot_name(),
                    self.broker.get_last_candles,
                    self.trading_config.get_symbol(),
                    self.trading_config.get_timeframe(),
                    tot_candles_count
                )

                self.logger.info("Calculating indicators on historical candles.")
                await self.calculate_indicators(candles)

                first_index = self.heikin_ashi_candles_buffer + self.get_minimum_frames_count() - 1
                last_index = tot_candles_count - 1

                for i in range(first_index, last_index):
                    self.logger.debug(f"Bootstrap frame {i + 1}, Candle data: {describe_candle(candles.iloc[i])}")

                    bootstrap_candles_logger.add_candle(candles.iloc[i])
                    self.should_enter, self.prev_state, self.cur_state, self.prev_condition_candle, self.cur_condition_candle = self.check_signals(
                        rates=candles, i=i, trading_direction=trading_direction, state=self.cur_state,
                        cur_condition_candle=self.cur_condition_candle
                    )

                self.logger.info(f"Bootstrap complete - Initial State: {self.cur_state}")

                self.send_message_with_details("🚀 Bootstrapping complete - <b>Bot ready for trading.</b>")
                self.notify_state_change(candles, last_index)
                self.initialized = True

                self.bootstrap_completed_event.set()
            except Exception as e:
                self.logger.error(f"Error in strategy bootstrap: {e}")
                self.initialized = False

    @exception_handler
    async def on_market_status_change(self, is_open: bool, closing_time: float, opening_time: float, initializing: bool):
        async with self.execution_lock:
            symbol = self.trading_config.get_symbol()
            time_ref = opening_time if is_open else closing_time
            self.logger.info(f"Market for {symbol} has {'opened' if is_open else 'closed'} at {unix_to_datetime(time_ref)}.")
            if is_open:
                self.market_open_event.set()
                if initializing:
                    self.send_message_with_details(f"🟢 Market for {symbol} is <b>open</b>.")
                else:
                    self.send_message_with_details(f"⏰🟢 Market for {symbol} has just <b>opened</b>. Resuming trading activities.")
            else:
                self.market_open_event.clear()
                if initializing:
                    self.send_message_with_details(f"⏸️ Market for {symbol} is <b>closed</b>.")
                else:
                    self.logger.info("Allowing the last tick to be processed before fully closing the market.")
                    self.allow_last_tick = True
                    self.send_message_with_details(f"🌙⏸️ Market for {symbol} has just <b>closed</b>. Pausing trading activities.")

    @exception_handler
    async def on_new_tick(self, timeframe: Timeframe, timestamp: datetime):
        await self.bootstrap_completed_event.wait()

        async with self.execution_lock:

            market_is_open = await execute_broker_call(self.config.get_bot_name(), self.broker.is_market_open, self.trading_config.get_symbol())
            if not market_is_open and not self.allow_last_tick:
                self.logger.info("Market is closed, skipping tick processing.")
                return

            if not self.initialized:
                self.logger.info("Strategy not initialized, skipping tick processing.")
                return

            candles_count = self.heikin_ashi_candles_buffer + self.get_minimum_frames_count()

            candles = await execute_broker_call(
                self.config.get_bot_name(),
                self.broker.get_last_candles,
                self.trading_config.get_symbol(),
                self.trading_config.get_timeframe(),
                candles_count
            )
            await self.calculate_indicators(candles)

            last_candle = candles.iloc[-1]
            self.logger.info(f"Candle: {describe_candle(last_candle)}")

            self.logger.debug("Checking for trading signals.")
            self.should_enter, self.prev_state, self.cur_state, self.prev_condition_candle, self.cur_condition_candle = self.check_signals(
                rates=candles, i=len(candles) - 1, trading_direction=self.trading_config.get_trading_direction(),
                state=self.cur_state, cur_condition_candle=self.cur_condition_candle
            )

            self.notify_state_change(candles, len(candles) - 1)

            if self.should_enter:
                order_type_enter = OpType.BUY if self.trading_config.get_trading_direction() == TradingDirection.LONG else OpType.SELL
                self.logger.info(f"Placing order of type {order_type_enter.label} due to trading signal.")

                if self.check_signal_confirmation_and_place_order(self.prev_condition_candle):
                    order = await self.prepare_order_to_place(last_candle)
                    await self.place_order(order)
            else:
                self.logger.info(f"No condition satisfied for candle {describe_candle(last_candle)}")

            if self.allow_last_tick:
                self.allow_last_tick = False

            try:
                self.live_candles_logger.add_candle(last_candle)
            except Exception as e:
                self.logger.error(f"Error while logging candle: {e}")

    @exception_handler
    async def prepare_order_to_place(self, cur_candle: Series) -> TradeOrder | None:
        symbol = self.trading_config.get_symbol()
        trading_direction = self.trading_config.get_trading_direction()
        order_type_enter = OpType.BUY if trading_direction == TradingDirection.LONG else OpType.SELL
        timeframe = self.trading_config.get_timeframe()
        magic_number = self.config.get_bot_magic_number()

        symbol_info = await execute_broker_call(
            self.config.get_bot_name(),
            self.broker.get_market_info,
            symbol
        )

        if symbol_info is None:
            self.logger.error("[place_order] Symbol info not found.")
            self.send_message_with_details("🚫 Symbol info not found for placing the order.")
            raise Exception(f"Symbol info {symbol} not found.")

        point = symbol_info.point
        volume_min = symbol_info.volume_min

        price = self.get_order_price(cur_candle, point, trading_direction)
        sl = self.get_stop_loss(cur_candle, point, trading_direction)
        tp = self.get_take_profit(cur_candle, price, point, timeframe, trading_direction)

        account_balance = await execute_broker_call(
            self.config.get_bot_name(),
            self.broker.get_account_balance
        )

        volume = self.get_volume(account_balance=account_balance, symbol_info=symbol_info, entry_price=price, stop_loss_price=sl)

        self.logger.info(f"[place_order] Account balance retrieved: {account_balance}, Calculated volume for the order on {symbol} at price {price}: {volume}")

        if volume < volume_min:
            self.logger.warning(f"[place_order] Volume of {volume} is less than minimum of {volume_min}")
            self.send_message_with_details(f"❗ Volume of {volume} is less than the minimum of {volume_min} for {symbol}.")
            return None

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

    def get_volume(self, account_balance, symbol_info, entry_price, stop_loss_price):
        risk_percent = self.trading_config.get_risk_percent()
        self.logger.info(
            f"Calculating volume for account balance {account_balance}, symbol info {symbol_info}, entry price {entry_price}, stop loss price {stop_loss_price}, and risk percent {risk_percent}")
        risk_amount = account_balance * risk_percent
        stop_loss_pips = abs(entry_price - stop_loss_price) / symbol_info.point
        pip_value = symbol_info.trade_contract_size * symbol_info.point
        volume = risk_amount / (stop_loss_pips * pip_value)
        # Adjust volume to meet broker's constraints
        adjusted_volume = max(
            symbol_info.volume_min,
            min(symbol_info.volume_max, round_to_step(volume, symbol_info.volume_step))
        )
        return adjusted_volume

    @exception_handler
    async def place_order(self, order: TradeOrder) -> bool:

        self.logger.info(f"[place_order] Placing order: {order}")

        response = self.broker.place_order(order)

        self.logger.debug(f"[place_order] Result of order placement: {response.success}")

        self.logger.message = f"{response.server_response_code} - {response.server_response_message}"

        if response.success:
            self.logger.info(f"[place_order] Order successfully placed. Platform log: \"{self.logger.message}\"")
            self.send_message_with_details(
                f"✅ <b>Order successfully placed with Deal ID {response.deal}:</b>\n\n"
                f"{order}"
            )
        else:
            self.logger.error("[place_order] Error while placing the order.")
            self.send_message_with_details(f"🚫 <b>Error while placing the order:</b>\n\n"
                                           f"{order}\n"
                                           f"Platform log: \"{self.logger.message}\"")

        return response.success

    def check_signal_confirmation_and_place_order(self, signal_candle):
        bot_name = self.config.get_bot_name()
        magic = self.config.get_bot_magic_number()
        open_dt = signal_candle['time_open'].strftime('%H:%M')
        close_dt = signal_candle['time_close'].strftime('%H:%M')

        signal_id = {
            "bot_name": bot_name,
            "magic_number": magic,
            "open_time": open_dt,
            "close_time": close_dt
        }

        # Retrieve the signal confirmation from MongoDB
        signal_confirmation = self.database.find_one("signals_confirmation", signal_id)

        if not signal_confirmation:
            self.logger.error(f"No confirmation found for signal with open time {open_dt} and close time {close_dt}")
            self.send_message_with_details(f"🚫 No confirmation found for signal with open time {open_dt} and close time {close_dt}. Not placing order by default.")
            return False

        confirmed = signal_confirmation.get("confirmation", False)
        user_username = signal_confirmation.get("user_name", "Unknown User")

        if confirmed:
            self.logger.info(f"Signal for candle {open_dt} - {close_dt} confirmed by {user_username}. Placing order.")
            self.send_message_with_details(f"✅ Signal for candle {open_dt} - {close_dt} confirmed by {user_username}. Placing order.")
        else:
            self.logger.info(f"Signal for candle {open_dt} - {close_dt} not confirmed by {user_username}. Not placing order.")
            self.send_message_with_details(f"🚫 Signal for candle {open_dt} - {close_dt} not confirmed by {user_username}. Not placing order.")

        return confirmed

    @exception_handler
    async def on_deal_closed(self, position: Position):
        async with self.execution_lock:
            self.logger.info(f"Deal closed: {position}")

            for deal in position.deals:
                if deal.order_source not in [OrderSource.STOP_LOSS, OrderSource.TAKE_PROFIT]:
                    self.logger.info(f"Skipping deal with ticket {deal.ticket} as it is not a stop loss or take profit.")
                    continue

            self.logger.info(f"Deal closed:\n{deal}")

            emoji = "🤑" if deal.profit > 0 else "😔"

            trade_details = (
                f"<b>Position ID:</b> {position.position_id}\n"
                f"<b>Timestamp:</b> {deal.time.strftime('%d/%m/%Y %H:%M:%S')}\n"
                f"<b>Symbol:</b> {deal.symbol}\n"
                f"<b>Volume:</b> {deal.volume}\n"
                f"<b>Price:</b> {deal.price}\n"
                f"<b>Stop Loss/Take Profit:</b> {deal.order_source.value}\n"
                f"<b>Profit/Loss:</b> {deal.profit}\n"
                f"<b>Commission:</b> {position.commission}\n"
                f"<b>Swap:</b> {deal.swap}"
                f"<b>Fee:</b> {deal.fee}"
            )

            # Send notification via Telegram
            self.send_message_with_details(f"{emoji} <b>Deal closed</b>\n\n{trade_details}")

    @exception_handler
    async def on_economic_event(self, event_info: dict):
        async with self.execution_lock:
            self.logger.info(f"Economic event occurred: {event_info}")

            event_name = event_info.get('event_name', 'Unknown Event')
            minutes_until_event = math.ceil(event_info.get('seconds_until_event', 1) / 60)
            symbol, magic_number = (self.trading_config.get_symbol(), self.config.get_bot_magic_number())

            message = (
                f"📰🔔 Economic event <b>{event_name}</b> is scheduled to occur in {minutes_until_event} minutes.\n"
            )
            self.send_message_with_details(message)

            positions: List[Deal] = await execute_broker_call(
                self.config.get_bot_name(),
                self.broker.get_open_positions,
                symbol=symbol
            )

            if not positions:
                message = f"ℹ️ No open positions found for forced closure due to the economic event <b>{event_name}</b>."
                self.logger.warning(message)
                self.send_message_with_details(message)
            else:
                for position in positions:
                    # Attempt to close the position
                    result: RequestResult = await execute_broker_call(
                        self.config.get_bot_name(),
                        self.broker.close_position,
                        position=position, comment=f"'{event_name}'", magic_number=magic_number
                    )
                    if result and result.success:
                        message = (
                            f"✅ Position {position.ticket} closed successfully due to the economic event <b>{event_name}</b>.\n"
                            f"ℹ️ This action was taken to mitigate potential risks associated with the event's impact on the markets."
                        )
                    else:
                        message = (
                            f"❌ Failed to close position {position.ticket} due to the economic event <b>{event_name}</b>.\n"
                            f"⚠️ Potential risks remain as the position could not be closed."
                        )
                    self.logger.info(message)
                    self.send_message_with_details(message)

    @exception_handler
    async def calculate_indicators(self, rates):
        # Convert candlestick to Heikin Ashi
        await self.heikin_ashi_values(rates)

        # Calculate indicators
        supertrend(super_trend_fast_period, super_trend_fast_multiplier, rates)
        supertrend(super_trend_slow_period, super_trend_slow_multiplier, rates)
        stochastic(stoch_k_period, stoch_d_period, stoch_smooth_k, rates)
        average_true_range(5, rates)
        average_true_range(2, rates)

        return rates

    @exception_handler
    async def heikin_ashi_values(self, df):
        # Ensure df is a DataFrame with the necessary columns
        if not isinstance(df, pd.DataFrame) or not {'open', 'high', 'low', 'close'}.issubset(df.columns):
            raise ValueError("Input must be a DataFrame with 'open', 'high', 'low', and 'close' columns.")

        # Get the symbol's point precision (e.g., 0.01, 0.0001)
        symbol_info: SymbolInfo = await execute_broker_call(
            self.config.get_bot_name(),
            self.broker.get_market_info,
            self.trading_config.get_symbol()
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

    def check_signals(
            self,
            rates: Series,
            i: int,
            trading_direction: TradingDirection,
            state=None,
            cur_condition_candle=None
    ) -> (bool, int, int, Series):
        """
        Analyzes market conditions to determine the appropriateness of entering a trade based on a set of predefined rules.

        Parameters:
        - rates (dict): A dictionary containing market data rates, with keys for time, close, and other indicator values.
        - i (int): The current index in the rates dictionary to check signals for. In live mode is always the last candle index.
        - params (dict): A dictionary containing parameters such as symbol, timeframe, and trading direction.
        - state (int, optional): The current state of the trading conditions, used for tracking across multiple calls. Defaults to None.
        - cur_condition_candle (Dataframe, optional): The last candle where a trading condition was met. Defaults to None.
        - notifications (bool, optional): Indicates whether to send notifications when conditions are met. Defaults to False.

        Returns:
        - should_enter (bool): Indicates whether the conditions suggest entering a trade.
        - prev_state (int): The previous state before the current check.
        - cur_state (int): The updated state after checking the current conditions.
        - prev_condition_candle (Series): The candle where a trading condition was previously met.
        - cur_condition_candle (Series): The candle where the latest trading condition was met.

        The function evaluates a series of trading conditions based on market direction (long or short), price movements, and indicators like Supertrend and Stochastic. It progresses through states as conditions are met, logging each step, and ultimately determines whether the strategy's criteria for entering a trade are satisfied.
        """

        cur_state = state if state is not None else 0
        prev_state = cur_state
        prev_condition_candle = cur_condition_candle
        should_enter = False
        cur_candle = rates.iloc[i]
        close = cur_candle['HA_close']
        supert_fast_prev, supert_slow_prev = rates[supertrend_fast_key][i - 1], rates[supertrend_slow_key][i - 1]
        supert_fast_cur = rates[supertrend_fast_key][i]
        stoch_k_cur, stoch_d_cur = rates[stoch_k_key][i], rates[stoch_d_key][i]
        is_long, is_short = trading_direction == TradingDirection.LONG, trading_direction == TradingDirection.SHORT
        int_time_open = lambda candle: -1 if candle is None else int(candle['time_open'].timestamp())
        int_time_close = lambda candle: -1 if candle is None else int(candle['time_close'].timestamp())

        # Condition 1
        can_check_1 = cur_state >= 0 and int_time_open(cur_candle) >= int_time_open(cur_condition_candle)
        self.logger.debug(f"Can check condition 1: {can_check_1}")
        if can_check_1:
            self.logger.debug(f"Before evaluating condition 1: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
            cond1 = (is_long and close >= supert_slow_prev) or (is_short and close < supert_slow_prev)
            if cond1:
                if cur_state == 0:
                    prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle, prev_condition_candle, cur_condition_candle, 1, cur_state)
            elif cur_state >= 1:
                prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle, prev_condition_candle, cur_condition_candle, 0, cur_state)
            self.logger.debug(f"After evaluating condition 1: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")

        # Condition 2
        can_check_2 = cur_state >= 1 and int_time_open(cur_candle) > int_time_open(cur_condition_candle)
        self.logger.debug(f"Can check condition 2: {can_check_2}")
        if can_check_2:
            self.logger.debug(f"Before evaluating condition 2: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
            cond2 = (is_long and close <= supert_fast_cur) or (is_short and close > supert_fast_cur)
            if cond2 and cur_state == 1:
                prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle, prev_condition_candle, cur_condition_candle, 2, cur_state)
            self.logger.debug(f"After evaluating condition 2: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")

        # Condition 3
        can_check_3 = cur_state >= 2 and int_time_open(cur_candle) >= int_time_open(cur_condition_candle)
        self.logger.debug(f"Can check condition 3: {can_check_3}")
        if can_check_3:
            self.logger.debug(f"Before evaluating condition 3: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
            cond3 = (is_long and close >= supert_fast_prev) or (is_short and close < supert_fast_prev)
            if cond3:
                if cur_state == 2:
                    prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle, prev_condition_candle, cur_condition_candle, 3, cur_state)
            elif cur_state >= 3:
                prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle, prev_condition_candle, cur_condition_candle, 2, cur_state)
            self.logger.debug(f"After evaluating condition 3: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")

        # Condition 4 (Stochastic)
        can_check_4 = cur_state >= 3
        self.logger.debug(f"Can check condition 4: {can_check_4}")
        if can_check_4:
            self.logger.debug(f"Before evaluating condition 4: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
            cond4 = (is_long and stoch_k_cur > stoch_d_cur and stoch_d_cur < 50) or (is_short and stoch_k_cur < stoch_d_cur and stoch_d_cur > 50)
            if cond4 and cur_state == 3:
                prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle, prev_condition_candle, cur_condition_candle, 4, cur_state)
            self.logger.debug(f"After evaluating condition 4: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")

        # Condition 5 (Final condition for entry)
        time_tolerance = 30
        can_check_5 = cur_state == 4 and int(cur_candle['time_open'].timestamp()) > int(cur_condition_candle['time_open'].timestamp())
        self.logger.debug(f"Can check condition 5: {can_check_5}")
        if can_check_5:
            lower, upper = int_time_close(cur_condition_candle), int_time_close(cur_condition_candle) + time_tolerance
            cond5 = lower <= int_time_open(cur_candle) <= upper
            self.logger.debug(f"Lower Bound: {lower}, Upper Bound: {upper}, Current Candle Time: {int_time_open(cur_candle)}")
            # condition_5_met = to_int(cur_candle_time) >= lower_bound  # Uncomment for testing
            if cond5:
                self.logger.debug(f"Before evaluating condition 5: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
                prev_state, cur_state, prev_condition_candle, cur_condition_candle = self.update_state(cur_candle, prev_condition_candle, cur_condition_candle, 5, cur_state)
                should_enter = True
            self.logger.debug(f"After evaluating condition 5: prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")

        self.logger.debug(f"Returning: should_enter={should_enter}, prev_state={prev_state}, cur_state={cur_state}, cur_condition_candle={describe_candle(cur_condition_candle)}")
        return should_enter, prev_state, cur_state, prev_condition_candle, cur_condition_candle

    def update_state(
            self,
            cur_candle: Series,
            prev_condition_candle: Optional[Series],
            cur_condition_candle: Optional[Series],
            cur_state: int,
            prev_state: int
    ) -> Tuple[int, int, Optional[Series], Optional[Series]]:
        """
        Updates the state and the last candle that met the condition.

        Args:
            cur_candle (Series): The current candle.
            prev_condition_candle (Optional[Series]): The previous condition-matching candle.
            cur_condition_candle (Optional[Series]): The last candle that meets the condition.
            cur_state (int): The current new state.
            prev_state (int): The previous old state.

        Raises:
            ValueError: If the current candle time is earlier than the last condition-matching candle time.

        Returns:
            Tuple[int, int, Optional[Series], Optional[Series]]: (previous state, new state, previous condition candle, updated condition candle)
        """

        ret_state = cur_state if cur_state != prev_state else prev_state

        self.logger.debug(f"Changing state from {prev_state} to {cur_state}")

        cur_time_unix = dt_to_unix(cur_candle['time_open'])
        cur_condition_time_unix = dt_to_unix(cur_condition_candle['time_open']) if cur_condition_candle is not None else None

        if cur_condition_time_unix and cur_time_unix < cur_condition_time_unix:
            raise ValueError(
                f"Strategy current candle time {cur_candle['time_open']} cannot be prior to last condition-matching "
                f"candle time {cur_condition_candle['time_open']}."
            )

        if cur_state != prev_state:
            if cur_state == 0:
                self.logger.debug("State changed to 0. Resetting cur_condition_candle.")
                updated_candle = None
            else:
                prev_time = cur_condition_candle['time_open'] if cur_condition_candle is not None else None
                self.logger.debug(f"Strategy candle time change from {prev_time} -> {cur_candle['time_open']}")
                updated_candle = cur_candle
            prev_condition_candle = cur_condition_candle
        else:
            self.logger.debug(
                f"update_state called but no state change detected. Current state remains {cur_state}. "
                f"Called with candle time {cur_candle['time_open']}. Previous state was {prev_state}."
            )
            updated_candle = cur_condition_candle

        return prev_state, ret_state, prev_condition_candle, updated_candle

    def send_message(self, message, level=NotificationLevel.DEFAULT, reply_markup=None):
        # Check if the notification level is high enough to send

        t_chat_ids = self.trading_config.get_telegram_config().get_chat_ids()
        for chat_id in t_chat_ids:
            self.telegram.send_message(chat_id, message, reply_markup=reply_markup)

    def send_message_with_details(self, message, level=NotificationLevel.DEFAULT, reply_markup=None):
        symbol, bot_name, timeframe, trading_direction = (self.trading_config.get_symbol(),
                                                          self.config.get_bot_name(),
                                                          self.trading_config.get_timeframe(),
                                                          self.trading_config.get_trading_direction())
        direction_emoji = "📈" if trading_direction.name == "LONG" else "📉️"
        detailed_message = (
            f"{message}\n\n"
            "<b>Details:</b>\n\n"
            f"💻 <b>Bot name:</b> {bot_name}\n"
            f"💱 <b>Symbol:</b> {symbol}\n"
            f"📊 <b>Timeframe:</b> {timeframe.name}\n"
            f"{direction_emoji} <b>Direction:</b> {trading_direction.name}"
        )
        self.send_message(detailed_message, level, reply_markup)

    @exception_handler
    async def signal_confirmation_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        await query.answer()
        self.logger.debug(f"Callback query answered: {query}")

        # Retrieve data from callback, now in CSV format
        data = query.data.split(',')
        self.logger.debug(f"Data retrieved from callback: {data}")
        bot_name, magic, open_dt, close_dt, confirmed_flag = data
        confirmed = confirmed_flag == '1'
        user_username = query.from_user.username if query.from_user.username else "Unknown User"
        user_id = query.from_user.id
        chat_id = update.effective_chat.id
        chat_username = update.effective_chat.username
        self.logger.debug(
            f"Parsed data - bot_name: {bot_name}, magic: {magic}, open_dt: {open_dt}, close_dt: {close_dt}, confirmed: {confirmed}, user_username: {user_username}, user_id: {user_id}, chat_id: {chat_id}, chat_username: {chat_username}")

        # Convert open_dt and close_dt to datetime objects

        open_dt_datetime = unix_to_datetime(int(open_dt))
        close_dt_datetime = unix_to_datetime(int(close_dt))
        now = now_utc()
        if now > close_dt_datetime + timedelta(seconds=self.trading_config.get_timeframe().to_seconds()):
            self.logger.info(f"Signal from {open_dt_datetime} to {close_dt_datetime} is obsolete. Current time: {now}")
            await query.edit_message_text("⚠️ This signal is obsolete and can no longer be confirmed or ignored.")
            return

        # Create CSV formatted confirmation and blocking data
        csv_confirm = f"{bot_name},{magic},{open_dt},{close_dt},1"
        csv_block = f"{bot_name},{magic},{open_dt},{close_dt},0"
        self.logger.debug(f"CSV formatted data - confirm: {csv_confirm}, block: {csv_block}")

        current_bot_name = self.config.get_bot_name()
        current_magic_number = self.config.get_bot_magic_number()
        self.logger.debug(f"Current bot configuration - bot_name: {current_bot_name}, magic_number: {current_magic_number}")
        if bot_name != current_bot_name or int(magic) != current_magic_number:
            self.logger.info(
                f"Ignored update for bot_name '{bot_name}' and magic number '{magic}' as they do not match the current instance '{current_bot_name}' and magic number '{current_magic_number}'.")
            return

        # Set the keyboard buttons with updated callback data
        if confirmed:
            keyboard = [
                [
                    InlineKeyboardButton("Confirmed ✔️", callback_data=csv_confirm),
                    InlineKeyboardButton("Ignored", callback_data=csv_block)
                ]
            ]
        else:
            keyboard = [
                [
                    InlineKeyboardButton("Confirm", callback_data=csv_confirm),
                    InlineKeyboardButton("Ignored ✔️", callback_data=csv_block)
                ]
            ]
        self.logger.debug(f"Keyboard set with updated callback data: {keyboard}")

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
        self.logger.debug(f"Database object to upsert: {obj}")
        self.database.upsert("signals_confirmation", {"bot_name": bot_name,
                                                      "magic_number": int(magic),
                                                      "open_time": open_dt,
                                                      "close_time": close_dt}, obj)
        self.logger.debug("Database updated with new signal confirmation")

        choice_text = "✅ Confirm" if confirmed else "🚫 Ignore"

        open_dt_formatted = open_dt_datetime.strftime('%Y-%m-%d %H:%M:%S UTC')
        close_dt_formatted = close_dt_datetime.strftime('%Y-%m-%d %H:%M:%S UTC')

        message = f"ℹ️ Your choice to <b>{choice_text}</b> the signal for the candle from {open_dt_formatted} to {close_dt_formatted} has been successfully saved."
        self.send_message(message)
        self.logger.debug(f"Confirmation message sent: {message}")

    def get_signal_confirmation_dialog(self, candle) -> InlineKeyboardMarkup:
        self.logger.debug("Starting signal confirmation dialog creation")
        bot_name = self.config.get_bot_name()
        magic = self.config.get_bot_magic_number()

        open_dt = dt_to_unix(candle['time_open'])
        close_dt = dt_to_unix(candle['time_close'])

        csv_confirm = f"{bot_name},{magic},{open_dt},{close_dt},1"
        csv_block = f"{bot_name},{magic},{open_dt},{close_dt},0"
        self.logger.debug(f"CSV data - confirm: {csv_confirm}, block: {csv_block}")

        keyboard = [
            [
                InlineKeyboardButton("Confirm", callback_data=csv_confirm),
                InlineKeyboardButton("Ignore", callback_data=csv_block)
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        self.logger.debug(f"Keyboard created: {keyboard}")

        return reply_markup

    @exception_handler
    async def shutdown(self):
        self.logger.info("Shutting down the bot.")
        await self.telegram.stop()
