import math
import threading
from datetime import timedelta, datetime
from typing import Any, Optional, Dict, Tuple

import MetaTrader5 as mt5
import pandas as pd

from brokers.broker_interface import BrokerAPI
from datao.Deal import Deal
from datao.Position import Position
from datao.RequestResult import RequestResult
from datao.SymbolInfo import SymbolInfo
from datao.SymbolPrice import SymbolPrice
from datao.TradeOrder import TradeOrder
from utils.config import ConfigReader
from utils.enums import Timeframe, FillingType, OpType, DealType, OrderSource, PositionType
from utils.logger import log_warning, log_error, log_info, log_debug
from utils.utils_functions import now_utc, dt_to_unix, unix_to_datetime

# https://www.mql5.com/en/docs/constants/tradingconstants/dealproperties
# https://www.mql5.com/en/articles/40
# https://www.mql5.com/en/docs/python_metatrader5/mt5positionsget_py
# https://www.mql5.com/en/docs/python_metatrader5/mt5historydealsget_py

DEAL_TYPE_MAPPING = {
    0: DealType.BUY,  # DEAL_TYPE_BUY
    1: DealType.SELL,  # DEAL_TYPE_SELL
    # Other types are classified as 'OTHER'
}

POSITION_TYPE_MAPPING = {
    0: PositionType.LONG,  # DEAL_TYPE_BUY
    1: PositionType.SHORT,  # DEAL_TYPE_SELL
    # Other types are classified as 'OTHER'
}

REASON_MAPPING = {
    4: OrderSource.STOP_LOSS,  # DEAL_REASON_SL
    5: OrderSource.TAKE_PROFIT,  # DEAL_REASON_TP
    0: OrderSource.MANUAL,  # DEAL_REASON_CLIENT
    1: OrderSource.MANUAL,  # DEAL_REASON_MOBILE
    2: OrderSource.MANUAL,  # DEAL_REASON_WEB
    3: OrderSource.BOT,  # DEAL_REASON_EXPERT
    6: OrderSource.MANUAL,  # DEAL_REASON_SO (Stop Out)
    7: OrderSource.MANUAL,  # DEAL_REASON_ROLLOVER
    8: OrderSource.MANUAL,  # DEAL_REASON_VMARGIN
    9: OrderSource.MANUAL,  # DEAL_REASON_SPLIT
    # Other reasons are classified as 'OTHER'
}


class MT5Broker(BrokerAPI):

    def __init__(self, config: ConfigReader):
        if not mt5.initialize():
            log_error(f"initialization failed, error code {mt5.last_error()}")
            mt5.shutdown()
            raise Exception("Failed to initialize MT5")
        log_info("MT5 initialized successfully")

        # Set up connection with MT5 account
        account = config.get_mt5_account()
        log_info(f"Trying to connect with account {account} and provided server credentials.")
        if not mt5.login(account, password=config.get_mt5_password(), server=config.get_mt5_server()):
            log_error(f"Failed to connect to account #{account}, error code: {mt5.last_error()}")
            raise Exception("Failed to initialize MT5")
        log_info("Login success")
        log_info(mt5.account_info())

        self._callbacks_lock = threading.Lock()
        self._running = True

    # Conversion Methods
    def filling_type_to_mt5(self, filling_type: FillingType):
        conversion_dict = {
            FillingType.FOK: mt5.ORDER_FILLING_FOK,
            FillingType.IOC: mt5.ORDER_FILLING_IOC,
            FillingType.RETURN: mt5.ORDER_FILLING_RETURN
        }
        return conversion_dict[filling_type]

    def timeframe_to_mt5(self, timeframe: Timeframe):
        conversion_dict = {
            Timeframe.M1: mt5.TIMEFRAME_M1,
            Timeframe.M5: mt5.TIMEFRAME_M5,
            Timeframe.M15: mt5.TIMEFRAME_M15,
            Timeframe.M30: mt5.TIMEFRAME_M30,
            Timeframe.H1: mt5.TIMEFRAME_H1,
            Timeframe.H4: mt5.TIMEFRAME_H4,
            Timeframe.D1: mt5.TIMEFRAME_D1
        }
        return conversion_dict[timeframe]

    def order_type_to_mt5(self, order_type: OpType):
        conversion_dict = {
            OpType.BUY: mt5.ORDER_TYPE_BUY,
            OpType.SELL: mt5.ORDER_TYPE_SELL
        }
        return conversion_dict[order_type]

    # Utility and Market Data Methods
    def is_market_open(self, symbol: str) -> bool:
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            log_warning(f"{symbol} not found, cannot retrieve symbol info.")
            return False
        return not symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED

    def get_broker_timezone_offset(self, symbol) -> Optional[int]:
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            log_warning(f"{symbol} not found, cannot retrieve symbol info.")
            return None

        if symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED:
            log_warning(f"Market closed for {symbol}. Cannot get the broker server timezone offset.")
            return 0

        # Get the current broker time and UTC time to calculate offset
        broker_time = symbol_info.time
        utc_unix_timestamp = dt_to_unix(now_utc())
        time_diff_seconds = abs(broker_time - utc_unix_timestamp)
        offset_hours = math.ceil(time_diff_seconds / 3600)

        log_debug(f"Broker timestamp: {broker_time}, UTC timestamp: {utc_unix_timestamp}, Offset: {offset_hours} hours")
        return offset_hours

    def get_market_info(self, symbol: str) -> Optional[SymbolInfo]:
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            log_error(f"{symbol} not found, cannot place order.")
            return None

        if not symbol_info.visible:
            log_error(f"{symbol} is not visible. Attempting to enable.")
            if not mt5.symbol_select(symbol, True):
                log_error("symbol_select() failed, cannot place order.")
                return None

        return SymbolInfo(
            symbol=symbol,
            volume_min=symbol_info.volume_min,
            volume_max=symbol_info.volume_max,
            point=symbol_info.point,
            trade_mode=symbol_info.trade_mode,
            trade_contract_size=symbol_info.trade_contract_size,
            volume_step=symbol_info.volume_step,
            default_filling_mode=symbol_info.filling_mode
        )

    def get_symbol_price(self, symbol: str) -> Optional[SymbolPrice]:
        symbol_info_tick = mt5.symbol_info_tick(symbol)
        if symbol_info_tick is None:
            log_error(f"{symbol} not found.")
            return None

        return SymbolPrice(symbol_info_tick.ask, symbol_info_tick.bid)

    def get_last_candles(self, symbol: str, timeframe: Timeframe, count: int = 1, position: int = 0) -> pd.DataFrame:
        # Fetch one more candle than requested to potentially exclude the open candle
        rates = mt5.copy_rates_from_pos(symbol, self.timeframe_to_mt5(timeframe), position, count + 1)
        df = pd.DataFrame(rates)

        # Rename 'time' to 'time_open' and convert to datetime
        df['time_open'] = pd.to_datetime(df['time'], unit='s')
        df.drop(columns=['time'], inplace=True)

        # Calculate 'time_close' and add original broker times
        timeframe_duration = timeframe.to_seconds()
        df['time_close'] = df['time_open'] + pd.to_timedelta(timeframe_duration, unit='s')
        df['time_open_broker'] = df['time_open']
        df['time_close_broker'] = df['time_close']

        # Convert from broker timezone to UTC
        timezone_offset = self.get_broker_timezone_offset(symbol)
        log_debug(f"Timezone offset: {timezone_offset} hours")
        df['time_open'] -= pd.to_timedelta(timezone_offset, unit='h')
        df['time_close'] -= pd.to_timedelta(timezone_offset, unit='h')

        # Arrange columns for clarity
        columns_order = ['time_open', 'time_close', 'time_open_broker', 'time_close_broker']
        df = df[columns_order + [col for col in df.columns if col not in columns_order]]

        # Check and exclude the last candle if it's still open
        current_time = now_utc()
        log_debug(f"Current UTC time: {current_time.strftime('%d/%m/%Y %H:%M:%S')}")
        if current_time < df.iloc[-1]['time_close']:
            log_debug(f"Excluding last open candle with close time: {df.iloc[-1]['time_close'].strftime('%d/%m/%Y %H:%M:%S')}")
            df = df.iloc[:-1]

        # Ensure DataFrame has exactly 'count' rows
        return df.iloc[-count:].reset_index(drop=True)

    def shutdown(self):
        mt5.shutdown()
        log_info("MT5 shutdown successfully")

    def get_working_directory(self):
        return mt5.terminal_info().data_path + "\\MQL5\\Files"

    # Account Methods
    def get_account_balance(self) -> float:
        account_info = mt5.account_info()
        if account_info is None:
            raise Exception("Failed to retrieve account information")

        log_info(f"Account balance: {account_info.balance}")
        return account_info.balance

    def get_account_leverage(self) -> float:
        account_info = mt5.account_info()
        if account_info is None:
            raise Exception("Failed to retrieve account information")

        log_info(f"Account leverage: {account_info.leverage}")
        return account_info.leverage

    # Order Placement Methods
    def place_order(self, request: TradeOrder) -> RequestResult:
        # Implement order placement logic similar to the previous _place_order_sync
        symbol_info = mt5.symbol_info(request.symbol)
        if symbol_info is None:
            raise Exception(f"Symbol {request.symbol} not found")

        if not symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_FULL:
            raise Exception(f"Market is closed for symbol {request.symbol}, cannot place order.")

        filling_mode = request.filling_mode or self.find_filling_mode(request.symbol)
        log_debug(f"Filling mode for {request.symbol}: {filling_mode}")

        op_type = self.order_type_to_mt5(request.order_type)

        mt5_request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": request.symbol,
            "volume": request.volume,
            "type": op_type,
            "price": request.order_price,
            "sl": request.sl,
            "tp": request.tp,
            "magic": request.magic_number,
            "comment": request.comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": self.filling_type_to_mt5(filling_mode),
        }

        log_debug(f"Send_order_request payload: {mt5_request}")

        result = mt5.order_send(mt5_request)
        response = RequestResult(request, result)

        if not response.success:
            log_error(f"Order failed, retcode={response.server_response_code}, description={response.comment}")
            raise Exception(f"Order send failed with retcode {response.server_response_code}")

        return response

    def close_position(self, position: Position, comment: Optional[str] = None, magic_number: Optional[int] = None) -> RequestResult:
        # Prepare request for closing the position
        filling_mode = self.find_filling_mode(position.symbol)

        close_request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": position.symbol,
            "volume": position.volume,
            "type": mt5.ORDER_TYPE_SELL if position.position_type == PositionType.LONG else mt5.ORDER_TYPE_BUY,
            "position": position.ticket,
            "magic": magic_number,
            "comment": comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling_mode.value,
        }

        result = mt5.order_send(close_request)
        req_result = RequestResult(close_request, result)
        if req_result.success:
            log_info(f"Position {position.ticket} successfully closed.")
        else:
            log_error(f"Error closing position {position.ticket}, error code = {result.retcode}, message = {result.comment}")

        return req_result

    # Position and Deal Mapping
    def map_open_position(self, pos_obj: Any, timezone_offset: int) -> Position:
        pos_type, source = self.classify_position(pos_obj)
        return Position(
            position_id=pos_obj.identifier,
            ticket=pos_obj.ticket,
            volume=pos_obj.volume,
            symbol=pos_obj.symbol,
            time=unix_to_datetime(pos_obj.time) - timedelta(hours=timezone_offset) if pos_obj.time else None,
            price_open=pos_obj.price_open,
            price_current=pos_obj.price_current,
            swap=pos_obj.swap,
            profit=pos_obj.profit,
            sl=pos_obj.sl,
            tp=pos_obj.tp,
            position_type=pos_type,
            order_source=source,
            comment=pos_obj.comment,
            open=True
        )

    def map_deal(self, deal_obj: Any, timezone_offset: int) -> Deal:
        time = unix_to_datetime(deal_obj.time) - timedelta(hours=timezone_offset) if deal_obj.time else None
        deal_type, source = self.classify_deal(deal_obj)

        return Deal(
            ticket=deal_obj.ticket,
            order=deal_obj.order,
            time=time,
            magic=deal_obj.magic,
            position_id=deal_obj.position_id,
            volume=deal_obj.volume,
            price=deal_obj.price,
            commission=deal_obj.commission,
            swap=deal_obj.swap,
            profit=deal_obj.profit,
            fee=deal_obj.fee,
            symbol=deal_obj.symbol,
            comment=deal_obj.comment,
            external_id=str(deal_obj.ticket),
            deal_type=deal_type,
            order_source=source
        )

    def get_open_positions(self, symbol: str) -> Dict[int, Position]:
        open_positions = mt5.positions_get(symbol=symbol)
        if not open_positions:
            return {}

        timezone_offset = self.get_broker_timezone_offset(symbol)
        mapped_positions = {pos.position_id: self.map_open_position(pos, timezone_offset) for pos in open_positions}

        oldest_time = min(mapped_positions.values(), key=lambda pos: pos.time).time
        deals = mt5.history_deals_get(dt_to_unix(oldest_time), dt_to_unix(now_utc() + timedelta(hours=timezone_offset)), group=symbol)
        for deal in deals:
            deal_mapped = self.map_deal(deal, timezone_offset)
            if deal_mapped.position_id in mapped_positions:
                mapped_positions[deal_mapped.position_id].deals.append(deal_mapped)

        return mapped_positions

    def get_historical_positions(self, from_tms: datetime, to_tms: datetime, symbol: Optional[str] = None, magic_number: Optional[int] = None) -> Dict[int, Position]:
        from_unix = dt_to_unix(from_tms)
        to_unix = dt_to_unix(to_tms)
        deals = mt5.history_deals_get(from_unix, to_unix)
        timezone_offset = self.get_broker_timezone_offset(symbol)

        if deals is None:
            return {}

        filtered_deals = list(
            deal for deal in deals
            if (magic_number is None or deal.magic == magic_number)
            and (symbol is None or deal.symbol == symbol)
        )

        filtered_deals = sorted(filtered_deals, key=lambda x: (x.symbol, x.time))

        positions: Dict[int, Position] = {}

        for deal in filtered_deals:
            try:
                if deal.position_id not in positions:
                    positions[deal.position_id] = Position(position_id=deal.position_id, symbol=deal.symbol, open=False)

                positions[deal.position_id].deals.append(self.map_deal(deal, timezone_offset))
            except Exception as e:
                log_error(f"Error while processing deal with ticket {deal.ticket}: {e}")
            continue

        return positions

    # Classification Methods
    def classify_position(self, pos_obj: Any) -> Tuple[PositionType, Optional[OrderSource]]:
        pos_type = POSITION_TYPE_MAPPING.get(pos_obj.type, PositionType.OTHER)
        source = REASON_MAPPING.get(pos_obj.reason, OrderSource.OTHER)
        return pos_type, source

    def classify_deal(self, deal_obj: Any) -> Tuple[DealType, Optional[OrderSource]]:
        deal_type = DEAL_TYPE_MAPPING.get(deal_obj.type, DealType.OTHER)
        source = REASON_MAPPING.get(deal_obj.reason, OrderSource.OTHER)
        return deal_type, source
