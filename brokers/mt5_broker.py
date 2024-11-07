import math
import threading
from datetime import timedelta, datetime, timezone
from typing import Any, Optional, List, Dict, Union, Tuple

import MetaTrader5 as mt5
import pandas as pd
from pandas import Series

from brokers.broker_interface import BrokerAPI
from datao.Deal import Deal
from datao.Position import Position
from datao.RequestResult import RequestResult
from datao.SymbolInfo import SymbolInfo
from datao.SymbolPrice import SymbolPrice
from datao.TradeOrder import TradeOrder
from utils.config import ConfigReader
from utils.enums import Timeframe, FillingType, OpType, DealType, OrderSource
from utils.logger import log_warning, log_error, log_info, log_debug
from utils.utils_functions import now_utc, dt_to_unix, unix_to_datetime

# https://www.mql5.com/en/docs/constants/tradingconstants/dealproperties
# https://www.mql5.com/en/articles/40
# https://www.mql5.com/en/docs/python_metatrader5/mt5positionsget_py
# https://www.mql5.com/en/docs/python_metatrader5/mt5historydealsget_py

DEAL_TYPE_MAPPING = {
    0: DealType.BUY,  # DEAL_TYPE_BUY
    1: DealType.SELL,  # DEAL_TYPE_SELL
    # Altri tipi vengono classificati come 'OTHER'
}

# Mappatura dei reason a OrderSource
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
    # Altri reason vengono classificati come 'OTHER'
}


class MT5Broker(BrokerAPI):

    def __init__(self, config: ConfigReader):
        if not mt5.initialize():
            log_error(f"initialization failed, error code {mt5.last_error()}")
            mt5.shutdown()
            raise Exception("Failed to initialize MT5")

        log_info("MT5 initialized successfully")

        log_info(f"Trying to connect with account {config.get_mt5_account()} and password {config.get_mt5_password()} and server {config.get_mt5_server()}")
        if not mt5.login(config.get_mt5_account(), password=config.get_mt5_password(), server=config.get_mt5_server()):
            log_error(f"failed to connect at account #{config.get_mt5_account()}, error code: {mt5.last_error()}")
            raise Exception("Failed to initialize MT5")

        log_info("Login success")
        log_info(mt5.account_info())

        self._callbacks_lock = threading.Lock()
        self._running = True

    def get_last_candles(self, symbol: str, timeframe: Timeframe, count: int = 1, position: int = 0) -> Series:
        # Fetch one more candle than requested, to account for possibly excluding the open candle
        rates = mt5.copy_rates_from_pos(symbol, self.timeframe_to_mt5(timeframe), position, count + 1)
        # Convert rates to a DataFrame
        df = pd.DataFrame(rates)

        # Rename 'time' to 'time_open' and convert it to datetime
        df['time_open'] = pd.to_datetime(df['time'], unit='s')
        df.drop(columns=['time'], inplace=True)  # Drop the original 'time' column

        # Calculate 'time_close' by adding the timeframe duration (in seconds) to 'time_open'
        timeframe_duration = timeframe.to_seconds()
        df['time_close'] = df['time_open'] + pd.to_timedelta(timeframe_duration, unit='s')

        # Add original broker times
        df['time_open_broker'] = df['time_open']
        df['time_close_broker'] = df['time_close']

        # Convert from broker timezone to UTC
        timezone_offset = self.get_broker_timezone_offset(symbol)
        log_debug(f"Timezone offset is {timezone_offset} hours")
        df['time_open'] = df['time_open'].apply(lambda x: x.replace(microsecond=0) - timedelta(hours=timezone_offset))
        df['time_close'] = df['time_close'].apply(lambda x: x.replace(microsecond=0) - timedelta(hours=timezone_offset))

        # Move time columns ahead
        columns_order = ['time_open',
                         'time_close',
                         'time_open_broker',
                         'time_close_broker'] + [col for col in df.columns if
                                                 col not in ['time_open', 'time_close', 'time_open_broker', 'time_close_broker']]
        df = df[columns_order]

        # If the last candle is open, exclude it from the DataFrame
        current_time = now_utc()
        log_debug(f"Current UTC time: {current_time.strftime('%d/%m/%Y %H:%M:%S')}")
        if current_time < df.iloc[-1]['time_close']:
            log_debug(f"Excluding the last open candle with close time: {df.iloc[-1]['time_close'].strftime('%d/%m/%Y %H:%M:%S')}")
            df = df.iloc[:-1]

        # Ensure the DataFrame has exactly 'rates_count' rows
        df = df.iloc[-count:]
        df = df.reset_index(drop=True)

        return df

    def get_broker_timezone_offset(self, symbol) -> Any | None:
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            log_warning(f"{symbol} not found, can not call symbol_info().")
            return None

        market_closed = symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED
        if market_closed:
            log_warning(f"Market closed for {symbol}. Cannot get the broker server timezone offset.")
            return None

        # Get the current broker time
        broker_time = symbol_info.time

        # Get the current UTC time
        utc_datetime = now_utc()
        utc_unix_timestamp = dt_to_unix(utc_datetime)

        # Calculate the difference in seconds
        time_diff_seconds = abs(broker_time - utc_unix_timestamp)

        # Convert the difference to hours, rounding up to the nearest hour
        offset_hours = math.ceil(time_diff_seconds / 3600)

        log_debug(
            f"[get_broker_timezone_offset] Broker Unix timestamp: {broker_time}, UTC Unix timestamp: {utc_unix_timestamp}, UTC time: {utc_datetime.strftime('%d/%m/%Y %H:%M:%S')}, Offset: {offset_hours} hours")
        return offset_hours

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

    def close_order(self, order_id: int):
        # Implement order closing logic
        pass

    def get_market_status(self, symbol: str) -> bool:
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            log_warning(f"{symbol} not found, can not call symbol_info().")
            return False
        return not symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED

    def get_market_info(self, symbol: str) -> SymbolInfo | None:
        symbol_info = mt5.symbol_info(symbol)

        if symbol_info is None:
            log_error(f"{symbol} not found, it is not possible to place the order.")
            return None

        if not symbol_info.visible:
            log_error(f"{symbol} it's not visible, I try to enable it.")
            if not mt5.symbol_select(symbol, True):
                log_error("symbol_select() failed, cannot place order.")
                return None

        return SymbolInfo(symbol, symbol_info.volume_min, symbol_info.volume_max, symbol_info.point, symbol_info.trade_mode, symbol_info.trade_contract_size, symbol_info.volume_step,
                          symbol_info.filling_mode)

    def shutdown(self):
        mt5.shutdown()
        log_info("MT5 shutdown successfully")

    # Separated from get_symbol_info since find_filling_mode requires the market to be open.
    def find_filling_mode(self, symbol: str) -> FillingType:
        result = None

        for i in range(2):
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": self.get_market_info(symbol).volume_min,
                "type": mt5.ORDER_TYPE_BUY,
                "price": self.get_symbol_price(symbol).ask,
                "type_filling": i,
                "type_time": mt5.ORDER_TIME_GTC
            }

            result = mt5.order_check(request)
            if result.comment == "Done":
                return FillingType.from_mt5_value(i)

        add_part_log = f" Check response details: {result.comment}" if result is not None else ""
        raise ValueError(f"No valid filling mode found for symbol {symbol}.{add_part_log}")

    def get_symbol_price(self, symbol: str) -> SymbolPrice | None:
        symbol_info_tick = mt5.symbol_info_tick(symbol)

        if symbol_info_tick is None:
            log_error(f"{symbol} not found.")
            return None

        return SymbolPrice(symbol_info_tick.ask, symbol_info_tick.bid)

    def place_order(self, request: TradeOrder) -> RequestResult:
        # Implement order placement logic similar to the previous _place_order_sync
        symbol_info = mt5.symbol_info(request.symbol)
        if symbol_info is None:
            raise Exception(f"Symbol {request.symbol} not found")

        if not symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_FULL:
            raise Exception(f"Market is closed for symbol {request.symbol}, cannot place order.")

        filling_mode = request.filling_mode
        if not filling_mode:
            filling_mode = self.find_filling_mode(request.symbol)
            log_debug(f"Filling mode set to {filling_mode}")

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
            if response.server_response_code == mt5.TRADE_RETCODE_MARKET_CLOSED:
                raise Exception(f"Market is closed for symbol {request.symbol}, cannot place order.")
            else:
                error_message = f"Order send failed, retcode={response.server_response_code}, description={response.comment}"
                log_error(error_message)
                raise Exception(error_message)

        return response

    def get_working_directory(self):
        return mt5.terminal_info().data_path + "\\MQL5\\Files"

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

    def map_deal(self, deal_obj: any, timezone_offset: int) -> Deal:

        """
        Mappa un oggetto TradeDeal o TradePosition di MT5 a un'istanza della dataclass Deal.

        Args:
            symbol: Simbolo del trading (es. 'EURUSD')
            deal_obj: Oggetto TradeDeal o TradePosition recuperato da mt5.history_deals_get() o mt5.positions_get().
            timezone_offset: Offset del fuso orario del broker in ore.

        Returns:
            Istanza della dataclass Deal.
        """
        # Converti il tempo Unix in datetime e applica l'offset del fuso orario
        time = unix_to_datetime(deal_obj.time) - timedelta(hours=timezone_offset) if deal_obj.time else None

        # Classifica il singolo deal
        deal_type_enum, source_enum = self.classify_deal(deal_obj)

        # Creare l'oggetto Deal
        deal = Deal(
            ticket=deal_obj.ticket if hasattr(deal_obj, 'ticket') else None,
            order=deal_obj.order if hasattr(deal_obj, 'order') else None,
            time=time,
            magic=deal_obj.magic if hasattr(deal_obj, 'magic') else None,
            position_id=deal_obj.position_id if hasattr(deal_obj, 'position_id') else None,
            volume=deal_obj.volume if hasattr(deal_obj, 'volume') else None,
            price=deal_obj.price if hasattr(deal_obj, 'price') else None,
            price_open=deal_obj.price_open if hasattr(deal_obj, 'price_open') else None,
            commission=deal_obj.commission if hasattr(deal_obj, 'commission') else None,
            swap=deal_obj.swap if hasattr(deal_obj, 'swap') else None,
            profit=deal_obj.profit if hasattr(deal_obj, 'profit') else None,
            fee=deal_obj.fee if hasattr(deal_obj, 'fee') else None,
            symbol=deal_obj.symbol if hasattr(deal_obj, 'symbol') else None,
            comment=deal_obj.comment if hasattr(deal_obj, 'comment') else None,
            external_id=str(deal_obj.ticket) if hasattr(deal_obj, 'external_id') else None,
            deal_type=deal_type_enum,
            order_source=source_enum
        )

        return deal

    def get_open_positions(self, symbol: str, magic_number: Optional[int] = None) -> List[Deal]:
        """
           Ottiene le posizioni aperte per un dato simbolo, eventualmente filtrate per magic_number.

           :param symbol: Simbolo del trading (es. 'EURUSD')
           :param magic_number: Magic number per filtrare le posizioni (opzionale)
           :return: Lista di istanze della dataclass Position
           """
        deals = mt5.positions_get(symbol=symbol)

        if not deals:
            return []

        # Filtra per magic_number se specificato
        if magic_number is not None:
            deals = [pos for pos in deals if pos.magic == magic_number]

        timezone_offset = self.get_broker_timezone_offset(symbol)

        # Mappa gli oggetti TradePosition a istanze della dataclass Position
        mapped_deals = [self.map_deal(pos, timezone_offset) for pos in deals]

        return mapped_deals

    def close_position(self, position: Deal, comment: Optional[str] = None, magic_number: Optional[int] = None) -> RequestResult:
        filling_mode = self.find_filling_mode(position.symbol)

        close_request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": position.symbol,
            "volume": position.volume,
            "type": mt5.ORDER_TYPE_SELL if position.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY,
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

    def get_positions(self, from_tms: datetime, to_tms: datetime, symbol: Optional[str] = None, magic_number: Optional[int] = None) -> Dict[int, Position]:
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

        positions: Dict[int, Position] = {}

        for deal in filtered_deals:
            try:

                # Aggiungere il deal alla posizione corrispondente
                if deal.position_id not in positions:
                    positions[deal.position_id] = Position(position_id=deal.position_id, symbol=deal.symbol)

                positions[deal.position_id].deals.append(self.map_deal(deal, timezone_offset))
            except Exception as e:
                log_error(f"Errore nel processare il deal ticket {deal.ticket}: {e}")
            continue

        return positions

    def classify_deal(self, deal_obj: any) -> Tuple[DealType, Optional[OrderSource]]:
        """
        Classifica un singolo deal in DealType e OrderSource.

        Args:
            deal_obj: Oggetto TradeDeal o TradePosition recuperato da mt5.history_deals_get() o mt5.positions_get().

        Returns:
            Una tupla contenente il DealType e, se il deal è un'uscita, l'OrderSource.
        """
        # Determinare il DealType basato su 'type'
        deal_type_enum = DEAL_TYPE_MAPPING.get(deal_obj.type, DealType.OTHER)

        # Determinare l'OrderSource se il deal è un'uscita
        source_enum = None
        if hasattr(deal_obj, 'reason'):
            # Se il DealType è SELL o BUY e si tratta di un deal storico, potrebbe essere un'uscita
            source_enum = REASON_MAPPING.get(deal_obj.reason, OrderSource.OTHER)

        return deal_type_enum, source_enum

    def create_positions_dict(self, deals: List[any], timezone_offset: int) -> Dict[int, Position]:
        """
        Crea un dizionario che mappa position_id a oggetti Position contenenti i deals storici.

        Args:
            deals: Lista di oggetti TradeDeal recuperati da mt5.history_deals_get().
            timezone_offset: Offset del fuso orario del broker in ore.

        Returns:
            Un dizionario che mappa position_id a oggetti Position.
        """
        positions: Dict[int, Position] = {}

        for deal_obj in deals:
            try:
                # Filtra i deals senza position_id (es. CREDIT, BALANCE, ecc.)
                if deal_obj.position_id == 0:
                    continue

                # Mappa il TradeDeal a un'istanza di Deal
                deal = self.map_deal(deal_obj, timezone_offset)

                # Aggiungere il deal alla posizione corrispondente
                if deal.position_id not in positions:
                    positions[deal.position_id] = Position(position_id=deal.position_id, symbol=deal.symbol)

                positions[deal.position_id].deals.append(deal)

            except Exception as e:
                log_error(f"Errore nel processare il deal ticket {deal_obj.ticket}: {e}")
                continue

        return positions
