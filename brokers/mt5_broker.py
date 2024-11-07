import math
import threading
from datetime import timedelta, datetime, timezone
from typing import Any, Optional, List, Dict

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
from utils.enums import Timeframe, FillingType, OpType, DealType, ExitReason
from utils.logger import log_warning, log_error, log_info, log_debug
from utils.utils_functions import now_utc, dt_to_unix, unix_to_datetime

DEAL_TYPE_MAPPING = {
    0: DealType.BUY,  # DEAL_TYPE_BUY
    1: DealType.SELL,  # DEAL_TYPE_SELL
    # Altri tipi vengono classificati come 'OTHER'
}

# Mappatura dei reason a ExitReason
REASON_MAPPING = {
    4: ExitReason.STOP_LOSS,  # DEAL_REASON_SL
    5: ExitReason.TAKE_PROFIT,  # DEAL_REASON_TP
    0: ExitReason.MANUAL,  # DEAL_REASON_CLIENT
    1: ExitReason.MANUAL,  # DEAL_REASON_MOBILE
    2: ExitReason.MANUAL,  # DEAL_REASON_WEB
    3: ExitReason.BOT,  # DEAL_REASON_EXPERT
    6: ExitReason.MANUAL,  # DEAL_REASON_SO (Stop Out)
    7: ExitReason.MANUAL,  # DEAL_REASON_ROLLOVER
    8: ExitReason.MANUAL,  # DEAL_REASON_VMARGIN
    9: ExitReason.MANUAL,  # DEAL_REASON_SPLIT
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

    def map_trade_position(self, symbol, trade_pos: mt5.TradePosition) -> Deal:
        """
        Mappa un oggetto TradePosition di MetaTrader5 a un'istanza della dataclass Deal.
        """
        # Conversione dei timestamp in oggetti datetime con precisione ai millisecondi

        timezone_offset = self.get_broker_timezone_offset(symbol)
        log_debug(f"Timezone offset is {timezone_offset} hours")

        time_open = datetime.fromtimestamp(trade_pos.time, tz=timezone.utc).replace(microsecond=(trade_pos.time_msc % 1000) * 1000) - timedelta(hours=timezone_offset)
        time_update = datetime.fromtimestamp(trade_pos.time_update, tz=timezone.utc).replace(microsecond=(trade_pos.time_update_msc % 1000) * 1000) - timedelta(hours=timezone_offset)

        return Deal(
            ticket=trade_pos.ticket,
            time=time_open,
            time_msc=trade_pos.time_msc,
            time_update=time_update,
            time_update_msc=trade_pos.time_update_msc,
            type=trade_pos.type,
            magic=trade_pos.magic,
            identifier=trade_pos.identifier,
            reason=trade_pos.reason,
            volume=trade_pos.volume,
            price_open=trade_pos.price_open,
            sl=trade_pos.sl,
            tp=trade_pos.tp,
            price_current=trade_pos.price_current,
            swap=trade_pos.swap,
            profit=trade_pos.profit,
            symbol=trade_pos.symbol,
            comment=trade_pos.comment,
            external_id=trade_pos.external_id,
        )

    def get_open_positions(self, symbol: str, magic_number: Optional[int] = None) -> List[Deal]:
        """
           Ottiene le posizioni aperte per un dato simbolo, eventualmente filtrate per magic_number.

           :param symbol: Simbolo del trading (es. 'EURUSD')
           :param magic_number: Magic number per filtrare le posizioni (opzionale)
           :return: Lista di istanze della dataclass Position
           """
        positions = mt5.positions_get(symbol=symbol)

        if not positions:
            return []

        # Filtra per magic_number se specificato
        if magic_number is not None:
            positions = [pos for pos in positions if pos.magic == magic_number]

        df = pd.DataFrame(list(positions), columns=positions[0]._asdict().keys())
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df.drop(['time_update', 'time_msc', 'time_update_msc', 'external_id'], axis=1, inplace=True)
        print(df)

        # Mappa gli oggetti TradePosition a istanze della dataclass Position
        mapped_positions = [self.map_trade_position(symbol, pos) for pos in positions]

        return mapped_positions

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

    def get_deals(self, from_tms: datetime, to_tms: datetime, symbol: Optional[str] = None, magic_number: Optional[int] = None) -> Dict[int, List[Any]]:
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
                # Assumiamo che i deals passati siano già filtrati (es. position_id != 0)

                # Classifica il singolo deal
                deal_type_enum, exit_reason_enum = self.classify_deal(deal)

                # Creare l'oggetto Deal
                deal_obj = Deal(
                    ticket=deal.ticket,
                    order=deal.order,
                    time=unix_to_datetime(deal.time) - timedelta(hours=timezone_offset) if deal.time is not None else None,
                    time_msc=deal.time_msc,
                    type=deal.type,
                    entry=deal.entry,
                    magic=deal.magic,
                    position_id=deal.position_id,
                    reason=deal.reason,
                    volume=deal.volume,
                    price=deal.price,
                    commission=deal.commission,
                    swap=deal.swap,
                    profit=deal.profit,
                    fee=deal.fee,
                    symbol=deal.symbol,
                    comment=deal.comment,
                    external_id=deal.external_id,
                    deal_type=deal_type_enum,
                    exit_reason=exit_reason_enum
                )

                # Aggiungere il deal alla posizione corrispondente
                if deal.position_id not in positions:
                    positions[deal.position_id] = Position(position_id=deal.position_id, symbol=deal.symbol)

                positions[deal.position_id].deals.append(deal_obj)
            except Exception as e:
                log_error(f"Errore nel processare il deal ticket {deal.ticket}: {e}")
            continue

        return positions

    def classify_deal(self, deal) -> (DealType, Optional[ExitReason]):
        """
        Classifica un singolo deal in DealType e ExitReason.

        Args:
            deal: Oggetto deal recuperato da mt5.history_deals_get().

        Returns:
            Una tupla contenente il DealType e, se il deal è un'uscita, l'ExitReason.
        """
        # Determinare il DealType basato su 'type'
        deal_type_enum = DEAL_TYPE_MAPPING.get(deal.type, DealType.OTHER)

        # Determinare l'ExitReason se il deal è un'uscita
        exit_reason_enum = None
        if deal_type_enum in [DealType.SELL, DealType.BUY]:
            # Se il DealType è SELL o BUY, potrebbe essere un'uscita
            exit_reason_enum = REASON_MAPPING.get(deal.reason, ExitReason.OTHER)

        return deal_type_enum, exit_reason_enum
