from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, List, Dict

from pandas import Series

from datao import SymbolInfo, TradeOrder, SymbolPrice
from datao.Deal import Deal
from datao.Position import Position
from datao.RequestResult import RequestResult


class BrokerAPI(ABC):

    @abstractmethod
    def get_last_candles(self, symbol: str, timeframe: int, count: int) -> Series:
        pass

    @abstractmethod
    def get_symbol_price(self, symbol: str) -> SymbolPrice:
        pass

    @abstractmethod
    def place_order(self, request: TradeOrder) -> RequestResult:
        pass

    @abstractmethod
    def close_order(self, order_id: int):
        pass

    @abstractmethod
    def get_market_status(self, symbol: str) -> bool:
        pass

    @abstractmethod
    def get_market_info(self, symbol: str) -> SymbolInfo:
        pass

    @abstractmethod
    def get_broker_timezone_offset(self, symbol) -> int:
        pass

    @abstractmethod
    def get_working_directory(self) -> str:
        pass

    @abstractmethod
    def shutdown(self):
        pass

    @abstractmethod
    def get_account_balance(self) -> float:
        pass

    @abstractmethod
    def get_account_leverage(self) -> float:
        pass

    @abstractmethod
    def get_open_positions(self, symbol: str) -> dict[int, Position]:
        pass

    @abstractmethod
    def get_historical_positions(self, from_tms: datetime, to_tms: datetime, symbol: Optional[str] = None, magic_number: Optional[int] = None) -> Dict[int, List[Deal]]:
        pass

    @abstractmethod
    def close_position(self, position: Deal, comment: Optional[str] = None, magic_number: Optional[int] = None) -> RequestResult:
        pass
