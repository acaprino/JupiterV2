from abc import ABC, abstractmethod

from pandas import Series

from datao import SymbolInfo, TradeOrder, SymbolPrice


class BrokerAPI(ABC):

    @abstractmethod
    def get_last_candles(self, symbol: str, timeframe: int, count: int) -> Series:
        pass

    @abstractmethod
    def get_symbol_price(self, symbol: str) -> SymbolPrice:
        pass

    @abstractmethod
    def place_order(self, request: TradeOrder):
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

