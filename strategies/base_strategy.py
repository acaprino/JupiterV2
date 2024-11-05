# strategies/base_strategy.py

from abc import ABC, abstractmethod
import asyncio
from typing import Dict, Optional

from datao import TradeOrder


class TradingStrategy(ABC):
    """
    Classe base astratta per le strategie di trading.
    """

    @abstractmethod
    async def bootstrap(self):
        """
        Metodo chiamato per inizializzare la strategia prima di processare le prime candele.
        """
        pass

    @abstractmethod
    async def on_new_candle(self, candle: dict):
        """
        Metodo chiamato quando una nuova candela è disponibile.
        """
        pass

    @abstractmethod
    async def on_market_status_change(self, is_open: bool, closing_time: Optional[float], opening_time: Optional[float]):
        """
        Metodo chiamato quando lo stato del mercato cambia.
        """
        pass

    @abstractmethod
    async def on_deal_closed(self, deal_info: dict):
        """
        Metodo chiamato quando un deal viene chiuso.
        """
        pass

    @abstractmethod
    async def on_economic_event(self, event_info: dict):
        """
        Metodo chiamato quando si verifica un evento economico.
        """
        pass
