from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from utils.enums import DealType, OrderSource


@dataclass
class Deal:
    ticket: int
    order: int
    time: datetime
    magic: int
    position_id: int
    volume: float
    price: float
    commission: float
    swap: float
    profit: float
    fee: float
    symbol: str
    comment: str
    external_id: str
    deal_type: DealType = DealType.OTHER
    order_source: Optional[OrderSource] = None
