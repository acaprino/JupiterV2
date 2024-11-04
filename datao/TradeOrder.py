from dataclasses import dataclass
from typing import Optional

from utils.enums import OpType, FillingType


@dataclass
class TradeOrder:
    order_type: OpType
    symbol: str
    order_price: float
    volume: float
    sl: float  # Stop Loss
    tp: float  # Take Profit
    comment: str
    filling_mode: Optional[FillingType] = None
    magic_number: Optional[int] = None
