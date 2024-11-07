from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from utils.enums import DealType, ExitReason


@dataclass
class Deal:
    ticket: int
    order: int
    time: datetime
    time_msc: int
    type: int
    entry: int
    magic: int
    position_id: int
    reason: int
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
    exit_reason: Optional[ExitReason] = None  # Solo per uscite
