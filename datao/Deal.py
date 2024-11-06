from dataclasses import dataclass
from datetime import datetime


@dataclass
class Deal:
    ticket: int
    time: datetime  # Opening time as a datetime object
    time_msc: int  # Milliseconds of the opening time
    time_update: datetime  # Last update time as a datetime object
    time_update_msc: int  # Milliseconds of the last update
    type: int
    magic: int
    identifier: int
    reason: int
    volume: float
    price_open: float
    sl: float
    tp: float
    price_current: float
    swap: float
    profit: float
    symbol: str
    comment: str
    external_id: str
    position_id: int
