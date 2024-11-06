from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Position:
    ticket: int
    time: datetime  # Tempo di apertura come oggetto datetime
    time_msc: int  # Millisecondi del tempo di apertura
    time_update: datetime  # Ultima volta di aggiornamento come oggetto datetime
    time_update_msc: int  # Millisecondi dell'ultimo aggiornamento
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
