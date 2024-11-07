from dataclasses import field, dataclass
from typing import List

from datao.Deal import Deal


@dataclass
class Position:
    position_id: int
    symbol: str
    deals: List[Deal] = field(default_factory=list)