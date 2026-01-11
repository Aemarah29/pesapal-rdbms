from __future__ import annotations
from typing import Any, Dict, Optional


class HashIndex:
    """
    Basic in-memory index:
      value -> _rid

    Used for PRIMARY KEY and UNIQUE columns.
    """
    def __init__(self) -> None:
        self.map: Dict[Any, int] = {}

    def add(self, value: Any, rid: int) -> None:
        if value in self.map:
            raise ValueError("Unique constraint violation")
        self.map[value] = rid

    def remove(self, value: Any) -> None:
        self.map.pop(value, None)

    def get(self, value: Any) -> Optional[int]:
        return self.map.get(value)
