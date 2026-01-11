from __future__ import annotations
from enum import Enum
from typing import Any


class ColumnType(str, Enum):
    INT = "INT"
    TEXT = "TEXT"
    BOOL = "BOOL"


def coerce_value(col_type: ColumnType, value: Any) -> Any:
    """
    Convert user-provided values into the correct Python type
    based on the column definition.
    """
    if value is None:
        return None

    if col_type == ColumnType.INT:
        if isinstance(value, bool):
            raise ValueError("BOOL cannot be used where INT is expected")
        return int(value)

    if col_type == ColumnType.TEXT:
        return str(value)

    if col_type == ColumnType.BOOL:
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        if s in ("true", "1", "yes"):
            return True
        if s in ("false", "0", "no"):
            return False
        raise ValueError(f"Invalid BOOL value: {value}")

    raise ValueError(f"Unknown column type: {col_type}")
