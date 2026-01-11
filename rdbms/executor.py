from __future__ import annotations
from typing import Any, Dict, List

from .catalog import Column, TableSchema
from .table import MiniDB
from .parser import CreateTable, Insert, Select
from .types import ColumnType


def execute(db: MiniDB, stmt):
    """
    Execute a parsed statement against the database.
    Returns a result object (usually list of rows for SELECT, or a message).
    """
    if isinstance(stmt, CreateTable):
        cols: List[Column] = []
        for name, ctype, pk, unique, not_null in stmt.columns:
            cols.append(Column(name=name, col_type=ctype, primary_key=pk, unique=unique, not_null=not_null))
        schema = TableSchema(name=stmt.table, columns=cols)
        db.create_table(schema)
        return f"OK (table '{stmt.table}' created)"

    if isinstance(stmt, Insert):
        row: Dict[str, Any] = {}
        for c, v in zip(stmt.columns, stmt.values):
            row[c] = v
        rid = db.insert(stmt.table, row)
        return f"OK (inserted, _rid={rid})"

    if isinstance(stmt, Select):
        rows = db.select(stmt.table, stmt.columns, where=stmt.where)
        return rows

    raise ValueError(f"Unknown statement type: {type(stmt)}")
