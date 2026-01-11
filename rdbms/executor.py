from __future__ import annotations
from typing import Any, Dict, List

from .catalog import Column, TableSchema
from .table import MiniDB
from .parser import CreateTable, Insert, Select, Update, Delete


def execute(db: MiniDB, stmt):
    """
    Execute a parsed statement against the database.
    Returns:
      - list[dict] for SELECT
      - string message for others
    """
    if isinstance(stmt, CreateTable):
        cols: List[Column] = []
        for name, ctype, pk, unique, not_null in stmt.columns:
            cols.append(
                Column(
                    name=name,
                    col_type=ctype,
                    primary_key=pk,
                    unique=unique,
                    not_null=not_null,
                )
            )
        schema = TableSchema(name=stmt.table, columns=cols)
        db.create_table(schema)
        return f"OK (table '{stmt.table}' created)"

    if isinstance(stmt, Insert):
        row: Dict[str, Any] = {c: v for c, v in zip(stmt.columns, stmt.values)}
        rid = db.insert(stmt.table, row)
        return f"OK (inserted, _rid={rid})"

    if isinstance(stmt, Select):
        return db.select(stmt.table, stmt.columns, where=stmt.where)

    if isinstance(stmt, Update):
        updates: Dict[str, Any] = {c: v for c, v in stmt.assignments}
        n = db.update(stmt.table, updates, where=stmt.where)
        return f"OK (updated {n} rows)"

    if isinstance(stmt, Delete):
        n = db.delete(stmt.table, where=stmt.where)
        return f"OK (deleted {n} rows)"

    raise ValueError(f"Unknown statement type: {type(stmt)}")
