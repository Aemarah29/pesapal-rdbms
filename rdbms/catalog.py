from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Set

from .types import ColumnType


@dataclass(frozen=True)
class Column:
    """
    Represents a single column in a table:
    - name: column name
    - col_type: INT / TEXT / BOOL
    - primary_key: if True, uniquely identifies rows
    - unique: if True, values must be unique
    - not_null: if True, value cannot be None
    """
    name: str
    col_type: ColumnType
    primary_key: bool = False
    unique: bool = False
    not_null: bool = False

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["col_type"] = self.col_type.value
        return d

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Column":
        return Column(
            name=d["name"],
            col_type=ColumnType(d["col_type"]),
            primary_key=bool(d.get("primary_key", False)),
            unique=bool(d.get("unique", False)),
            not_null=bool(d.get("not_null", False)),
        )


@dataclass
class TableSchema:
    """
    Represents a table definition: name + list of columns.
    """
    name: str
    columns: List[Column]

    def pk_column(self) -> Optional[str]:
        pks = [c.name for c in self.columns if c.primary_key]
        if len(pks) == 0:
            return None
        if len(pks) > 1:
            raise ValueError("Only one PRIMARY KEY column is supported.")
        return pks[0]

    def unique_columns(self) -> Set[str]:
        """
        In this DB, PRIMARY KEY implies UNIQUE.
        """
        return {c.name for c in self.columns if c.unique or c.primary_key}

    def column_map(self) -> Dict[str, Column]:
        """
        Convenience: column name -> Column object
        """
        return {c.name: c for c in self.columns}

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "columns": [c.to_dict() for c in self.columns]}

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "TableSchema":
        return TableSchema(
            name=d["name"],
            columns=[Column.from_dict(x) for x in d["columns"]],
        )


class Catalog:
    """
    Stores all known table schemas.
    Think of it like the DB's 'system metadata'.
    """
    def __init__(self) -> None:
        self.tables: Dict[str, TableSchema] = {}

    def add_table(self, schema: TableSchema) -> None:
        if schema.name in self.tables:
            raise ValueError(f"Table already exists: {schema.name}")
        # validate pk (ensures max one PK)
        _ = schema.pk_column()
        self.tables[schema.name] = schema

    def get(self, table_name: str) -> TableSchema:
        if table_name not in self.tables:
            raise KeyError(f"Unknown table: {table_name}")
        return self.tables[table_name]

    def list_tables(self) -> List[str]:
        return sorted(self.tables.keys())

    def to_dict(self) -> Dict[str, Any]:
        return {"tables": {name: schema.to_dict() for name, schema in self.tables.items()}}

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Catalog":
        cat = Catalog()
        for name, schema_d in d.get("tables", {}).items():
            cat.tables[name] = TableSchema.from_dict(schema_d)
        return cat
