from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple

from .catalog import TableSchema, Catalog
from .index import HashIndex
from .storage import Storage
from .types import coerce_value

Where = List[Tuple[str, str, Any]]  # Example: [("id","=",1)]


class MiniDB:
    """
    Minimal database engine:
      - loads/saves catalog (schemas)
      - stores rows in JSONL files
      - enforces PRIMARY KEY and UNIQUE via HashIndex
      - supports INSERT, SELECT, UPDATE, DELETE (simple WHERE)
    """

    def __init__(self, storage: Storage) -> None:
        self.storage = storage
        self.catalog: Catalog = self.storage.load_catalog()

        # indexes[table_name][column_name] = HashIndex
        self.indexes: Dict[str, Dict[str, HashIndex]] = {}
        self._rebuild_all_indexes()

    # --------- Index building ---------

    def _rebuild_all_indexes(self) -> None:
        """
        When DB starts, rebuild indexes by scanning table files.
        """
        for t in self.catalog.list_tables():
            self._rebuild_indexes_for_table(t)

    def _rebuild_indexes_for_table(self, table: str) -> None:
        schema = self.catalog.get(table)
        idx_cols = schema.unique_columns()

        self.indexes[table] = {c: HashIndex() for c in idx_cols}
        rows = self.storage.read_rows(table)

        for r in rows:
            rid = r["_rid"]
            for c in idx_cols:
                v = r.get(c)
                if v is None:
                    continue
                self.indexes[table][c].add(v, rid)

    # --------- DDL ---------

    def create_table(self, schema: TableSchema) -> None:
        """
        Registers a table schema in the catalog and saves it to disk.
        """
        self.catalog.add_table(schema)
        self.storage.save_catalog(self.catalog)
        self.indexes[schema.name] = {c: HashIndex() for c in schema.unique_columns()}

    # --------- DML ---------

    def insert(self, table: str, row: Dict[str, Any]) -> int:
        """
        Inserts a row into table, enforcing types and constraints.
        Returns internal row id (_rid).
        """
        schema = self.catalog.get(table)
        cmap = schema.column_map()

        # 1) Type coercion + NOT NULL validation
        coerced: Dict[str, Any] = {}
        for col_name, col in cmap.items():
            coerced[col_name] = coerce_value(col.col_type, row.get(col_name))
            if col.not_null and coerced[col_name] is None:
                raise ValueError(f"{col_name} cannot be NULL")

        # 2) Enforce UNIQUE / PK using indexes (fast)
        for ucol in schema.unique_columns():
            v = coerced.get(ucol)
            if v is None:
                continue
            idx = self.indexes[table][ucol]
            if idx.get(v) is not None:
                raise ValueError(f"Unique constraint violation on {ucol}={v}")

        # 3) Persist row (append to JSONL)
        rid = self.storage.append_row(table, coerced)

        # 4) Update indexes
        for ucol in schema.unique_columns():
            v = coerced.get(ucol)
            if v is None:
                continue
            self.indexes[table][ucol].add(v, rid)

        return rid

    # --------- Querying ---------

    def _match_where(self, row: Dict[str, Any], where: Optional[Where]) -> bool:
        """
        For now: WHERE supports only equality tests with AND semantics.
        """
        if not where:
            return True
        for col, op, val in where:
            if op != "=":
                raise ValueError("Only '=' is supported in WHERE for now")
            if row.get(col) != val:
                return False
        return True

    def select(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        where: Optional[Where] = None,
    ) -> List[Dict[str, Any]]:
        """
        Returns list of dict rows.
        columns: list of column names or ["*"]
        """
        rows = self.storage.read_rows(table)

        # Small optimization: WHERE col=val with indexed col
        if where and len(where) == 1:
            col, op, val = where[0]
            if op == "=" and col in self.indexes.get(table, {}):
                rid = self.indexes[table][col].get(val)
                if rid is None:
                    return []
                rows = [r for r in rows if r["_rid"] == rid]

        out: List[Dict[str, Any]] = []
        for r in rows:
            if self._match_where(r, where):
                if columns is None or columns == ["*"]:
                    out.append({k: v for k, v in r.items() if k != "_rid"})
                else:
                    out.append({c: r.get(c) for c in columns})
        return out

    def update(
        self,
        table: str,
        updates: Dict[str, Any],
        where: Optional[Where] = None,
    ) -> int:
        """
        Update rows that match WHERE. Returns number of rows updated.

        Simple implementation:
        - reads all rows
        - applies updates
        - rewrites table file
        - rebuilds indexes

        This is not transactional (fine for this challenge).
        """
        schema = self.catalog.get(table)
        cmap = schema.column_map()

        rows = self.storage.read_rows(table)
        new_rows: List[Dict[str, Any]] = []
        updated_count = 0

        for r in rows:
            if self._match_where(r, where):
                updated = dict(r)

                for col_name, new_val in updates.items():
                    if col_name not in cmap:
                        raise ValueError(f"Unknown column: {col_name}")
                    col = cmap[col_name]
                    updated[col_name] = coerce_value(col.col_type, new_val)
                    if col.not_null and updated[col_name] is None:
                        raise ValueError(f"{col_name} cannot be NULL")

                new_rows.append(updated)
                updated_count += 1
            else:
                new_rows.append(r)

        self.storage.rewrite_rows(table, new_rows)
        self._rebuild_indexes_for_table(table)
        return updated_count

    def delete(self, table: str, where: Optional[Where] = None) -> int:
        """
        Delete rows that match WHERE. Returns number of rows deleted.

        Simple implementation:
        - filters rows
        - rewrites table file
        - rebuilds indexes
        """
        rows = self.storage.read_rows(table)
        kept: List[Dict[str, Any]] = []
        deleted = 0

        for r in rows:
            if self._match_where(r, where):
                deleted += 1
            else:
                kept.append(r)

        self.storage.rewrite_rows(table, kept)
        self._rebuild_indexes_for_table(table)
        return deleted
