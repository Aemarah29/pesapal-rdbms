from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from .catalog import Catalog


class Storage:
    """
    File-based persistence:
      - data/catalog.json for schema metadata
      - data/<table>.jsonl for table rows (one JSON row per line)
    """
    def __init__(self, data_dir: str = "data") -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.catalog_path = self.data_dir / "catalog.json"

    # ----- Catalog persistence -----

    def load_catalog(self) -> Catalog:
        if not self.catalog_path.exists():
            return Catalog()
        data = json.loads(self.catalog_path.read_text(encoding="utf-8"))
        return Catalog.from_dict(data)

    def save_catalog(self, catalog: Catalog) -> None:
        self.catalog_path.write_text(
            json.dumps(catalog.to_dict(), indent=2),
            encoding="utf-8",
        )

    # ----- Row persistence -----

    def table_path(self, table: str) -> Path:
        return self.data_dir / f"{table}.jsonl"

    def count_rows(self, table: str) -> int:
        path = self.table_path(table)
        if not path.exists():
            return 0
        with path.open("r", encoding="utf-8") as f:
            return sum(1 for _ in f)

    def append_row(self, table: str, row: Dict[str, Any]) -> int:
        """
        Appends a row to the table file, returns a row id (_rid).
        """
        path = self.table_path(table)
        rid = self.count_rows(table)
        row = dict(row)
        row["_rid"] = rid  # internal row id
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")
        return rid

    def read_rows(self, table: str) -> List[Dict[str, Any]]:
        path = self.table_path(table)
        if not path.exists():
            return []
        rows: List[Dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        return rows

    def rewrite_rows(self, table: str, rows: Iterable[Dict[str, Any]]) -> None:
        """
        Rewrites the entire table file (useful for UPDATE/DELETE later).
        """
        path = self.table_path(table)
        with path.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
