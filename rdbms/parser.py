from __future__ import annotations
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple
import re

from .types import ColumnType


# --------- AST (Parsed command objects) ---------

@dataclass
class CreateTable:
    table: str
    columns: List[Tuple[str, ColumnType, bool, bool, bool]]  # (name, type, pk, unique, not_null)


@dataclass
class Insert:
    table: str
    columns: List[str]
    values: List[Any]


@dataclass
class Select:
    table: str
    columns: List[str]  # ["*"] allowed
    where: Optional[List[Tuple[str, str, Any]]]  # [("id","=",1), ...]


@dataclass
class Update:
    table: str
    assignments: List[Tuple[str, Any]]  # [("name","John"), ...]
    where: Optional[List[Tuple[str, str, Any]]]


@dataclass
class Delete:
    table: str
    where: Optional[List[Tuple[str, str, Any]]]


# --------- Helpers ---------

def _split_commas_outside_parens(s: str) -> List[str]:
    parts = []
    buf = []
    depth = 0
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append("".join(buf).strip())
    return [p for p in parts if p]


def _parse_literal(tok: str) -> Any:
    tok = tok.strip()
    # quoted string
    if (tok.startswith('"') and tok.endswith('"')) or (tok.startswith("'") and tok.endswith("'")):
        return tok[1:-1]
    # bool
    low = tok.lower()
    if low in ("true", "false"):
        return low == "true"
    # int
    if re.fullmatch(r"-?\d+", tok):
        return int(tok)
    # NULL
    if low == "null":
        return None
    # fallback: bare identifier string
    return tok


def _parse_where(where_raw: str) -> List[Tuple[str, str, Any]]:
    # support: col = value AND col2 = value2 ...
    conditions = [x.strip() for x in re.split(r"\s+and\s+", where_raw, flags=re.IGNORECASE) if x.strip()]
    parsed = []
    for cond in conditions:
        if "=" not in cond:
            raise ValueError("Only '=' supported in WHERE")
        left, right = cond.split("=", 1)
        parsed.append((left.strip(), "=", _parse_literal(right.strip())))
    return parsed


# --------- Statement regex ---------

_CREATE_RE = re.compile(r"^\s*create\s+table\s+([a-zA-Z_]\w*)\s*\((.*)\)\s*$", re.IGNORECASE | re.DOTALL)

_INSERT_RE = re.compile(
    r"^\s*insert\s+into\s+([a-zA-Z_]\w*)\s*\((.*?)\)\s*values\s*\((.*?)\)\s*$",
    re.IGNORECASE | re.DOTALL,
)

_SELECT_RE = re.compile(
    r"^\s*select\s+(.*?)\s+from\s+([a-zA-Z_]\w*)(?:\s+where\s+(.*))?\s*$",
    re.IGNORECASE | re.DOTALL,
)

_UPDATE_RE = re.compile(
    r"^\s*update\s+([a-zA-Z_]\w*)\s+set\s+(.*?)(?:\s+where\s+(.*))?\s*$",
    re.IGNORECASE | re.DOTALL,
)

_DELETE_RE = re.compile(
    r"^\s*delete\s+from\s+([a-zA-Z_]\w*)(?:\s+where\s+(.*))?\s*$",
    re.IGNORECASE | re.DOTALL,
)


def parse_statement(sql: str):
    """
    Parse a single SQL-like statement WITHOUT the trailing semicolon.
    Returns one of: CreateTable, Insert, Select, Update, Delete.
    """
    sql = sql.strip()

    m = _CREATE_RE.match(sql)
    if m:
        table = m.group(1)
        cols_raw = m.group(2).strip()
        col_defs = _split_commas_outside_parens(cols_raw)

        columns: List[Tuple[str, ColumnType, bool, bool, bool]] = []
        for cdef in col_defs:
            parts = cdef.strip().split()
            if len(parts) < 2:
                raise ValueError(f"Bad column definition: {cdef}")

            name = parts[0]
            ctype = ColumnType(parts[1].upper())

            rest = " ".join(parts[2:]).upper()
            pk = "PRIMARY KEY" in rest
            unique = "UNIQUE" in rest
            not_null = "NOT NULL" in rest

            columns.append((name, ctype, pk, unique, not_null))

        return CreateTable(table=table, columns=columns)

    m = _INSERT_RE.match(sql)
    if m:
        table = m.group(1)
        cols = [c.strip() for c in m.group(2).split(",") if c.strip()]
        vals = _split_commas_outside_parens(m.group(3))
        values = [_parse_literal(v) for v in vals]

        if len(cols) != len(values):
            raise ValueError("INSERT columns count must match VALUES count")

        return Insert(table=table, columns=cols, values=values)

    m = _SELECT_RE.match(sql)
    if m:
        cols_raw = m.group(1).strip()
        table = m.group(2).strip()
        where_raw = m.group(3)

        cols = ["*"] if cols_raw == "*" else [c.strip() for c in cols_raw.split(",") if c.strip()]
        where = _parse_where(where_raw) if where_raw else None
        return Select(table=table, columns=cols, where=where)

    m = _UPDATE_RE.match(sql)
    if m:
        table = m.group(1).strip()
        assigns_raw = m.group(2).strip()
        where_raw = m.group(3)

        assigns_parts = _split_commas_outside_parens(assigns_raw)
        assignments: List[Tuple[str, Any]] = []
        for a in assigns_parts:
            if "=" not in a:
                raise ValueError("Bad SET assignment (expected col=value)")
            left, right = a.split("=", 1)
            assignments.append((left.strip(), _parse_literal(right.strip())))

        where = _parse_where(where_raw) if where_raw else None
        return Update(table=table, assignments=assignments, where=where)

    m = _DELETE_RE.match(sql)
    if m:
        table = m.group(1).strip()
        where_raw = m.group(2)
        where = _parse_where(where_raw) if where_raw else None
        return Delete(table=table, where=where)

    raise ValueError("Unrecognized statement. Supported: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE.")


def split_sql_script(script: str) -> List[str]:
    """
    Split input into statements separated by semicolons.
    Assumes no semicolons inside string literals for v1.
    """
    statements = []
    buf = []
    in_single = False
    in_double = False

    for ch in script:
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double

        if ch == ";" and not in_single and not in_double:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
        else:
            buf.append(ch)

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements
