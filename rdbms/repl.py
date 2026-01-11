from __future__ import annotations
from typing import Any
import json

from .storage import Storage
from .table import MiniDB
from .parser import split_sql_script, parse_statement
from .executor import execute


def _print_result(result: Any) -> None:
    if isinstance(result, list):
        # pretty print rows
        if not result:
            print("(0 rows)")
            return
        print(json.dumps(result, indent=2))
        print(f"({len(result)} rows)")
    else:
        print(result)


def main() -> None:
    storage = Storage("data")
    db = MiniDB(storage)

    print("MiniRDBMS REPL. End statements with ';'. Type '.exit' to quit.")
    buf = ""

    while True:
        prompt = "db> " if not buf else "... "
        line = input(prompt)

        if not buf and line.strip().lower() == ".exit":
            break

        buf += line + "\n"

        # only run when we see a semicolon
        if ";" not in buf:
            continue

        try:
            statements = split_sql_script(buf)
            for s in statements:
                stmt = parse_statement(s)
                result = execute(db, stmt)
                _print_result(result)
        except Exception as e:
            print(f"ERROR: {e}")

        buf = ""


if __name__ == "__main__":
    main()
