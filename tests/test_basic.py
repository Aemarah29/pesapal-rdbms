from rdbms.storage import Storage
from rdbms.table import MiniDB
from rdbms.catalog import TableSchema, Column
from rdbms.types import ColumnType


def test_insert_unique_pk():
    # Use separate test data directory so we don't pollute main /data
    s = Storage("data_test")

    # Clean previous test run files
    if s.catalog_path.exists():
        s.catalog_path.unlink()

    tp = s.table_path("users")
    if tp.exists():
        tp.unlink()

    db = MiniDB(s)

    # Create a table schema: users(id PK, email UNIQUE, name)
    db.create_table(
        TableSchema(
            name="users",
            columns=[
                Column("id", ColumnType.INT, primary_key=True),
                Column("email", ColumnType.TEXT, unique=True),
                Column("name", ColumnType.TEXT),
            ],
        )
    )

    # Insert a row
    db.insert("users", {"id": 1, "email": "a@b.com", "name": "Aser"})

    # Query it back
    rows = db.select("users", ["*"], where=[("id", "=", 1)])
    assert rows[0]["email"] == "a@b.com"
