from __future__ import annotations
from flask import Flask, request, redirect, render_template_string

from rdbms.storage import Storage
from rdbms.table import MiniDB
from rdbms.catalog import TableSchema, Column
from rdbms.types import ColumnType

app = Flask(__name__)

# Use a separate data folder for the web demo so it doesn't mix with your REPL data
storage = Storage("data_web")
db = MiniDB(storage)

TABLE = "tasks"

HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>MiniRDBMS Web Demo</title>
</head>
<body>
  <h1>MiniRDBMS Web Demo (CRUD)</h1>

  <h2>Add task</h2>
  <form method="post" action="/add">
    <label>ID (int):</label>
    <input name="id" required />
    <br />
    <label>Title (text):</label>
    <input name="title" required />
    <br />
    <label>Done (true/false):</label>
    <input name="done" value="false" />
    <br />
    <button type="submit">Add</button>
  </form>

  <h2>Tasks</h2>
  {% if tasks|length == 0 %}
    <p>No tasks yet.</p>
  {% else %}
    <table border="1" cellpadding="6" cellspacing="0">
      <tr>
        <th>id</th>
        <th>title</th>
        <th>done</th>
        <th>actions</th>
      </tr>
      {% for t in tasks %}
        <tr>
          <td>{{ t["id"] }}</td>
          <td>{{ t["title"] }}</td>
          <td>{{ t["done"] }}</td>
          <td>
            <form method="post" action="/delete" style="display:inline;">
              <input type="hidden" name="id" value="{{ t['id'] }}" />
              <button type="submit">Delete</button>
            </form>
          </td>
        </tr>
      {% endfor %}
    </table>
  {% endif %}

  <p style="margin-top: 24px;">
    Data is stored using your RDBMS in <code>data_web/</code>.
  </p>
</body>
</html>
"""


def ensure_table() -> None:
    # Create the table only if it doesn't exist
    if TABLE in db.catalog.tables:
        return
    db.create_table(
        TableSchema(
            name=TABLE,
            columns=[
                Column("id", ColumnType.INT, primary_key=True),
                Column("title", ColumnType.TEXT, not_null=True),
                Column("done", ColumnType.BOOL, not_null=True),
            ],
        )
    )


@app.get("/")
def home():
    ensure_table()
    tasks = db.select(TABLE, ["*"])
    return render_template_string(HTML, tasks=tasks)


@app.post("/add")
def add():
    ensure_table()
    task_id = request.form.get("id")
    title = request.form.get("title")
    done = request.form.get("done", "false")

    db.insert(TABLE, {"id": task_id, "title": title, "done": done})
    return redirect("/")


@app.post("/delete")
def delete():
    ensure_table()
    task_id = request.form.get("id")
    db.delete(TABLE, where=[("id", "=", int(task_id))])
    return redirect("/")


if __name__ == "__main__":
    app.run(debug=True)
