from __future__ import annotations

import sqlite3
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

from flask import Flask, flash, get_flashedmessages, redirect, rendertemplatestring, request, url_for

from config import AppConfig


PREFERRED_TABLES = ("suggestions", "pending_items", "items")
PREFERRED_TEXT_COLUMNS = ("content", "body", "text", "message", "title", "note")
PREFERRED_STATUS_COLUMNS = ("status", "state")


@dataclass(slots=True)
class DashboardState:
    status: str = "starting"
    workers: int = 0
    active_jobs: int = 0
    last_error: str | None = None


class DoeDatabase:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS suggestions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id INTEGER NOT NULL DEFAULT 0,
                    content TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()

    def available_tables(self) -> list[str]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite%' ORDER BY name"
            ).fetchall()
        return [str(row[0]) for row in rows]

    def pick_table(self) -> str:
        tables = self.available_tables()
        for name in PREFERRED_TABLES:
            if name in tables:
                return name
        return "suggestions"

    def table_columns(self, table: str) -> list[str]:
        with self.connect() as conn:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return [str(row[1]) for row in rows]

    def list_pending(self) -> list[dict[str, Any]]:
        table = self.pick_table()
        columns = self.table_columns(table)
        if not columns:
            return []

        id_col = "id" if "id" in columns else "rowid"
        status_col = next((c for c in PREFERRED_STATUS_COLUMNS if c in columns), None)
        text_col = next((c for c in PREFERRED_TEXT_COLUMNS if c in columns), None)

        if status_col is None:
            return []

        select_cols = [id_col]
        if text_col and text_col != id_col:
            select_cols.append(text_col)
        select_cols.extend([c for c in ("conversation_id", status_col, "created_at", "updated_at") if c in columns and c not in select_cols])

        query = f"SELECT {', '.join(select_cols)} FROM {table} WHERE {status_col} = ? ORDER BY {id_col} ASC"
        with self.connect() as conn:
            rows = conn.execute(query, ("pending",)).fetchall()

        return [dict(row) for row in rows]

    def get_item(self, item_id: int) -> dict[str, Any] | None:
        table = self.pick_table()
        columns = self.table_columns(table)
        if not columns:
            return None

        id_col = "id" if "id" in columns else "rowid"
        with self.connect() as conn:
            row = conn.execute(f"SELECT * FROM {table} WHERE {id_col} = ?", (item_id,)).fetchone()
        return dict(row) if row else None

    def update_item(self, item_id: int, content: str, status: str) -> None:
        table = self.pick_table()
        columns = self.table_columns(table)
        if not columns:
            raise ValueError("No editable table found")

        id_col = "id" if "id" in columns else "rowid"
        text_col = next((c for c in PREFERRED_TEXT_COLUMNS if c in columns), None)
        status_col = next((c for c in PREFERRED_STATUS_COLUMNS if c in columns), None)
        updates: list[str] = []
        values: list[Any] = []

        if text_col is not None:
            updates.append(f"{text_col} = ?")
            values.append(content)
        if status_col is not None:
            updates.append(f"{status_col} = ?")
            values.append(status)
        if "updated_at" in columns:
            updates.append("updated_at = CURRENT_TIMESTAMP")
        if not updates:
            raise ValueError("Table has no editable content/status columns")

        values.append(item_id)
        sql = f"UPDATE {table} SET {', '.join(updates)} WHERE {id_col} = ?"
        with self.connect() as conn:
            conn.execute(sql, values)
            conn.commit()


DASHBOARD_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DOE Dashboard</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; background: #f7f7fb; color: #161616; }
    .grid { display: grid; grid-template-columns: 1fr; gap: 16px; max-width: 1200px; }
    .card { background: white; border: 1px solid #ddd; border-radius: 10px; padding: 16px; box-shadow: 0 2px 8px rgba(0,0,0,.04); }
    .muted { color: #666; }
    .row { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }
    .badge { display:inline-block; padding: 4px 10px; border-radius: 999px; background: #eef; }
    textarea { width: 100%; min-height: 90px; font-family: inherit; }
    input[type=text], select { width: 100%; padding: 8px; }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; border-bottom: 1px solid #ececec; padding: 10px 8px; vertical-align: top; }
    .actions { display: flex; gap: 8px; }
    .flash { padding: 10px 12px; border-radius: 8px; background: #fff4d6; border: 1px solid #efd28a; }
    .small { font-size: 12px; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  </style>
</head>
<body>
  <div class="grid">
    <div class="card">
      <div class="row">
        <h1 style="margin:0">DOE Dashboard</h1>
        <span class="badge">{{ state.status }}</span>
      </div>
      <p class="muted">Database: <span class="mono">{{ db_path }}</span></p>
      <p class="muted">Workers: {{ state.workers }} · Active jobs: {{ state.active_jobs }}</p>
      {% if state.last_error %}
        <p class="flash">Last error: {{ state.last_error }}</p>
      {% endif %}
      {% for message in flashes %}
        <p class="flash">{{ message }}</p>
      {% endfor %}
    </div>

    <div class="card">
      <div class="row" style="justify-content: space-between;">
        <h2 style="margin:0">Pending items</h2>
        <a href="{{ url_for('index') }}">Refresh</a>
      </div>
      {% if items %}
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Conversation</th>
              <th>Content</th>
              <th>Status</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {% for item in items %}
            <tr>
              <td class="mono">{{ item.get('id') }}</td>
              <td class="mono">{{ item.get('conversation_id', '') }}</td>
              <td>{{ item.get('content') or item.get('body') or item.get('text') or item.get('message') or item.get('title') or item.get('note') or '' }}</td>
              <td class="mono">{{ item.get('status') or item.get('state') or '' }}</td>
              <td>
                <form method="post" action="{{ url_for('edit_item', item_id=item.get('id')) }}">
                  <div class="actions">
                    <select name="status">
                      {% for s in ['pending','edited','approved','done','ignored'] %}
                        <option value="{{ s }}" {% if (item.get('status') or item.get('state')) == s %}selected{% endif %}>{{ s }}</option>
                      {% endfor %}
                    </select>
                  </div>
                  <textarea name="content">{{ item.get('content') or item.get('body') or item.get('text') or item.get('title') or item.get('note') or '' }}</textarea>
                  <div class="actions" style="margin-top:8px;">
                    <button type="submit">Save</button>
                    <a href="{{ url_for('view_item', item_id=item.get('id')) }}">View</a>
                  </div>
                </form>
              </td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
      {% else %}
        <p class="muted">No pending items found.</p>
      {% endif %}
    </div>
  </div>
</body>
</html>
"""

ITEM_TEMPLATE = """
<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Item {{ item.get('id') }}</title></head>
<body style="font-family: Arial, sans-serif; margin: 24px;">
  <p><a href="{{ url_for('index') }}">Back</a></p>
  <pre style="white-space: pre-wrap; background: #f6f6f6; padding: 16px; border-radius: 8px;">{{ item | tojson(indent=2) }}</pre>
</body>
</html>
"""


def create_app(config: AppConfig, state: DashboardState | None = None, database: DoeDatabase | None = None) -> Flask:
    app = Flask(__name__)
    app.secret_key = config.secret_key
    db = database or DoeDatabase(config.db_path)
    db.ensure_schema()
    runtime_state = state or DashboardState(status="running", workers=config.worker_threads)

    @app.route("/")
    def index() -> str:
        flashes = getflashedmessages()
        return rendertemplatestring(
            DASHBOARD_TEMPLATE,
            state=asdict(runtime_state),
            db_path=str(config.db_path),
            items=db.list_pending(),
            flashes=flashes,
        )

    @app.route("/item/<int:item_id>")
    def view_item(item_id: int) -> str:
        item = db.get_item(item_id)
        if item is None:
            return ("Not found", 404)
        return rendertemplatestring(ITEM_TEMPLATE, item=item)

    @app.route("/item/<int:item_id>/edit", methods=["POST"])
    def edit_item(item_id: int):
        content = request.form.get("content", "")
        status = request.form.get("status", "pending")
        db.update_item(item_id, content, status)
        flash(f"Saved item {item_id}")
        return redirect(url_for("index"))

    @app.route("/health")
    def health() -> dict[str, Any]:
        return {"status": runtime_state.status, "workers": runtime_state.workers, "active_jobs": runtime_state.active_jobs}

    @app.route("/api/pending")
    def api_pending() -> dict[str, Any]:
        return {"items": db.list_pending()}

    return app
