from __future__ import annotations

import argparse
import dataclasses
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Iterable, Optional
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

DB_PATH = Path(os.getenv("DOE_DB_PATH", "doe.db"))
DEFAULT_HOST = os.getenv("DASHBOARD_HOST", "127.0.0.1")
DEFAULT_PORT = int(os.getenv("DASHBOARD_PORT", "8000"))
MAIN_ACCOUNT_WEBHOOK_URL = os.getenv("MAIN_ACCOUNT_WEBHOOK_URL") or os.getenv("DISCORD_WEBHOOK_URL")

PHONE_RE = re.compile(r"(?:\+?\d[\d\s().-]{7,}\d)")
HANDLE_RE = re.compile(r"(?<!\w)(?:@[A-Za-z0-9_.-]{2,}|discord\.gg\/[A-Za-z0-9_.-]+|https?://(?:discord\.gg|discord\.com/invite)/[A-Za-z0-9_.-]+)")
DEFAULT_TABLES = {
    "suggestions": "suggestions",
    "user_profiles": "user_profiles",
    "messages": "messages",
}


@dataclass(slots=True)
class Suggestion:
    id: int
    conversation_id: int
    content: str
    status: str


class DashboardStore:
    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def ensure_schema(self) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS suggestions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'edited')),
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()

    def list_pending_suggestions(self) -> list[Suggestion]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT id, conversation_id, content, status FROM suggestions WHERE status = 'pending' ORDER BY id ASC"
            ).fetchall()
        return [self._row_to_suggestion(row) for row in rows]

    def get_suggestion(self, suggestion_id: int) -> Suggestion | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT id, conversation_id, content, status FROM suggestions WHERE id = ?",
                (suggestion_id,),
            ).fetchone()
        return self._row_to_suggestion(row) if row else None

    def update_suggestion(self, suggestion_id: int, *, content: str | None = None, status: str | None = None) -> Suggestion:
        suggestion = self.get_suggestion(suggestion_id)
        if suggestion is None:
            raise ValueError(f"Suggestion {suggestion_id} not found")

        new_content = content if content is not None else suggestion.content
        new_status = status if status is not None else suggestion.status

        with self.connect() as conn:
            conn.execute(
                """
                UPDATE suggestions
                SET content = ?, status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (new_content, new_status, suggestion_id),
            )
            conn.commit()

        return Suggestion(id=suggestion.id, conversation_id=suggestion.conversation_id, content=new_content, status=new_status)

    def record_message(self, conversation_id: int, content: str, direction: str = "outbound", metadata: dict[str, Any] | None = None) -> int:
        sent_at = datetime.now(timezone.utc).isoformat()
        metadata_json = json.dumps(metadata or {}, ensure_ascii=False)
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO messages (conversation_id, direction, content, metadata_json, sent_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (conversation_id, direction, content, metadata_json, sent_at),
            )
            conn.commit()
            return int(cur.lastrowid)

    @staticmethod
    def _row_to_suggestion(row: sqlite3.Row) -> Suggestion:
        return Suggestion(
            id=int(row["id"]),
            conversation_id=int(row["conversation_id"]),
            content=str(row["content"]),
            status=str(row["status"]),
        )


class MessageSender:
    def __init__(self, db: DashboardStore) -> None:
        self.db = db
        self._send_dm = self._resolve_send_dm()

    @staticmethod
    def _resolve_send_dm() -> Callable[..., Any] | None:
        try:
            from scraper import send_dm  # type: ignore

            return send_dm
        except Exception:
            return None

    def send(self, suggestion: Suggestion) -> dict[str, Any]:
        payload = {
            "conversation_id": suggestion.conversation_id,
            "content": suggestion.content,
            "suggestion_id": suggestion.id,
        }
        send_result: Any = None
        if self._send_dm is not None:
            try:
                send_result = self._send_dm(suggestion.conversation_id, suggestion.content)
            except TypeError:
                send_result = self._send_dm(suggestion.content, suggestion.conversation_id)
        else:
            send_result = {"sent": False, "reason": "scraper.send_dm not available"}

        message_id = self.db.record_message(
            suggestion.conversation_id,
            suggestion.content,
            direction="outbound",
            metadata={"source": "dashboard", "suggestion_id": suggestion.id, "send_result": _json_safe(send_result)},
        )
        payload["message_id"] = message_id
        payload["send_result"] = _json_safe(send_result)
        return payload


class GoalTracker:
    def __init__(self, webhook_url: str | None = MAIN_ACCOUNT_WEBHOOK_URL) -> None:
        self.webhook_url = webhook_url

    def inspect(self, message: str, *, conversation_id: int, suggestion_id: int) -> dict[str, Any] | None:
        phones = sorted({match.group(0).strip() for match in PHONE_RE.finditer(message)})
        handles = sorted({match.group(0).strip() for match in HANDLE_RE.finditer(message)})
        if not phones and not handles:
            return None
        return {
            "conversation_id": conversation_id,
            "suggestion_id": suggestion_id,
            "phones": phones,
            "handles": handles,
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }

    def notify(self, event: dict[str, Any]) -> bool:
        if not self.webhook_url:
            return False
        body = json.dumps(
            {
                "content": (
                    "goal tracking: approved message contains contact info\n"
                    f"conversation_id={event['conversation_id']} suggestion_id={event['suggestion_id']}\n"
                    f"phones={', '.join(event['phones']) or 'none'} handles={', '.join(event['handles']) or 'none'}"
                )
            },
            ensure_ascii=False,
        ).encode("utf-8")
        request = Request(self.webhook_url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urlopen(request, timeout=10):
                return True
        except Exception:
            return False


class DashboardService:
    def __init__(self, store: DashboardStore | None = None, sender: MessageSender | None = None, tracker: GoalTracker | None = None) -> None:
        self.store = store or DashboardStore()
        self.sender = sender or MessageSender(self.store)
        self.tracker = tracker or GoalTracker()

    def list_pending(self) -> list[dict[str, Any]]:
        return [dataclasses.asdict(item) for item in self.store.list_pending_suggestions()]

    def view(self, suggestion_id: int) -> dict[str, Any]:
        suggestion = self.store.get_suggestion(suggestion_id)
        if suggestion is None:
            raise ValueError(f"Suggestion {suggestion_id} not found")
        return dataclasses.asdict(suggestion)

    def edit(self, suggestion_id: int, new_content: str) -> dict[str, Any]:
        updated = self.store.update_suggestion(suggestion_id, content=new_content, status="edited")
        return dataclasses.asdict(updated)

    def approve(self, suggestion_id: int, content: str | None = None, send_now: bool = True) -> dict[str, Any]:
        if content is not None:
            suggestion = self.store.update_suggestion(suggestion_id, content=content, status="approved")
        else:
            suggestion = self.store.update_suggestion(suggestion_id, status="approved")

        result: dict[str, Any] = {"suggestion": dataclasses.asdict(suggestion), "sent": False}
        if send_now:
            send_result = self.sender.send(suggestion)
            result["sent"] = True
            result["send_result"] = send_result
            goal_event = self.tracker.inspect(suggestion.content, conversation_id=suggestion.conversation_id, suggestion_id=suggestion.id)
            if goal_event:
                result["goal_tracking"] = goal_event
                result["goal_tracking_notified"] = self.tracker.notify(goal_event)
        return result


class DashboardHTTPRequestHandler(BaseHTTPRequestHandler):
    service = DashboardService()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/suggestions":
            self._send_json({"suggestions": self.service.list_pending()})
            return
        if parsed.path.startswith("/suggestions/"):
            suggestion_id = self._extract_id(parsed.path)
            if suggestion_id is None:
                self._send_error(HTTPStatus.BAD_REQUEST, "Invalid suggestion id")
                return
            try:
                self._send_json(self.service.view(suggestion_id))
            except ValueError as exc:
                self._send_error(HTTPStatus.NOT_FOUND, str(exc))
            return
        self._send_json(
            {
                "message": "dashboard online",
                "routes": ["GET /suggestions", "GET /suggestions/<id>", "POST /suggestions/<id>/edit", "POST /suggestions/<id>/approve"],
            }
        )

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        body = self._read_json()
        if parsed.path.startswith("/suggestions/") and parsed.path.endswith("/edit"):
            suggestion_id = self._extract_id(parsed.path, suffix="/edit")
            if suggestion_id is None:
                self._send_error(HTTPStatus.BAD_REQUEST, "Invalid suggestion id")
                return
            content = _extract_content(body)
            if not content:
                self._send_error(HTTPStatus.BAD_REQUEST, "Missing content")
                return
            try:
                self._send_json(self.service.edit(suggestion_id, content))
            except ValueError as exc:
                self._send_error(HTTPStatus.NOT_FOUND, str(exc))
            return

        if parsed.path.startswith("/suggestions/") and parsed.path.endswith("/approve"):
            suggestion_id = self._extract_id(parsed.path, suffix="/approve")
            if suggestion_id is None:
                self._send_error(HTTPStatus.BAD_REQUEST, "Invalid suggestion id")
                return
            content = _extract_content(body)
            send_now = bool(body.get("send_now", True)) if isinstance(body, dict) else True
            try:
                self._send_json(self.service.approve(suggestion_id, content=content, send_now=send_now))
            except ValueError as exc:
                self._send_error(HTTPStatus.NOT_FOUND, str(exc))
            return

        self._send_error(HTTPStatus.NOT_FOUND, "Unknown route")

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}

    def _send_json(self, payload: Any, status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_error(self, status: HTTPStatus, message: str) -> None:
        self._send_json({"error": message}, status=int(status))

    @staticmethod
    def _extract_id(path: str, suffix: str = "") -> int | None:
        stem = path.removeprefix("/suggestions/")
        if suffix and stem.endswith(suffix):
            stem = stem[: -len(suffix)]
        try:
            return int(stem.strip("/"))
        except ValueError:
            return None


def _extract_content(body: dict[str, Any]) -> str | None:
    if not body:
        return None
    for key in ("content", "text", "message"):
        value = body.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        if dataclasses.is_dataclass(value):
            return dataclasses.asdict(value)
        if isinstance(value, dict):
            return {str(k): _json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [_json_safe(item) for item in value]
        return str(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manual suggestion dashboard for discord outreach engine")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to doe.db")
    subparsers = parser.add_subparsers(dest="command")

    serve = subparsers.add_parser("serve", help="Run the HTTP dashboard")
    serve.add_argument("--host", default=DEFAULT_HOST)
    serve.add_argument("--port", default=DEFAULT_PORT, type=int)

    subparsers.add_parser("list", help="List pending suggestions")

    show = subparsers.add_parser("show", help="Show a suggestion")
    show.add_argument("suggestion_id", type=int)

    edit = subparsers.add_parser("edit", help="Edit a suggestion")
    edit.add_argument("suggestion_id", type=int)
    edit.add_argument("content", help="Updated suggestion content")

    approve = subparsers.add_parser("approve", help="Approve and send a suggestion")
    approve.add_argument("suggestion_id", type=int)
    approve.add_argument("--content", help="Optional replacement content before approval")
    approve.add_argument("--no-send", action="store_true", help="Mark approved without sending")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    store = DashboardStore(Path(args.db))
    store.ensure_schema()
    service = DashboardService(store=store)

    if args.command == "serve":
        DashboardHTTPRequestHandler.service = service
        server = ThreadingHTTPServer((args.host, args.port), DashboardHTTPRequestHandler)
        print(f"dashboard listening on http://{args.host}:{args.port}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
        return 0

    if args.command == "list":
        print(json.dumps(service.list_pending(), ensure_ascii=False, indent=2))
        return 0

    if args.command == "show":
        print(json.dumps(service.view(args.suggestion_id), ensure_ascii=False, indent=2))
        return 0

    if args.command == "edit":
        print(json.dumps(service.edit(args.suggestion_id, args.content), ensure_ascii=False, indent=2))
        return 0

    if args.command == "approve":
        print(
            json.dumps(
                service.approve(args.suggestion_id, content=args.content, send_now=not args.no_send),
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
