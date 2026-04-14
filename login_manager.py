from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import aiohttp
import discord


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProxySettings:
    host: str
    port: int
    username: str | None = None
    password: str | None = None

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def auth(self) -> aiohttp.BasicAuth | None:
        if self.username is None and self.password is None:
            return None
        return aiohttp.BasicAuth(self.username or "", self.password or "")


@dataclass(frozen=True)
class AccountSessionConfig:
    account_id: int
    token: str
    proxy: ProxySettings
    label: str | None = None


class SQLiteSessionStore:
    """Loads Discord account sessions and their assigned proxies from SQLite."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)

    def load_sessions(self) -> list[AccountSessionConfig]:
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            account_columns = self._table_columns(conn, "accounts")
            proxy_columns = self._table_columns(conn, "proxies")

            if "token" not in account_columns:
                raise ValueError("accounts table must include a token column")
            if not proxy_columns:
                raise ValueError("proxies table is missing or empty")

            account_rows = conn.execute("SELECT * FROM accounts").fetchall()
            proxy_rows = conn.execute("SELECT * FROM proxies").fetchall()
            if not proxy_rows:
                raise ValueError("No proxy rows were found in the proxies table")

            proxies_by_id = self._index_proxies(proxy_rows) if "id" in proxy_columns else {}
            configs: list[AccountSessionConfig] = []

            for index, row in enumerate(account_rows):
                if not self._is_enabled(row, account_columns):
                    continue

                proxy = self._resolve_proxy(
                    conn=conn,
                    row=row,
                    account_columns=account_columns,
                    proxy_rows=proxy_rows,
                    proxies_by_id=proxies_by_id,
                    fallback_index=index,
                )

                account_id = int(row["id"]) if "id" in account_columns and row["id"] is not None else index
                label = str(row["label"]) if "label" in account_columns and row["label"] is not None else None
                configs.append(
                    AccountSessionConfig(
                        account_id=account_id,
                        token=str(row["token"]),
                        proxy=proxy,
                        label=label,
                    )
                )

            return configs

    def _table_columns(self, conn: sqlite3.Connection, table: str) -> set[str]:
        try:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        except sqlite3.DatabaseError:
            return set()
        return {str(row[1]) for row in rows}

    def _index_proxies(self, proxy_rows: Iterable[sqlite3.Row]) -> dict[int, ProxySettings]:
        indexed: dict[int, ProxySettings] = {}
        for row in proxy_rows:
            if row["id"] is None:
                continue
            indexed[int(row["id"])] = self._row_to_proxy(row)
        return indexed

    def _row_to_proxy(self, row: sqlite3.Row) -> ProxySettings:
        host = self._require_value(row, "host")
        port = int(self._require_value(row, "port"))
        auth_value = row["auth"] if "auth" in row.keys() else None
        username, password = self._parse_auth(auth_value)
        return ProxySettings(host=host, port=port, username=username, password=password)

    def _resolve_proxy(
        self,
        *,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        account_columns: set[str],
        proxy_rows: list[sqlite3.Row],
        proxies_by_id: dict[int, ProxySettings],
        fallback_index: int,
    ) -> ProxySettings:
        if {"proxy_host", "proxy_port"}.issubset(account_columns):
            auth_value = row["proxy_auth"] if "proxy_auth" in account_columns else None
            username, password = self._parse_auth(auth_value)
            return ProxySettings(
                host=str(row["proxy_host"]),
                port=int(row["proxy_port"]),
                username=username,
                password=password,
            )

        if "proxy_id" in account_columns and row["proxy_id"] is not None:
            proxy = proxies_by_id.get(int(row["proxy_id"]))
            if proxy is not None:
                return proxy
            account_ref = row["id"] if "id" in row.keys() else "unknown"
            raise ValueError(f"Account {account_ref} references missing proxy_id {row['proxy_id']}")

        if proxy_rows:
            return self._row_to_proxy(proxy_rows[fallback_index % len(proxy_rows)])

        raise ValueError("Unable to assign a proxy to the account row")

    def _is_enabled(self, row: sqlite3.Row, account_columns: set[str]) -> bool:
        if "enabled" not in account_columns:
            return True
        value = row["enabled"]
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip().lower() not in {"0", "false", "no", "off"}
        return bool(value)

    def _parse_auth(self, raw: Any) -> tuple[str | None, str | None]:
        if raw in (None, ""):
            return None, None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        if isinstance(raw, dict):
            username = raw.get("username") or raw.get("user") or raw.get("login")
            password = raw.get("password") or raw.get("pass")
            return (str(username) if username is not None else None, str(password) if password is not None else None)
        if isinstance(raw, str):
            text = raw.strip()
            if not text:
                return None, None
            if text.startswith("{"):
                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError:
                    parsed = None
                if isinstance(parsed, dict):
                    return self._parse_auth(parsed)
            if ":" in text:
                username, password = text.split(":", 1)
                return username or None, password or None
            return text, None
        return str(raw), None

    def _require_value(self, row: sqlite3.Row, key: str) -> Any:
        if key not in row.keys() or row[key] is None:
            raise ValueError(f"Missing required proxy field: {key}")
        return row[key]


class ManagedDiscordClient(discord.Client):
    def __init__(
        self,
        *,
        session_name: str,
        proxy: ProxySettings,
    ) -> None:
        intents = discord.Intents.none()
        super().__init__(
            intents=intents,
            self_bot=True,
            proxy=proxy.url,
            proxy_auth=proxy.auth,
        )
        self.session_name = session_name
        self.proxy_settings = proxy

    async def on_ready(self) -> None:
        logger.info(
            "Session %s connected as %s via %s",
            self.session_name,
            self.user,
            self.proxy_settings.url,
        )

    async def on_disconnect(self) -> None:
        logger.warning("Session %s disconnected", self.session_name)


@dataclass
class SessionHandle:
    config: AccountSessionConfig
    client: ManagedDiscordClient
    task: asyncio.Task[None]


class SessionManager:
    def __init__(self, db_path: str | Path) -> None:
        self.store = SQLiteSessionStore(db_path)
        self._handles: list[SessionHandle] = []
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        sessions = self.store.load_sessions()
        if not sessions:
            logger.warning("No enabled accounts found")
            return

        self._handles = [self._create_handle(session) for session in sessions]
        await asyncio.gather(*(handle.task for handle in self._handles))

    async def stop(self) -> None:
        self._stop_event.set()
        await asyncio.gather(*(self._shutdown_handle(handle) for handle in self._handles), return_exceptions=True)

    def _create_handle(self, config: AccountSessionConfig) -> SessionHandle:
        session_name = config.label or f"account-{config.account_id}"
        client = ManagedDiscordClient(session_name=session_name, proxy=config.proxy)
        task = asyncio.create_task(self._run_session(client, config), name=session_name)
        return SessionHandle(config=config, client=client, task=task)

    async def _run_session(self, client: ManagedDiscordClient, config: AccountSessionConfig) -> None:
        session_name = config.label or f"account-{config.account_id}"
        logger.info(
            "Starting session %s for account %s using proxy %s:%s",
            session_name,
            config.account_id,
            config.proxy.host,
            config.proxy.port,
        )
        try:
            await client.start(config.token)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Session %s failed", session_name)
        finally:
            if not client.is_closed():
                await client.close()

    async def _shutdown_handle(self, handle: SessionHandle) -> None:
        if not handle.client.is_closed():
            await handle.client.close()
        if not handle.task.done():
            handle.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await handle.task


async def main() -> None:
    parser = argparse.ArgumentParser(description="Manage multiple discord.py-self sessions from SQLite")
    parser.add_argument("--db-path", default="accounts.db", help="Path to the SQLite database")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    manager = SessionManager(args.db_path)

    try:
        await manager.run()
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
        await manager.stop()


if __name__ == "__main__":
    asyncio.run(main())
