from __future__ import annotations

import asyncio
import json
import logging
import random
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Protocol, Sequence

logger = logging.getLogger(__name__)


class DiscordLikeService(Protocol):
    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def fetch_members(self) -> Iterable[Any]: ...

    async def add_reaction(
        self,
        message_id: int,
        emoji: str,
        channel_id: int | None = None,
    ) -> None: ...


@dataclass(frozen=True)
class MemberSnapshot:
    external_user_id: str
    username: str
    status: str | None = None
    display_name: str | None = None
    raw: dict[str, Any] | None = None


class DiscordMetadataSynchronizer:
    def __init__(
        self,
        *,
        db_path: str | Path,
        service: DiscordLikeService,
        platform: str = "discord",
        jitter_range: tuple[float, float] = (0.25, 1.25),
    ) -> None:
        self.db_path = Path(db_path)
        self.service = service
        self.platform = platform
        self.jitter_range = jitter_range

    async def connect(self) -> None:
        await self._jitter()
        await self.service.connect()
        await self._jitter()

    async def close(self) -> None:
        await self._jitter()
        await self.service.close()
        await self._jitter()

    async def sync_members(self) -> int:
        await self._jitter()
        members = await self.service.fetch_members()
        await self._jitter()

        snapshots = []
        if hasattr(members, \"__aiter__\"):
            async for member in members:
                snapshots.append(self._normalize_member(member))
        else:
            for member in members:
                snapshots.append(self._normalize_member(member))
        
        written = 0
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            columns = self._table_columns(conn, \"user_profiles\")
            if not columns:
                raise ValueError(\"user_profiles table does not exist or has no columns\")

            for snapshot in snapshots:
                await self._jitter()
                self._upsert_profile(conn, columns, snapshot)
                written += 1
                await self._jitter()

            conn.commit()

        return written

    async def add_reaction(
        self,
        *,
        message_id: int,
        emoji: str,
        channel_id: int | None = None,
    ) -> None:
        await self._jitter()
        await self.service.add_reaction(message_id=message_id, emoji=emoji, channel_id=channel_id)
        await self._jitter()

    def _normalize_member(self, member: Any) -> MemberSnapshot:
        if isinstance(member, MemberSnapshot):
            return member

        if isinstance(member, dict):
            member_id = self._first_text(member, (\"id\", \"user_id\", \"discord_id\", \"external_user_id\"))
            username = self._first_text(member, (\"username\", \"name\", \"display_name\", \"global_name\"))
            if not member_id:
                raise ValueError(\"member payload is missing an id\")
            if not username:
                username = member_id
            return MemberSnapshot(
                external_user_id=member_id,
                username=username,
                status=self._first_text(member, (\"status\", \"presence\", \"activity\")),
                display_name=self._first_text(member, (\"display_name\", \"global_name\", \"name\")),
                raw=member,
            )

        member_id = self._extract_attr(member, (\"id\", \"user_id\", \"discord_id\", \"external_user_id\"))
        username = self._extract_attr(member, (\"username\", \"name\", \"display_name\", \"global_name\"))
        status = self._extract_attr(member, (\"status\", \"presence\", \"activity\"))
        display_name = self._extract_attr(member, (\"display_name\", \"global_name\", \"name\"))
        if member_id is None:
            raise ValueError(\"member payload is missing an id\")
        if username is None:
            username = str(member_id)
        return MemberSnapshot(
            external_user_id=str(member_id),
            username=str(username),
            status=str(status) if status is not None else None,
            display_name=str(display_name) if display_name is not None else None,
            raw=self._safe_raw(member),
        )

    def _upsert_profile(
        self,
        conn: sqlite3.Connection,
        columns: set[str],
        snapshot: MemberSnapshot,
    ) -> None:
        values: dict[str, Any] = {}

        self._assign(values, columns, \"platform\", self.platform)
        self._assign(values, columns, \"external_user_id\", snapshot.external_user_id)
        self._assign(values, columns, \"user_id\", snapshot.external_user_id)
        self._assign(values, columns, \"discord_id\", snapshot.external_user_id)
        self._assign(values, columns, \"username\", snapshot.username)
        self._assign(values, columns, \"name\", snapshot.username)
        self._assign(values, columns, \"display_name\", snapshot.display_name or snapshot.username)
        self._assign(values, columns, \"status\", snapshot.status)
        self._assign(values, columns, \"raw_profile_json\", json.dumps(snapshot.raw, ensure_ascii=False) if snapshot.raw is not None else None)
        self._assign(values, columns, \"metadata_json\", json.dumps(snapshot.raw, ensure_ascii=False) if snapshot.raw is not None else None)
        self._assign(values, columns, \"raw_json\", json.dumps(snapshot.raw, ensure_ascii=False) if snapshot.raw is not None else None)
        self._assign(values, columns, \"last_scraped_at\", self._now())
        self._assign(values, columns, \"updated_at\", self._now())
        self._assign(values, columns, \"created_at\", self._now())

        lookup_column = self._pick_lookup_column(columns)
        lookup_value = snapshot.external_user_id if lookup_column in {\"external_user_id\", \"user_id\", \"discord_id\"} else snapshot.username

        existing_rowid = None
        if lookup_column is not None and lookup_value is not None:
            existing_rowid = self._find_existing_rowid(conn, lookup_column, lookup_value, self.platform if \"platform\" in columns else None)

        if existing_rowid is None:
            self._insert_profile(conn, columns, values)
        else:
            self._update_profile(conn, columns, values, existing_rowid)

    def _insert_profile(self, conn: sqlite3.Connection, columns: set[str], values: dict[str, Any]) -> None:
        insert_columns = [column for column in values.keys() if column in columns and values[column] is not None]
        if not insert_columns:
            raise ValueError(\"No supported user_profiles columns were found for insertion\")
        placeholders = ", ".join([\"?\"] * len(insert_columns))
        column_sql = ", ".join(insert_columns)
        conn.execute(
            f\"INSERT INTO user_profiles ({column_sql}) VALUES ({placeholders})\",
            [values[column] for column in insert_columns],
        )

    def _update_profile(
        self,
        conn: sqlite3.Connection,
        columns: set[str],
        values: dict[str, Any],
        rowid: int,
    ) -> None:
        update_columns = [column for column in values.keys() if column in columns and values[column] is not None and column != \"created_at\"]
        if not update_columns:
            return
        assignments = ", ".join(f\"{column} = ?\" for column in update_columns)
        conn.execute(
            f\"UPDATE user_profiles SET {assignments} WHERE rowid = ?\",
            [values[column] for column in update_columns] + [rowid],
        )

    def _find_existing_rowid(
        self,
        conn: sqlite3.Connection,
        lookup_column: str,
        lookup_value: str,
        platform_value: str | None,
    ) -> int | None:
        try:
            if platform_value is not None:
                row = conn.execute(
                    f\"SELECT rowid FROM user_profiles WHERE {lookup_column} = ? AND platform = ? LIMIT 1\",
                    (lookup_value, platform_value),
                ).fetchone()
            else:
                row = conn.execute(
                    f\"SELECT rowid FROM user_profiles WHERE {lookup_column} = ? LIMIT 1\",
                    (lookup_value,),
                ).fetchone()
        except sqlite3.DatabaseError:
            return None
        if row is None:
            return None
        return int(row[0])

    def _pick_lookup_column(self, columns: set[str]) -> str | None:
        for candidate in (\"external_user_id\", \"user_id\", \"discord_id\", \"username\", \"name\"):
            if candidate in columns:
                return candidate
        return None

    def _table_columns(self, conn: sqlite3.Connection, table: str) -> set[str]:
        try:
            rows = conn.execute(f\"PRAGMA table_info({table})\").fetchall()
        except sqlite3.DatabaseError:
            return set()
        return {str(row[1]) for row in rows}

    def _assign(self, values: dict[str, Any], columns: set[str], column: str, value: Any) -> None:
        if column in columns and value is not None:
            values[column] = value

    def _first_text(self, payload: dict[str, Any], keys: Sequence[str]) -> str | None:
        for key in keys:
            value = payload.get(key)
            if value is None:
                continue
            return str(value)
        return None

    def _extract_attr(self, obj: Any, names: Sequence[str]) -> Any:
        for name in names:
            if hasattr(obj, name):
                value = getattr(obj, name)
                if value is not None:
                    return value
        return None

    def _safe_raw(self, obj: Any) -> dict[str, Any] | None:
        if isinstance(obj, dict):
            return obj
        result: dict[str, Any] = {}
        for key in (\"id\", \"user_id\", \"discord_id\", \"external_user_id\", \"username\", \"name\", \"display_name\", \"global_name\", \"status\"):
            if hasattr(obj, key):
                value = getattr(obj, key)
                if value is not None:
                    result[key] = value
        return result or None

    def _now(self) -> str:
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()

    async def _jitter(self) -> None:
        low, high = self.jitter_range
        await asyncio.sleep(random.uniform(low, high))


class MessageReactionUtility:
    def __init__(self, service: DiscordLikeService, jitter_range: tuple[float, float] = (0.2, 0.8)) -> None:
        self.service = service
        self.jitter_range = jitter_range

    async def add_reaction(self, *, message_id: int, emoji: str, channel_id: int | None = None) -> None:
        await self._jitter()
        await self.service.add_reaction(message_id=message_id, emoji=emoji, channel_id=channel_id)
        await self._jitter()

    async def _jitter(self) -> None:
        low, high = self.jitter_range
        await asyncio.sleep(random.uniform(low, high))


__all__ = [
    \"DiscordLikeService\",
    \"DiscordMetadataSynchronizer\",
    \"MemberSnapshot\",
    \"MessageReactionUtility\",
]
