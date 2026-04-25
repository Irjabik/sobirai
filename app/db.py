from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiosqlite

from .config import DELIVERY_MODES
from .sources import all_source_usernames


@dataclass
class NormalizedPost:
    platform: str
    source_key: str
    channel_username: str
    channel_title: str
    source_message_id: int
    source_message_date: datetime
    source_link: str
    text: str
    channel_category: str | None = None
    media_group_id: str | None = None
    media_type: str | None = None
    media_file_id: str | None = None
    media_path: str | None = None
    media_duration: int | None = None
    media_width: int | None = None
    media_height: int | None = None
    media_thumb_path: str | None = None


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._create_schema()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        return self._conn

    async def _create_schema(self) -> None:
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
              user_id INTEGER PRIMARY KEY,
              username TEXT,
              first_name TEXT,
              is_paused INTEGER NOT NULL DEFAULT 0,
              started_at TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS user_settings (
              user_id INTEGER PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
              delivery_mode TEXT NOT NULL DEFAULT 'instant',
              digest_interval_hours INTEGER NOT NULL DEFAULT 12,
              digest_filter_enabled INTEGER NOT NULL DEFAULT 1,
              last_digest_sent_at TEXT,
              mute_all INTEGER NOT NULL DEFAULT 0,
              mute_news INTEGER NOT NULL DEFAULT 0,
              mute_tech INTEGER NOT NULL DEFAULT 0,
              mute_author INTEGER NOT NULL DEFAULT 0,
              mute_creative INTEGER NOT NULL DEFAULT 0,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS source_posts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              platform TEXT NOT NULL DEFAULT 'tg',
              source_key TEXT NOT NULL,
              channel_username TEXT NOT NULL,
              channel_category TEXT,
              source_message_id INTEGER NOT NULL,
              channel_title TEXT NOT NULL,
              source_message_date TEXT NOT NULL,
              source_link TEXT NOT NULL,
              text TEXT NOT NULL,
              media_group_id TEXT,
              media_type TEXT,
              media_file_id TEXT,
              media_path TEXT,
              created_at TEXT NOT NULL,
              UNIQUE(platform, source_key, source_message_id)
            );

            CREATE TABLE IF NOT EXISTS delivery_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL,
              source_post_id INTEGER NOT NULL REFERENCES source_posts(id) ON DELETE CASCADE,
              status TEXT NOT NULL,
              attempts INTEGER NOT NULL DEFAULT 1,
              last_error TEXT,
              latency_ms INTEGER,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              UNIQUE(user_id, source_post_id)
            );

            CREATE TABLE IF NOT EXISTS source_cursors (
              platform TEXT NOT NULL DEFAULT 'tg',
              source_key TEXT NOT NULL,
              last_message_id INTEGER NOT NULL DEFAULT 0,
              updated_at TEXT NOT NULL,
              PRIMARY KEY(platform, source_key)
            );

            CREATE TABLE IF NOT EXISTS user_blocked_channels (
              user_id INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
              channel_username TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY(user_id, channel_username)
            );
            """
        )
        # Compatibility migration for pre-existing DB files.
        for stmt in (
            "ALTER TABLE source_posts ADD COLUMN platform TEXT NOT NULL DEFAULT 'tg'",
            "ALTER TABLE source_posts ADD COLUMN source_key TEXT",
            "ALTER TABLE source_posts ADD COLUMN channel_category TEXT",
            "ALTER TABLE source_posts ADD COLUMN media_group_id TEXT",
            "ALTER TABLE users ADD COLUMN started_at TEXT",
            "ALTER TABLE user_settings ADD COLUMN digest_interval_hours INTEGER NOT NULL DEFAULT 12",
            "ALTER TABLE user_settings ADD COLUMN digest_filter_enabled INTEGER NOT NULL DEFAULT 1",
            "ALTER TABLE user_settings ADD COLUMN last_digest_sent_at TEXT",
            "ALTER TABLE user_settings ADD COLUMN mute_news INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE user_settings ADD COLUMN mute_tech INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE user_settings ADD COLUMN mute_author INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE user_settings ADD COLUMN mute_creative INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE source_posts ADD COLUMN media_duration INTEGER",
            "ALTER TABLE source_posts ADD COLUMN media_width INTEGER",
            "ALTER TABLE source_posts ADD COLUMN media_height INTEGER",
            "ALTER TABLE source_posts ADD COLUMN media_thumb_path TEXT",
        ):
            try:
                await self.conn.execute(stmt)
            except aiosqlite.OperationalError:
                pass
        await self.conn.execute(
            """
            UPDATE source_posts
            SET source_key = coalesce(source_key, lower(channel_username))
            WHERE source_key IS NULL OR source_key=''
            """
        )
        await self.conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_source_posts_platform_key_msg
            ON source_posts(platform, source_key, source_message_id)
            """
        )
        await self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_source_posts_platform_key_date
            ON source_posts(platform, source_key, source_message_date)
            """
        )
        # Legacy DB compatibility: migrate old cursor table keyed by channel_username.
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS source_cursors_v2 (
              platform TEXT NOT NULL,
              source_key TEXT NOT NULL,
              last_message_id INTEGER NOT NULL DEFAULT 0,
              updated_at TEXT NOT NULL,
              PRIMARY KEY(platform, source_key)
            )
            """
        )
        try:
            await self.conn.execute(
                """
                INSERT OR IGNORE INTO source_cursors_v2(platform, source_key, last_message_id, updated_at)
                SELECT 'tg', lower(channel_username), last_message_id, updated_at
                FROM source_cursors
                """
            )
        except aiosqlite.OperationalError:
            pass
        await self.conn.execute(
            """
            UPDATE users
            SET started_at = coalesce(started_at, created_at, updated_at, ?)
            WHERE started_at IS NULL
            """,
            (self._now(),),
        )
        await self.conn.execute(
            """
            UPDATE user_settings
            SET delivery_mode='digest',
                digest_interval_hours=12
            WHERE delivery_mode='digest_12h'
            """
        )
        await self.conn.execute(
            """
            UPDATE user_settings
            SET delivery_mode='digest',
                digest_interval_hours=24
            WHERE delivery_mode='digest_24h'
            """
        )
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS generated_channel_posts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              source_post_id INTEGER NOT NULL UNIQUE REFERENCES source_posts(id) ON DELETE CASCADE,
              status TEXT NOT NULL DEFAULT 'processing',
              llm_provider TEXT,
              llm_model TEXT,
              prompt_version TEXT,
              title TEXT,
              post_text TEXT,
              summary TEXT,
              fingerprint TEXT,
              duplicate_of_source_post_id INTEGER REFERENCES source_posts(id),
              channel_chat_id INTEGER NOT NULL,
              channel_message_id INTEGER,
              error TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              published_at TEXT
            );

            CREATE TABLE IF NOT EXISTS publish_daily_counters (
              day_utc TEXT NOT NULL PRIMARY KEY,
              published_count INTEGER NOT NULL DEFAULT 0,
              updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_generated_channel_posts_status_created
            ON generated_channel_posts(status, created_at);

            CREATE INDEX IF NOT EXISTS idx_generated_channel_posts_fingerprint
            ON generated_channel_posts(fingerprint);

            CREATE INDEX IF NOT EXISTS idx_generated_channel_posts_published_at
            ON generated_channel_posts(published_at);
            """
        )
        await self.conn.commit()

    @staticmethod
    def _now() -> str:
        return datetime.now(tz=timezone.utc).isoformat()

    @staticmethod
    def _since_12h() -> str:
        return (datetime.now(tz=timezone.utc) - timedelta(hours=12)).isoformat()

    @staticmethod
    def _since_7d() -> str:
        return (datetime.now(tz=timezone.utc) - timedelta(days=7)).isoformat()

    async def upsert_user(self, user_id: int, username: str | None, first_name: str | None) -> None:
        now = self._now()
        await self.conn.execute(
            """
            INSERT INTO users(user_id, username, first_name, started_at, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
              username=excluded.username,
              first_name=excluded.first_name,
              updated_at=excluded.updated_at
            """,
            (user_id, username, first_name, now, now, now),
        )
        await self.conn.execute(
            """
            INSERT INTO user_settings(user_id, updated_at)
            VALUES(?, ?)
            ON CONFLICT(user_id) DO NOTHING
            """,
            (user_id, now),
        )
        await self.conn.commit()

    async def set_pause(self, user_id: int, is_paused: bool) -> None:
        now = self._now()
        # On resume, shift delivery watermark forward to avoid sending backlog
        # accumulated while notifications were paused.
        if is_paused:
            await self.conn.execute(
                "UPDATE users SET is_paused=?, updated_at=? WHERE user_id=?",
                (1, now, user_id),
            )
        else:
            await self.conn.execute(
                "UPDATE users SET is_paused=?, started_at=?, updated_at=? WHERE user_id=?",
                (0, now, now, user_id),
            )
        await self.conn.commit()

    async def reset_delivery_started_at_for_all_users(self) -> None:
        now = self._now()
        await self.conn.execute(
            "UPDATE users SET started_at=?, updated_at=?",
            (now, now),
        )
        await self.conn.commit()

    async def set_delivery_mode(self, user_id: int, mode: str) -> None:
        if mode not in DELIVERY_MODES:
            raise ValueError(f"Unsupported mode: {mode}")
        await self.conn.execute(
            "UPDATE user_settings SET delivery_mode=?, updated_at=? WHERE user_id=?",
            (mode, self._now(), user_id),
        )
        await self.conn.commit()

    async def set_digest_interval_hours(self, user_id: int, hours: int) -> None:
        if hours < 1 or hours > 168:
            raise ValueError("hours must be in [1, 168]")
        await self.conn.execute(
            """
            UPDATE user_settings
            SET delivery_mode='digest',
                digest_interval_hours=?,
                updated_at=?
            WHERE user_id=?
            """,
            (hours, self._now(), user_id),
        )
        await self.conn.commit()

    async def set_digest_filter_enabled(self, user_id: int, enabled: bool) -> None:
        await self.conn.execute(
            """
            UPDATE user_settings
            SET digest_filter_enabled=?,
                updated_at=?
            WHERE user_id=?
            """,
            (1 if enabled else 0, self._now(), user_id),
        )
        await self.conn.commit()

    async def touch_digest_sent_at(self, user_id: int) -> None:
        await self.conn.execute(
            "UPDATE user_settings SET last_digest_sent_at=?, updated_at=? WHERE user_id=?",
            (self._now(), self._now(), user_id),
        )
        await self.conn.commit()

    async def set_mute_all(self, user_id: int, mute_all: bool) -> None:
        await self.conn.execute(
            "UPDATE user_settings SET mute_all=?, updated_at=? WHERE user_id=?",
            (1 if mute_all else 0, self._now(), user_id),
        )
        await self.conn.commit()

    async def set_category_block(self, user_id: int, category_key: str, blocked: bool) -> None:
        field_map = {
            "news": "mute_news",
            "tech": "mute_tech",
            "author": "mute_author",
            "creative": "mute_creative",
        }
        field = field_map.get(category_key)
        if field is None:
            raise ValueError(f"Unknown category key: {category_key}")
        await self.conn.execute(
            f"UPDATE user_settings SET {field}=?, updated_at=? WHERE user_id=?",
            (1 if blocked else 0, self._now(), user_id),
        )
        await self.conn.commit()

    async def get_category_blocks(self, user_id: int) -> dict[str, bool]:
        query = """
          SELECT mute_news, mute_tech, mute_author, mute_creative
          FROM user_settings
          WHERE user_id=?
        """
        async with self.conn.execute(query, (user_id,)) as cur:
            row = await cur.fetchone()
        if not row:
            return {"news": False, "tech": False, "author": False, "creative": False}
        return {
            "news": bool(row["mute_news"]),
            "tech": bool(row["mute_tech"]),
            "author": bool(row["mute_author"]),
            "creative": bool(row["mute_creative"]),
        }

    async def block_channel(self, user_id: int, channel_username: str) -> bool:
        normalized = channel_username.strip().lower()
        if not normalized.startswith("@"):
            normalized = f"@{normalized}"
        if normalized not in all_source_usernames():
            return False
        await self.conn.execute(
            """
            INSERT OR IGNORE INTO user_blocked_channels(user_id, channel_username, created_at)
            VALUES(?, ?, ?)
            """,
            (user_id, normalized, self._now()),
        )
        await self.conn.commit()
        return True

    async def unblock_channel(self, user_id: int, channel_username: str) -> None:
        normalized = channel_username.strip().lower()
        if not normalized.startswith("@"):
            normalized = f"@{normalized}"
        await self.conn.execute(
            "DELETE FROM user_blocked_channels WHERE user_id=? AND channel_username=?",
            (user_id, normalized),
        )
        await self.conn.commit()

    async def list_blocked_channels(self, user_id: int) -> list[str]:
        async with self.conn.execute(
            "SELECT channel_username FROM user_blocked_channels WHERE user_id=? ORDER BY channel_username",
            (user_id,),
        ) as cur:
            rows = await cur.fetchall()
        return [row["channel_username"] for row in rows]

    async def get_user_status(self, user_id: int) -> dict[str, Any] | None:
        query = """
          SELECT u.user_id, u.username, u.first_name, u.is_paused, s.delivery_mode, s.digest_interval_hours, s.mute_all,
                 s.digest_filter_enabled, s.mute_news, s.mute_tech, s.mute_author, s.mute_creative
          FROM users u
          JOIN user_settings s ON s.user_id = u.user_id
          WHERE u.user_id=?
        """
        async with self.conn.execute(query, (user_id,)) as cur:
            row = await cur.fetchone()
        if row is None:
            return None
        return dict(row)

    async def get_active_users_for_mode(self, mode: str) -> list[int]:
        query = """
          SELECT u.user_id
          FROM users u
          JOIN user_settings s ON s.user_id = u.user_id
          WHERE u.is_paused=0 AND s.mute_all=0 AND s.delivery_mode=?
        """
        async with self.conn.execute(query, (mode,)) as cur:
            rows = await cur.fetchall()
        return [row["user_id"] for row in rows]

    async def insert_post_if_new(self, post: NormalizedPost) -> int | None:
        now = self._now()
        cursor = await self.conn.execute(
            """
            INSERT OR IGNORE INTO source_posts(
              platform, source_key, channel_username, channel_category, source_message_id, channel_title,
              source_message_date, source_link, text, media_group_id, media_type, media_file_id, media_path,
              media_duration, media_width, media_height, media_thumb_path,
              created_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                post.platform,
                post.source_key,
                post.channel_username,
                post.channel_category,
                post.source_message_id,
                post.channel_title,
                post.source_message_date.isoformat(),
                post.source_link,
                post.text,
                post.media_group_id,
                post.media_type,
                post.media_file_id,
                post.media_path,
                post.media_duration,
                post.media_width,
                post.media_height,
                post.media_thumb_path,
                now,
            ),
        )
        await self.conn.commit()
        if cursor.rowcount == 0:
            return None
        return cursor.lastrowid

    async def get_post(self, post_id: int) -> dict[str, Any] | None:
        async with self.conn.execute("SELECT * FROM source_posts WHERE id=?", (post_id,)) as cur:
            row = await cur.fetchone()
        return dict(row) if row else None

    async def mark_delivery(
        self,
        user_id: int,
        source_post_id: int,
        status: str,
        attempts: int,
        last_error: str | None = None,
        latency_ms: int | None = None,
    ) -> None:
        now = self._now()
        await self.conn.execute(
            """
            INSERT INTO delivery_events(
              user_id, source_post_id, status, attempts, last_error, latency_ms, created_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, source_post_id) DO UPDATE SET
              status=excluded.status,
              attempts=excluded.attempts,
              last_error=excluded.last_error,
              latency_ms=excluded.latency_ms,
              updated_at=excluded.updated_at
            """,
            (user_id, source_post_id, status, attempts, last_error, latency_ms, now, now),
        )
        await self.conn.commit()

    async def undelivered_for_mode(self, mode: str, limit: int = 200) -> list[dict[str, Any]]:
        since_12h = self._since_12h()
        query = """
          SELECT p.*, u.user_id
          FROM source_posts p
          JOIN users u ON u.is_paused=0
          JOIN user_settings s ON s.user_id=u.user_id
          LEFT JOIN user_blocked_channels ub
            ON ub.user_id=u.user_id AND ub.channel_username=lower(p.channel_username)
          LEFT JOIN delivery_events d ON d.user_id=u.user_id AND d.source_post_id=p.id
          WHERE s.mute_all=0
            AND s.delivery_mode=?
            AND ub.channel_username IS NULL
            AND p.source_message_date >= u.started_at
            AND NOT (s.mute_news=1 AND coalesce(lower(p.channel_category), '')='новости')
            AND NOT (s.mute_tech=1 AND coalesce(lower(p.channel_category), '')='технические')
            AND NOT (s.mute_author=1 AND coalesce(lower(p.channel_category), '')='авторские')
            AND NOT (s.mute_creative=1 AND coalesce(lower(p.channel_category), '')='креативные')
            AND p.source_message_date >= ?
            AND (d.id IS NULL OR d.status != 'sent')
          ORDER BY p.source_message_date ASC
          LIMIT ?
        """
        async with self.conn.execute(query, (mode, since_12h, limit)) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def undelivered_for_user(
        self, user_id: int, hours_window: int, limit: int = 200
    ) -> list[dict[str, Any]]:
        cutoff = (
            datetime.now(tz=timezone.utc) - timedelta(hours=max(1, int(hours_window)))
        ).isoformat()
        query = """
          SELECT p.*, u.user_id
          FROM source_posts p
          JOIN users u ON u.user_id=? AND u.is_paused=0
          JOIN user_settings s ON s.user_id=u.user_id
          LEFT JOIN user_blocked_channels ub
            ON ub.user_id=u.user_id AND ub.channel_username=lower(p.channel_username)
          LEFT JOIN delivery_events d ON d.user_id=u.user_id AND d.source_post_id=p.id
          WHERE s.mute_all=0
            AND ub.channel_username IS NULL
            AND p.source_message_date >= u.started_at
            AND NOT (s.mute_news=1 AND coalesce(lower(p.channel_category), '')='новости')
            AND NOT (s.mute_tech=1 AND coalesce(lower(p.channel_category), '')='технические')
            AND NOT (s.mute_author=1 AND coalesce(lower(p.channel_category), '')='авторские')
            AND NOT (s.mute_creative=1 AND coalesce(lower(p.channel_category), '')='креативные')
            AND p.source_message_date >= ?
            AND (d.id IS NULL OR d.status != 'sent')
          ORDER BY p.source_message_date ASC
          LIMIT ?
        """
        async with self.conn.execute(query, (user_id, cutoff, limit)) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def undelivered_for_user_unfiltered(self, user_id: int, limit: int = 200) -> list[dict[str, Any]]:
        cutoff = self._since_7d()
        query = """
          SELECT p.*, u.user_id
          FROM source_posts p
          JOIN users u ON u.user_id=? AND u.is_paused=0
          JOIN user_settings s ON s.user_id=u.user_id
          LEFT JOIN user_blocked_channels ub
            ON ub.user_id=u.user_id AND ub.channel_username=lower(p.channel_username)
          LEFT JOIN delivery_events d ON d.user_id=u.user_id AND d.source_post_id=p.id
          WHERE s.mute_all=0
            AND ub.channel_username IS NULL
            AND p.source_message_date >= u.started_at
            AND NOT (s.mute_news=1 AND coalesce(lower(p.channel_category), '')='новости')
            AND NOT (s.mute_tech=1 AND coalesce(lower(p.channel_category), '')='технические')
            AND NOT (s.mute_author=1 AND coalesce(lower(p.channel_category), '')='авторские')
            AND NOT (s.mute_creative=1 AND coalesce(lower(p.channel_category), '')='креативные')
            AND p.source_message_date >= ?
            AND (d.id IS NULL OR d.status != 'sent')
          ORDER BY p.source_message_date ASC
          LIMIT ?
        """
        async with self.conn.execute(query, (user_id, cutoff, limit)) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def get_cursor(self, platform: str, source_key: str) -> int:
        async with self.conn.execute(
            "SELECT last_message_id FROM source_cursors_v2 WHERE platform=? AND source_key=?",
            (platform, source_key),
        ) as cur:
            row = await cur.fetchone()
        return int(row["last_message_id"]) if row else 0

    async def set_cursor(self, platform: str, source_key: str, last_message_id: int) -> None:
        await self.conn.execute(
            """
            INSERT INTO source_cursors_v2(platform, source_key, last_message_id, updated_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(platform, source_key) DO UPDATE SET
              last_message_id=excluded.last_message_id,
              updated_at=excluded.updated_at
            """,
            (platform, source_key, last_message_id, self._now()),
        )
        await self.conn.commit()

    async def health_stats(self) -> dict[str, Any]:
        stats: dict[str, Any] = {}
        for table in ("users", "source_posts", "delivery_events"):
            async with self.conn.execute(f"SELECT COUNT(*) as c FROM {table}") as cur:
                row = await cur.fetchone()
            stats[f"{table}_count"] = row["c"] if row else 0

        async with self.conn.execute(
            "SELECT status, COUNT(*) c FROM delivery_events GROUP BY status"
        ) as cur:
            rows = await cur.fetchall()
        stats["delivery_status"] = {row["status"]: row["c"] for row in rows}
        x_cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=24)).isoformat()
        async with self.conn.execute(
            """
            SELECT COUNT(*) as c
            FROM source_posts
            WHERE platform='x' AND source_message_date >= ?
            """,
            (x_cutoff,),
        ) as cur:
            row = await cur.fetchone()
        stats["x_posts_last_24h"] = row["c"] if row else 0

        async with self.conn.execute(
            "SELECT status, COUNT(*) c FROM generated_channel_posts GROUP BY status"
        ) as cur:
            rows = await cur.fetchall()
        stats["channel_post_status"] = {row["status"]: row["c"] for row in rows}

        day_utc = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        async with self.conn.execute(
            "SELECT published_count FROM publish_daily_counters WHERE day_utc=?",
            (day_utc,),
        ) as cur:
            row = await cur.fetchone()
        stats["channel_published_today_utc"] = int(row["published_count"]) if row else 0
        stats["channel_publish_day_utc"] = day_utc

        async with self.conn.execute(
            """
            SELECT
              SUM(CASE WHEN status='published' AND datetime(coalesce(published_at, updated_at)) >= datetime('now', '-1 hour') THEN 1 ELSE 0 END) AS pub_1h,
              SUM(CASE WHEN status='duplicate' AND datetime(updated_at) >= datetime('now', '-1 hour') THEN 1 ELSE 0 END) AS dup_1h,
              SUM(CASE WHEN status='failed' AND datetime(updated_at) >= datetime('now', '-1 hour') THEN 1 ELSE 0 END) AS fail_1h,
              SUM(CASE WHEN status='published' AND datetime(coalesce(published_at, updated_at)) >= datetime('now', '-24 hour') THEN 1 ELSE 0 END) AS pub_24h,
              SUM(CASE WHEN status='duplicate' AND datetime(updated_at) >= datetime('now', '-24 hour') THEN 1 ELSE 0 END) AS dup_24h,
              SUM(CASE WHEN status='failed' AND datetime(updated_at) >= datetime('now', '-24 hour') THEN 1 ELSE 0 END) AS fail_24h
            FROM generated_channel_posts
            """
        ) as cur:
            win = await cur.fetchone()
        pub_1h = int((win["pub_1h"] if win else 0) or 0)
        dup_1h = int((win["dup_1h"] if win else 0) or 0)
        fail_1h = int((win["fail_1h"] if win else 0) or 0)
        pub_24h = int((win["pub_24h"] if win else 0) or 0)
        dup_24h = int((win["dup_24h"] if win else 0) or 0)
        fail_24h = int((win["fail_24h"] if win else 0) or 0)

        async with self.conn.execute(
            """
            SELECT
              SUM(CASE WHEN status='duplicate' AND datetime(updated_at) >= datetime('now', '-24 hour') THEN 1 ELSE 0 END) AS dup_total_24h,
              SUM(CASE WHEN status='duplicate' AND datetime(updated_at) >= datetime('now', '-24 hour') AND error='exact_fingerprint_match' THEN 1 ELSE 0 END) AS dup_exact_24h,
              SUM(CASE WHEN status='duplicate' AND datetime(updated_at) >= datetime('now', '-24 hour') AND error='link_overlap_duplicate' THEN 1 ELSE 0 END) AS dup_link_24h,
              SUM(CASE WHEN status='duplicate' AND datetime(updated_at) >= datetime('now', '-24 hour') AND error LIKE 'post_llm_%' THEN 1 ELSE 0 END) AS dup_post_llm_24h,
              SUM(CASE WHEN status='duplicate' AND datetime(updated_at) >= datetime('now', '-24 hour') AND error LIKE 'near_duplicate_jaccard>=%' THEN 1 ELSE 0 END) AS dup_near_24h
            FROM generated_channel_posts
            """
        ) as cur:
            dup = await cur.fetchone()
        dup_total_24h = int((dup["dup_total_24h"] if dup else 0) or 0)
        dup_exact_24h = int((dup["dup_exact_24h"] if dup else 0) or 0)
        dup_link_24h = int((dup["dup_link_24h"] if dup else 0) or 0)
        dup_post_llm_24h = int((dup["dup_post_llm_24h"] if dup else 0) or 0)
        dup_near_24h = int((dup["dup_near_24h"] if dup else 0) or 0)
        denom_24h = pub_24h + dup_24h + fail_24h
        stats["channel_windows"] = {
            "published_1h": pub_1h,
            "duplicate_1h": dup_1h,
            "failed_1h": fail_1h,
            "published_24h": pub_24h,
            "duplicate_24h": dup_24h,
            "failed_24h": fail_24h,
            "duplicate_ratio_24h": round((dup_24h / denom_24h), 4) if denom_24h else 0.0,
        }
        stats["channel_duplicate_reasons_24h"] = {
            "total": dup_total_24h,
            "exact": dup_exact_24h,
            "near": dup_near_24h,
            "post_llm": dup_post_llm_24h,
            "link_overlap": dup_link_24h,
            "exact_share": round((dup_exact_24h / dup_total_24h), 4) if dup_total_24h else 0.0,
            "near_share": round((dup_near_24h / dup_total_24h), 4) if dup_total_24h else 0.0,
            "post_llm_share": round((dup_post_llm_24h / dup_total_24h), 4) if dup_total_24h else 0.0,
            "link_overlap_share": round((dup_link_24h / dup_total_24h), 4) if dup_total_24h else 0.0,
        }
        return stats

    async def latest_posts_for_user(self, user_id: int, limit: int = 30) -> list[dict[str, Any]]:
        return await self.latest_posts_for_user_window(user_id=user_id, limit=limit, hours_window=12)

    async def latest_posts_for_user_window(
        self, user_id: int, hours_window: int, limit: int = 30
    ) -> list[dict[str, Any]]:
        cutoff = (
            datetime.now(tz=timezone.utc) - timedelta(hours=max(1, int(hours_window)))
        ).isoformat()
        query = """
          SELECT p.*
          FROM source_posts p
          JOIN users u ON u.user_id=?
          JOIN user_settings s ON s.user_id=u.user_id
          LEFT JOIN user_blocked_channels ub
            ON ub.user_id=u.user_id AND ub.channel_username=lower(p.channel_username)
          WHERE s.mute_all=0
            AND ub.channel_username IS NULL
            AND p.source_message_date >= u.started_at
            AND NOT (s.mute_news=1 AND coalesce(lower(p.channel_category), '')='новости')
            AND NOT (s.mute_tech=1 AND coalesce(lower(p.channel_category), '')='технические')
            AND NOT (s.mute_author=1 AND coalesce(lower(p.channel_category), '')='авторские')
            AND NOT (s.mute_creative=1 AND coalesce(lower(p.channel_category), '')='креативные')
            AND p.source_message_date >= ?
          ORDER BY p.source_message_date DESC
          LIMIT ?
        """
        async with self.conn.execute(query, (user_id, cutoff, limit)) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def latest_posts_for_user_unfiltered(self, user_id: int, limit: int = 30) -> list[dict[str, Any]]:
        cutoff = self._since_7d()
        query = """
          SELECT p.*
          FROM source_posts p
          JOIN users u ON u.user_id=?
          JOIN user_settings s ON s.user_id=u.user_id
          LEFT JOIN user_blocked_channels ub
            ON ub.user_id=u.user_id AND ub.channel_username=lower(p.channel_username)
          WHERE s.mute_all=0
            AND ub.channel_username IS NULL
            AND p.source_message_date >= u.started_at
            AND NOT (s.mute_news=1 AND coalesce(lower(p.channel_category), '')='новости')
            AND NOT (s.mute_tech=1 AND coalesce(lower(p.channel_category), '')='технические')
            AND NOT (s.mute_author=1 AND coalesce(lower(p.channel_category), '')='авторские')
            AND NOT (s.mute_creative=1 AND coalesce(lower(p.channel_category), '')='креативные')
            AND p.source_message_date >= ?
          ORDER BY p.source_message_date DESC
          LIMIT ?
        """
        async with self.conn.execute(query, (user_id, cutoff, limit)) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def get_due_digest_users(self) -> list[dict[str, Any]]:
        query = """
          SELECT u.user_id, s.digest_interval_hours, s.digest_filter_enabled, s.last_digest_sent_at
          FROM users u
          JOIN user_settings s ON s.user_id=u.user_id
          WHERE u.is_paused=0 AND s.mute_all=0 AND s.delivery_mode='digest'
        """
        async with self.conn.execute(query) as cur:
            rows = await cur.fetchall()
        now = datetime.now(tz=timezone.utc)
        due: list[dict[str, Any]] = []
        for row in rows:
            last = row["last_digest_sent_at"]
            hours = int(row["digest_interval_hours"] or 12)
            if not last:
                due.append(dict(row))
                continue
            try:
                last_dt = datetime.fromisoformat(last)
            except Exception:
                due.append(dict(row))
                continue
            if (now - last_dt).total_seconds() >= hours * 3600:
                due.append(dict(row))
        return due

    async def reset_stale_channel_processing(self, stale_before_iso: str) -> int:
        """Сброс зависших processing (краш процесса), чтобы не блокировать source_post_id."""
        now = self._now()
        cur = await self.conn.execute(
            """
            UPDATE generated_channel_posts
            SET status='failed',
                error='stale_processing_timeout',
                updated_at=?
            WHERE status='processing' AND updated_at < ?
            """,
            (now, stale_before_iso),
        )
        await self.conn.commit()
        return int(cur.rowcount or 0)

    async def list_channel_autopublish_candidates(self, limit: int = 25) -> list[dict[str, Any]]:
        query = """
          SELECT p.*
          FROM source_posts p
          WHERE NOT EXISTS (
            SELECT 1 FROM generated_channel_posts g WHERE g.source_post_id = p.id
          )
          ORDER BY p.source_message_date ASC
          LIMIT ?
        """
        async with self.conn.execute(query, (limit,)) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def claim_channel_processing(self, source_post_id: int, channel_chat_id: int) -> bool:
        now = self._now()
        cur = await self.conn.execute(
            """
            INSERT OR IGNORE INTO generated_channel_posts(
              source_post_id, status, channel_chat_id, created_at, updated_at
            )
            VALUES(?, 'processing', ?, ?, ?)
            """,
            (source_post_id, channel_chat_id, now, now),
        )
        await self.conn.commit()
        return bool(cur.rowcount == 1)

    async def get_generated_channel_post_by_source_id(
        self, source_post_id: int
    ) -> dict[str, Any] | None:
        async with self.conn.execute(
            "SELECT * FROM generated_channel_posts WHERE source_post_id=?",
            (source_post_id,),
        ) as cur:
            row = await cur.fetchone()
        return dict(row) if row else None

    async def update_generated_channel_post(
        self,
        source_post_id: int,
        *,
        status: str | None = None,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        prompt_version: str | None = None,
        title: str | None = None,
        post_text: str | None = None,
        summary: str | None = None,
        fingerprint: str | None = None,
        duplicate_of_source_post_id: int | None = None,
        channel_message_id: int | None = None,
        error: str | None = None,
        published_at: str | None = None,
        clear_duplicate_of: bool = False,
        clear_error: bool = False,
    ) -> None:
        row = await self.get_generated_channel_post_by_source_id(source_post_id)
        if not row:
            return
        now = self._now()
        fields: list[str] = []
        values: list[Any] = []

        def set_field(name: str, value: Any) -> None:
            fields.append(f"{name}=?")
            values.append(value)

        if status is not None:
            set_field("status", status)
        if llm_provider is not None:
            set_field("llm_provider", llm_provider)
        if llm_model is not None:
            set_field("llm_model", llm_model)
        if prompt_version is not None:
            set_field("prompt_version", prompt_version)
        if title is not None:
            set_field("title", title)
        if post_text is not None:
            set_field("post_text", post_text)
        if summary is not None:
            set_field("summary", summary)
        if fingerprint is not None:
            set_field("fingerprint", fingerprint)
        if duplicate_of_source_post_id is not None:
            set_field("duplicate_of_source_post_id", duplicate_of_source_post_id)
        if clear_duplicate_of:
            set_field("duplicate_of_source_post_id", None)
        if channel_message_id is not None:
            set_field("channel_message_id", channel_message_id)
        if clear_error:
            set_field("error", None)
        elif error is not None:
            set_field("error", error)
        if published_at is not None:
            set_field("published_at", published_at)

        set_field("updated_at", now)
        if not fields:
            return
        sql = f"UPDATE generated_channel_posts SET {', '.join(fields)} WHERE source_post_id=?"
        values.append(source_post_id)
        await self.conn.execute(sql, values)
        await self.conn.commit()

    async def find_channel_fingerprint_duplicate(
        self, fingerprint: str, exclude_source_post_id: int
    ) -> int | None:
        async with self.conn.execute(
            """
            SELECT source_post_id
            FROM generated_channel_posts
            WHERE fingerprint=?
              AND source_post_id != ?
              AND status='published'
            LIMIT 1
            """,
            (fingerprint, exclude_source_post_id),
        ) as cur:
            row = await cur.fetchone()
        return int(row["source_post_id"]) if row else None

    async def list_recent_published_source_texts_for_channel_dedup(
        self, limit: int = 300
    ) -> list[tuple[int, str]]:
        query = """
          SELECT p.id as sid, p.text
          FROM source_posts p
          JOIN generated_channel_posts g ON g.source_post_id = p.id
          WHERE g.status = 'published'
          ORDER BY datetime(coalesce(g.published_at, g.updated_at)) DESC, g.id DESC
          LIMIT ?
        """
        async with self.conn.execute(query, (limit,)) as cur:
            rows = await cur.fetchall()
        return [(int(r["sid"]), str(r["text"] or "")) for r in rows]

    async def list_recent_published_source_records_for_channel_dedup(
        self, limit: int = 300
    ) -> list[dict[str, Any]]:
        query = """
          SELECT
            p.id as sid,
            p.source_key,
            p.source_link,
            p.text,
            p.media_type,
            p.media_file_id,
            p.media_path
          FROM source_posts p
          JOIN generated_channel_posts g ON g.source_post_id = p.id
          WHERE g.status = 'published'
          ORDER BY datetime(coalesce(g.published_at, g.updated_at)) DESC, g.id DESC
          LIMIT ?
        """
        async with self.conn.execute(query, (limit,)) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def list_recent_published_generated_texts_for_channel_dedup(
        self, limit: int = 300
    ) -> list[tuple[int, str]]:
        query = """
          SELECT g.source_post_id AS sid,
                 trim(coalesce(g.title, '') || ' ' || coalesce(g.post_text, '')) AS generated_text
          FROM generated_channel_posts g
          WHERE g.status = 'published'
          ORDER BY datetime(coalesce(g.published_at, g.updated_at)) DESC, g.id DESC
          LIMIT ?
        """
        async with self.conn.execute(query, (limit,)) as cur:
            rows = await cur.fetchall()
        return [(int(r["sid"]), str(r["generated_text"] or "")) for r in rows]

    async def get_channel_daily_publish_count(self, day_utc: str) -> int:
        async with self.conn.execute(
            "SELECT published_count FROM publish_daily_counters WHERE day_utc=?",
            (day_utc,),
        ) as cur:
            row = await cur.fetchone()
        return int(row["published_count"]) if row else 0

    async def increment_channel_daily_publish_count(self, day_utc: str) -> None:
        now = self._now()
        await self.conn.execute(
            """
            INSERT INTO publish_daily_counters(day_utc, published_count, updated_at)
            VALUES(?, 1, ?)
            ON CONFLICT(day_utc) DO UPDATE SET
              published_count = published_count + 1,
              updated_at=excluded.updated_at
            """,
            (day_utc, now),
        )
        await self.conn.commit()

    async def list_source_posts_by_media_group(self, media_group_id: str) -> list[dict[str, Any]]:
        async with self.conn.execute(
            """
            SELECT *
            FROM source_posts
            WHERE media_group_id=?
            ORDER BY source_message_date ASC, id ASC
            """,
            (media_group_id,),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

