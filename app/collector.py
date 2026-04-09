from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.tl.types import Message

from .db import Database, NormalizedPost
from .metrics import RuntimeMetrics
from .sources import SOURCES

logger = logging.getLogger(__name__)


def message_link(channel_username: str, message_id: int) -> str:
    username = channel_username.lstrip("@")
    return f"https://t.me/{username}/{message_id}"


async def normalize_message(
    client: TelegramClient,
    media_dir: Path,
    channel_username: str,
    channel_category: str,
    channel_title: str,
    msg: Message,
) -> NormalizedPost | None:
    if msg.id is None:
        return None
    text = msg.message or ""
    media_type = None
    media_file_id = None
    media_path = None
    if msg.photo:
        media_type = "photo"
        downloaded = await client.download_media(
            msg, file=str(media_dir / f"{channel_username.lstrip('@')}_{msg.id}_photo")
        )
        media_path = str(downloaded) if downloaded else None
    elif msg.video:
        media_type = "video"
        downloaded = await client.download_media(
            msg, file=str(media_dir / f"{channel_username.lstrip('@')}_{msg.id}_video")
        )
        media_path = str(downloaded) if downloaded else None

    date = msg.date.astimezone(timezone.utc) if msg.date else None
    if date is None:
        return None
    return NormalizedPost(
        channel_username=channel_username,
        channel_title=channel_title,
        source_message_id=msg.id,
        source_message_date=date,
        source_link=message_link(channel_username, msg.id),
        text=text,
        channel_category=channel_category,
        media_type=media_type,
        media_file_id=media_file_id,
        media_path=media_path,
    )


async def collect_new_posts(
    client: TelegramClient, db: Database, metrics: RuntimeMetrics, media_dir: Path
) -> list[int]:
    new_post_ids: list[int] = []
    media_dir.mkdir(parents=True, exist_ok=True)
    freshness_cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=12)
    for source in SOURCES:
        cursor = await db.get_cursor(source.username)
        try:
            entity = await client.get_entity(source.username)
            title = getattr(entity, "title", source.username)

            # First run bootstrap: do not backfill years of history.
            # Read only a small latest slice, keep fresh items, and move cursor to current top id.
            if cursor == 0:
                latest_messages = await client.get_messages(entity, limit=50)
                newest_seen = max((msg.id or 0) for msg in latest_messages) if latest_messages else 0
                for msg in reversed(latest_messages):
                    msg_date = msg.date.astimezone(timezone.utc) if msg.date else None
                    if msg_date is None or msg_date < freshness_cutoff:
                        continue
                    normalized = await normalize_message(
                        client,
                        media_dir,
                        source.username,
                        source.category,
                        title,
                        msg,
                    )
                    if normalized is None:
                        continue
                    post_id = await db.insert_post_if_new(normalized)
                    if post_id:
                        new_post_ids.append(post_id)
                        metrics.collected_posts += 1
                if newest_seen > 0:
                    await db.set_cursor(source.username, newest_seen)
                continue

            newest_seen = cursor
            async for msg in client.iter_messages(entity, min_id=cursor, reverse=True):
                normalized = await normalize_message(
                    client,
                    media_dir,
                    source.username,
                    source.category,
                    title,
                    msg,
                )
                if normalized is None:
                    continue
                post_id = await db.insert_post_if_new(normalized)
                newest_seen = max(newest_seen, msg.id or 0)
                if post_id:
                    new_post_ids.append(post_id)
                    metrics.collected_posts += 1
            if newest_seen > cursor:
                await db.set_cursor(source.username, newest_seen)
        except RPCError as exc:
            logger.warning("Collect failed for %s: %s", source.username, exc)
            continue
    return new_post_ids

