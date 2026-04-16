from __future__ import annotations

import asyncio
import errno
import logging
from asyncio import to_thread
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.tl.types import Message

from .db import Database, NormalizedPost
from .metrics import RuntimeMetrics
from .sources import SOURCES

logger = logging.getLogger(__name__)
X_RSSHUB_FAIL_COOLDOWN = timedelta(minutes=5)
_x_rsshub_fail_until: dict[str, datetime] = {}


def source_link(platform: str, source_username: str, message_id: int) -> str:
    username = source_username.lstrip("@")
    if platform == "x":
        return f"https://x.com/{username}/status/{message_id}"
    return f"https://t.me/{username}/{message_id}"


async def normalize_message(
    client: TelegramClient,
    media_dir: Path,
    channel_username: str,
    channel_category: str,
    channel_title: str,
    msg: Message,
    *,
    media_download_enabled: bool = True,
) -> NormalizedPost | None:
    if msg.id is None:
        return None
    text = msg.message or ""
    media_type = None
    media_file_id = None
    media_path = None
    if media_download_enabled and msg.photo:
        media_type = "photo"
        try:
            downloaded = await client.download_media(
                msg, file=str(media_dir / f"{channel_username.lstrip('@')}_{msg.id}_photo.jpg")
            )
            media_path = str(downloaded) if downloaded else None
        except OSError as exc:
            if exc.errno == errno.ENOSPC:
                logger.error(
                    "Disk full while downloading photo from %s msg=%s; fallback to text-only",
                    channel_username,
                    msg.id,
                )
                media_type = None
                media_path = None
            else:
                raise
    elif media_download_enabled and msg.video:
        media_type = "video"
        try:
            downloaded = await client.download_media(
                msg, file=str(media_dir / f"{channel_username.lstrip('@')}_{msg.id}_video.mp4")
            )
            media_path = str(downloaded) if downloaded else None
        except OSError as exc:
            if exc.errno == errno.ENOSPC:
                logger.error(
                    "Disk full while downloading video from %s msg=%s; fallback to text-only",
                    channel_username,
                    msg.id,
                )
                media_type = None
                media_path = None
            else:
                raise

    date = msg.date.astimezone(timezone.utc) if msg.date else None
    if date is None:
        return None
    return NormalizedPost(
        platform="tg",
        source_key=channel_username.strip().lower(),
        channel_username=channel_username,
        channel_title=channel_title,
        source_message_id=msg.id,
        source_message_date=date,
        source_link=source_link("tg", channel_username, msg.id),
        text=text,
        channel_category=channel_category,
        media_group_id=str(msg.grouped_id) if getattr(msg, "grouped_id", None) else None,
        media_type=media_type,
        media_file_id=media_file_id,
        media_path=media_path,
    )


def _parse_rsshub_feed(raw: bytes, handle: str, since_id: int, limit: int) -> list[dict]:
    root = ElementTree.fromstring(raw)
    channel = root.find("channel")
    if channel is None:
        return []
    fetched: list[dict] = []
    for item in channel.findall("item"):
        link = (item.findtext("link") or "").strip()
        title = (item.findtext("title") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        if "/status/" not in link:
            continue
        tw_id_str = link.split("/status/", 1)[1].split("/", 1)[0].split("?", 1)[0]
        if not tw_id_str.isdigit():
            continue
        tw_id = int(tw_id_str)
        if tw_id <= since_id:
            continue
        if pub:
            dt = parsedate_to_datetime(pub).astimezone(timezone.utc)
        else:
            dt = datetime.now(tz=timezone.utc)
        fetched.append(
            {
                "id": tw_id,
                "date": dt,
                "text": title,
                "source_link": source_link("x", handle, tw_id),
                "external_url": None,
            }
        )
        if len(fetched) >= limit:
            break
    fetched.sort(key=lambda x: int(x["id"]))
    return fetched[:limit]


def _fetch_x_items_rsshub_blocking(base_url: str, handle: str, since_id: int, limit: int) -> list[dict]:
    username = handle.lstrip("@")
    clean_base = base_url.rstrip("/")
    user_part = quote(username, safe="")
    urls = (
        f"{clean_base}/x/user/{user_part}",
        f"{clean_base}/twitter/user/{user_part}",
    )
    last_error: Exception | None = None
    for url in urls:
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; SobiraiBot/1.0)"})
            with urlopen(req, timeout=20) as resp:
                raw = resp.read()
            return _parse_rsshub_feed(raw, handle, since_id, limit)
        except Exception as exc:
            last_error = exc
            continue
    raise RuntimeError(f"rsshub route lookup failed for {handle}: {last_error}")


async def collect_new_posts(
    client: TelegramClient,
    db: Database,
    metrics: RuntimeMetrics,
    media_dir: Path,
    *,
    enable_x_sources: bool = True,
    x_rsshub_base_url: str = "https://rsshub.app",
    x_fetch_timeout_seconds: int = 25,
    media_download_enabled: bool = True,
) -> list[int]:
    new_post_ids: list[int] = []
    media_dir.mkdir(parents=True, exist_ok=True)
    freshness_cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=12)
    for source in SOURCES:
        cursor = await db.get_cursor(source.platform, source.source_key)
        if source.platform == "x":
            if not enable_x_sources:
                continue
            now_utc = datetime.now(tz=timezone.utc)
            fail_until = _x_rsshub_fail_until.get(source.source_key)
            if fail_until is not None and now_utc < fail_until:
                continue
            rows: list[dict] = []
            try:
                rows = await asyncio.wait_for(
                    to_thread(
                        _fetch_x_items_rsshub_blocking,
                        x_rsshub_base_url,
                        source.username,
                        cursor,
                        40 if cursor == 0 else 100,
                    ),
                    timeout=max(5, x_fetch_timeout_seconds),
                )
            except Exception as exc:
                logger.warning(
                    "Collect failed for X source %s via RSSHub (%s): %s",
                    source.username,
                    x_rsshub_base_url,
                    exc,
                )
                _x_rsshub_fail_until[source.source_key] = now_utc + X_RSSHUB_FAIL_COOLDOWN
                continue
            try:
                _x_rsshub_fail_until.pop(source.source_key, None)
                newest_seen = cursor
                for item in sorted(rows, key=lambda x: int(x["id"])):
                    item_date = item["date"]
                    if item_date.tzinfo is None:
                        item_date = item_date.replace(tzinfo=timezone.utc)
                    item_date = item_date.astimezone(timezone.utc)
                    if cursor == 0 and item_date < freshness_cutoff:
                        newest_seen = max(newest_seen, int(item["id"]))
                        continue
                    text = (item.get("text") or "").strip()
                    if not text:
                        text = "[без текста]"
                    normalized = NormalizedPost(
                        platform="x",
                        source_key=source.source_key,
                        channel_username=source.username,
                        channel_title=f"X {source.username}",
                        source_message_id=int(item["id"]),
                        source_message_date=item_date,
                        source_link=item["source_link"],
                        text=text,
                        channel_category=source.category,
                    )
                    post_id = await db.insert_post_if_new(normalized)
                    newest_seen = max(newest_seen, int(item["id"]))
                    if post_id:
                        new_post_ids.append(post_id)
                        metrics.collected_posts += 1
                if newest_seen > cursor:
                    await db.set_cursor(source.platform, source.source_key, newest_seen)
            except Exception as exc:
                logger.warning("Collect failed for X source %s during normalization/save: %s", source.username, exc)
            continue
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
                        media_download_enabled=media_download_enabled,
                    )
                    if normalized is None:
                        continue
                    post_id = await db.insert_post_if_new(normalized)
                    if post_id:
                        new_post_ids.append(post_id)
                        metrics.collected_posts += 1
                if newest_seen > 0:
                    await db.set_cursor(source.platform, source.source_key, newest_seen)
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
                    media_download_enabled=media_download_enabled,
                )
                if normalized is None:
                    continue
                post_id = await db.insert_post_if_new(normalized)
                newest_seen = max(newest_seen, msg.id or 0)
                if post_id:
                    new_post_ids.append(post_id)
                    metrics.collected_posts += 1
            if newest_seen > cursor:
                await db.set_cursor(source.platform, source.source_key, newest_seen)
        except RPCError as exc:
            logger.warning("Collect failed for %s: %s", source.username, exc)
            continue
    return new_post_ids

