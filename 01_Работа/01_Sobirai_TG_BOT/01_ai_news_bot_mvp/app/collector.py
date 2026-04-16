from __future__ import annotations

import asyncio
import errno
import logging
import json
from asyncio import to_thread
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.tl.types import Message

from .db import Database, NormalizedPost
from .metrics import RuntimeMetrics
from .sources import SOURCES

logger = logging.getLogger(__name__)
_last_x_api_fetch_at: datetime | None = None


class XApiRateLimited(Exception):
    def __init__(self, retry_after_seconds: int):
        super().__init__(f"X API rate limited (Retry-After={retry_after_seconds}s)")
        self.retry_after_seconds = retry_after_seconds


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

def _fetch_x_items_xapi_blocking(
    handle: str,
    since_id: int,
    limit: int,
    bearer_token: str,
    base_url: str,
) -> list[dict]:
    base_url = base_url.rstrip("/")
    username = handle.lstrip("@")
    encoded_username = quote(username, safe="")

    # 1) /users/by/username/:username -> get numeric user id
    user_url = f"{base_url}/users/by/username/{encoded_username}"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; SobiraiBot/1.0)",
        "Authorization": f"Bearer {bearer_token}",
    }
    req = Request(user_url, headers=headers)
    try:
        with urlopen(req, timeout=20) as resp:
            user_payload = json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        if exc.code in (401, 403):
            return []
        if exc.code == 429:
            retry_after = int(exc.headers.get("Retry-After", "60"))
            raise XApiRateLimited(retry_after)
        raise

    user_data = user_payload.get("data") or {}
    user_id_raw = user_data.get("id")
    if not user_id_raw:
        return []
    user_id = int(user_id_raw)

    # 2) /users/:id/tweets with pagination until we gather `limit` tweets with id > since_id
    tweets_url = f"{base_url}/users/{user_id}/tweets"
    pagination_token: str | None = None
    fetched: list[dict] = []

    while True:
        max_results = min(max(1, limit - len(fetched)), 100)
        query_params: dict[str, str] = {
            "tweet.fields": "created_at,text",
            "max_results": str(max_results),
            "exclude": "replies,retweets",
        }
        if pagination_token:
            query_params["pagination_token"] = pagination_token

        url = f"{tweets_url}?{urlencode(query_params)}"
        req2 = Request(url, headers=headers)
        try:
            with urlopen(req2, timeout=20) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code in (401, 403):
                return []
            if exc.code == 429:
                retry_after = int(exc.headers.get("Retry-After", "60"))
                raise XApiRateLimited(retry_after)
            raise

        for tweet in payload.get("data", []) or []:
            tw_id_raw = tweet.get("id")
            if not tw_id_raw:
                continue
            tw_id = int(tw_id_raw)
            if tw_id <= since_id:
                continue

            created_at_raw = tweet.get("created_at") or ""
            if created_at_raw:
                # X API uses ISO8601 timestamps, typically with "Z"
                dt = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00")).astimezone(timezone.utc)
            else:
                dt = datetime.now(tz=timezone.utc)

            text = (tweet.get("text") or "").strip()
            if not text:
                text = "[без текста]"

            fetched.append(
                {
                    "id": tw_id,
                    "date": dt,
                    "text": text,
                    "source_link": source_link("x", handle, tw_id),
                    "external_url": None,
                }
            )

            if len(fetched) >= limit:
                break

        if len(fetched) >= limit:
            break

        meta = payload.get("meta") or {}
        next_token = meta.get("next_token")
        if not next_token:
            break
        pagination_token = next_token

    fetched.sort(key=lambda x: int(x["id"]))
    return fetched[:limit]


async def collect_new_posts(
    client: TelegramClient,
    db: Database,
    metrics: RuntimeMetrics,
    media_dir: Path,
    *,
    enable_x_sources: bool = True,
    x_api_bearer_token: str = "",
    x_api_base_url: str = "https://api.x.com/2",
    x_api_fetch_interval_seconds: int = 60,
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
            global _last_x_api_fetch_at
            if _last_x_api_fetch_at is not None:
                next_allowed = _last_x_api_fetch_at + timedelta(seconds=x_api_fetch_interval_seconds)
                if now_utc < next_allowed:
                    continue
            _last_x_api_fetch_at = now_utc
            logger.info("Collecting X source %s via X API", source.username)
            rows: list[dict] = []
            try:
                rows = await asyncio.wait_for(
                    to_thread(
                        source.username,
                        cursor,
                        40 if cursor == 0 else 100,
                        x_api_bearer_token,
                        x_api_base_url,
                    ),
                    timeout=max(5, x_fetch_timeout_seconds),
                )
            except XApiRateLimited as exc:
                logger.warning(
                    "X API rate limited for %s, retry_after=%ss",
                    source.username,
                    exc.retry_after_seconds,
                )
                # Сдвигаем "последний запрос" так, чтобы следующий allowed был через Retry-After.
                _last_x_api_fetch_at = now_utc - timedelta(seconds=x_api_fetch_interval_seconds) + timedelta(
                    seconds=exc.retry_after_seconds
                )
                continue
            except Exception as exc:
                logger.warning(
                    "Collect failed for X source %s via X API: %s",
                    source.username,
                    exc,
                )
                continue
            try:
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

