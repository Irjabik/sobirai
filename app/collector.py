from __future__ import annotations

import asyncio
import errno
import logging
import json
from asyncio import to_thread
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
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
_x_source_next_allowed_at: dict[str, datetime] = {}
_x_source_user_cache: dict[str, tuple[int, datetime]] = {}
_x_round_robin_index: int = 0
_x_request_timestamps: list[datetime] = []
_x_global_cooldown_until: datetime | None = None


class XApiRateLimited(Exception):
    def __init__(self, retry_after_seconds: int):
        super().__init__(f"X API rate limited (Retry-After={retry_after_seconds}s)")
        self.retry_after_seconds = retry_after_seconds


class XApiAuthError(Exception):
    """Raised when token/permissions for X API are invalid."""


def source_link(platform: str, source_username: str, message_id: int) -> str:
    username = source_username.lstrip("@")
    if platform == "x":
        return f"https://x.com/{username}/status/{message_id}"
    return f"https://t.me/{username}/{message_id}"


async def _fetch_channel_album_messages(
    client: TelegramClient,
    entity: Any,
    anchor: Message,
    *,
    window: int = 12,
) -> list[Message]:
    """
    Fetch other channel messages that belong to the same Telegram media album as ``anchor``.

    Telethon may deliver album parts as separate ``Message`` updates (often with consecutive ids).
    We proactively pull a small id window around the anchor so all parts exist in DB before
    instant delivery runs in the same collector tick.
    """
    grouped_id = getattr(anchor, "grouped_id", None)
    anchor_id = anchor.id
    if grouped_id is None or anchor_id is None:
        return [anchor]

    span = max(1, min(30, int(window)))
    low = max(1, int(anchor_id) - span + 1)
    high = int(anchor_id) + span - 1
    ids = list(range(low, high + 1))
    try:
        batch = await client.get_messages(entity, ids=ids)
    except RPCError:
        return [anchor]

    if not batch:
        return [anchor]

    out: list[Message] = []
    for msg in batch:
        if msg is None or getattr(msg, "id", None) is None:
            continue
        if getattr(msg, "grouped_id", None) != grouped_id:
            continue
        out.append(msg)
    if not out:
        return [anchor]
    out.sort(key=lambda m: int(m.id))
    return out


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
    *,
    cached_user_id: int | None = None,
    max_pages_per_source: int = 1,
    max_results: int = 20,
) -> tuple[list[dict], int, int | None, int, bool]:
    base_url = base_url.rstrip("/")
    username = handle.lstrip("@")
    encoded_username = quote(username, safe="")
    request_count = 0
    pages_polled = 0
    used_cache = cached_user_id is not None

    # 1) /users/by/username/:username -> get numeric user id (or use cache)
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; SobiraiBot/1.0)",
        "Authorization": f"Bearer {bearer_token}",
    }
    if cached_user_id is None:
        user_url = f"{base_url}/users/by/username/{encoded_username}"
        req = Request(user_url, headers=headers)
        try:
            request_count += 1
            with urlopen(req, timeout=20) as resp:
                user_payload = json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code in (401, 403):
                raise XApiAuthError("token/permissions wrong")
            if exc.code == 429:
                retry_after = int(exc.headers.get("Retry-After", "60"))
                raise XApiRateLimited(retry_after)
            if exc.code == 404:
                return ([], request_count, None, pages_polled, used_cache)
            raise

        user_data = user_payload.get("data") or {}
        user_id_raw = user_data.get("id")
        if not user_id_raw:
            return ([], request_count, None, pages_polled, used_cache)
        user_id = int(user_id_raw)
    else:
        user_id = cached_user_id

    # 2) /users/:id/tweets with pagination until we gather `limit` tweets with id > since_id
    tweets_url = f"{base_url}/users/{user_id}/tweets"
    pagination_token: str | None = None
    fetched: list[dict] = []
    page_limit = max(1, max_pages_per_source)
    per_page_limit = max(5, min(100, max_results))

    while pages_polled < page_limit:
        pages_polled += 1
        requested = min(max(1, limit - len(fetched)), per_page_limit)
        query_params: dict[str, str] = {
            "tweet.fields": "created_at,text",
            "max_results": str(requested),
            "exclude": "replies,retweets",
            "since_id": str(max(0, since_id)),
        }
        if pagination_token:
            query_params["pagination_token"] = pagination_token

        url = f"{tweets_url}?{urlencode(query_params)}"
        req2 = Request(url, headers=headers)
        try:
            request_count += 1
            with urlopen(req2, timeout=20) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code in (401, 403):
                raise XApiAuthError("token/permissions wrong")
            if exc.code == 429:
                retry_after = int(exc.headers.get("Retry-After", "60"))
                raise XApiRateLimited(retry_after)
            if exc.code == 404:
                return ([], request_count, None, pages_polled, used_cache)
            raise

        page_has_new = False
        for tweet in payload.get("data", []) or []:
            tw_id_raw = tweet.get("id")
            if not tw_id_raw:
                continue
            tw_id = int(tw_id_raw)
            if tw_id <= since_id:
                continue
            page_has_new = True

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

        # Early stop: if current page did not bring any newer tweet, next pages are usually older.
        if not page_has_new:
            break

        meta = payload.get("meta") or {}
        next_token = meta.get("next_token")
        if not next_token:
            break
        pagination_token = next_token

    fetched.sort(key=lambda x: int(x["id"]))
    return (fetched[:limit], request_count, user_id, pages_polled, used_cache)


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
    x_api_sources_per_tick: int = 1,
    x_api_user_cache_ttl_seconds: int = 86400,
    x_api_max_pages_per_source: int = 1,
    x_api_max_results: int = 20,
    x_api_max_requests_per_hour: int = 120,
    x_fetch_timeout_seconds: int = 25,
    media_download_enabled: bool = True,
) -> list[int]:
    new_post_ids: list[int] = []
    media_dir.mkdir(parents=True, exist_ok=True)
    freshness_cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=12)
    now_utc = datetime.now(tz=timezone.utc)

    # Hourly soft budget guard for X API.
    global _x_request_timestamps
    _x_request_timestamps = [t for t in _x_request_timestamps if (now_utc - t) < timedelta(hours=1)]
    metrics.x_api_requests_last_hour = len(_x_request_timestamps)

    x_sources = [s for s in SOURCES if s.platform == "x"] if enable_x_sources else []
    selected_x_source_keys: set[str] = set()
    if x_sources:
        global _x_round_robin_index
        count = min(max(1, x_api_sources_per_tick), len(x_sources))
        start_idx = _x_round_robin_index % len(x_sources)
        for i in range(count):
            selected_x_source_keys.add(x_sources[(start_idx + i) % len(x_sources)].source_key)
        _x_round_robin_index = (_x_round_robin_index + count) % len(x_sources)

    global _x_global_cooldown_until
    if _x_global_cooldown_until is not None and now_utc < _x_global_cooldown_until:
        selected_x_source_keys = set()

    for source in SOURCES:
        cursor = await db.get_cursor(source.platform, source.source_key)
        if source.platform == "x":
            if not enable_x_sources or source.source_key not in selected_x_source_keys:
                continue
            if len(_x_request_timestamps) >= max(1, x_api_max_requests_per_hour):
                logger.warning(
                    "X API hourly budget reached (%s req/h). Skip source=%s until window slides",
                    x_api_max_requests_per_hour,
                    source.username,
                )
                continue

            next_allowed = _x_source_next_allowed_at.get(source.source_key)
            if next_allowed is not None and now_utc < next_allowed:
                continue

            logger.info("Collecting X source %s via X API", source.username)
            rows: list[dict] = []
            request_count = 0
            pages_polled = 0
            try:
                cached_user = _x_source_user_cache.get(source.source_key)
                cached_user_id: int | None = None
                if cached_user is not None:
                    user_id, expires_at = cached_user
                    if now_utc < expires_at:
                        cached_user_id = user_id
                    else:
                        _x_source_user_cache.pop(source.source_key, None)

                rows, request_count, resolved_user_id, pages_polled, used_cache = await asyncio.wait_for(
                    to_thread(
                        _fetch_x_items_xapi_blocking,
                        source.username,
                        cursor,
                        40 if cursor == 0 else 100,
                        x_api_bearer_token,
                        x_api_base_url,
                        cached_user_id=cached_user_id,
                        max_pages_per_source=x_api_max_pages_per_source,
                        max_results=x_api_max_results,
                    ),
                    timeout=max(5, x_fetch_timeout_seconds),
                )
                _x_request_timestamps.extend([now_utc] * request_count)
                metrics.x_api_requests_last_hour = len(_x_request_timestamps)
                metrics.x_api_requests += request_count
                metrics.x_api_requests_total += request_count
                metrics.x_api_sources_polled += 1
                if used_cache:
                    metrics.x_api_cache_hits += 1
                else:
                    metrics.x_api_cache_misses += 1
                if resolved_user_id is not None:
                    _x_source_user_cache[source.source_key] = (
                        resolved_user_id,
                        now_utc + timedelta(seconds=max(60, x_api_user_cache_ttl_seconds)),
                    )
                _x_source_next_allowed_at[source.source_key] = now_utc + timedelta(
                    seconds=x_api_fetch_interval_seconds
                )
            except XApiRateLimited as exc:
                logger.warning(
                    "X API rate limited for %s, retry_after=%ss",
                    source.username,
                    exc.retry_after_seconds,
                )
                _x_source_next_allowed_at[source.source_key] = now_utc + timedelta(
                    seconds=max(1, exc.retry_after_seconds)
                )
                _x_global_cooldown_until = now_utc + timedelta(seconds=max(1, exc.retry_after_seconds))
                metrics.x_api_rate_limited += 1
                metrics.x_api_sources_polled += 1
                continue
            except XApiAuthError as exc:
                logger.warning(
                    "Collect skipped for X source %s: token/permissions issue (%s)",
                    source.username,
                    exc,
                )
                _x_source_next_allowed_at[source.source_key] = now_utc + timedelta(minutes=15)
                _x_global_cooldown_until = now_utc + timedelta(minutes=5)
                metrics.x_api_auth_errors += 1
                metrics.x_api_sources_polled += 1
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
                inserted_for_source = 0
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
                        metrics.x_collected_posts += 1
                        inserted_for_source += 1
                if newest_seen > cursor:
                    await db.set_cursor(source.platform, source.source_key, newest_seen)
                if inserted_for_source == 0 and pages_polled <= 1:
                    logger.info(
                        "No new X posts for %s (cursor=%s). requests=%s pages=%s",
                        source.username,
                        cursor,
                        request_count,
                        pages_polled,
                    )
            except Exception as exc:
                logger.warning("Collect failed for X source %s during normalization/save: %s", source.username, exc)
            continue
        try:
            entity = await client.get_entity(source.username)
            title = getattr(entity, "title", source.username)
            album_handled: set[int] = set()

            # First run bootstrap: do not backfill years of history.
            # Read only a small latest slice, keep fresh items, and move cursor to current top id.
            if cursor == 0:
                latest_messages = await client.get_messages(entity, limit=50)
                newest_seen = max((msg.id or 0) for msg in latest_messages) if latest_messages else 0
                for msg in reversed(latest_messages):
                    msg_date = msg.date.astimezone(timezone.utc) if msg.date else None
                    if msg_date is None or msg_date < freshness_cutoff:
                        continue
                    grouped_id = getattr(msg, "grouped_id", None)
                    if grouped_id and int(grouped_id) in album_handled:
                        continue
                    batch = (
                        await _fetch_channel_album_messages(client, entity, msg)
                        if grouped_id
                        else [msg]
                    )
                    if grouped_id:
                        album_handled.add(int(grouped_id))
                    for part in batch:
                        normalized = await normalize_message(
                            client,
                            media_dir,
                            source.username,
                            source.category,
                            title,
                            part,
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
                grouped_id = getattr(msg, "grouped_id", None)
                if grouped_id and int(grouped_id) in album_handled:
                    continue
                batch = (
                    await _fetch_channel_album_messages(client, entity, msg)
                    if grouped_id
                    else [msg]
                )
                if grouped_id:
                    album_handled.add(int(grouped_id))
                for part in batch:
                    normalized = await normalize_message(
                        client,
                        media_dir,
                        source.username,
                        source.category,
                        title,
                        part,
                        media_download_enabled=media_download_enabled,
                    )
                    if normalized is None:
                        continue
                    post_id = await db.insert_post_if_new(normalized)
                    newest_seen = max(newest_seen, part.id or 0)
                    if post_id:
                        new_post_ids.append(post_id)
                        metrics.collected_posts += 1
            if newest_seen > cursor:
                await db.set_cursor(source.platform, source.source_key, newest_seen)
        except RPCError as exc:
            logger.warning("Collect failed for %s: %s", source.username, exc)
            continue
    return new_post_ids

