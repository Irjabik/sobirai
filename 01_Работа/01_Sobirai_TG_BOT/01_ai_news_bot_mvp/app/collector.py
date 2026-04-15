from __future__ import annotations

import asyncio
import errno
import json
import logging
from asyncio import to_thread
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.tl.types import Message

from .db import Database, NormalizedPost
from .metrics import RuntimeMetrics
from .sources import SOURCES

logger = logging.getLogger(__name__)
X_SNSCRAPE_BLOCK_COOLDOWN = timedelta(minutes=30)
_x_snscrape_blocked_until: dict[str, datetime] = {}
X_SOURCE_FAILURE_COOLDOWN = timedelta(minutes=10)
_x_source_fail_until: dict[str, datetime] = {}
X_GLOBAL_FALLBACK_OUTAGE_COOLDOWN = timedelta(minutes=5)
_x_global_fallback_outage_until: datetime | None = None


def _is_x_blocked_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "blocked (404)" in message or ("searchtimeline" in message and "404" in message)


def _is_transport_refused_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "connection refused" in text or "timed out" in text or "network is unreachable" in text


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


def _fetch_x_items_blocking(handle: str, since_id: int, limit: int) -> list[dict]:
    try:
        import snscrape.modules.twitter as sntwitter
    except Exception as exc:
        raise RuntimeError("snscrape is not installed") from exc
    username = handle.lstrip("@")
    fetched: list[dict] = []
    for tw in sntwitter.TwitterUserScraper(username).get_items():
        if len(fetched) >= limit:
            break
        tw_id = int(getattr(tw, "id", 0) or 0)
        if tw_id <= since_id:
            continue
        content = getattr(tw, "rawContent", None) or getattr(tw, "content", "") or ""
        date = getattr(tw, "date", None)
        if date is None:
            continue
        ext_url = None
        links = getattr(tw, "outlinks", None) or []
        if links:
            ext_url = str(links[0])
        fetched.append(
            {
                "id": tw_id,
                "date": date,
                "text": content,
                "source_link": source_link("x", handle, tw_id),
                "external_url": ext_url,
            }
        )
    return fetched


def _fetch_x_items_syndication_blocking(handle: str, since_id: int, limit: int) -> list[dict]:
    username = handle.lstrip("@")
    params = urlencode(
        {
            "screen_name": username,
            "count": max(5, min(limit, 100)),
            "include_rts": "true",
            "exclude_replies": "false",
        }
    )
    url = f"https://cdn.syndication.twimg.com/timeline/profile?{params}"
    payload: dict = {}
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; SobiraiBot/1.0)"})
    with urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        payload = _decode_syndication_payload(raw, handle)
    tweets: list[dict] = []
    global_objects = payload.get("globalObjects")
    if isinstance(global_objects, dict):
        tweets_map = global_objects.get("tweets", {})
        if isinstance(tweets_map, dict):
            tweets = [v for v in tweets_map.values() if isinstance(v, dict)]
    if not tweets and isinstance(payload.get("tweets"), list):
        tweets = [v for v in payload.get("tweets", []) if isinstance(v, dict)]

    fetched: list[dict] = []
    for tw in tweets:
        tw_id_raw = tw.get("id") or tw.get("id_str")
        if not tw_id_raw:
            continue
        tw_id = int(tw_id_raw)
        if tw_id <= since_id:
            continue
        text = (tw.get("full_text") or tw.get("text") or "").strip()
        created_at = tw.get("created_at")
        if not created_at:
            continue
        # Example format: "Tue Apr 15 10:43:21 +0000 2026"
        dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y").astimezone(timezone.utc)
        fetched.append(
            {
                "id": tw_id,
                "date": dt,
                "text": text,
                "source_link": source_link("x", handle, tw_id),
                "external_url": None,
            }
        )
    fetched.sort(key=lambda x: int(x["id"]))
    return fetched[:limit]


def _decode_syndication_payload(raw: str, handle: str) -> dict:
    cleaned = raw.strip()
    if not cleaned:
        raise ValueError("empty syndication response body")
    # Some proxies prepend garbage text before JSON. Try strict first, then extract JSON frame.
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end == -1 or end <= start:
            preview = cleaned[:120].replace("\n", " ")
            raise ValueError(f"non-json syndication response for {handle}: {preview}")
        framed = cleaned[start : end + 1]
        return json.loads(framed)


def _fetch_x_items_nitter_rss_blocking(handle: str, since_id: int, limit: int) -> list[dict]:
    username = handle.lstrip("@")
    rss_urls = (
        f"https://nitter.net/{username}/rss",
        f"https://nitter.poast.org/{username}/rss",
        f"https://nitter.privacydev.net/{username}/rss",
    )
    last_error: Exception | None = None
    for url in rss_urls:
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; SobiraiBot/1.0)"})
            with urlopen(req, timeout=20) as resp:
                raw = resp.read()
            root = ElementTree.fromstring(raw)
            channel = root.find("channel")
            if channel is None:
                continue
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
            if fetched:
                fetched.sort(key=lambda x: int(x["id"]))
                return fetched[:limit]
        except (ElementTree.ParseError, URLError, ValueError) as exc:
            last_error = exc
            continue
    raise RuntimeError(f"nitter rss fallback failed for {handle}: {last_error}")


async def collect_new_posts(
    client: TelegramClient,
    db: Database,
    metrics: RuntimeMetrics,
    media_dir: Path,
    *,
    enable_x_sources: bool = True,
    x_use_snscrape: bool = False,
    x_fetch_timeout_seconds: int = 25,
    x_fetch_retries: int = 0,
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
            global _x_global_fallback_outage_until
            source_fail_until = _x_source_fail_until.get(source.source_key)
            if source_fail_until is not None and now_utc < source_fail_until:
                logger.info(
                    "Skipping X source %s until %s due to repeated transport failures",
                    source.username,
                    source_fail_until.isoformat(),
                )
                continue
            rows: list[dict] = []
            if x_use_snscrape:
                try:
                    blocked_until = _x_snscrape_blocked_until.get(source.source_key)
                    allow_snscrape = blocked_until is None or now_utc >= blocked_until
                    if allow_snscrape:
                        total_attempts = max(1, x_fetch_retries + 1)
                        backoff_seconds = 1.0
                        for attempt in range(1, total_attempts + 1):
                            try:
                                rows = await asyncio.wait_for(
                                    to_thread(
                                        _fetch_x_items_blocking,
                                        source.username,
                                        cursor,
                                        40 if cursor == 0 else 100,
                                    ),
                                    timeout=max(5, x_fetch_timeout_seconds),
                                )
                                _x_snscrape_blocked_until.pop(source.source_key, None)
                                break
                            except Exception as exc:
                                if _is_x_blocked_error(exc):
                                    _x_snscrape_blocked_until[source.source_key] = now_utc + X_SNSCRAPE_BLOCK_COOLDOWN
                                    raise exc
                                if attempt >= total_attempts:
                                    raise exc
                                logger.warning(
                                    "X fetch retry %s/%s for %s after error: %s",
                                    attempt,
                                    total_attempts,
                                    source.username,
                                    exc,
                                )
                                await asyncio.sleep(backoff_seconds)
                                backoff_seconds = min(backoff_seconds * 2, 10)
                    else:
                        raise RuntimeError(
                            f"snscrape cooldown active until {blocked_until.isoformat()}"
                        )
                except Exception as exc:
                    logger.warning(
                        "Collect failed for X source %s via snscrape: %s; trying syndication fallback",
                        source.username,
                        exc,
                    )
                    try:
                        rows = await to_thread(
                            _fetch_x_items_syndication_blocking,
                            source.username,
                            cursor,
                            40 if cursor == 0 else 100,
                        )
                    except Exception as fallback_exc:
                        logger.warning(
                            "Collect failed for X source %s via syndication fallback: %s; trying nitter rss fallback",
                            source.username,
                            fallback_exc,
                        )
                        try:
                            rows = await to_thread(
                                _fetch_x_items_nitter_rss_blocking,
                                source.username,
                                cursor,
                                40 if cursor == 0 else 100,
                            )
                        except Exception as rss_exc:
                            logger.warning(
                                "Collect failed for X source %s via nitter rss fallback: %s",
                                source.username,
                                rss_exc,
                            )
                            _x_source_fail_until[source.source_key] = now_utc + X_SOURCE_FAILURE_COOLDOWN
                            continue
            else:
                try:
                    rows = await to_thread(
                        _fetch_x_items_syndication_blocking,
                        source.username,
                        cursor,
                        40 if cursor == 0 else 100,
                    )
                except Exception as fallback_exc:
                    logger.warning(
                        "Collect failed for X source %s via syndication fallback: %s; trying nitter rss fallback",
                        source.username,
                        fallback_exc,
                    )
                    try:
                        rows = await to_thread(
                            _fetch_x_items_nitter_rss_blocking,
                            source.username,
                            cursor,
                            40 if cursor == 0 else 100,
                        )
                    except Exception as rss_exc:
                        logger.warning(
                            "Collect failed for X source %s via nitter rss fallback: %s; trying emergency snscrape",
                            source.username,
                            rss_exc,
                        )
                        if _is_transport_refused_error(rss_exc):
                            _x_global_fallback_outage_until = now_utc + X_GLOBAL_FALLBACK_OUTAGE_COOLDOWN
                            _x_source_fail_until[source.source_key] = now_utc + X_SOURCE_FAILURE_COOLDOWN
                            logger.warning(
                                "Skipping emergency snscrape for %s due to transport outage; global cooldown until %s",
                                source.username,
                                _x_global_fallback_outage_until.isoformat(),
                            )
                            continue
                        if _x_global_fallback_outage_until is not None and now_utc < _x_global_fallback_outage_until:
                            _x_source_fail_until[source.source_key] = now_utc + X_SOURCE_FAILURE_COOLDOWN
                            logger.warning(
                                "Skipping emergency snscrape for %s due to active global outage cooldown until %s",
                                source.username,
                                _x_global_fallback_outage_until.isoformat(),
                            )
                            continue
                        try:
                            rows = await asyncio.wait_for(
                                to_thread(
                                    _fetch_x_items_blocking,
                                    source.username,
                                    cursor,
                                    40 if cursor == 0 else 100,
                                ),
                                timeout=max(5, x_fetch_timeout_seconds),
                            )
                        except Exception as sn_exc:
                            logger.warning(
                                "Collect failed for X source %s via emergency snscrape: %s",
                                source.username,
                                sn_exc,
                            )
                            _x_source_fail_until[source.source_key] = now_utc + X_SOURCE_FAILURE_COOLDOWN
                            continue
            try:
                _x_source_fail_until.pop(source.source_key, None)
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

