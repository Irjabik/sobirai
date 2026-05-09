from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from time import monotonic
from typing import Any

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramNetworkError, TelegramRetryAfter
from aiogram.types import FSInputFile, InputMediaPhoto, InputMediaVideo

from .db import Database
from .formatting import (
    TELEGRAM_CAPTION_HARD_LIMIT,
    deduplicate_digest_posts,
    expanded_source_post_ids_for_digest,
    render_digest_list,
    render_full_post_text,
)
from .metrics import RuntimeMetrics

logger = logging.getLogger(__name__)


def _safe_retry_after(exc: TelegramRetryAfter) -> float:
    value = getattr(exc, "retry_after", 1)
    try:
        return max(1.0, float(value))
    except Exception:
        return 1.0


def _is_caption_too_long(exc: Exception) -> bool:
    return "caption is too long" in str(exc).lower()


def _is_request_entity_too_large(exc: Exception) -> bool:
    return "request entity too large" in str(exc).lower()


def _is_user_delivery_blocked(exc: Exception) -> bool:
    text = str(exc).lower()
    return "bot was blocked by the user" in text or "user is deactivated" in text


def _video_send_options(post: dict) -> dict[str, Any]:
    """
    Optional Bot API fields so clients show duration and a JPEG thumbnail (<= ~200 KB, max 320 px).
    """
    opts: dict[str, Any] = {"supports_streaming": True}
    d = post.get("media_duration")
    if d is not None:
        opts["duration"] = int(d)
    w = post.get("media_width")
    if w is not None:
        opts["width"] = int(w)
    h = post.get("media_height")
    if h is not None:
        opts["height"] = int(h)
    tp = post.get("media_thumb_path")
    if tp:
        p = Path(tp)
        if p.is_file() and p.stat().st_size > 0:
            opts["thumbnail"] = FSInputFile(p)
    return opts


async def send_post_to_user(
    bot: Bot,
    db: Database,
    metrics: RuntimeMetrics,
    user_id: int,
    post: dict,
) -> None:
    start = monotonic()
    is_media_post = post.get("media_type") in {"photo", "video"}

    full_text = render_full_post_text(
        channel_title=post["channel_title"],
        channel_username=post["channel_username"],
        text=post["text"],
        source_link=post["source_link"],
    )

    if is_media_post:
        # Текст помещается в caption (≤1024) — отправим одним сообщением.
        # Иначе шлём медиа отдельным сообщением + полный текст следом, без обрезки.
        text_fits_caption = len(full_text) <= TELEGRAM_CAPTION_HARD_LIMIT
        caption: str | None
        send_text_separately: bool
        if text_fits_caption:
            caption = full_text
            send_text_separately = False
        else:
            caption = None  # медиа без подписи
            send_text_separately = True
    else:
        caption = full_text
        text_fits_caption = True
        send_text_separately = False

    attempts = 0
    backoff = 1.0
    last_error = None

    while attempts < 3:
        attempts += 1
        try:
            if post["media_type"] == "photo" and post["media_file_id"]:
                await bot.send_photo(chat_id=user_id, photo=post["media_file_id"], caption=caption)
            elif post["media_type"] == "video" and post["media_file_id"]:
                await bot.send_video(
                    chat_id=user_id,
                    video=post["media_file_id"],
                    caption=caption,
                    **_video_send_options(post),
                )
            elif post["media_type"] == "photo" and post["media_path"]:
                await bot.send_photo(
                    chat_id=user_id,
                    photo=FSInputFile(post["media_path"]),
                    caption=caption,
                )
            elif post["media_type"] == "video" and post["media_path"]:
                await bot.send_video(
                    chat_id=user_id,
                    video=FSInputFile(post["media_path"]),
                    caption=caption,
                    **_video_send_options(post),
                )
            else:
                await bot.send_message(
                    chat_id=user_id,
                    text=full_text,
                    disable_web_page_preview=True,
                )
            if send_text_separately:
                await bot.send_message(
                    chat_id=user_id,
                    text=full_text,
                    disable_web_page_preview=True,
                )
            latency = int((monotonic() - start) * 1000)
            await db.mark_delivery(user_id, post["id"], "sent", attempts, None, latency)
            metrics.sent_messages += 1
            return
        except TelegramAPIError as exc:
            if _is_user_delivery_blocked(exc):
                await db.set_pause(user_id, True)
                await db.mark_delivery(
                    user_id,
                    post["id"],
                    "failed",
                    attempts,
                    "blocked_by_user_auto_paused",
                    None,
                )
                logger.warning(
                    "User delivery disabled (bot blocked/deactivated) user=%s post=%s",
                    user_id,
                    post["id"],
                )
                metrics.failed_messages += 1
                return
            if _is_caption_too_long(exc):
                # Telegram отверг длинный caption. Шлём медиа без подписи + полный текст
                # отдельным сообщением, чтобы ничего не обрезать.
                caption = None
                send_text_separately = True
                logger.warning(
                    "Delivery caption-too-long user=%s post=%s; switching to media+text-split",
                    user_id,
                    post["id"],
                )
                await asyncio.sleep(0.2)
                continue
            if _is_request_entity_too_large(exc):
                # File is too large for Bot API upload; fallback to text-only delivery (полный текст).
                try:
                    await bot.send_message(
                        chat_id=user_id,
                        text=full_text,
                        disable_web_page_preview=True,
                    )
                    latency = int((monotonic() - start) * 1000)
                    await db.mark_delivery(
                        user_id,
                        post["id"],
                        "sent",
                        attempts,
                        "media_too_large_fallback_text",
                        latency,
                    )
                    metrics.sent_messages += 1
                    logger.warning(
                        "Delivery fallback text-only user=%s post=%s due to oversized media",
                        user_id,
                        post["id"],
                    )
                    return
                except TelegramAPIError:
                    pass
            last_error = str(exc)
            metrics.retry_attempts += 1
            if isinstance(exc, TelegramRetryAfter):
                sleep_for = _safe_retry_after(exc)
                logger.warning(
                    "Delivery throttled user=%s post=%s retry_after=%.1fs err=%s",
                    user_id,
                    post["id"],
                    sleep_for,
                    exc,
                )
            elif isinstance(exc, TelegramNetworkError):
                sleep_for = backoff * 2
                logger.warning(
                    "Delivery network error user=%s post=%s wait=%.1fs err=%s",
                    user_id,
                    post["id"],
                    sleep_for,
                    exc,
                )
            else:
                sleep_for = backoff
                logger.warning(
                    "Delivery attempt failed user=%s post=%s media_group=%s media_type=%s wait=%.1fs err=%s",
                    user_id,
                    post["id"],
                    post.get("media_group_id"),
                    post.get("media_type"),
                    sleep_for,
                    exc,
                )
            await asyncio.sleep(sleep_for)
            backoff *= 2

    await db.mark_delivery(user_id, post["id"], "failed", attempts, last_error, None)
    metrics.failed_messages += 1


async def deliver_mode(bot: Bot, db: Database, metrics: RuntimeMetrics, mode: str) -> None:
    rows = await db.undelivered_for_mode(mode)
    idx = 0
    while idx < len(rows):
        row = rows[idx]
        group_id = row.get("media_group_id")
        if not group_id:
            await send_post_to_user(bot, db, metrics, row["user_id"], row)
            idx += 1
            continue

        group_rows: list[dict] = [row]
        j = idx + 1
        while j < len(rows):
            nxt = rows[j]
            if (
                nxt["user_id"] == row["user_id"]
                and nxt.get("media_group_id") == group_id
                and nxt["channel_username"] == row["channel_username"]
            ):
                group_rows.append(nxt)
                j += 1
            else:
                break
        sent = await send_media_group_to_user(bot, db, row["user_id"], group_rows)
        if sent:
            metrics.sent_messages += 1
            for post in group_rows:
                await db.mark_delivery(row["user_id"], post["id"], "sent", 1, None, None)
        else:
            metrics.failed_messages += 1
            for post in group_rows:
                await db.mark_delivery(
                    row["user_id"],
                    post["id"],
                    "failed",
                    1,
                    "media group delivery failed",
                    None,
                )
        idx = j


async def send_media_group_to_user(bot: Bot, db: Database, user_id: int, posts: list[dict]) -> bool:
    if not posts:
        return True
    posts = sorted(posts, key=lambda p: int(p["source_message_id"]))
    caption_idx = 0
    for i, p in enumerate(posts):
        if (p.get("text") or "").strip():
            caption_idx = i
            break
    ordered_posts = [posts[caption_idx]] + [p for j, p in enumerate(posts) if j != caption_idx]
    main = ordered_posts[0]
    full_text = render_full_post_text(
        channel_title=main["channel_title"],
        channel_username=main["channel_username"],
        text=main["text"],
        source_link=main["source_link"],
    )
    text_fits_caption = len(full_text) <= TELEGRAM_CAPTION_HARD_LIMIT
    caption: str | None = full_text if text_fits_caption else None
    send_text_separately = not text_fits_caption

    media_items: list[InputMediaPhoto | InputMediaVideo] = []
    for i, post in enumerate(ordered_posts):
        cap = caption if i == 0 else None
        if post["media_type"] == "photo":
            media = post["media_file_id"] or (
                FSInputFile(post["media_path"]) if post.get("media_path") else None
            )
            if media is None:
                continue
            media_items.append(InputMediaPhoto(media=media, caption=cap))
        elif post["media_type"] == "video":
            media = post["media_file_id"] or (
                FSInputFile(post["media_path"]) if post.get("media_path") else None
            )
            if media is None:
                continue
            media_items.append(
                InputMediaVideo(media=media, caption=cap, **_video_send_options(post)),
            )
    if not media_items:
        return False

    attempts = 0
    backoff = 1.0
    while attempts < 3:
        attempts += 1
        try:
            await bot.send_media_group(chat_id=user_id, media=media_items)
            if send_text_separately:
                try:
                    await bot.send_message(
                        chat_id=user_id,
                        text=full_text,
                        disable_web_page_preview=True,
                    )
                except TelegramAPIError as exc:
                    logger.warning(
                        "Album text-tail send failed user=%s group=%s err=%s",
                        user_id,
                        posts[0].get("media_group_id"),
                        exc,
                    )
            return True
        except TelegramAPIError as exc:
            if _is_user_delivery_blocked(exc):
                await db.set_pause(user_id, True)
                logger.warning(
                    "User delivery disabled (bot blocked/deactivated) user=%s group=%s",
                    user_id,
                    posts[0].get("media_group_id"),
                )
                return False
            if _is_caption_too_long(exc):
                # Telegram отверг caption у альбома. Отдаём альбом без caption и шлём
                # полный текст следом отдельным сообщением — без обрезки.
                send_text_separately = True
                media_items = []
                for i, post in enumerate(ordered_posts):
                    if post["media_type"] == "photo":
                        media = post["media_file_id"] or (
                            FSInputFile(post["media_path"]) if post.get("media_path") else None
                        )
                        if media is not None:
                            media_items.append(InputMediaPhoto(media=media, caption=None))
                    elif post["media_type"] == "video":
                        media = post["media_file_id"] or (
                            FSInputFile(post["media_path"]) if post.get("media_path") else None
                        )
                        if media is not None:
                            media_items.append(
                                InputMediaVideo(media=media, caption=None, **_video_send_options(post)),
                            )
                await asyncio.sleep(0.2)
                continue
            if _is_request_entity_too_large(exc):
                # At least send text context (полный текст) instead of losing the whole album.
                try:
                    await bot.send_message(chat_id=user_id, text=full_text, disable_web_page_preview=True)
                    logger.warning(
                        "Media group oversized; sent text-only fallback user=%s group=%s",
                        user_id,
                        posts[0].get("media_group_id"),
                    )
                    return True
                except TelegramAPIError:
                    pass
            if isinstance(exc, TelegramRetryAfter):
                sleep_for = _safe_retry_after(exc)
            elif isinstance(exc, TelegramNetworkError):
                sleep_for = backoff * 2
            else:
                sleep_for = backoff
            logger.warning(
                "Media group delivery failed user=%s group=%s wait=%.1fs err=%s",
                user_id,
                posts[0].get("media_group_id"),
                sleep_for,
                exc,
            )
            await asyncio.sleep(sleep_for)
            backoff *= 2
    return False


async def deliver_configurable_digests(bot: Bot, db: Database, metrics: RuntimeMetrics) -> None:
    users = await db.get_due_digest_users()
    for row in users:
        user_id = int(row["user_id"])
        hours = int(row["digest_interval_hours"] or 12)
        filter_enabled = bool(row.get("digest_filter_enabled", 1))
        if filter_enabled:
            posts = await db.undelivered_for_user(user_id=user_id, hours_window=hours, limit=300)
        else:
            posts = await db.undelivered_for_user_unfiltered(user_id=user_id, limit=300)
        if not posts:
            await db.touch_digest_sent_at(user_id)
            continue
        deduped_posts = deduplicate_digest_posts(posts, limit=10)
        if deduped_posts:
            sent = await send_digest_list_to_user(
                bot=bot,
                db=db,
                user_id=user_id,
                posts=deduped_posts,
                hours_window=hours if filter_enabled else 0,
            )
            digest_ids = expanded_source_post_ids_for_digest(deduped_posts)
            if sent:
                metrics.sent_messages += 1
                for pid in digest_ids:
                    await db.mark_delivery(
                        user_id=user_id,
                        source_post_id=pid,
                        status="sent",
                        attempts=1,
                    )
            else:
                metrics.failed_messages += 1
                for pid in digest_ids:
                    await db.mark_delivery(
                        user_id=user_id,
                        source_post_id=pid,
                        status="failed",
                        attempts=1,
                        last_error="digest list delivery failed",
                    )
        await db.touch_digest_sent_at(user_id)


async def send_digest_list_to_user(
    bot: Bot,
    db: Database,
    user_id: int,
    posts: list[dict],
    hours_window: int,
) -> bool:
    text = render_digest_list(posts, hours_window=hours_window)
    attempts = 0
    backoff = 1.0
    while attempts < 3:
        attempts += 1
        try:
            await bot.send_message(
                chat_id=user_id,
                text=text,
                disable_web_page_preview=True,
            )
            return True
        except TelegramAPIError as exc:
            if _is_user_delivery_blocked(exc):
                await db.set_pause(user_id, True)
                logger.warning("Digest delivery disabled (bot blocked/deactivated) user=%s", user_id)
                return False
            if isinstance(exc, TelegramRetryAfter):
                sleep_for = _safe_retry_after(exc)
            elif isinstance(exc, TelegramNetworkError):
                sleep_for = backoff * 2
            else:
                sleep_for = backoff
            logger.warning("Digest delivery failed user=%s wait=%.1fs err=%s", user_id, sleep_for, exc)
            await asyncio.sleep(sleep_for)
            backoff *= 2
    return False

