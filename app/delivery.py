from __future__ import annotations

import asyncio
import logging
from time import monotonic

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError
from aiogram.types import FSInputFile, InputMediaPhoto, InputMediaVideo

from .db import Database
from .formatting import (
    deduplicate_digest_posts,
    expanded_source_post_ids_for_digest,
    render_caption,
    render_digest_list,
)
from .metrics import RuntimeMetrics

logger = logging.getLogger(__name__)


async def send_post_to_user(
    bot: Bot,
    db: Database,
    metrics: RuntimeMetrics,
    user_id: int,
    post: dict,
) -> None:
    start = monotonic()
    caption = render_caption(
        channel_title=post["channel_title"],
        channel_username=post["channel_username"],
        source_date=post["source_message_date"],
        text=post["text"],
        source_link=post["source_link"],
    )
    attempts = 0
    backoff = 1.0
    last_error = None

    while attempts < 3:
        attempts += 1
        try:
            if post["media_type"] == "photo" and post["media_file_id"]:
                await bot.send_photo(chat_id=user_id, photo=post["media_file_id"], caption=caption)
            elif post["media_type"] == "video" and post["media_file_id"]:
                await bot.send_video(chat_id=user_id, video=post["media_file_id"], caption=caption)
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
                )
            else:
                await bot.send_message(
                    chat_id=user_id,
                    text=caption,
                    disable_web_page_preview=True,
                )
            latency = int((monotonic() - start) * 1000)
            await db.mark_delivery(user_id, post["id"], "sent", attempts, None, latency)
            metrics.sent_messages += 1
            return
        except TelegramAPIError as exc:
            last_error = str(exc)
            metrics.retry_attempts += 1
            logger.warning("Delivery attempt failed user=%s post=%s err=%s", user_id, post["id"], exc)
            await asyncio.sleep(backoff)
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
        sent = await send_media_group_to_user(bot, row["user_id"], group_rows)
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


async def send_media_group_to_user(bot: Bot, user_id: int, posts: list[dict]) -> bool:
    if not posts:
        return True
    posts = sorted(posts, key=lambda p: int(p["source_message_id"]))
    main = next((p for p in posts if p.get("text")), posts[0])
    caption = render_caption(
        channel_title=main["channel_title"],
        channel_username=main["channel_username"],
        source_date=main["source_message_date"],
        text=main["text"],
        source_link=main["source_link"],
    )

    media_items: list[InputMediaPhoto | InputMediaVideo] = []
    for i, post in enumerate(posts):
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
            media_items.append(InputMediaVideo(media=media, caption=cap))
    if not media_items:
        return False

    attempts = 0
    backoff = 1.0
    while attempts < 3:
        attempts += 1
        try:
            await bot.send_media_group(chat_id=user_id, media=media_items)
            return True
        except TelegramAPIError as exc:
            logger.warning("Media group delivery failed user=%s err=%s", user_id, exc)
            await asyncio.sleep(backoff)
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
            logger.warning("Digest delivery failed user=%s err=%s", user_id, exc)
            await asyncio.sleep(backoff)
            backoff *= 2
    return False

