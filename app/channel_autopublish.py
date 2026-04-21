from __future__ import annotations

import asyncio
import html
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import urlparse

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramNetworkError, TelegramRetryAfter
from aiogram.types import FSInputFile, InputMediaPhoto, InputMediaVideo

from .config import Settings
from .db import Database
from .llm_client import RoutedLlmResult, call_llm_with_fallback
from .metrics import RuntimeMetrics
from .prompts_channel import CHANNEL_REWRITE_PROMPT_VERSION, CHANNEL_REWRITE_SYSTEM_PROMPT_V1, build_channel_rewrite_user_message
from .text_norm import (
    fingerprint_text,
    has_new_details_vs_reference,
    near_duplicate_score,
)

logger = logging.getLogger(__name__)

TELEGRAM_MAX_MESSAGE_LEN = 4096
TELEGRAM_MAX_CAPTION_LEN = 1024
CHANNEL_BRAND_FOOTER_HTML = '<a href="https://t.me/sobirai_news">Sobirai_News</a>'
URL_RE = re.compile(r"(https?://[^\s<>\"]+)", flags=re.IGNORECASE)
READ_MORE_PATTERNS = (
    re.compile(r"\bчитать\s*далее\b[:\s\-–—]*.*$", flags=re.IGNORECASE | re.DOTALL),
    re.compile(r"\bread\s*more\b[:\s\-–—]*.*$", flags=re.IGNORECASE | re.DOTALL),
    re.compile(r"\bдалее\s+по\s+ссылке\b[:\s\-–—]*.*$", flags=re.IGNORECASE | re.DOTALL),
)


def _safe_retry_after(exc: TelegramRetryAfter) -> float:
    value = getattr(exc, "retry_after", 1)
    try:
        return max(1.0, float(value))
    except Exception:
        return 1.0


def _ensure_bold_title(title: str) -> str:
    t = (title or "").strip()
    if not t:
        return ""
    if t.startswith("<b>") and t.endswith("</b>"):
        return t
    return f"<b>{t}</b>"


def _strip_trailing_read_more(text: str) -> str:
    out = (text or "").strip()
    if not out:
        return ""
    for p in READ_MORE_PATTERNS:
        out = p.sub("", out).strip()
    # Remove trailing teaser punctuation artifacts often left by channel previews.
    out = re.sub(r"(?:\s*(?:\.\.\.|…)\s*)+$", "", out).strip()
    return out


def _extract_first_url(text: str) -> str | None:
    m = URL_RE.search(text or "")
    if not m:
        return None
    return m.group(1).strip().rstrip(").,;")


def _resource_label_from_url(url: str) -> str:
    host = (urlparse(url).netloc or "").lower().strip()
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return "Источник"
    parts = host.split(".")
    if len(parts) >= 2:
        name = parts[-2]
    else:
        name = parts[0]
    if not name:
        return host or "Источник"
    return name.capitalize()


def _build_source_link_block(raw_text: str, source_link: str) -> str:
    url = _extract_first_url(raw_text) or (source_link or "").strip()
    if not url:
        return ""
    safe_url = html.escape(url, quote=True)
    safe_label = html.escape(_resource_label_from_url(url))
    return f'Источник: <a href="{safe_url}">{safe_label}</a>'


def _build_channel_message(title: str, post_text: str, source_link_block: str) -> str:
    t = _ensure_bold_title(title)
    b = (post_text or "").strip()
    if t and b:
        body = f"{t}\n\n{b}"
    elif b:
        body = b
    elif t:
        body = t
    else:
        body = ""
    if source_link_block:
        body = f"{body}\n\n{source_link_block}" if body else source_link_block
    body = f"{body}\n{CHANNEL_BRAND_FOOTER_HTML}" if body else CHANNEL_BRAND_FOOTER_HTML
    if len(body) > TELEGRAM_MAX_MESSAGE_LEN:
        body = body[: TELEGRAM_MAX_MESSAGE_LEN - 30] + "\n…(текст обрезан)"
    return body


def _as_caption(text: str) -> str:
    if len(text) <= TELEGRAM_MAX_CAPTION_LEN:
        return text
    return text[: TELEGRAM_MAX_CAPTION_LEN - 18] + "\n…(подпись обрезана)"


def _validate_llm_payload(parsed: dict[str, Any]) -> tuple[bool, str]:
    st = parsed.get("status")
    if st not in {"ok", "skip", "skip_duplicate"}:
        return False, "invalid_status"
    if st == "ok":
        for key in ("title", "post_text", "short_summary"):
            v = parsed.get(key)
            if not isinstance(v, str) or not v.strip():
                return False, f"empty_or_bad_{key}"
        ht = parsed.get("hashtags")
        if ht is not None and not isinstance(ht, list):
            return False, "bad_hashtags_type"
    return True, ""


async def _send_channel_message_with_retry(
    bot: Bot,
    metrics: RuntimeMetrics,
    chat_id: int,
    text: str,
) -> int:
    attempts = 0
    backoff = 1.0
    last_err: str | None = None
    while attempts < 3:
        attempts += 1
        try:
            msg = await bot.send_message(
                chat_id=chat_id,
                text=text,
                disable_web_page_preview=True,
            )
            return int(msg.message_id)
        except TelegramRetryAfter as exc:
            metrics.channel_telegram_retries += 1
            wait = _safe_retry_after(exc)
            logger.warning(
                "Channel publish throttled chat=%s retry_after=%.1fs",
                chat_id,
                wait,
            )
            await asyncio.sleep(wait)
        except (TelegramNetworkError, ConnectionError) as exc:
            metrics.channel_telegram_retries += 1
            last_err = f"network:{exc}"
            logger.warning("Channel publish network err=%s backoff=%.1fs", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(8.0, backoff * 2.0)
        except TelegramAPIError as exc:
            last_err = str(exc)
            logger.warning("Channel publish TelegramAPIError: %s", exc)
            raise
    raise RuntimeError(last_err or "channel_send_failed")


async def _send_single_media_with_retry(
    bot: Bot,
    metrics: RuntimeMetrics,
    chat_id: int,
    post: dict[str, Any],
    caption: str,
) -> int:
    attempts = 0
    backoff = 1.0
    last_err: str | None = None
    while attempts < 3:
        attempts += 1
        try:
            media_type = str(post.get("media_type") or "")
            file_id = post.get("media_file_id")
            media_path = post.get("media_path")
            thumb_path = str(post.get("media_thumb_path") or "").strip()
            thumb_file = FSInputFile(thumb_path) if thumb_path and Path(thumb_path).exists() else None
            if media_type == "photo":
                if file_id:
                    msg = await bot.send_photo(chat_id=chat_id, photo=file_id, caption=caption)
                elif media_path:
                    msg = await bot.send_photo(chat_id=chat_id, photo=FSInputFile(media_path), caption=caption)
                else:
                    raise RuntimeError("single_photo_missing_file")
            elif media_type == "video":
                if file_id:
                    msg = await bot.send_video(chat_id=chat_id, video=file_id, caption=caption, supports_streaming=True)
                elif media_path:
                    msg = await bot.send_video(
                        chat_id=chat_id,
                        video=FSInputFile(media_path),
                        caption=caption,
                        supports_streaming=True,
                        thumbnail=thumb_file,
                    )
                else:
                    raise RuntimeError("single_video_missing_file")
            else:
                raise RuntimeError(f"unsupported_single_media_type:{media_type}")
            return int(msg.message_id)
        except TelegramRetryAfter as exc:
            metrics.channel_telegram_retries += 1
            wait = _safe_retry_after(exc)
            await asyncio.sleep(wait)
        except (TelegramNetworkError, ConnectionError) as exc:
            metrics.channel_telegram_retries += 1
            last_err = f"network:{exc}"
            await asyncio.sleep(backoff)
            backoff = min(8.0, backoff * 2.0)
        except TelegramAPIError as exc:
            last_err = str(exc)
            if "request entity too large" in str(exc).lower():
                raise RuntimeError("media_request_too_large")
            raise
    raise RuntimeError(last_err or "single_media_send_failed")


def _build_group_media_items(posts: list[dict[str, Any]], caption: str) -> list[InputMediaPhoto | InputMediaVideo]:
    items: list[InputMediaPhoto | InputMediaVideo] = []
    for i, p in enumerate(posts):
        media_type = str(p.get("media_type") or "")
        media_file_id = p.get("media_file_id")
        media_path = p.get("media_path")
        media_obj: str | FSInputFile | None = None
        if media_file_id:
            media_obj = str(media_file_id)
        elif media_path:
            media_obj = FSInputFile(str(media_path))
        if media_obj is None:
            continue
        cap = _as_caption(caption) if i == 0 else None
        if media_type == "photo":
            items.append(InputMediaPhoto(media=media_obj, caption=cap))
        elif media_type == "video":
            thumb_path = str(p.get("media_thumb_path") or "").strip()
            thumb_file = FSInputFile(thumb_path) if thumb_path and Path(thumb_path).exists() else None
            items.append(InputMediaVideo(media=media_obj, caption=cap, supports_streaming=True, thumbnail=thumb_file))
    return items


async def _send_media_group_with_retry(
    bot: Bot,
    metrics: RuntimeMetrics,
    chat_id: int,
    group_posts: list[dict[str, Any]],
    caption: str,
) -> int:
    attempts = 0
    backoff = 1.0
    last_err: str | None = None
    while attempts < 3:
        attempts += 1
        try:
            media = _build_group_media_items(group_posts, caption)
            if not media:
                raise RuntimeError("media_group_empty_items")
            msgs = await bot.send_media_group(chat_id=chat_id, media=media)
            if not msgs:
                raise RuntimeError("media_group_empty_response")
            return int(msgs[0].message_id)
        except TelegramRetryAfter as exc:
            metrics.channel_telegram_retries += 1
            await asyncio.sleep(_safe_retry_after(exc))
        except (TelegramNetworkError, ConnectionError) as exc:
            metrics.channel_telegram_retries += 1
            last_err = f"network:{exc}"
            await asyncio.sleep(backoff)
            backoff = min(8.0, backoff * 2.0)
        except TelegramAPIError as exc:
            last_err = str(exc)
            if "request entity too large" in str(exc).lower():
                raise RuntimeError("media_group_request_too_large")
            raise
    raise RuntimeError(last_err or "media_group_send_failed")


async def _process_one_source_post(
    *,
    db: Database,
    bot: Bot,
    metrics: RuntimeMetrics,
    settings: Settings,
    post: dict[str, Any],
) -> None:
    source_post_id = int(post["id"])
    channel_chat_id = int(settings.channel_chat_id or 0)
    day_utc = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    claimed = await db.claim_channel_processing(source_post_id, channel_chat_id)
    if not claimed:
        return

    metrics.channel_candidates_seen += 1
    raw_text = str(post.get("text") or "")
    cleaned_text = _strip_trailing_read_more(raw_text)
    if cleaned_text:
        raw_text = cleaned_text

    async def fail(msg: str) -> None:
        metrics.channel_failed += 1
        await db.update_generated_channel_post(
            source_post_id,
            status="failed",
            error=msg[:500],
        )
        logger.warning("channel_autopublish source_post_id=%s FAILED %s", source_post_id, msg)

    async def skip(status: str, reason: str, **kwargs: Any) -> None:
        if status == "duplicate":
            metrics.channel_duplicates += 1
        elif status == "skipped_by_limit":
            metrics.channel_skipped_limit += 1
        else:
            metrics.channel_skipped += 1
        await db.update_generated_channel_post(
            source_post_id,
            status=status,
            error=reason[:500],
            **kwargs,
        )
        logger.info(
            "channel_autopublish source_post_id=%s status=%s reason=%s",
            source_post_id,
            status,
            reason,
        )

    if len(raw_text.strip()) < settings.channel_min_candidate_chars:
        await skip("skipped", "candidate_too_short")
        return

    fp = fingerprint_text(raw_text)
    await db.update_generated_channel_post(source_post_id, fingerprint=fp)

    dup_exact = await db.find_channel_fingerprint_duplicate(fp, source_post_id)
    if dup_exact is not None:
        await skip(
            "duplicate",
            "exact_fingerprint_match",
            duplicate_of_source_post_id=dup_exact,
        )
        return

    recent = await db.list_recent_published_source_texts_for_channel_dedup(limit=300)
    near_dup_of: int | None = None
    best_score = 0.0
    for other_id, other_text in recent:
        if other_id == source_post_id:
            continue
        score = near_duplicate_score(raw_text, other_text)
        if score > best_score:
            best_score = score
        if score >= settings.channel_near_dup_jaccard:
            if not has_new_details_vs_reference(raw_text, other_text):
                near_dup_of = other_id
                break

    if near_dup_of is not None:
        await skip(
            "duplicate",
            f"near_duplicate_jaccard>={settings.channel_near_dup_jaccard:.2f}",
            duplicate_of_source_post_id=near_dup_of,
        )
        return

    daily = await db.get_channel_daily_publish_count(day_utc)
    if daily >= settings.channel_max_posts_per_day:
        await skip("skipped_by_limit", "daily_limit_pre_llm")
        return

    user_msg = build_channel_rewrite_user_message(raw_text[: settings.llm_max_input_chars])
    metrics.channel_llm_calls += 1
    t0 = monotonic()
    llm: RoutedLlmResult = call_llm_with_fallback(
        settings,
        system_prompt=CHANNEL_REWRITE_SYSTEM_PROMPT_V1,
        user_message=user_msg,
    )
    dt_ms = int((monotonic() - t0) * 1000)
    if not llm.ok or llm.parsed is None:
        await fail(f"llm_error:{llm.error_code}:attempts={llm.attempts}")
        return

    ok_schema, schema_reason = _validate_llm_payload(llm.parsed)
    if not ok_schema:
        await fail(f"llm_schema:{schema_reason}")
        return

    st = str(llm.parsed.get("status"))
    if st == "skip":
        await skip("skipped", "llm_status_skip")
        return
    if st == "skip_duplicate":
        await skip("duplicate", "llm_status_skip_duplicate")
        return

    title = str(llm.parsed.get("title") or "").strip()
    post_text = str(llm.parsed.get("post_text") or "").strip()
    short_summary = str(llm.parsed.get("short_summary") or "").strip()
    source_link_block = _build_source_link_block(raw_text, str(post.get("source_link") or ""))

    await db.update_generated_channel_post(
        source_post_id,
        status="generated",
        llm_provider=llm.provider_used,
        llm_model=llm.model_used,
        prompt_version=CHANNEL_REWRITE_PROMPT_VERSION,
        title=title,
        post_text=post_text,
        summary=short_summary,
        clear_error=True,
    )
    logger.debug(
        "channel_autopublish source_post_id=%s generated llm_latency_ms=%s",
        source_post_id,
        dt_ms,
    )

    daily2 = await db.get_channel_daily_publish_count(day_utc)
    if daily2 >= settings.channel_max_posts_per_day:
        await skip(
            "skipped_by_limit",
            "daily_limit_post_llm",
            title=title,
            post_text=post_text,
            summary=short_summary,
        )
        return

    outgoing = _build_channel_message(
        title,
        post_text,
        source_link_block,
    )
    if not outgoing.strip():
        await fail("empty_outgoing_after_build")
        return

    msg_id: int
    publish_reason: str | None = None
    try:
        media_group_id = str(post.get("media_group_id") or "")
        media_type = str(post.get("media_type") or "")
        has_single_media = media_type in {"photo", "video"} and (
            post.get("media_file_id") or post.get("media_path")
        )
        if media_group_id:
            group_posts = await db.list_source_posts_by_media_group(media_group_id)
            msg_id = await _send_media_group_with_retry(
                bot,
                metrics,
                channel_chat_id,
                group_posts,
                outgoing,
            )
            publish_reason = "media_group_sent"
            for gp in group_posts:
                sid = int(gp["id"])
                if sid == source_post_id:
                    continue
                await db.claim_channel_processing(sid, channel_chat_id)
                await db.update_generated_channel_post(
                    sid,
                    status="published",
                    llm_provider=llm.provider_used,
                    llm_model=llm.model_used,
                    prompt_version=CHANNEL_REWRITE_PROMPT_VERSION,
                    title=title,
                    post_text=post_text,
                    summary=short_summary,
                    channel_message_id=msg_id,
                    published_at=datetime.now(tz=timezone.utc).isoformat(),
                    error="media_group_sent_member",
                )
        elif has_single_media:
            msg_id = await _send_single_media_with_retry(
                bot,
                metrics,
                channel_chat_id,
                post,
                _as_caption(outgoing),
            )
            publish_reason = "single_media_sent"
        else:
            msg_id = await _send_channel_message_with_retry(bot, metrics, channel_chat_id, outgoing)
            publish_reason = "text_sent"
    except Exception as exc:
        logger.warning("channel_autopublish media publish failed, fallback text-only: %s", exc)
        try:
            msg_id = await _send_channel_message_with_retry(bot, metrics, channel_chat_id, outgoing)
            await db.update_generated_channel_post(
                source_post_id,
                error=f"media_fallback_text:{str(exc)[:200]}",
            )
        except Exception as exc2:
            await fail(f"telegram_publish:{exc2!s}"[:500])
            return

    now_iso = datetime.now(tz=timezone.utc).isoformat()
    await db.update_generated_channel_post(
        source_post_id,
        status="published",
        channel_message_id=msg_id,
        published_at=now_iso,
        error=publish_reason,
    )
    await db.increment_channel_daily_publish_count(day_utc)
    metrics.channel_published += 1
    metrics.sent_messages += 1
    logger.info(
        "channel_autopublish published source_post_id=%s msg_id=%s day_utc=%s",
        source_post_id,
        msg_id,
        day_utc,
    )


async def run_channel_autopublish_loop(
    db: Database,
    bot: Bot,
    metrics: RuntimeMetrics,
    settings: Settings,
    stop_event: asyncio.Event,
) -> None:
    logger.info(
        "Channel autopublish loop started chat_id=%s poll=%ss max/day=%s "
        "llm_candidates_per_tick=%s llm_gap_s=%.1f",
        settings.channel_chat_id,
        settings.channel_poll_seconds,
        settings.channel_max_posts_per_day,
        settings.channel_llm_candidates_per_tick,
        settings.channel_llm_gap_seconds,
    )
    while not stop_event.is_set():
        try:
            stale_before = (datetime.now(tz=timezone.utc) - timedelta(seconds=900)).isoformat()
            n_reset = await db.reset_stale_channel_processing(stale_before)
            if n_reset:
                logger.warning("channel_autopublish reset_stale_processing rows=%s", n_reset)

            cap = max(1, min(20, int(settings.channel_llm_candidates_per_tick)))
            candidates = await db.list_channel_autopublish_candidates(limit=cap)
            for i, post in enumerate(candidates):
                if stop_event.is_set():
                    break
                try:
                    await _process_one_source_post(
                        db=db,
                        bot=bot,
                        metrics=metrics,
                        settings=settings,
                        post=post,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    metrics.channel_failed += 1
                    logger.exception(
                        "channel_autopublish tick failure source_post_id=%s",
                        post.get("id"),
                    )
                    try:
                        pid = int(post["id"])
                        await db.update_generated_channel_post(
                            pid,
                            status="failed",
                            error="unhandled_pipeline_exception",
                        )
                    except Exception:
                        logger.exception("channel_autopublish failed to persist error row")
                if i + 1 < len(candidates) and settings.channel_llm_gap_seconds > 0:
                    await asyncio.sleep(float(settings.channel_llm_gap_seconds))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Channel autopublish loop outer failure")
        await asyncio.sleep(max(5, int(settings.channel_poll_seconds)))
