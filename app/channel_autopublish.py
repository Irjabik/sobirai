from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from time import monotonic
from typing import Any

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramNetworkError, TelegramRetryAfter

from .config import Settings
from .db import Database
from .llm_groq import call_groq_chat_json
from .metrics import RuntimeMetrics
from .prompts_channel import CHANNEL_REWRITE_PROMPT_VERSION, CHANNEL_REWRITE_SYSTEM_PROMPT_V1, build_channel_rewrite_user_message
from .text_norm import (
    fingerprint_text,
    has_new_details_vs_reference,
    near_duplicate_score,
)

logger = logging.getLogger(__name__)

TELEGRAM_MAX_MESSAGE_LEN = 4096


def _safe_retry_after(exc: TelegramRetryAfter) -> float:
    value = getattr(exc, "retry_after", 1)
    try:
        return max(1.0, float(value))
    except Exception:
        return 1.0


def _build_channel_message(title: str, post_text: str, hashtags: list[Any]) -> str:
    t = (title or "").strip()
    b = (post_text or "").strip()
    if t and b:
        body = f"{t}\n\n{b}"
    elif b:
        body = b
    elif t:
        body = t
    else:
        body = ""
    tags: list[str] = []
    if isinstance(hashtags, list):
        for h in hashtags:
            s = str(h).strip()
            if not s:
                continue
            s = s.lstrip("#")
            if s:
                tags.append(f"#{s}")
    if tags:
        body = f"{body}\n\n{' '.join(tags)}" if body else " ".join(tags)
    if len(body) > TELEGRAM_MAX_MESSAGE_LEN:
        body = body[: TELEGRAM_MAX_MESSAGE_LEN - 30] + "\n…(текст обрезан)"
    return body


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
    llm = call_groq_chat_json(
        api_key=settings.groq_api_key,
        model=settings.llm_model,
        system_prompt=CHANNEL_REWRITE_SYSTEM_PROMPT_V1,
        user_message=user_msg,
        max_output_tokens=settings.llm_max_output_tokens,
        timeout_seconds=settings.llm_timeout_seconds,
        max_retries=settings.llm_max_retries,
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
    hashtags_raw = llm.parsed.get("hashtags") or []

    await db.update_generated_channel_post(
        source_post_id,
        status="generated",
        llm_provider=settings.llm_provider,
        llm_model=settings.llm_model,
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

    outgoing = _build_channel_message(title, post_text, hashtags_raw if isinstance(hashtags_raw, list) else [])
    if not outgoing.strip():
        await fail("empty_outgoing_after_build")
        return

    try:
        msg_id = await _send_channel_message_with_retry(bot, metrics, channel_chat_id, outgoing)
    except Exception as exc:
        await fail(f"telegram_publish:{exc!s}"[:500])
        return

    now_iso = datetime.now(tz=timezone.utc).isoformat()
    await db.update_generated_channel_post(
        source_post_id,
        status="published",
        channel_message_id=msg_id,
        published_at=now_iso,
        clear_error=True,
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
