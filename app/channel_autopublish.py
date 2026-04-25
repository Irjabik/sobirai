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
    new_details_signal,
    near_duplicate_score,
    significant_tokens,
)

logger = logging.getLogger(__name__)

TELEGRAM_MAX_MESSAGE_LEN = 4096
TELEGRAM_MAX_CAPTION_LEN = 1024
CHANNEL_BRAND_FOOTER_HTML = '<a href="https://t.me/sobirai_news">Sobirai_News</a>'
BRAND_FOOTER_LINE_RE = re.compile(
    r"(?im)^\s*(?:<a\s+href=\"https?://t\.me/sobirai_news\">sobirai_news</a>|sobirai_news)\s*$"
)
URL_RE = re.compile(r"https?://[^\s<>()]+", flags=re.IGNORECASE)
READ_MORE_PATTERNS = (
    re.compile(r"\bчитать\s*далее\b[:\s\-–—]*.*$", flags=re.IGNORECASE | re.DOTALL),
    re.compile(r"\bread\s*more\b[:\s\-–—]*.*$", flags=re.IGNORECASE | re.DOTALL),
    re.compile(r"\bдалее\s+по\s+ссылке\b[:\s\-–—]*.*$", flags=re.IGNORECASE | re.DOTALL),
)
FORBIDDEN_CTA_PATTERNS = (
    re.compile(r"(?im)^\s*читайте\s+подробности.*$"),
    re.compile(r"(?im)^\s*читать\s+подробнее.*$"),
    re.compile(r"(?im)^\s*подробности\s+в\s+источнике.*$"),
    re.compile(r"(?im)^\s*в\s+оригинальной\s+статье.*$"),
)
TELEGRAM_HOSTS = {"t.me", "telegram.me", "telegram.org", "www.telegram.org"}
NON_NEWS_MARKERS = (
    "подпишись",
    "подписывайтесь",
    "скидк",
    "промокод",
    "розыгрыш",
    "реклама",
    "ваканси",
    "ищем",
    "мое мнение",
    "я считаю",
)
NEWS_SIGNAL_MARKERS = (
    "выпуст",
    "запуст",
    "обнов",
    "представ",
    "анонс",
    "релиз",
    "добав",
    "утечк",
    "опубликов",
    "объяв",
    "привлек",
    "получил",
    "исправ",
)
TOPIC_STOPWORDS = {
    "также",
    "теперь",
    "может",
    "могут",
    "через",
    "после",
    "проект",
    "помощью",
    "который",
    "которая",
    "которые",
    "система",
    "инструмент",
    "компания",
    "разработчики",
    "пользователи",
    "ассистенты",
    "искусственный",
    "интеллект",
    "модель",
    "новый",
    "новая",
    "новые",
    "получили",
    "зрение",
    "архитектора",
}


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


def _normalize_for_compare(text: str) -> str:
    s = re.sub(r"<[^>]+>", "", text or "")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def _remove_duplicate_title_in_body(title: str, post_text: str) -> str:
    lines = [ln.strip() for ln in (post_text or "").splitlines()]
    lines = [ln for ln in lines if ln]
    if not lines:
        return ""
    title_norm = _normalize_for_compare(title)
    if title_norm and _normalize_for_compare(lines[0]) == title_norm:
        lines = lines[1:]
    return "\n\n".join(lines).strip()


def _strip_forbidden_cta(text: str) -> str:
    out = (text or "").strip()
    if not out:
        return ""
    for pattern in FORBIDDEN_CTA_PATTERNS:
        out = pattern.sub("", out)
    out = re.sub(r"\n{3,}", "\n\n", out).strip()
    return out


def _strip_linklike_cta_without_links(text: str, has_links: bool) -> str:
    if has_links:
        return (text or "").strip()
    out = (text or "").strip()
    out = re.sub(r"(?im)^\s*читайте.*(?:подробност|источник|репозитор).*$", "", out)
    out = re.sub(r"(?im)^\s*подробност.*(?:в|на).*$", "", out)
    out = re.sub(r"\n{3,}", "\n\n", out).strip()
    return out


def _source_key(post: dict[str, Any]) -> str:
    return str(post.get("source_key") or post.get("channel_username") or "").strip().lstrip("@").lower()


def _is_text_only_source(post: dict[str, Any], settings: Settings) -> bool:
    if not settings.channel_text_only_sources:
        return False
    return _source_key(post) in set(settings.channel_text_only_sources)


def _looks_like_non_news(raw_text: str, title: str, post_text: str) -> bool:
    raw = (raw_text or "").lower()
    text = f"{title}\n{post_text}".lower()
    has_non_news = any(x in raw or x in text for x in NON_NEWS_MARKERS)
    has_news_signal = any(x in raw or x in text for x in NEWS_SIGNAL_MARKERS)
    if has_non_news and not has_news_signal:
        return True
    compact = re.sub(r"\s+", " ", text).strip()
    if len(compact) < 140 and not has_news_signal:
        return True
    return False


def _canonicalize_url(url: str) -> str:
    return (url or "").strip().rstrip(").,;:!?")


def _is_external_non_telegram_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False
    if not host:
        return False
    if host.startswith("www."):
        host = host[4:]
    return host not in TELEGRAM_HOSTS


def _label_for_url(url: str) -> str:
    p = urlparse(url)
    host = (p.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path_parts = [x for x in (p.path or "").split("/") if x]
    if "github.com" in host and len(path_parts) >= 2:
        return f"{path_parts[0]}/{path_parts[1]}"
    if path_parts:
        return path_parts[-1][:40] or host
    return host or "Ссылка"


def _anchor_candidates(url: str) -> list[str]:
    p = urlparse(url)
    host = (p.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    parts = [x for x in (p.path or "").split("/") if x]
    out: list[str] = []
    if host:
        out.append(host)
        root = host.split(".")[0]
        if len(root) >= 3:
            out.append(root)
    if "github.com" in host and len(parts) >= 2:
        out.append(f"{parts[0]}/{parts[1]}")
        out.append(parts[1])
    elif parts:
        out.append(parts[-1])
    seen: set[str] = set()
    uniq: list[str] = []
    for c in out:
        c = c.strip()
        if len(c) < 3:
            continue
        key = c.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(c)
    return uniq


def _extract_external_links(text: str) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for m in URL_RE.findall(text or ""):
        url = _canonicalize_url(m)
        if not url or not _is_external_non_telegram_url(url):
            continue
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append({"url": url, "label": _label_for_url(url), "candidates": _anchor_candidates(url)})
    return out


def _external_non_telegram_urls(text: str) -> set[str]:
    return {str(x.get("url") or "").lower() for x in _extract_external_links(text)}


def _token_overlap_score(a: str, b: str) -> float:
    ta = significant_tokens(a, min_len=4)
    tb = significant_tokens(b, min_len=4)
    if not ta and not tb:
        return 1.0
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / max(1, len(ta | tb))


def _topic_tokens(text: str) -> set[str]:
    tokens = significant_tokens(text, min_len=5)
    return {t for t in tokens if t not in TOPIC_STOPWORDS}


def _topic_overlap_score(a: str, b: str) -> float:
    ta = _topic_tokens(a)
    tb = _topic_tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / max(1, min(len(ta), len(tb)))


def _post_has_media(post: dict[str, Any]) -> bool:
    media_type = str(post.get("media_type") or "")
    return media_type in {"photo", "video"} and bool(post.get("media_file_id") or post.get("media_path"))


def _is_strong_new_details(reason: str) -> bool:
    return reason in {
        "large_length_delta",
        "length_plus_numbers",
        "numbers_plus_tokens",
        "many_new_numbers",
    }


def _topic_memory_duplicate_decision(
    candidate: str,
    reference: str,
    *,
    threshold: float,
    same_source: bool = False,
    current_links: set[str] | None = None,
    reference_links: set[str] | None = None,
    current_has_media: bool = False,
    reference_has_media: bool = False,
) -> tuple[bool, str]:
    current_links = current_links or set()
    reference_links = reference_links or set()
    has_new, new_reason = new_details_signal(candidate, reference)
    has_strong_new = has_new and _is_strong_new_details(new_reason)
    link_overlap = bool(current_links and current_links.intersection(reference_links))
    topic_score = _topic_overlap_score(candidate, reference)
    lexical_score = _token_overlap_score(candidate, reference)
    shingle_score = near_duplicate_score(candidate, reference)

    if link_overlap and not has_strong_new:
        return True, "topic_memory_link_overlap"
    if same_source and reference_has_media and not current_has_media and not has_strong_new:
        if link_overlap or max(topic_score, lexical_score) >= max(0.28, threshold - 0.1):
            return True, "topic_memory_same_source_text_after_media"
    if same_source and max(topic_score, lexical_score, shingle_score) >= max(0.30, threshold - 0.08) and not has_strong_new:
        return True, "topic_memory_same_source"
    if max(topic_score, lexical_score) >= threshold and not has_strong_new:
        return True, f"topic_memory_overlap>={threshold:.2f}"
    return False, f"topic_memory_ok:{new_reason}:score={max(topic_score, lexical_score, shingle_score):.2f}"


def _compose_generated_dedup_text(title: str, post_text: str) -> str:
    t = (title or "").strip()
    b = (post_text or "").strip()
    if t and b:
        return f"{t}\n{b}"
    return t or b


def _inject_inline_links(post_text: str, links: list[dict[str, Any]]) -> tuple[str, set[str]]:
    out = post_text or ""
    used_urls: set[str] = set()
    for item in links:
        url = str(item.get("url") or "")
        candidates = item.get("candidates") or []
        if not url or not candidates:
            continue
        safe_url = html.escape(url, quote=True)
        inserted = False
        for cand in candidates:
            pattern = re.compile(rf"\b({re.escape(cand)})\b", flags=re.IGNORECASE)

            def _repl(match: re.Match[str]) -> str:
                nonlocal inserted
                inserted = True
                text = match.group(1)
                return f'<a href="{safe_url}">{text}</a>'

            out2, n = pattern.subn(_repl, out, count=1)
            if n > 0 and inserted:
                out = out2
                used_urls.add(url)
                break
    return out, used_urls


def _build_links_block(links: list[dict[str, Any]]) -> str:
    if not links:
        return ""
    parts: list[str] = []
    for item in links:
        url = str(item.get("url") or "")
        label = str(item.get("label") or "Ссылка")
        if not url:
            continue
        parts.append(f'<a href="{html.escape(url, quote=True)}">{html.escape(label)}</a>')
    if not parts:
        return ""
    return f"Полезные ссылки: {' · '.join(parts)}"


def _build_channel_message(title: str, post_text: str, links_block: str) -> str:
    t = _ensure_bold_title(title)
    b = _remove_duplicate_title_in_body(title, post_text)
    b = _strip_forbidden_cta(b)
    # Model may append brand footer itself (plain or HTML); keep one footer from code path.
    b = BRAND_FOOTER_LINE_RE.sub("", b).strip()
    if t and b:
        body = f"{t}\n\n{b}"
    elif b:
        body = b
    elif t:
        body = t
    else:
        body = ""
    if links_block:
        body = f"{body}\n\n{links_block}" if body else links_block
    body = f"{body}\n\n{CHANNEL_BRAND_FOOTER_HTML}" if body else CHANNEL_BRAND_FOOTER_HTML
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
    settings: Settings,
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
                    msg = await bot.send_video(
                        chat_id=chat_id,
                        video=file_id,
                        caption=caption,
                        supports_streaming=True,
                    )
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


def _build_group_media_items(
    posts: list[dict[str, Any]],
    caption: str,
) -> list[InputMediaPhoto | InputMediaVideo]:
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
    settings: Settings,
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
            media = _build_group_media_items(
                group_posts,
                caption,
            )
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
            if reason == "exact_fingerprint_match":
                metrics.channel_duplicates_exact += 1
            elif reason == "link_overlap_duplicate":
                metrics.channel_duplicates_link_overlap += 1
            elif reason.startswith("topic_memory_") or reason.startswith("post_llm_topic_memory_"):
                metrics.channel_duplicates_topic_memory += 1
            elif reason.startswith("post_llm_"):
                metrics.channel_duplicates_post_llm += 1
            elif reason.startswith("near_duplicate_jaccard>="):
                metrics.channel_duplicates_near += 1
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

    lookback = int(settings.channel_dedup_lookback_limit)
    recent = await db.list_recent_published_source_records_for_channel_dedup(limit=lookback)
    current_external_links = _external_non_telegram_urls(raw_text)
    current_source_key = str(post.get("source_key") or "").strip().lower()
    current_has_media = _post_has_media(post)
    near_dup_of: int | None = None
    duplicate_reason: str | None = None
    topic_memory_limit = max(10, int(settings.channel_topic_memory_limit))
    for idx, other in enumerate(recent):
        other_id = int(other["sid"])
        other_text = str(other.get("text") or "")
        if other_id == source_post_id:
            continue
        other_external_links = _external_non_telegram_urls(other_text)
        score = near_duplicate_score(raw_text, other_text)
        lexical_score = _token_overlap_score(raw_text, other_text)
        if current_external_links.intersection(other_external_links):
            has_new, new_reason = new_details_signal(raw_text, other_text)
            if not (has_new and _is_strong_new_details(new_reason)):
                near_dup_of = other_id
                duplicate_reason = "link_overlap_duplicate"
                break
        if idx < topic_memory_limit:
            other_source_key = str(other.get("source_key") or "").strip().lower()
            is_topic_dup, topic_reason = _topic_memory_duplicate_decision(
                raw_text,
                other_text,
                threshold=float(settings.channel_topic_memory_threshold),
                same_source=bool(current_source_key and current_source_key == other_source_key),
                current_links=current_external_links,
                reference_links=other_external_links,
                current_has_media=current_has_media,
                reference_has_media=bool(other.get("media_type") and (other.get("media_file_id") or other.get("media_path"))),
            )
            if is_topic_dup:
                near_dup_of = other_id
                duplicate_reason = topic_reason
                break
        if max(score, lexical_score) >= settings.channel_near_dup_jaccard:
            if not has_new_details_vs_reference(raw_text, other_text):
                near_dup_of = other_id
                duplicate_reason = f"near_duplicate_jaccard>={settings.channel_near_dup_jaccard:.2f}"
                break

    if near_dup_of is not None:
        await skip(
            "duplicate",
            duplicate_reason or f"near_duplicate_jaccard>={settings.channel_near_dup_jaccard:.2f}",
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
    llm: RoutedLlmResult = await asyncio.to_thread(
        call_llm_with_fallback,
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
    extracted_links = _extract_external_links(raw_text)
    post_text, used_urls = _inject_inline_links(post_text, extracted_links)
    links_block = _build_links_block([x for x in extracted_links if str(x.get("url") or "") not in used_urls])
    post_text = _strip_linklike_cta_without_links(post_text, has_links=bool(extracted_links))
    if _looks_like_non_news(raw_text, title, post_text):
        await skip("skipped", "post_llm_non_news_gate")
        return
    generated_probe = _compose_generated_dedup_text(title, post_text)
    if generated_probe:
        generated_fp = fingerprint_text(generated_probe)
        recent_generated = await db.list_recent_published_generated_texts_for_channel_dedup(limit=lookback)
        for other_id, other_generated in recent_generated:
            if other_id == source_post_id:
                continue
            if fingerprint_text(other_generated) == generated_fp:
                await skip(
                    "duplicate",
                    "post_llm_exact_duplicate",
                    duplicate_of_source_post_id=other_id,
                )
                return
            score = near_duplicate_score(generated_probe, other_generated)
            lexical_score = _token_overlap_score(generated_probe, other_generated)
            if other_generated:
                is_topic_dup, topic_reason = _topic_memory_duplicate_decision(
                    generated_probe,
                    other_generated,
                    threshold=float(settings.channel_topic_memory_threshold),
                )
                if is_topic_dup:
                    await skip(
                        "duplicate",
                        f"post_llm_{topic_reason}",
                        duplicate_of_source_post_id=other_id,
                    )
                    return
            if max(score, lexical_score) >= settings.channel_near_dup_jaccard and not has_new_details_vs_reference(
                generated_probe, other_generated
            ):
                await skip(
                    "duplicate",
                    "post_llm_near_duplicate",
                    duplicate_of_source_post_id=other_id,
                )
                return
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

    outgoing = _build_channel_message(title, post_text, links_block)
    if not outgoing.strip():
        await fail("empty_outgoing_after_build")
        return

    msg_id: int
    publish_reason: str | None = None
    force_text_only = False
    media_group_id = ""
    try:
        media_group_id = str(post.get("media_group_id") or "")
        media_type = str(post.get("media_type") or "")
        force_text_only = _is_text_only_source(post, settings)
        has_single_media = media_type in {"photo", "video"} and (
            post.get("media_file_id") or post.get("media_path")
        )
        if media_group_id and not force_text_only:
            group_posts = await db.list_source_posts_by_media_group(media_group_id)
            msg_id = await _send_media_group_with_retry(
                bot,
                metrics,
                settings,
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
        elif has_single_media and not force_text_only:
            msg_id = await _send_single_media_with_retry(
                bot,
                metrics,
                settings,
                channel_chat_id,
                post,
                _as_caption(outgoing),
            )
            publish_reason = "single_media_sent"
        else:
            msg_id = await _send_channel_message_with_retry(bot, metrics, channel_chat_id, outgoing)
            publish_reason = "text_sent" if not force_text_only else "text_sent_watermark_policy"
    except Exception as exc:
        await fail(f"telegram_publish:{exc!s}"[:500])
        return

    if force_text_only and media_group_id:
        group_posts = await db.list_source_posts_by_media_group(media_group_id)
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
                error="text_sent_watermark_policy_member",
            )

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
