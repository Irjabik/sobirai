from __future__ import annotations

import asyncio
import html
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import urlparse

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramNetworkError, TelegramRetryAfter
from aiogram.types import (
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
)

from .config import Settings
from .db import Database
from .delivery import _video_send_options
from .llm_client import RoutedLlmResult, call_llm_with_fallback
from .media_quality import is_low_info_photo
from .media_watermark import add_watermark_photo, watermarked_photo_path
from .metrics import RuntimeMetrics
from .video_transcode import (
    probe_video_dims,
    transcode_video_for_telegram,
    transcoded_video_path,
)
from .prompts_channel import (
    CHANNEL_REWRITE_PROMPT_VERSION,
    CHANNEL_REWRITE_SYSTEM_PROMPT_V1,
    build_channel_rewrite_user_message,
    build_exemplar_block,
)
from .text_norm import (
    extract_ai_entities,
    fingerprint_text,
    new_details_signal,
    near_duplicate_score,
    significant_tokens,
)

logger = logging.getLogger(__name__)

TELEGRAM_MAX_MESSAGE_LEN = 4096
TELEGRAM_MAX_CAPTION_LEN = 1024
CHANNEL_BRAND_FOOTER_HTML = '<a href="https://t.me/AutomyAI"><b>Automy AI | Новости ИИ</b></a>'
URL_RE = re.compile(
    r"(https?://[^\s<>\"'`]+|www\.[^\s<>\"'`]+|t\.me/[^\s<>\"'`]+)",
    flags=re.IGNORECASE,
)
LINKLIKE_CTA_LINE_RE = re.compile(
    r"(подробност[ьи]\s+по\s+ссылке|подробност[ьи].*ссылк|ссылка\s+ниже|перейд[иите]+\s+по\s+ссылке)",
    flags=re.IGNORECASE,
)
# Чистим LLM-сгенерированные дубли бренд-строки. Поддерживаем и новое имя Automy AI,
# и старое Sobirai_News (на случай если оно осталось в эталонах/промптах LLM).
BRAND_FOOTER_LINE_RE = re.compile(
    r'(?im)^\s*(?:'
    r'<a\s+href="https?://t\.me/[Aa]utomy[Aa][Ii]"[^>]*>(?:<b>)?Automy AI \| Новости ИИ(?:</b>)?</a>'
    r'|<a\s+href="https?://t\.me/sobirai_news"[^>]*>(?:<b>)?Sobirai_News(?:</b>)?</a>'
    r'|Automy AI \| Новости ИИ'
    r'|Automy AI'
    r'|Sobirai_News'
    r'|AI:\s*\w+'
    r')\s*$'
)
URL_TRAIL_PUNCT = ".,);:!?]>"
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
# Жёсткие маркеры рекламы (российский закон): если найдены — публикуем НИКОГДА,
# независимо от наличия news_signal. Реальные кейсы: посты с «erid: ...» от Яндекса и т.п.
HARD_AD_MARKERS = (
    "erid:",
    "erid ",
    "реклама. ооо",
    "реклама ооо",
    "реклама от",
    "#промо",
    "#реклама",
    "rkn.gov.ru",
    "kreativ.rt.ru",
)
# Жёсткие маркеры обзоров/мнений: пользователь явно не хочет такие в новостной канал.
HARD_REVIEW_MARKERS = (
    "мы попробовали",
    "мы протестировали",
    "наш опыт",
    "наше мнение",
    "мы пришли к выводу",
    "в итоге мы",
    "поделюсь опытом",
    "наш отзыв",
    "отзыв о",
    "мы убедились",
    "по нашему опыту",
    "делимся мнением",
    "делюсь опытом",
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


def _source_key(post: dict[str, Any]) -> str:
    return str(post.get("source_key") or post.get("channel_username") or "").strip().lstrip("@").lower()


def _is_text_only_source(post: dict[str, Any], settings: Settings) -> bool:
    if not settings.channel_text_only_sources:
        return False
    return _source_key(post) in set(settings.channel_text_only_sources)


def _has_hard_ad_marker(raw_lower: str) -> bool:
    return any(m in raw_lower for m in HARD_AD_MARKERS)


def _has_hard_review_marker(raw_lower: str) -> bool:
    return any(m in raw_lower for m in HARD_REVIEW_MARKERS)


def _looks_like_non_news_source(raw_text: str) -> tuple[bool, str]:
    """Pre-LLM gate по сырому тексту источника. Возвращает (skip, reason)."""
    raw = (raw_text or "").lower()
    if _has_hard_ad_marker(raw):
        return True, "ad_disclosure_marker"
    if _has_hard_review_marker(raw):
        return True, "review_marker"
    has_non_news = any(x in raw for x in NON_NEWS_MARKERS)
    has_news_signal = any(x in raw for x in NEWS_SIGNAL_MARKERS)
    if has_non_news and not has_news_signal:
        return True, "soft_non_news_no_signal"
    return False, ""


def _looks_like_non_news(raw_text: str, title: str, post_text: str) -> bool:
    raw = (raw_text or "").lower()
    if _has_hard_ad_marker(raw):
        return True
    if _has_hard_review_marker(raw):
        return True
    text = f"{title}\n{post_text}".lower()
    if _has_hard_ad_marker(text) or _has_hard_review_marker(text):
        return True
    has_non_news = any(x in raw or x in text for x in NON_NEWS_MARKERS)
    has_news_signal = any(x in raw or x in text for x in NEWS_SIGNAL_MARKERS)
    if has_non_news and not has_news_signal:
        return True
    compact = re.sub(r"\s+", " ", text).strip()
    if len(compact) < 140 and not has_news_signal:
        return True
    return False


# Указательные эмодзи в финале строки без URL/HTML — указывают в пустоту
POINTER_CHARS_RE = re.compile(
    "[\U0001F447\U0001F446\U0001F449\U0001F448☝]️?"
)
# <a href="X">Y</a> -> X (raw URL); pipeline пересоберёт ссылку
HTML_A_RE = re.compile(r"<a\s+href=\"([^\"]+)\"[^>]*>([^<]*)</a>", flags=re.IGNORECASE)
HTML_OTHER_TAG_RE = re.compile(r"</?(?:b|i|s|u|code|pre|em|strong|tg-spoiler|span|br|p|div)[^>]*>", flags=re.IGNORECASE)
USELESS_LINKS_HEADER_RE = re.compile(
    r"(?im)^\s*(?:полезные\s+ссылки|источник[аи]?|оригинал)\s*:.*$"
)
# CTA-фразы вида «читайте оригинал», «читайте полностью», «подробнее по ссылке»,
# «читайте оригинальное расследование» — пустая болтовня в новостном посте.
USELESS_CTA_LINE_RE = re.compile(
    r"(?im)^\s*"
    r"(?:читайте|читать|подробнее|подробности)\s+"
    r"(?:оригинал\w*|подробнее|полностью|по\s+ссылке|на\s+\w+|тут|здесь|ниже|выше)"
    r".*$"
)


def _strip_llm_html(text: str) -> str:
    """Убирает HTML-теги из вывода LLM. <a href> схлопывается в plain URL — pipeline их потом красиво обернёт."""
    if not text:
        return text
    out = HTML_A_RE.sub(lambda m: m.group(1), text)
    out = HTML_OTHER_TAG_RE.sub("", out)
    return out


def _strip_useless_link_headers(text: str) -> str:
    """Чистит мусорные «ссылочные» строки от LLM:
    - «Полезные ссылки: TIME» / «Источник: X» / «Оригинал:» без URL — пустые заголовки.
    - «Читайте оригинал», «Читать полностью», «Подробнее по ссылке» — пустая CTA-болтовня.

    Pipeline сам соберёт блок ссылок из оставшихся сырых URL ниже по конвейеру.
    """
    if not text:
        return text
    cleaned: list[str] = []
    for line in text.splitlines():
        if USELESS_LINKS_HEADER_RE.match(line) and not URL_RE.search(line) and "<a " not in line.lower():
            continue
        if USELESS_CTA_LINE_RE.match(line):
            continue
        cleaned.append(line)
    out = "\n".join(cleaned)
    return re.sub(r"\n{3,}", "\n\n", out).strip()


def _strip_dangling_pointer_emojis(text: str) -> str:
    """Указательные эмодзи (👇👆👉👈☝️) в конце строк без URL/HTML-ссылки указывают в пустоту."""
    if not text:
        return text
    out_lines: list[str] = []
    for line in text.splitlines():
        if not POINTER_CHARS_RE.search(line):
            out_lines.append(line)
            continue
        if URL_RE.search(line) or "<a " in line.lower():
            out_lines.append(line)
            continue
        cleaned = POINTER_CHARS_RE.sub("", line)
        cleaned = re.sub(r"[\s ]+$", "", cleaned)
        cleaned = re.sub(r"[\s ]*[:\-—–]\s*$", "", cleaned)
        out_lines.append(cleaned.rstrip())
    return "\n".join(out_lines)


def _safe_retry_after(exc: TelegramRetryAfter) -> float:
    value = getattr(exc, "retry_after", 1)
    try:
        return max(1.0, float(value))
    except Exception:
        return 1.0


def _provider_label(provider: str) -> str:
    if provider == "openrouter":
        return "OpenRouter"
    return provider.capitalize() or "Unknown"


def _ensure_bold_title(title: str) -> str:
    t = (title or "").strip()
    if not t:
        return ""
    if t.startswith("<b>") and t.endswith("</b>"):
        return t
    return f"<b>{t}</b>"


def _plain_text_for_compare(text: str) -> str:
    t = re.sub(r"<[^>]+>", " ", text or "")
    t = re.sub(r"[^\w\sа-яё]+", " ", t, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", t).strip().lower()


def _strip_repeated_title_from_body(title: str, body: str) -> str:
    t = _plain_text_for_compare(title)
    lines = (body or "").splitlines()
    while lines and not lines[0].strip():
        lines.pop(0)
    if lines and t and _plain_text_for_compare(lines[0]) == t:
        lines.pop(0)
        while lines and not lines[0].strip():
            lines.pop(0)
    return "\n".join(lines).strip()


def _build_channel_message(title: str, post_text: str, hashtags: list[Any], provider: str) -> str:
    t = _ensure_bold_title(title)
    b = BRAND_FOOTER_LINE_RE.sub("", post_text or "")
    b = _strip_repeated_title_from_body(title, b)
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
    body = BRAND_FOOTER_LINE_RE.sub("", body)
    body = re.sub(r"\n{3,}", "\n\n", body).strip()
    body = f"{body}\n\n{CHANNEL_BRAND_FOOTER_HTML}" if body else CHANNEL_BRAND_FOOTER_HTML
    if len(body) > TELEGRAM_MAX_MESSAGE_LEN:
        body = body[: TELEGRAM_MAX_MESSAGE_LEN - 30] + "\n…(текст обрезан)"
    return body


def _as_caption(text: str) -> str:
    if len(text) <= TELEGRAM_MAX_CAPTION_LEN:
        return text
    return text[: TELEGRAM_MAX_CAPTION_LEN - 18] + "\n…(подпись обрезана)"


def _strip_linklike_cta_without_links(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return raw
    if URL_RE.search(raw):
        return raw
    cleaned_lines: list[str] = []
    for line in raw.splitlines():
        if LINKLIKE_CTA_LINE_RE.search(line):
            continue
        cleaned_lines.append(line)
    cleaned = "\n".join(cleaned_lines)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned


def _normalize_url_candidate(raw: str) -> str | None:
    s = (raw or "").strip()
    if not s:
        return None
    s = s.rstrip(URL_TRAIL_PUNCT)
    if s.lower().startswith("www."):
        s = f"https://{s}"
    elif s.lower().startswith("t.me/"):
        s = f"https://{s}"
    if not s.lower().startswith(("http://", "https://")):
        return None
    try:
        p = urlparse(s)
    except Exception:
        return None
    if not p.netloc:
        return None
    return s


def _label_for_url(url: str) -> str:
    p = urlparse(url)
    host = (p.netloc or "").lower()
    host = host[4:] if host.startswith("www.") else host
    if host in {"x.com", "twitter.com"}:
        return "X/Twitter"
    if host in {"t.me", "telegram.me"}:
        return "Telegram"
    if host:
        return host
    return "Ссылка"


def _is_telegram_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False
    if host.startswith("www."):
        host = host[4:]
    return host in {"t.me", "telegram.me", "telegram.org"}


def _extract_urls(text: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for m in URL_RE.findall(text or ""):
        norm = _normalize_url_candidate(m)
        if not norm:
            continue
        key = norm.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(norm)
    return out


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


def _external_non_telegram_urls(text: str) -> set[str]:
    return {u.lower() for u in _extract_urls(text) if not _is_telegram_url(u)}


def _post_has_media(post: dict[str, Any]) -> bool:
    media_type = str(post.get("media_type") or "")
    return media_type in {"photo", "video"} and bool(post.get("media_file_id") or post.get("media_path"))


async def _apply_photo_watermark(post: dict[str, Any], settings: Settings) -> dict[str, Any]:
    """Если включено и есть локальный media_path — подменяет на watermarked-версию.

    Если media_path нет (только file_id), watermark не применяется: качать файл с TG ради
    обработки тяжелее, чем профит от знака на одиночном переслaнном изображении.
    """
    if not settings.enable_channel_watermark:
        return post
    if str(post.get("media_type") or "") != "photo":
        return post
    media_path = post.get("media_path")
    if not media_path:
        return post
    src = Path(str(media_path))
    if not src.is_file():
        return post
    wm_path = watermarked_photo_path(src)
    if not wm_path.is_file() or wm_path.stat().st_size == 0:
        ok = await asyncio.to_thread(add_watermark_photo, src, wm_path)
        if not ok:
            return post
    if not wm_path.is_file() or wm_path.stat().st_size == 0:
        return post
    new_post = dict(post)
    new_post["media_path"] = str(wm_path)
    new_post["media_file_id"] = None
    return new_post


async def _apply_video_transcode(post: dict[str, Any], settings: Settings) -> dict[str, Any]:
    """Если включено и есть локальный media_path — перекодирует в telegram-friendly mp4.

    После транскодинга обновляет width/height/duration (через ffprobe), чтобы Telegram
    UI не растягивал плеер по старым размерам исходника.
    """
    if not settings.enable_channel_video_transcode:
        return post
    if str(post.get("media_type") or "") != "video":
        return post
    media_path = post.get("media_path")
    if not media_path:
        return post
    src = Path(str(media_path))
    if not src.is_file():
        return post
    out_path = transcoded_video_path(src)
    if not out_path.is_file() or out_path.stat().st_size == 0:
        ok = await asyncio.to_thread(
            transcode_video_for_telegram,
            src,
            out_path,
            max_input_size_mb=int(settings.channel_video_max_input_mb),
        )
        if not ok:
            return post
    if not out_path.is_file() or out_path.stat().st_size == 0:
        return post
    new_post = dict(post)
    new_post["media_path"] = str(out_path)
    new_post["media_file_id"] = None
    dims = await asyncio.to_thread(probe_video_dims, out_path)
    if dims is not None:
        d, w, h = dims
        if d is not None:
            new_post["media_duration"] = d
        if w is not None:
            new_post["media_width"] = w
        if h is not None:
            new_post["media_height"] = h
    return new_post


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


def _canonicalize_links_presentation(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return raw
    urls = [u for u in _extract_urls(raw) if not _is_telegram_url(u)]
    # Удаляем "Полезные ссылки" и сырой URL-список, потом собираем аккуратный блок заново.
    # Любая строка, начинающаяся с «Полезные ссылки:» — снос, мы построим свежий блок ниже.
    no_header = re.sub(r"(?im)^\s*полезные\s+ссылки\s*:.*$", "", raw)
    no_raw_lines = re.sub(r"(?im)^\s*(?:https?://\S+|www\.\S+)\s*$", "", no_header)
    body = re.sub(r"\n{3,}", "\n\n", no_raw_lines).strip()
    if not urls:
        return body
    links = [f'<a href="{u}">{_label_for_url(u)}</a>' for u in urls]
    links_block = "Полезные ссылки: " + " · ".join(links)
    return f"{body}\n\n{links_block}" if body else links_block


def _beautify_links_block(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return raw

    # Не трогаем текст, если там уже есть HTML-ссылки от модели.
    if "<a " in raw.lower():
        return raw

    urls_seen: set[str] = set()

    def repl(match: re.Match[str]) -> str:
        token = match.group(0)
        norm = _normalize_url_candidate(token)
        if not norm:
            return token
        key = norm.lower()
        if key in urls_seen:
            return ""
        urls_seen.add(key)
        label = _label_for_url(norm)
        return f'<a href="{norm}">{label}</a>'

    out = URL_RE.sub(repl, raw)
    out = re.sub(r"(?:[ \t]*[·•][ \t]*){2,}", " · ", out)
    # \s включает \n — нельзя его жадно сжимать, иначе абзацы становятся одной строкой.
    out = re.sub(r"[ \t]{2,}", " ", out)

    # Если модель написала "Полезные ссылки", но валидных URL нет — убираем такую строку.
    cleaned_lines: list[str] = []
    for line in out.splitlines():
        if "полезные ссылки" in line.lower() and "<a href=" not in line.lower():
            continue
        cleaned_lines.append(line)
    out = "\n".join(cleaned_lines)
    out = re.sub(r"\n{3,}", "\n\n", out).strip()
    return _canonicalize_links_presentation(out)


def _compose_generated_dedup_text(title: str, post_text: str) -> str:
    t = (title or "").strip()
    b = (post_text or "").strip()
    if t and b:
        return f"{t}\n{b}"
    return t or b


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
            if media_type == "photo":
                if file_id:
                    msg = await bot.send_photo(chat_id=chat_id, photo=file_id, caption=caption)
                elif media_path:
                    msg = await bot.send_photo(chat_id=chat_id, photo=FSInputFile(media_path), caption=caption)
                else:
                    raise RuntimeError("single_photo_missing_file")
            elif media_type == "video":
                opts = _video_send_options(post)
                if file_id:
                    msg = await bot.send_video(
                        chat_id=chat_id,
                        video=file_id,
                        caption=caption,
                        **opts,
                    )
                elif media_path:
                    msg = await bot.send_video(
                        chat_id=chat_id,
                        video=FSInputFile(media_path),
                        caption=caption,
                        **opts,
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
            opts = _video_send_options(p)
            items.append(InputMediaVideo(media=media_obj, caption=cap, **opts))
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


async def _publish_generated_post(
    *,
    db: Database,
    bot: Bot,
    metrics: RuntimeMetrics,
    settings: Settings,
    source_post_id: int,
) -> tuple[bool, str]:
    """Публикует сохранённый сгенерированный пост в канал.

    Загружает title/post_text/hashtags/summary из generated_channel_posts, проверяет дневной лимит,
    собирает outgoing и отправляет в канал (одиночное медиа / альбом / текст).
    Возвращает (ok, reason_or_msg_id).
    """
    post = await db.get_post(source_post_id)
    if not post:
        await db.update_generated_channel_post(
            source_post_id, status="failed", error="source_post_missing"
        )
        return False, "source_post_missing"
    gen = await db.get_generated_channel_post_by_source_id(source_post_id)
    if not gen:
        return False, "generated_post_missing"

    # Если админ заменил/добавил фото — подменяем медиа источника на admin-файл.
    admin_media = str(gen.get("admin_media_path") or "").strip()
    if admin_media and Path(admin_media).is_file():
        post = dict(post)  # mutable copy
        post["media_type"] = "photo"
        post["media_path"] = admin_media
        post["media_file_id"] = None
        post["media_group_id"] = None
        logger.info("Using admin-provided media for source_post_id=%s: %s", source_post_id, admin_media)

    title = str(gen.get("title") or "").strip()
    post_text = str(gen.get("post_text") or "").strip()
    summary = str(gen.get("summary") or "").strip()
    llm_provider = str(gen.get("llm_provider") or "")
    llm_model = str(gen.get("llm_model") or "")
    prompt_version = str(gen.get("prompt_version") or CHANNEL_REWRITE_PROMPT_VERSION)
    # Хэштеги отключены — независимо от того что было в БД.
    hashtags: list[Any] = []

    channel_chat_id = int(settings.channel_chat_id or 0)
    day_utc = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    daily = await db.get_channel_daily_publish_count(day_utc)
    if daily >= settings.channel_max_posts_per_day:
        await db.update_generated_channel_post(
            source_post_id,
            status="skipped_by_limit",
            error="daily_limit_publish",
        )
        metrics.channel_skipped_limit += 1
        return False, "daily_limit"

    outgoing = _build_channel_message(title, post_text, hashtags, llm_provider)
    if not outgoing.strip():
        await db.update_generated_channel_post(
            source_post_id, status="failed", error="empty_outgoing_after_build"
        )
        metrics.channel_failed += 1
        return False, "empty_outgoing"

    msg_id: int
    publish_reason: str | None = None
    try:
        media_group_id = str(post.get("media_group_id") or "")
        media_type = str(post.get("media_type") or "")
        force_text_only = _is_text_only_source(post, settings)
        has_single_media = media_type in {"photo", "video"} and (
            post.get("media_file_id") or post.get("media_path")
        )
        # Если у поста одиночное фото и оно «пустое» (белая обложка-плейсхолдер
        # источника) — публикуем как text-only, без бесполезной картинки в углу.
        if (
            has_single_media
            and media_type == "photo"
            and not media_group_id
            and not force_text_only
        ):
            local_photo = post.get("media_path")
            # PIL-операции внутри is_low_info_photo блокирующие — вынесем в thread,
            # чтобы не задерживать event loop при пачке постов.
            if local_photo and await asyncio.to_thread(is_low_info_photo, local_photo):
                logger.info(
                    "Demote low-info photo to text-only source_post_id=%s path=%s",
                    source_post_id, local_photo,
                )
                has_single_media = False
        if media_group_id and not force_text_only:
            group_posts = await db.list_source_posts_by_media_group(media_group_id)
            processed_group: list[dict[str, Any]] = []
            for gp in group_posts:
                gp = await _apply_photo_watermark(gp, settings)
                gp = await _apply_video_transcode(gp, settings)
                processed_group.append(gp)
            group_posts = processed_group
            msg_id = await _send_media_group_with_retry(
                bot, metrics, settings, channel_chat_id, group_posts, outgoing,
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
                    llm_provider=llm_provider,
                    llm_model=llm_model,
                    prompt_version=prompt_version,
                    title=title,
                    post_text=post_text,
                    summary=summary,
                    channel_message_id=msg_id,
                    published_at=datetime.now(tz=timezone.utc).isoformat(),
                    error="media_group_sent_member",
                )
        elif has_single_media and not force_text_only:
            send_post = await _apply_photo_watermark(post, settings)
            send_post = await _apply_video_transcode(send_post, settings)
            msg_id = await _send_single_media_with_retry(
                bot, metrics, channel_chat_id, send_post, _as_caption(outgoing),
            )
            publish_reason = "single_media_sent"
        else:
            msg_id = await _send_channel_message_with_retry(
                bot, metrics, channel_chat_id, outgoing
            )
            publish_reason = "text_sent"
    except Exception as exc:
        # Если у источника есть медиа, не публикуем «голый текст» при сбое.
        await db.update_generated_channel_post(
            source_post_id,
            status="failed",
            error=f"telegram_publish:{exc!s}"[:500],
        )
        metrics.channel_failed += 1
        return False, str(exc)

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
    # Сразу же предлагаем админу оценить пост — оценка попадёт в эталоны для следующих публикаций.
    await send_feedback_prompt_to_admin(
        bot=bot, settings=settings, db=db, source_post_id=source_post_id, msg_id=msg_id,
    )
    return True, str(msg_id)


def review_main_keyboard(
    source_post_id: int,
    current_rating: int = 0,
    *,
    has_generated_image: bool = False,
) -> InlineKeyboardMarkup:
    """Главная клавиатура превью: действия + оценка ДО публикации.

    current_rating — если уже стоит, подсвечивает выбранную звезду галочкой.
    has_generated_image — если True, рядом с «🎨 Сгенерировать» появляется
        «🚫 Убрать фото» (постить text-only).
    """
    star_row = []
    for n in range(1, 6):
        prefix = "✓" if n == current_rating else ""
        star_row.append(
            InlineKeyboardButton(text=f"{prefix}⭐{n}", callback_data=f"rrate:{n}:{source_post_id}")
        )
    image_label = "🎨 Перегенерировать фото" if has_generated_image else "🎨 Сгенерировать фото"
    image_row = [InlineKeyboardButton(text=image_label, callback_data=f"rev:imggen:{source_post_id}")]
    if has_generated_image:
        image_row.append(
            InlineKeyboardButton(text="🚫 Убрать фото", callback_data=f"rev:imgrm:{source_post_id}")
        )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Опубликовать", callback_data=f"rev:pub:{source_post_id}")],
            [InlineKeyboardButton(text="✏️ Скорректировать", callback_data=f"rev:edit:{source_post_id}")],
            image_row,
            star_row,
            [
                InlineKeyboardButton(text="💬 Комментарий", callback_data=f"rate:comment:{source_post_id}"),
                InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"rev:skip:{source_post_id}"),
            ],
        ]
    )


def review_edit_keyboard(source_post_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📝 Заголовок", callback_data=f"rev:edit_title:{source_post_id}")],
            [InlineKeyboardButton(text="✏️ Тело поста", callback_data=f"rev:edit_body:{source_post_id}")],
            [InlineKeyboardButton(text="📷 Заменить фото", callback_data=f"rev:edit_media:{source_post_id}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"rev:back:{source_post_id}")],
        ]
    )


def feedback_rating_keyboard(source_post_id: int, current_rating: int = 0) -> InlineKeyboardMarkup:
    """Клавиатура оценки поста после публикации. Подсвечивает текущую оценку галочкой."""
    star_row = []
    for n in range(1, 6):
        prefix = "✅ " if n == current_rating else ""
        star_row.append(
            InlineKeyboardButton(text=f"{prefix}⭐{n}", callback_data=f"rate:{n}:{source_post_id}")
        )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            star_row,
            [InlineKeyboardButton(text="💬 Комментарий", callback_data=f"rate:comment:{source_post_id}")],
        ]
    )


def _admin_chat_ids(settings: Settings) -> tuple[int, ...]:
    """Возвращает все chat_id админов: новый список ADMIN_CHAT_IDS либо одиночный ADMIN_CHAT_ID."""
    if settings.admin_chat_ids:
        return settings.admin_chat_ids
    if settings.admin_chat_id:
        return (int(settings.admin_chat_id),)
    return ()


def _is_admin(settings: Settings, user_id: int | None) -> bool:
    if user_id is None:
        return False
    return int(user_id) in _admin_chat_ids(settings)


async def send_feedback_prompt_to_admin(
    *,
    bot: Bot,
    settings: Settings,
    db: Database,
    source_post_id: int,
    msg_id: int | str,
) -> None:
    """Шлёт админу follow-up с кнопками оценки 1-5 после успешной публикации поста.

    Если оценка уже стоит (поставил в превью до публикации) — follow-up не шлём, не спамим.
    """
    if not settings.enable_feedback_learning:
        return
    admin_ids = _admin_chat_ids(settings)
    if not admin_ids:
        return
    existing = await db.get_post_feedback(source_post_id)
    current = int(existing["rating"]) if existing else 0
    if current > 0:
        # уже оценили в превью — повторное сообщение не нужно
        return
    text = (
        f"📨 Опубликован пост id={source_post_id} (msg={msg_id})\n\n"
        f"Оцени — это формирует эталоны для следующих публикаций:"
    )
    kb = feedback_rating_keyboard(source_post_id, current_rating=current)
    for admin_id in admin_ids:
        try:
            await bot.send_message(chat_id=admin_id, text=text, reply_markup=kb)
        except TelegramAPIError as exc:
            logger.warning("Feedback prompt send failed admin=%s err=%s", admin_id, exc)


async def _notify_admin_raw_source_post(
    *,
    db: Database,
    bot: Bot,
    settings: Settings,
    source_post_id: int,
) -> None:
    """Шлёт админу оригинал поста (без обработки LLM) — чтобы было видно ВСЕ собранные посты,
    даже те что потом будут отфильтрованы pre-LLM gates.

    После LLM, если пост проходит фильтры, отдельно прилетает превью с переписанной версией и кнопками.
    """
    if not settings.enable_channel_review:
        return
    admin_ids = _admin_chat_ids(settings)
    if not admin_ids:
        return

    post = await db.get_post(source_post_id)
    if not post:
        return

    source_text = str(post.get("text") or "").strip()
    source_username = str(post.get("channel_username") or "")
    source_link = str(post.get("source_link") or "")

    header = f"📥 <b>Новый пост из {html.escape(source_username)}</b>"
    if source_link:
        header += f' (<a href="{html.escape(source_link, quote=True)}">источник</a>)'
    header += f"\n<i>id={source_post_id} — обрабатываю…</i>"

    if source_text:
        body = html.escape(source_text)
        if len(body) > 3500:
            body = body[:3500].rstrip() + "…"
    else:
        body = "<i>(без текста — только медиа)</i>"

    full_text = f"{header}\n\n{body}"
    if len(full_text) > 4090:
        full_text = full_text[:4080] + "…"

    media_type = str(post.get("media_type") or "")
    media_group_id = str(post.get("media_group_id") or "")
    has_single_media = media_type in {"photo", "video"} and bool(
        post.get("media_path") or post.get("media_file_id")
    )

    if media_group_id:
        group_posts = await db.list_source_posts_by_media_group(media_group_id)
    else:
        group_posts = None

    for admin_id in admin_ids:
        try:
            if media_group_id and group_posts:
                media_items = _build_group_media_items(group_posts, "")
                if media_items:
                    await bot.send_media_group(chat_id=admin_id, media=media_items)
                await bot.send_message(chat_id=admin_id, text=full_text, disable_web_page_preview=True)
            elif has_single_media:
                file_id = post.get("media_file_id")
                media_path = post.get("media_path")
                if media_type == "photo":
                    if file_id:
                        await bot.send_photo(chat_id=admin_id, photo=file_id)
                    elif media_path:
                        await bot.send_photo(chat_id=admin_id, photo=FSInputFile(media_path))
                elif media_type == "video":
                    if file_id:
                        await bot.send_video(chat_id=admin_id, video=file_id)
                    elif media_path:
                        await bot.send_video(chat_id=admin_id, video=FSInputFile(media_path))
                await bot.send_message(chat_id=admin_id, text=full_text, disable_web_page_preview=True)
            else:
                await bot.send_message(chat_id=admin_id, text=full_text, disable_web_page_preview=True)
        except TelegramAPIError as exc:
            logger.warning("Raw source post notification failed admin=%s err=%s", admin_id, exc)
        except Exception:
            logger.exception(
                "Raw source post unexpected failure admin=%s source_post_id=%s",
                admin_id, source_post_id,
            )


async def _send_review_preview_to_admin(
    *,
    db: Database,
    bot: Bot,
    settings: Settings,
    source_post_id: int,
) -> bool:
    """Отправляет админу превью поста с медиа и inline-кнопками.

    Загружает данные из БД (поэтому работает и для свежих постов, и для повторного показа после редактирования).
    Применяет watermark/transcode сразу (cache переиспользуется при публикации).
    """
    # Контрольная точка на самом входе — если функция упадёт где-то ниже,
    # хотя бы будет видно что её вообще вызывали.
    async def _checkpoint(name: str) -> None:
        try:
            stamp_local = datetime.now(tz=timezone.utc).strftime("%H:%M:%S")
            await db.set_bot_secret(
                "last_preview_trace",
                f"checkpoint={name} | post_id={source_post_id} | {stamp_local} UTC",
            )
        except Exception:
            pass

    await _checkpoint("entered")
    admin_ids = _admin_chat_ids(settings)
    if not admin_ids:
        await _checkpoint("no_admins")
        return False
    await _checkpoint(f"admins_loaded n={len(admin_ids)}")

    post = await db.get_post(source_post_id)
    gen = await db.get_generated_channel_post_by_source_id(source_post_id)
    if not post or not gen:
        await _checkpoint(f"post_or_gen_missing post={bool(post)} gen={bool(gen)}")
        logger.warning("review preview: missing post or gen for source_post_id=%s", source_post_id)
        return False
    await _checkpoint("post_and_gen_loaded")

    # Если админ уже добавлял своё фото к этому посту — показываем его в превью вместо источникового.
    admin_media = str(gen.get("admin_media_path") or "").strip()
    admin_media_is_file = bool(admin_media) and Path(admin_media).is_file() if admin_media else False
    admin_media_size = -1
    if admin_media_is_file:
        try:
            admin_media_size = int(Path(admin_media).stat().st_size)
        except OSError:
            pass
    if admin_media and admin_media_is_file:
        post = dict(post)
        post["media_type"] = "photo"
        post["media_path"] = admin_media
        post["media_file_id"] = None
        post["media_group_id"] = None
        await _checkpoint(f"post_overridden_with_admin_media size={admin_media_size}")
    else:
        await _checkpoint(
            f"no_override admin_media='{admin_media[:80]}' is_file={admin_media_is_file}"
        )

    # Оригинал отправляется отдельно через _notify_admin_raw_source_post в начале
    # _process_one_source_post, ДО фильтров и LLM. Здесь только переписанная версия.

    title = str(gen.get("title") or "").strip()
    post_text = str(gen.get("post_text") or "").strip()
    summary = str(gen.get("summary") or "").strip()
    # Хэштеги выключены глобально.
    hashtags: list[Any] = []

    preview_outgoing = _build_channel_message(title, post_text, hashtags, "preview")

    header = f"<b>📝 Пост на ревью</b> id:{source_post_id}\n"
    summary_block = f"<i>{summary}</i>\n\n" if summary else "\n"
    full_text = f"{header}{summary_block}— — —\n\n{preview_outgoing}"
    if len(full_text) > 4000:
        full_text = full_text[:3950] + "…"

    # Подгружаем текущую оценку (если ставилась раньше) чтобы подсветить её на клавиатуре.
    existing_feedback = await db.get_post_feedback(source_post_id)
    current_rating = int(existing_feedback["rating"]) if existing_feedback else 0
    has_generated_image = bool(admin_media and Path(admin_media).is_file())
    kb = review_main_keyboard(
        source_post_id,
        current_rating=current_rating,
        has_generated_image=has_generated_image,
    )
    media_type = str(post.get("media_type") or "")
    media_group_id = str(post.get("media_group_id") or "")
    post_media_path = str(post.get("media_path") or "")
    post_media_file_id = str(post.get("media_file_id") or "")
    has_single_media = media_type in {"photo", "video"} and bool(post.get("media_path") or post.get("media_file_id"))

    # watermark/transcode применяются один раз, до рассылки админам.
    if media_group_id:
        group_posts = await db.list_source_posts_by_media_group(media_group_id)
        processed_group: list[dict[str, Any]] = []
        for gp in group_posts:
            gp = await _apply_photo_watermark(gp, settings)
            gp = await _apply_video_transcode(gp, settings)
            processed_group.append(gp)
        send_post: dict[str, Any] | None = None
    else:
        processed_group = []
        if has_single_media:
            await _checkpoint(f"before_watermark media_type={media_type}")
            send_post = await _apply_photo_watermark(post, settings)
            await _checkpoint("after_watermark")
            send_post = await _apply_video_transcode(send_post, settings)
            await _checkpoint("after_transcode")
        else:
            send_post = None
            await _checkpoint(f"no_media_branch media_type='{media_type}'")

    any_sent = False
    # Если ни один админ не получил превью — сохраняем последнюю ошибку
    # в bot_secret 'last_image_gen_error' чтобы /diagimage показал точное
    # место падения. Раньше ошибка уходила только в bothost-логи и была
    # невидима в чате.
    last_send_error: str | None = None
    for admin_id in admin_ids:
        try:
            if media_group_id and processed_group:
                media_items = _build_group_media_items(processed_group, "")
                if media_items:
                    await bot.send_media_group(chat_id=admin_id, media=media_items)
                await bot.send_message(
                    chat_id=admin_id,
                    text=full_text,
                    disable_web_page_preview=True,
                    reply_markup=kb,
                )
            elif has_single_media and send_post is not None:
                file_id = send_post.get("media_file_id")
                media_path = send_post.get("media_path")
                if media_type == "photo":
                    if file_id:
                        await bot.send_photo(chat_id=admin_id, photo=file_id)
                    elif media_path:
                        await bot.send_photo(chat_id=admin_id, photo=FSInputFile(media_path))
                elif media_type == "video":
                    opts = _video_send_options(send_post)
                    if file_id:
                        await bot.send_video(chat_id=admin_id, video=file_id, **opts)
                    elif media_path:
                        await bot.send_video(chat_id=admin_id, video=FSInputFile(media_path), **opts)
                await bot.send_message(
                    chat_id=admin_id,
                    text=full_text,
                    disable_web_page_preview=True,
                    reply_markup=kb,
                )
            else:
                await bot.send_message(
                    chat_id=admin_id,
                    text=full_text,
                    disable_web_page_preview=True,
                    reply_markup=kb,
                )
            any_sent = True
        except TelegramAPIError as exc:
            logger.warning("Review preview send failed admin=%s err=%s", admin_id, exc)
            mp = ""
            sz = -1
            if has_single_media and send_post is not None:
                mp = str(send_post.get("media_path") or "")
                try:
                    if mp:
                        sz = int(Path(mp).stat().st_size)
                except OSError:
                    pass
            last_send_error = (
                f"admin={admin_id} TelegramAPIError: {exc}\n"
                f"media_type={media_type} media_path={mp} size_bytes={sz}"
            )
        except Exception as exc:
            logger.exception(
                "Review preview unexpected failure admin=%s source_post_id=%s",
                admin_id, source_post_id,
            )
            mp = ""
            sz = -1
            if has_single_media and send_post is not None:
                mp = str(send_post.get("media_path") or "")
                try:
                    if mp:
                        sz = int(Path(mp).stat().st_size)
                except OSError:
                    pass
            last_send_error = (
                f"admin={admin_id} {type(exc).__name__}: {exc}\n"
                f"media_type={media_type} media_path={mp} size_bytes={sz}"
            )

    # Финальная трассировка — пишем в bot_secret отчёт о том, какой бранч
    # был выбран и что происходило с медиа. Помогает понять, почему фото
    # не доходит до админа когда никаких exception нет.
    try:
        stamp = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        if media_group_id:
            branch = "media_group"
        elif has_single_media and send_post is not None:
            branch = f"single_{media_type}"
        else:
            branch = "text_only"
        send_post_path = ""
        if send_post is not None:
            send_post_path = str(send_post.get("media_path") or "")
        trace = (
            f"stage=preview_trace | post_id={source_post_id} | {stamp}\n"
            f"admin_media='{admin_media}' is_file={admin_media_is_file} size={admin_media_size}\n"
            f"post.media_type='{media_type}' post.media_path='{post_media_path}' "
            f"post.media_file_id='{post_media_file_id}' post.media_group_id='{media_group_id}'\n"
            f"has_single_media={has_single_media} branch={branch} "
            f"send_post.media_path='{send_post_path}'\n"
            f"any_sent={any_sent} last_send_error={last_send_error or '(none)'}"
        )
        await db.set_bot_secret("last_preview_trace", trace[:3500])
    except Exception:
        logger.exception("Failed to persist preview trace")
    return any_sent


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

    # Шлём оригинал поста админу СРАЗУ — до любых фильтров.
    # Так админ видит каждый собранный пост (даже короткий, даже дубликат, даже не-AI).
    # Канал получает только переписанные посты, прошедшие фильтры; админ — всё подряд.
    await _notify_admin_raw_source_post(
        db=db, bot=bot, settings=settings, source_post_id=source_post_id,
    )

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

    # Pre-LLM gates: реклама, обзоры/мнения, не-AI темы. Экономит LLM-кредиты
    # и ловит то, что post-LLM фильтр пропускал (например, рекламу с «релизы» в тексте).
    is_non_news, non_news_reason = _looks_like_non_news_source(raw_text)
    if is_non_news:
        await skip("skipped", f"pre_llm_{non_news_reason}")
        return
    raw_lower_for_gate = raw_text.lower()
    pre_llm_entities = extract_ai_entities(raw_text)
    has_news_signal_pre = any(x in raw_lower_for_gate for x in NEWS_SIGNAL_MARKERS)
    if not pre_llm_entities and not has_news_signal_pre:
        await skip("skipped", "pre_llm_no_ai_topic")
        return

    lookback = int(settings.channel_dedup_lookback_limit)
    dedup_window_hours = max(1, int(settings.channel_dedup_window_hours))
    dedup_since_iso = (
        datetime.now(tz=timezone.utc) - timedelta(hours=dedup_window_hours)
    ).isoformat()
    recent = await db.list_recent_published_source_records_for_channel_dedup(
        limit=lookback, since_iso=dedup_since_iso
    )
    current_external_links = _external_non_telegram_urls(raw_text)
    current_source_key = str(post.get("source_key") or "").strip().lower()
    current_has_media = _post_has_media(post)
    current_entities = extract_ai_entities(raw_text)
    entity_min_overlap = max(1, int(settings.channel_entity_min_overlap))
    entity_lexical_min = float(settings.channel_entity_lexical_min)
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
        has_new, new_reason = new_details_signal(raw_text, other_text)
        has_strong_new = has_new and _is_strong_new_details(new_reason)
        if current_external_links.intersection(other_external_links):
            if not has_strong_new:
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
        if max(score, lexical_score) >= settings.channel_near_dup_jaccard and not has_strong_new:
            near_dup_of = other_id
            duplicate_reason = f"near_duplicate_jaccard>={settings.channel_near_dup_jaccard:.2f}"
            break
        if current_entities and not has_strong_new:
            other_entities = extract_ai_entities(other_text)
            entity_overlap = current_entities & other_entities
            if (
                len(entity_overlap) >= entity_min_overlap
                and lexical_score >= entity_lexical_min
            ):
                near_dup_of = other_id
                duplicate_reason = (
                    f"entity_overlap>={len(entity_overlap)}:lex={lexical_score:.2f}"
                )
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

    # Few-shot: подгружаем оценённые посты как эталоны для подражания.
    system_prompt_full = CHANNEL_REWRITE_SYSTEM_PROMPT_V1
    if settings.enable_feedback_learning and (
        settings.feedback_best_examples > 0 or settings.feedback_worst_examples > 0
    ):
        try:
            since_iso = (
                datetime.now(tz=timezone.utc) - timedelta(days=settings.feedback_lookback_days)
            ).isoformat()
            best = await db.list_top_rated_posts(
                limit=settings.feedback_best_examples,
                min_rating=4,
                since_iso=since_iso,
            )
            worst = await db.list_worst_rated_posts(
                limit=settings.feedback_worst_examples,
                max_rating=2,
                since_iso=since_iso,
            )
            exemplar_block = build_exemplar_block(best, worst)
            if exemplar_block:
                system_prompt_full = system_prompt_full + exemplar_block
                logger.debug(
                    "Injected exemplars into prompt: best=%s worst=%s",
                    len(best),
                    len(worst),
                )
        except Exception:
            logger.exception("Failed to build exemplar block — continue without")

    metrics.channel_llm_calls += 1
    t0 = monotonic()
    llm: RoutedLlmResult = await asyncio.to_thread(
        call_llm_with_fallback,
        settings,
        system_prompt=system_prompt_full,
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
    hashtags_raw = llm.parsed.get("hashtags") or []
    # Сначала вычистить мусор от LLM, потом запустить штатные хелперы.
    post_text = _strip_llm_html(post_text)
    post_text = _strip_useless_link_headers(post_text)
    post_text = _strip_dangling_pointer_emojis(post_text)
    post_text = _strip_linklike_cta_without_links(post_text)
    post_text = _beautify_links_block(post_text)

    # Минимальная длина body — отсекаем только совсем мусорные ответы LLM.
    # Короткие посты (200-450 символов) теперь разрешены промптом v7.
    if len(re.sub(r"<[^>]+>", "", post_text or "").strip()) < 100:
        await skip("skipped", "post_llm_too_short")
        return

    if _looks_like_non_news(raw_text, title, post_text):
        await skip("skipped", "post_llm_non_news_gate")
        return

    generated_probe = _compose_generated_dedup_text(title, post_text)
    if generated_probe:
        generated_fp = fingerprint_text(generated_probe)
        recent_generated = await db.list_recent_published_generated_texts_for_channel_dedup(
            limit=lookback, since_iso=dedup_since_iso
        )
        generated_entities = extract_ai_entities(generated_probe)
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
            has_new_g, new_reason_g = new_details_signal(generated_probe, other_generated)
            has_strong_new_g = has_new_g and _is_strong_new_details(new_reason_g)
            if other_generated and other_id != source_post_id:
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
            if max(score, lexical_score) >= settings.channel_near_dup_jaccard and not has_strong_new_g:
                await skip(
                    "duplicate",
                    "post_llm_near_duplicate",
                    duplicate_of_source_post_id=other_id,
                )
                return
            if generated_entities and not has_strong_new_g:
                other_gen_entities = extract_ai_entities(other_generated)
                gen_overlap = generated_entities & other_gen_entities
                if (
                    len(gen_overlap) >= entity_min_overlap
                    and lexical_score >= entity_lexical_min
                ):
                    await skip(
                        "duplicate",
                        f"post_llm_entity_overlap>={len(gen_overlap)}",
                        duplicate_of_source_post_id=other_id,
                    )
                    return

    # Хэштеги отключены глобально — игнорируем что вернёт LLM.
    hashtags_list: list[Any] = []
    hashtags_json_str = json.dumps(hashtags_list, ensure_ascii=False)

    await db.update_generated_channel_post(
        source_post_id,
        status="generated",
        llm_provider=llm.provider_used,
        llm_model=llm.model_used,
        prompt_version=CHANNEL_REWRITE_PROMPT_VERSION,
        title=title,
        post_text=post_text,
        summary=short_summary,
        hashtags_json=hashtags_json_str,
        clear_error=True,
    )
    logger.debug(
        "channel_autopublish source_post_id=%s generated llm_latency_ms=%s",
        source_post_id,
        dt_ms,
    )

    # Если включена ручная модерация — отправляем превью админу и не публикуем сразу.
    if settings.enable_channel_review and _admin_chat_ids(settings):
        await db.update_generated_channel_post(
            source_post_id,
            status="pending_review",
            clear_error=True,
        )
        sent_preview = await _send_review_preview_to_admin(
            db=db,
            bot=bot,
            settings=settings,
            source_post_id=source_post_id,
        )
        if not sent_preview:
            await fail("review_preview_send_failed")
        return

    # Авто-публикация без модерации.
    await _publish_generated_post(
        db=db,
        bot=bot,
        metrics=metrics,
        settings=settings,
        source_post_id=source_post_id,
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
