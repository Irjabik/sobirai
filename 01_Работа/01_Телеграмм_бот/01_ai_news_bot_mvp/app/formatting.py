from __future__ import annotations

import html
import re
from datetime import datetime
from urllib.parse import urlparse

from .config import LONG_TEXT_LIMIT

URL_RE = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)


def truncate_text(text: str, limit: int = LONG_TEXT_LIMIT) -> tuple[str, bool]:
    cleaned = (text or "").strip()
    if len(cleaned) <= limit:
        return cleaned, False
    sliced = cleaned[:limit].rstrip()
    return f"{sliced}\n\n… Читать далее в оригинале.", True


def render_caption(
    channel_title: str,
    channel_username: str,
    source_date: str | datetime,
    text: str,
    source_link: str,
) -> str:
    if isinstance(source_date, datetime):
        date_str = source_date.strftime("%Y-%m-%d %H:%M")
    else:
        try:
            date_str = datetime.fromisoformat(source_date).strftime("%Y-%m-%d %H:%M")
        except Exception:
            date_str = source_date
    body, _ = truncate_text(text)
    return (
        f"<b>{channel_title}</b> ({channel_username})\n"
        f"<i>{date_str}</i>\n\n"
        f"{body}\n\n"
        f"Оригинал: {source_link}"
    )


def deduplicate_digest_posts(posts: list[dict], limit: int = 10) -> list[dict]:
    """
    Deduplicate posts for digest using a stable MVP strategy:
    1) external URL from text
    2) normalized text prefix
    3) fallback source + message id
    """
    seen: set[str] = set()
    unique: list[dict] = []
    for post in posts:
        key = _digest_dedup_key(post)
        if key in seen:
            continue
        seen.add(key)
        unique.append(post)
        if len(unique) >= limit:
            break
    return unique


def render_digest_list(posts: list[dict], hours_window: int) -> str:
    if not posts:
        if hours_window > 0:
            return f"Пока нет новых постов за последние {hours_window} часов по вашим фильтрам."
        return "Пока нет постов по вашим фильтрам."
    if hours_window > 0:
        lines = [f"<b>Дайджест AI за {hours_window} часов</b>"]
    else:
        lines = ["<b>Дайджест AI (без фильтра по времени)</b>"]
    for idx, post in enumerate(posts, start=1):
        channel = html.escape(post.get("channel_username") or "")
        link = html.escape(post.get("source_link") or "", quote=True)
        summary = html.escape(_brief(post.get("text") or "Без текста"))
        lines.append(f'{idx}. <a href="{link}">{summary}</a>\nИсточник: {channel}')
    return "\n\n".join(lines)


def _brief(text: str, limit: int = 110) -> str:
    normalized = " ".join((text or "").strip().split())
    if len(normalized) <= limit:
        return normalized
    return normalized[:limit].rstrip() + "…"


def _normalize_text(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_external_url(text: str) -> str | None:
    for match in URL_RE.findall(text or ""):
        try:
            host = (urlparse(match).netloc or "").lower()
        except Exception:
            host = ""
        if "t.me" not in host and "telegram.me" not in host:
            return match
    return None


def _digest_dedup_key(post: dict) -> str:
    text = post.get("text") or ""
    external_url = _extract_external_url(text)
    if external_url:
        return f"url:{external_url.lower()}"
    normalized_prefix = _normalize_text(text)[:220]
    if normalized_prefix:
        return f"text:{normalized_prefix}"
    return f"fallback:{post.get('channel_username','')}:{post.get('source_message_id','')}"

