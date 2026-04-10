from __future__ import annotations

import html
import re
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from .config import LONG_TEXT_LIMIT

URL_RE = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)
# «часть 1 из 2», «(Часть 1 из 2)», multiline
SERIES_RE = re.compile(
    r"(?is)(?:\(\s*)?\bчасть\s*(\d+)\s*из\s*(\d+)\b(?:\s*\))?",
)
ENG_SERIES_RE = re.compile(r"(?is)\bpart\s*(\d+)\s*/\s*(\d+)\b")
DIGEST_MAX_PER_CHANNEL_DEFAULT = 2


def _channel_key_for_digest(post: dict) -> str:
    """Единый ключ канала для дайджеста: игнорируем @ и регистр (иначе лимит 2/канал обходится)."""
    u = (post.get("channel_username") or "").strip().lstrip("@").lower()
    return u or "unknown"


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
    *,
    text_limit: int = LONG_TEXT_LIMIT,
    max_length: int | None = None,
) -> str:
    body, _ = truncate_text(text, limit=text_limit)
    caption = f"<b>{channel_title}</b> ({channel_username})\n\n{body}\n\nОригинал: {source_link}"
    if max_length is not None and len(caption) > max_length:
        header = f"<b>{channel_title}</b> ({channel_username})\n\n"
        footer = f"\n\nОригинал: {source_link}"
        available = max(40, max_length - len(header) - len(footer))
        body, _ = truncate_text(text, limit=available)
        caption = f"{header}{body}{footer}"
        if len(caption) > max_length:
            caption = caption[: max_length - 1]
    return caption


def deduplicate_digest_posts(posts: list[dict], limit: int = 10) -> list[dict]:
    """
    Prepare digest list: merge multi-part series, dedupe, round-robin by channel,
    max DIGEST_MAX_PER_CHANNEL_DEFAULT items per channel.
    """
    return prepare_digest_posts(
        posts,
        limit=limit,
        max_per_channel=DIGEST_MAX_PER_CHANNEL_DEFAULT,
    )


def prepare_digest_posts(
    posts: list[dict],
    *,
    limit: int = 10,
    max_per_channel: int = DIGEST_MAX_PER_CHANNEL_DEFAULT,
) -> list[dict]:
    if not posts:
        return []
    newest_first = sorted(posts, key=_post_sort_key, reverse=True)
    merged = merge_digest_series(newest_first)
    merged = sorted(merged, key=_post_sort_key, reverse=True)
    unique = _dedupe_digest_posts_all(merged)
    unique = sorted(unique, key=_post_sort_key, reverse=True)
    return round_robin_digest_select(unique, limit=limit, max_per_channel=max_per_channel)


def merge_digest_series(posts: list[dict]) -> list[dict]:
    """Склеивает посты одной серии (часть N из M) с одного канала в один элемент."""
    groups: dict[tuple[str, str], list[dict]] = {}
    ungrouped: list[dict] = []

    for post in posts:
        ch = _channel_key_for_digest(post)
        is_series, base_key = _series_group_key(post)
        if not is_series:
            ungrouped.append(post)
            continue
        groups.setdefault((ch, base_key), []).append(post)

    result: list[dict] = list(ungrouped)
    for items in groups.values():
        if len(items) == 1:
            result.append(items[0])
        else:
            result.append(_merge_series_posts(items))
    return result


def round_robin_digest_select(
    posts: list[dict],
    *,
    limit: int = 10,
    max_per_channel: int = DIGEST_MAX_PER_CHANNEL_DEFAULT,
) -> list[dict]:
    """
    Берём посты в порядке round-robin по каналам (канал с самым свежим топом первый),
    не больше max_per_channel с одного канала.
    ``posts`` — уже отсортированы от новых к старым глобально.
    """
    if not posts or limit <= 0:
        return []
    channel_order: list[str] = []
    seen: set[str] = set()
    for p in posts:
        ck = _channel_key_for_digest(p)
        if ck not in seen:
            seen.add(ck)
            channel_order.append(ck)
    queues: dict[str, list[dict]] = {ck: [] for ck in channel_order}
    for p in posts:
        ck = _channel_key_for_digest(p)
        if ck in queues:
            queues[ck].append(p)

    result: list[dict] = []
    counts = {ck: 0 for ck in channel_order}
    ptrs = {ck: 0 for ck in channel_order}
    while len(result) < limit:
        progressed = False
        for ck in channel_order:
            if len(result) >= limit:
                break
            if counts[ck] >= max_per_channel:
                continue
            q = queues.get(ck, [])
            i = ptrs[ck]
            if i < len(q):
                result.append(q[i])
                ptrs[ck] = i + 1
                counts[ck] += 1
                progressed = True
        if not progressed:
            break
    return result


def expanded_source_post_ids_for_digest(posts: list[dict]) -> list[int]:
    """Все id постов в БД, которые покрывает дайджест (включая части серии)."""
    ids: list[int] = []
    for p in posts:
        merged = p.get("merged_source_post_ids")
        if merged:
            ids.extend(int(x) for x in merged)
        else:
            ids.append(int(p["id"]))
    return ids


def _dedupe_digest_posts_all(posts: list[dict]) -> list[dict]:
    seen: set[str] = set()
    unique: list[dict] = []
    for post in posts:
        key = _digest_dedup_key(post)
        if key in seen:
            continue
        seen.add(key)
        unique.append(post)
    return unique


def _post_sort_key(post: dict) -> float:
    raw = post.get("source_message_date")
    if raw is None:
        return 0.0
    if isinstance(raw, datetime):
        return raw.timestamp()
    try:
        s = str(raw).replace("Z", "+00:00")
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return 0.0


def _series_match(text: str) -> Any:
    m = SERIES_RE.search(text or "")
    if m:
        return m
    return ENG_SERIES_RE.search(text or "")


def _series_group_key(post: dict) -> tuple[bool, str]:
    text = post.get("text") or ""
    m = _series_match(text)
    if not m:
        return False, ""
    base = (text[: m.start()] + text[m.end() :]).strip()
    base = re.sub(r"\s+", " ", base)
    if len(base) > 220:
        base = base[:220]
    norm = _normalize_text(base)[:140] if base else ""
    if not norm:
        norm = _normalize_text(text)[:140]
    return True, norm


def _part_number(post: dict) -> int:
    m = _series_match(post.get("text") or "")
    if not m:
        return 0
    return int(m.group(1))


def _merge_series_posts(items: list[dict]) -> dict:
    sorted_items = sorted(items, key=_part_number)
    first = sorted_items[0]
    merged = dict(first)
    texts = [(it.get("text") or "").strip() for it in sorted_items]
    merged["text"] = "\n\n".join(t for t in texts if t)
    merged["merged_source_post_ids"] = [int(it["id"]) for it in sorted_items]
    merged["source_link"] = first.get("source_link") or sorted_items[0].get("source_link")
    best_ts = max((_post_sort_key(it) for it in sorted_items), default=0.0)
    for it in sorted_items:
        if _post_sort_key(it) == best_ts:
            merged["source_message_date"] = it.get("source_message_date")
            break
    return merged


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
        ck = _channel_key_for_digest(post)
        channel = html.escape(f"@{ck}" if ck != "unknown" else "@unknown")
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
    # Keep all distinct source messages in digest; only collapse obvious reposts by same external URL.
    # This avoids dropping "batch" posts from one channel that have similar text.
    channel = _channel_key_for_digest(post)
    msg_id = post.get("source_message_id") or post.get("id")
    if msg_id:
        return f"msg:{channel}:{msg_id}"
    return f"fallback:{post.get('channel_username','')}:{post.get('source_message_id','')}"

