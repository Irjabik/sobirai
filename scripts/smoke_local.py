"""Локальная проверка без сети и без секретов: импорты и число каналов."""

from __future__ import annotations

import asyncio
import sys
import tempfile
from pathlib import Path

from aiogram.types import InputMediaVideo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MIN_TG_SOURCES = 10
MIN_X_SOURCES = 1


async def _check_channel_schema_migration() -> None:
    from app.db import Database
    from app.text_norm import fingerprint_text

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = Path(f.name)
    try:
        db = Database(path)
        await db.connect()
        async with db.conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name IN ('generated_channel_posts', 'publish_daily_counters')
            ORDER BY name
            """
        ) as cur:
            names = [r[0] for r in await cur.fetchall()]
        assert names == [
            "generated_channel_posts",
            "publish_daily_counters",
        ], names
        now = Database._now()
        raw_text = "OpenAI выпустила тестовую модель для проверки точного дедупа."
        fp = fingerprint_text(raw_text)
        cursor = await db.conn.execute(
            """
            INSERT INTO source_posts(
              platform, source_key, channel_username, source_message_id, channel_title,
              source_message_date, source_link, text, created_at
            )
            VALUES('tg', 'test', '@test', 1, 'Test', ?, 'https://t.me/test/1', ?, ?)
            """,
            (now, raw_text, now),
        )
        failed_source_id = int(cursor.lastrowid)
        cursor = await db.conn.execute(
            """
            INSERT INTO source_posts(
              platform, source_key, channel_username, source_message_id, channel_title,
              source_message_date, source_link, text, created_at
            )
            VALUES('tg', 'test', '@test', 2, 'Test', ?, 'https://t.me/test/2', ?, ?)
            """,
            (now, raw_text, now),
        )
        retry_source_id = int(cursor.lastrowid)
        await db.conn.execute(
            """
            INSERT INTO generated_channel_posts(
              source_post_id, status, fingerprint, channel_chat_id, created_at, updated_at
            )
            VALUES(?, 'failed', ?, -1001, ?, ?)
            """,
            (failed_source_id, fp, now, now),
        )
        await db.conn.commit()
        assert await db.find_channel_fingerprint_duplicate(fp, retry_source_id) is None
        await db.update_generated_channel_post(failed_source_id, status="published")
        assert await db.find_channel_fingerprint_duplicate(fp, retry_source_id) == failed_source_id
        await db.close()
    finally:
        path.unlink(missing_ok=True)


def main() -> None:
    from app.sources import SOURCES
    from app.text_norm import fingerprint_text, has_new_details_vs_reference
    from app.llm_client import RoutedLlmResult
    from app import llm_sambanova  # noqa: F401
    from app.channel_autopublish import (
        _build_group_media_items,
        _build_channel_message,
        _extract_external_links,
        _external_non_telegram_urls,
        _inject_inline_links,
        _is_external_non_telegram_url,
        _strip_trailing_read_more,
        _topic_memory_duplicate_decision,
    )

    n = len(SOURCES)
    tg_count = sum(1 for s in SOURCES if s.platform == "tg")
    x_count = sum(1 for s in SOURCES if s.platform == "x")
    assert tg_count >= MIN_TG_SOURCES, f"expected >= {MIN_TG_SOURCES} tg sources, got {tg_count}"
    assert x_count >= MIN_X_SOURCES, f"expected >= {MIN_X_SOURCES} x sources, got {x_count}"
    print(f"ok: {n} sources (tg={tg_count}, x={x_count})")

    assert fingerprint_text("Hello  world") == fingerprint_text("hello world")
    print("ok: text_norm fingerprint")

    _ = RoutedLlmResult(
        ok=False,
        parsed=None,
        error_code=None,
        attempts=0,
        provider_used="sambanova",
        model_used="Meta-Llama-3.1-8B-Instruct",
    )
    print("ok: llm_client and llm_sambanova import")

    cleaned = _strip_trailing_read_more("Новость дня. Читать далее: https://example.com/full")
    assert "читать далее" not in cleaned.lower(), cleaned
    print("ok: source text cleanup (read more)")

    msg = _build_channel_message("<b>Заголовок</b>", "<b>Заголовок</b>\n\nТекст поста", "")
    assert "#" not in msg, msg
    assert "Источник:" not in msg, msg
    assert msg.count("Заголовок") == 1, msg
    print("ok: channel message builder (no hashtags + no source block + dedup title)")

    assert not _is_external_non_telegram_url("https://t.me/test"), "telegram URL should be excluded"
    links = _extract_external_links("Релиз тут https://github.com/openai/openai-python и docs https://docs.python.org/3/")
    assert len(links) == 2, links
    enriched, used = _inject_inline_links("Обновили openai-python и документацию.", links)
    assert used, "expected at least one inline link insertion"
    msg2 = _build_channel_message("<b>Заголовок</b>", enriched, "Полезные ссылки: <a href=\"https://docs.python.org/3/\">python.org</a>")
    assert "Полезные ссылки:" in msg2 and msg2.index("Полезные ссылки:") < msg2.index("Sobirai_News"), msg2
    print("ok: external links extraction/enrichment/fallback placement")

    items = _build_group_media_items(
        [{"media_type": "video", "media_file_id": "video_file_id_1"}],
        "caption",
    )
    assert items and isinstance(items[0], InputMediaVideo), items
    print("ok: video media_group items always use InputMediaVideo")

    base = (
        "OpenAI выпустила новую модель GPT-5.3 для разработки. "
        "Компания заявила ускорение инференса на 40% и снижение стоимости."
    )
    same_topic_rephrase = (
        "OpenAI выпустила GPT-5.3 для разработки. Компания сообщила, что инференс ускорен на 40%, "
        "а стоимость снижена."
    )
    strong_update = (
        "OpenAI выпустила GPT-5.3 для разработки. Инференс ускорили на 40%, цена снижена, "
        "а еще добавили контекст 2M токенов и поддержку function-calling."
    )
    assert not has_new_details_vs_reference(same_topic_rephrase, base)
    assert has_new_details_vs_reference(strong_update, base)
    print("ok: dedup regression (same topic duplicate + real update)")

    gitnexus_with_media = (
        "GitNexus: новый инструмент для ИИ-ассистентов, дающий им зрение архитектора. "
        "Claude и Cursor читают локальный граф знаний проекта и помогают с рефакторингом. "
        "https://github.com/idosal/gitnexus"
    )
    gitnexus_text_copy = (
        "ИИ-ассистенты Cursor и Claude теперь могут видеть архитектуру проекта благодаря GitNexus. "
        "Это помогает точнее и безопаснее предлагать рефакторинг кода. "
        "https://github.com/idosal/gitnexus"
    )
    is_dup, reason = _topic_memory_duplicate_decision(
        gitnexus_text_copy,
        gitnexus_with_media,
        threshold=0.42,
        same_source=True,
        current_links=_external_non_telegram_urls(gitnexus_text_copy),
        reference_links=_external_non_telegram_urls(gitnexus_with_media),
        current_has_media=False,
        reference_has_media=True,
    )
    assert is_dup, reason
    assert reason in {"topic_memory_link_overlap", "topic_memory_same_source_text_after_media"}, reason
    print("ok: topic memory blocks same-source text duplicate after media")

    asyncio.run(_check_channel_schema_migration())
    print("ok: channel autopublish DB tables + failed exact dedup retry")


if __name__ == "__main__":
    main()
