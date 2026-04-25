"""Локальная проверка без сети и без секретов: импорты и число каналов."""

from __future__ import annotations

import asyncio
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MIN_TG_SOURCES = 10
MIN_X_SOURCES = 1


async def _check_channel_schema_migration() -> None:
    from app.db import Database

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
        await db.close()
    finally:
        path.unlink(missing_ok=True)


def main() -> None:
    from app.sources import SOURCES
    from app.text_norm import fingerprint_text, has_new_details_vs_reference
    from app.llm_client import RoutedLlmResult
    from app import llm_sambanova  # noqa: F401
    from app.channel_autopublish import (
        _build_channel_message,
        _beautify_links_block,
        _external_non_telegram_urls,
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

    msg = _build_channel_message("<b>Заголовок</b>", "<b>Заголовок</b>\n\nТекст поста", [], "sambanova")
    assert "#" not in msg, msg
    assert msg.count("Заголовок") == 1, msg
    print("ok: channel message builder (no hashtags + dedup title)")

    assert not _external_non_telegram_urls("https://t.me/test"), "telegram URL should be excluded"
    enriched = _beautify_links_block(
        "Релиз тут https://github.com/openai/openai-python и docs https://docs.python.org/3/"
    )
    assert "<a href=" in enriched, enriched
    msg2 = _build_channel_message("<b>Заголовок</b>", enriched, [], "sambanova")
    assert "Sobirai_News" in msg2, msg2
    print("ok: external links extraction/enrichment")

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
    print("ok: channel autopublish DB tables")


if __name__ == "__main__":
    main()
