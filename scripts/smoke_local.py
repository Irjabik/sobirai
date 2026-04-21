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
    from app.text_norm import fingerprint_text
    from app.llm_client import RoutedLlmResult
    from app import llm_sambanova  # noqa: F401
    from app.channel_autopublish import _build_channel_message, _strip_trailing_read_more

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

    msg = _build_channel_message("<b>Заголовок</b>", "<b>Заголовок</b>\n\nТекст поста")
    assert "#" not in msg, msg
    assert "Источник:" not in msg, msg
    assert msg.count("Заголовок") == 1, msg
    print("ok: channel message builder (no hashtags + no source block + dedup title)")

    asyncio.run(_check_channel_schema_migration())
    print("ok: channel autopublish DB tables")


if __name__ == "__main__":
    main()
