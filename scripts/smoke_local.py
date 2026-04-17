"""Локальная проверка без сети и без секретов: импорты и число каналов."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

EXPECTED_TG_SOURCES = 29
EXPECTED_X_SOURCES = 5


def main() -> None:
    from app.sources import SOURCES

    n = len(SOURCES)
    tg_count = sum(1 for s in SOURCES if s.platform == "tg")
    x_count = sum(1 for s in SOURCES if s.platform == "x")
    assert tg_count == EXPECTED_TG_SOURCES, f"expected {EXPECTED_TG_SOURCES} tg sources, got {tg_count}"
    assert x_count == EXPECTED_X_SOURCES, f"expected {EXPECTED_X_SOURCES} x sources, got {x_count}"
    print(f"ok: {n} sources (tg={tg_count}, x={x_count})")


if __name__ == "__main__":
    main()
