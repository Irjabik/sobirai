"""Локальная проверка без сети и без секретов: импорты и число каналов."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

EXPECTED_SOURCES = 29


def main() -> None:
    from app.sources import SOURCES

    n = len(SOURCES)
    assert n == EXPECTED_SOURCES, f"expected {EXPECTED_SOURCES} sources, got {n}"
    print(f"ok: {n} channels in SOURCES")


if __name__ == "__main__":
    main()
