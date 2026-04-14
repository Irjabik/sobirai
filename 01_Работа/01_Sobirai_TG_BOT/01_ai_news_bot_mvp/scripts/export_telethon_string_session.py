#!/usr/bin/env python3
"""Выводит TELETHON_SESSION_STRING для Docker/PaaS (Bothost), где .session на диске не сохраняется.

Запуск из корня проекта с заполненным .venv и .env:
  python scripts/export_telethon_string_session.py

Скопируй одну строку вывода в переменную окружения TELETHON_SESSION_STRING на хостинге.
Не коммить и не светить эту строку — это полноценный доступ к аккаунту MTProto.
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

# корень проекта = родитель scripts/
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env")

from telethon import TelegramClient
from telethon.sessions import StringSession


async def main() -> None:
    api_id_raw = os.getenv("TELEGRAM_API_ID", "").strip()
    api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
    if not api_id_raw.isdigit() or not api_hash:
        print("Нужны TELEGRAM_API_ID и TELEGRAM_API_HASH в .env", file=sys.stderr)
        sys.exit(1)

    session_path = os.getenv("TELETHON_SESSION", "./data/telethon_session")
    os.chdir(_ROOT)
    client = TelegramClient(session_path, int(api_id_raw), api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        print("Сначала войди в Telethon локально: python -m app.main", file=sys.stderr)
        sys.exit(1)
    line = StringSession.save(client.session)
    print(line)
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
