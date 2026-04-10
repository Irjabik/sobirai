from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

LONG_TEXT_LIMIT = 1200
DELIVERY_MODES = ("instant", "digest")


@dataclass(frozen=True)
class Settings:
    bot_token: str
    telegram_api_id: int
    telegram_api_hash: str
    database_path: Path
    telethon_session: Path
    telethon_session_string: Optional[str]
    log_level: str = "INFO"

    @staticmethod
    def from_env() -> "Settings":
        bot_token = os.getenv("BOT_TOKEN", "").strip()
        api_id_raw = os.getenv("TELEGRAM_API_ID", "").strip()
        api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
        db_path = Path(os.getenv("DATABASE_PATH", "./data/bot.db"))
        session_path = Path(os.getenv("TELETHON_SESSION", "./data/telethon_session"))
        sess_str = os.getenv("TELETHON_SESSION_STRING", "").strip()
        log_level = os.getenv("LOG_LEVEL", "INFO").upper().strip()

        if not bot_token:
            raise ValueError("BOT_TOKEN is required")
        if not api_id_raw.isdigit():
            raise ValueError("TELEGRAM_API_ID must be numeric")
        if not api_hash:
            raise ValueError("TELEGRAM_API_HASH is required")
        return Settings(
            bot_token=bot_token,
            telegram_api_id=int(api_id_raw),
            telegram_api_hash=api_hash,
            database_path=db_path,
            telethon_session=session_path,
            telethon_session_string=sess_str if sess_str else None,
            log_level=log_level,
        )
