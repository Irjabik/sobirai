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
    collector_poll_seconds: int = 3
    digest_poll_seconds: int = 60
    log_level: str = "INFO"

    @staticmethod
    def from_env() -> "Settings":
        bot_token = os.getenv("BOT_TOKEN", "").strip()
        api_id_raw = os.getenv("TELEGRAM_API_ID", "").strip()
        api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
        db_path = Path(os.getenv("DATABASE_PATH", "./data/bot.db"))
        session_path = Path(os.getenv("TELETHON_SESSION", "./data/telethon_session"))
        sess_str = os.getenv("TELETHON_SESSION_STRING", "").strip()
        collector_poll_raw = os.getenv("COLLECTOR_POLL_SECONDS", "3").strip()
        digest_poll_raw = os.getenv("DIGEST_POLL_SECONDS", "60").strip()
        log_level = os.getenv("LOG_LEVEL", "INFO").upper().strip()

        if not bot_token:
            raise ValueError("BOT_TOKEN is required")
        if not api_id_raw.isdigit():
            raise ValueError("TELEGRAM_API_ID must be numeric")
        if not api_hash:
            raise ValueError("TELEGRAM_API_HASH is required")
        if not collector_poll_raw.isdigit() or int(collector_poll_raw) < 1:
            raise ValueError("COLLECTOR_POLL_SECONDS must be a positive integer")
        if not digest_poll_raw.isdigit() or int(digest_poll_raw) < 5:
            raise ValueError("DIGEST_POLL_SECONDS must be an integer >= 5")
        return Settings(
            bot_token=bot_token,
            telegram_api_id=int(api_id_raw),
            telegram_api_hash=api_hash,
            database_path=db_path,
            telethon_session=session_path,
            telethon_session_string=sess_str if sess_str else None,
            collector_poll_seconds=int(collector_poll_raw),
            digest_poll_seconds=int(digest_poll_raw),
            log_level=log_level,
        )
