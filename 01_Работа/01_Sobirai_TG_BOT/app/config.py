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
    x_api_bearer_token: str
    collector_poll_seconds: int = 3
    digest_poll_seconds: int = 60
    enable_x_sources: bool = True
    x_api_base_url: str = "https://api.x.com/2"
    x_api_fetch_interval_seconds: int = 60
    x_api_sources_per_tick: int = 1
    x_api_user_cache_ttl_seconds: int = 86400
    x_api_max_pages_per_source: int = 1
    x_api_max_results: int = 20
    x_api_max_requests_per_hour: int = 120
    x_fetch_timeout_seconds: int = 25
    enable_media_downloads: bool = True
    min_free_disk_mb: int = 512
    media_retention_days: int = 3
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
        enable_x_raw = os.getenv("ENABLE_X_SOURCES", "1").strip().lower()
        x_bearer_token = os.getenv("X_API_BEARER_TOKEN", "").strip()
        x_api_base_url = os.getenv("X_API_BASE_URL", "https://api.x.com/2").strip()
        x_api_fetch_interval_raw = os.getenv("X_API_FETCH_INTERVAL_SECONDS", "60").strip()
        x_api_sources_per_tick_raw = os.getenv("X_API_SOURCES_PER_TICK", "1").strip()
        x_api_user_cache_ttl_raw = os.getenv("X_API_USER_CACHE_TTL_SECONDS", "86400").strip()
        x_api_max_pages_raw = os.getenv("X_API_MAX_PAGES_PER_SOURCE", "1").strip()
        x_api_max_results_raw = os.getenv("X_API_MAX_RESULTS", "20").strip()
        x_api_max_requests_per_hour_raw = os.getenv("X_API_MAX_REQUESTS_PER_HOUR", "120").strip()
        x_timeout_raw = os.getenv("X_FETCH_TIMEOUT_SECONDS", "25").strip()
        media_downloads_raw = os.getenv("ENABLE_MEDIA_DOWNLOADS", "1").strip().lower()
        min_free_disk_mb_raw = os.getenv("MIN_FREE_DISK_MB", "512").strip()
        media_retention_days_raw = os.getenv("MEDIA_RETENTION_DAYS", "3").strip()
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
        if x_timeout_raw.isdigit() is False or int(x_timeout_raw) < 5:
            raise ValueError("X_FETCH_TIMEOUT_SECONDS must be an integer >= 5")
        if not x_bearer_token:
            raise ValueError("X_API_BEARER_TOKEN is required")
        if not x_api_fetch_interval_raw.isdigit() or int(x_api_fetch_interval_raw) < 5:
            raise ValueError("X_API_FETCH_INTERVAL_SECONDS must be an integer >= 5")
        if not x_api_sources_per_tick_raw.isdigit() or int(x_api_sources_per_tick_raw) < 1:
            raise ValueError("X_API_SOURCES_PER_TICK must be an integer >= 1")
        if not x_api_user_cache_ttl_raw.isdigit() or int(x_api_user_cache_ttl_raw) < 60:
            raise ValueError("X_API_USER_CACHE_TTL_SECONDS must be an integer >= 60")
        if not x_api_max_pages_raw.isdigit() or int(x_api_max_pages_raw) < 1:
            raise ValueError("X_API_MAX_PAGES_PER_SOURCE must be an integer >= 1")
        if not x_api_max_results_raw.isdigit() or int(x_api_max_results_raw) < 5:
            raise ValueError("X_API_MAX_RESULTS must be an integer >= 5")
        if not x_api_max_requests_per_hour_raw.isdigit() or int(x_api_max_requests_per_hour_raw) < 1:
            raise ValueError("X_API_MAX_REQUESTS_PER_HOUR must be an integer >= 1")
        if not x_api_base_url:
            raise ValueError("X_API_BASE_URL is required")
        if min_free_disk_mb_raw.isdigit() is False or int(min_free_disk_mb_raw) < 64:
            raise ValueError("MIN_FREE_DISK_MB must be an integer >= 64")
        if media_retention_days_raw.isdigit() is False or int(media_retention_days_raw) < 1:
            raise ValueError("MEDIA_RETENTION_DAYS must be an integer >= 1")
        return Settings(
            bot_token=bot_token,
            telegram_api_id=int(api_id_raw),
            telegram_api_hash=api_hash,
            database_path=db_path,
            telethon_session=session_path,
            telethon_session_string=sess_str if sess_str else None,
            collector_poll_seconds=int(collector_poll_raw),
            digest_poll_seconds=int(digest_poll_raw),
            enable_x_sources=enable_x_raw in {"1", "true", "yes", "on"},
            x_api_bearer_token=x_bearer_token,
            x_api_base_url=x_api_base_url,
            x_api_fetch_interval_seconds=int(x_api_fetch_interval_raw),
            x_api_sources_per_tick=int(x_api_sources_per_tick_raw),
            x_api_user_cache_ttl_seconds=int(x_api_user_cache_ttl_raw),
            x_api_max_pages_per_source=int(x_api_max_pages_raw),
            x_api_max_results=int(x_api_max_results_raw),
            x_api_max_requests_per_hour=int(x_api_max_requests_per_hour_raw),
            x_fetch_timeout_seconds=int(x_timeout_raw),
            enable_media_downloads=media_downloads_raw in {"1", "true", "yes", "on"},
            min_free_disk_mb=int(min_free_disk_mb_raw),
            media_retention_days=int(media_retention_days_raw),
            log_level=log_level,
        )
