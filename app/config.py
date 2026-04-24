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
    enable_channel_autopublish: bool = False
    channel_chat_id: Optional[int] = None
    channel_max_posts_per_day: int = 20
    channel_poll_seconds: int = 30
    channel_min_candidate_chars: int = 24
    channel_near_dup_jaccard: float = 0.82
    channel_llm_candidates_per_tick: int = 2
    channel_llm_gap_seconds: float = 15.0
<<<<<<< HEAD:app/config.py
    channel_video_no_compression: bool = True
    channel_text_only_sources: tuple[str, ...] = ()
=======
    channel_dedup_lookback_limit: int = 600
    channel_video_no_compression: bool = True
>>>>>>> dc6bde5 (fix(channel): harden dedup and normalize link presentation):01_Работа/01_Sobirai_TG_BOT/app/config.py
    llm_provider: str = "sambanova"
    llm_primary_provider: str = "sambanova"
    llm_fallback_provider: str = "groq"
    llm_fallback_enabled: bool = True
    sambanova_api_key: str = ""
    sambanova_model: str = "Meta-Llama-3.1-8B-Instruct"
    sambanova_api_base: str = "https://api.sambanova.ai/v1"
    groq_api_key: str = ""
    llm_model: str = "llama-3.1-8b-instant"
    llm_timeout_seconds: float = 25.0
    llm_max_retries: int = 2
    llm_max_input_chars: int = 6000
    llm_max_output_tokens: int = 500
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
        enable_ch_raw = os.getenv("ENABLE_CHANNEL_AUTOPUBLISH", "0").strip().lower()
        channel_chat_raw = os.getenv("CHANNEL_CHAT_ID", "").strip()
        channel_max_posts_raw = os.getenv("CHANNEL_MAX_POSTS_PER_DAY", "20").strip()
        channel_poll_raw = os.getenv("CHANNEL_POLL_SECONDS", "30").strip()
        channel_min_text_raw = os.getenv("CHANNEL_MIN_CANDIDATE_CHARS", "24").strip()
        channel_near_dup_raw = os.getenv("CHANNEL_NEAR_DUP_JACCARD", "0.82").strip()
        channel_llm_per_tick_raw = os.getenv("CHANNEL_LLM_CANDIDATES_PER_TICK", "2").strip()
        channel_llm_gap_raw = os.getenv("CHANNEL_LLM_GAP_SECONDS", "15").strip()
<<<<<<< HEAD:app/config.py
        channel_video_no_compression_raw = os.getenv("CHANNEL_VIDEO_NO_COMPRESSION", "1").strip().lower()
        channel_text_only_sources_raw = os.getenv("CHANNEL_TEXT_ONLY_SOURCES", "").strip()
=======
        channel_dedup_lookback_raw = os.getenv("CHANNEL_DEDUP_LOOKBACK_LIMIT", "600").strip()
        channel_video_no_compression_raw = os.getenv("CHANNEL_VIDEO_NO_COMPRESSION", "1").strip().lower()
>>>>>>> dc6bde5 (fix(channel): harden dedup and normalize link presentation):01_Работа/01_Sobirai_TG_BOT/app/config.py
        llm_provider = os.getenv("LLM_PROVIDER", "sambanova").strip().lower()
        llm_primary_raw = os.getenv("LLM_PRIMARY_PROVIDER", "").strip().lower()
        llm_primary_provider = llm_primary_raw or llm_provider or "sambanova"
        llm_fallback_provider = os.getenv("LLM_FALLBACK_PROVIDER", "groq").strip().lower()
        llm_fallback_enabled_raw = os.getenv("LLM_FALLBACK_ENABLED", "1").strip().lower()
        sambanova_key = os.getenv("SAMBANOVA_API_KEY", "").strip()
        sambanova_model = os.getenv("SAMBANOVA_MODEL", "Meta-Llama-3.1-8B-Instruct").strip()
        sambanova_api_base = os.getenv("SAMBANOVA_API_BASE", "https://api.sambanova.ai/v1").strip()
        groq_key = os.getenv("GROQ_API_KEY", "").strip()
        llm_model = os.getenv("LLM_MODEL", "llama-3.1-8b-instant").strip()
        llm_timeout_raw = os.getenv("LLM_TIMEOUT_SECONDS", "25").strip()
        llm_max_retries_raw = os.getenv("LLM_MAX_RETRIES", "2").strip()
        llm_max_input_raw = os.getenv("LLM_MAX_INPUT_CHARS", "6000").strip()
        llm_max_out_raw = os.getenv("LLM_MAX_OUTPUT_TOKENS", "500").strip()
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
        enable_x_sources = enable_x_raw in {"1", "true", "yes", "on"}
        if enable_x_sources and not x_bearer_token:
            raise ValueError("X_API_BEARER_TOKEN is required when ENABLE_X_SOURCES is enabled")
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

        enable_channel_autopublish = enable_ch_raw in {"1", "true", "yes", "on"}
        channel_chat_id: Optional[int] = None
        if channel_chat_raw:
            try:
                channel_chat_id = int(channel_chat_raw)
            except ValueError as exc:
                raise ValueError("CHANNEL_CHAT_ID must be an integer like -100...") from exc
        if not channel_max_posts_raw.isdigit() or int(channel_max_posts_raw) < 1:
            raise ValueError("CHANNEL_MAX_POSTS_PER_DAY must be an integer >= 1")
        if not channel_poll_raw.isdigit() or int(channel_poll_raw) < 5:
            raise ValueError("CHANNEL_POLL_SECONDS must be an integer >= 5")
        if not channel_min_text_raw.isdigit() or int(channel_min_text_raw) < 1:
            raise ValueError("CHANNEL_MIN_CANDIDATE_CHARS must be an integer >= 1")
        try:
            channel_near_dup_jaccard = float(channel_near_dup_raw.replace(",", "."))
        except ValueError as exc:
            raise ValueError("CHANNEL_NEAR_DUP_JACCARD must be a float in (0,1]") from exc
        if channel_near_dup_jaccard <= 0 or channel_near_dup_jaccard > 1:
            raise ValueError("CHANNEL_NEAR_DUP_JACCARD must be in (0, 1]")
        if not channel_llm_per_tick_raw.isdigit() or int(channel_llm_per_tick_raw) < 1:
            raise ValueError("CHANNEL_LLM_CANDIDATES_PER_TICK must be an integer >= 1")
        if int(channel_llm_per_tick_raw) > 20:
            raise ValueError("CHANNEL_LLM_CANDIDATES_PER_TICK must be <= 20")
        try:
            channel_llm_gap_seconds = float(channel_llm_gap_raw.replace(",", "."))
        except ValueError as exc:
            raise ValueError("CHANNEL_LLM_GAP_SECONDS must be a number") from exc
        if channel_llm_gap_seconds < 0 or channel_llm_gap_seconds > 300:
            raise ValueError("CHANNEL_LLM_GAP_SECONDS must be in [0, 300]")
<<<<<<< HEAD:app/config.py
        channel_text_only_sources = tuple(
            dict.fromkeys(
                s.strip().lstrip("@").lower()
                for s in channel_text_only_sources_raw.split(",")
                if s.strip()
            )
        )
=======
        if not channel_dedup_lookback_raw.isdigit() or int(channel_dedup_lookback_raw) < 50:
            raise ValueError("CHANNEL_DEDUP_LOOKBACK_LIMIT must be an integer >= 50")
        if int(channel_dedup_lookback_raw) > 5000:
            raise ValueError("CHANNEL_DEDUP_LOOKBACK_LIMIT must be <= 5000")
>>>>>>> dc6bde5 (fix(channel): harden dedup and normalize link presentation):01_Работа/01_Sobirai_TG_BOT/app/config.py

        try:
            llm_timeout_seconds = float(llm_timeout_raw.replace(",", "."))
        except ValueError as exc:
            raise ValueError("LLM_TIMEOUT_SECONDS must be a number >= 5") from exc
        if llm_timeout_seconds < 5:
            raise ValueError("LLM_TIMEOUT_SECONDS must be >= 5")
        if not llm_max_retries_raw.isdigit() or int(llm_max_retries_raw) < 0:
            raise ValueError("LLM_MAX_RETRIES must be an integer >= 0")
        if not llm_max_input_raw.isdigit() or int(llm_max_input_raw) < 500:
            raise ValueError("LLM_MAX_INPUT_CHARS must be an integer >= 500")
        if not llm_max_out_raw.isdigit() or int(llm_max_out_raw) < 64:
            raise ValueError("LLM_MAX_OUTPUT_TOKENS must be an integer >= 64")
        if llm_primary_provider not in {"sambanova", "groq"}:
            raise ValueError("LLM_PRIMARY_PROVIDER must be 'sambanova' or 'groq'")
        if llm_fallback_provider not in {"sambanova", "groq"}:
            raise ValueError("LLM_FALLBACK_PROVIDER must be 'sambanova' or 'groq'")
        llm_fallback_enabled = llm_fallback_enabled_raw in {"1", "true", "yes", "on"}
        if llm_fallback_provider == llm_primary_provider:
            llm_fallback_enabled = False
        if not sambanova_api_base:
            raise ValueError("SAMBANOVA_API_BASE is required")

        if enable_channel_autopublish:
            if channel_chat_id is None:
                raise ValueError("CHANNEL_CHAT_ID is required when ENABLE_CHANNEL_AUTOPUBLISH=1")
            if llm_primary_provider == "sambanova" and not sambanova_key:
                raise ValueError("SAMBANOVA_API_KEY is required when primary provider is sambanova")
            if llm_primary_provider == "groq" and not groq_key:
                raise ValueError("GROQ_API_KEY is required when primary provider is groq")
            if llm_fallback_enabled and llm_fallback_provider == "sambanova" and not sambanova_key:
                raise ValueError("SAMBANOVA_API_KEY is required when fallback provider is sambanova")
            if llm_fallback_enabled and llm_fallback_provider == "groq" and not groq_key:
                raise ValueError("GROQ_API_KEY is required when fallback provider is groq")

        return Settings(
            bot_token=bot_token,
            telegram_api_id=int(api_id_raw),
            telegram_api_hash=api_hash,
            database_path=db_path,
            telethon_session=session_path,
            telethon_session_string=sess_str if sess_str else None,
            enable_channel_autopublish=enable_channel_autopublish,
            channel_chat_id=channel_chat_id,
            channel_max_posts_per_day=int(channel_max_posts_raw),
            channel_poll_seconds=int(channel_poll_raw),
            channel_min_candidate_chars=int(channel_min_text_raw),
            channel_near_dup_jaccard=channel_near_dup_jaccard,
            channel_llm_candidates_per_tick=int(channel_llm_per_tick_raw),
            channel_llm_gap_seconds=channel_llm_gap_seconds,
<<<<<<< HEAD:app/config.py
            channel_video_no_compression=channel_video_no_compression_raw in {"1", "true", "yes", "on"},
            channel_text_only_sources=channel_text_only_sources,
=======
            channel_dedup_lookback_limit=int(channel_dedup_lookback_raw),
            channel_video_no_compression=channel_video_no_compression_raw in {"1", "true", "yes", "on"},
>>>>>>> dc6bde5 (fix(channel): harden dedup and normalize link presentation):01_Работа/01_Sobirai_TG_BOT/app/config.py
            llm_provider=llm_provider,
            llm_primary_provider=llm_primary_provider,
            llm_fallback_provider=llm_fallback_provider,
            llm_fallback_enabled=llm_fallback_enabled,
            sambanova_api_key=sambanova_key,
            sambanova_model=sambanova_model,
            sambanova_api_base=sambanova_api_base,
            groq_api_key=groq_key,
            llm_model=llm_model,
            llm_timeout_seconds=llm_timeout_seconds,
            llm_max_retries=int(llm_max_retries_raw),
            llm_max_input_chars=int(llm_max_input_raw),
            llm_max_output_tokens=int(llm_max_out_raw),
            collector_poll_seconds=int(collector_poll_raw),
            digest_poll_seconds=int(digest_poll_raw),
            enable_x_sources=enable_x_sources,
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
