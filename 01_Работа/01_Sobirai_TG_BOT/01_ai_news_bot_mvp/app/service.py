from __future__ import annotations

import asyncio
import logging
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from aiogram import Bot
from telethon import TelegramClient

from .collector import collect_new_posts
from .db import Database
from .delivery import deliver_configurable_digests, deliver_mode
from .metrics import RuntimeMetrics

logger = logging.getLogger(__name__)


async def run_collector_loop(
    client: TelegramClient,
    db: Database,
    bot: Bot,
    metrics: RuntimeMetrics,
    media_dir: Path,
    stop_event: asyncio.Event,
    poll_seconds: int = 3,
    enable_x_sources: bool = True,
    x_api_bearer_token: str = "",
    x_api_base_url: str = "https://api.x.com/2",
    x_api_fetch_interval_seconds: int = 60,
    x_fetch_timeout_seconds: int = 25,
    enable_media_downloads: bool = True,
    min_free_disk_mb: int = 512,
    media_retention_days: int = 3,
) -> None:
    last_media_cleanup_at: datetime | None = None
    while not stop_event.is_set():
        try:
            if not client.is_connected():
                logger.warning("Telethon disconnected, trying reconnect")
                await client.connect()
            if not await client.is_user_authorized():
                logger.error("Telethon session is not authorized; collector tick skipped")
                await asyncio.sleep(max(1, poll_seconds))
                continue
            now = datetime.now(tz=timezone.utc)
            if last_media_cleanup_at is None or (now - last_media_cleanup_at) >= timedelta(hours=1):
                removed_files, removed_bytes = _cleanup_old_media(media_dir, media_retention_days)
                if removed_files > 0:
                    logger.info(
                        "Media cleanup done: removed_files=%s removed_mb=%.2f",
                        removed_files,
                        removed_bytes / (1024 * 1024),
                    )
                last_media_cleanup_at = now
            free_bytes = shutil.disk_usage(media_dir).free
            free_mb = free_bytes // (1024 * 1024)
            allow_media = enable_media_downloads and free_mb >= max(64, min_free_disk_mb)
            if enable_media_downloads and not allow_media:
                logger.warning(
                    "Low free disk space (%s MB). Media download disabled for this tick",
                    free_mb,
                )
            await collect_new_posts(
                client,
                db,
                metrics,
                media_dir,
                enable_x_sources=enable_x_sources,
                x_api_bearer_token=x_api_bearer_token,
                x_api_base_url=x_api_base_url,
                x_api_fetch_interval_seconds=x_api_fetch_interval_seconds,
                x_fetch_timeout_seconds=x_fetch_timeout_seconds,
                media_download_enabled=allow_media,
            )
            await deliver_mode(bot, db, metrics, "instant")
        except ConnectionError:
            logger.exception("Collector connection error, forcing Telethon reconnect")
            try:
                await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(2)
            try:
                await client.connect()
            except Exception:
                logger.exception("Telethon reconnect failed")
        except Exception:
            logger.exception("Collector loop failure")
        await asyncio.sleep(max(1, poll_seconds))


def _cleanup_old_media(media_dir: Path, retention_days: int) -> tuple[int, int]:
    if retention_days < 1:
        return (0, 0)
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=retention_days)
    removed_files = 0
    removed_bytes = 0
    for item in media_dir.glob("*"):
        try:
            if not item.is_file():
                continue
            mtime = datetime.fromtimestamp(item.stat().st_mtime, tz=timezone.utc)
            if mtime < cutoff:
                size = item.stat().st_size
                item.unlink(missing_ok=True)
                removed_files += 1
                removed_bytes += size
        except FileNotFoundError:
            continue
        except Exception:
            logger.warning("Failed to cleanup media file: %s", item, exc_info=True)
    return (removed_files, removed_bytes)


async def run_configurable_digest_loop(
    db: Database,
    bot: Bot,
    metrics: RuntimeMetrics,
    stop_event: asyncio.Event,
    poll_seconds: int = 60,
) -> None:
    while not stop_event.is_set():
        try:
            await deliver_configurable_digests(bot, db, metrics)
        except Exception:
            logger.exception("Configurable digest loop failure")
        await asyncio.sleep(max(5, poll_seconds))

