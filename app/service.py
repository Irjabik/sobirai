from __future__ import annotations

import asyncio
import logging
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
) -> None:
    while not stop_event.is_set():
        try:
            await collect_new_posts(client, db, metrics, media_dir)
            await deliver_mode(bot, db, metrics, "instant")
        except Exception:
            logger.exception("Collector loop failure")
        await asyncio.sleep(10)


async def run_configurable_digest_loop(
    db: Database,
    bot: Bot,
    metrics: RuntimeMetrics,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        try:
            await deliver_configurable_digests(bot, db, metrics)
        except Exception:
            logger.exception("Configurable digest loop failure")
        await asyncio.sleep(60)

