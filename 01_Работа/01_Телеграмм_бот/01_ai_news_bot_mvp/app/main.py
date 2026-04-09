from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from telethon import TelegramClient

from .bot_handlers import router
from .config import Settings
from .db import Database
from .metrics import RuntimeMetrics
from .service import run_collector_loop, run_configurable_digest_loop


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


async def start() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level)

    db = Database(settings.database_path)
    await db.connect()

    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(router)

    telethon_session = str(settings.telethon_session)
    telethon_client = TelegramClient(
        telethon_session, settings.telegram_api_id, settings.telegram_api_hash
    )
    await telethon_client.start()

    metrics = RuntimeMetrics()
    stop_event = asyncio.Event()
    media_dir = Path("./data/media")
    media_dir.mkdir(parents=True, exist_ok=True)

    collector_task = asyncio.create_task(
        run_collector_loop(telethon_client, db, bot, metrics, media_dir, stop_event)
    )
    digest_task = asyncio.create_task(
        run_configurable_digest_loop(db, bot, metrics, stop_event)
    )

    try:
        await dp.start_polling(bot, db=db)
    finally:
        stop_event.set()
        for task in (collector_task, digest_task):
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        await db.close()
        await telethon_client.disconnect()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(start())

