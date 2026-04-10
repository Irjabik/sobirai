from __future__ import annotations

import asyncio
import logging
import sys
from contextlib import suppress
from pathlib import Path

# Хостинги вроде Bothost часто стартуют `python app/main.py` — без пакета ломаются `from .foo`.
if __name__ == "__main__" and __package__ is None:
    _pkg_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(_pkg_dir.parent))
    __package__ = _pkg_dir.name

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from telethon import TelegramClient

from .bot_handlers import router
from .config import Settings
from .db import Database
from .metrics import RuntimeMetrics
from .service import run_collector_loop, run_configurable_digest_loop

logger = logging.getLogger(__name__)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


async def start() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    logger.info("Sobirai: старт, загрузка конфигурации ок")

    db = Database(settings.database_path)
    await db.connect()
    logger.info("Sobirai: SQLite подключена (%s)", settings.database_path)

    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(router)

    telethon_session = str(settings.telethon_session)
    _tbase = Path(telethon_session)
    _session_file = (
        (_tbase if _tbase.is_absolute() else Path.cwd() / _tbase).resolve().with_suffix(
            ".session"
        )
    )
    logger.info(
        "Telethon: cwd=%s session_prefix=%s ожидаемый_файл=%s exists=%s size=%s",
        Path.cwd(),
        telethon_session,
        _session_file,
        _session_file.exists(),
        _session_file.stat().st_size if _session_file.exists() else 0,
    )

    telethon_client: TelegramClient | None = None
    _tc = TelegramClient(
        telethon_session, settings.telegram_api_id, settings.telegram_api_hash
    )
    try:
        await _tc.connect()
        if await _tc.is_user_authorized():
            telethon_client = _tc
            logger.info("Telethon session OK, collector enabled.")
        else:
            if _session_file.exists() and _session_file.stat().st_size > 0:
                logger.warning(
                    "Telethon: файл сессии есть (%s), но пользователь не авторизован. "
                    "Частая причина — на хостинге другие TELEGRAM_API_ID / TELEGRAM_API_HASH, "
                    "чем при создании .session локально (должны совпадать с my.telegram.org).",
                    _session_file,
                )
            else:
                logger.warning(
                    "Telethon: нет файла сессии по пути %s (или пустой). "
                    "Загрузите telethon_session.session в каталог data относительно рабочей директории "
                    "процесса (см. cwd выше) либо задайте TELETHON_SESSION абсолютным путём.",
                    _session_file,
                )
            logger.warning(
                "Сбор из каналов отключён; команды бота через Bot API работают."
            )
            await _tc.disconnect()
    except Exception:
        logger.exception("Telethon: ошибка подключения, коллектор отключён.")
        with suppress(Exception):
            await _tc.disconnect()

    metrics = RuntimeMetrics()
    stop_event = asyncio.Event()
    media_dir = Path("./data/media")
    media_dir.mkdir(parents=True, exist_ok=True)

    collector_task: asyncio.Task[None] | None = None
    if telethon_client is not None:
        collector_task = asyncio.create_task(
            run_collector_loop(telethon_client, db, bot, metrics, media_dir, stop_event)
        )
    digest_task = asyncio.create_task(
        run_configurable_digest_loop(db, bot, metrics, stop_event)
    )

    try:
        logger.info("Sobirai: запуск long polling Bot API…")
        await dp.start_polling(bot, db=db)
    finally:
        stop_event.set()
        to_join: list[asyncio.Task[None]] = [digest_task]
        if collector_task is not None:
            to_join.append(collector_task)
        for task in to_join:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        await db.close()
        if telethon_client is not None:
            await telethon_client.disconnect()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(start())
