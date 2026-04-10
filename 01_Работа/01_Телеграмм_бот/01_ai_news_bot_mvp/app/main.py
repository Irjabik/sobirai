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
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from telethon import TelegramClient
from telethon.sessions import StringSession

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
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    telethon_client: TelegramClient | None = None
    _session_file: Path | None = None
    _pre_size = 0
    _tc: TelegramClient

    if settings.telethon_session_string:
        logger.info(
            "Telethon: режим TELETHON_SESSION_STRING (файл .session на сервере не нужен), длина=%s",
            len(settings.telethon_session_string),
        )
        _tc = TelegramClient(
            StringSession(settings.telethon_session_string),
            settings.telegram_api_id,
            settings.telegram_api_hash,
        )
    else:
        telethon_session = str(settings.telethon_session)
        _tbase = Path(telethon_session)
        _session_file = (
            (_tbase if _tbase.is_absolute() else Path.cwd() / _tbase)
            .resolve()
            .with_suffix(".session")
        )
        _pre_exists = _session_file.exists()
        _pre_size = _session_file.stat().st_size if _pre_exists else 0
        logger.info(
            "Telethon: cwd=%s session_prefix=%s ожидаемый_файл=%s до_connect_exists=%s до_connect_size=%s",
            Path.cwd(),
            telethon_session,
            _session_file,
            _pre_exists,
            _pre_size,
        )
        _tc = TelegramClient(
            telethon_session,
            settings.telegram_api_id,
            settings.telegram_api_hash,
        )

    try:
        await _tc.connect()
        if _session_file is not None:
            _post_size = _session_file.stat().st_size if _session_file.exists() else 0
        else:
            _post_size = 0
        authorized = await _tc.is_user_authorized()
        logger.info(
            "Telethon: после connect post_file_size=%s (pre_file=%s) authorized=%s string_env=%s",
            _post_size,
            _pre_size,
            authorized,
            settings.telethon_session_string is not None,
        )
        if authorized:
            telethon_client = _tc
            logger.info("Telethon session OK, collector enabled.")
        else:
            if settings.telethon_session_string:
                logger.warning(
                    "Telethon: TELETHON_SESSION_STRING не прошла проверку — сгенерируйте строку заново "
                    "скриптом scripts/export_telethon_string_session.py (локально, с теми же API_ID/HASH) "
                    "и обновите переменную на хостинге."
                )
            elif _session_file is not None and _pre_size == 0 and _post_size > 0:
                logger.warning(
                    "Telethon: файла не было; клиент создал новую сессию (%s байт) без входа. "
                    "На PaaS файл из менеджера часто недоступен процессу — задайте TELETHON_SESSION_STRING "
                    "или загрузите .session в персистентный каталог. Путь: %s",
                    _post_size,
                    _session_file,
                )
            elif _session_file is not None and _pre_size > 0:
                logger.warning(
                    "Telethon: файл был (%s байт), но вход не принят — API_ID/HASH или битый файл. %s",
                    _pre_size,
                    _session_file,
                )
            elif _session_file is not None:
                logger.warning(
                    "Telethon: нет рабочей сессии (%s). Задайте TELETHON_SESSION_STRING (см. скрипт в scripts/).",
                    _session_file,
                )
            else:
                logger.warning("Telethon: сессия не авторизована.")
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
            run_collector_loop(
                telethon_client,
                db,
                bot,
                metrics,
                media_dir,
                stop_event,
                poll_seconds=settings.collector_poll_seconds,
            )
        )
    digest_task = asyncio.create_task(
        run_configurable_digest_loop(
            db,
            bot,
            metrics,
            stop_event,
            poll_seconds=settings.digest_poll_seconds,
        )
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

