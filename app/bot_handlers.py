from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message, ReplyKeyboardRemove

from .config import DELIVERY_MODES
from .db import Database
from .formatting import deduplicate_digest_posts, render_digest_list
from .sources import CATEGORY_KEYS, KEY_TO_CATEGORY, grouped_sources

router = Router()


def settings_keyboard() -> str:
    return (
        "Настройки:\n"
        "- /mute_on\n"
        "- /mute_off\n"
        "- /mode_instant\n"
        "- /digest 12 (включить авто-дайджест каждые 12 часов)\n"
        "- /digest (прислать дайджест прямо сейчас)"
    )


@router.message(Command("start"))
async def cmd_start(message: Message, db: Database) -> None:
    user = message.from_user
    if user is None:
        return
    await db.upsert_user(user.id, user.username, user.first_name)
    await message.answer(
        "Привет! Я Sobirai — бот-парсер новостей из каналов про ИИ.\n"
        "\n"
        "Нажми /help, чтобы увидеть все функции и команды.",
        reply_markup=ReplyKeyboardRemove(),
    )


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    await message.answer(
        "Доступные команды:\n"
        "/start — начать\n"
        "/help — помощь\n"
        "/sources — список каналов\n"
        "/categories — категории и их статус\n"
        "/my_filters — мои фильтры\n"
        "/block_category &lt;новости|технические|авторские|креативные&gt;\n"
        "/unblock_category &lt;новости|технические|авторские|креативные&gt;\n"
        "/block_channel @username — исключить канал\n"
        "/unblock_channel @username — вернуть канал\n"
        "/digest — собрать свежий дайджест сейчас\n"
        "/digest &lt;часы&gt; — включить авто-дайджест с интервалом (1-168)\n"
        "/digest_filter_off — отключить фильтр по окну часов\n"
        "/digest_filter_on — включить фильтр по окну часов\n"
        "/pause — пауза уведомлений\n"
        "/resume — возобновить уведомления\n"
        "/health — базовая диагностика\n"
        "/mute_on и /mute_off — выключить/включить уведомления\n"
        "/mode_instant — мгновенно"
    )


@router.message(Command("sources"))
async def cmd_sources(message: Message) -> None:
    grouped = grouped_sources()
    lines: list[str] = ["Источники MVP:"]
    for category in ("Новости", "Технические", "Авторские", "Креативные"):
        lines.append(f"\n<b>{category}</b>")
        for channel in grouped.get(category, []):
            lines.append(f"• {channel}")
    await message.answer("\n".join(lines))


@router.message(Command("pause"))
async def cmd_pause(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    await db.set_pause(message.from_user.id, True)
    await message.answer("Уведомления поставлены на паузу. Вернуть: /resume")


@router.message(Command("resume"))
async def cmd_resume(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    await db.set_pause(message.from_user.id, False)
    await message.answer("Уведомления возобновлены.")


@router.message(Command("mute_on"))
async def cmd_mute_on(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    await db.set_mute_all(message.from_user.id, True)
    await message.answer("Mute включен. Вы не будете получать новые уведомления.")


@router.message(Command("mute_off"))
async def cmd_mute_off(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    await db.set_mute_all(message.from_user.id, False)
    await message.answer("Mute выключен. Уведомления включены.")


@router.message(Command("mode_instant"))
async def cmd_mode_instant(message: Message, db: Database) -> None:
    await _set_mode(message, db, "instant")


@router.message(Command("categories"))
async def cmd_categories(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    blocks = await db.get_category_blocks(message.from_user.id)
    lines = ["Категории (по умолчанию все включены):"]
    for key in ("news", "tech", "author", "creative"):
        name = KEY_TO_CATEGORY[key]
        status = "исключена" if blocks.get(key, False) else "включена"
        lines.append(f"• {name}: {status}")
    await message.answer("\n".join(lines))


@router.message(Command("my_filters"))
async def cmd_my_filters(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    user_id = message.from_user.id
    blocks = await db.get_category_blocks(user_id)
    blocked_channels = await db.list_blocked_channels(user_id)
    blocked_categories = [KEY_TO_CATEGORY[k] for k, v in blocks.items() if v]

    lines = ["Текущие фильтры:"]
    if blocked_categories:
        lines.append("Категории-исключения: " + ", ".join(blocked_categories))
    else:
        lines.append("Категории-исключения: нет")
    if blocked_channels:
        lines.append("Каналы-исключения:")
        lines.extend(f"• {c}" for c in blocked_channels)
    else:
        lines.append("Каналы-исключения: нет")
    await message.answer("\n".join(lines))


@router.message(Command("block_category"))
async def cmd_block_category(message: Message, db: Database) -> None:
    await _set_category_block(message, db, blocked=True)


@router.message(Command("unblock_category"))
async def cmd_unblock_category(message: Message, db: Database) -> None:
    await _set_category_block(message, db, blocked=False)


@router.message(Command("block_channel"))
async def cmd_block_channel(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    username = _extract_arg(message.text)
    if not username:
        await message.answer("Использование: /block_channel @username")
        return
    ok = await db.block_channel(message.from_user.id, username)
    if not ok:
        await message.answer("Канал не найден в списке источников.")
        return
    await message.answer(f"Канал {username} исключён из вашей выдачи.")


@router.message(Command("unblock_channel"))
async def cmd_unblock_channel(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    username = _extract_arg(message.text)
    if not username:
        await message.answer("Использование: /unblock_channel @username")
        return
    await db.unblock_channel(message.from_user.id, username)
    await message.answer(f"Канал {username} снова включён в вашу выдачу.")


@router.message(Command("health"))
async def cmd_health(message: Message, db: Database) -> None:
    stats = await db.health_stats()
    await message.answer(
        "Health snapshot:\n"
        f"users={stats.get('users_count', 0)}\n"
        f"posts={stats.get('source_posts_count', 0)}\n"
        f"delivery={stats.get('delivery_events_count', 0)}\n"
        f"status={stats.get('delivery_status', {})}"
    )


@router.message(Command("digest"))
async def cmd_digest(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    arg = _extract_arg(message.text)
    if arg:
        try:
            hours = int(arg)
        except ValueError:
            await message.answer("Использование: /digest &lt;часы&gt;, где часы — число от 1 до 168.")
            return
        if hours < 1 or hours > 168:
            await message.answer("Интервал должен быть от 1 до 168 часов.")
            return
        await db.set_digest_interval_hours(message.from_user.id, hours)
        await message.answer(
            f"Авто-дайджест включен: каждые {hours} ч.\n"
            "Чтобы вернуться к мгновенным уведомлениям: /mode_instant"
        )
        return
    await _send_digest(message, db)


@router.message(Command("digest_filter_off"))
async def cmd_digest_filter_off(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    await db.set_digest_filter_enabled(message.from_user.id, False)
    await message.answer("Фильтр времени для дайджеста отключен. Будут показываться последние посты без ограничения по часам.")


@router.message(Command("digest_filter_on"))
async def cmd_digest_filter_on(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    await db.set_digest_filter_enabled(message.from_user.id, True)
    await message.answer("Фильтр времени для дайджеста включен.")


async def _set_mode(message: Message, db: Database, mode: str) -> None:
    if message.from_user is None:
        return
    if mode not in DELIVERY_MODES:
        await message.answer("Неизвестный режим.")
        return
    await db.set_delivery_mode(message.from_user.id, mode)
    await message.answer(
        f"Режим уведомлений установлен: {mode}",
        reply_markup=ReplyKeyboardRemove(),
    )


async def _set_category_block(message: Message, db: Database, blocked: bool) -> None:
    if message.from_user is None:
        return
    value = _extract_arg(message.text)
    if not value:
        await message.answer(
            "Укажи категорию: новости, технические, авторские, креативные.\n"
            "Пример: /block_category новости"
        )
        return
    key = CATEGORY_KEYS.get(value.lower())
    if key is None:
        await message.answer("Неизвестная категория.")
        return
    await db.set_category_block(message.from_user.id, key, blocked)
    state = "исключена" if blocked else "включена"
    await message.answer(f"Категория '{value}' {state}.")


def _extract_arg(text: str | None) -> str | None:
    if not text:
        return None
    parts = text.strip().split(maxsplit=1)
    if len(parts) < 2:
        return None
    value = parts[1].strip()
    return value if value else None


async def _send_digest(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    user_id = message.from_user.id
    status = await db.get_user_status(user_id)
    hours_window = int(status.get("digest_interval_hours", 12)) if status else 12
    filter_enabled = bool(status.get("digest_filter_enabled", 1)) if status else True
    if filter_enabled:
        posts = await db.latest_posts_for_user_window(
            user_id=user_id, hours_window=hours_window, limit=300
        )
    else:
        posts = await db.latest_posts_for_user_unfiltered(user_id=user_id, limit=300)
    posts = deduplicate_digest_posts(posts, limit=10)
    if not posts:
        if filter_enabled:
            await message.answer(
                f"Пока нет новых постов за последние {hours_window} часов по вашим фильтрам.",
                reply_markup=ReplyKeyboardRemove(),
            )
        else:
            await message.answer(
                "Пока нет постов по вашим фильтрам.",
                reply_markup=ReplyKeyboardRemove(),
            )
        return

    label_hours = hours_window if filter_enabled else 0
    digest_text = render_digest_list(posts, hours_window=label_hours)
    await message.answer(
        digest_text,
        disable_web_page_preview=True,
        reply_markup=ReplyKeyboardRemove(),
    )

