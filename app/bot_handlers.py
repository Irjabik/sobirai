from __future__ import annotations

from typing import Optional

from aiogram import F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

from .config import DELIVERY_MODES
from .db import Database
from .formatting import (
    deduplicate_digest_posts,
    format_digest_interval_ru,
    format_hours_window_ru,
    render_digest_list,
)
from .keyboards import (
    BTN_CANCEL,
    BTN_DIGEST,
    BTN_FILTERS,
    BTN_MODES,
    BTN_SOURCES_HELP,
    CHANNELS_PER_PAGE,
    MAIN_MENU_LABELS,
    cancel_reply,
    channel_picker_indices,
    inline_channel_page,
    inline_digest,
    inline_filters_menu,
    inline_modes,
    inline_sources_help,
    main_menu_reply,
)
from .sources import CATEGORY_KEYS, KEY_TO_CATEGORY, SOURCES, grouped_sources

router = Router()

# Публичные команды (без /health): один текст для /start, /help и inline «Помощь».
PUBLIC_COMMANDS_TEXT = (
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
    "/digest &lt;часы&gt; — авто-дайджест: интервал 1–168 ч (до 7 суток)\n"
    "/digest_filter_off — отключить фильтр по окну часов\n"
    "/digest_filter_on — включить фильтр по окну часов\n"
    "/pause — пауза уведомлений\n"
    "/resume — возобновить уведомления\n"
    "/mute_on и /mute_off — выключить/включить уведомления\n"
    "/mode_instant — мгновенно"
)


class MenuStates(StatesGroup):
    waiting_digest_hours = State()
    waiting_channel_block = State()
    waiting_channel_unblock = State()


def _blocked_indices_from_channels(blocked_channels: list[str]) -> set[int]:
    blocked_lower = {c.strip().lower() for c in blocked_channels}
    out: set[int] = set()
    for i, src in enumerate(SOURCES):
        if src.username.lower() in blocked_lower:
            out.add(i)
    return out


async def _answer(
    message: Message | None,
    query: CallbackQuery | None,
    text: str,
    *,
    reply_markup: Optional[object] = None,
    disable_web_page_preview: bool | None = None,
) -> None:
    kb = main_menu_reply() if reply_markup is None else reply_markup
    kw: dict = {}
    if kb is not None:
        kw["reply_markup"] = kb
    if disable_web_page_preview is not None:
        kw["disable_web_page_preview"] = disable_web_page_preview
    if message is not None:
        await message.answer(text, **kw)
    elif query is not None:
        if query.message is not None:
            await query.message.answer(text, **kw)
        elif query.from_user is not None:
            await query.bot.send_message(query.from_user.id, text, **kw)


async def deliver_digest(
    db: Database,
    user_id: int,
    message: Message | None,
    query: CallbackQuery | None,
) -> None:
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
            win = format_hours_window_ru(hours_window)
            await _answer(
                message,
                query,
                f"Пока нет новых постов по вашим фильтрам за последние {win}.",
            )
        else:
            await _answer(
                message,
                query,
                "Пока нет постов по вашим фильтрам.",
            )
        return

    label_hours = hours_window if filter_enabled else 0
    digest_text = render_digest_list(posts, hours_window=label_hours)
    await _answer(
        message,
        query,
        digest_text,
        disable_web_page_preview=True,
    )


async def present_main_menu_choice(message: Message, db: Database) -> None:
    if message.from_user is None or message.text is None:
        return
    text = message.text
    if text == BTN_MODES:
        await message.answer("Режимы уведомлений:", reply_markup=inline_modes())
    elif text == BTN_DIGEST:
        await message.answer(
            "Дайджест: авто-отправка раз в интервал от 1 часа до 7 суток.\n"
            "Ниже — быстрый выбор (часы и дни) или «Свой интервал» — любое число часов 1–168.",
            reply_markup=inline_digest(),
        )
    elif text == BTN_FILTERS:
        blocks = await db.get_category_blocks(message.from_user.id)
        await message.answer("Фильтры:", reply_markup=inline_filters_menu(blocks))
    elif text == BTN_SOURCES_HELP:
        await message.answer("Источники и помощь:", reply_markup=inline_sources_help())


@router.message(Command("start"))
async def cmd_start(message: Message, db: Database, state: FSMContext) -> None:
    await state.clear()
    user = message.from_user
    if user is None:
        return
    await db.upsert_user(user.id, user.username, user.first_name)
    await message.answer(
        "Привет! Я Sobirai — бот-парсер новостей из каналов про ИИ.\n\n"
        "Снизу четыре кнопки меню — откройте нужный раздел.\n\n"
        "<b>Все доступные команды:</b>\n"
        f"{PUBLIC_COMMANDS_TEXT}",
        reply_markup=main_menu_reply(),
    )


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    await message.answer(
        "Главное меню — кнопки внизу чата (Режимы, Дайджест, Фильтры, Источники и помощь).\n\n"
        "<b>Все доступные команды:</b>\n"
        f"{PUBLIC_COMMANDS_TEXT}",
        reply_markup=main_menu_reply(),
    )


@router.message(Command("sources"))
async def cmd_sources(message: Message) -> None:
    grouped = grouped_sources()
    lines: list[str] = ["Источники MVP:"]
    for category in ("Новости", "Технические", "Авторские", "Креативные"):
        lines.append(f"\n<b>{category}</b>")
        for channel in grouped.get(category, []):
            lines.append(f"• {channel}")
    await message.answer("\n".join(lines), reply_markup=main_menu_reply())


@router.message(Command("pause"))
async def cmd_pause(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    await db.set_pause(message.from_user.id, True)
    await message.answer(
        "Уведомления поставлены на паузу. Вернуть: /resume",
        reply_markup=main_menu_reply(),
    )


@router.message(Command("resume"))
async def cmd_resume(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    await db.set_pause(message.from_user.id, False)
    await message.answer("Уведомления возобновлены.", reply_markup=main_menu_reply())


@router.message(Command("mute_on"))
async def cmd_mute_on(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    await db.set_mute_all(message.from_user.id, True)
    await message.answer(
        "Mute включен. Вы не будете получать новые уведомления.",
        reply_markup=main_menu_reply(),
    )


@router.message(Command("mute_off"))
async def cmd_mute_off(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    await db.set_mute_all(message.from_user.id, False)
    await message.answer(
        "Mute выключен. Уведомления включены.",
        reply_markup=main_menu_reply(),
    )


@router.message(Command("mode_instant"))
async def cmd_mode_instant(message: Message, db: Database) -> None:
    await _set_mode(message, None, db, "instant")


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
    await message.answer("\n".join(lines), reply_markup=main_menu_reply())


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
    await message.answer("\n".join(lines), reply_markup=main_menu_reply())


@router.message(Command("block_category"))
async def cmd_block_category(message: Message, db: Database) -> None:
    await _set_category_block(message, None, db, blocked=True)


@router.message(Command("unblock_category"))
async def cmd_unblock_category(message: Message, db: Database) -> None:
    await _set_category_block(message, None, db, blocked=False)


@router.message(Command("block_channel"))
async def cmd_block_channel(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    username = _extract_arg(message.text)
    if not username:
        await message.answer(
            "Использование: /block_channel @username",
            reply_markup=main_menu_reply(),
        )
        return
    ok = await db.block_channel(message.from_user.id, username)
    if not ok:
        await message.answer(
            "Канал не найден в списке источников.",
            reply_markup=main_menu_reply(),
        )
        return
    await message.answer(
        f"Канал {username} исключён из вашей выдачи.",
        reply_markup=main_menu_reply(),
    )


@router.message(Command("unblock_channel"))
async def cmd_unblock_channel(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    username = _extract_arg(message.text)
    if not username:
        await message.answer(
            "Использование: /unblock_channel @username",
            reply_markup=main_menu_reply(),
        )
        return
    await db.unblock_channel(message.from_user.id, username)
    await message.answer(
        f"Канал {username} снова включён в вашу выдачу.",
        reply_markup=main_menu_reply(),
    )


@router.message(Command("health"))
async def cmd_health(message: Message, db: Database) -> None:
    stats = await db.health_stats()
    await message.answer(
        "Health snapshot:\n"
        f"users={stats.get('users_count', 0)}\n"
        f"posts={stats.get('source_posts_count', 0)}\n"
        f"delivery={stats.get('delivery_events_count', 0)}\n"
        f"status={stats.get('delivery_status', {})}",
        reply_markup=main_menu_reply(),
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
            await message.answer(
                "Использование: /digest &lt;часы&gt; — целое число часов от 1 до 168 (до 7 суток).",
                reply_markup=main_menu_reply(),
            )
            return
        if hours < 1 or hours > 168:
            await message.answer(
                "Интервал от 1 часа до 168 ч (7 суток).",
                reply_markup=main_menu_reply(),
            )
            return
        await db.set_digest_interval_hours(message.from_user.id, hours)
        human = format_digest_interval_ru(hours)
        await message.answer(
            f"Авто-дайджест включён: примерно раз в {human}.\n"
            "Мгновенный режим: «Режимы» → «Мгновенно» или /mode_instant.",
            reply_markup=main_menu_reply(),
        )
        return
    await deliver_digest(db, message.from_user.id, message, None)


@router.message(Command("digest_filter_off"))
async def cmd_digest_filter_off(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    await db.set_digest_filter_enabled(message.from_user.id, False)
    await message.answer(
        "Фильтр времени для дайджеста отключен. Будут показываться последние посты без ограничения по часам.",
        reply_markup=main_menu_reply(),
    )


@router.message(Command("digest_filter_on"))
async def cmd_digest_filter_on(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    await db.set_digest_filter_enabled(message.from_user.id, True)
    await message.answer(
        "Фильтр времени для дайджеста включен.",
        reply_markup=main_menu_reply(),
    )


@router.message(F.text == BTN_CANCEL, StateFilter(MenuStates))
async def cmd_cancel_fsm(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Ок.", reply_markup=main_menu_reply())


@router.message(StateFilter(MenuStates.waiting_digest_hours))
async def fsm_digest_hours(message: Message, db: Database, state: FSMContext) -> None:
    if message.from_user is None or not message.text:
        return
    if message.text in MAIN_MENU_LABELS:
        await state.clear()
        await present_main_menu_choice(message, db)
        return
    raw = message.text.strip()
    try:
        hours = int(raw)
    except ValueError:
        await message.answer(
            "Нужно целое число часов от 1 до 168 (до 7 суток). Повторите или «Отмена».",
            reply_markup=cancel_reply(),
        )
        return
    if hours < 1 or hours > 168:
        await message.answer(
            "Интервал от 1 часа до 168 ч. Повторите или «Отмена».",
            reply_markup=cancel_reply(),
        )
        return
    await db.set_digest_interval_hours(message.from_user.id, hours)
    await state.clear()
    human = format_digest_interval_ru(hours)
    await message.answer(
        f"Авто-дайджест включён: примерно раз в {human}.\n"
        "Мгновенный режим: «Режимы» → «Мгновенно».",
        reply_markup=main_menu_reply(),
    )


@router.message(StateFilter(MenuStates.waiting_channel_block))
async def fsm_channel_block(message: Message, db: Database, state: FSMContext) -> None:
    if message.from_user is None or not message.text:
        return
    if message.text in MAIN_MENU_LABELS:
        await state.clear()
        await present_main_menu_choice(message, db)
        return
    username = message.text.strip()
    ok = await db.block_channel(message.from_user.id, username)
    await state.clear()
    if not ok:
        await message.answer(
            "Канал не найден в списке источников. Проверьте @username.",
            reply_markup=main_menu_reply(),
        )
        return
    await message.answer(
        f"Канал {username.strip()} исключён из вашей выдачи.",
        reply_markup=main_menu_reply(),
    )


@router.message(StateFilter(MenuStates.waiting_channel_unblock))
async def fsm_channel_unblock(message: Message, db: Database, state: FSMContext) -> None:
    if message.from_user is None or not message.text:
        return
    if message.text in MAIN_MENU_LABELS:
        await state.clear()
        await present_main_menu_choice(message, db)
        return
    username = message.text.strip()
    await db.unblock_channel(message.from_user.id, username)
    await state.clear()
    await message.answer(
        f"Канал {username.strip()} снова включён в вашу выдачу.",
        reply_markup=main_menu_reply(),
    )


@router.message(F.text.in_(MAIN_MENU_LABELS))
async def main_menu_text(message: Message, db: Database, state: FSMContext) -> None:
    await state.clear()
    await present_main_menu_choice(message, db)


@router.callback_query(F.data.startswith("rg:"))
async def cb_modes(query: CallbackQuery, db: Database) -> None:
    if query.from_user is None or query.data is None:
        return
    uid = query.from_user.id
    d = query.data
    if d == "rg:i":
        await db.set_delivery_mode(uid, "instant")
        await query.answer("Режим: мгновенно")
        await _answer(None, query, "Режим уведомлений установлен: instant")
    elif d == "rg:p":
        await db.set_pause(uid, True)
        await query.answer("Пауза")
        await _answer(None, query, "Уведомления поставлены на паузу. Вернуть: /resume")
    elif d == "rg:r":
        await db.set_pause(uid, False)
        await query.answer("Возобновлено")
        await _answer(None, query, "Уведомления возобновлены.")
    elif d == "rg:m1":
        await db.set_mute_all(uid, True)
        await query.answer("Mute вкл")
        await _answer(None, query, "Mute включен. Вы не будете получать новые уведомления.")
    elif d == "rg:m0":
        await db.set_mute_all(uid, False)
        await query.answer("Mute выкл")
        await _answer(None, query, "Mute выключен. Уведомления включены.")
    else:
        await query.answer()


@router.callback_query(F.data.startswith("dg:"))
async def cb_digest(query: CallbackQuery, db: Database, state: FSMContext) -> None:
    if query.from_user is None or query.data is None:
        return
    uid = query.from_user.id
    d = query.data
    if d == "dg:n":
        await query.answer("Собираю…")
        await deliver_digest(db, uid, None, query)
        return
    if d == "dg:ask":
        await state.set_state(MenuStates.waiting_digest_hours)
        await query.answer()
        if query.message is not None:
            await query.message.answer(
                "Свой интервал: введите целое число часов от 1 до 168 (это до 7 суток).\n"
                "«Отмена» — выход.",
                reply_markup=cancel_reply(),
            )
        return
    if d == "dg:fo":
        await db.set_digest_filter_enabled(uid, False)
        await query.answer("Фильтр выкл")
        await _answer(
            None,
            query,
            "Фильтр времени для дайджеста отключен. Будут показываться последние посты без ограничения по часам.",
        )
        return
    if d == "dg:fn":
        await db.set_digest_filter_enabled(uid, True)
        await query.answer("Фильтр вкл")
        await _answer(None, query, "Фильтр времени для дайджеста включен.")
        return
    if d.startswith("dg:h:"):
        try:
            hours = int(d.split(":", 2)[2])
        except (ValueError, IndexError):
            await query.answer("Ошибка")
            return
        if hours < 1 or hours > 168:
            await query.answer("Неверный интервал")
            return
        await db.set_digest_interval_hours(uid, hours)
        human = format_digest_interval_ru(hours)
        await query.answer(f"Интервал: {human}")
        await _answer(
            None,
            query,
            f"Авто-дайджест включён: примерно раз в {human}.\n"
            "Мгновенный режим: «Режимы» → «Мгновенно».",
        )
        return
    await query.answer()


@router.callback_query(F.data.startswith("fc:"))
async def cb_filters(query: CallbackQuery, db: Database, state: FSMContext) -> None:
    if query.from_user is None or query.data is None:
        return
    uid = query.from_user.id
    parts = query.data.split(":")
    blocked_list = await db.list_blocked_channels(uid)
    blocked_idx = _blocked_indices_from_channels(blocked_list)

    if len(parts) == 2:
        if parts[1] == "mf":
            blocks = await db.get_category_blocks(uid)
            blocked_channels = await db.list_blocked_channels(uid)
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
            await query.answer()
            await _answer(None, query, "\n".join(lines))
            return
        if parts[1] == "bc":
            await state.set_state(MenuStates.waiting_channel_block)
            await query.answer()
            if query.message is not None:
                await query.message.answer(
                    "Отправьте @username канала, который нужно скрыть.\n«Отмена» — выход.",
                    reply_markup=cancel_reply(),
                )
            return
        if parts[1] == "uc":
            await state.set_state(MenuStates.waiting_channel_unblock)
            await query.answer()
            if query.message is not None:
                await query.message.answer(
                    "Отправьте @username канала, который нужно вернуть.\n«Отмена» — выход.",
                    reply_markup=cancel_reply(),
                )
            return

    if len(parts) == 3:
        if parts[1] == "bc" and parts[2] in ("news", "tech", "author", "creative"):
            await db.set_category_block(uid, parts[2], True)
            blocks = await db.get_category_blocks(uid)
            await query.answer("Скрыто")
            if query.message is not None:
                try:
                    await query.message.edit_reply_markup(
                        reply_markup=inline_filters_menu(blocks)
                    )
                except Exception:
                    pass
            return
        if parts[1] == "uc" and parts[2] in ("news", "tech", "author", "creative"):
            await db.set_category_block(uid, parts[2], False)
            blocks = await db.get_category_blocks(uid)
            await query.answer("Показано")
            if query.message is not None:
                try:
                    await query.message.edit_reply_markup(
                        reply_markup=inline_filters_menu(blocks)
                    )
                except Exception:
                    pass
            return
        if parts[1] == "bi":
            try:
                idx = int(parts[2])
            except ValueError:
                await query.answer("Ошибка")
                return
            if idx < 0 or idx >= len(SOURCES):
                await query.answer("Ошибка")
                return
            username = SOURCES[idx].username
            ok = await db.block_channel(uid, username)
            await query.answer("Скрыт" if ok else "Не найден")
            blocked_list = await db.list_blocked_channels(uid)
            blocked_idx = _blocked_indices_from_channels(blocked_list)
            if query.message is not None:
                try:
                    eligible = channel_picker_indices(blocked_idx, True)
                    if not eligible:
                        await query.message.edit_text(
                            "Все каналы из списка уже скрыты."
                        )
                    else:
                        page = _channel_page_from_callback(
                            query.data, blocked_idx, True
                        )
                        max_p = (len(eligible) - 1) // CHANNELS_PER_PAGE
                        page = min(page, max_p)
                        await query.message.edit_text(
                            "Выберите канал, чтобы скрыть:",
                            reply_markup=inline_channel_page(
                                page, blocked_idx, pick_block=True
                            ),
                        )
                except Exception:
                    pass
            if ok:
                await _answer(None, query, f"Канал {username} исключён из вашей выдачи.")
            return
        if parts[1] == "ui":
            try:
                idx = int(parts[2])
            except ValueError:
                await query.answer("Ошибка")
                return
            if idx < 0 or idx >= len(SOURCES):
                await query.answer("Ошибка")
                return
            username = SOURCES[idx].username
            await db.unblock_channel(uid, username)
            await query.answer("Вернул")
            blocked_list = await db.list_blocked_channels(uid)
            blocked_idx = _blocked_indices_from_channels(blocked_list)
            if query.message is not None:
                try:
                    eligible = channel_picker_indices(blocked_idx, False)
                    if not eligible:
                        await query.message.edit_text(
                            "Скрытых каналов больше нет."
                        )
                    else:
                        page = _channel_page_from_callback(
                            query.data, blocked_idx, False
                        )
                        max_p = (len(eligible) - 1) // CHANNELS_PER_PAGE
                        page = min(page, max_p)
                        await query.message.edit_text(
                            "Выберите канал, чтобы вернуть:",
                            reply_markup=inline_channel_page(
                                page, blocked_idx, pick_block=False
                            ),
                        )
                except Exception:
                    pass
            await _answer(None, query, f"Канал {username} снова включён в вашу выдачу.")
            return
        if parts[1] == "cp":
            try:
                page = int(parts[2])
            except ValueError:
                await query.answer()
                return
            eligible = channel_picker_indices(blocked_idx, True)
            if not eligible:
                await query.answer("Все каналы скрыты")
                return
            max_page = (len(eligible) - 1) // CHANNELS_PER_PAGE
            page = max(0, min(page, max_page))
            await query.answer()
            if query.message is not None:
                try:
                    await query.message.edit_text(
                        "Выберите канал, чтобы скрыть:",
                        reply_markup=inline_channel_page(
                            page, blocked_idx, pick_block=True
                        ),
                    )
                except Exception:
                    await query.message.answer(
                        "Выберите канал, чтобы скрыть:",
                        reply_markup=inline_channel_page(
                            page, blocked_idx, pick_block=True
                        ),
                    )
            return
        if parts[1] == "up":
            try:
                page = int(parts[2])
            except ValueError:
                await query.answer()
                return
            eligible = channel_picker_indices(blocked_idx, False)
            if not eligible:
                await query.answer("Нет скрытых")
                return
            max_page = (len(eligible) - 1) // CHANNELS_PER_PAGE
            page = max(0, min(page, max_page))
            await query.answer()
            if query.message is not None:
                try:
                    await query.message.edit_text(
                        "Выберите канал, чтобы вернуть:",
                        reply_markup=inline_channel_page(
                            page, blocked_idx, pick_block=False
                        ),
                    )
                except Exception:
                    await query.message.answer(
                        "Выберите канал, чтобы вернуть:",
                        reply_markup=inline_channel_page(
                            page, blocked_idx, pick_block=False
                        ),
                    )
            return

    await query.answer()


def _channel_page_from_callback(
    data: str, blocked_idx: set[int], pick_block: bool
) -> int:
    parts = data.split(":")
    if len(parts) >= 3 and parts[1] in ("bi", "ui"):
        try:
            idx = int(parts[2])
        except ValueError:
            return 0
        eligible = channel_picker_indices(blocked_idx, pick_block)
        try:
            pos = eligible.index(idx)
        except ValueError:
            return 0
        return pos // CHANNELS_PER_PAGE
    return 0


@router.callback_query(F.data.startswith("src:"))
async def cb_sources_help(query: CallbackQuery) -> None:
    if query.data is None:
        return
    if query.data == "src:list":
        await query.answer()
        grouped = grouped_sources()
        lines: list[str] = ["Источники MVP:"]
        for category in ("Новости", "Технические", "Авторские", "Креативные"):
            lines.append(f"\n<b>{category}</b>")
            for channel in grouped.get(category, []):
                lines.append(f"• {channel}")
        await _answer(None, query, "\n".join(lines))
        return
    if query.data == "src:help":
        await query.answer()
        await _answer(
            None,
            query,
            "<b>Все доступные команды:</b>\n" + PUBLIC_COMMANDS_TEXT,
        )
        return
    await query.answer()


async def _set_mode(message: Message, _query: CallbackQuery | None, db: Database, mode: str) -> None:
    if message.from_user is None:
        return
    if mode not in DELIVERY_MODES:
        await message.answer("Неизвестный режим.", reply_markup=main_menu_reply())
        return
    await db.set_delivery_mode(message.from_user.id, mode)
    await message.answer(
        f"Режим уведомлений установлен: {mode}",
        reply_markup=main_menu_reply(),
    )


async def _set_category_block(
    message: Message, _query: CallbackQuery | None, db: Database, blocked: bool
) -> None:
    if message.from_user is None:
        return
    value = _extract_arg(message.text)
    if not value:
        await message.answer(
            "Укажи категорию: новости, технические, авторские, креативные.\n"
            "Пример: /block_category новости",
            reply_markup=main_menu_reply(),
        )
        return
    key = CATEGORY_KEYS.get(value.lower())
    if key is None:
        await message.answer("Неизвестная категория.", reply_markup=main_menu_reply())
        return
    await db.set_category_block(message.from_user.id, key, blocked)
    state = "исключена" if blocked else "включена"
    await message.answer(
        f"Категория '{value}' {state}.",
        reply_markup=main_menu_reply(),
    )


def _extract_arg(text: str | None) -> str | None:
    if not text:
        return None
    parts = text.strip().split(maxsplit=1)
    if len(parts) < 2:
        return None
    value = parts[1].strip()
    return value if value else None
