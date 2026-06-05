from __future__ import annotations

import logging
from typing import Optional

from aiogram import Bot, F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from .config import DELIVERY_MODES, Settings
from .db import Database
from .formatting import (
    deduplicate_digest_posts,
    format_digest_interval_ru,
    format_hours_window_ru,
    render_digest_list,
)
from .metrics import RuntimeMetrics
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
from .sources import (
    CATEGORY_KEYS,
    KEY_TO_CATEGORY,
    SOURCES,
    grouped_sources_by_platform,
)

router = Router()

# Метка кода — обновляется каждый коммит. Видна в /diagimage. Если в боте
# показывается старая метка — Bothost держит старый процесс, нужен Restart.
CODE_STAMP = "2026-06-04 v11 — revert 1080x1350 (OOM-fix)"

# Публичные команды (без /health): /help и inline «Помощь (команды)»; /start — короткая отсылка сюда.
PUBLIC_COMMANDS_TEXT = (
    "/start — начать\n"
    "/help — помощь\n"
    "/sources — список источников\n"
    "/categories — категории и их статус\n"
    "/my_filters — мои фильтры\n"
    "/block_category &lt;новости|технические|авторские|креативные&gt;\n"
    "/unblock_category &lt;новости|технические|авторские|креативные&gt;\n"
    "/block_channel @username — исключить источник (TG канал или X аккаунт)\n"
    "/unblock_channel @username — вернуть источник\n"
    "/digest — собрать свежий дайджест сейчас\n"
    "/digest &lt;часы&gt; — авто-дайджест, интервал от 1 ч до 7 дней\n"
    "/digest_filter_off — отключить фильтр по окну часов\n"
    "/digest_filter_on — включить фильтр по окну часов\n"
    "/pause — пауза уведомлений\n"
    "/resume — возобновить уведомления\n"
    "/mute_on и /mute_off — выключить/включить уведомления\n"
    "/mode_instant — мгновенно\n"
    "/pending — показать висящие посты на ревью (для админа)"
)


class MenuStates(StatesGroup):
    waiting_digest_hours = State()
    waiting_channel_block = State()
    waiting_channel_unblock = State()
    editing_review_title = State()
    editing_review_body = State()
    editing_review_tags = State()
    editing_review_media = State()
    editing_feedback_comment = State()
    waiting_queue_time = State()


logger = logging.getLogger(__name__)


def _is_admin(query_or_message, settings: Settings) -> bool:
    user = getattr(query_or_message, "from_user", None)
    if user is None:
        return False
    user_id = int(user.id)
    if settings.admin_chat_ids and user_id in settings.admin_chat_ids:
        return True
    if settings.admin_chat_id is not None and user_id == int(settings.admin_chat_id):
        return True
    return False


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
            "Дайджест: авто-отправка с выбранным интервалом (от 1 часа до 7 суток).\n"
            "Ниже — «Сейчас», частые интервалы или «Свой интервал» (любое число часов 1–168).",
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
        "Все доступные команды: /help",
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


@router.message(Command("myid"))
async def cmd_myid(message: Message) -> None:
    """Показывает Telegram user_id отправителя — для удобства добавления второго админа."""
    user = message.from_user
    if user is None:
        await message.answer("Не удалось определить ваш ID.")
        return
    await message.answer(
        f"Ваш Telegram user_id: <code>{user.id}</code>\n"
        f"Имя: {user.full_name}\n"
        f"Username: @{user.username or '—'}"
    )


@router.message(Command("admins"))
async def cmd_admins(message: Message, settings: Settings) -> None:
    """Диагностика multi-admin: показывает кого бот считает админами и узнаёт ли отправителя."""
    user = message.from_user
    user_id = int(user.id) if user else 0
    admin_ids: list[int] = []
    if settings.admin_chat_ids:
        admin_ids.extend(settings.admin_chat_ids)
    if settings.admin_chat_id and settings.admin_chat_id not in admin_ids:
        admin_ids.append(int(settings.admin_chat_id))
    is_admin = user_id in admin_ids
    lines = [
        "<b>Multi-admin диагностика</b>",
        "",
        f"Ваш ID: <code>{user_id}</code>",
        f"Бот считает вас админом: {'✅ ДА' if is_admin else '❌ НЕТ'}",
        "",
        f"Загружено админов: <b>{len(admin_ids)}</b>",
    ]
    for i, aid in enumerate(admin_ids, 1):
        marker = " ← вы" if aid == user_id else ""
        lines.append(f"{i}. <code>{aid}</code>{marker}")
    if not admin_ids:
        lines.append("<i>(пусто — задайте ADMIN_CHAT_ID или ADMIN_CHAT_IDS в env)</i>")
    lines.extend([
        "",
        "<i>Если ваш ID есть в списке, но сообщения не приходят — значит вы не нажали /start этому боту, или Bothost не перезапустил процесс после смены env.</i>",
    ])
    await message.answer("\n".join(lines))


PENDING_PREVIEW_LIMIT = 10


def _pending_header_kb(total: int, offset: int) -> InlineKeyboardMarkup:
    """Шапка под пачку висящих постов: «Следующие N» (если есть) + «Удалить все»."""
    rows: list[list[InlineKeyboardButton]] = []
    next_offset = offset + PENDING_PREVIEW_LIMIT
    if next_offset < total:
        remaining = total - next_offset
        next_count = min(PENDING_PREVIEW_LIMIT, remaining)
        rows.append([
            InlineKeyboardButton(
                text=f"📥 Следующие {next_count} (осталось {remaining})",
                callback_data=f"pending:more:{next_offset}",
            )
        ])
    rows.append([
        InlineKeyboardButton(
            text=f"🗑 Удалить все висящие ({total})",
            callback_data="pending:wipe",
        )
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _send_pending_batch(
    *, db: Database, bot: Bot, settings: Settings, chat_id: int, offset: int,
) -> None:
    """Отдаёт админу пачку из PENDING_PREVIEW_LIMIT висящих постов начиная с offset."""
    total = await db.count_pending_review_posts()
    if total == 0:
        await bot.send_message(chat_id=chat_id, text="📭 На ревью пусто — все посты разобраны.")
        return

    ids = await db.list_pending_review_posts(limit=PENDING_PREVIEW_LIMIT, offset=offset)
    shown = len(ids)
    if shown == 0:
        # offset вылез за конец очереди (например, между вызовами кто-то отскипал).
        await bot.send_message(
            chat_id=chat_id,
            text=(
                f"📭 На этом offset постов нет.\n"
                f"Всего висит: <b>{total}</b>. Запусти <code>/pending</code> заново."
            ),
        )
        return

    header_lines = [
        f"📋 На ревью висит <b>{total}</b> постов.",
        f"Показываю <b>{shown}</b> (с {offset + 1} по {offset + shown}).",
        "",
        "Дальше идут карточки — публикуй или скипай как обычно.",
    ]
    await bot.send_message(
        chat_id=chat_id,
        text="\n".join(header_lines),
        reply_markup=_pending_header_kb(total, offset),
    )

    from .channel_autopublish import _send_review_preview_to_admin
    for source_post_id in ids:
        try:
            await _send_review_preview_to_admin(
                db=db, bot=bot, settings=settings, source_post_id=source_post_id,
            )
        except Exception:
            logger.exception("pending preview send failed source_post_id=%s", source_post_id)


@router.message(Command("pending"))
async def cmd_pending(message: Message, db: Database, bot: Bot, settings: Settings) -> None:
    """Показывает первую пачку висящих постов. Дальше — по кнопке «📥 Следующие»."""
    if not _is_admin(message, settings):
        await message.answer("Команда только для админа.")
        return
    await _send_pending_batch(
        db=db, bot=bot, settings=settings, chat_id=message.chat.id, offset=0,
    )


@router.callback_query(F.data.startswith("pending:more:"))
async def cb_pending_more(
    query: CallbackQuery, db: Database, bot: Bot, settings: Settings,
) -> None:
    """Показывает следующую пачку висящих по offset из callback_data."""
    if not _is_admin(query, settings):
        await query.answer("Только для админа.", show_alert=True)
        return
    parts = (query.data or "").split(":")
    try:
        offset = int(parts[2]) if len(parts) >= 3 else 0
    except ValueError:
        offset = 0
    await query.answer("Загружаю следующую пачку…")
    chat_id = query.message.chat.id if query.message else None
    if chat_id is None:
        return
    await _send_pending_batch(
        db=db, bot=bot, settings=settings, chat_id=chat_id, offset=max(0, offset),
    )


@router.callback_query(F.data == "pending:wipe")
async def cb_pending_wipe(query: CallbackQuery, db: Database, settings: Settings) -> None:
    """Массово переводит висящие посты в skipped."""
    if not _is_admin(query, settings):
        await query.answer("Только для админа.", show_alert=True)
        return

    affected = await db.dismiss_all_pending_review()
    await query.answer(f"Очищено: {affected}", show_alert=False)

    msg = query.message
    if msg is not None:
        try:
            await msg.edit_text(
                f"🗑 Очищено висящих постов: <b>{affected}</b>.\n"
                f"Статусы переведены в <code>skipped</code> "
                f"(<code>error=dismissed_by_admin</code>) — записи в БД сохранены."
            )
        except Exception:
            logger.exception("pending wipe edit_text failed")


# === SMART-SCHEDULER: очередь публикаций по расписанию ===


def _queue_time_picker_kb(source_post_id: int) -> InlineKeyboardMarkup:
    """Меню выбора времени для постановки в очередь.

    Пресеты считаются от текущего момента (UTC), отдельная кнопка для
    ручного ввода через FSM. Колбэки: qslot:<id>:<preset>, qmanual:<id>.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⏰ Через 30 мин", callback_data=f"qslot:{source_post_id}:+30m"),
            InlineKeyboardButton(text="⏰ Через 1 ч", callback_data=f"qslot:{source_post_id}:+1h"),
        ],
        [
            InlineKeyboardButton(text="⏰ Через 3 ч", callback_data=f"qslot:{source_post_id}:+3h"),
        ],
        [
            InlineKeyboardButton(text="✏️ Своё время", callback_data=f"qmanual:{source_post_id}"),
            InlineKeyboardButton(text="🚫 Отмена", callback_data=f"qcancel:{source_post_id}"),
        ],
    ])


def _preset_to_utc(preset: str):
    """Преобразует ключ пресета в UTC datetime. None если ключ неизвестен."""
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    now = _dt.now(tz=_tz.utc)
    if preset == "+30m":
        return now + _td(minutes=30)
    if preset == "+1h":
        return now + _td(hours=1)
    if preset == "+3h":
        return now + _td(hours=3)
    if preset == "tom09":
        # 09:00 МСК = 06:00 UTC
        tomorrow = now + _td(days=1)
        return tomorrow.replace(hour=6, minute=0, second=0, microsecond=0)
    if preset == "tom18":
        # 18:00 МСК = 15:00 UTC
        tomorrow = now + _td(days=1)
        return tomorrow.replace(hour=15, minute=0, second=0, microsecond=0)
    return None


def _parse_queue_time_input(text: str):
    """Парсит ручной ввод времени → UTC datetime. None если не понял.

    Форматы (всё в МСК):
      14:30           — сегодня в 14:30 МСК (если прошло — завтра)
      5.06 14:30      — конкретная дата
      05.06 14:30
      +30m            — через 30 минут
      +2h             — через 2 часа
    """
    import re
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    text = (text or "").strip()
    now_utc = _dt.now(tz=_tz.utc)

    m = re.match(r"^\+(\d{1,4})([mh])$", text, re.IGNORECASE)
    if m:
        n = int(m.group(1))
        unit = m.group(2).lower()
        if n == 0:
            return None
        if unit == "m":
            return now_utc + _td(minutes=n)
        return now_utc + _td(hours=n)

    m = re.match(r"^(\d{1,2}):(\d{2})$", text)
    if m:
        hh, mm = int(m.group(1)), int(m.group(2))
        if hh > 23 or mm > 59:
            return None
        now_msk = now_utc + _td(hours=3)
        target_msk = now_msk.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if target_msk <= now_msk:
            target_msk = target_msk + _td(days=1)
        return target_msk - _td(hours=3)

    m = re.match(r"^(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?\s+(\d{1,2}):(\d{2})$", text)
    if m:
        dd, MM, yyyy_raw, hh, mm = m.groups()
        dd, MM, hh, mm = int(dd), int(MM), int(hh), int(mm)
        if not (1 <= dd <= 31 and 1 <= MM <= 12 and 0 <= hh <= 23 and 0 <= mm <= 59):
            return None
        now_msk = now_utc + _td(hours=3)
        try:
            if yyyy_raw is not None:
                yyyy = int(yyyy_raw)
                if yyyy < 100:
                    yyyy += 2000
                target_msk = now_msk.replace(year=yyyy, month=MM, day=dd, hour=hh, minute=mm, second=0, microsecond=0)
            else:
                target_msk = now_msk.replace(month=MM, day=dd, hour=hh, minute=mm, second=0, microsecond=0)
                if target_msk <= now_msk:
                    target_msk = target_msk.replace(year=target_msk.year + 1)
        except ValueError:
            return None
        if target_msk <= now_msk:
            return None
        return target_msk - _td(hours=3)

    return None


def _format_msk(dt) -> str:
    """UTC datetime → 'DD.MM HH:MM МСК'."""
    from datetime import timedelta as _td
    msk = dt + _td(hours=3)
    return msk.strftime("%d.%m %H:%M МСК")


async def _schedule_and_confirm(
    *, db: Database, query: CallbackQuery | None, message: Message | None,
    source_post_id: int, when_utc,
) -> None:
    """Запись в БД + ответ админу. Используется и для пресетов, и для ручного ввода."""
    from datetime import datetime as _dt, timezone as _tz
    if when_utc <= _dt.now(tz=_tz.utc):
        text = "❌ Это время уже прошло. Укажи будущее время."
        if query:
            await query.answer(text, show_alert=True)
        elif message:
            await message.answer(text)
        return

    ok = await db.set_post_scheduled_for(source_post_id, when_utc.isoformat())
    if not ok:
        status = await db.get_generated_status(source_post_id) or "не найден"
        text = f"❌ Не получилось — статус поста: {status}"
        if query:
            await query.answer(text, show_alert=True)
        elif message:
            await message.answer(text)
        return

    human = _format_msk(when_utc)
    confirm = (
        f"📅 Пост id={source_post_id} запланирован на <b>{human}</b>.\n"
        f"<code>/queue</code> · <code>/unqueue {source_post_id}</code>"
    )
    if query:
        await query.answer(f"📅 На {human}")
        if query.message:
            try:
                await query.message.edit_text(confirm)
            except Exception:
                try:
                    await query.message.answer(confirm)
                except Exception:
                    logger.exception("queue confirm send failed post=%s", source_post_id)
    elif message:
        await message.answer(confirm, reply_markup=main_menu_reply())


@router.callback_query(F.data.startswith("qslot:"))
async def cb_queue_preset_slot(
    query: CallbackQuery, db: Database, settings: Settings,
) -> None:
    if not _is_admin(query, settings):
        await query.answer("Только для админа.", show_alert=True)
        return
    parts = (query.data or "").split(":")
    if len(parts) != 3:
        await query.answer()
        return
    try:
        source_post_id = int(parts[1])
    except ValueError:
        await query.answer("Битый id")
        return
    when = _preset_to_utc(parts[2])
    if when is None:
        await query.answer("Неизвестный пресет")
        return
    await _schedule_and_confirm(
        db=db, query=query, message=None,
        source_post_id=source_post_id, when_utc=when,
    )


@router.callback_query(F.data.startswith("qmanual:"))
async def cb_queue_manual_open(
    query: CallbackQuery, state: FSMContext, settings: Settings,
) -> None:
    if not _is_admin(query, settings):
        await query.answer("Только для админа.", show_alert=True)
        return
    parts = (query.data or "").split(":")
    if len(parts) != 2:
        await query.answer()
        return
    try:
        source_post_id = int(parts[1])
    except ValueError:
        await query.answer("Битый id")
        return
    await state.set_state(MenuStates.waiting_queue_time)
    await state.update_data(queue_source_post_id=source_post_id)
    await query.answer()
    if query.message is not None:
        await query.message.answer(
            f"✏️ Пришли время публикации для поста id={source_post_id}.\n\n"
            "<b>Форматы (всё в МСК):</b>\n"
            "• <code>14:30</code> — сегодня в 14:30 (если прошло — завтра)\n"
            "• <code>5.06 14:30</code> — конкретная дата\n"
            "• <code>5.06.2026 14:30</code> — с явным годом\n"
            "• <code>+30m</code> — через 30 минут\n"
            "• <code>+2h</code> — через 2 часа\n\n"
            "«Отмена» — выйти без правок.",
            reply_markup=cancel_reply(),
        )


@router.callback_query(F.data.startswith("qcancel:"))
async def cb_queue_picker_cancel(
    query: CallbackQuery, settings: Settings,
) -> None:
    if not _is_admin(query, settings):
        await query.answer()
        return
    await query.answer("Отменил")
    if query.message:
        try:
            await query.message.edit_text("🚫 Отмена. Время не выбрано — пост остался в ревью.")
        except Exception:
            pass


@router.message(StateFilter(MenuStates.waiting_queue_time))
async def on_queue_time_input(
    message: Message, db: Database, state: FSMContext, settings: Settings,
) -> None:
    if not _is_admin(message, settings):
        return
    text = (message.text or "").strip()
    if text in {BTN_CANCEL, "/cancel", "Отмена"}:
        await state.clear()
        await message.answer("Отменил. Пост остался в ревью.", reply_markup=main_menu_reply())
        return

    data = await state.get_data()
    sid = int(data.get("queue_source_post_id") or 0)
    if sid == 0:
        await state.clear()
        await message.answer(
            "Потерял контекст. Открой пост заново и нажми «📅 В очередь».",
            reply_markup=main_menu_reply(),
        )
        return

    when_utc = _parse_queue_time_input(text)
    if when_utc is None:
        await message.answer(
            "❌ Не понял формат. Примеры:\n"
            "<code>14:30</code> · <code>5.06 14:30</code> · <code>+2h</code> · <code>+30m</code>"
        )
        return

    await state.clear()
    await _schedule_and_confirm(
        db=db, query=None, message=message,
        source_post_id=sid, when_utc=when_utc,
    )


async def _compute_next_queue_slot(
    db: Database, settings: Settings,
) -> tuple[str, str]:
    """Возвращает (ISO для БД, человеческий формат для ответа админу).

    Правило:
      candidate = max(now, last_scheduled_for) + interval_min
      если candidate вне окна [start_utc..end_utc] — переносим на
      ближайший start_utc следующего дня.
    """
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    now = _dt.now(tz=_tz.utc)
    interval = max(15, int(settings.channel_queue_interval_min))
    last_iso = await db.get_latest_scheduled_for()
    if last_iso:
        try:
            last = _dt.fromisoformat(last_iso)
            if last.tzinfo is None:
                last = last.replace(tzinfo=_tz.utc)
        except ValueError:
            last = now
    else:
        last = now
    candidate = max(now, last) + _td(minutes=interval)
    h_start = max(0, min(23, int(settings.channel_queue_hour_start_utc)))
    h_end = max(h_start + 1, min(24, int(settings.channel_queue_hour_end_utc)))
    if candidate.hour < h_start:
        candidate = candidate.replace(hour=h_start, minute=0, second=0, microsecond=0)
    elif candidate.hour >= h_end:
        next_day = candidate + _td(days=1)
        candidate = next_day.replace(hour=h_start, minute=0, second=0, microsecond=0)
    # Человеческий формат — в МСК (UTC+3), чтобы было привычно
    msk = candidate + _td(hours=3)
    return candidate.isoformat(), msk.strftime("%d.%m %H:%M МСК")


@router.message(Command("queue"))
async def cmd_queue(message: Message, db: Database, settings: Settings) -> None:
    """Очередь публикаций: что и когда уйдёт в канал."""
    if not _is_admin(message, settings):
        await message.answer("Команда только для админа.")
        return
    items = await db.list_queued_posts(limit=50)
    if not items:
        await message.answer(
            "📅 Очередь пустая.\n\n"
            "Чтобы поставить пост в очередь — на превью нажми «📅 В очередь»."
        )
        return
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    lines = [f"📅 <b>Очередь публикаций</b> ({len(items)} шт.):", ""]
    for item in items:
        sid = item["source_post_id"]
        title = (str(item.get("title") or "")).strip()[:70] or "(без заголовка)"
        when_iso = str(item.get("scheduled_for") or "")
        try:
            when = _dt.fromisoformat(when_iso)
            if when.tzinfo is None:
                when = when.replace(tzinfo=_tz.utc)
            when_msk = when + _td(hours=3)
            when_str = when_msk.strftime("%d.%m %H:%M МСК")
        except ValueError:
            when_str = when_iso
        lines.append(f"• <b>{when_str}</b> — {title} <code>(id={sid})</code>")
    lines.extend([
        "",
        f"Интервал: <b>{settings.channel_queue_interval_min} мин</b>. "
        f"Окно (UTC): <b>{settings.channel_queue_hour_start_utc}:00–"
        f"{settings.channel_queue_hour_end_utc}:00</b>",
        "",
        "Отменить отдельный пост: <code>/unqueue &lt;id&gt;</code>",
    ])
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3990] + "…"
    await message.answer(text)


def _friendly_filter_reason(status: str, error: str) -> str:
    """Превращает технический status/error в короткое человеческое объяснение."""
    s = (status or "").strip().lower()
    e = (error or "").strip().lower()
    # Дубликаты
    if s == "duplicate":
        if "exact_fingerprint" in e:
            return "точный дубликат"
        if "link_overlap" in e:
            return "дубликат по ссылкам"
        if "topic_memory" in e:
            return "тема уже была"
        if "near_duplicate_jaccard" in e:
            return "похож на недавний"
        if "post_llm" in e:
            return "дубликат (после LLM)"
        return "дубликат"
    # Скип по разным причинам
    if s == "skipped":
        if "admin_skipped" in e:
            return "ты скипнул"
        if "dismissed_by_admin" in e:
            return "массовая очистка"
        if "candidate_too_short" in e or "post_llm_too_short" in e:
            return "слишком короткий"
        if "no_ai_topic" in e or "non_news" in e:
            return "не про AI / не новость"
        if "skipped_by_limit" in e:
            return "лимит постов в день"
        if "llm_status_skip" in e:
            return "LLM сказала пропустить"
        if "pre_llm" in e:
            return "отсеян до LLM"
        if "post_llm" in e:
            return "отсеян после LLM"
        if e:
            return f"skipped ({e[:40]})"
        return "skipped"
    if s == "failed":
        if e:
            return f"ошибка ({e[:40]})"
        return "ошибка"
    return s or "?"


def _short_when(updated_at_iso: str) -> str:
    """ISO → '5 мин назад' / '2 ч назад' / '03.06 14:32'."""
    from datetime import datetime as _dt, timezone as _tz
    if not updated_at_iso:
        return "?"
    try:
        t = _dt.fromisoformat(updated_at_iso)
        if t.tzinfo is None:
            t = t.replace(tzinfo=_tz.utc)
    except ValueError:
        return updated_at_iso[:16]
    delta = _dt.now(tz=_tz.utc) - t
    secs = int(delta.total_seconds())
    if secs < 60:
        return f"{secs}с назад"
    if secs < 3600:
        return f"{secs // 60} мин назад"
    if secs < 86400:
        return f"{secs // 3600} ч назад"
    # Старше суток — абсолютное время в МСК
    from datetime import timedelta as _td
    msk = t + _td(hours=3)
    return msk.strftime("%d.%m %H:%M МСК")


@router.message(Command("lastfiltered"))
async def cmd_lastfiltered(message: Message, db: Database, settings: Settings) -> None:
    """Последние посты, которые не дошли до превью (отсеяны или ошиблись)."""
    if not _is_admin(message, settings):
        await message.answer("Команда только для админа.")
        return
    # По умолчанию 10, можно `/lastfiltered 20` (до 50).
    parts = (message.text or "").split(maxsplit=1)
    limit = 10
    if len(parts) >= 2:
        try:
            limit = max(1, min(50, int(parts[1].strip())))
        except ValueError:
            pass
    items = await db.list_filtered_posts_with_meta(limit=limit)
    if not items:
        await message.answer("📭 Отфильтрованных нет — всё идёт в ревью.")
        return
    lines = [f"🗑 <b>Последние {len(items)} отфильтрованных:</b>", ""]
    for idx, item in enumerate(items, 1):
        sid = item["source_post_id"]
        status = str(item.get("status") or "")
        err = str(item.get("error") or "")
        reason = _friendly_filter_reason(status, err)
        when = _short_when(str(item.get("updated_at") or ""))
        src = str(item.get("source_username") or "?")
        title = (str(item.get("title") or item.get("source_text") or "")).strip()
        title = " ".join(title.split())[:90]  # схлопываем переносы
        if not title:
            title = "(без текста)"
        link = str(item.get("source_link") or "")
        title_html = f'<a href="{link}">{title}</a>' if link else title
        lines.append(f"<b>{idx}.</b> @{src} · <i>{when}</i>")
        lines.append(f"    {title_html}")
        lines.append(f"    <code>↳ {reason}</code>")
        lines.append("")
    lines.append("Лимит можно увеличить: <code>/lastfiltered 30</code> (до 50).")
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3990] + "…"
    await message.answer(text, disable_web_page_preview=True)


@router.message(Command("unqueue"))
async def cmd_unqueue(message: Message, db: Database, settings: Settings) -> None:
    """Возвращает пост из очереди обратно в pending_review."""
    if not _is_admin(message, settings):
        await message.answer("Команда только для админа.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: <code>/unqueue &lt;source_post_id&gt;</code>")
        return
    try:
        source_post_id = int(parts[1].strip())
    except ValueError:
        await message.answer("Нужен числовой id поста.")
        return
    ok = await db.unqueue_post(source_post_id)
    if ok:
        await message.answer(
            f"↩️ Пост id={source_post_id} вернулся в <b>pending_review</b>. "
            f"Покажется в <code>/pending</code>."
        )
    else:
        status = await db.get_generated_status(source_post_id) or "не найден"
        await message.answer(f"Не получилось. Текущий статус поста: <code>{status}</code>")


# === ДАЙДЖЕСТ /scan — компактный список висящих с одной кнопкой на пост ===

SCAN_LIMIT = 10
_SCAN_NUMS = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]


def _scan_message(items: list[dict], total: int) -> tuple[str, InlineKeyboardMarkup]:
    """Рендерит компактный дайджест и клавиатуру с парой кнопок на каждый пост."""
    lines = [f"📋 <b>Дайджест</b>: висит {total}, показываю {len(items)}.", ""]
    rows: list[list[InlineKeyboardButton]] = []
    for idx, item in enumerate(items):
        num = _SCAN_NUMS[idx] if idx < len(_SCAN_NUMS) else f"{idx + 1}."
        title = (str(item.get("title") or item.get("summary") or "").strip())[:120]
        if not title:
            title = "(без заголовка)"
        src = str(item.get("source_username") or "?")
        lines.append(f"{num} <b>{title}</b>")
        lines.append(f"    <i>@{src}</i>")
        sid = int(item["source_post_id"])
        rows.append([
            InlineKeyboardButton(text=f"{num} ✅", callback_data=f"scan:pub:{sid}"),
            InlineKeyboardButton(text=f"{num} ⏭", callback_data=f"scan:skip:{sid}"),
        ])
    rows.append([
        InlineKeyboardButton(text=f"🔄 Обновить (осталось {total})", callback_data="scan:refresh"),
    ])
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3990] + "…"
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(Command("scan"))
async def cmd_scan(message: Message, db: Database, settings: Settings) -> None:
    """Компактный дайджест висящих постов: цифровые кнопки ✅/⏭ на каждый.

    Не присылает фото каждого — только список. Деталный просмотр через
    /pending или клик «✅» (опубликует, не показывая полное превью).
    """
    if not _is_admin(message, settings):
        await message.answer("Команда только для админа.")
        return
    total = await db.count_pending_review_posts()
    if total == 0:
        await message.answer("📭 На ревью пусто — все посты разобраны.")
        return
    items = await db.list_pending_review_with_meta(limit=SCAN_LIMIT)
    text, kb = _scan_message(items, total)
    await message.answer(text, reply_markup=kb, disable_web_page_preview=True)


@router.callback_query(F.data.startswith("scan:pub:"))
async def cb_scan_pub(
    query: CallbackQuery, db: Database, bot: Bot, settings: Settings,
) -> None:
    if not _is_admin(query, settings):
        await query.answer("Только для админа.", show_alert=True)
        return
    try:
        source_post_id = int((query.data or "").split(":")[2])
    except (IndexError, ValueError):
        await query.answer("Битый id")
        return
    claimed = await db.try_claim_for_publish(source_post_id)
    if not claimed:
        status = await db.get_generated_status(source_post_id) or "?"
        await query.answer(f"Уже в статусе {status}", show_alert=False)
        return
    await query.answer("🚀 Публикую…")
    from .channel_autopublish import _publish_generated_post
    try:
        await _publish_generated_post(db=db, bot=bot, settings=settings, source_post_id=source_post_id)
    except Exception:
        logger.exception("scan:pub publish failed source_post_id=%s", source_post_id)


@router.callback_query(F.data.startswith("scan:skip:"))
async def cb_scan_skip(query: CallbackQuery, db: Database, settings: Settings) -> None:
    if not _is_admin(query, settings):
        await query.answer("Только для админа.", show_alert=True)
        return
    try:
        source_post_id = int((query.data or "").split(":")[2])
    except (IndexError, ValueError):
        await query.answer("Битый id")
        return
    await db.update_generated_channel_post(
        source_post_id, status="skipped", error="admin_skipped_in_scan"
    )
    await query.answer(f"⏭ Пост {source_post_id} скипнут")


@router.callback_query(F.data == "scan:refresh")
async def cb_scan_refresh(
    query: CallbackQuery, db: Database, bot: Bot, settings: Settings,
) -> None:
    """Перерисовывает дайджест: новые висящие посты (без скипнутых)."""
    if not _is_admin(query, settings):
        await query.answer("Только для админа.", show_alert=True)
        return
    total = await db.count_pending_review_posts()
    chat_id = query.message.chat.id if query.message else None
    if chat_id is None:
        return
    if total == 0:
        await query.answer("Очередь пустая")
        try:
            if query.message:
                await query.message.edit_text("📭 На ревью пусто — все посты разобраны.")
        except Exception:
            pass
        return
    items = await db.list_pending_review_with_meta(limit=SCAN_LIMIT)
    text, kb = _scan_message(items, total)
    await query.answer("Обновлено")
    # Новый месседж проще, чем edit (картинки в kb могут не пересобраться).
    await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb, disable_web_page_preview=True)


@router.message(Command("sources"))
async def cmd_sources(message: Message) -> None:
    grouped = grouped_sources_by_platform()
    lines: list[str] = ["Источники MVP:"]
    for platform, title in (("tg", "Telegram"), ("x", "Twitter/X")):
        lines.append(f"\n<b>{title}</b>")
        for category in ("Новости", "Технические", "Авторские", "Креативные"):
            rows = grouped.get(platform, {}).get(category, [])
            if not rows:
                continue
            lines.append(f"\n{category}:")
            for channel in rows:
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
            "Использование: /block_channel @username\nПодходит и для Telegram-каналов, и для Twitter/X-аккаунтов.",
            reply_markup=main_menu_reply(),
        )
        return
    ok = await db.block_channel(message.from_user.id, username)
    if not ok:
        await message.answer(
            "Источник не найден в списке источников.",
            reply_markup=main_menu_reply(),
        )
        return
    await message.answer(
        f"Источник {username} исключён из вашей выдачи.",
        reply_markup=main_menu_reply(),
    )


@router.message(Command("unblock_channel"))
async def cmd_unblock_channel(message: Message, db: Database) -> None:
    if message.from_user is None:
        return
    username = _extract_arg(message.text)
    if not username:
        await message.answer(
            "Использование: /unblock_channel @username\nПодходит и для Telegram-каналов, и для Twitter/X-аккаунтов.",
            reply_markup=main_menu_reply(),
        )
        return
    await db.unblock_channel(message.from_user.id, username)
    await message.answer(
        f"Источник {username} снова включён в вашу выдачу.",
        reply_markup=main_menu_reply(),
    )


@router.message(Command("health"))
async def cmd_health(message: Message, db: Database, metrics: RuntimeMetrics) -> None:
    stats = await db.health_stats()
    runtime = metrics.snapshot()
    await message.answer(
        "Health snapshot:\n"
        f"users={stats.get('users_count', 0)}\n"
        f"posts={stats.get('source_posts_count', 0)}\n"
        f"delivery={stats.get('delivery_events_count', 0)}\n"
        f"status={stats.get('delivery_status', {})}\n"
        f"x_api_requests={runtime.get('x_api_requests', 0)}\n"
        f"x_api_requests_total={runtime.get('x_api_requests_total', 0)}\n"
        f"x_api_requests_last_hour={runtime.get('x_api_requests_last_hour', 0)}\n"
        f"x_api_sources_polled={runtime.get('x_api_sources_polled', 0)}\n"
        f"x_api_cache_hits={runtime.get('x_api_cache_hits', 0)}\n"
        f"x_api_cache_misses={runtime.get('x_api_cache_misses', 0)}\n"
        f"x_api_rate_limited={runtime.get('x_api_rate_limited', 0)}\n"
        f"x_api_auth_errors={runtime.get('x_api_auth_errors', 0)}\n"
        f"x_collected_posts={runtime.get('x_collected_posts', 0)}\n"
        f"x_posts_last_24h={stats.get('x_posts_last_24h', runtime.get('x_posts_last_24h', 0))}\n"
        f"x_requests_per_post={runtime.get('x_requests_per_post', 0.0)}\n"
        f"channel_post_status={stats.get('channel_post_status', {})}\n"
        f"channel_published_today_utc={stats.get('channel_published_today_utc', 0)} "
        f"(day={stats.get('channel_publish_day_utc', '')})\n"
        f"channel_llm_calls={runtime.get('channel_llm_calls', 0)}\n"
        f"channel_published={runtime.get('channel_published', 0)}\n"
        f"channel_duplicates={runtime.get('channel_duplicates', 0)}\n"
        f"duplicates_exact={runtime.get('channel_duplicates_exact', 0)}\n"
        f"duplicates_near={runtime.get('channel_duplicates_near', 0)}\n"
        f"duplicates_post_llm={runtime.get('channel_duplicates_post_llm', 0)}\n"
        f"duplicates_link_overlap={runtime.get('channel_duplicates_link_overlap', 0)}\n"
        f"channel_windows={stats.get('channel_windows', {})}\n"
        f"channel_duplicate_reasons_24h={stats.get('channel_duplicate_reasons_24h', {})}\n"
        f"channel_skipped_limit={runtime.get('channel_skipped_limit', 0)}\n"
        f"channel_failed={runtime.get('channel_failed', 0)}",
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
                "Использование: /digest &lt;часы&gt; — целое число часов, от 1 ч до 7 дней.",
                reply_markup=main_menu_reply(),
            )
            return
        if hours < 1 or hours > 168:
            await message.answer(
                "Интервал от 1 ч до 7 дней.",
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
            "Источник не найден в списке источников. Проверьте @username.",
            reply_markup=main_menu_reply(),
        )
        return
    await message.answer(
        f"Источник {username.strip()} исключён из вашей выдачи.",
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
        f"Источник {username.strip()} снова включён в вашу выдачу.",
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
                    "Отправьте @username источника (TG канал или X аккаунт), который нужно скрыть.\n«Отмена» — выход.",
                    reply_markup=cancel_reply(),
                )
            return
        if parts[1] == "uc":
            await state.set_state(MenuStates.waiting_channel_unblock)
            await query.answer()
            if query.message is not None:
                await query.message.answer(
                    "Отправьте @username источника (TG канал или X аккаунт), который нужно вернуть.\n«Отмена» — выход.",
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
                await _answer(None, query, f"Источник {username} исключён из вашей выдачи.")
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
            await _answer(None, query, f"Источник {username} снова включён в вашу выдачу.")
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
        grouped = grouped_sources_by_platform()
        lines: list[str] = ["Источники MVP:"]
        for platform, title in (("tg", "Telegram"), ("x", "Twitter/X")):
            lines.append(f"\n<b>{title}</b>")
            for category in ("Новости", "Технические", "Авторские", "Креативные"):
                rows = grouped.get(platform, {}).get(category, [])
                if not rows:
                    continue
                lines.append(f"\n{category}:")
                for channel in rows:
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


@router.message(Command("setllmkey"))
async def cmd_setllmkey(
    message: Message,
    db: Database,
    settings: Settings,
) -> None:
    """Админ-команда: записать API-ключ OpenRouter в bot.db (переживает все деплои Bothost).

    Использование: /setllmkey <api_key>

    После сохранения нужен Restart бота. При следующем старте бот подхватит ключ из БД и канал заработает.
    """
    if not _is_admin(message, settings):
        return  # тихо, не намекая что команда есть

    raw = (message.text or "").split(maxsplit=1)
    if len(raw) < 2:
        await message.answer(
            "Использование: <code>/setllmkey sk-or-v1-...</code>\n\n"
            "Команда сохранит API-ключ в БД, бот подхватит после Restart.",
        )
        return

    new_key = raw[1].strip()
    if not new_key.startswith("sk-or-"):
        await message.answer(
            "Ключ должен начинаться с <code>sk-or-v1-...</code> — это формат OpenRouter.\n"
            "Если хочешь другой провайдер — напиши, добавлю.",
        )
        return
    if len(new_key) < 30:
        await message.answer("Слишком короткий ключ. OpenRouter обычно ~70+ символов.")
        return

    await db.set_bot_secret("openrouter_api_key", new_key)
    masked = new_key[:12] + "…" + new_key[-4:]
    await message.answer(
        f"✅ Ключ сохранён в БД (<code>{masked}</code>).\n\n"
        "Теперь нажми <b>Restart</b> в Bothost — бот подхватит ключ при следующем старте, "
        "канал начнёт публиковать.\n\n"
        "Ключ хранится в bot.db и переживает все деплои/перезапуски."
    )


@router.message(Command("transcode"))
async def cmd_transcode(
    message: Message,
    db: Database,
    settings: Settings,
) -> None:
    """Включить/выключить транскод видео (override CHANNEL_VIDEO_NO_COMPRESSION в env).

    Usage:
      /transcode on   — включить (no_compression=0)
      /transcode off  — выключить (no_compression=1)
      /transcode env  — снять override, вернуться к ENV
      /transcode      — показать текущий статус
    """
    if not _is_admin(message, settings):
        return

    raw = (message.text or "").split(maxsplit=1)
    db_value = await db.get_bot_secret("channel_video_no_compression") or ""
    active = settings.channel_video_no_compression

    if len(raw) < 2:
        await message.answer(
            "<b>Транскод видео</b>\n\n"
            f"Активно сейчас: {'❌ off (skip transcode)' if active else '✅ on (transcode applied)'}\n"
            f"В БД override: <code>{db_value or '(пусто, читается ENV)'}</code>\n\n"
            "<b>Команды:</b>\n"
            "<code>/transcode on</code> — включить транскод\n"
            "<code>/transcode off</code> — выключить транскод\n"
            "<code>/transcode env</code> — снять override, читать ENV\n\n"
            "<i>После изменения — Restart бота, чтобы подхватил.</i>"
        )
        return

    arg = raw[1].strip().lower()
    if arg == "env":
        await db.set_bot_secret("channel_video_no_compression", "")
        await message.answer("✅ Override в БД снят. После Restart бот будет читать ENV.")
        return
    if arg in {"on", "1", "true", "yes"}:
        await db.set_bot_secret("channel_video_no_compression", "0")
        await message.answer(
            "✅ Транскод включён в БД (no_compression=0).\n\n"
            "Нажми <b>Restart</b> в Bothost — видео начнут перекодироваться в H264 main 720p AAC + faststart."
        )
        return
    if arg in {"off", "0", "false", "no"}:
        await db.set_bot_secret("channel_video_no_compression", "1")
        await message.answer(
            "⚠️ Транскод выключен в БД (no_compression=1).\n\n"
            "После Restart видео будут уходить как есть. Telegram может показать как documents (облачко)."
        )
        return
    await message.answer("❌ Неизвестный аргумент. Используй <code>on</code>, <code>off</code>, <code>env</code> или без аргументов.")


@router.message(Command("diagimage"))
async def cmd_diagimage(
    message: Message,
    db: Database,
    settings: Settings,
) -> None:
    """Диагностика генерации обложек: последняя ошибка + последние попытки в БД."""
    if not _is_admin(message, settings):
        return

    last_err = await db.get_bot_secret("last_image_gen_error") or ""
    last_trace = await db.get_bot_secret("last_preview_trace") or ""
    last_imggen_trace = await db.get_bot_secret("last_imggen_trace") or ""
    model_override = await db.get_bot_secret("image_gen_model") or ""

    # Последние 5 попыток из лога
    async with db.conn.execute(
        "SELECT source_post_id, model, cost_usd, success, prompt, created_at "
        "FROM image_generation_log ORDER BY id DESC LIMIT 5"
    ) as cur:
        rows = [dict(r) for r in await cur.fetchall()]

    lines = [
        "<b>🎨 Диагностика генерации обложек</b>",
        f"<i>code: {CODE_STAMP}</i>",
        "",
        "<b>Настройки:</b>",
        f"  ENABLE_IMAGE_GENERATION: {'✅ on' if settings.enable_image_generation else '❌ off'}",
        f"  Активная модель: <code>{settings.image_gen_model}</code>",
        f"  Override в БД: <code>{model_override or '(пусто)'}</code>",
        f"  OpenRouter API key: {'✅ есть' if settings.openrouter_api_key else '❌ нет'}",
        "",
        f"<b>Последние попытки ({len(rows)}):</b>",
    ]
    if not rows:
        lines.append("  <i>(пусто)</i>")
    else:
        for r in rows:
            ok = "✅" if r["success"] else "❌"
            prompt_head = (r["prompt"] or "")[:80].replace("\n", " ")
            lines.append(
                f"  {ok} post={r['source_post_id']} model={r['model']} "
                f"${r['cost_usd']:.4f} | {prompt_head}…"
            )

    if last_err:
        lines.extend([
            "",
            "<b>Последняя ошибка:</b>",
            f"<code>{last_err[:1500]}</code>",
        ])
    else:
        lines.extend(["", "<i>Ошибок не зафиксировано.</i>"])

    if last_imggen_trace:
        lines.extend([
            "",
            "<b>Trace последнего ручного «🎨 Сгенерировать»:</b>",
            f"<code>{last_imggen_trace[:1500]}</code>",
        ])

    if last_trace:
        lines.extend([
            "",
            "<b>Trace последнего автопревью:</b>",
            f"<code>{last_trace[:1500]}</code>",
        ])

    lines.extend([
        "",
        "<b>Команды:</b>",
        "<code>/setimagemodel google/gemini-2.5-flash-image</code> — переключить модель",
        "<code>/setimagemodel openai/dall-e-3</code>",
        "<code>/setimagemodel black-forest-labs/flux-1.1-pro</code>",
        "<code>/setimagemodel env</code> — снять override (читать ENV)",
    ])

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3990] + "…"
    await message.answer(text)


@router.message(Command("setimagemodel"))
async def cmd_setimagemodel(
    message: Message,
    db: Database,
    settings: Settings,
) -> None:
    """Переключить модель image-gen без рестарта env."""
    if not _is_admin(message, settings):
        return

    raw = (message.text or "").split(maxsplit=1)
    if len(raw) < 2:
        await message.answer(
            "<b>Использование:</b>\n"
            "<code>/setimagemodel google/gemini-2.5-flash-image</code>\n"
            "<code>/setimagemodel black-forest-labs/flux-schnell</code>\n"
            "<code>/setimagemodel black-forest-labs/flux-1.1-pro</code>\n"
            "<code>/setimagemodel openai/dall-e-3</code>\n"
            "<code>/setimagemodel env</code> — снять override\n\n"
            f"Сейчас: <code>{settings.image_gen_model}</code>"
        )
        return

    arg = raw[1].strip()
    if arg.lower() == "env":
        await db.set_bot_secret("image_gen_model", "")
        await message.answer("✅ Override снят. После Restart бот будет читать IMAGE_GEN_MODEL из ENV.")
        return
    if "/" not in arg:
        await message.answer("❌ Похоже не на имя модели OpenRouter (обычно <code>provider/model</code>).")
        return
    await db.set_bot_secret("image_gen_model", arg)
    await message.answer(
        f"✅ Модель сохранена в БД: <code>{arg}</code>\n\n"
        "Нажми <b>Restart</b> в Bothost — бот подхватит при следующем старте."
    )


@router.message(Command("imagegen"))
async def cmd_imagegen(
    message: Message,
    db: Database,
    settings: Settings,
) -> None:
    """Включить/выключить генерацию обложек (override ENABLE_IMAGE_GENERATION)."""
    if not _is_admin(message, settings):
        return

    raw = (message.text or "").split(maxsplit=1)
    db_value = await db.get_bot_secret("enable_image_generation") or ""
    active = settings.enable_image_generation

    if len(raw) < 2:
        await message.answer(
            "<b>Генерация обложек (info-карточки)</b>\n\n"
            f"Активно сейчас: {'✅ on' if active else '❌ off'}\n"
            f"Рендерер: <code>Pillow (info-card)</code>\n"
            f"LLM-парсер: <code>{settings.image_gen_model}</code> (только парсит пост → JSON)\n"
            f"Цена/картинка: <code>~$0.0001</code> (только LLM-парсинг)\n"
            f"Дневной бюджет: <code>${settings.image_gen_daily_budget_usd:.2f}</code>\n"
            f"В БД override: <code>{db_value or '(пусто, читается ENV)'}</code>\n\n"
            "<b>Подкоманды:</b>\n"
            "<code>/imagegen on</code> — включить\n"
            "<code>/imagegen off</code> — выключить\n"
            "<code>/imagegen env</code> — снять override\n\n"
            "<b>Установка ассетов:</b>\n"
            "<code>/installfonts</code> — скачать Inter Bold + ExtraBold\n"
            "<code>/uploadlogo openai URL</code> — залить лого компании\n"
            "<code>/listlogos</code> — список залитых лого\n\n"
            "<i>После изменения настроек — Restart бота.</i>"
        )
        return

    arg = raw[1].strip().lower()
    if arg == "env":
        await db.set_bot_secret("enable_image_generation", "")
        await message.answer("✅ Override снят. После Restart бот читает ENV.")
        return
    if arg in {"on", "1", "true", "yes"}:
        await db.set_bot_secret("enable_image_generation", "1")
        await message.answer("✅ Включено в БД. Нажми <b>Restart</b> в Bothost.")
        return
    if arg in {"off", "0", "false", "no"}:
        await db.set_bot_secret("enable_image_generation", "0")
        await message.answer("⚠️ Выключено в БД. После Restart кнопка «🎨 Сгенерировать» перестанет работать.")
        return
    await message.answer("❌ Используй on / off / env или без аргументов.")


@router.message(Command("imagebudget"))
async def cmd_imagebudget(
    message: Message,
    db: Database,
    settings: Settings,
) -> None:
    """Показывает расходы на генерацию обложек за день и неделю."""
    if not _is_admin(message, settings):
        return

    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    now = _dt.now(tz=_tz.utc)
    day_since = (now - _td(days=1)).isoformat()
    week_since = (now - _td(days=7)).isoformat()
    month_since = (now - _td(days=30)).isoformat()

    day = await db.get_image_gen_stats(since_iso=day_since)
    week = await db.get_image_gen_stats(since_iso=week_since)
    month = await db.get_image_gen_stats(since_iso=month_since)

    budget = settings.image_gen_daily_budget_usd
    used_pct = (day["total_cost"] / budget * 100) if budget > 0 else 0
    remaining = max(0.0, budget - day["total_cost"])

    await message.answer(
        "<b>💰 Бюджет генерации обложек</b>\n\n"
        f"<b>Сегодня (24ч):</b>\n"
        f"  Попыток: {day['attempts']}, успешных: {day['successes']}\n"
        f"  Потрачено: <code>${day['total_cost']:.4f}</code> / ${budget:.2f} ({used_pct:.0f}%)\n"
        f"  Осталось: <code>${remaining:.4f}</code>\n\n"
        f"<b>Неделя:</b> {week['successes']}/{week['attempts']}, "
        f"<code>${week['total_cost']:.4f}</code>\n"
        f"<b>Месяц:</b> {month['successes']}/{month['attempts']}, "
        f"<code>${month['total_cost']:.4f}</code>\n\n"
        f"Модель: <code>{settings.image_gen_model}</code>\n"
        f"Цена за картинку: <code>${settings.image_gen_cost_usd:.4f}</code>"
    )


@router.message(Command("uploadwave"))
async def cmd_uploadwave_help(
    message: Message,
    settings: Settings,
) -> None:
    """Показывает инструкцию по заливке wave-tight.png."""
    if not _is_admin(message, settings):
        return
    await message.answer(
        "<b>Заливка бренд-волны Automy AI</b>\n\n"
        "Отправь файл <code>wave-tight.png</code> боту обычным документом "
        "с подписью <code>/uploadwave</code>.\n\n"
        "Бот сохранит его в <code>/app/data/assets/wave-tight.png</code>. "
        "Использовать как brand-stamp на карточке (top-left).\n\n"
        "Файл лежит в репо <code>01_Работа/02_Automy/Инста/Посты/build/img/wave-tight.png</code> "
        "(или в инсте-проекте бренда)."
    )


@router.message(F.document, F.chat.type == "private", StateFilter(None))
async def cb_uploaded_font(
    message: Message,
    bot: Bot,
    settings: Settings,
) -> None:
    """Принимает TTF/OTF файл в личке (вне любого FSM-состояния) и сохраняет в /app/data/fonts/.

    Фильтры:
    - private chat — не перехватывает документы в групповых чатах
    - StateFilter(None) — не мешает FSM-флоу (editing_review_media и т.д.)

    Достаточно ПРОСТО отправить TTF/OTF файл боту — без подписи.
    Имя файла используется как есть (например Inter-Bold.ttf).

    Опционально, можно добавить подпись:
      /uploadfont           — сохранить под оригинальным именем
      /uploadfont Bold      — сохранить как Inter-Bold.ttf
      /uploadfont ExtraBold — сохранить как Inter-ExtraBold.ttf
      /uploadfont SomeName  — сохранить как SomeName.ttf
    """
    if not _is_admin(message, settings):
        return
    doc = message.document
    if doc is None:
        return
    fname = (doc.file_name or "").lower()
    caption = (message.caption or "").strip()

    # Спецветка: /uploadwave + PNG → сохранить в /app/data/assets/wave-tight.png
    if caption.startswith("/uploadwave") and fname.endswith(".png"):
        import os
        from pathlib import Path as _Path

        assets_dir = _Path(os.getenv("DATA_DIR", "/app/data")) / "assets"
        assets_dir.mkdir(parents=True, exist_ok=True)
        dest = assets_dir / "wave-tight.png"
        await message.answer(f"⏳ Сохраняю {doc.file_name} → wave-tight.png ...")
        try:
            file = await bot.get_file(doc.file_id)
            await bot.download_file(file.file_path, destination=dest)
            size_kb = dest.stat().st_size // 1024
            await message.answer(
                f"✅ <code>wave-tight.png</code> сохранён ({size_kb} KB) в /app/data/assets/.\n\n"
                "При следующем рендере карточки логотип появится в brand-stamp top-left."
            )
        except Exception as exc:
            await message.answer(f"❌ Не получилось скачать: {exc}")
        return

    if not (fname.endswith(".ttf") or fname.endswith(".otf")):
        # Не шрифт и не wave — игнорируем тихо.
        return

    import os
    from pathlib import Path as _Path

    fonts_dir = _Path(os.getenv("DATA_DIR", "/app/data")) / "fonts"
    fonts_dir.mkdir(parents=True, exist_ok=True)

    caption = (message.caption or "").strip()
    target: str
    if caption.startswith("/uploadfont"):
        parts = caption.split(maxsplit=1)
        if len(parts) >= 2 and parts[1].strip():
            arg = parts[1].strip()
            low = arg.lower()
            if low in {"bold", "regular"}:
                target = "Inter-Bold.ttf"
            elif low == "extrabold":
                target = "Inter-ExtraBold.ttf"
            elif low == "black":
                target = "Roboto-Black.ttf"
            elif arg.endswith(".ttf") or arg.endswith(".otf"):
                target = arg
            else:
                target = f"{arg}.ttf"
        else:
            target = doc.file_name or "Inter-Bold.ttf"
    else:
        target = doc.file_name or "Inter-Bold.ttf"

    await message.answer(f"⏳ Скачиваю <code>{doc.file_name}</code> → <code>{target}</code> ...")

    try:
        file = await bot.get_file(doc.file_id)
        dest = fonts_dir / target
        await bot.download_file(file.file_path, destination=dest)
        size_kb = dest.stat().st_size // 1024
        # Проверим что Pillow реально умеет его открыть
        ok_msg = ""
        try:
            from PIL import ImageFont
            ImageFont.truetype(str(dest), 40)
            ok_msg = " ✓ Pillow прочитал шрифт"
        except Exception as exc:
            ok_msg = f" ⚠️ Pillow не смог открыть: {exc}"
        await message.answer(
            f"✅ <code>{target}</code> сохранён ({size_kb} KB).{ok_msg}\n\n"
            "Подхватится при следующем рендере карточки — рестарт не нужен."
        )
    except Exception as exc:
        await message.answer(f"❌ Не получилось скачать: {exc}")


@router.message(Command("listfonts"))
async def cmd_listfonts(
    message: Message,
    settings: Settings,
) -> None:
    """Показывает что лежит в /app/data/fonts/ и какие шрифты будут использоваться."""
    if not _is_admin(message, settings):
        return

    import os
    from pathlib import Path as _Path

    fonts_dir = _Path(os.getenv("DATA_DIR", "/app/data")) / "fonts"
    files = sorted(fonts_dir.glob("*.ttf")) + sorted(fonts_dir.glob("*.otf")) if fonts_dir.is_dir() else []

    lines = ["<b>Шрифты в /app/data/fonts/:</b>"]
    if not files:
        lines.append("  <i>(пусто — Pillow будет искать системные DejaVu/Liberation)</i>")
    else:
        for f in files:
            size_kb = f.stat().st_size // 1024
            lines.append(f"  • <code>{f.name}</code> ({size_kb} KB)")

    # Что реально найдёт _load_font
    lines.append("")
    lines.append("<b>Какие шрифты выбирает рендерер:</b>")
    try:
        from .image_card import _load_font
        for weight in ("Bold", "ExtraBold"):
            font = _load_font(40, weight=weight)
            if font is None:
                src = "(None)"
            else:
                path = getattr(font, "path", None) or "(default PIL)"
                src = str(path)
            lines.append(f"  • {weight}: <code>{src}</code>")
    except Exception as exc:
        lines.append(f"  <i>ошибка проверки: {exc}</i>")

    lines.append("")
    lines.append(
        "<b>Как залить:</b> отправь TTF/OTF файл боту обычным документом. "
        "Не нужно никаких подписей — имя файла используется как есть."
    )
    await message.answer("\n".join(lines))


@router.message(Command("installfonts"))
async def cmd_installfonts(
    message: Message,
    settings: Settings,
) -> None:
    """Скачивает Inter (Bold + ExtraBold) и кладёт в /app/data/fonts/.

    Использование:
      /installfonts            — скачать дефолтные Inter Bold + ExtraBold
      /installfonts URL FILENAME — кастомный шрифт (имя должно быть Inter-<Weight>.ttf)
    """
    if not _is_admin(message, settings):
        return

    raw = (message.text or "").split(maxsplit=2)
    # Каждый файл = список URL для fallback'а. Останавливаемся на первом успехе.
    if len(raw) == 1:
        files: list[tuple[str, list[str]]] = [
            ("NotoSans-Bold.ttf", [
                "https://github.com/notofonts/notofonts.github.io/raw/main/fonts/NotoSans/hinted/ttf/NotoSans-Bold.ttf",
                "https://cdn.jsdelivr.net/gh/notofonts/notofonts.github.io@main/fonts/NotoSans/hinted/ttf/NotoSans-Bold.ttf",
                "https://github.com/googlefonts/noto-fonts/raw/main/hinted/ttf/NotoSans/NotoSans-Bold.ttf",
                "https://cdn.jsdelivr.net/gh/googlefonts/noto-fonts@main/hinted/ttf/NotoSans/NotoSans-Bold.ttf",
            ]),
            ("NotoSans-Black.ttf", [
                "https://github.com/notofonts/notofonts.github.io/raw/main/fonts/NotoSans/hinted/ttf/NotoSans-Black.ttf",
                "https://cdn.jsdelivr.net/gh/notofonts/notofonts.github.io@main/fonts/NotoSans/hinted/ttf/NotoSans-Black.ttf",
                "https://github.com/googlefonts/noto-fonts/raw/main/hinted/ttf/NotoSans/NotoSans-Black.ttf",
                "https://cdn.jsdelivr.net/gh/googlefonts/noto-fonts@main/hinted/ttf/NotoSans/NotoSans-Black.ttf",
            ]),
        ]
    elif len(raw) >= 3:
        files = [(raw[2].strip(), [raw[1].strip()])]
    else:
        await message.answer(
            "<b>Использование:</b>\n"
            "<code>/installfonts</code> — скачать NotoSans Bold + Black (с fallback URL)\n"
            "<code>/installfonts URL FILENAME</code> — кастомный шрифт"
        )
        return

    import asyncio
    import os
    from pathlib import Path as _Path
    from urllib.request import Request, urlopen

    fonts_dir = _Path(os.getenv("DATA_DIR", "/app/data")) / "fonts"
    fonts_dir.mkdir(parents=True, exist_ok=True)

    await message.answer(f"⏳ Скачиваю {len(files)} файлов в /app/data/fonts/ (с fallback) ...")

    def _try_urls(filename: str, urls: list[str]) -> tuple[bool, str]:
        last_err = ""
        for url in urls:
            try:
                req = Request(url, headers={"User-Agent": "sobirai-bot/1.0 (+https://github.com/Irjabik/sobirai)"})
                with urlopen(req, timeout=60) as resp:
                    if resp.status != 200:
                        last_err = f"HTTP {resp.status}"
                        continue
                    data = resp.read()
                if len(data) < 1024:
                    last_err = f"подозрительно мал ({len(data)} bytes)"
                    continue
                (fonts_dir / filename).write_bytes(data)
                return True, f"{len(data) // 1024} KB от {url.split('/')[2]}"
            except Exception as exc:
                last_err = f"{type(exc).__name__}: {exc}"
                continue
        return False, last_err

    results: list[str] = []
    errors: list[str] = []
    for filename, urls in files:
        ok, info = await asyncio.to_thread(_try_urls, filename, urls)
        if ok:
            results.append(f"✅ {filename} — {info}")
        else:
            errors.append(f"❌ {filename}: все {len(urls)} URL упали ({info})")

    msg = "\n".join(results + errors)
    if results:
        msg += "\n\nГотово. Шрифт подхватится при следующем рендере карточки."
    if errors:
        msg += (
            "\n\n<i>Если все URL упали — у Bothost нет доступа к GitHub/jsdelivr. "
            "Карточка всё равно работает: на Linux обычно стоит DejaVu Sans, "
            "Pillow его подхватит автоматически. Кириллица будет читаемой.</i>"
        )
    await message.answer(msg)


def _lobe_urls(slug: str) -> list[str]:
    """lobehub/lobe-icons — коллекция AI-брендов в PNG. Два формата: light/dark/color."""
    base = "https://raw.githubusercontent.com/lobehub/lobe-icons/master/packages/static-png"
    cdn = "https://cdn.jsdelivr.net/gh/lobehub/lobe-icons@master/packages/static-png"
    return [
        f"{base}/dark/{slug}.png",
        f"{base}/light/{slug}.png",
        f"{base}/color/{slug}.png",
        f"{cdn}/dark/{slug}.png",
        f"{cdn}/light/{slug}.png",
        f"{cdn}/color/{slug}.png",
    ]


DEFAULT_AI_LOGOS: tuple[tuple[str, list[str]], ...] = (
    ("openai", _lobe_urls("openai")),
    ("anthropic", _lobe_urls("anthropic")),
    ("google", _lobe_urls("google")),
    ("meta", _lobe_urls("meta")),
    ("microsoft", _lobe_urls("microsoft")),
    ("nvidia", _lobe_urls("nvidia")),
    ("apple", _lobe_urls("apple")),
    ("amazon", _lobe_urls("aws")),
    ("xai", _lobe_urls("xai")),
    ("perplexity", _lobe_urls("perplexity")),
    ("mistral", _lobe_urls("mistral")),
    ("deepmind", _lobe_urls("deepmind")),
    ("huggingface", _lobe_urls("huggingface")),
    ("deepseek", _lobe_urls("deepseek")),
    ("cohere", _lobe_urls("cohere")),
)


@router.message(Command("installlogos"))
async def cmd_installlogos(
    message: Message,
    settings: Settings,
) -> None:
    """Скачивает топ-15 AI-логотипов разом. URL'ы Wikipedia, гарантированно PNG."""
    if not _is_admin(message, settings):
        return

    import asyncio
    import os
    from pathlib import Path as _Path
    from urllib.request import Request, urlopen

    logos_dir = _Path(os.getenv("DATA_DIR", "/app/data")) / "logos"
    logos_dir.mkdir(parents=True, exist_ok=True)

    await message.answer(
        f"⏳ Скачиваю {len(DEFAULT_AI_LOGOS)} логотипов в /app/data/logos/ ...\n"
        "<i>(может занять 30-60 сек)</i>"
    )

    def _download_with_fallback(company_id: str, urls: list[str]) -> tuple[bool, str]:
        last = ""
        for url in urls:
            try:
                req = Request(url, headers={"User-Agent": "sobirai-bot/1.0 (+https://github.com/Irjabik/sobirai)"})
                with urlopen(req, timeout=45) as resp:
                    if resp.status != 200:
                        last = f"HTTP {resp.status}"
                        continue
                    data = resp.read()
                if not data.startswith(b"\x89PNG"):
                    last = "не PNG"
                    continue
                (logos_dir / f"{company_id}.png").write_bytes(data)
                return True, f"{len(data) // 1024} KB"
            except Exception as exc:
                last = f"{type(exc).__name__}"
                continue
        return False, last

    ok_lines: list[str] = []
    err_lines: list[str] = []
    for company_id, urls in DEFAULT_AI_LOGOS:
        ok, info = await asyncio.to_thread(_download_with_fallback, company_id, urls)
        if ok:
            ok_lines.append(f"✅ {company_id} ({info})")
        else:
            err_lines.append(f"❌ {company_id}: {info}")

    body = "\n".join(ok_lines + err_lines)
    body += (
        f"\n\n<b>Итого:</b> {len(ok_lines)}/{len(DEFAULT_AI_LOGOS)}\n\n"
    )
    if err_lines:
        body += (
            "<i>Для непрошедших — найди прямую ссылку на PNG и залей вручную:</i>\n"
            "<code>/uploadlogo COMPANY_ID URL</code>"
        )
    else:
        body += "Готово. Рендер карточек уже умеет их подхватывать."
    if len(body) > 4000:
        body = body[:3990] + "…"
    await message.answer(body)


@router.message(Command("uploadlogo"))
async def cmd_uploadlogo(
    message: Message,
    settings: Settings,
) -> None:
    """Скачивает PNG-логотип компании и кладёт в /app/data/logos/<company_id>.png.

    Использование:
      /uploadlogo openai https://upload.wikimedia.org/.../OpenAI_logo.svg.png
    """
    if not _is_admin(message, settings):
        return

    raw = (message.text or "").split(maxsplit=2)
    if len(raw) < 3:
        await message.answer(
            "<b>Использование:</b>\n"
            "<code>/uploadlogo COMPANY_ID URL</code>\n\n"
            "Пример:\n"
            "<code>/uploadlogo openai https://upload.wikimedia.org/wikipedia/commons/4/4d/OpenAI_Logo.svg.png</code>\n\n"
            "COMPANY_ID должно совпадать с тем что LLM возвращает в meta.company_id "
            "(openai, anthropic, google, meta, microsoft, nvidia, apple, amazon, xai, perplexity, mistral, deepseek, deepmind, huggingface, cohere)."
        )
        return

    company_id = raw[1].strip().lower()
    url = raw[2].strip()
    if not company_id.isascii() or not all(c.isalnum() or c in "-_" for c in company_id):
        await message.answer("❌ COMPANY_ID должен быть ASCII (только буквы, цифры, '-', '_').")
        return
    if not url.startswith(("http://", "https://")):
        await message.answer("❌ Это не HTTP URL.")
        return

    import asyncio
    import os
    from pathlib import Path as _Path
    from urllib.request import Request, urlopen

    logos_dir = _Path(os.getenv("DATA_DIR", "/app/data")) / "logos"
    logos_dir.mkdir(parents=True, exist_ok=True)
    dest = logos_dir / f"{company_id}.png"

    await message.answer(f"⏳ Скачиваю {url} → {dest.name} ...")

    def _download() -> tuple[bool, str]:
        try:
            req = Request(url, headers={"User-Agent": "sobirai-bot/1.0"})
            with urlopen(req, timeout=120) as resp:
                if resp.status != 200:
                    return False, f"HTTP {resp.status}"
                data = resp.read()
            # Грубая валидация: PNG начинается с \x89PNG
            if not data.startswith(b"\x89PNG"):
                return False, f"Не похоже на PNG (первые байты: {data[:8]!r})"
            dest.write_bytes(data)
            return True, f"сохранено {len(data) // 1024} KB"
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"

    ok, info = await asyncio.to_thread(_download)
    if ok:
        await message.answer(
            f"✅ /app/data/logos/{company_id}.png — {info}\n\n"
            "При следующей генерации карточки для этой компании логотип появится."
        )
    else:
        await message.answer(f"❌ Не получилось: {info}")


@router.message(Command("listlogos"))
async def cmd_listlogos(
    message: Message,
    settings: Settings,
) -> None:
    """Список залитых логотипов в /app/data/logos/."""
    if not _is_admin(message, settings):
        return

    import os
    from pathlib import Path as _Path

    logos_dir = _Path(os.getenv("DATA_DIR", "/app/data")) / "logos"
    if not logos_dir.is_dir():
        await message.answer("Папка /app/data/logos/ ещё не создана. Залей первый: /uploadlogo")
        return

    files = sorted(logos_dir.glob("*.png"))
    if not files:
        await message.answer("Логотипов пока нет. Залей: <code>/uploadlogo openai &lt;URL&gt;</code>")
        return

    lines = [f"<b>Логотипов в /app/data/logos/:</b> {len(files)}", ""]
    for f in files:
        size_kb = f.stat().st_size // 1024
        lines.append(f"• <code>{f.stem}</code> ({size_kb} KB)")
    await message.answer("\n".join(lines))


@router.message(Command("installwkhtml"))
async def cmd_installwkhtml(
    message: Message,
    settings: Settings,
) -> None:
    """Скачивает статический wkhtmltoimage в /app/data/bin/.

    Использование:
      /installwkhtml URL_К_БИНАРНИКУ
      /installwkhtml — показать инструкцию и список рабочих URL.

    wkhtmltoimage умеет рендерить HTML+CSS через headless Chromium-like
    в PNG. Это даёт идеальную типографику для карточек канала по
    сравнению с Pillow.
    """
    if not _is_admin(message, settings):
        return

    raw = (message.text or "").split(maxsplit=1)
    if len(raw) < 2:
        await message.answer(
            "<b>Установка wkhtmltoimage (HTML → PNG)</b>\n\n"
            "Это «правильный» рендерер карточек: HTML+CSS через headless "
            "Chromium даёт идеальную типографику. Заменяет ручной Pillow.\n\n"
            "<b>Шаг 1.</b> Найди подходящий URL для Linux x86_64:\n"
            "Официальные релизы: https://github.com/wkhtmltopdf/packaging/releases\n\n"
            "Прямой бинарник (без deb-обёртки):\n"
            "<code>https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6.1-3/wkhtmltox-0.12.6.1-3.ubuntu-22.04.amd64.deb</code>\n\n"
            "<i>Это .deb — бот распакует, найдёт внутри wkhtmltoimage</i>\n\n"
            "<b>Шаг 2.</b>\n"
            "<code>/installwkhtml URL</code>\n\n"
            "<b>Шаг 3.</b> После установки следующий вызов "
            "«🎨 Сгенерировать фото» автоматически использует HTML-рендер.\n\n"
            "Проверить: /diaghtml"
        )
        return

    url = raw[1].strip()
    if not url.startswith(("http://", "https://")):
        await message.answer("❌ Это не HTTP URL.")
        return

    import asyncio
    import os
    import subprocess
    import tarfile
    import tempfile
    from pathlib import Path as _Path
    from urllib.request import Request, urlopen

    bin_dir = _Path(os.getenv("DATA_DIR", "/app/data")) / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)

    await message.answer(f"⏳ Скачиваю {url} ...")

    def _install() -> tuple[bool, str]:
        try:
            req = Request(url, headers={"User-Agent": "sobirai-bot/1.0"})
            with urlopen(req, timeout=300) as resp:
                if resp.status != 200:
                    return False, f"HTTP {resp.status}"
                with tempfile.NamedTemporaryFile(delete=False, suffix=_Path(url).suffix) as tmp:
                    tmp_path = tmp.name
                    while True:
                        chunk = resp.read(1024 * 1024)
                        if not chunk:
                            break
                        tmp.write(chunk)
            size_mb = _Path(tmp_path).stat().st_size / 1024 / 1024
            # Распаковываем .deb (это просто ar-архив с tar внутри)
            found: list[str] = []
            if url.endswith(".deb"):
                with tempfile.TemporaryDirectory() as td:
                    # ar -x <deb> → data.tar.xz
                    r = subprocess.run(["ar", "x", tmp_path], cwd=td, capture_output=True)
                    if r.returncode != 0:
                        return False, f"ar x failed: {r.stderr.decode('utf-8', errors='replace')[:200]}"
                    data_tar = None
                    for ext in ("data.tar.xz", "data.tar.gz", "data.tar.zst", "data.tar"):
                        p = _Path(td) / ext
                        if p.is_file():
                            data_tar = p
                            break
                    if not data_tar:
                        return False, "data.tar.* not found in .deb"
                    with tarfile.open(str(data_tar), "r:*") as tar:
                        for m in tar.getmembers():
                            base = _Path(m.name).name
                            if base in ("wkhtmltoimage", "wkhtmltopdf") and m.isfile():
                                extracted = tar.extractfile(m)
                                if extracted is None:
                                    continue
                                dest = bin_dir / base
                                with open(dest, "wb") as out:
                                    while True:
                                        blk = extracted.read(1024 * 1024)
                                        if not blk:
                                            break
                                        out.write(blk)
                                dest.chmod(0o755)
                                found.append(f"{base} ({dest.stat().st_size // 1024 // 1024} MB)")
            elif url.endswith((".tar.xz", ".tar.gz", ".tgz")):
                with tarfile.open(tmp_path, "r:*") as tar:
                    for m in tar.getmembers():
                        base = _Path(m.name).name
                        if base in ("wkhtmltoimage", "wkhtmltopdf") and m.isfile():
                            extracted = tar.extractfile(m)
                            if extracted is None:
                                continue
                            dest = bin_dir / base
                            with open(dest, "wb") as out:
                                while True:
                                    blk = extracted.read(1024 * 1024)
                                    if not blk:
                                        break
                                    out.write(blk)
                            dest.chmod(0o755)
                            found.append(f"{base} ({dest.stat().st_size // 1024 // 1024} MB)")
            else:
                # Прямой бинарник
                dest = bin_dir / "wkhtmltoimage"
                import shutil as _shutil
                _shutil.move(tmp_path, dest)
                dest.chmod(0o755)
                found.append(f"wkhtmltoimage ({dest.stat().st_size // 1024 // 1024} MB)")
            try:
                _Path(tmp_path).unlink(missing_ok=True)
            except Exception:
                pass
            if not found:
                return False, f"Архив скачан ({size_mb:.1f} MB), но wkhtmltoimage не найден."
            return True, ", ".join(found)
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"

    ok, info = await asyncio.to_thread(_install)
    if ok:
        await message.answer(
            f"✅ Установлено в /app/data/bin/: {info}\n\n"
            "Следующая генерация карточки автоматически пойдёт через HTML+CSS. "
            "Проверка: /diaghtml"
        )
    else:
        await message.answer(f"❌ Не получилось: {info}")


@router.message(Command("diaghtml"))
async def cmd_diaghtml(
    message: Message,
    settings: Settings,
) -> None:
    """Диагностика HTML-рендерера карточек."""
    if not _is_admin(message, settings):
        return

    from .image_html_renderer import _find_wkhtmltoimage, html_renderer_available
    import os
    from pathlib import Path as _Path

    path = _find_wkhtmltoimage()
    bin_dir = _Path(os.getenv("DATA_DIR", "/app/data")) / "bin"

    lines = [
        "<b>🎨 HTML-рендерер карточек</b>",
        "",
        f"wkhtmltoimage найден: {'✅' if path else '❌'}",
        f"  path: <code>{path or '(не найден)'}</code>",
    ]
    if path:
        p = _Path(path)
        if p.is_file():
            size_mb = p.stat().st_size / 1024 / 1024
            lines.append(f"  размер: {size_mb:.1f} MB")
    lines.extend([
        "",
        f"Bin-папка: <code>{bin_dir}</code>",
    ])
    if bin_dir.is_dir():
        for f in sorted(bin_dir.iterdir()):
            size_kb = f.stat().st_size // 1024
            lines.append(f"  • {f.name} ({size_kb} KB)")
    else:
        lines.append("  <i>(не создана)</i>")

    lines.extend([
        "",
        f"Активный рендерер: <b>{'HTML+CSS (wkhtmltoimage)' if html_renderer_available() else 'Pillow fallback'}</b>",
        "",
        "Если wkhtmltoimage ❌ — карточки рендерятся через Pillow.",
        "Поставить: /installwkhtml",
    ])
    await message.answer("\n".join(lines))


@router.message(Command("installffmpeg"))
async def cmd_installffmpeg(
    message: Message,
    db: Database,
    settings: Settings,
) -> None:
    """Скачивает статический ffmpeg по URL и кладёт в /app/data/ (переживает деплои).

    Использование:
      /installffmpeg https://johnvansickle.com/ffmpeg/builds/ffmpeg-release-amd64-static.tar.xz

    Поддерживает .tar.xz / .tar.gz архивы с ffmpeg+ffprobe внутри, либо прямую
    ссылку на бинарник ffmpeg.
    """
    if not _is_admin(message, settings):
        return

    raw = (message.text or "").split(maxsplit=1)
    if len(raw) < 2:
        await message.answer(
            "<b>Использование:</b>\n"
            "<code>/installffmpeg URL_АРХИВА</code>\n\n"
            "Готовый статический билд для Linux x86_64:\n"
            "<code>https://johnvansickle.com/ffmpeg/builds/ffmpeg-release-amd64-static.tar.xz</code>\n\n"
            "Бот скачает архив, найдёт внутри ffmpeg и ffprobe и положит их в /app/data/. "
            "После Restart бот их подхватит и видео начнут транскодироваться."
        )
        return

    url = raw[1].strip()
    if not url.startswith(("http://", "https://")):
        await message.answer("❌ Это не HTTP URL.")
        return

    import os
    import asyncio
    import tarfile
    import tempfile
    import shutil as _shutil
    from pathlib import Path as _Path
    from urllib.request import Request, urlopen

    data_dir = _Path(os.getenv("DATA_DIR", "/app/data"))
    data_dir.mkdir(parents=True, exist_ok=True)

    await message.answer(f"⏳ Скачиваю {url} ...")

    def _download_and_extract() -> tuple[bool, str]:
        try:
            req = Request(url, headers={"User-Agent": "sobirai-bot/1.0"})
            with urlopen(req, timeout=180) as resp:
                if resp.status != 200:
                    return False, f"HTTP {resp.status}"
                with tempfile.NamedTemporaryFile(delete=False, suffix=_Path(url).suffix or ".bin") as tmp:
                    tmp_path = tmp.name
                    while True:
                        chunk = resp.read(1024 * 1024)
                        if not chunk:
                            break
                        tmp.write(chunk)
            size_mb = _Path(tmp_path).stat().st_size / 1024 / 1024
            found = []
            # Архив?
            if url.endswith((".tar.xz", ".tar.gz", ".tar.bz2", ".tgz", ".tar")):
                with tarfile.open(tmp_path, "r:*") as tar:
                    members = tar.getmembers()
                    for m in members:
                        base = _Path(m.name).name
                        if base in ("ffmpeg", "ffprobe") and m.isfile():
                            extracted = tar.extractfile(m)
                            if extracted is None:
                                continue
                            dest = data_dir / base
                            with open(dest, "wb") as out:
                                while True:
                                    blk = extracted.read(1024 * 1024)
                                    if not blk:
                                        break
                                    out.write(blk)
                            dest.chmod(0o755)
                            found.append(f"{base} ({dest.stat().st_size // 1024 // 1024} MB)")
            else:
                # Прямой бинарник
                fname = _Path(url).name.lower()
                if "ffprobe" in fname:
                    dest = data_dir / "ffprobe"
                else:
                    dest = data_dir / "ffmpeg"
                _shutil.move(tmp_path, dest)
                dest.chmod(0o755)
                found.append(f"{dest.name} ({dest.stat().st_size // 1024 // 1024} MB)")
                tmp_path = None
            try:
                if tmp_path:
                    _Path(tmp_path).unlink(missing_ok=True)
            except Exception:
                pass
            if not found:
                return False, f"Архив скачан ({size_mb:.1f} MB), но ffmpeg/ffprobe внутри не найдены."
            return True, ", ".join(found)
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"

    ok, info = await asyncio.to_thread(_download_and_extract)
    if not ok:
        await message.answer(f"❌ Не получилось: {info}")
        return

    await message.answer(
        f"✅ Распаковано в /app/data/: {info}\n\n"
        "Теперь нажми <b>Restart</b> в Bothost. После старта /diagvideo покажет ffmpeg ✅. "
        "Не забудь снять <code>CHANNEL_VIDEO_NO_COMPRESSION</code> в env (или поставить 0)."
    )


@router.message(Command("diagvideo"))
async def cmd_diagvideo(
    message: Message,
    db: Database,
    settings: Settings,
) -> None:
    """Диагностика видео-пайплайна: есть ли ffmpeg, какие настройки, что в БД у последних видео."""
    if not _is_admin(message, settings):
        return

    from .ffmpeg_runtime import (
        FFMPEG_PATH, FFPROBE_PATH, ffmpeg_available, ffprobe_available,
    )
    import os
    import importlib.metadata
    from datetime import datetime as _dt, timezone as _tz
    from pathlib import Path as _Path

    # Сколько весит pip-пакетный бинарник
    ffmpeg_size_mb = None
    if FFMPEG_PATH and _Path(FFMPEG_PATH).is_file():
        ffmpeg_size_mb = round(_Path(FFMPEG_PATH).stat().st_size / 1024 / 1024, 1)

    # Какие версии pip-пакетов установлены сейчас в процессе
    pkg_versions: dict[str, str] = {}
    for pkg in ("static-ffmpeg", "static_ffmpeg", "imageio-ffmpeg", "imageio_ffmpeg", "imageio"):
        try:
            pkg_versions[pkg] = importlib.metadata.version(pkg)
        except importlib.metadata.PackageNotFoundError:
            pass

    # Версия кода (mtime ffmpeg_runtime.py)
    rt_path = _Path(__file__).parent / "ffmpeg_runtime.py"
    rt_mtime = ""
    if rt_path.is_file():
        rt_mtime = _dt.fromtimestamp(rt_path.stat().st_mtime, tz=_tz.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Если ffmpeg не нашёлся — пробуем ещё раз вручную и ловим исключения
    manual_static_err = ""
    manual_imageio_err = ""
    if not FFMPEG_PATH:
        try:
            from static_ffmpeg import add_paths  # type: ignore
            add_paths()
            import shutil as _shutil
            f1 = _shutil.which("ffmpeg")
            manual_static_err = f"add_paths() ok, which→{f1 or 'None'}"
        except Exception as e:
            manual_static_err = f"{type(e).__name__}: {e}"
        try:
            import imageio_ffmpeg  # type: ignore
            p = imageio_ffmpeg.get_ffmpeg_exe()
            manual_imageio_err = f"get_ffmpeg_exe→{p if p else 'None'}"
        except Exception as e:
            manual_imageio_err = f"{type(e).__name__}: {e}"

    # Последние 5 видео из source_posts с метаданными
    async with db.conn.execute(
        """
        SELECT id, channel_username, media_type, media_path, media_file_id,
               media_duration, media_width, media_height, media_thumb_path
          FROM source_posts
         WHERE media_type='video'
         ORDER BY id DESC
         LIMIT 5
        """,
    ) as cur:
        videos = [dict(row) for row in await cur.fetchall()]

    lines = [
        "<b>🎬 Диагностика видео-пайплайна</b>",
        "",
        "<b>ffmpeg/ffprobe:</b>",
        f"  ffmpeg доступен: {'✅' if ffmpeg_available() else '❌'}",
        f"  ffprobe доступен: {'✅' if ffprobe_available() else '❌'}",
        f"  ffmpeg path: <code>{FFMPEG_PATH or '(не найден)'}</code>",
        f"  ffprobe path: <code>{FFPROBE_PATH or '(не найден)'}</code>",
    ]
    if ffmpeg_size_mb is not None:
        lines.append(f"  размер бинарника: {ffmpeg_size_mb} MB")
    lines.extend([
        "",
        f"<b>Версия кода:</b> ffmpeg_runtime.py mtime <code>{rt_mtime or '?'}</code>",
        "",
        "<b>Установлено в pip:</b>",
    ])
    if pkg_versions:
        for pkg, ver in pkg_versions.items():
            lines.append(f"  • {pkg}=={ver}")
    else:
        lines.append("  <i>(ничего из ffmpeg-пакетов не найдено в окружении)</i>")

    if not FFMPEG_PATH:
        lines.extend([
            "",
            "<b>Ручные попытки резолва:</b>",
            f"  static-ffmpeg: <code>{manual_static_err or '—'}</code>",
            f"  imageio-ffmpeg: <code>{manual_imageio_err or '—'}</code>",
        ])
    lines.extend([
        "",
        "<b>Настройки:</b>",
        f"  ENABLE_CHANNEL_VIDEO_TRANSCODE: {'✅ on' if settings.enable_channel_video_transcode else '❌ off'}",
        f"  CHANNEL_VIDEO_NO_COMPRESSION: {'⚠️ on (skip transcode)' if settings.channel_video_no_compression else '✅ off (transcode applied)'}",
        f"  CHANNEL_VIDEO_MAX_INPUT_MB: {settings.channel_video_max_input_mb}",
        "",
        f"<b>Последние видео в БД ({len(videos)}):</b>",
    ])

    if not videos:
        lines.append("  <i>(нет видео)</i>")
    else:
        for v in videos:
            local_path = v.get("media_path") or ""
            local_exists = "✅" if local_path and os.path.isfile(local_path) else "❌"
            size_mb = ""
            if local_path and os.path.isfile(local_path):
                size_mb = f", {round(os.path.getsize(local_path) / 1024 / 1024, 1)} MB"
            duration = v.get("media_duration")
            width = v.get("media_width")
            height = v.get("media_height")
            thumb = v.get("media_thumb_path") or ""
            thumb_ok = "✅" if thumb and os.path.isfile(thumb) else "❌"
            meta_ok = "✅" if (duration and width and height) else "❌"
            lines.extend([
                "",
                f"  id={v['id']} {v['channel_username']}",
                f"    локальный файл: {local_exists}{size_mb}",
                f"    метаданные (d/w/h): {meta_ok} ({duration}/{width}/{height})",
                f"    thumbnail: {thumb_ok}",
            ])

    lines.extend([
        "",
        "<b>Что означает:</b>",
        "• ffmpeg ❌ → транскодинг не работает, видео уходит как documents",
        "• метаданные ❌ → у Telegram нет width/height/duration → облачко",
        "• thumbnail ❌ → нет превью первого кадра → клиент не разворачивает",
        "• VIDEO_NO_COMPRESSION on → пропускаем transcode (если original H264 — норм)",
    ])

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3990] + "…"
    await message.answer(text)


@router.message(Command("setchannel"))
async def cmd_setchannel(
    message: Message,
    db: Database,
    bot: Bot,
    settings: Settings,
) -> None:
    """Сохраняет CHANNEL_CHAT_ID в bot.db (обход для Bothost не пробрасывающего ENV).

    Использование:
      /setchannel -1001234567890   — задать целевой канал
      /setchannel @automyai         — взять chat_id по @username (бот должен быть админом)
      /setchannel -                  — очистить (вернуться к ENV)
    """
    if not _is_admin(message, settings):
        return

    raw = (message.text or "").split(maxsplit=1)
    if len(raw) < 2:
        await message.answer(
            "<b>Использование:</b>\n"
            "<code>/setchannel -1001234567890</code> — указать chat_id явно\n"
            "<code>/setchannel @automyai</code> — взять chat_id по username (бот должен быть в канале админом)\n"
            "<code>/setchannel -</code> — очистить (вернуться к ENV)\n\n"
            f"Сейчас активен: <code>{settings.channel_chat_id}</code>"
        )
        return

    payload = raw[1].strip()

    if payload == "-":
        await db.set_bot_secret("channel_chat_id", "")
        await message.answer(
            "✅ channel_chat_id в БД очищен. После Restart бот будет читать только ENV."
        )
        return

    chat_id: int | None = None
    if payload.startswith("@") or (not payload.lstrip("-").isdigit()):
        # Резолвим username через getChat
        try:
            chat = await bot.get_chat(payload if payload.startswith("@") else f"@{payload}")
            chat_id = int(chat.id)
            chat_title = chat.title or chat.username or str(chat_id)
            await message.answer(
                f"Найден канал: <b>{html_escape_safe(chat_title)}</b>\n"
                f"chat_id: <code>{chat_id}</code>\n\n"
                f"Сохраняю..."
            )
        except Exception as exc:
            await message.answer(
                f"❌ Не получилось получить chat_id по {payload}.\n"
                f"Ошибка: {exc!s}\n\n"
                "Проверь что бот добавлен в канал админом, или укажи chat_id вручную: <code>/setchannel -100...</code>"
            )
            return
    else:
        try:
            chat_id = int(payload)
        except ValueError:
            await message.answer(
                f"❌ <code>{payload}</code> — не похоже на chat_id и не на @username."
            )
            return

    if chat_id is None or chat_id >= 0:
        await message.answer(
            f"❌ chat_id канала должен начинаться с -100... Получено: <code>{chat_id}</code>"
        )
        return

    await db.set_bot_secret("channel_chat_id", str(chat_id))
    await message.answer(
        f"✅ Сохранено в БД: <code>{chat_id}</code>\n\n"
        "Теперь нажми <b>Restart</b> в Bothost — бот подхватит при следующем старте.\n\n"
        "<i>Не забудь добавить бота в канал админом с правом «Post messages», "
        "иначе публикации будут падать с «not enough rights».</i>"
    )


@router.message(Command("getchannel"))
async def cmd_getchannel(
    message: Message,
    db: Database,
    settings: Settings,
) -> None:
    """Показывает активный CHANNEL_CHAT_ID и значение в БД."""
    if not _is_admin(message, settings):
        return

    db_value = await db.get_bot_secret("channel_chat_id") or ""
    await message.answer(
        "<b>CHANNEL_CHAT_ID — текущее состояние</b>\n\n"
        f"Активный (settings): <code>{settings.channel_chat_id}</code>\n"
        f"В БД (bot_secrets): <code>{db_value or '(пусто)'}</code>\n\n"
        "Если в БД есть, а в активном другой — нужен Restart бота, чтобы подхватить."
    )


def html_escape_safe(value: str) -> str:
    import html as _html
    return _html.escape(value or "")


@router.message(Command("setadmins"))
async def cmd_setadmins(
    message: Message,
    db: Database,
    settings: Settings,
) -> None:
    """Сохраняет список admin_chat_ids в bot.db (обход для Bothost не пробрасывающего ENV).

    Использование:
      /setadmins 1038987193,889623800   — задать список (через запятую)
      /setadmins -                       — очистить (вернуться к ENV)

    После сохранения нужен Restart бота. Файл bot.db переживает деплои.
    """
    if not _is_admin(message, settings):
        return  # тихо

    raw = (message.text or "").split(maxsplit=1)
    if len(raw) < 2:
        await message.answer(
            "<b>Использование:</b>\n"
            "<code>/setadmins 1038987193,889623800</code> — задать список\n"
            "<code>/setadmins -</code> — очистить (вернуться к ENV)\n\n"
            "ID можно узнать через /myid (юзер пишет команду боту, получает свой ID)."
        )
        return

    payload = raw[1].strip()
    if payload == "-":
        await db.set_bot_secret("admin_chat_ids", "")
        await message.answer(
            "✅ Список админов в БД очищен. После Restart бот будет читать только ENV."
        )
        return

    ids: list[int] = []
    bad: list[str] = []
    for token in payload.replace(";", ",").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            ids.append(int(token))
        except ValueError:
            bad.append(token)
    if bad:
        await message.answer(
            f"❌ Не похоже на user_id: {', '.join(bad)}\n\n"
            "Должны быть только цифры через запятую: <code>1038987193,889623800</code>"
        )
        return
    if not ids:
        await message.answer("❌ Список пустой. Используй <code>/setadmins -</code> чтобы очистить.")
        return

    seen: set[int] = set()
    deduped = [i for i in ids if not (i in seen or seen.add(i))]
    value = ",".join(str(i) for i in deduped)
    await db.set_bot_secret("admin_chat_ids", value)
    pretty = "\n".join(f"  {i+1}. <code>{aid}</code>" for i, aid in enumerate(deduped))
    await message.answer(
        f"✅ Сохранено в БД ({len(deduped)} админов):\n{pretty}\n\n"
        "Теперь нажми <b>Restart</b> в Bothost. После старта оба админа начнут получать посты в личку.\n\n"
        "<i>Не забудь, что второй админ должен сам нажать /start у этого бота — иначе Telegram "
        "не разрешит боту писать ему первым.</i>"
    )


@router.message(Command("listadmins"))
async def cmd_listadmins(
    message: Message,
    db: Database,
    settings: Settings,
) -> None:
    """Показывает текущий список админов в БД и в ENV."""
    if not _is_admin(message, settings):
        return

    db_value = await db.get_bot_secret("admin_chat_ids") or ""
    db_ids = [t.strip() for t in db_value.split(",") if t.strip()]
    env_active = list(settings.admin_chat_ids)

    lines = [
        "<b>Админы — текущее состояние</b>",
        "",
        f"<b>Активны сейчас (settings.admin_chat_ids):</b> {len(env_active)}",
    ]
    for i, aid in enumerate(env_active, 1):
        lines.append(f"  {i}. <code>{aid}</code>")
    lines.extend([
        "",
        f"<b>В БД (bot_secrets.admin_chat_ids):</b> {len(db_ids)}",
    ])
    if db_ids:
        for i, aid in enumerate(db_ids, 1):
            lines.append(f"  {i}. <code>{aid}</code>")
    else:
        lines.append("  <i>(пусто)</i>")
    lines.extend([
        "",
        "Если в БД есть, а в активных нет — значит бот ещё не рестартовали после /setadmins.",
    ])
    await message.answer("\n".join(lines))


@router.callback_query(F.data.startswith("rev:"))
async def cb_review(
    query: CallbackQuery,
    db: Database,
    bot: Bot,
    metrics: RuntimeMetrics,
    settings: Settings,
    state: FSMContext,
) -> None:
    if not _is_admin(query, settings):
        await query.answer("Доступ только админу.", show_alert=True)
        return
    if query.data is None:
        await query.answer()
        return
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer()
        return
    action = parts[1]
    try:
        source_post_id = int(parts[2])
    except ValueError:
        await query.answer("Битый id")
        return

    # Late import чтобы избежать circular import.
    from .channel_autopublish import (
        _publish_generated_post,
        _send_review_preview_to_admin,
        review_edit_keyboard,
        review_main_keyboard,
    )

    if action == "pub":
        # Атомарный claim: только один админ из всех «выиграет» гонку и реально пойдёт в канал.
        claimed = await db.try_claim_for_publish(source_post_id)
        if not claimed:
            current_status = await db.get_generated_status(source_post_id) or "?"
            await query.answer(
                f"Уже обрабатывается другим админом (статус: {current_status}).",
                show_alert=True,
            )
            if query.message is not None:
                try:
                    await query.message.edit_reply_markup(reply_markup=None)
                except Exception:
                    pass
            return
        await query.answer("Публикую...")
        ok, info = await _publish_generated_post(
            db=db, bot=bot, metrics=metrics, settings=settings, source_post_id=source_post_id,
        )
        result_text = (
            f"✅ Опубликовано (msg_id={info})" if ok else f"❌ Не получилось: {info}"
        )
        if query.message is not None:
            try:
                await query.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            try:
                await query.message.answer(result_text)
            except Exception:
                logger.exception("Failed to send admin publish result")
        return

    if action == "skip":
        await query.answer("Пропущено")
        await db.update_generated_channel_post(
            source_post_id, status="skipped", error="admin_skipped"
        )
        if query.message is not None:
            try:
                await query.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            try:
                await query.message.answer(f"⏭ Пропущен пост id={source_post_id}")
            except Exception:
                pass
        return

    if action == "queue":
        # Показываем time picker — пресеты + кнопка ручного ввода.
        # Сам расчёт и запись в БД происходят в колбэке qslot/qmanual.
        await query.answer()
        if query.message is not None:
            try:
                await query.message.answer(
                    f"📅 Когда опубликовать пост id={source_post_id}?\n"
                    f"Выбери пресет или нажми «✏️ Своё время».",
                    reply_markup=_queue_time_picker_kb(source_post_id),
                )
            except Exception:
                logger.exception("queue picker show failed post=%s", source_post_id)
        return

    if action == "edit":
        # Меняем клавиатуру на меню редактирования
        await query.answer()
        if query.message is not None:
            try:
                await query.message.edit_reply_markup(
                    reply_markup=review_edit_keyboard(source_post_id)
                )
            except Exception:
                logger.exception("Failed to switch to edit keyboard")
        return

    if action == "back":
        # Возвращаемся к основным кнопкам с текущей оценкой
        await query.answer()
        existing = await db.get_post_feedback(source_post_id)
        current_rating = int(existing["rating"]) if existing else 0
        gen = await db.get_generated_channel_post_by_source_id(source_post_id)
        has_img = bool((gen or {}).get("admin_media_path"))
        if query.message is not None:
            try:
                await query.message.edit_reply_markup(
                    reply_markup=review_main_keyboard(
                        source_post_id,
                        current_rating=current_rating,
                        has_generated_image=has_img,
                    )
                )
            except Exception:
                logger.exception("Failed to switch back to main keyboard")
        return

    if action == "imggen":
        # Контрольная точка на самом входе в callback — пишется при ЛЮБОМ
        # нажатии «🎨 Сгенерировать фото». Видно в /diagimage в секции Trace.
        try:
            from datetime import datetime as _dtcp, timezone as _tzcp
            _cp_stamp = _dtcp.now(tz=_tzcp.utc).strftime("%H:%M:%S")
            await db.set_bot_secret(
                "last_imggen_trace",
                f"cb_imggen=entered | post_id={source_post_id} | {_cp_stamp} UTC",
            )
        except Exception:
            pass
        if not settings.enable_image_generation:
            await query.answer("Генерация фото выключена. /imagegen on", show_alert=True)
            return
        if not settings.openrouter_api_key:
            await query.answer("Нет OpenRouter API ключа. /setllmkey", show_alert=True)
            return
        # Дневной бюджет
        from datetime import datetime as _dt, timedelta as _td, timezone as _tz
        from pathlib import Path as _Path
        since = (_dt.now(tz=_tz.utc) - _td(days=1)).isoformat()
        stats = await db.get_image_gen_stats(since_iso=since)
        if stats["total_cost"] >= settings.image_gen_daily_budget_usd:
            await query.answer(
                f"Дневной бюджет ${settings.image_gen_daily_budget_usd:.2f} исчерпан "
                f"(потрачено ${stats['total_cost']:.3f}). /imagebudget — детали.",
                show_alert=True,
            )
            return

        gen = await db.get_generated_channel_post_by_source_id(source_post_id)
        if not gen:
            await query.answer("Нет данных по посту", show_alert=True)
            return
        title = str(gen.get("title") or "")
        post_text = str(gen.get("post_text") or "")

        await query.answer("⏳ Генерирую (5-15 сек)...")

        from .image_generator import generate_post_image
        import os as _os
        data_dir = _os.getenv("DATA_DIR", "/app/data")
        path = None
        prompt: str | None = None
        err: str | None = None
        try:
            path, prompt, err = await generate_post_image(
                source_post_id=source_post_id,
                title=title,
                post_text=post_text,
                api_key=settings.openrouter_api_key,
                data_dir=data_dir,
                image_model=settings.image_gen_model,
            )
        except Exception as exc:
            logger.exception("imggen crash for post %s", source_post_id)
            err = f"crash: {type(exc).__name__}: {exc}"

        await db.log_image_generation(
            source_post_id=source_post_id,
            prompt=prompt or "(empty)",
            model=settings.image_gen_model,
            cost_usd=settings.image_gen_cost_usd if path else 0.0,
            success=bool(path),
        )
        # Сохраним последнюю ошибку для /diagimage
        if err:
            from datetime import datetime as _dt, timezone as _tz
            stamp = _dt.now(tz=_tz.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            payload = f"post_id={source_post_id} | model={settings.image_gen_model} | {stamp}\n{err}"
            if prompt:
                payload += f"\nprompt: {prompt[:500]}"
            await db.set_bot_secret("last_image_gen_error", payload[:3500])

        if path is None:
            if query.message is not None:
                short_err = (err or "unknown")[:200]
                await query.message.answer(
                    f"❌ Не получилось сгенерировать.\n\n<code>{short_err}</code>\n\n"
                    "Подробности: /diagimage"
                )
            return
        # Сохраняем путь в БД и перевыпускаем превью с новой картинкой
        await db.update_generated_channel_post(
            source_post_id, admin_media_path=str(path),
        )
        from .channel_autopublish import _send_review_preview_to_admin
        sent_ok = False
        send_exc: Exception | None = None
        try:
            sent_ok = await _send_review_preview_to_admin(
                db=db, bot=bot, settings=settings, source_post_id=source_post_id,
                trace_key="last_imggen_trace",
            )
        except Exception as exc:
            # На случай, если функция всё-таки выбросит вверх. Обычно она
            # сама ловит TelegramAPIError на каждом админе и пишет в БД.
            logger.exception("preview send after imggen raised post=%s", source_post_id)
            send_exc = exc

        if not sent_ok or send_exc is not None:
            # Превью не дошло ни до одного админа. Внутри
            # _send_review_preview_to_admin последняя ошибка уже записана
            # в last_image_gen_error. Если вылетело наружу — дописываем
            # тип исключения отдельно.
            if send_exc is not None:
                from datetime import datetime as _dt2, timezone as _tz2
                stamp = _dt2.now(tz=_tz2.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                file_size = -1
                try:
                    file_size = int(_Path(str(path)).stat().st_size)
                except OSError:
                    pass
                await db.set_bot_secret(
                    "last_image_gen_error",
                    (
                        f"post_id={source_post_id} | stage=preview_send_outer | {stamp}\n"
                        f"file={path} size_bytes={file_size}\n"
                        f"{type(send_exc).__name__}: {send_exc}"
                    )[:3500],
                )
            if query.message is not None:
                await query.message.answer(
                    "⚠️ Фото сгенерировано, но превью не отправилось.\n"
                    "Подробности: /diagimage"
                )
            return
        # Полный успех — чистим прошлую ошибку только тут
        await db.set_bot_secret("last_image_gen_error", "")
        return

    if action == "imgrm":
        await query.answer("Фото убрано — пост уйдёт как text-only")
        await db.update_generated_channel_post(
            source_post_id, admin_media_path="",
        )
        from .channel_autopublish import _send_review_preview_to_admin
        await _send_review_preview_to_admin(
            db=db, bot=bot, settings=settings, source_post_id=source_post_id,
        )
        return

    if action == "edit_media":
        await state.set_state(MenuStates.editing_review_media)
        await state.update_data(review_source_post_id=source_post_id)
        await query.answer()
        if query.message is not None:
            await query.message.answer(
                f"📷 Пришли фото для поста id={source_post_id}.\n"
                "Просто отправь картинку из галереи или сделай новую.\n\n"
                "Если хочешь убрать админ-фото и вернуть оригинальное — пришли слово <code>сброс</code>.\n\n"
                "«Отмена» — выйти без правок.",
                reply_markup=cancel_reply(),
            )
        return

    if action in ("edit_title", "edit_body", "edit_tags"):
        # Подгружаю текущее значение поля и предлагаю прислать новое
        gen = await db.get_generated_channel_post_by_source_id(source_post_id)
        if not gen:
            await query.answer("Пост не найден")
            return

        if action == "edit_title":
            current = str(gen.get("title") or "").strip()
            await state.set_state(MenuStates.editing_review_title)
            prompt = (
                f"📝 Пришли новый <b>заголовок</b> для поста id={source_post_id}.\n"
                "6-14 слов, без эмодзи, без HTML.\n\n"
                f"<i>Сейчас:</i> {current}\n\n"
                "«Отмена» — выйти без правок."
            )
        elif action == "edit_body":
            current = str(gen.get("post_text") or "").strip()
            preview = current[:300] + ("…" if len(current) > 300 else "")
            await state.set_state(MenuStates.editing_review_body)
            prompt = (
                f"✏️ Пришли новое <b>тело поста</b> id={source_post_id} (без заголовка).\n"
                "Минимум 100 символов, можно несколько абзацев.\n\n"
                f"<i>Сейчас (начало):</i>\n{preview}\n\n"
                "«Отмена» — выйти без правок."
            )
        else:  # edit_tags
            current_tags = []
            try:
                import json as _json
                current_tags = _json.loads(gen.get("hashtags_json") or "[]")
                if not isinstance(current_tags, list):
                    current_tags = []
            except (TypeError, ValueError):
                current_tags = []
            current = " ".join(f"#{t}" for t in current_tags) if current_tags else "(пусто)"
            await state.set_state(MenuStates.editing_review_tags)
            prompt = (
                f"🏷 Пришли <b>хэштеги</b> для поста id={source_post_id}.\n"
                "До 3 штук, через пробел или запятую, символ # необязателен. Пример: openai gpt5 ai\n\n"
                f"<i>Сейчас:</i> {current}\n\n"
                "«Отмена» — выйти без правок."
            )

        await state.update_data(review_source_post_id=source_post_id)
        await query.answer()
        if query.message is not None:
            await query.message.answer(prompt, reply_markup=cancel_reply())
        return

    await query.answer()


async def _resend_preview_after_edit(
    db: Database,
    bot: Bot,
    metrics: RuntimeMetrics,
    settings: Settings,
    chat_id: int,
    source_post_id: int,
) -> None:
    from .channel_autopublish import _send_review_preview_to_admin
    sent = await _send_review_preview_to_admin(
        db=db, bot=bot, settings=settings, source_post_id=source_post_id,
    )
    if not sent:
        await bot.send_message(
            chat_id=chat_id,
            text="✅ Сохранено. Не получилось обновить превью — возьми старое сверху и нажми «Опубликовать».",
        )


def _is_cancel_message(message: Message) -> bool:
    if not message.text:
        return False
    return message.text.strip() == BTN_CANCEL or message.text in MAIN_MENU_LABELS


async def _get_review_post_id_from_state(state: FSMContext) -> int | None:
    data = await state.get_data()
    raw = data.get("review_source_post_id")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


@router.message(StateFilter(MenuStates.editing_review_title))
async def fsm_edit_review_title(
    message: Message,
    db: Database,
    bot: Bot,
    metrics: RuntimeMetrics,
    settings: Settings,
    state: FSMContext,
) -> None:
    if not _is_admin(message, settings):
        await state.clear()
        return
    if not message.text:
        return
    if _is_cancel_message(message):
        await state.clear()
        await message.answer("Окей, правку отменил.", reply_markup=main_menu_reply())
        return

    sid = await _get_review_post_id_from_state(state)
    if sid is None:
        await state.clear()
        await message.answer("Контекст потерян, начните заново.", reply_markup=main_menu_reply())
        return

    new_title = message.text.strip()
    if len(new_title) < 4 or len(new_title) > 200:
        await message.answer(
            "Заголовок должен быть от 4 до 200 символов. Повторите или «Отмена».",
            reply_markup=cancel_reply(),
        )
        return

    from .channel_autopublish import _strip_llm_html
    cleaned = _strip_llm_html(new_title).strip()

    await db.update_generated_channel_post(sid, title=cleaned, clear_error=True)
    await state.clear()
    await message.answer("📝 Заголовок обновлён, обновляю превью…", reply_markup=main_menu_reply())
    await _resend_preview_after_edit(db, bot, metrics, settings, message.chat.id, sid)


@router.message(StateFilter(MenuStates.editing_review_body))
async def fsm_edit_review_body(
    message: Message,
    db: Database,
    bot: Bot,
    metrics: RuntimeMetrics,
    settings: Settings,
    state: FSMContext,
) -> None:
    if not _is_admin(message, settings):
        await state.clear()
        return
    if not message.text:
        return
    if _is_cancel_message(message):
        await state.clear()
        await message.answer("Окей, правку отменил.", reply_markup=main_menu_reply())
        return

    sid = await _get_review_post_id_from_state(state)
    if sid is None:
        await state.clear()
        await message.answer("Контекст потерян, начните заново.", reply_markup=main_menu_reply())
        return

    new_body = message.text.strip()
    if len(new_body) < 100:
        await message.answer(
            "Слишком короткий текст (мин 100 символов). Пришлите содержательный пост или «Отмена».",
            reply_markup=cancel_reply(),
        )
        return

    from .channel_autopublish import (
        _strip_llm_html,
        _strip_useless_link_headers,
        _strip_dangling_pointer_emojis,
        _strip_linklike_cta_without_links,
        _beautify_links_block,
    )

    cleaned = _strip_llm_html(new_body)
    cleaned = _strip_useless_link_headers(cleaned)
    cleaned = _strip_dangling_pointer_emojis(cleaned)
    cleaned = _strip_linklike_cta_without_links(cleaned)
    cleaned = _beautify_links_block(cleaned)

    await db.update_generated_channel_post(sid, post_text=cleaned, clear_error=True)
    await state.clear()
    await message.answer("✏️ Тело обновлено, обновляю превью…", reply_markup=main_menu_reply())
    await _resend_preview_after_edit(db, bot, metrics, settings, message.chat.id, sid)


@router.message(StateFilter(MenuStates.editing_review_media))
async def fsm_edit_review_media(
    message: Message,
    db: Database,
    bot: Bot,
    metrics: RuntimeMetrics,
    settings: Settings,
    state: FSMContext,
) -> None:
    if not _is_admin(message, settings):
        await state.clear()
        return
    if _is_cancel_message(message):
        await state.clear()
        await message.answer("Окей, фото не меняем.", reply_markup=main_menu_reply())
        return

    sid = await _get_review_post_id_from_state(state)
    if sid is None:
        await state.clear()
        await message.answer("Контекст потерян, начните заново.", reply_markup=main_menu_reply())
        return

    # Сброс admin-фото
    if message.text and message.text.strip().lower() in ("сброс", "reset", "удалить"):
        await db.update_generated_channel_post(sid, admin_media_path="", clear_error=True)
        await state.clear()
        await message.answer(
            f"📷 Админ-фото убрано для поста id={sid}, вернётся оригинальное.\nОбновляю превью…",
            reply_markup=main_menu_reply(),
        )
        await _resend_preview_after_edit(db, bot, metrics, settings, message.chat.id, sid)
        return

    # Извлекаем file_id фото или документа-картинки
    file_id: str | None = None
    if message.photo:
        # photo — список PhotoSize по возрастанию размера, берём максимум
        file_id = message.photo[-1].file_id
    elif message.document and message.document.mime_type and message.document.mime_type.startswith("image/"):
        file_id = message.document.file_id

    if not file_id:
        await message.answer(
            "Не вижу фото в сообщении. Пришли картинку (как фото или документ).\n"
            "Или «сброс» чтобы убрать админ-фото, или «Отмена».",
            reply_markup=cancel_reply(),
        )
        return

    # Скачиваем файл в /app/data/media/admin_<sid>.jpg
    import os
    from pathlib import Path as _P
    media_dir = _P(os.getenv("DATA_DIR", "/app/data")) / "media"
    media_dir.mkdir(parents=True, exist_ok=True)
    out_path = media_dir / f"admin_{sid}.jpg"
    try:
        await bot.download(file_id, destination=out_path)
    except Exception as exc:
        logger.exception("Failed to download admin photo")
        await message.answer(f"Ошибка скачивания фото: {exc!s}\nПопробуй ещё раз.")
        return

    if not out_path.is_file() or out_path.stat().st_size == 0:
        await message.answer("Файл скачался пустым. Попробуй другую картинку.")
        return

    await db.update_generated_channel_post(sid, admin_media_path=str(out_path), clear_error=True)
    await state.clear()
    await message.answer(
        f"📷 Фото сохранено для поста id={sid} ({out_path.stat().st_size // 1024} KB).\n"
        "Обновляю превью…",
        reply_markup=main_menu_reply(),
    )
    await _resend_preview_after_edit(db, bot, metrics, settings, message.chat.id, sid)


@router.callback_query(F.data.startswith("rrate:"))
async def cb_review_rate(
    query: CallbackQuery,
    db: Database,
    settings: Settings,
) -> None:
    """Оценка поста ДО публикации, прямо на превью."""
    if not _is_admin(query, settings):
        await query.answer("Доступ только админу.", show_alert=True)
        return
    if query.data is None:
        await query.answer()
        return
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer()
        return
    try:
        rating = int(parts[1])
        source_post_id = int(parts[2])
    except ValueError:
        await query.answer("Битые данные")
        return
    if not (1 <= rating <= 5):
        await query.answer("Оценка 1-5")
        return

    await db.upsert_post_feedback(source_post_id, rating=rating)
    await query.answer(f"Оценка {rating}/5 сохранена")

    from .channel_autopublish import review_main_keyboard
    gen = await db.get_generated_channel_post_by_source_id(source_post_id)
    has_img = bool((gen or {}).get("admin_media_path"))
    if query.message is not None:
        try:
            await query.message.edit_reply_markup(
                reply_markup=review_main_keyboard(
                    source_post_id, current_rating=rating, has_generated_image=has_img,
                )
            )
        except Exception:
            logger.exception("Failed to refresh review keyboard with rating")


@router.callback_query(F.data.startswith("rate:"))
async def cb_rate(
    query: CallbackQuery,
    db: Database,
    settings: Settings,
    state: FSMContext,
) -> None:
    if not _is_admin(query, settings):
        await query.answer("Доступ только админу.", show_alert=True)
        return
    if query.data is None:
        await query.answer()
        return
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer()
        return
    action = parts[1]
    try:
        source_post_id = int(parts[2])
    except ValueError:
        await query.answer("Битый id")
        return

    from .channel_autopublish import feedback_rating_keyboard

    if action == "comment":
        await state.set_state(MenuStates.editing_feedback_comment)
        await state.update_data(feedback_source_post_id=source_post_id)
        await query.answer()
        if query.message is not None:
            await query.message.answer(
                f"💬 Пришли комментарий к посту id={source_post_id}.\n"
                "Что именно понравилось или нет — это поможет ИИ генерировать лучше.\n\n"
                "«Отмена» — выйти без правок.",
                reply_markup=cancel_reply(),
            )
        return

    # action = '1'..'5'
    if action.isdigit() and 1 <= int(action) <= 5:
        rating = int(action)
        await db.upsert_post_feedback(source_post_id, rating=rating)
        await query.answer(f"Оценка {rating}/5 сохранена")
        if query.message is not None:
            try:
                await query.message.edit_reply_markup(
                    reply_markup=feedback_rating_keyboard(source_post_id, current_rating=rating)
                )
            except Exception:
                pass
        return

    await query.answer()


@router.message(StateFilter(MenuStates.editing_feedback_comment))
async def fsm_edit_feedback_comment(
    message: Message,
    db: Database,
    settings: Settings,
    state: FSMContext,
) -> None:
    if not _is_admin(message, settings):
        await state.clear()
        return
    if not message.text:
        return
    if _is_cancel_message(message):
        await state.clear()
        await message.answer("Окей, комментарий отменил.", reply_markup=main_menu_reply())
        return

    data = await state.get_data()
    raw = data.get("feedback_source_post_id")
    try:
        source_post_id = int(raw)
    except (TypeError, ValueError):
        await state.clear()
        await message.answer("Контекст потерян, начните заново.", reply_markup=main_menu_reply())
        return

    comment = message.text.strip()[:1000]
    if len(comment) < 3:
        await message.answer(
            "Слишком коротко. Напиши хотя бы пару слов или «Отмена».",
            reply_markup=cancel_reply(),
        )
        return

    await db.upsert_post_feedback(source_post_id, comment=comment)
    await state.clear()
    await message.answer(
        f"💬 Комментарий сохранён к посту id={source_post_id}. Спасибо!",
        reply_markup=main_menu_reply(),
    )


@router.message(StateFilter(MenuStates.editing_review_tags))
async def fsm_edit_review_tags(
    message: Message,
    db: Database,
    bot: Bot,
    metrics: RuntimeMetrics,
    settings: Settings,
    state: FSMContext,
) -> None:
    if not _is_admin(message, settings):
        await state.clear()
        return
    if not message.text:
        return
    if _is_cancel_message(message):
        await state.clear()
        await message.answer("Окей, правку отменил.", reply_markup=main_menu_reply())
        return

    sid = await _get_review_post_id_from_state(state)
    if sid is None:
        await state.clear()
        await message.answer("Контекст потерян, начните заново.", reply_markup=main_menu_reply())
        return

    raw = message.text.strip()
    import json as _json, re as _re
    parts_raw = _re.split(r"[\s,;]+", raw)
    tags = []
    for p in parts_raw:
        p = p.strip().lstrip("#").lower()
        if p and len(p) <= 30 and p.isalnum():
            tags.append(p)
        if len(tags) >= 3:
            break

    await db.update_generated_channel_post(
        sid,
        hashtags_json=_json.dumps(tags, ensure_ascii=False),
        clear_error=True,
    )
    await state.clear()
    if tags:
        msg = "🏷 Хэштеги обновлены: " + " ".join(f"#{t}" for t in tags)
    else:
        msg = "🏷 Хэштеги очищены."
    await message.answer(msg + "\n\nОбновляю превью…", reply_markup=main_menu_reply())
    await _resend_preview_after_edit(db, bot, metrics, settings, message.chat.id, sid)
