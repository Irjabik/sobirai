from __future__ import annotations

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

from .sources import KEY_TO_CATEGORY, SOURCES

BTN_MODES = "Режимы"
BTN_DIGEST = "Дайджест"
BTN_FILTERS = "Фильтры"
BTN_SOURCES_HELP = "Источники и помощь"

MAIN_MENU_LABELS = frozenset({BTN_MODES, BTN_DIGEST, BTN_FILTERS, BTN_SOURCES_HELP})

BTN_CANCEL = "Отмена"

CATEGORY_ORDER = ("news", "tech", "author", "creative")

# (часы для callback dg:h:N, подпись). Остальные значения — «Свой интервал…».
DIGEST_PRESETS: tuple[tuple[int, str], ...] = (
    (1, "1 час"),
    (24, "1 день"),
    (168, "7 дней"),
)

CHANNELS_PER_PAGE = 8


def main_menu_reply() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_MODES), KeyboardButton(text=BTN_DIGEST)],
            [KeyboardButton(text=BTN_FILTERS), KeyboardButton(text=BTN_SOURCES_HELP)],
        ],
        resize_keyboard=True,
    )


def cancel_reply() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
        resize_keyboard=True,
    )


def inline_modes() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Мгновенно", callback_data="rg:i")],
            [
                InlineKeyboardButton(text="Пауза", callback_data="rg:p"),
                InlineKeyboardButton(text="Продолжить", callback_data="rg:r"),
            ],
            [
                InlineKeyboardButton(text="Mute вкл", callback_data="rg:m1"),
                InlineKeyboardButton(text="Mute выкл", callback_data="rg:m0"),
            ],
        ]
    )


def inline_digest() -> InlineKeyboardMarkup:
    row1 = [
        InlineKeyboardButton(text="Сейчас", callback_data="dg:n"),
    ]
    rows: list[list[InlineKeyboardButton]] = [row1]
    interval_row: list[InlineKeyboardButton] = []
    for hours, label in DIGEST_PRESETS:
        interval_row.append(
            InlineKeyboardButton(text=label, callback_data=f"dg:h:{hours}")
        )
        if len(interval_row) >= 3:
            rows.append(interval_row)
            interval_row = []
    if interval_row:
        rows.append(interval_row)
    rows.append(
        [InlineKeyboardButton(text="Свой интервал…", callback_data="dg:ask")]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="Фильтр времени: вкл", callback_data="dg:fn"
            ),
            InlineKeyboardButton(
                text="Фильтр времени: выкл", callback_data="dg:fo"
            ),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def inline_filters_category_rows(blocks: dict[str, bool]) -> list[list[InlineKeyboardButton]]:
    rows: list[list[InlineKeyboardButton]] = []
    for key in CATEGORY_ORDER:
        name = KEY_TO_CATEGORY[key]
        blocked = blocks.get(key, False)
        if blocked:
            label = f"Показать: {name}"
            data = f"fc:uc:{key}"
        else:
            label = f"Скрыть: {name}"
            data = f"fc:bc:{key}"
        rows.append([InlineKeyboardButton(text=label, callback_data=data)])
    return rows


def inline_filters_menu(blocks: dict[str, bool]) -> InlineKeyboardMarkup:
    rows = inline_filters_category_rows(blocks)
    rows.append(
        [InlineKeyboardButton(text="Мои фильтры", callback_data="fc:mf")]
    )
    rows.append(
        [
            InlineKeyboardButton(text="Скрыть канал (список)", callback_data="fc:cp:0"),
            InlineKeyboardButton(text="Вернуть канал (список)", callback_data="fc:up:0"),
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(text="Скрыть канал (@)", callback_data="fc:bc"),
            InlineKeyboardButton(text="Вернуть канал (@)", callback_data="fc:uc"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_picker_indices(blocked_indices: set[int], pick_block: bool) -> list[int]:
    """Индексы SOURCES: скрыть — только ещё не скрытые; вернуть — только скрытые."""
    n = len(SOURCES)
    if pick_block:
        return [i for i in range(n) if i not in blocked_indices]
    return [i for i in range(n) if i in blocked_indices]


def inline_channel_page(
    page: int,
    blocked_indices: set[int],
    pick_block: bool,
) -> InlineKeyboardMarkup:
    """pick_block=True: скрыть канал; False: вернуть канал."""
    eligible = channel_picker_indices(blocked_indices, pick_block)
    per = CHANNELS_PER_PAGE
    start = page * per
    chunk = eligible[start : start + per]
    rows: list[list[InlineKeyboardButton]] = []
    for i in chunk:
        username = SOURCES[i].username
        short = username if len(username) <= 28 else username[:25] + "…"
        if pick_block:
            data = f"fc:bi:{i}"
            label = f"− {short}"
        else:
            data = f"fc:ui:{i}"
            label = f"+ {short}"
        rows.append([InlineKeyboardButton(text=label, callback_data=data)])

    nav: list[InlineKeyboardButton] = []
    prefix = "fc:cp" if pick_block else "fc:up"
    if page > 0:
        nav.append(
            InlineKeyboardButton(text="«", callback_data=f"{prefix}:{page - 1}")
        )
    if start + per < len(eligible):
        nav.append(
            InlineKeyboardButton(text="»", callback_data=f"{prefix}:{page + 1}")
        )
    if nav:
        rows.append(nav)

    return InlineKeyboardMarkup(inline_keyboard=rows)


def inline_sources_help() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Список каналов", callback_data="src:list")],
            [InlineKeyboardButton(text="Помощь (команды)", callback_data="src:help")],
        ]
    )
