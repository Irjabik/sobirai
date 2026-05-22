"""Pillow-рендерер info-карточки для канала Automy AI.

Стиль: тёмный графит фон, скруглённая карточка, акцентный цвет под тип новости,
крупный логотип/название компании, главная цифра гигантом, подблок с деталью,
pill-бейдж. Текст всегда идеальный (мы рисуем сами), цена $0 (только DeepSeek).
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

CANVAS = 1024
BG_OUTER = (22, 22, 22)        # #161616 — фон всей картинки
CARD_BG = (31, 31, 31)         # #1f1f1f — карточка
CARD_BORDER = (48, 48, 48)     # #303030 — обводка
TEXT_WHITE = (245, 245, 245)
TEXT_MUTED = (140, 140, 140)
TEXT_WATERMARK = (90, 90, 90)
DIVIDER = (60, 60, 60)

# Палитра акцентов под типы новостей. DeepSeek выбирает по смыслу.
ACCENT_COLORS: dict[str, tuple[int, int, int]] = {
    "red":     (239, 68, 68),    # #ef4444 — иски, утечки, инциденты
    "orange":  (249, 115, 22),   # #f97316 — увольнения, регуляция
    "green":   (16, 185, 129),   # #10b981 — сделки, инвестиции
    "blue":    (59, 130, 246),   # #3b82f6 — релизы, API
    "purple":  (168, 85, 247),   # #a855f7 — mega-релизы, research
    "cyan":    (6, 182, 212),    # #06b6d4 — robotics, hardware
    "yellow":  (234, 179, 8),    # #eab308 — предупреждения
    "neutral": (148, 163, 184),  # #94a3b8 — нейтральные
}


@dataclass(frozen=True)
class CardMeta:
    company_label: str            # "OPENAI" / "ANTHROPIC" / "ROBOTICS" (если нет компании — тема)
    company_id: str | None        # "openai" / None — для поиска логотипа в /app/data/logos/<id>.png
    category_label: str           # "DATA LEAK" / "RELEASE" / "DEAL"
    main_value: str               # "ИСК" / "$4B" / "GPT-5"
    sub_label: str                # "PLAINTIFFS"
    sub_value: str                # "Calif. users"
    sub_caption: str = ""         # "(class action)" — может быть пустым
    pill_icon: str = ""           # эмодзи (рисуется как glyph если шрифт поддерживает)
    pill_text: str = ""           # "PRIVACY SCANDAL"
    accent: str = "neutral"       # ключ из ACCENT_COLORS


def _fonts_dir() -> Path:
    data_dir = os.getenv("DATA_DIR", "/app/data")
    return Path(data_dir) / "fonts"


def _logos_dir() -> Path:
    data_dir = os.getenv("DATA_DIR", "/app/data")
    return Path(data_dir) / "logos"


def _load_font(size: int, *, weight: str = "Bold"):
    """Загружает Inter-<weight>.ttf из /app/data/fonts/. Fallback на default."""
    try:
        from PIL import ImageFont
    except ImportError:
        return None
    fonts_dir = _fonts_dir()
    candidates = [
        fonts_dir / f"Inter-{weight}.ttf",
        fonts_dir / f"Inter-{weight.lower()}.ttf",
        fonts_dir / f"inter-{weight.lower()}.ttf",
    ]
    for path in candidates:
        if path.is_file():
            try:
                return ImageFont.truetype(str(path), size)
            except OSError:
                continue
    try:
        return ImageFont.load_default(size=size)
    except (TypeError, AttributeError):
        # Старые версии Pillow не принимают size в load_default
        return ImageFont.load_default()


def _text_with_spacing(draw, xy, text, font, fill, *, letter_spacing: int = 0, center_x: int | None = None) -> tuple[int, int]:
    """Рисует текст с межбуквенным интервалом. Возвращает (width, height) занятой области."""
    x, y = xy
    if center_x is not None:
        # Сначала измеряем суммарную ширину с интервалом
        total_w = 0
        chars = list(text)
        for i, ch in enumerate(chars):
            bbox = font.getbbox(ch)
            char_w = bbox[2] - bbox[0]
            total_w += char_w
            if i < len(chars) - 1:
                total_w += letter_spacing
        x = center_x - total_w // 2
    bbox_h = font.getbbox("Ag")[3] - font.getbbox("Ag")[1]
    cur_x = x
    for ch in text:
        draw.text((cur_x, y), ch, font=font, fill=fill)
        bbox = font.getbbox(ch)
        cur_x += (bbox[2] - bbox[0]) + letter_spacing
    return (cur_x - x, bbox_h)


def _measure_text(font, text: str, letter_spacing: int = 0) -> tuple[int, int]:
    total_w = 0
    chars = list(text)
    for i, ch in enumerate(chars):
        bbox = font.getbbox(ch)
        total_w += bbox[2] - bbox[0]
        if i < len(chars) - 1:
            total_w += letter_spacing
    bbox = font.getbbox(text or "Ag")
    h = bbox[3] - bbox[1]
    return total_w, h


def _draw_divider_with_label(draw, *, y: int, label: str, color: tuple[int, int, int], canvas_w: int) -> None:
    """Рисует «──── LABEL ────» c label по центру."""
    font = _load_font(20, weight="Bold")
    text_w, _ = _measure_text(font, label, letter_spacing=3)
    gap = 28
    line_len = 110
    label_x = (canvas_w - text_w) // 2
    # Левая линия
    draw.line([(label_x - gap - line_len, y), (label_x - gap, y)], fill=color, width=2)
    # Правая линия
    draw.line([(label_x + text_w + gap, y), (label_x + text_w + gap + line_len, y)], fill=color, width=2)
    # Текст
    cur_x = label_x
    for i, ch in enumerate(label):
        draw.text((cur_x, y - 13), ch, font=font, fill=color)
        bbox = font.getbbox(ch)
        cur_x += (bbox[2] - bbox[0]) + (3 if i < len(label) - 1 else 0)


def _draw_pill(draw, *, center_x: int, y: int, icon: str, text: str, fill: tuple[int, int, int], text_color=TEXT_WHITE) -> None:
    """Рисует pill-бейдж с фоном acc цвета."""
    font_pill = _load_font(22, weight="Bold")
    icon_font = _load_font(24, weight="Bold")
    icon_str = (icon or "").strip()
    icon_w, icon_h = (0, 0)
    if icon_str:
        icon_w, _ = _measure_text(icon_font, icon_str)
    text_w, text_h = _measure_text(font_pill, text, letter_spacing=2)
    h_total = max(icon_h, text_h)
    pad_x = 26
    inner_gap = 12 if icon_str else 0
    pill_w = pad_x * 2 + icon_w + inner_gap + text_w
    pill_h = 56
    left = center_x - pill_w // 2
    right = left + pill_w
    top = y
    bot = y + pill_h
    radius = pill_h // 2
    # Полупрозрачный фон (имитируем смешением с CARD_BG)
    bg = tuple(
        int(0.18 * fill[i] + 0.82 * CARD_BG[i])
        for i in range(3)
    )
    draw.rounded_rectangle([left, top, right, bot], radius=radius, fill=bg, outline=fill, width=2)
    # Иконка
    cur_x = left + pad_x
    if icon_str:
        draw.text((cur_x, top + (pill_h - icon_h) // 2 - 4), icon_str, font=icon_font, fill=fill)
        cur_x += icon_w + inner_gap
    # Текст
    text_y = top + (pill_h - text_h) // 2 - 4
    for i, ch in enumerate(text):
        draw.text((cur_x, text_y), ch, font=font_pill, fill=text_color)
        bbox = font_pill.getbbox(ch)
        cur_x += (bbox[2] - bbox[0]) + (2 if i < len(text) - 1 else 0)


def _try_open_logo(company_id: str | None):
    if not company_id:
        return None
    try:
        from PIL import Image
    except ImportError:
        return None
    path = _logos_dir() / f"{company_id}.png"
    if not path.is_file():
        return None
    try:
        return Image.open(path).convert("RGBA")
    except Exception as exc:
        logger.warning("logo open failed for %s: %s", company_id, exc)
        return None


def render_info_card(meta: CardMeta) -> bytes:
    """Рендерит карточку 1024x1024, возвращает PNG bytes."""
    try:
        from PIL import Image, ImageDraw
    except ImportError as exc:
        raise RuntimeError(f"Pillow not available: {exc}")

    img = Image.new("RGB", (CANVAS, CANVAS), BG_OUTER)
    draw = ImageDraw.Draw(img)

    # Скруглённая карточка
    card_margin = 48
    card_radius = 36
    draw.rounded_rectangle(
        [card_margin, card_margin, CANVAS - card_margin, CANVAS - card_margin],
        radius=card_radius,
        fill=CARD_BG,
        outline=CARD_BORDER,
        width=2,
    )

    accent = ACCENT_COLORS.get(meta.accent, ACCENT_COLORS["neutral"])

    # === Верхний блок: логотип + название ===
    cur_y = 100
    logo = _try_open_logo(meta.company_id)
    if logo is not None:
        # Resize до 200px по высоте, центрируем
        target_h = 200
        ratio = target_h / logo.height
        target_w = int(logo.width * ratio)
        logo_resized = logo.resize((target_w, target_h), Image.LANCZOS)
        paste_x = (CANVAS - target_w) // 2
        img.paste(logo_resized, (paste_x, cur_y), logo_resized)
        cur_y += target_h + 24
    else:
        # Без логотипа — оставим место поменьше
        cur_y = 150

    # Название (капс, белый, ExtraBold, letterspaced)
    name_font = _load_font(40, weight="ExtraBold")
    name_w, name_h = _measure_text(name_font, meta.company_label, letter_spacing=6)
    _text_with_spacing(
        draw,
        (0, cur_y),
        meta.company_label.upper(),
        name_font,
        TEXT_WHITE,
        letter_spacing=6,
        center_x=CANVAS // 2,
    )
    cur_y += name_h + 40

    # === Разделитель с категорией ===
    _draw_divider_with_label(draw, y=cur_y + 10, label=meta.category_label.upper(), color=accent, canvas_w=CANVAS)
    cur_y += 50

    # === Главная цифра (огромная, акцент-цвет) ===
    main_font = _load_font(120, weight="ExtraBold")
    main_w, main_h = _measure_text(main_font, meta.main_value)
    _text_with_spacing(
        draw,
        (0, cur_y),
        meta.main_value,
        main_font,
        accent,
        letter_spacing=0,
        center_x=CANVAS // 2,
    )
    cur_y += main_h + 50

    # Тонкая линия-разделитель
    draw.line([(CANVAS // 2 - 200, cur_y), (CANVAS // 2 + 200, cur_y)], fill=DIVIDER, width=2)
    cur_y += 40

    # === Подблок: иконка + sub_label + sub_value + sub_caption ===
    sub_label_font = _load_font(20, weight="Bold")
    sub_value_font = _load_font(34, weight="ExtraBold")
    sub_caption_font = _load_font(18, weight="Bold")

    sub_label_w, sub_label_h = _measure_text(sub_label_font, meta.sub_label.upper(), letter_spacing=3)
    _text_with_spacing(
        draw,
        (0, cur_y),
        meta.sub_label.upper(),
        sub_label_font,
        TEXT_MUTED,
        letter_spacing=3,
        center_x=CANVAS // 2,
    )
    cur_y += sub_label_h + 14

    if meta.sub_value:
        sub_value_w, sub_value_h = _measure_text(sub_value_font, meta.sub_value)
        _text_with_spacing(
            draw,
            (0, cur_y),
            meta.sub_value,
            sub_value_font,
            TEXT_WHITE,
            letter_spacing=0,
            center_x=CANVAS // 2,
        )
        cur_y += sub_value_h + 8

    if meta.sub_caption:
        sc_w, sc_h = _measure_text(sub_caption_font, meta.sub_caption)
        _text_with_spacing(
            draw,
            (0, cur_y),
            meta.sub_caption,
            sub_caption_font,
            TEXT_MUTED,
            letter_spacing=0,
            center_x=CANVAS // 2,
        )
        cur_y += sc_h + 20

    # === Pill ===
    if meta.pill_text:
        # позиционируем pill ~80px над watermark
        pill_y = CANVAS - 200
        _draw_pill(draw, center_x=CANVAS // 2, y=pill_y, icon=meta.pill_icon, text=meta.pill_text, fill=accent)

    # === Watermark «automy ai» внизу ===
    wm_font = _load_font(18, weight="Bold")
    wm_text = "automy ai"
    wm_w, wm_h = _measure_text(wm_font, wm_text, letter_spacing=4)
    _text_with_spacing(
        draw,
        (0, CANVAS - 100),
        wm_text,
        wm_font,
        TEXT_WATERMARK,
        letter_spacing=4,
        center_x=CANVAS // 2,
    )

    out = BytesIO()
    img.save(out, format="PNG", optimize=True)
    return out.getvalue()
