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
    """Загружает Bold/ExtraBold TTF.

    Порядок поиска:
    1. /app/data/fonts/Inter-<weight>.ttf
    2. /app/data/fonts/Roboto-<weight or Black>.ttf
    3. /app/data/fonts/NotoSans-<weight>.ttf
    4. Системный шрифт (Pillow ищет в /usr/share/fonts/): DejaVu, Liberation
    5. PIL default (last resort, без кириллицы)
    """
    try:
        from PIL import ImageFont
    except ImportError:
        return None
    fonts_dir = _fonts_dir()
    if weight.lower() == "extrabold":
        local_names = ["Inter-ExtraBold.ttf", "Roboto-Black.ttf", "NotoSans-Black.ttf", "Inter-Bold.ttf", "Roboto-Bold.ttf", "NotoSans-Bold.ttf"]
        system_names = ["DejaVuSans-Bold.ttf", "LiberationSans-Bold.ttf", "FreeSansBold.ttf", "Arial-Bold.ttf"]
    else:
        local_names = ["Inter-Bold.ttf", "Roboto-Bold.ttf", "NotoSans-Bold.ttf", "Inter-ExtraBold.ttf", "Roboto-Black.ttf"]
        system_names = ["DejaVuSans-Bold.ttf", "LiberationSans-Bold.ttf", "FreeSansBold.ttf", "Arial-Bold.ttf"]

    for name in local_names:
        path = fonts_dir / name
        if path.is_file():
            try:
                return ImageFont.truetype(str(path), size)
            except OSError:
                continue

    # Системные шрифты — Pillow найдёт их сам через /usr/share/fonts/
    for name in system_names:
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue

    try:
        return ImageFont.load_default(size=size)
    except (TypeError, AttributeError):
        return ImageFont.load_default()


def _visual_bbox(font, text: str, letter_spacing: int = 0) -> tuple[int, int, int, int]:
    """Возвращает (min_x, min_y, max_x, max_y) реального видимого glyph-bbox.

    Использует font.getmask() который возвращает только пиксели «чернил»,
    в отличие от getbbox() который учитывает advance width (включает невидимые
    отступы). Это даёт точное визуальное центрирование без типографических
    смещений (например доллар $ имеет advance с padding слева).
    """
    if not text:
        return (0, 0, 0, 0)
    # Рисуем целиком на временной маске, измеряем
    from PIL import Image as _Img, ImageDraw as _ID
    # bbox оценка от advance width — для размера временного канваса
    advance_w = 0
    for i, ch in enumerate(text):
        advance_w += font.getbbox(ch)[2] - font.getbbox(ch)[0]
        if i < len(text) - 1:
            advance_w += letter_spacing
    advance_h = font.getbbox("Ag")[3] - font.getbbox("Ag")[1]
    pad = 20
    tmp = _Img.new("L", (advance_w + pad * 2, advance_h + pad * 2), 0)
    td = _ID.Draw(tmp)
    cur_x = pad
    for ch in text:
        td.text((cur_x, pad), ch, font=font, fill=255)
        bbox = font.getbbox(ch)
        cur_x += (bbox[2] - bbox[0]) + letter_spacing
    real = tmp.getbbox()
    if real is None:
        return (0, 0, advance_w, advance_h)
    # Возвращаем относительно "точки рисования" (0,0)
    return (real[0] - pad, real[1] - pad, real[2] - pad, real[3] - pad)


def _measure_text(font, text: str, letter_spacing: int = 0) -> tuple[int, int]:
    """Размер видимого glyph-bbox (без advance-padding)."""
    bbox = _visual_bbox(font, text, letter_spacing)
    return (bbox[2] - bbox[0], bbox[3] - bbox[1])


def _text_with_spacing(draw, xy, text, font, fill, *, letter_spacing: int = 0, center_x: int | None = None) -> tuple[int, int]:
    """Рисует текст с межбуквенным интервалом. center_x — точный визуальный центр.

    Использует mask-bbox чтобы центрировать видимые glyph'ы, а не их advance-width.
    """
    if not text:
        return (0, 0)
    x, y = xy
    if center_x is not None:
        vis_bbox = _visual_bbox(font, text, letter_spacing)
        vis_w = vis_bbox[2] - vis_bbox[0]
        # Корректируем x так, чтобы визуальный центр совпал с center_x.
        # Первая буква рисуется на cur_x, её видимый glyph начинается на cur_x + vis_bbox[0].
        x = center_x - vis_w // 2 - vis_bbox[0]
    bbox_h = font.getbbox("Ag")[3] - font.getbbox("Ag")[1]
    cur_x = x
    for ch in text:
        draw.text((cur_x, y), ch, font=font, fill=fill)
        bbox = font.getbbox(ch)
        cur_x += (bbox[2] - bbox[0]) + letter_spacing
    return (cur_x - x, bbox_h)


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


def _draw_pill(draw, *, center_x: int, y: int, icon: str, text: str, fill: tuple[int, int, int]) -> None:
    """Pill-бейдж с яркой solid заливкой acc-цветом и текстом по центру.

    Параметр icon оставлен для обратной совместимости, но игнорируется —
    pill теперь содержит только текст для чистого визуального центрирования.
    """
    font_pill = _load_font(26, weight="ExtraBold")
    text_w, text_h = _measure_text(font_pill, text, letter_spacing=3)
    pad_x = 44
    pill_w = pad_x * 2 + text_w
    pill_h = 72
    left = center_x - pill_w // 2
    right = left + pill_w
    top = y
    bot = y + pill_h
    radius = pill_h // 2
    draw.rounded_rectangle([left, top, right, bot], radius=radius, fill=fill)
    # Текст по точному визуальному центру pill
    _text_with_spacing(
        draw,
        (0, top + (pill_h - text_h) // 2 - 6),
        text,
        font_pill,
        TEXT_WHITE,
        letter_spacing=3,
        center_x=center_x,
    )


def _draw_radial_glow(img, *, center: tuple[int, int], radius: int, color: tuple[int, int, int], max_alpha: int = 80) -> None:
    """Рисует мягкое свечение accent-цветом (radial fade)."""
    from PIL import Image, ImageDraw as _ID
    glow = Image.new("RGBA", img.size, (0, 0, 0, 0))
    gd = _ID.Draw(glow)
    cx, cy = center
    # Несколько концентрических кругов с убывающей alpha
    steps = 24
    for i in range(steps, 0, -1):
        r = int(radius * i / steps)
        alpha = int(max_alpha * (1 - (i / steps) ** 0.7))
        gd.ellipse([cx - r, cy - r, cx + r, cy + r], fill=(color[0], color[1], color[2], alpha))
    img.alpha_composite(glow)


def _draw_main_value_block(
    img, draw, *, center_y: int, value: str, accent: tuple[int, int, int]
) -> int:
    """Рисует главную цифру в цветной плашке с обводкой и подсветкой.

    Возвращает новый cur_y (низ блока + отступ).
    """
    main_font = _load_font(120, weight="ExtraBold")
    text_w, text_h = _measure_text(main_font, value)
    # Плашка вокруг
    pad_x = 50
    pad_y = 28
    block_w = text_w + pad_x * 2
    block_h = text_h + pad_y * 2
    block_left = (CANVAS - block_w) // 2
    block_right = block_left + block_w
    block_top = center_y
    block_bot = block_top + block_h
    radius = 28
    # Полупрозрачный фон (смешиваем с CARD_BG)
    bg = tuple(int(0.10 * accent[i] + 0.90 * CARD_BG[i]) for i in range(3))
    draw.rounded_rectangle(
        [block_left, block_top, block_right, block_bot],
        radius=radius,
        fill=bg,
        outline=accent,
        width=3,
    )
    # Текст
    _text_with_spacing(
        draw,
        (0, block_top + pad_y - 6),
        value,
        main_font,
        accent,
        letter_spacing=0,
        center_x=CANVAS // 2,
    )
    return block_bot + 40


def _draw_logo_circle(img, *, center: tuple[int, int], radius: int) -> None:
    """Рисует круглую подложку под логотипом — чуть светлее карточки."""
    try:
        from PIL import ImageDraw as _ID
    except ImportError:
        return
    layer = img
    d = _ID.Draw(layer)
    cx, cy = center
    d.ellipse(
        [cx - radius, cy - radius, cx + radius, cy + radius],
        fill=(38, 38, 38),
        outline=(60, 60, 60),
        width=2,
    )


def _try_open_logo(company_id: str | None):
    """Открывает PNG-логотип. Если он преимущественно тёмный — инвертирует
    в светлый, чтобы был виден на тёмной карточке (#1f1f1f)."""
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
        logo = Image.open(path).convert("RGBA")
    except Exception as exc:
        logger.warning("logo open failed for %s: %s", company_id, exc)
        return None

    # Auto-crop по непрозрачным краям — у многих PNG есть padding с одной стороны,
    # из-за чего после paste(center) визуальный центр glyph'а смещён.
    try:
        bbox = logo.split()[-1].getbbox()
        if bbox and (bbox != (0, 0, logo.width, logo.height)):
            logo = logo.crop(bbox)
    except Exception:
        pass

    # Считаем среднюю яркость только видимых (alpha > 50) пикселей
    try:
        r, g, b, a = logo.split()
    except ValueError:
        return logo
    pixels = list(zip(r.getdata(), g.getdata(), b.getdata(), a.getdata()))
    visible = [(R + G + B) / 3 for R, G, B, A in pixels if A > 50]
    if not visible:
        return logo
    avg = sum(visible) / len(visible)
    # Если средняя яркость видимой части < 110, считаем логотип «тёмным» и инвертируем
    # RGB до светлой версии. Alpha не трогаем, чтобы сохранить прозрачность.
    if avg < 110:
        from PIL import ImageOps
        rgb = Image.merge("RGB", (r, g, b))
        inverted = ImageOps.invert(rgb)
        # Чуть осветляем до белого если уже близко к белому
        inv_r, inv_g, inv_b = inverted.split()
        logo = Image.merge("RGBA", (inv_r, inv_g, inv_b, a))
        logger.debug("logo %s auto-inverted (avg=%.1f)", company_id, avg)
    return logo


def render_info_card(meta: CardMeta) -> bytes:
    """Рендерит карточку 1024x1024, возвращает PNG bytes."""
    try:
        from PIL import Image, ImageDraw
    except ImportError as exc:
        raise RuntimeError(f"Pillow not available: {exc}")

    accent = ACCENT_COLORS.get(meta.accent, ACCENT_COLORS["neutral"])

    # === Базовый слой ===
    img = Image.new("RGBA", (CANVAS, CANVAS), (*BG_OUTER, 255))

    # Glow по диагонали — два мягких пятна accent-цветом
    _draw_radial_glow(img, center=(180, 200), radius=450, color=accent, max_alpha=70)
    _draw_radial_glow(img, center=(CANVAS - 150, CANVAS - 220), radius=420, color=accent, max_alpha=50)

    draw = ImageDraw.Draw(img)

    # Скруглённая карточка
    card_margin = 48
    card_radius = 40
    draw.rounded_rectangle(
        [card_margin, card_margin, CANVAS - card_margin, CANVAS - card_margin],
        radius=card_radius,
        fill=(*CARD_BG, 235),
        outline=CARD_BORDER,
        width=2,
    )

    # === Верхний блок: круглая подложка + логотип + название ===
    cur_y = 90
    logo = _try_open_logo(meta.company_id)
    if logo is not None:
        logo_size = 180
        circle_r = 130
        circle_cx = CANVAS // 2
        circle_cy = cur_y + circle_r
        _draw_logo_circle(img, center=(circle_cx, circle_cy), radius=circle_r)
        # Resize logo
        ratio = logo_size / max(logo.width, logo.height)
        new_w = int(logo.width * ratio)
        new_h = int(logo.height * ratio)
        logo_resized = logo.resize((new_w, new_h), Image.LANCZOS)
        img.paste(logo_resized, (circle_cx - new_w // 2, circle_cy - new_h // 2), logo_resized)
        cur_y = circle_cy + circle_r + 20
    else:
        cur_y = 130

    # Название компании / темы
    name_font = _load_font(46, weight="ExtraBold")
    _, name_h = _measure_text(name_font, meta.company_label, letter_spacing=8)
    _text_with_spacing(
        draw,
        (0, cur_y),
        meta.company_label.upper(),
        name_font,
        TEXT_WHITE,
        letter_spacing=8,
        center_x=CANVAS // 2,
    )
    cur_y += name_h + 36

    # === Разделитель с категорией ===
    _draw_divider_with_label(draw, y=cur_y + 10, label=meta.category_label.upper(), color=accent, canvas_w=CANVAS)
    cur_y += 56

    # === Главная цифра в плашке с обводкой ===
    cur_y = _draw_main_value_block(img, draw, center_y=cur_y, value=meta.main_value, accent=accent)

    # === Подблок ===
    sub_label_font = _load_font(22, weight="Bold")
    sub_value_font = _load_font(40, weight="ExtraBold")
    sub_caption_font = _load_font(20, weight="Bold")

    _, sub_label_h = _measure_text(sub_label_font, meta.sub_label.upper(), letter_spacing=4)
    _text_with_spacing(
        draw,
        (0, cur_y),
        meta.sub_label.upper(),
        sub_label_font,
        TEXT_MUTED,
        letter_spacing=4,
        center_x=CANVAS // 2,
    )
    cur_y += sub_label_h + 16

    if meta.sub_value:
        _, sv_h = _measure_text(sub_value_font, meta.sub_value)
        _text_with_spacing(
            draw,
            (0, cur_y),
            meta.sub_value,
            sub_value_font,
            TEXT_WHITE,
            letter_spacing=0,
            center_x=CANVAS // 2,
        )
        cur_y += sv_h + 10

    if meta.sub_caption:
        _, sc_h = _measure_text(sub_caption_font, meta.sub_caption)
        _text_with_spacing(
            draw,
            (0, cur_y),
            meta.sub_caption,
            sub_caption_font,
            TEXT_MUTED,
            letter_spacing=0,
            center_x=CANVAS // 2,
        )
        cur_y += sc_h + 24

    # === Solid pill снизу (яркий) ===
    if meta.pill_text:
        pill_y = CANVAS - 190
        _draw_pill(draw, center_x=CANVAS // 2, y=pill_y, icon=meta.pill_icon, text=meta.pill_text, fill=accent)

    # NB: текстовый watermark «automy ai» НЕ рисуем здесь — фирменный
    # PNG-watermark уже накладывается через _apply_photo_watermark в
    # channel_autopublish при публикации.

    out = BytesIO()
    img.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()
