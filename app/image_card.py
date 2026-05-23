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
# Светлый дизайн как в исходном рефе SpaceX/OpenAI/Anthropic
BG_OUTER = (245, 245, 247)     # #f5f5f7 — общий светлый фон
CARD_BG = (255, 255, 255)      # #ffffff — белая карточка
CARD_BORDER = (225, 225, 230)  # тонкая серая обводка
TEXT_DARK = (24, 24, 27)       # основной тёмный текст
TEXT_MUTED = (110, 113, 122)   # подписи / категории
DIVIDER = (220, 222, 228)      # тонкие разделители

# Палитра акцентов под светлый фон (более насыщенные чем для тёмного).
ACCENT_COLORS: dict[str, tuple[int, int, int]] = {
    "red":     (220, 38, 38),    # #dc2626 — иски, утечки, инциденты
    "orange":  (234, 88, 12),    # #ea580c — увольнения, регуляция
    "green":   (5, 150, 105),    # #059669 — сделки, инвестиции
    "blue":    (37, 99, 235),    # #2563eb — релизы, API
    "purple":  (124, 58, 237),   # #7c3aed — mega-релизы, research
    "cyan":    (8, 145, 178),    # #0891b2 — robotics, hardware
    "yellow":  (202, 138, 4),    # #ca8a04 — предупреждения
    "neutral": (71, 85, 105),    # #475569 — нейтральные
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

    Auto-shrink: если текст не помещается в максимальную ширину pill (700px),
    уменьшаем шрифт с 26pt до 16pt пока не влезет. Если даже на 16pt не влез —
    обрезаем с многоточием.
    """
    MAX_PILL_W = 760
    PAD_X = 44
    PILL_H = 72
    LETTER_SPACING = 3
    sizes = [26, 24, 22, 20, 18, 16]
    chosen_size = sizes[-1]
    chosen_text = text
    text_w = 0
    text_h = 0
    for size in sizes:
        font_try = _load_font(size, weight="ExtraBold")
        tw, th = _measure_text(font_try, text, letter_spacing=LETTER_SPACING)
        if tw + PAD_X * 2 <= MAX_PILL_W:
            chosen_size = size
            text_w, text_h = tw, th
            break
    else:
        # Все размеры не влезли — обрезаем
        font_min = _load_font(sizes[-1], weight="ExtraBold")
        truncated = text
        while truncated:
            tw, th = _measure_text(font_min, truncated + "…", letter_spacing=LETTER_SPACING)
            if tw + PAD_X * 2 <= MAX_PILL_W:
                chosen_text = truncated + "…"
                text_w, text_h = tw, th
                break
            truncated = truncated[:-1]
        else:
            chosen_text = "…"

    font_pill = _load_font(chosen_size, weight="ExtraBold")
    pill_w = min(MAX_PILL_W, PAD_X * 2 + text_w)
    left = center_x - pill_w // 2
    right = left + pill_w
    top = y
    bot = y + PILL_H
    radius = PILL_H // 2
    draw.rounded_rectangle([left, top, right, bot], radius=radius, fill=fill)
    _text_with_spacing(
        draw,
        (0, top + (PILL_H - text_h) // 2 - 6),
        chosen_text,
        font_pill,
        (255, 255, 255),
        letter_spacing=LETTER_SPACING,
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
    """Главная цифра огромным шрифтом — accent-цветом на белом фоне (без плашки).

    На светлом фоне accent сам по себе яркий и читается, плашка с обводкой
    избыточна. Auto-shrink: если значение длинное (длиннее 9 символов),
    уменьшаем шрифт.
    """
    text_len = len(value)
    if text_len <= 5:
        font_size = 160
    elif text_len <= 8:
        font_size = 130
    elif text_len <= 12:
        font_size = 100
    else:
        font_size = 80
    main_font = _load_font(font_size, weight="ExtraBold")
    _, text_h = _measure_text(main_font, value)
    _text_with_spacing(
        draw,
        (0, center_y),
        value,
        main_font,
        accent,
        letter_spacing=0,
        center_x=CANVAS // 2,
    )
    return center_y + text_h + 50


def _draw_logo_circle(img, *, center: tuple[int, int], radius: int) -> None:
    """Рисует круглую подложку под логотипом — светло-серая на белой карточке."""
    try:
        from PIL import ImageDraw as _ID
    except ImportError:
        return
    d = _ID.Draw(img)
    cx, cy = center
    d.ellipse(
        [cx - radius, cy - radius, cx + radius, cy + radius],
        fill=(248, 248, 250),
        outline=(228, 230, 235),
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

    # На светлом фоне тёмные логотипы (OpenAI, Apple, Anthropic чёрные) выглядят
    # как родные — auto-invert НЕ нужен. Светлые/белые логотипы наоборот станут
    # невидимыми, нужно их затемнить.
    try:
        r, g, b, a = logo.split()
    except ValueError:
        return logo
    pixels = list(zip(r.getdata(), g.getdata(), b.getdata(), a.getdata()))
    visible = [(R + G + B) / 3 for R, G, B, A in pixels if A > 50]
    if not visible:
        return logo
    avg = sum(visible) / len(visible)
    # Если средняя яркость > 200 (логотип почти белый/очень светлый) — инвертируем
    # в тёмный для светлого фона карточки. RGB only, alpha сохраняем.
    if avg > 200:
        from PIL import ImageOps
        rgb = Image.merge("RGB", (r, g, b))
        inverted = ImageOps.invert(rgb)
        inv_r, inv_g, inv_b = inverted.split()
        logo = Image.merge("RGBA", (inv_r, inv_g, inv_b, a))
        logger.debug("logo %s darkened (avg=%.1f)", company_id, avg)
    return logo


def render_info_card(meta: CardMeta) -> bytes:
    """Рендерит карточку 1024x1024, возвращает PNG bytes."""
    try:
        from PIL import Image, ImageDraw
    except ImportError as exc:
        raise RuntimeError(f"Pillow not available: {exc}")

    accent = ACCENT_COLORS.get(meta.accent, ACCENT_COLORS["neutral"])

    # === Базовый слой: белый фон + карточка ===
    img = Image.new("RGB", (CANVAS, CANVAS), BG_OUTER)
    draw = ImageDraw.Draw(img)

    # Карточка с тонкой обводкой
    card_margin = 48
    card_radius = 40
    draw.rounded_rectangle(
        [card_margin, card_margin, CANVAS - card_margin, CANVAS - card_margin],
        radius=card_radius,
        fill=CARD_BG,
        outline=CARD_BORDER,
        width=2,
    )

    # Тонкая accent-цветная полоска у верхней кромки карточки — даёт характер
    draw.rectangle(
        [card_margin, card_margin, CANVAS - card_margin, card_margin + 6],
        fill=accent,
    )

    # === Верхний блок: логотип + название ===
    cur_y = 110
    logo = _try_open_logo(meta.company_id)
    if logo is not None:
        logo_size = 200
        ratio = logo_size / max(logo.width, logo.height)
        new_w = int(logo.width * ratio)
        new_h = int(logo.height * ratio)
        logo_resized = logo.resize((new_w, new_h), Image.LANCZOS)
        # Конвертируем в RGBA для paste с alpha-маской
        if logo_resized.mode != "RGBA":
            logo_resized = logo_resized.convert("RGBA")
        # Paste нужен RGBA-base — конвертируем img временно
        img_rgba = img.convert("RGBA")
        img_rgba.paste(logo_resized, (CANVAS // 2 - new_w // 2, cur_y), logo_resized)
        img = img_rgba.convert("RGB")
        draw = ImageDraw.Draw(img)
        cur_y += new_h + 30
    else:
        cur_y = 160

    # Название компании / темы
    name_font = _load_font(46, weight="ExtraBold")
    _, name_h = _measure_text(name_font, meta.company_label, letter_spacing=8)
    _text_with_spacing(
        draw,
        (0, cur_y),
        meta.company_label.upper(),
        name_font,
        TEXT_DARK,
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
            TEXT_DARK,
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
