"""Pillow-рендерер карточки в стиле Automy AI / Instagram-карусели.

Дизайн-система из 01_Работа/02_Automy/Инста/Посты/ДИЗАЙН_КАРУСЕЛИ.md:
- Палитра: только ink (#0d0d0d), white (#ffffff), оранжевый (#F67F2F)
- Шрифт: Inter Bold + ExtraBold (+ Black как fallback ExtraBold)
- Структура content-slide:
  • верх 1080×760 — editorial-фото (генерится AI)
  • brand-stamp top-left: волна + «automy ai»
  • низ 1080×590 — белый блок: eyebrow + h1 (с pill) + body + footnote
- Pill: оранжевая заливка под ключевым словом, белый текст
- Без эмодзи, иконок, других цветов
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# === Размеры по дизайн-системе Automy AI ===
CARD_W = 1080
CARD_H = 1350
PHOTO_H = 760
TEXT_H = CARD_H - PHOTO_H  # 590
TEXT_PAD_TOP = 50
TEXT_PAD_SIDES = 64
TEXT_PAD_BOTTOM = 56
TEXT_GAP = 18  # между блоками текста
BRAND_STAMP_TOP = 44
BRAND_STAMP_LEFT = 56
BRAND_STAMP_WAVE_W = 96
BRAND_STAMP_WAVE_H = 63
BRAND_STAMP_GAP = 12
BRAND_STAMP_WM_SIZE = 40

# === Палитра — строго по дизайн-системе ===
INK = (13, 13, 13)             # #0d0d0d основной чёрный
INK_SOFT = (26, 26, 26)        # #1a1a1a
MUTED = (74, 74, 74)            # #4a4a4a — footnote, body на CTA
MUTED_2 = (107, 107, 107)      # #6b6b6b
LINE = (229, 229, 226)         # #e5e5e2
GRAY_BG = (246, 246, 244)      # #f6f6f4
PAPER = (244, 241, 234)        # #f4f1ea — placeholder если фото не загрузилось
ORANGE = (246, 127, 47)        # #F67F2F — единственный акцент
ORANGE_DEEP = (200, 95, 26)    # #C85F1A — eyebrow
WHITE = (255, 255, 255)

# === Типографика ===
TITLE_SIZE = 84
TITLE_LETTER_SPACING = -3       # -0.035em ≈ -3px на 84pt
TITLE_LINE_HEIGHT = 1.18
BODY_SIZE = 40
FOOTNOTE_SIZE = 32
EYEBROW_SIZE = 24
EYEBROW_LETTER_SPACING = 5      # 0.22em ≈ 5px на 24pt
WM_SIZE = 40                    # wordmark в brand stamp


@dataclass(frozen=True)
class AutomyCardMeta:
    """Слоты для рендера карточки в стиле Automy AI.

    Все тексты на русском (кроме имён брендов и моделей).
    Никакой латиницы кроме брендов, букву «ё» не используем.
    """
    eyebrow: str                  # категория: "РЕЛИЗ", "СДЕЛКА", "УТЕЧКА"
    headline: str                 # h1, 2-3 строки. Содержит pill_word.
    pill_word: str                # ключевое слово в headline, обернётся в оранжевый pill
    body: str = ""                # 1-2 предложения. Если пусто — не рисуем.
    footnote: str = ""            # мелкий серый внизу. Цифра/нюанс.
    photo_path: str | Path | None = None  # путь к editorial-фото (от AI или из источника)
    photo_is_dark: bool = True    # если True — brand-stamp white, иначе чёрный


def _fonts_dir() -> Path:
    return Path(os.getenv("DATA_DIR", "/app/data")) / "fonts"


def _assets_dir() -> Path:
    """Папка для бренд-ассетов в persistent volume."""
    return Path(os.getenv("DATA_DIR", "/app/data")) / "assets"


def _load_font(size: int, *, weight: str = "Bold"):
    """Inter Bold/ExtraBold/Black из /app/data/fonts с system fallback."""
    try:
        from PIL import ImageFont
    except ImportError:
        return None
    fonts_dir = _fonts_dir()
    if weight.lower() == "extrabold":
        local = ["Inter-ExtraBold.ttf", "Inter-Black.ttf", "Roboto-Black.ttf", "NotoSans-Black.ttf"]
        system = ["DejaVuSans-Bold.ttf", "LiberationSans-Bold.ttf"]
    elif weight.lower() == "black":
        local = ["Inter-Black.ttf", "Inter-ExtraBold.ttf", "Roboto-Black.ttf", "NotoSans-Black.ttf"]
        system = ["DejaVuSans-Bold.ttf", "LiberationSans-Bold.ttf"]
    else:  # Bold
        local = ["Inter-Bold.ttf", "Roboto-Bold.ttf", "NotoSans-Bold.ttf"]
        system = ["DejaVuSans-Bold.ttf", "LiberationSans-Bold.ttf"]
    for name in local:
        path = fonts_dir / name
        if path.is_file():
            try:
                return ImageFont.truetype(str(path), size)
            except OSError:
                continue
    for name in system:
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue
    try:
        return ImageFont.load_default(size=size)
    except (TypeError, AttributeError):
        return ImageFont.load_default()


def _visual_bbox(font, text: str, letter_spacing: int = 0) -> tuple[int, int, int, int]:
    """Реальный bbox видимых glyph'ов через временную маску."""
    if not text:
        return (0, 0, 0, 0)
    from PIL import Image as _Img, ImageDraw as _ID
    advance_w = 0
    for i, ch in enumerate(text):
        advance_w += font.getbbox(ch)[2] - font.getbbox(ch)[0]
        if i < len(text) - 1:
            advance_w += letter_spacing
    advance_h = font.getbbox("Ag")[3] - font.getbbox("Ag")[1]
    pad = 20
    tmp = _Img.new("L", (max(1, advance_w) + pad * 2, advance_h + pad * 2), 0)
    td = _ID.Draw(tmp)
    cur_x = pad
    for ch in text:
        td.text((cur_x, pad), ch, font=font, fill=255)
        bbox = font.getbbox(ch)
        cur_x += (bbox[2] - bbox[0]) + letter_spacing
    real = tmp.getbbox()
    if real is None:
        return (0, 0, advance_w, advance_h)
    return (real[0] - pad, real[1] - pad, real[2] - pad, real[3] - pad)


def _measure_text(font, text: str, letter_spacing: int = 0) -> tuple[int, int]:
    bbox = _visual_bbox(font, text, letter_spacing)
    return (bbox[2] - bbox[0], bbox[3] - bbox[1])


def _draw_text_line(draw, x: int, y: int, text: str, font, fill, *, letter_spacing: int = 0) -> int:
    """Рисует одну строку с letter-spacing. Возвращает advance-ширину (для wrap)."""
    cur_x = x
    for ch in text:
        draw.text((cur_x, y), ch, font=font, fill=fill)
        bbox = font.getbbox(ch)
        cur_x += (bbox[2] - bbox[0]) + letter_spacing
    return cur_x - x


def _advance_width(font, text: str, letter_spacing: int = 0) -> int:
    cur = 0
    for i, ch in enumerate(text):
        cur += font.getbbox(ch)[2] - font.getbbox(ch)[0]
        if i < len(text) - 1:
            cur += letter_spacing
    return cur


def _wrap_words(font, words: list[str], max_w: int, *, letter_spacing: int = 0, space_w: int | None = None) -> list[list[str]]:
    """Разбивает слова на строки чтобы каждая помещалась в max_w."""
    if space_w is None:
        space_w = _advance_width(font, " ", letter_spacing)
    lines: list[list[str]] = []
    cur: list[str] = []
    cur_w = 0
    for w in words:
        ww = _advance_width(font, w, letter_spacing)
        sep = space_w if cur else 0
        if cur_w + sep + ww <= max_w or not cur:
            cur.append(w)
            cur_w += sep + ww
        else:
            lines.append(cur)
            cur = [w]
            cur_w = ww
    if cur:
        lines.append(cur)
    return lines


def _draw_brand_stamp(img, *, dark_photo: bool) -> None:
    """Brand stamp top-left: волна + 'automy ai'. Если фото тёмное — белый текст
    с drop-shadow; если светлое — чёрный без теней."""
    from PIL import Image, ImageDraw, ImageFilter
    draw = ImageDraw.Draw(img)
    wave_path = _assets_dir() / "wave-tight.png"
    cur_x = BRAND_STAMP_LEFT
    cur_y = BRAND_STAMP_TOP
    has_wave = False
    if wave_path.is_file():
        try:
            with Image.open(wave_path) as raw:
                raw.load()
                wave = raw.convert("RGBA")
            ratio = BRAND_STAMP_WAVE_H / wave.height
            new_w = int(wave.width * ratio)
            wave_resized = wave.resize((new_w, BRAND_STAMP_WAVE_H), Image.LANCZOS)
            # На тёмном фото — оставляем оригинальный оранжевый, добавляем drop-shadow
            # На светлом — оригинальный оранжевый и так читается
            img.paste(wave_resized, (cur_x, cur_y), wave_resized)
            cur_x += new_w + BRAND_STAMP_GAP
            has_wave = True
        except Exception as exc:
            logger.warning("brand-stamp wave open failed: %s", exc)
    wm_font = _load_font(WM_SIZE, weight="ExtraBold")
    wm_text = "automy ai"
    wm_w, wm_h = _measure_text(wm_font, wm_text, letter_spacing=-1)
    wm_y = cur_y + (BRAND_STAMP_WAVE_H - wm_h) // 2 - 4 if has_wave else cur_y
    fill = WHITE if dark_photo else INK
    if dark_photo:
        # Имитация text-shadow: рисуем тёмные копии с blur, потом сам текст.
        shadow_layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
        sd = ImageDraw.Draw(shadow_layer)
        _draw_text_line(sd, cur_x, wm_y + 2, wm_text, wm_font, (0, 0, 0, 180), letter_spacing=-1)
        shadow_layer = shadow_layer.filter(ImageFilter.GaussianBlur(radius=4))
        img.alpha_composite(shadow_layer) if img.mode == "RGBA" else img.paste(shadow_layer, (0, 0), shadow_layer)
    _draw_text_line(draw, cur_x, wm_y, wm_text, wm_font, fill, letter_spacing=-1)


def _draw_eyebrow(draw, *, x: int, y: int, text: str, color=ORANGE_DEEP) -> int:
    """Eyebrow: 24pt Bold uppercase, letter-spacing 0.22em ≈ 5px. Возвращает высоту."""
    font = _load_font(EYEBROW_SIZE, weight="Bold")
    text_up = text.upper()
    _, h = _measure_text(font, text_up, letter_spacing=EYEBROW_LETTER_SPACING)
    _draw_text_line(draw, x, y, text_up, font, color, letter_spacing=EYEBROW_LETTER_SPACING)
    return h


def _split_headline_around_pill(headline: str, pill_word: str) -> tuple[str, str, str]:
    """Разбивает headline на (before, pill, after). pill_word ищется case-insensitive."""
    if not pill_word:
        return headline, "", ""
    low_h = headline.lower()
    low_p = pill_word.lower()
    idx = low_h.find(low_p)
    if idx < 0:
        return headline, "", ""
    end = idx + len(pill_word)
    return headline[:idx].rstrip(), headline[idx:end], headline[end:].lstrip()


def _draw_pill_word(img, draw, *, x: int, y: int, word: str, font, letter_spacing: int = -2) -> tuple[int, int]:
    """Рисует слово с оранжевой подложкой (pill). Возвращает (advance_w, height)."""
    text_w, text_h = _measure_text(font, word, letter_spacing=letter_spacing)
    # padding em-style на крупном кегле
    pad_x = int(font.size * 0.26)
    pad_top = int(font.size * 0.02)
    pad_bot = int(font.size * 0.12)
    radius = int(font.size * 0.22)
    # Baseline glyph'а
    vis = _visual_bbox(font, word, letter_spacing)
    rect_left = x + vis[0] - pad_x
    rect_top = y + vis[1] - pad_top
    rect_right = x + vis[2] + pad_x
    rect_bot = y + vis[3] + pad_bot
    draw.rounded_rectangle(
        [rect_left, rect_top, rect_right, rect_bot],
        radius=radius,
        fill=ORANGE,
    )
    _draw_text_line(draw, x, y, word, font, WHITE, letter_spacing=letter_spacing)
    advance = _advance_width(font, word, letter_spacing)
    return advance, text_h


def _draw_headline_with_pill(
    img, draw, *, x: int, y: int, max_w: int,
    headline: str, pill_word: str,
    font, line_height_mult: float,
    letter_spacing: int = -2,
) -> int:
    """Рисует h1 с pill на ключевом слове. Делит фразу на строки чтобы помещалась.

    Возвращает суммарную высоту блока (низ - y).
    """
    before, pill, after = _split_headline_around_pill(headline, pill_word)
    # Собираем все «токены» в порядке: слова before + [PILL] + слова after
    space_w = _advance_width(font, " ", letter_spacing)
    line_h = int(font.size * line_height_mult)

    # Считаем токены
    Token = tuple[str, bool]  # (text, is_pill)
    tokens: list[Token] = []
    for w in before.split() if before else []:
        tokens.append((w, False))
    if pill:
        tokens.append((pill, True))
    for w in after.split() if after else []:
        tokens.append((w, False))

    # Разбиваем на строки
    lines: list[list[Token]] = []
    cur_line: list[Token] = []
    cur_w = 0

    def token_advance(t: Token) -> int:
        w_text = t[0]
        if t[1]:
            # pill включает em-padding
            return _advance_width(font, w_text, letter_spacing) + int(font.size * 0.52)
        return _advance_width(font, w_text, letter_spacing)

    for t in tokens:
        ta = token_advance(t)
        sep = space_w if cur_line else 0
        if cur_line and cur_w + sep + ta > max_w:
            lines.append(cur_line)
            cur_line = [t]
            cur_w = ta
        else:
            cur_line.append(t)
            cur_w += sep + ta
    if cur_line:
        lines.append(cur_line)

    # Рисуем
    cy = y
    for ln in lines:
        cx = x
        first_on_line = True
        for t in ln:
            if not first_on_line:
                cx += space_w
            first_on_line = False
            if t[1]:  # pill
                advance, _ = _draw_pill_word(img, draw, x=cx, y=cy, word=t[0], font=font, letter_spacing=letter_spacing)
                cx += advance + int(font.size * 0.30)
            else:
                _draw_text_line(draw, cx, cy, t[0], font, INK, letter_spacing=letter_spacing)
                cx += _advance_width(font, t[0], letter_spacing)
        cy += line_h
    return cy - y


def _draw_body_paragraph(
    draw, *, x: int, y: int, max_w: int, text: str, font, color, line_height_mult: float, letter_spacing: int = 0,
) -> int:
    """Разбивает body на строки и рисует. Возвращает высоту блока."""
    if not text:
        return 0
    words = text.split()
    lines = _wrap_words(font, words, max_w, letter_spacing=letter_spacing)
    line_h = int(font.size * line_height_mult)
    cy = y
    for ln in lines:
        s = " ".join(ln)
        _draw_text_line(draw, x, cy, s, font, color, letter_spacing=letter_spacing)
        cy += line_h
    return cy - y


def _make_paper_photo() -> "Image.Image":
    """Если фото не загрузилось — рисуем простой off-white placeholder."""
    from PIL import Image
    return Image.new("RGB", (CARD_W, PHOTO_H), PAPER)


def _load_photo(photo_path: str | Path | None) -> "Image.Image":
    """Загружает и подгоняет editorial-фото под 1080×760 c object-fit cover."""
    from PIL import Image
    if photo_path:
        p = Path(photo_path)
        if p.is_file() and p.stat().st_size > 0:
            try:
                with Image.open(p) as raw:
                    raw.load()
                    src = raw.convert("RGB")
            except Exception as exc:
                logger.warning("photo open failed: %s", exc)
                return _make_paper_photo()
            # object-fit cover для 1080×760
            target_ar = CARD_W / PHOTO_H
            src_ar = src.width / src.height
            if src_ar > target_ar:
                # Шире чем нужно — crop по бокам
                new_h = src.height
                new_w = int(src.height * target_ar)
                left = (src.width - new_w) // 2
                src = src.crop((left, 0, left + new_w, new_h))
            else:
                # Уже чем нужно — crop по верху/низу. Сдвигаем чуть выше (правило третей).
                new_w = src.width
                new_h = int(src.width / target_ar)
                top = max(0, (src.height - new_h) // 2)
                src = src.crop((0, top, new_w, top + new_h))
            return src.resize((CARD_W, PHOTO_H), Image.LANCZOS)
    return _make_paper_photo()


def render_automy_card(meta: AutomyCardMeta) -> bytes:
    """Финальная карточка 1080×1350 в стиле Automy AI."""
    try:
        from PIL import Image, ImageDraw
    except ImportError as exc:
        raise RuntimeError(f"Pillow not available: {exc}")

    img = Image.new("RGB", (CARD_W, CARD_H), WHITE)

    # === Photo zone 1080×760 ===
    photo = _load_photo(meta.photo_path)
    img.paste(photo, (0, 0))

    # NB: brand-stamp top-left УБРАН — фирменный watermark уже накладывается
    # через _apply_photo_watermark (channel_autopublish) внизу справа.
    # Дублировать в углу не нужно.
    draw = ImageDraw.Draw(img)

    # === Text zone 1080×590 (белый блок снизу) ===
    text_x = TEXT_PAD_SIDES
    max_text_w = CARD_W - 2 * TEXT_PAD_SIDES
    text_y = PHOTO_H + TEXT_PAD_TOP

    # Eyebrow
    if meta.eyebrow:
        _draw_eyebrow(draw, x=text_x, y=text_y, text=meta.eyebrow)
        text_y += EYEBROW_SIZE + TEXT_GAP + 6

    # H1 с pill
    title_font = _load_font(TITLE_SIZE, weight="Black")
    h_used = _draw_headline_with_pill(
        img, draw,
        x=text_x, y=text_y, max_w=max_text_w,
        headline=meta.headline,
        pill_word=meta.pill_word,
        font=title_font,
        line_height_mult=TITLE_LINE_HEIGHT,
        letter_spacing=TITLE_LETTER_SPACING,
    )
    text_y += h_used + TEXT_GAP + 4

    # Сначала рассчитаем место под footnote (он позиционируется снизу),
    # чтобы body не наезжал на него.
    footnote_block_h = 0
    footnote_lines: list[list[str]] = []
    footnote_y = CARD_H - TEXT_PAD_BOTTOM
    if meta.footnote:
        footnote_font = _load_font(FOOTNOTE_SIZE, weight="Bold")
        footnote_lines = _wrap_words(footnote_font, meta.footnote.split(), max_text_w)
        # Ограничиваем footnote до 2 строк
        footnote_lines = footnote_lines[:2]
        footnote_block_h = int(FOOTNOTE_SIZE * 1.30) * len(footnote_lines)
        footnote_y = CARD_H - TEXT_PAD_BOTTOM - footnote_block_h + int(FOOTNOTE_SIZE * 0.1)

    # Body — обрезаем по числу строк чтобы влез между h1 и footnote
    if meta.body:
        body_font = _load_font(BODY_SIZE, weight="Bold")
        body_line_h = int(BODY_SIZE * 1.28)
        # Зазор минимум 20px между body и footnote
        gap_to_footnote = 24
        available_h = footnote_y - text_y - gap_to_footnote
        max_body_lines = max(1, available_h // body_line_h)
        words = meta.body.split()
        body_lines = _wrap_words(body_font, words, max_text_w)
        # Обрезка с многоточием если не влезает
        truncated = False
        if len(body_lines) > max_body_lines:
            body_lines = body_lines[:max_body_lines]
            truncated = True
        # Добавляем многоточие к последней строке если обрезали
        cy = text_y
        for i, ln in enumerate(body_lines):
            s = " ".join(ln)
            if truncated and i == len(body_lines) - 1:
                # Подгоняем чтобы вместе с многоточием помещалось
                while s and _advance_width(body_font, s + "…") > max_text_w:
                    s = s.rsplit(" ", 1)[0] if " " in s else s[:-1]
                s = s.rstrip(",.;:") + "…"
            _draw_text_line(draw, text_x, cy, s, body_font, INK)
            cy += body_line_h

    # Footnote — внизу text-зоны
    if footnote_lines:
        footnote_font = _load_font(FOOTNOTE_SIZE, weight="Bold")
        cy = footnote_y
        for ln in footnote_lines:
            _draw_text_line(draw, text_x, cy, " ".join(ln), footnote_font, MUTED)
            cy += int(FOOTNOTE_SIZE * 1.30)

    out = BytesIO()
    img.save(out, format="PNG", optimize=True)
    return out.getvalue()


# === Backward-compat: старая CardMeta + render_info_card до 8096ee1 ===
# Старые callers по-прежнему импортируют их, конвертируем в AutomyCardMeta.
ACCENT_COLORS: dict[str, tuple[int, int, int]] = {
    "red": ORANGE, "orange": ORANGE, "green": ORANGE, "blue": ORANGE,
    "purple": ORANGE, "cyan": ORANGE, "yellow": ORANGE, "neutral": ORANGE,
}


@dataclass(frozen=True)
class CardMeta:
    company_label: str
    company_id: str | None
    category_label: str
    main_value: str
    sub_label: str
    sub_value: str
    sub_caption: str = ""
    pill_icon: str = ""
    pill_text: str = ""
    accent: str = "orange"


def render_info_card(meta: CardMeta) -> bytes:
    """Адаптер для старого API: маппим CardMeta → AutomyCardMeta."""
    # Headline = «<company_label> <main_value>», pill_word = main_value
    headline = f"{meta.company_label}: {meta.main_value}" if meta.company_label and meta.main_value else (meta.company_label or meta.main_value or "AI NEWS")
    automy = AutomyCardMeta(
        eyebrow=meta.category_label or "AI NEWS",
        headline=headline,
        pill_word=meta.main_value or meta.company_label,
        body=f"{meta.sub_label}: {meta.sub_value}" if meta.sub_label and meta.sub_value else (meta.sub_value or ""),
        footnote=meta.sub_caption or meta.pill_text or "",
        photo_path=None,
        photo_is_dark=False,
    )
    return render_automy_card(automy)
