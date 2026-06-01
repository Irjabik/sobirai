from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

WATERMARK_OPACITY = 0.65
WATERMARK_PHOTO_SCALE = 0.13
WATERMARK_MIN_WIDTH = 80
WATERMARK_MAX_WIDTH = 280
# Если средняя яркость зоны watermark ниже порога — берём светлый лого, иначе тёмный.
# 0..255, 110 = чуть темнее середины (под тёмное фото уже нужен белый знак).
WATERMARK_DARK_BG_THRESHOLD = 110

ASSETS_DIR = Path(__file__).resolve().parent / "assets"
LOGO_DARK_PATH = ASSETS_DIR / "automy_watermark.png"           # чёрный, для светлых фото
LOGO_LIGHT_PATH = ASSETS_DIR / "automy_watermark_light.png"    # белый, для тёмных фото
DEFAULT_LOGO_PATH = LOGO_DARK_PATH  # обратная совместимость с существующими вызовами


def _pillow_available() -> bool:
    try:
        from PIL import Image  # noqa: F401
        return True
    except ImportError:
        return False


def _normalize_logo(logo, target_rgb: tuple[int, int, int], alpha_boost: float = 1.8):
    """Унифицирует цвет логотипа и поднимает alpha, чтобы все буквы были одинаково плотными.

    Исходный лого может иметь «ai» в более светлом RGB и/или с пониженной alpha — после этого
    обе части (automy + ai) будут одного цвета и одинаковой видимости.
    """
    from PIL import Image
    logo = logo.convert("RGBA")
    r_band, g_band, b_band, a_band = logo.split()
    # alpha: умножаем на boost, клампим в 0..255
    a_boosted = a_band.point(lambda a: min(255, int(a * alpha_boost)))
    # RGB: заливаем сплошным целевым цветом
    solid_r = Image.new("L", logo.size, target_rgb[0])
    solid_g = Image.new("L", logo.size, target_rgb[1])
    solid_b = Image.new("L", logo.size, target_rgb[2])
    return Image.merge("RGBA", (solid_r, solid_g, solid_b, a_boosted))


def _avg_luminance(image, box: tuple[int, int, int, int]) -> float:
    """Средняя яркость прямоугольной области (0..255). 0=чёрная, 255=белая."""
    crop = image.crop(box).convert("L")
    pixels = crop.getdata()
    n = len(pixels)
    if n == 0:
        return 128.0
    return sum(pixels) / n


def _pick_logo_for_background(base_image, target_w: int, target_h: int, padding: int) -> Path:
    """Выбирает чёрный или белый лого по яркости угла под watermark."""
    x1 = max(0, base_image.width - target_w - padding * 2)
    y1 = max(0, base_image.height - target_h - padding * 2)
    box = (x1, y1, base_image.width, base_image.height)
    avg = _avg_luminance(base_image, box)
    if avg < WATERMARK_DARK_BG_THRESHOLD and LOGO_LIGHT_PATH.is_file():
        return LOGO_LIGHT_PATH
    return LOGO_DARK_PATH


def add_watermark_photo(
    input_path: Path,
    output_path: Path,
    *,
    logo_path: Path | None = None,
    opacity: float = WATERMARK_OPACITY,
    scale: float = WATERMARK_PHOTO_SCALE,
) -> bool:
    """Накладывает логотип в правый нижний угол фото.

    По умолчанию авто-выбор: чёрный лого на светлых фото, белый на тёмных.
    Если передан явный `logo_path` — используется он, без анализа яркости.
    """
    if not _pillow_available():
        logger.warning("Pillow not installed, skipping photo watermark")
        return False
    try:
        input_path = Path(input_path)
        output_path = Path(output_path)
    except TypeError:
        return False
    if not input_path.is_file():
        logger.warning("Watermark input missing: %s", input_path)
        return False
    try:
        from PIL import Image
        with Image.open(input_path) as raw_base:
            raw_base.load()
            base = raw_base.convert("RGBA") if raw_base.mode != "RGBA" else raw_base.copy()

        target_w = int(base.width * scale)
        target_w = max(WATERMARK_MIN_WIDTH, min(WATERMARK_MAX_WIDTH, target_w))

        # Чтобы знать целевую высоту для замера яркости, нужен размер логотипа.
        # Берём дефолтный (DARK) — высота обоих лого практически одинаковая.
        with Image.open(LOGO_DARK_PATH) as probe_logo:
            ratio = target_w / max(1, probe_logo.width)
            target_h = max(20, int(probe_logo.height * ratio))

        padding = max(10, int(base.width * 0.02))

        # Авто-выбор лого по яркости зоны watermark
        chosen_logo_path = Path(logo_path) if logo_path else _pick_logo_for_background(
            base, target_w, target_h, padding
        )
        if not chosen_logo_path.is_file():
            logger.warning("Chosen logo missing: %s", chosen_logo_path)
            return False

        with Image.open(chosen_logo_path) as raw_logo:
            raw_logo.load()
            logo_raw = raw_logo.convert("RGBA")
        # Унифицируем цвет: для тёмного логотипа — чисто чёрный, для светлого — кремово-белый.
        if chosen_logo_path == LOGO_LIGHT_PATH:
            logo_raw = _normalize_logo(logo_raw, target_rgb=(248, 244, 238), alpha_boost=2.4)
        else:
            logo_raw = _normalize_logo(logo_raw, target_rgb=(0, 0, 0), alpha_boost=1.8)
        ratio = target_w / max(1, logo_raw.width)
        target_h = max(20, int(logo_raw.height * ratio))
        logo = logo_raw.resize((target_w, target_h), Image.LANCZOS)

        alpha = logo.split()[3]
        alpha = alpha.point(lambda p: int(p * opacity))
        logo.putalpha(alpha)

        pos = (base.width - target_w - padding, base.height - target_h - padding)
        overlay = Image.new("RGBA", base.size, (0, 0, 0, 0))
        overlay.paste(logo, pos, logo)
        result = Image.alpha_composite(base, overlay).convert("RGB")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        result.save(output_path, "JPEG", quality=88, optimize=True)
        logger.debug(
            "Watermarked %s -> %s logo=%s",
            input_path.name,
            output_path.name,
            chosen_logo_path.name,
        )
        return True
    except Exception:
        logger.exception("Photo watermark failed input=%s", input_path)
        return False


def watermarked_photo_path(original_path: str | Path) -> Path:
    """Кладём watermarked рядом с оригиналом — тогда cleanup в service.py сметает обоих."""
    src = Path(original_path)
    return src.with_name(f"{src.stem}_wm.jpg")
