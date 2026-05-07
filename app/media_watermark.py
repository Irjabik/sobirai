from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

WATERMARK_OPACITY = 0.65
WATERMARK_PHOTO_SCALE = 0.18
WATERMARK_MIN_WIDTH = 60
WATERMARK_MAX_WIDTH = 320

# Корень репозитория -> app/assets/sobirai_watermark.jpg
DEFAULT_LOGO_PATH = Path(__file__).resolve().parent / "assets" / "sobirai_watermark.jpg"


def _pillow_available() -> bool:
    try:
        from PIL import Image  # noqa: F401
        return True
    except ImportError:
        return False


def add_watermark_photo(
    input_path: Path,
    output_path: Path,
    *,
    logo_path: Path = DEFAULT_LOGO_PATH,
    opacity: float = WATERMARK_OPACITY,
    scale: float = WATERMARK_PHOTO_SCALE,
) -> bool:
    """Накладывает логотип Sobirai в правый нижний угол фото. Возвращает True при успехе."""
    if not _pillow_available():
        logger.warning("Pillow not installed, skipping photo watermark")
        return False
    try:
        input_path = Path(input_path)
        output_path = Path(output_path)
        logo_path = Path(logo_path)
    except TypeError:
        return False
    if not input_path.is_file() or not logo_path.is_file():
        logger.warning(
            "Watermark inputs missing: input_exists=%s logo_exists=%s",
            input_path.is_file(),
            logo_path.is_file(),
        )
        return False
    try:
        from PIL import Image
        base = Image.open(input_path)
        if base.mode != "RGBA":
            base = base.convert("RGBA")
        logo = Image.open(logo_path).convert("RGBA")

        target_w = int(base.width * scale)
        target_w = max(WATERMARK_MIN_WIDTH, min(WATERMARK_MAX_WIDTH, target_w))
        ratio = target_w / max(1, logo.width)
        target_h = max(20, int(logo.height * ratio))
        logo = logo.resize((target_w, target_h), Image.LANCZOS)

        alpha = logo.split()[3]
        alpha = alpha.point(lambda p: int(p * opacity))
        logo.putalpha(alpha)

        padding = max(10, int(base.width * 0.02))
        pos = (base.width - target_w - padding, base.height - target_h - padding)
        overlay = Image.new("RGBA", base.size, (0, 0, 0, 0))
        overlay.paste(logo, pos, logo)
        result = Image.alpha_composite(base, overlay).convert("RGB")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        result.save(output_path, "JPEG", quality=88, optimize=True)
        return True
    except Exception:
        logger.exception("Photo watermark failed input=%s", input_path)
        return False


def watermarked_photo_path(original_path: str | Path) -> Path:
    """Кладём watermarked рядом с оригиналом — тогда cleanup в service.py сметает обоих."""
    src = Path(original_path)
    return src.with_name(f"{src.stem}_wm.jpg")
