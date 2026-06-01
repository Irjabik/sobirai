"""Эвристики качества медиа.

Используется в пайплайне публикации, чтобы не выдавать в канал «пустые» картинки —
белые/однотонные обложки, шаблонные плейсхолдеры от каналов-источников и т. п.
В таких случаях лучше опубликовать пост как text-only, чем приклеивать к нему
бесполезную картинку с watermark в углу.
"""
from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Порог стандартного отклонения яркости/цвета в каналах RGB. Меньше = более «пустая»
# картинка. 10 — эмпирический порог: пустая обложка с мелкой подписью в углу даёт
# std≈4-5 после downsample; нормальный логотип компании на фоне даёт std≈30+;
# реальные фото — std существенно выше. Картинка из скриншота с белым фоном и
# серым «automy ai» в углу (watermark) попадает строго ниже порога.
LOW_INFO_STD_THRESHOLD = 10.0
# Размер уменьшенной копии для оценки. 32x32 = 1024 пикселя — быстро и достаточно.
PROBE_SIZE = 32


def _pillow_available() -> bool:
    try:
        from PIL import Image  # noqa: F401
        return True
    except ImportError:
        return False


def is_low_info_photo(path: str | Path) -> bool:
    """Возвращает True, если картинка похожа на однотонную «обложку»-плейсхолдер.

    Эвристика: уменьшаем фото до 32x32, считаем std по каналам RGB. Если максимум
    std среди каналов меньше порога — картинка слишком однообразная для публикации.

    При любой ошибке (нет Pillow, файл повреждён) возвращает False — лучше опубликовать
    как было, чем зарезать настоящую картинку из-за бага детектора.
    """
    if not _pillow_available():
        return False
    try:
        from PIL import Image
    except ImportError:
        return False
    try:
        p = Path(path)
        if not p.is_file() or p.stat().st_size == 0:
            return False
        with Image.open(p) as raw:
            raw.load()
            img = raw.convert("RGB")
        img.thumbnail((PROBE_SIZE, PROBE_SIZE))
        pixels = list(img.getdata())
        if not pixels:
            return False

        n = len(pixels)
        # std по каналам — простая выборочная дисперсия.
        sum_r = sum_g = sum_b = 0
        for r, g, b in pixels:
            sum_r += r
            sum_g += g
            sum_b += b
        mean_r = sum_r / n
        mean_g = sum_g / n
        mean_b = sum_b / n
        var_r = sum((r - mean_r) ** 2 for r, _, _ in pixels) / n
        var_g = sum((g - mean_g) ** 2 for _, g, _ in pixels) / n
        var_b = sum((b - mean_b) ** 2 for _, _, b in pixels) / n
        std_max = max(var_r, var_g, var_b) ** 0.5

        if std_max < LOW_INFO_STD_THRESHOLD:
            logger.info("Detected low-info photo (std_max=%.2f) path=%s", std_max, p.name)
            return True
        return False
    except Exception:
        logger.exception("is_low_info_photo failed path=%s", path)
        return False
