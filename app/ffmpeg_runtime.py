"""Locates ffmpeg/ffprobe binaries — system or bundled via imageio-ffmpeg.

На Bothost (и других PaaS без ffmpeg в базовом Docker-образе) системный ffmpeg недоступен.
Решение: pip-пакет imageio-ffmpeg качает статический ffmpeg-бинарник в свой кэш и отдаёт путь.

Если imageio-ffmpeg не установлен или не смог скачать — отдаём имя 'ffmpeg' как fallback,
который сработает только при наличии системного ffmpeg.
"""
from __future__ import annotations

import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


def _find_bundled_ffmpeg() -> str | None:
    """Возвращает путь к ffmpeg, скачанному пакетом imageio-ffmpeg (если установлен)."""
    try:
        import imageio_ffmpeg  # type: ignore
        path = imageio_ffmpeg.get_ffmpeg_exe()
        if path and Path(path).exists():
            return path
    except Exception as exc:
        logger.debug("imageio-ffmpeg not available: %s", exc)
    return None


def _resolve_ffmpeg() -> str | None:
    sys_path = shutil.which("ffmpeg")
    if sys_path:
        return sys_path
    return _find_bundled_ffmpeg()


def _resolve_ffprobe(ffmpeg_path: str | None) -> str | None:
    sys_path = shutil.which("ffprobe")
    if sys_path:
        return sys_path
    # imageio-ffmpeg не везёт ffprobe, но иногда статические бандлы кладут его рядом с ffmpeg.
    if ffmpeg_path:
        parent = Path(ffmpeg_path).parent
        for name in ("ffprobe", "ffprobe.exe"):
            candidate = parent / name
            if candidate.exists():
                return str(candidate)
    return None


# Резолвим один раз при импорте — кешируется на время жизни процесса.
FFMPEG_PATH: str | None = _resolve_ffmpeg()
FFPROBE_PATH: str | None = _resolve_ffprobe(FFMPEG_PATH)

if FFMPEG_PATH:
    logger.info("ffmpeg resolved: %s (system=%s)", FFMPEG_PATH, shutil.which("ffmpeg") is not None)
else:
    logger.warning(
        "ffmpeg NOT resolved. Видео не будут транскодироваться (Telegram покажет как documents)."
        " Установи imageio-ffmpeg в requirements.txt или системный ffmpeg в Docker."
    )

if FFPROBE_PATH:
    logger.debug("ffprobe resolved: %s", FFPROBE_PATH)
else:
    logger.info("ffprobe не найден — fallback на imageio.v3.immeta для метаданных видео.")


def ffmpeg_available() -> bool:
    return FFMPEG_PATH is not None


def ffprobe_available() -> bool:
    return FFPROBE_PATH is not None


def get_ffmpeg() -> str:
    """Возвращает путь к ffmpeg или строку 'ffmpeg' если binary не найден (вызов упадёт)."""
    return FFMPEG_PATH or "ffmpeg"


def get_ffprobe() -> str:
    return FFPROBE_PATH or "ffprobe"
