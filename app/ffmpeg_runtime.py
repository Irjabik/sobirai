"""Locates ffmpeg/ffprobe binaries — system, static-ffmpeg, or imageio-ffmpeg.

На Bothost и подобных PaaS системного ffmpeg нет. Порядок поиска:
1) системный (shutil.which) — самый дешёвый, если есть в образе;
2) static-ffmpeg — pip-пакет, бандлит бинарник ПРЯМО в .whl, не требует сети
   на старте процесса. Подходит для PaaS, блокирующих github releases;
3) imageio-ffmpeg — pip-пакет, качает бинарник с github на первый запрос.
   Работает только если у процесса есть сеть к github releases.

static-ffmpeg несёт и ffmpeg, и ffprobe — для нас идеально.
"""
from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


def _find_in_data_dir() -> tuple[str | None, str | None]:
    """Ищет ffmpeg и ffprobe в DATA_DIR (persistent volume Bothost: /app/data).

    Залить туда бинарники можно через /installffmpeg <url> — они переживут все
    деплои/перезапуски. Это самый надёжный способ на хостингах, где собственный
    Dockerfile недоступен и pip игнорирует requirements.txt.
    """
    data_dir = os.getenv("DATA_DIR", "/app/data")
    ffmpeg = Path(data_dir) / "ffmpeg"
    ffprobe = Path(data_dir) / "ffprobe"
    out: list[str | None] = []
    for p in (ffmpeg, ffprobe):
        if p.is_file() and p.stat().st_size > 0:
            try:
                # Гарантируем executable. Не страшно если уже стоит.
                p.chmod(0o755)
            except OSError:
                pass
            out.append(str(p))
        else:
            out.append(None)
    return out[0], out[1]


def _find_static_ffmpeg() -> tuple[str | None, str | None]:
    """Возвращает (ffmpeg, ffprobe) от пакета static-ffmpeg, или (None, None)."""
    try:
        from static_ffmpeg import add_paths  # type: ignore
        add_paths()  # добавляет директории с ffmpeg/ffprobe в PATH процесса
    except Exception as exc:
        logger.debug("static-ffmpeg not available or add_paths failed: %s", exc)
        return None, None
    # После add_paths shutil.which должен найти оба
    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")
    return ffmpeg, ffprobe


def _find_imageio_ffmpeg() -> str | None:
    """Возвращает путь к ffmpeg, скачанному пакетом imageio-ffmpeg (если установлен)."""
    try:
        import imageio_ffmpeg  # type: ignore
        path = imageio_ffmpeg.get_ffmpeg_exe()
        if path and Path(path).exists():
            return path
    except Exception as exc:
        logger.debug("imageio-ffmpeg not available: %s", exc)
    return None


def _resolve_ffmpeg_and_ffprobe() -> tuple[str | None, str | None]:
    # 0) Бинарники, залитые в persistent /app/data/ через /installffmpeg.
    #    Самый надёжный путь — переживает деплои даже когда хост игнорирует
    #    requirements.txt.
    data_ffmpeg, data_ffprobe = _find_in_data_dir()
    if data_ffmpeg and data_ffprobe:
        return data_ffmpeg, data_ffprobe

    # 1) Системный
    sys_ffmpeg = shutil.which("ffmpeg")
    sys_ffprobe = shutil.which("ffprobe")
    ffmpeg = data_ffmpeg or sys_ffmpeg
    ffprobe = data_ffprobe or sys_ffprobe
    if ffmpeg and ffprobe:
        return ffmpeg, ffprobe

    # 2) static-ffmpeg — несёт оба
    static_ffmpeg, static_ffprobe = _find_static_ffmpeg()
    ffmpeg = ffmpeg or static_ffmpeg
    ffprobe = ffprobe or static_ffprobe
    if ffmpeg and ffprobe:
        return ffmpeg, ffprobe

    # 3) imageio-ffmpeg (только ffmpeg)
    if not ffmpeg:
        ffmpeg = _find_imageio_ffmpeg()
        # Иногда ffprobe лежит рядом со скачанным ffmpeg
        if ffmpeg and not ffprobe:
            parent = Path(ffmpeg).parent
            for name in ("ffprobe", "ffprobe.exe"):
                candidate = parent / name
                if candidate.exists():
                    ffprobe = str(candidate)
                    break
    return ffmpeg, ffprobe


# Резолвим один раз при импорте — кешируется на время жизни процесса.
FFMPEG_PATH, FFPROBE_PATH = _resolve_ffmpeg_and_ffprobe()

if FFMPEG_PATH:
    logger.info(
        "ffmpeg resolved: %s (system=%s)",
        FFMPEG_PATH,
        bool(shutil.which("ffmpeg")) and FFMPEG_PATH == shutil.which("ffmpeg"),
    )
else:
    logger.warning(
        "ffmpeg NOT resolved. Видео не будут транскодироваться (Telegram покажет как documents)."
        " Установи static-ffmpeg или системный ffmpeg в Docker."
    )

if FFPROBE_PATH:
    logger.info("ffprobe resolved: %s", FFPROBE_PATH)
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
