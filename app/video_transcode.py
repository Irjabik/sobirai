from __future__ import annotations

import json
import logging
import subprocess
import time
from pathlib import Path

from .ffmpeg_runtime import (
    ffmpeg_available,
    ffprobe_available,
    get_ffmpeg,
    get_ffprobe,
)

logger = logging.getLogger(__name__)

MAX_VIDEO_BITRATE_KBPS = 2500
TARGET_AUDIO_BITRATE = "128k"
AUDIO_BITRATE_KBPS = 128
TRANSCODE_TIMEOUT_SECONDS = 300
DEFAULT_MAX_INPUT_MB = 300

# Ниже этого битрейта картинка превращается в кашу — такое видео лучше
# не жать вовсе и отдать пост текстом. 48 МБ / 400 kbps ≈ 16 минут:
# всё, что длиннее, физически не влезает в лимит Bot API с приличным качеством.
MIN_ACCEPTABLE_VIDEO_KBPS = 400

# Потолок Bot API на отправку файла ботом — 50 МБ. Берём с запасом на
# служебные поля multipart, иначе Telegram отвечает Request Entity Too Large.
TELEGRAM_UPLOAD_LIMIT_MB = 48

# Гарантированно совместимый с Telegram streamable mp4: H264 main 720p+AAC+faststart.
VIDEO_FILTER = (
    "scale='min(1280,iw)':'min(720,ih)':force_original_aspect_ratio=decrease,"
    "scale=trunc(iw/2)*2:trunc(ih/2)*2"
)



def _target_video_kbps(input_path: Path, size_mb: float) -> int | None:
    """Битрейт, при котором результат влезет в лимит Bot API.

    None — уложиться с приемлемым качеством нельзя (слишком длинное видео),
    транскодировать бессмысленно.
    """
    probed = _probe_via_ffprobe(input_path) or _probe_via_imageio(input_path)
    duration = probed[0] if probed else None
    if not duration or duration <= 0:
        return None
    # Запас под контейнер и служебные поля multipart.
    budget_mb = TELEGRAM_UPLOAD_LIMIT_MB - 3
    total_kbps = int(budget_mb * 1024 * 8 / duration)
    video_kbps = total_kbps - AUDIO_BITRATE_KBPS
    if video_kbps < MIN_ACCEPTABLE_VIDEO_KBPS:
        return None
    source_kbps = int(size_mb * 1024 * 8 / duration)
    # Никогда не поднимаем битрейт выше исходного: раньше константа 2500k
    # раздувала 83 МБ до 235 МБ.
    return max(1, min(video_kbps, MAX_VIDEO_BITRATE_KBPS, int(source_kbps * 0.95)))


def transcoded_video_path(original_path: str | Path) -> Path:
    src = Path(original_path)
    return src.with_name(f"{src.stem}_tg.mp4")


def transcode_video_for_telegram(
    input_path: Path,
    output_path: Path,
    *,
    max_input_size_mb: int = DEFAULT_MAX_INPUT_MB,
) -> bool:
    if not ffmpeg_available():
        logger.warning("ffmpeg not available, skipping video transcode path=%s", input_path)
        return False
    input_path = Path(input_path)
    output_path = Path(output_path)
    if not input_path.is_file():
        return False
    size_mb = input_path.stat().st_size / (1024 * 1024)
    if max_input_size_mb > 0 and size_mb > max_input_size_mb:
        logger.warning(
            "Skip video transcode: %.1f MB > %s MB cap path=%s",
            size_mb,
            max_input_size_mb,
            input_path,
        )
        return False
    video_kbps = _target_video_kbps(input_path, size_mb)
    if video_kbps is None:
        logger.warning(
            "Skip video transcode: под лимит %s МБ не уложиться с приемлемым"
            " качеством (%.1f МБ) path=%s",
            TELEGRAM_UPLOAD_LIMIT_MB, size_mb, input_path,
        )
        return False
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(
        "Video transcode start: source=%s size=%.1f MB target=%skbps",
        input_path.name, size_mb, video_kbps,
    )
    started_at = time.monotonic()
    bitrate = f"{video_kbps}k"
    # nice + один поток: на сервере рядом живут боты и сайты Automy (2 ядра).
    cmd = [
        "nice", "-n", "15",
        get_ffmpeg(), "-y", "-hide_banner", "-loglevel", "error",
        "-threads", "1",
        "-i", str(input_path),
        "-vf", VIDEO_FILTER,
        "-c:v", "libx264", "-profile:v", "main", "-level", "4.0",
        "-preset", "veryfast", "-pix_fmt", "yuv420p",
        "-b:v", bitrate, "-maxrate", bitrate, "-bufsize", "4M",
        "-c:a", "aac", "-b:a", TARGET_AUDIO_BITRATE, "-ac", "2",
        "-movflags", "+faststart",
        str(output_path),
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            timeout=TRANSCODE_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired:
        logger.warning("Video transcode timed out path=%s", input_path)
        try:
            output_path.unlink(missing_ok=True)
        except OSError:
            pass
        return False
    except OSError as exc:
        logger.warning("Video transcode OSError: %s path=%s", exc, input_path)
        return False
    if result.returncode != 0 or not output_path.exists() or output_path.stat().st_size == 0:
        err_tail = result.stderr.decode("utf-8", errors="replace")[-500:]
        logger.warning("Video transcode failed rc=%s err=%s", result.returncode, err_tail)
        try:
            output_path.unlink(missing_ok=True)
        except OSError:
            pass
        return False
    elapsed = time.monotonic() - started_at
    out_mb = output_path.stat().st_size / (1024 * 1024)
    # Короткий лёгкий клип перекодирование только раздувало (2.1 МБ -> 11.5 МБ).
    # Если исходник и так влезает в лимит и он меньше — отдаём исходник.
    if out_mb >= size_mb and size_mb <= TELEGRAM_UPLOAD_LIMIT_MB:
        logger.info(
            "Video transcode discarded: source=%s %.1f MB -> %.1f MB, оставляю исходник",
            input_path.name, size_mb, out_mb,
        )
        try:
            output_path.unlink(missing_ok=True)
        except OSError:
            pass
        return False
    logger.info(
        "Video transcode complete: source=%s took %.1fs output_size=%.1f MB",
        input_path.name, elapsed, out_mb,
    )
    return True


def _probe_via_ffprobe(path: Path) -> tuple[int | None, int | None, int | None] | None:
    if not ffprobe_available():
        return None
    cmd = [
        get_ffprobe(), "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height,duration",
        "-of", "json",
        str(path),
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=30, check=False)
        if r.returncode != 0:
            return None
        data = json.loads(r.stdout.decode("utf-8"))
    except (subprocess.TimeoutExpired, OSError, json.JSONDecodeError):
        return None
    streams = data.get("streams") or []
    if not streams:
        return None
    s = streams[0]
    try:
        w = int(s["width"]) if s.get("width") is not None else None
        h = int(s["height"]) if s.get("height") is not None else None
    except (TypeError, ValueError):
        w = h = None
    try:
        d = int(float(s["duration"])) if s.get("duration") is not None else None
    except (TypeError, ValueError):
        d = None
    return (d, w, h)


def _probe_via_imageio(path: Path) -> tuple[int | None, int | None, int | None] | None:
    """Fallback на imageio.v3.immeta когда ffprobe недоступен."""
    try:
        from imageio.v3 import immeta  # type: ignore
        meta = immeta(str(path))
    except Exception as exc:
        logger.debug("imageio immeta failed for %s: %s", path, exc)
        return None
    size = meta.get("size") or meta.get("source_size")
    w = h = None
    if isinstance(size, (list, tuple)) and len(size) >= 2:
        try:
            w = int(size[0])
            h = int(size[1])
        except (TypeError, ValueError):
            w = h = None
    duration = meta.get("duration")
    try:
        d = int(float(duration)) if duration is not None else None
    except (TypeError, ValueError):
        d = None
    if w is None and h is None and d is None:
        return None
    return (d, w, h)


def probe_video_dims(path: Path) -> tuple[int | None, int | None, int | None] | None:
    """Возвращает (duration_sec, width, height) для перекодированного файла или None."""
    result = _probe_via_ffprobe(path)
    if result is not None:
        return result
    return _probe_via_imageio(path)
