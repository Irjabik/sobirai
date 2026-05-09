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

TARGET_VIDEO_BITRATE = "2500k"
TARGET_AUDIO_BITRATE = "128k"
TRANSCODE_TIMEOUT_SECONDS = 300
DEFAULT_MAX_INPUT_MB = 50

# Гарантированно совместимый с Telegram streamable mp4: H264 main 720p+AAC+faststart.
VIDEO_FILTER = (
    "scale='min(1280,iw)':'min(720,ih)':force_original_aspect_ratio=decrease,"
    "scale=trunc(iw/2)*2:trunc(ih/2)*2"
)


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
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Video transcode start: source=%s size=%.1f MB", input_path.name, size_mb)
    started_at = time.monotonic()
    cmd = [
        get_ffmpeg(), "-y", "-hide_banner", "-loglevel", "error",
        "-i", str(input_path),
        "-vf", VIDEO_FILTER,
        "-c:v", "libx264", "-profile:v", "main", "-level", "4.0",
        "-preset", "veryfast", "-pix_fmt", "yuv420p",
        "-b:v", TARGET_VIDEO_BITRATE, "-maxrate", TARGET_VIDEO_BITRATE, "-bufsize", "4M",
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
