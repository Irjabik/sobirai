"""HTML+CSS → headless Chrome/wkhtmltoimage → PNG.

Это «правильный» способ генерации брендовых карточек по сравнению с
прямым рисованием Pillow:

- Идеальная типографика (CSS управляет letter-spacing, line-height, kerning)
- Любые Google Fonts через @import — без возни с TTF в /app/data
- Pixel-perfect рендеринг точно как в браузере
- Дизайнер правит CSS вместо чтения Python

Бэкенды (по приоритету):
1. wkhtmltoimage — статический бинарник, заливается через /installwkhtml
   в /app/data/bin/wkhtmltoimage (как ffmpeg)
2. playwright / chromium — если вдруг установлены
3. None — генератор делает fallback на Pillow (image_card.render_automy_card)
"""
from __future__ import annotations

import html as _html
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from string import Template

logger = logging.getLogger(__name__)


def _wkhtml_paths() -> tuple[Path, Path]:
    """Возвращает пути к wkhtmltoimage и wkhtmltopdf в /app/data/bin/."""
    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    bin_dir = data_dir / "bin"
    return bin_dir / "wkhtmltoimage", bin_dir / "wkhtmltopdf"


def _find_wkhtmltoimage() -> str | None:
    """1) Системный, 2) Скачанный через /installwkhtml в /app/data/bin/."""
    sys_path = shutil.which("wkhtmltoimage")
    if sys_path:
        return sys_path
    bin_path, _ = _wkhtml_paths()
    if bin_path.is_file():
        try:
            bin_path.chmod(0o755)
        except OSError:
            pass
        return str(bin_path)
    return None


def html_renderer_available() -> bool:
    return _find_wkhtmltoimage() is not None


# === HTML+CSS шаблон карточки 1080×1350 в стиле Automy AI ===
# Точная вёрстка по дизайн-системе из 01_Работа/02_Automy/Инста/Посты.
CARD_HTML_TEMPLATE = Template(r"""<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

:root {
  --ink:        #0d0d0d;
  --muted:      #4a4a4a;
  --orange:     #F67F2F;
  --orange-deep:#C85F1A;
  --paper:      #f4f1ea;
  --white:      #ffffff;
}

* { box-sizing: border-box; margin: 0; padding: 0; }

html, body {
  width: 1080px; height: 1350px;
  overflow: hidden;
  background: var(--white);
  font-family: 'Inter', system-ui, sans-serif;
  font-feature-settings: 'cv02','cv03','cv04','cv11','ss01';
  -webkit-font-smoothing: antialiased;
  text-rendering: optimizeLegibility;
}

.card {
  position: relative;
  width: 1080px; height: 1350px;
  background: var(--white);
  display: flex; flex-direction: column;
  overflow: hidden;
}

.photo {
  width: 1080px; height: 760px;
  position: relative; overflow: hidden;
  background: var(--paper);
  background-image: $photo_bg_css;
  background-size: cover;
  background-position: center;
}

.body {
  flex: 1;
  padding: 50px 64px 56px;
  background: var(--white);
  display: flex; flex-direction: column;
  gap: 18px;
}

.eyebrow {
  font-size: 24px; font-weight: 700;
  letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--orange-deep);
}

h1.title {
  font-size: 84px; font-weight: 900;
  line-height: 1.18; letter-spacing: -0.035em;
  color: var(--ink);
}

h1.title .accent {
  background: var(--orange); color: var(--white);
  padding: 0.02em 0.26em 0.12em;
  border-radius: 0.22em;
  -webkit-box-decoration-break: clone;
  box-decoration-break: clone;
  line-height: inherit;
}

p.body-text {
  font-size: 40px; font-weight: 500;
  line-height: 1.28; color: var(--ink);
  letter-spacing: -0.005em;
  max-width: 950px;
  /* Обрезаем body до 3 строк с многоточием */
  display: -webkit-box;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

p.footnote {
  font-size: 32px; font-weight: 600;
  line-height: 1.30; color: var(--muted);
  letter-spacing: -0.005em;
  max-width: 950px;
  margin-top: auto;
  /* Обрезаем footnote до 2 строк */
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}
</style>
</head>
<body>
<div class="card">
  <div class="photo"></div>
  <div class="body">
    <div class="eyebrow">$eyebrow</div>
    <h1 class="title">$headline_html</h1>
    $body_html
    $footnote_html
  </div>
</div>
</body>
</html>
""")


def _escape(text: str) -> str:
    return _html.escape((text or "").strip())


def _build_headline_html(headline: str, pill_word: str) -> str:
    """Заворачивает pill_word в <span class="accent">. Поиск case-insensitive,
    но сохраняет оригинальный регистр из headline."""
    headline = (headline or "").strip()
    if not pill_word or not headline:
        return _escape(headline)
    low_h = headline.lower()
    low_p = pill_word.lower().strip()
    idx = low_h.find(low_p)
    if idx < 0:
        return _escape(headline)
    end = idx + len(low_p)
    before = headline[:idx]
    pill = headline[idx:end]
    after = headline[end:]
    return f'{_escape(before)}<span class="accent">{_escape(pill)}</span>{_escape(after)}'


def _photo_bg_css(photo_path: str | os.PathLike | None) -> str:
    """Превращает локальный путь к фото в `url('file:///...')` для CSS."""
    if not photo_path:
        return "none"
    p = Path(photo_path)
    if not p.is_file():
        return "none"
    return f"url('file://{p.resolve()}')"


def build_card_html(
    *,
    eyebrow: str,
    headline: str,
    pill_word: str = "",
    body: str = "",
    footnote: str = "",
    photo_path: str | os.PathLike | None = None,
) -> str:
    """Собирает HTML карточки 1080×1350 по template."""
    body_html = f'<p class="body-text">{_escape(body)}</p>' if body else ""
    footnote_html = f'<p class="footnote">{_escape(footnote)}</p>' if footnote else ""
    return CARD_HTML_TEMPLATE.substitute(
        eyebrow=_escape(eyebrow.upper()),
        headline_html=_build_headline_html(headline, pill_word),
        body_html=body_html,
        footnote_html=footnote_html,
        photo_bg_css=_photo_bg_css(photo_path),
    )


def render_html_to_png_sync(html: str, *, width: int = 1080, height: int = 1350, timeout: int = 60) -> bytes | None:
    """Рендерит HTML в PNG через wkhtmltoimage. Возвращает bytes или None."""
    bin_path = _find_wkhtmltoimage()
    if not bin_path:
        logger.info("wkhtmltoimage not available — fallback to Pillow")
        return None

    with tempfile.TemporaryDirectory() as td:
        tdp = Path(td)
        html_file = tdp / "card.html"
        out_file = tdp / "card.png"
        html_file.write_text(html, encoding="utf-8")

        cmd = [
            bin_path,
            "--width", str(width),
            "--height", str(height),
            "--enable-local-file-access",
            "--javascript-delay", "300",  # дать Google Fonts подгрузиться
            "--quality", "95",
            "--format", "png",
            "--quiet",
            "--encoding", "utf-8",
            str(html_file),
            str(out_file),
        ]
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            logger.warning("wkhtmltoimage timed out after %ss", timeout)
            return None
        except OSError as exc:
            logger.warning("wkhtmltoimage exec failed: %s", exc)
            return None

        if result.returncode != 0 or not out_file.exists() or out_file.stat().st_size == 0:
            err = result.stderr.decode("utf-8", errors="replace")[-500:]
            logger.warning("wkhtmltoimage failed rc=%s err=%s", result.returncode, err)
            return None
        return out_file.read_bytes()


def render_card_to_png(
    *,
    eyebrow: str,
    headline: str,
    pill_word: str = "",
    body: str = "",
    footnote: str = "",
    photo_path: str | os.PathLike | None = None,
    width: int = 1080,
    height: int = 1350,
) -> bytes | None:
    """Главная функция: данные карточки → HTML → PNG через wkhtmltoimage.

    Если рендерер недоступен — возвращает None, вызывающий код должен
    сделать fallback на Pillow (render_automy_card).
    """
    html_doc = build_card_html(
        eyebrow=eyebrow, headline=headline, pill_word=pill_word,
        body=body, footnote=footnote, photo_path=photo_path,
    )
    return render_html_to_png_sync(html_doc, width=width, height=height)
