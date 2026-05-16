"""Генерация обложек постов через OpenRouter.

Два этапа:
1. Concept Mapper (DeepSeek через OpenRouter): по title+post_text выбирает один
   концепт-символ из словаря и собирает финальный image prompt с фиксированным
   стилем Automy AI (чёрный фон + монохром + минимализм + без текста).
2. Image Generator (Flux Schnell через OpenRouter): рендерит картинку 1024x1024.

Если не получилось — возвращаем None, бот публикует пост как text-only.
"""
from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import re
import socket
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

OPENROUTER_CHAT_COMPLETIONS_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_IMAGES_GENERATIONS_URL = "https://openrouter.ai/api/v1/images/generations"
DEFAULT_IMAGE_MODEL = "black-forest-labs/flux-schnell"
DEFAULT_PROMPT_MODEL = "deepseek/deepseek-chat-v3.1"
GENERATED_IMAGES_SUBDIR = "generated"


# --- Style Anchor: зашит в каждый prompt, не меняется -----------------------
STYLE_ANCHOR = (
    "Minimalist abstract composition on solid pure black background (#0a0a0a). "
    "Strict monochrome palette: pure white (#ffffff), light gray (#a0a0a0), "
    "medium gray (#666666). Centered single subject with massive negative space; "
    "subject occupies no more than 30% of the canvas. Clean curved or geometric "
    "lines, flat 2D vector design, no gradients except subtle within white shapes. "
    "No text, no letters, no numbers, no logos in the main composition. "
    "No human faces, no photorealism. Square 1:1 aspect ratio."
)

NEGATIVE_PROMPT = (
    "text in main subject, letters in main subject, numbers, captions, headers, "
    "faces, people, photorealistic, colored background, bright saturated colors, "
    "rainbow gradients, ornaments, cluttered composition, multiple subjects, "
    "border, frame, top banner, white stripe, white bar at the top, top edge highlight"
)


# --- Concept Mapper: словарь типов новостей → символов ----------------------
CONCEPT_DICT_DESCRIPTION = """\
Concept dictionary — choose ONE concept that best fits the news, then describe it
in your prompt using the listed visual cues:

1. release / new product / new model
   → three layered curved arcs stacked vertically, thin gray to thick white,
     soft glow on bottom arc (Automy AI signature arcs)

2. deal / investment / IPO / funding
   → three ascending parallel lines or stylized upward arrow made of three segments

3. layoffs / cuts / shutdown / cancellation
   → fragmented white circle broken into uneven arc segments drifting apart

4. robotics / hardware / physical AI
   → minimalist robot silhouette head, single eye-dot, geometric

5. RAG / embeddings / vector search / knowledge graph
   → three connected white circular nodes forming a triangle, thin gray edges

6. API / developer tool / framework / SDK
   → stylized angle brackets enclosing a small gray dot, vector style

7. video / multimodal / generative media
   → three triangular play symbols of different sizes overlapping

8. safety / security / incident / hack
   → minimalist shield (half-circle with center line) or warning triangle outline

9. research / paper / benchmark / scientific result
   → three horizontal lines of different lengths stacked, like data bars

10. partnership / merger / acquisition / collaboration
    → two overlapping circles forming a Venn diagram, monochrome

11. fallback (if none clearly fit)
    → three concentric curved arcs (the Automy AI logo signature)
"""


def _build_meta_prompt_for_concept() -> str:
    return (
        "You are a cover designer for the Telegram channel Automy AI (AI news in Russian).\n"
        "Every cover MUST follow this style:\n"
        f"{STYLE_ANCHOR}\n\n"
        f"{CONCEPT_DICT_DESCRIPTION}\n\n"
        "Task: read the news, pick ONE concept from the dictionary, and write a single-line "
        "image generation prompt in English that combines:\n"
        "  • the chosen concept's visual cues\n"
        "  • the full STYLE rules above (repeat them in your prompt)\n"
        "  • the canvas: square 1024x1024\n"
        "  • IMPORTANT: explicitly forbid any top white stripe/banner/frame/border at the edges.\n\n"
        "Output: a JSON object with a single key 'prompt' containing the final string.\n"
        "Example output:\n"
        '{"prompt": "Minimalist abstract composition on solid pure black background filling the '
        'entire canvas edge-to-edge with no borders or stripes. Three layered curved arcs stacked '
        "vertically, thin gray to thick white, soft glow on bottom arc representing breakthrough. "
        "Strict monochrome white/gray palette. Massive negative space. Flat 2D vector design, "
        "clean lines, no text in main subject. Square 1024x1024.\"}"
    )


# --- HTTP-обвязка (стиль такой же, как в llm_openrouter.py) -----------------
_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; SobiraiBot/1.0; +https://github.com/Irjabik/sobirai) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_REFERER = "https://github.com/Irjabik/sobirai"
_X_TITLE = "Sobirai AI News Bot"


def _http_post_json(url: str, payload: dict[str, Any], api_key: str, timeout: float) -> tuple[bool, Any, str | None]:
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": _DEFAULT_USER_AGENT,
        "HTTP-Referer": _REFERER,
        "X-Title": _X_TITLE,
    }
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        try:
            return True, json.loads(raw), None
        except json.JSONDecodeError:
            return False, raw, "invalid_json"
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")[:800]
        logger.warning("image-gen HTTP %s on %s: %s", exc.code, url, err_body)
        return False, err_body, f"http_{exc.code}"
    except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
        logger.warning("image-gen network error on %s: %s", url, exc)
        return False, None, "network"
    except Exception:
        logger.exception("image-gen unexpected error on %s", url)
        return False, None, "unknown"


# --- Этап 1: построение image prompt ----------------------------------------
def build_image_prompt_sync(
    *,
    title: str,
    post_text: str,
    api_key: str,
    model: str = DEFAULT_PROMPT_MODEL,
    timeout: float = 25.0,
) -> str | None:
    """Просит DeepSeek собрать image prompt в стиле Automy AI. Возвращает строку или None."""
    if not api_key:
        return None
    user_message = (
        f"Title: {title}\n\n"
        f"Body (truncated):\n{(post_text or '')[:600]}"
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": _build_meta_prompt_for_concept()},
            {"role": "user", "content": user_message},
        ],
        "max_tokens": 400,
        "temperature": 0.7,
        "response_format": {"type": "json_object"},
    }
    ok, data, err = _http_post_json(OPENROUTER_CHAT_COMPLETIONS_URL, payload, api_key, timeout)
    if not ok or not isinstance(data, dict):
        logger.warning("Concept mapper failed: %s", err)
        return None
    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return None
    parsed = _parse_json_object(content)
    if not parsed or "prompt" not in parsed:
        return None
    prompt = str(parsed.get("prompt") or "").strip()
    return prompt or None


def _parse_json_object(text: str) -> dict[str, Any] | None:
    t = (text or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", t)
        t = re.sub(r"\s*```$", "", t)
    try:
        obj = json.loads(t)
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}", t)
        if not m:
            return None
        try:
            obj = json.loads(m.group(0))
        except json.JSONDecodeError:
            return None
    return obj if isinstance(obj, dict) else None


# --- Этап 2: генерация изображения ------------------------------------------
def generate_image_bytes_sync(
    *,
    prompt: str,
    api_key: str,
    model: str = DEFAULT_IMAGE_MODEL,
    timeout: float = 60.0,
) -> bytes | None:
    """Зовёт OpenRouter image-gen endpoint. Возвращает PNG bytes или None.

    Сначала пробуем images/generations (OpenAI-style). Если 404 / unsupported —
    chat/completions с modalities=image (некоторые модели на OpenRouter работают так).
    """
    if not api_key or not prompt:
        return None

    full_prompt = prompt
    if "negative" not in full_prompt.lower():
        full_prompt = f"{prompt}\n\nNegative: {NEGATIVE_PROMPT}"

    # Попытка 1 — images/generations
    payload_a = {
        "model": model,
        "prompt": full_prompt,
        "n": 1,
        "size": "1024x1024",
        "response_format": "b64_json",
    }
    ok, data, err = _http_post_json(OPENROUTER_IMAGES_GENERATIONS_URL, payload_a, api_key, timeout)
    if ok and isinstance(data, dict):
        img = _extract_image_from_openai_response(data)
        if img:
            return img

    # Попытка 2 — chat/completions с modalities=image
    payload_b = {
        "model": model,
        "messages": [{"role": "user", "content": full_prompt}],
        "modalities": ["image"],
    }
    ok2, data2, err2 = _http_post_json(OPENROUTER_CHAT_COMPLETIONS_URL, payload_b, api_key, timeout)
    if ok2 and isinstance(data2, dict):
        img = _extract_image_from_chat_response(data2)
        if img:
            return img

    # Сохраняем детали последнего провала в БД (для /diagimage), если есть путь к ней
    detail = (
        f"images/generations: {err}\n"
        f"  body_a: {_short(data, 300)}\n"
        f"chat/completions modalities=image: {err2}\n"
        f"  body_b: {_short(data2, 300)}"
    )
    logger.warning("image-gen failed: %s", detail.replace("\n", " | "))
    return None


def _short(value: Any, limit: int = 300) -> str:
    s = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
    if len(s) > limit:
        return s[:limit] + "…"
    return s


def _extract_image_from_openai_response(data: dict[str, Any]) -> bytes | None:
    items = data.get("data") or []
    if not isinstance(items, list) or not items:
        return None
    first = items[0]
    if not isinstance(first, dict):
        return None
    b64 = first.get("b64_json")
    if isinstance(b64, str) and b64:
        try:
            return base64.b64decode(b64)
        except (binascii.Error, ValueError):
            return None
    url = first.get("url")
    if isinstance(url, str) and url.startswith(("http://", "https://")):
        return _download_image(url)
    return None


def _extract_image_from_chat_response(data: dict[str, Any]) -> bytes | None:
    choices = data.get("choices") or []
    if not choices or not isinstance(choices[0], dict):
        return None
    msg = choices[0].get("message") or {}
    images = msg.get("images")
    if isinstance(images, list) and images:
        first = images[0]
        if isinstance(first, dict):
            url_field = first.get("image_url") or first.get("url")
            if isinstance(url_field, dict):
                url_field = url_field.get("url")
            if isinstance(url_field, str):
                if url_field.startswith("data:image"):
                    after_comma = url_field.split(",", 1)
                    if len(after_comma) == 2:
                        try:
                            return base64.b64decode(after_comma[1])
                        except (binascii.Error, ValueError):
                            return None
                if url_field.startswith(("http://", "https://")):
                    return _download_image(url_field)
    content = msg.get("content")
    if isinstance(content, str):
        m = re.search(r"data:image[^,]+,([A-Za-z0-9+/=]+)", content)
        if m:
            try:
                return base64.b64decode(m.group(1))
            except (binascii.Error, ValueError):
                pass
        m2 = re.search(r"https?://\S+\.(?:png|jpg|jpeg|webp)", content)
        if m2:
            return _download_image(m2.group(0))
    return None


def _download_image(url: str, timeout: float = 30.0) -> bytes | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _DEFAULT_USER_AGENT})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except Exception as exc:
        logger.warning("failed to download generated image: %s", exc)
        return None


# --- Сохранение / поиск -----------------------------------------------------
def generated_images_dir(data_dir: str | Path) -> Path:
    d = Path(data_dir) / GENERATED_IMAGES_SUBDIR
    d.mkdir(parents=True, exist_ok=True)
    return d


def save_generated_image(source_post_id: int, image_bytes: bytes, data_dir: str | Path) -> Path:
    out = generated_images_dir(data_dir) / f"{source_post_id}.png"
    out.write_bytes(image_bytes)
    return out


# --- Высокоуровневый async wrapper ------------------------------------------
async def generate_post_image(
    *,
    source_post_id: int,
    title: str,
    post_text: str,
    api_key: str,
    data_dir: str | Path,
    image_model: str = DEFAULT_IMAGE_MODEL,
    prompt_model: str = DEFAULT_PROMPT_MODEL,
    fallback_models: tuple[str, ...] = ("google/gemini-2.5-flash-image", "openai/dall-e-3"),
) -> tuple[Path | None, str | None, str | None]:
    """Полный пайплайн: prompt → image → save.

    Возвращает (path, prompt, error_string). На успехе error=None. На провале
    error содержит подробности (для /diagimage).
    """
    if not api_key:
        return None, None, "no_api_key"

    prompt = await asyncio.to_thread(
        build_image_prompt_sync,
        title=title,
        post_text=post_text,
        api_key=api_key,
        model=prompt_model,
    )
    if not prompt:
        msg = "concept mapper returned empty prompt"
        logger.warning("image-gen %s for post %s", msg, source_post_id)
        return None, None, msg

    tried: list[str] = []
    for candidate in (image_model, *fallback_models):
        if candidate in tried:
            continue
        tried.append(candidate)
        image = await asyncio.to_thread(
            generate_image_bytes_sync,
            prompt=prompt,
            api_key=api_key,
            model=candidate,
        )
        if image:
            path = await asyncio.to_thread(save_generated_image, source_post_id, image, data_dir)
            logger.info(
                "image-gen ok post=%s model=%s bytes=%s path=%s",
                source_post_id, candidate, len(image), path,
            )
            return path, prompt, None
        logger.warning(
            "image-gen model=%s failed for post %s, trying fallback",
            candidate, source_post_id,
        )
    err = f"all models failed: {', '.join(tried)}. prompt_head={prompt[:120]}"
    return None, prompt, err
