"""Генерация карточек в стиле Automy AI Instagram-каруселей.

Пайплайн:
1. DeepSeek через OpenRouter парсит русскую новость в JSON с слотами
   (eyebrow, headline, pill_word, body, footnote) + image prompt в
   editorial-стиле Automy (off-white фон + один оранжевый акцент).
2. AI-модель (Flux Schnell → Gemini Flash Image → DALL-E 3 fallback)
   рендерит фоновое фото 1080×760 в верхнюю зону.
3. Pillow собирает финальную карточку 1080×1350 поверх фото:
   brand-stamp top-left → eyebrow → h1 с pill → body → footnote.
"""
from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import os
import re
import socket
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from .image_card import AutomyCardMeta, CardMeta, render_automy_card, render_info_card
from .image_html_renderer import html_renderer_available, render_card_to_png

logger = logging.getLogger(__name__)

OPENROUTER_CHAT_COMPLETIONS_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_IMAGES_GENERATIONS_URL = "https://openrouter.ai/api/v1/images/generations"
DEFAULT_IMAGE_MODEL = "black-forest-labs/flux-schnell"
DEFAULT_PROMPT_MODEL = "deepseek/deepseek-chat-v3.1"
GENERATED_IMAGES_SUBDIR = "generated"
GENERATED_PHOTOS_SUBDIR = "generated_photos"


META_SYSTEM_PROMPT = """\
Ты парсишь русскоязычную AI/tech новость в JSON для info-карточки канала
Automy AI (Instagram-карусель стиль). Все тексты — на русском (кроме имён
брендов и числовых значений).

ОБЯЗАТЕЛЬНЫЕ ПРАВИЛА ПО БРЕНДУ:
- Букву «ё» НЕ используем. Везде пишем «е» («еще», «все», «черный»).
- Длинное тире (—) обязательно где грамматически уместно, между словами.
  Дефис-минус (-) только внутри слов: AI-инструмент, 3-5 лет.
- Без слоп-фраз: «AI меняет всё», «революция», «прорыв», «не упусти шанс»,
  «эра AI». Конкретика > пафоса.
- Без эмодзи. Без декоративных символов.
- Тон взрослый, для предпринимателей. Без «братишек», «давайте», «погнали».

СЛОТЫ КАРТОЧКИ:

  • eyebrow — категория новости КАПСОМ (русский, max 18 символов):
    РЕЛИЗ, СДЕЛКА, ИНВЕСТИЦИИ, ИСК, УТЕЧКА, УВОЛЬНЕНИЯ, ИССЛЕДОВАНИЕ,
    БЕНЧМАРК, ПАРТНЕРСТВО, ПОГЛОЩЕНИЕ, ИНЦИДЕНТ, РЕГУЛЯЦИЯ, ОПЕНСОРС,
    БЕТА, API, РОБОТОТЕХНИКА, ИИ-АГЕНТЫ.

  • headline — главный тезис карточки в 2-3 строки. ВКЛЮЧАЕТ pill_word.
    Примеры в стиле Automy:
      «За AI платят 0.3% планеты»
      «Anthropic подняла $4 млрд от Amazon»
      «OpenAI обвиняют в передаче данных»
      «Meta режет 1100 сотрудников»
    Max 80 символов. Без точки в конце.

  • pill_word — ключевое слово (или короткая фраза 1-3 слова) ИЗ headline,
    которое будет обёрнуто в оранжевый pill. На нём должен держаться
    эмоциональный/смысловой акцент.
    Примеры:
      headline «За AI платят 0.3% планеты» → pill_word «0.3% планеты»
      headline «Anthropic подняла $4 млрд от Amazon» → pill_word «$4 млрд»
      headline «OpenAI обвиняют в передаче данных» → pill_word «обвиняют»
      headline «Meta режет 1100 сотрудников» → pill_word «1100 сотрудников»
    ВАЖНО: pill_word должен присутствовать в headline БУКВАЛЬНО,
    case-insensitive поиск. Иначе pill не отрисуется.

  • body — 1-2 предложения, развернуто. Утверждение + цифра/пример.
    Без точки в конце последнего предложения.
    Max 180 символов.

  • footnote — серая мелкая строка внизу. Цифра или нюанс.
    Без точки в конце. Max 100 символов. "" если нет хорошей фактуры.

  • image_prompt — английский prompt для editorial-фото в стиле Automy AI.
    Шаблон:
      "editorial photography of [OBJECT relating to news], flat off-white
       paper background, [KEY ELEMENT] is the only colored element glowing
       bright orange #F67F2F, everything else in soft monochrome black and
       white tones, sharp focus, soft studio lighting, magazine cover
       aesthetic, minimalist composition, 4:5 portrait, no text on image,
       plain unmarked"
    Подставляй конкретный объект и оранжевый элемент по смыслу новости.

  • photo_is_dark — true если фон фото будет тёмным/средним (для brand-stamp
    нужен белый текст), false если светлый off-white (нужен чёрный текст).
    В editorial-стиле Automy фон почти всегда off-white (светлый) — обычно false.

ВЫВОДИ строго один JSON со всеми 7 ключами. Без текста до и после.
"""


_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; SobiraiBot/1.0) AppleWebKit/537.36"
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
        logger.warning("image-gen HTTP %s: %s", exc.code, err_body)
        return False, err_body, f"http_{exc.code}"
    except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
        logger.warning("image-gen network: %s", exc)
        return False, None, "network"
    except Exception:
        logger.exception("image-gen unexpected")
        return False, None, "unknown"


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


def _build_card_slots_sync(
    *, title: str, post_text: str, api_key: str,
    model: str = DEFAULT_PROMPT_MODEL, timeout: float = 25.0,
) -> tuple[dict[str, Any] | None, str | None]:
    """LLM собирает JSON со слотами + image_prompt. Возвращает (slots, error)."""
    if not api_key:
        return None, "no_api_key"
    user_message = f"Заголовок: {title}\n\nТело новости:\n{(post_text or '')[:1500]}"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": META_SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        "max_tokens": 700,
        "temperature": 0.4,
        "response_format": {"type": "json_object"},
    }
    ok, data, err = _http_post_json(OPENROUTER_CHAT_COMPLETIONS_URL, payload, api_key, timeout)
    if not ok or not isinstance(data, dict):
        return None, f"chat_call_failed:{err}"
    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return None, "no_message_content"
    parsed = _parse_json_object(content)
    if not parsed:
        return None, "json_parse_failed"

    # Нормализуем поля
    def _s(v: Any, limit: int) -> str:
        s = ("" if v is None else str(v)).strip()
        # Запрет ё → е (бренд-правило)
        s = s.replace("ё", "е").replace("Ё", "Е")
        # Длинное тире
        s = re.sub(r"(?<=\s)-(?=\s)", "—", s)
        return s[:limit]

    slots = {
        "eyebrow": _s(parsed.get("eyebrow"), 28).upper() or "AI",
        "headline": _s(parsed.get("headline"), 100) or "AI NEWS",
        "pill_word": _s(parsed.get("pill_word"), 32) or "",
        "body": _s(parsed.get("body"), 220),
        "footnote": _s(parsed.get("footnote"), 140),
        "image_prompt": str(parsed.get("image_prompt") or "").strip()[:600],
        "photo_is_dark": bool(parsed.get("photo_is_dark", False)),
    }
    return slots, None


# === Image generation (AI photo) ===
def _generate_photo_bytes_sync(
    *, prompt: str, api_key: str, model: str = DEFAULT_IMAGE_MODEL, timeout: float = 60.0,
) -> bytes | None:
    """Генерит editorial-фото через OpenRouter. Возвращает PNG bytes или None."""
    if not api_key or not prompt:
        return None
    # images/generations endpoint.
    # Запрашиваем landscape 1024×768 (4:3) — точно ложится в нашу photo zone
    # 1080×760 (1.42 landscape). При cover-crop теряется ~5% вместо ~25%
    # как было с 1024×1024.
    # ВАЖНО: best-effort. Flux Schnell через OpenRouter может игнорировать
    # size и возвращать квадрат 1024×1024 — в этом случае cover-crop в
    # _load_photo (image_card.py) обрежет лишнее, не падая.
    payload_a = {
        "model": model,
        "prompt": prompt,
        "n": 1,
        "size": "1024x768",
        "response_format": "b64_json",
    }
    ok, data, err = _http_post_json(OPENROUTER_IMAGES_GENERATIONS_URL, payload_a, api_key, timeout)
    if ok and isinstance(data, dict):
        img = _extract_image_from_openai_response(data)
        if img:
            return img

    # chat/completions с modalities=image (для моделей не поддерживающих images/generations)
    payload_b = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "modalities": ["image"],
    }
    ok2, data2, err2 = _http_post_json(OPENROUTER_CHAT_COMPLETIONS_URL, payload_b, api_key, timeout)
    if ok2 and isinstance(data2, dict):
        img = _extract_image_from_chat_response(data2)
        if img:
            return img

    logger.warning("photo-gen failed: a=%s b=%s", err, err2)
    return None


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
        logger.warning("photo download failed: %s", exc)
        return None


# === Сохранение / общественные функции ===
def generated_images_dir(data_dir: str | Path) -> Path:
    d = Path(data_dir) / GENERATED_IMAGES_SUBDIR
    d.mkdir(parents=True, exist_ok=True)
    return d


def _generated_photos_dir(data_dir: str | Path) -> Path:
    d = Path(data_dir) / GENERATED_PHOTOS_SUBDIR
    d.mkdir(parents=True, exist_ok=True)
    return d


def save_generated_image(source_post_id: int, image_bytes: bytes, data_dir: str | Path) -> Path:
    out = generated_images_dir(data_dir) / f"{source_post_id}.png"
    out.write_bytes(image_bytes)
    return out


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
    """Полный пайплайн: LLM-слоты → AI-фото → Pillow-карточка → save.

    Возвращает (path_to_card, slots_json_for_log, error).
    """
    if not api_key:
        return None, None, "no_api_key"

    slots, err = await asyncio.to_thread(
        _build_card_slots_sync,
        title=title, post_text=post_text, api_key=api_key, model=prompt_model,
    )
    if slots is None:
        return None, None, err or "slots_failed"

    # Генерация фото: пробуем основную + fallback'и
    photo_bytes: bytes | None = None
    image_prompt = slots.get("image_prompt") or ""
    tried: list[str] = []
    for candidate in (image_model, *fallback_models):
        if candidate in tried:
            continue
        tried.append(candidate)
        photo_bytes = await asyncio.to_thread(
            _generate_photo_bytes_sync,
            prompt=image_prompt, api_key=api_key, model=candidate,
        )
        if photo_bytes:
            break

    photo_path: Path | None = None
    if photo_bytes:
        photos_dir = _generated_photos_dir(data_dir)
        photo_path = photos_dir / f"{source_post_id}.png"
        photo_path.write_bytes(photo_bytes)
    else:
        logger.warning("photo-gen returned None for post %s; rendering with paper placeholder", source_post_id)

    # Безопасное чтение слотов с дефолтами на случай рефакторинга словаря.
    s_eyebrow = slots.get("eyebrow") or "AI"
    s_headline = slots.get("headline") or "AI NEWS"
    s_pill = slots.get("pill_word") or ""
    s_body = slots.get("body") or ""
    s_footnote = slots.get("footnote") or ""

    # Сборка карточки: сначала пробуем HTML+CSS через wkhtmltoimage,
    # fallback — Pillow render_automy_card.
    card_bytes: bytes | None = None
    if html_renderer_available():
        try:
            card_bytes = await asyncio.to_thread(
                render_card_to_png,
                eyebrow=s_eyebrow,
                headline=s_headline,
                pill_word=s_pill,
                body=s_body,
                footnote=s_footnote,
                photo_path=photo_path,
            )
        except Exception:
            logger.exception("HTML renderer crashed for post %s", source_post_id)
            card_bytes = None
        if card_bytes:
            logger.info("post %s rendered via wkhtmltoimage (HTML+CSS)", source_post_id)

    if card_bytes is None:
        meta = AutomyCardMeta(
            eyebrow=s_eyebrow,
            headline=s_headline,
            pill_word=s_pill,
            body=s_body,
            footnote=s_footnote,
            photo_path=photo_path,
            photo_is_dark=slots.get("photo_is_dark", False),
        )
        try:
            card_bytes = await asyncio.to_thread(render_automy_card, meta)
        except Exception as exc:
            logger.exception("render_automy_card crashed for post %s", source_post_id)
            return None, json.dumps(slots, ensure_ascii=False), f"render_crash: {type(exc).__name__}: {exc}"
        logger.info("post %s rendered via Pillow fallback", source_post_id)

    out_path = await asyncio.to_thread(save_generated_image, source_post_id, card_bytes, data_dir)
    logger.info(
        "automy card rendered post=%s bytes=%s photo=%s eyebrow=%s",
        source_post_id, len(card_bytes), bool(photo_path), slots["eyebrow"],
    )
    slots_log = json.dumps(slots, ensure_ascii=False)
    return out_path, slots_log, None
