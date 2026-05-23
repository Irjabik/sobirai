"""Генерация info-карточек для канала Automy AI.

Пайплайн:
1. DeepSeek через OpenRouter парсит title+post_text и возвращает структурированный
   JSON со слотами карточки (компания, категория, главная цифра, подблок, pill).
2. Pillow через image_card.render_info_card рисует карточку 1024×1024
   в фирменном тёмном стиле.

AI больше НЕ рисует пиксели — только парсит смысл новости в структуру.
Это:
- бесплатно (только $0.0001 за DeepSeek-вызов на пост);
- идеальный текст всегда;
- консистентный бренд-стиль;
- ~1 секунда на рендер.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import socket
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from .image_card import ACCENT_COLORS, CardMeta, render_info_card

logger = logging.getLogger(__name__)

OPENROUTER_CHAT_COMPLETIONS_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_PROMPT_MODEL = "deepseek/deepseek-chat-v3.1"
GENERATED_IMAGES_SUBDIR = "generated"


META_SYSTEM_PROMPT = """\
Ты парсишь русскоязычные новости про ИИ/технологии в структурированный JSON
для тёмной info-карточки.

ВАЖНО: все текстовые поля кроме имён брендов и числовых значений ВСЕГДА
на русском. Никаких английских слов вроде RELEASE, REVENUE, NEW MODEL —
только их русские эквиваленты.

У карточки следующие слоты:
  • company_id      — латиница, lowercase id центральной компании из новости:
                      "openai", "anthropic", "google", "meta", "microsoft",
                      "nvidia", "apple", "amazon", "xai", "perplexity",
                      "mistral", "deepseek", "deepmind", "huggingface",
                      "cohere". null если нет центральной компании
                      (отраслевая новость, исследование, регуляция).
  • company_label   — отображаемое название КАПСОМ, макс 14 символов.
                      Если company_id указан → имя компании латиницей
                      ("OPENAI", "ANTHROPIC", "GOOGLE", "META").
                      Если company_id = null → русская тема КАПСОМ:
                      РОБОТОТЕХНИКА, ИССЛЕДОВАНИЕ, РЕГУЛЯЦИЯ, ОПЕНСОРС,
                      AGI, БЕЗОПАСНОСТЬ, ИНДУСТРИЯ, ЖЕЛЕЗО, ОБРАЗОВАНИЕ.
  • category_label  — русский тип новости КАПСОМ, 1-2 слова, макс 18 символов:
                      РЕЛИЗ, СДЕЛКА, ИНВЕСТИЦИИ, ИСК, УТЕЧКА, УВОЛЬНЕНИЯ,
                      ИССЛЕДОВАНИЕ, БЕНЧМАРК, ПАРТНЁРСТВО, ПОГЛОЩЕНИЕ,
                      ИНЦИДЕНТ, РЕГУЛЯЦИЯ, ОПЕНСОРС, БЕТА, API.
  • main_value      — главная цифра/значение из новости (имена моделей,
                      суммы, проценты, числа сотрудников). Примеры:
                      "$4 МЛРД", "GPT-5", "−1100", "17 МИН", "ИСК",
                      "100×", "13,6%", "v2.5 PRO". Макс 12 символов.
                      Это первое что бросается в глаза читателю.
  • sub_label       — русский подзаголовок КАПСОМ, макс 18 символов:
                      ИСТЦЫ, ИНВЕСТИЦИИ, ПОЛЬЗОВАТЕЛИ, ДЛИТЕЛЬНОСТЬ,
                      ВЫРУЧКА, ТОЧНОСТЬ, ЗАТРОНУТО, БЮДЖЕТ, СОТРУДНИКИ,
                      ОСНОВНОЕ, ФОКУС, КЛЮЧЕВОЕ.
  • sub_value       — короткий текст 1-3 слова, на русском или с цифрами.
                      Примеры: "Калифорнийцы", "$25 млрд+", "5 минут",
                      "10М+ юзеров", "12 стран", "Coding & agents",
                      "Speed & Cost". Макс 24 символа.
  • sub_caption     — маленькое уточнение в скобках, опционально. Макс 20 символов.
                      Примеры: "(коллект. иск)", "(2025)", "(в рантайме)",
                      "(AI Ultra)", "". Используй "" если нет хорошей подписи.
  • pill_text       — короткая русская фраза КАПСОМ, макс 22 символа:
                      "УТЕЧКА ДАННЫХ", "НОВАЯ МОДЕЛЬ", "РОСТ ОЦЕНКИ",
                      "МАССОВЫЕ УВОЛЬНЕНИЯ", "FREE-ТАРИФ", "БЕТА",
                      "ПАПЕР НЕДЕЛИ", "НОВЫЙ ТАРИФ".
  • pill_icon       — ВСЕГДА пустая строка "". Иконку в pill не используем.
  • accent          — один из: red, orange, green, blue, purple, cyan, yellow, neutral.
                      Выбирай по эмоциональному тону:
                        red    → иски, утечки, инциденты безопасности
                        orange → увольнения, отмены, регуляция
                        green  → сделки, рост оценки, позитивные релизы
                        blue   → обычные релизы, API, инструменты
                        purple → mega-релизы, фронтир-ресёрч, AGI
                        cyan   → робототехника, железо, физический AI
                        yellow → предупреждения, паузы, беты
                        neutral → нейтральные отраслевые новости

КРИТИЧНО: тщательно выбирай каждое поле. Карточка идёт прямо в канал.
Не выдумывай цифры — только то что есть в исходном тексте. Если в слоте
нет хорошей фактуры — пиши обобщённо («СТАТУС» для sub_label, «В разработке»
для sub_value).

Все РУССКИЕ поля строго кириллицей. Латиница только для company_label
с реальным брендом, для названий моделей (GPT-5, Claude 3.5) и для
англоязычных названий продуктов в sub_value (Speed & Cost, Free Tier).

Выводи СТРОГО один JSON-объект со всеми 9 ключами. Без текста до и после.
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


def _build_card_meta_sync(
    *,
    title: str,
    post_text: str,
    api_key: str,
    model: str = DEFAULT_PROMPT_MODEL,
    timeout: float = 25.0,
) -> tuple[CardMeta | None, str | None]:
    """Просит LLM построить JSON со слотами карточки. Возвращает (meta, error)."""
    if not api_key:
        return None, "no_api_key"

    user_message = (
        f"News title: {title}\n\n"
        f"News body (Russian, may be truncated):\n{(post_text or '')[:1200]}"
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": META_SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        "max_tokens": 500,
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

    company_id_raw = parsed.get("company_id")
    company_id = None if (company_id_raw in (None, "", "null")) else str(company_id_raw).strip().lower()

    accent_raw = str(parsed.get("accent") or "neutral").strip().lower()
    if accent_raw not in ACCENT_COLORS:
        accent_raw = "neutral"

    def _short(value: Any, limit: int) -> str:
        s = ("" if value is None else str(value)).strip()
        return s[:limit]

    meta = CardMeta(
        company_id=company_id,
        company_label=_short(parsed.get("company_label"), 14).upper() or "AI NEWS",
        category_label=_short(parsed.get("category_label"), 18).upper() or "UPDATE",
        main_value=_short(parsed.get("main_value"), 12) or "—",
        sub_label=_short(parsed.get("sub_label"), 18).upper() or "STATUS",
        sub_value=_short(parsed.get("sub_value"), 24) or "—",
        sub_caption=_short(parsed.get("sub_caption"), 20),
        pill_icon=_short(parsed.get("pill_icon"), 1),
        pill_text=_short(parsed.get("pill_text"), 22).upper(),
        accent=accent_raw,
    )
    return meta, None


# --- Сохранение -------------------------------------------------------------
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
    image_model: str = "",       # игнорируется (старый параметр, оставлен для совместимости с bot_handlers)
    prompt_model: str = DEFAULT_PROMPT_MODEL,
    fallback_models: tuple[str, ...] = (),  # игнорируется
) -> tuple[Path | None, str | None, str | None]:
    """Полный пайплайн: LLM → CardMeta → Pillow → save.

    Возвращает (path, debug_prompt_or_meta_json, error).
    """
    if not api_key:
        return None, None, "no_api_key"

    meta, err = await asyncio.to_thread(
        _build_card_meta_sync,
        title=title,
        post_text=post_text,
        api_key=api_key,
        model=prompt_model,
    )
    if meta is None:
        return None, None, err or "meta_build_failed"

    try:
        image_bytes = await asyncio.to_thread(render_info_card, meta)
    except Exception as exc:
        logger.exception("render_info_card crashed for post %s", source_post_id)
        return None, _meta_as_json(meta), f"render_crash: {type(exc).__name__}: {exc}"

    path = await asyncio.to_thread(save_generated_image, source_post_id, image_bytes, data_dir)
    logger.info(
        "info-card rendered post=%s bytes=%s company=%s accent=%s",
        source_post_id, len(image_bytes), meta.company_id or meta.company_label, meta.accent,
    )
    return path, _meta_as_json(meta), None


def _meta_as_json(meta: CardMeta) -> str:
    """Сериализует CardMeta в человекочитаемый JSON для логирования / диагностики."""
    return json.dumps(
        {
            "company_id": meta.company_id,
            "company_label": meta.company_label,
            "category_label": meta.category_label,
            "main_value": meta.main_value,
            "sub_label": meta.sub_label,
            "sub_value": meta.sub_value,
            "sub_caption": meta.sub_caption,
            "pill_icon": meta.pill_icon,
            "pill_text": meta.pill_text,
            "accent": meta.accent,
        },
        ensure_ascii=False,
    )
