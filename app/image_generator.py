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
You parse Russian-language AI/tech news into a structured JSON for a dark
minimalist info-card.

The card has these slots:
  • company_id      — lowercase machine id of the central company in the news
                      ("openai", "anthropic", "google", "meta", "microsoft",
                      "nvidia", "apple", "amazon", "xai", "perplexity",
                      "mistral", "deepseek", "deepmind", "huggingface",
                      "cohere"). USE NULL if no single company is central
                      (industry-wide news, research summaries, regulations).
  • company_label   — uppercase display label, max 14 chars.
                      If company_id is set → uppercase name ("OPENAI", "ANTHROPIC").
                      If company_id is NULL → broad TOPIC label in English caps:
                      ROBOTICS, RESEARCH, REGULATION, OPEN-SOURCE, AGI, SAFETY,
                      INDUSTRY, HARDWARE, EDUCATION, GAMING. Pick one.
  • category_label  — short English uppercase classifier (1-3 words, max 18 chars):
                      RELEASE, DEAL, FUNDING, LAWSUIT, DATA LEAK, LAYOFFS,
                      RESEARCH, BENCHMARK, PARTNERSHIP, ACQUISITION, INCIDENT,
                      REGULATION, OPEN SOURCE, BETA, API UPDATE.
  • main_value      — the single most striking value from the news. Examples:
                      "$4B", "GPT-5", "−1100", "17 МИН", "ИСК", "100×",
                      "v2.5 PRO", "13,6%". Max 12 chars, can contain Russian.
                      Should be the FIRST thing the reader notices.
  • sub_label       — English uppercase label for the secondary metric, max 18 chars.
                      Examples: PLAINTIFFS, FUNDING, USERS, DURATION, REVENUE,
                      ACCURACY, AFFECTED, BUDGET, EMPLOYEES.
  • sub_value       — short text 1-3 words, can be Russian. Examples:
                      "Calif. users", "$25B+", "5 минут", "10M+", "12 стран".
                      Max 24 chars.
  • sub_caption     — small clarifier in parentheses, optional. Max 20 chars.
                      Examples: "(class action)", "(2025)", "(RUN-RATE)", "".
                      Use "" if no good caption.
  • pill_icon       — single ASCII symbol fitting the news mood. Choose from:
                      "!" warning/danger, "*" feature, ">" launch, "$" money,
                      "^" growth, "v" decline, "+" addition, "x" cancellation,
                      "?" uncertainty, "&" partnership. NO emoji. Max 1 char.
  • pill_text       — short English uppercase phrase, max 22 chars. Examples:
                      "PRIVACY SCANDAL", "NEW MODEL", "VALUATION UP",
                      "MASS LAYOFFS", "FREE TIER", "BETA RELEASE",
                      "PAPER OF THE WEEK".
  • accent          — one of: red, orange, green, blue, purple, cyan, yellow, neutral.
                      Pick by emotional tone of the news:
                        red    → lawsuits, leaks, security incidents
                        orange → layoffs, cancellations, regulations
                        green  → deals, valuations up, positive launches
                        blue   → routine releases, API updates, tools
                        purple → mega releases, frontier research, AGI claims
                        cyan   → robotics, hardware, physical AI
                        yellow → warnings, paused projects, betas
                        neutral → industry news without strong sentiment

CRITICAL: choose every field thoughtfully. Card is shown directly to readers.
Do not invent numbers — only use what's in the source text. If a slot has no
good source data, use the most generic informative phrasing (e.g. "STATUS"
for sub_label and "В ПРОЦЕССЕ" for sub_value).

Output STRICTLY one JSON object with all 9 keys. No prose around it.
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
