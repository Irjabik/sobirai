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
import re
import socket
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

# image_card / image_html_renderer теперь не используются для финального
# рендера — после смены подхода на «чистое AI-фото» текстовые слои больше
# не рисуются. Импорты CardMeta / render_info_card сохранены только для
# обратной совместимости с другими модулями (если они импортируют отсюда).
from .image_card import CardMeta, render_info_card  # noqa: F401

logger = logging.getLogger(__name__)

OPENROUTER_CHAT_COMPLETIONS_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_IMAGES_GENERATIONS_URL = "https://openrouter.ai/api/v1/images/generations"
# Flux Schnell — дешёвый ($0.003/img) и быстрый. С усиленным prompt
# «no text, blank screens» галлюцинаций букв не должно быть. Fallback'и
# поднимают качество, если основной отказал.
DEFAULT_IMAGE_MODEL = "black-forest-labs/flux-schnell"
DEFAULT_PROMPT_MODEL = "deepseek/deepseek-chat-v3.1"
GENERATED_IMAGES_SUBDIR = "generated"
GENERATED_PHOTOS_SUBDIR = "generated_photos"


# Визуальный бриф — общий источник правды. Используется и при генерации
# image_prompt (META_SYSTEM_PROMPT), и при проверке (CRITIC_SYSTEM_PROMPT),
# чтобы критик чинил промпт по тем же правилам, по которым он создавался.
# Это МЕТОД (как придумать кадр под любую новость), а не словарь-таблица:
# таблица не покрывала хвост редких сюжетов и плодила дежурные повторы.
_IMAGE_BRIEF_GUIDE = """\
КАК ПРИДУМАТЬ КАДР — это МЕТОД, а не словарь. Примеры ниже показывают ход
мысли, их нельзя копировать дословно. Метод обязан давать свежий кадр для
ЛЮБОЙ новости, даже той, которой нет в примерах.

ШАГ 1. Вытащи ОДНУ суть — конкретное событие этой новости, не общую тему.
ДЕНЬГИ ВЫИГРЫВАЮТ: если суть про сумму, бюджет, перерасход, трату, убыток,
выручку, прибыль, инвестиции, раунд, IPO, цену, штраф — это денежная новость,
даже если в тексте есть сроки («за 4 месяца», «за год»). Срок — фон, не суть.

ШАГ 2. Выбери ТИП кадра:
  A) В новости есть конкретный физический ГЕРОЙ-предмет — чип, видеокарта,
     робот, дрон, авто, очки, смартфон, наушники, конкретное устройство или
     продукт? → СНИМИ ЕГО: крупный editorial-still этого самого предмета на
     off-white фоне, оранжевый акцент (подсветка/деталь). Уникальный кадр под
     конкретный продукт — метафора не нужна, и двух одинаковых не выйдет.
  B) Новость абстрактная — деньги, закон/регуляция, конкуренция, рост или
     падение, безопасность, увольнения, иск, партнерство, утечка? → Построй
     метафору из трех кубиков:
       ГЕРОЙ-ОБЪЕКТ (что физически олицетворяет суть)
       + ДЕЙСТВИЕ/СОСТОЯНИЕ (горит, утекает, рушится, заперт, перевешивает,
         опрокинут, раскалывается, стоит на пьедестале)
       + единственный оранжевый акцент #F67F2F.
     Собирай НЕОЧЕВИДНЫЙ объект именно под эту новость, а не дежурный символ.

ШАГ 3. Собери prompt по шаблону:
  "editorial product photography of [SCENE], smooth flat off-white paper
   background, [HERO ELEMENT] is the only colored element glowing bright
   orange #F67F2F, everything else in soft monochrome black and white tones,
   sharp focus, ultra detailed textures, soft studio lighting, magazine cover
   aesthetic, minimalist composition, 1:1 square, no text anywhere, no
   letters, no numbers, no typography, no labels, blank unmarked surfaces"

ПРИМЕРЫ ХОДА МЫСЛИ (иллюстрация метода, НЕ список для копирования):
  перерасход/сжигание бюджета → burning stack of cash banknotes, flames
       consuming paper money on a pedestal
  слив денег → coins draining through a funnel / a hole in a sack / slipping
       through open fingers
  убыток > доход → tipping balance scale: huge pile of coins outweighing a
       tiny one
  инвестиции/IPO → tall neat stacks of coins; golden bell on a pedestal
  увольнения → row of empty office chairs, one tipped over
  утечка данных → cracked glass vessel leaking glowing orange drops
  иск → judge gavel on a pedestal
  запрет/регуляция → padlock; road barrier; wall between two objects
  партнерство → handshake of a human hand and a robotic hand
  гонка → sprinter starting blocks; two chess kings facing off
  рост → ascending staircase of blocks, top one orange
  падение → toppling dominoes; cracked pedestal
  новый чип/железо → glossy black processor chip, orange circuit traces glowing
  робот/ИИ-агенты → minimalist matte robotic arm or android hand on a pedestal
  релиз модели → glossy black cube on a pedestal, orange seam of light
       splitting it open

ЖЕСТКИЕ ЗАПРЕТЫ (от повторов и дежурных дефолтов):
- НЕЛЬЗЯ сваливаться в дежурную затычку, когда метафора не придумывается.
  ЗАПРЕЩЕНЫ как «затычка»: песочные часы, часы, будильник, календарь,
  светящийся шар или сфера, облако данных, неоновый мозг, абстрактные
  светящиеся линии, серверный шкаф, generic «device with button». Они не
  значат НИЧЕГО и повторяются из поста в пост. Не идет метафора — вернись к
  ШАГУ 2A и сними конкретный предмет из новости.
- ВРЕМЯ (часы, песочные часы, секундомер, календарь) — только если новость
  буквально про дедлайн, таймер или дату. Для денег и всего прочего — нет.
- Каждый промпт ОБЯЗАН нести деталь именно ЭТОЙ новости (предмет, продукт,
  субъект), чтобы две разные новости не дали одинаковый кадр.
- буквы/цифры/слова в кадре; экраны с UI (если экран есть — выключен, черный);
  газеты, документы, билборды, ценники с цифрами.

ПРОВЕРКА перед выводом: представь это фото без подписи рядом с заголовком.
1) Суть ясна за 1 секунду? 2) Кадр специфичен ИМЕННО для этой новости, а не
подойдет к любой? 3) Это не дежурный символ (часы/шар/облако/шкаф)? Если хоть
одно «нет» — переделай по ШАГУ 2."""


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
    Картинка идет в пост БЕЗ подписи, поэтому метафора обязана считываться
    за 1 секунду. Никаких «серверных шкафов» и «абстрактных устройств» —
    они выглядят как холодильники и не значат ничего. Собери его так:

""" + _IMAGE_BRIEF_GUIDE + """

  • photo_is_dark — true если фон фото будет тёмным/средним (для brand-stamp
    нужен белый текст), false если светлый off-white (нужен чёрный текст).
    В editorial-стиле Automy фон почти всегда off-white (светлый) — обычно false.

ВЫВОДИ строго один JSON со всеми 7 ключами. Без текста до и после.
"""


# Шаг-критик: вторая дешёвая LLM смотрит на новость + предложенный
# image_prompt и решает, считывается ли картинка как ЭТА новость за 1 секунду.
# Если нет — переписывает промпт по той же карте метафор. Это ловит главный
# промах (метафора «время» для денежной новости и т. п.) ДО генерации фото.
CRITIC_SYSTEM_PROMPT = """\
Ты арт-директор канала Automy AI. Тебе дают русскую AI-новость и английский
image_prompt, который уже сгенерировали для editorial-фото к ней. Фото пойдет
в пост БЕЗ подписи — метафора обязана считываться за 1 секунду и попадать
в СУТЬ новости.

Оцени строго: если зритель увидит ТОЛЬКО эту картинку рядом с заголовком —
поймет ли он суть новости за 1 секунду? Частые провалы, которые ты обязан
ловить (любой из них = fits:false):
- метафора «время» (песочные часы, часы, календарь) для новости про ДЕНЬГИ
  (трата, бюджет, перерасход, убыток, выручка, инвестиции);
- дежурная затычка-дефолт (часы, светящийся шар/сфера, облако данных,
  неоновый мозг, серверный шкаф, абстрактные линии) — она подходит к любой
  новости и потому не подходит ни к одной;
- кадр про тему вообще, а не про конкретную драму ИМЕННО этой новости (нет
  story-specific детали — предмета/продукта/субъекта из текста);
- абстракция без смысла.

Чини промпт по этому методу:

""" + _IMAGE_BRIEF_GUIDE + """

ВЫВЕДИ строго один JSON без текста до и после:
{
  "fits": true|false,            // считывается ли исходный промпт как новость
  "reason": "1 короткая фраза по-русски, почему да/нет",
  "fixed_prompt": "английский image_prompt — исправленный по методу, если
                   fits=false; если fits=true, верни исходный промпт без
                   изменений. Всегда полный рабочий промпт по шаблону ШАГА 3."
}
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


def _critique_image_prompt_sync(
    *, title: str, post_text: str, image_prompt: str, api_key: str,
    model: str = DEFAULT_PROMPT_MODEL, timeout: float = 20.0,
) -> tuple[str, str | None]:
    """Шаг-критик: проверяет, считывается ли image_prompt как ЭТА новость,
    и при промахе переписывает его по карте метафор.

    Возвращает (prompt_to_use, note). prompt_to_use — всегда рабочий промпт:
    при любой ошибке/таймауте отдаём исходный (генерацию не блокируем).
    note — короткая строка для лога ("fit", "fixed: …", "skip:…").
    """
    image_prompt = (image_prompt or "").strip()
    if not api_key or not image_prompt:
        return image_prompt, "skip:no_input"

    user_message = (
        f"Новость (заголовок): {title}\n\n"
        f"Новость (тело):\n{(post_text or '')[:1200]}\n\n"
        f"Предложенный image_prompt:\n{image_prompt}"
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": CRITIC_SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        "max_tokens": 500,
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }
    ok, data, err = _http_post_json(OPENROUTER_CHAT_COMPLETIONS_URL, payload, api_key, timeout)
    if not ok or not isinstance(data, dict):
        return image_prompt, f"skip:call_failed:{err}"
    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return image_prompt, "skip:no_content"
    parsed = _parse_json_object(content)
    if not parsed:
        return image_prompt, "skip:parse_failed"

    fits = bool(parsed.get("fits", True))
    fixed = str(parsed.get("fixed_prompt") or "").strip()[:600]
    reason = str(parsed.get("reason") or "").strip()[:120]
    if fits:
        return image_prompt, "fit"
    # fits=false — берём исправленный промпт, но только если он осмысленный
    # (не пустой и не вырожденный). Иначе остаёмся на исходном.
    if len(fixed) >= 40:
        logger.info("image-prompt critic rewrote: %s", reason or "(no reason)")
        return fixed, f"fixed: {reason}"
    return image_prompt, f"skip:bad_fix ({reason})"


# === Image generation (AI photo) ===
def _preferred_size_for_model(model: str) -> list[str]:
    """Подбирает оптимальный размер под модель + fallback'и.

    Цель — получить исходное фото в квадратной ориентации (1:1),
    чтобы после cover-crop по центру не было апскейла.
    """
    m = (model or "").lower()
    if "dall-e-3" in m:
        # DALL-E 3: пробуем 1024×1024 (квадрат).
        return ["1024x1024"]
    if "flux" in m:
        # Flux Schnell: стабильнее всего 1024×1024 (квадрат).
        return ["1024x1024"]
    if "gemini" in m and "image" in m:
        # Gemini 2.5 Flash Image отдаёт фиксированный размер ~1024×1024.
        return ["1024x1024"]
    return ["1024x1024"]


def _generate_photo_bytes_sync(
    *, prompt: str, api_key: str, model: str = DEFAULT_IMAGE_MODEL, timeout: float = 60.0,
) -> bytes | None:
    """Генерит editorial-фото через OpenRouter. Возвращает PNG bytes или None."""
    if not api_key or not prompt:
        return None

    # Пробуем по очереди размеры от крупного к стандартному.
    last_err: str | None = None
    for size in _preferred_size_for_model(model):
        payload_a = {
            "model": model,
            "prompt": prompt,
            "n": 1,
            "size": size,
            "response_format": "b64_json",
        }
        ok, data, err = _http_post_json(OPENROUTER_IMAGES_GENERATIONS_URL, payload_a, api_key, timeout)
        if ok and isinstance(data, dict):
            img = _extract_image_from_openai_response(data)
            if img:
                logger.info("photo-gen ok model=%s size=%s bytes=%s", model, size, len(img))
                return img
        last_err = err

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
            logger.info("photo-gen ok model=%s via chat bytes=%s", model, len(img))
            return img

    logger.warning("photo-gen failed: a=%s b=%s", last_err, err2)
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
    # Финальная карточка теперь JPEG (Telegram всё равно перекодирует, а
    # на 2160×2700 PNG раздувается до нескольких МБ без выигрыша в качестве).
    out = generated_images_dir(data_dir) / f"{source_post_id}.jpg"
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

    # Шаг-критик: проверяем, что image_prompt реально считывается как эта
    # новость (главный промах — метафора «время» для денежной новости).
    # При промахе критик переписывает промпт; при любой ошибке остаёмся на
    # исходном — генерацию это не блокирует.
    image_prompt = slots.get("image_prompt") or ""
    image_prompt, critic_note = await asyncio.to_thread(
        _critique_image_prompt_sync,
        title=title, post_text=post_text, image_prompt=image_prompt,
        api_key=api_key, model=prompt_model,
    )
    slots["image_prompt"] = image_prompt
    slots["image_prompt_critic"] = critic_note

    # Генерация фото: пробуем основную + fallback'и
    photo_bytes: bytes | None = None
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

    if not photo_bytes:
        # Без AI-фото нет смысла продолжать — текстовые слои больше не рисуем.
        return None, json.dumps(slots, ensure_ascii=False), "photo_generation_failed"

    # Сохраняем исходное AI-фото (для дебага / повторных правок дизайна).
    photos_dir = _generated_photos_dir(data_dir)
    photo_path = photos_dir / f"{source_post_id}.png"
    await asyncio.to_thread(photo_path.write_bytes, photo_bytes)

    # Карточка = AI-фото обработанное под канвас 1080×1080 (квадрат Telegram).
    # Cover-crop по центру + лёгкий sharpen чтобы детали лучше читались.
    # Никаких текстовых слоёв — фото говорит само за себя.
    try:
        card_bytes = await asyncio.to_thread(_finalize_card_from_photo, photo_bytes)
    except Exception as exc:
        logger.exception("photo finalize crashed for post %s", source_post_id)
        return None, json.dumps(slots, ensure_ascii=False), f"finalize_crash: {type(exc).__name__}: {exc}"

    out_path = await asyncio.to_thread(save_generated_image, source_post_id, card_bytes, data_dir)
    logger.info(
        "photo-only card saved post=%s out_bytes=%s ai_bytes=%s",
        source_post_id, len(card_bytes), len(photo_bytes),
    )
    slots_log = json.dumps(slots, ensure_ascii=False)
    return out_path, slots_log, None


# Канвас 1080×1080 (квадрат, оптимально для Telegram preview + водяной знак).
CARD_W = 1080
CARD_H = 1080


def _finalize_card_from_photo(photo_bytes: bytes) -> bytes:
    """AI-фото → cover-crop по центру + LANCZOS + лёгкий sharpen → JPEG 1080×1080."""
    from io import BytesIO

    from PIL import Image, ImageFilter

    with Image.open(BytesIO(photo_bytes)) as raw:
        raw.load()
        src = raw.convert("RGB")

    src_ar = src.width / src.height
    target_ar = CARD_W / CARD_H  # всегда 1:1

    if src_ar > target_ar:
        new_h = src.height
        new_w = int(src.height * target_ar)
        left = (src.width - new_w) // 2
        cropped = src.crop((left, 0, left + new_w, new_h))
    else:
        new_w = src.width
        new_h = int(src.width / target_ar)
        top = (src.height - new_h) // 2  # центр, не смещённо
        cropped = src.crop((0, top, new_w, top + new_h))

    out = cropped.resize((CARD_W, CARD_H), Image.LANCZOS)
    out = out.filter(ImageFilter.UnsharpMask(radius=1.2, percent=70, threshold=3))

    buf = BytesIO()
    out.save(buf, format="JPEG", quality=92, optimize=True, progressive=True)
    return buf.getvalue()
