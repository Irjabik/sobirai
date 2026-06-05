from __future__ import annotations

import hashlib
import re


def normalize_for_fingerprint(text: str) -> str:
    """Нормализация для exact-hash: нижний регистр, схлопывание пробелов, без URL-шума."""
    t = (text or "").lower()
    t = re.sub(r"https?://\S+", " ", t)
    t = re.sub(r"[^\w\sа-яё]+", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def fingerprint_text(text: str) -> str:
    n = normalize_for_fingerprint(text)
    return hashlib.sha256(n.encode("utf-8")).hexdigest()


def word_shingles(text: str, k: int = 5) -> set[str]:
    """Шинглы по словам (дешевый near-dup)."""
    words = normalize_for_fingerprint(text).split()
    if not words:
        return set()
    if len(words) < k:
        return {" ".join(words)}
    return {" ".join(words[i : i + k]) for i in range(len(words) - k + 1)}


def jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def extract_numbers(text: str) -> set[str]:
    return set(re.findall(r"\d[\d\s,.]*\d|\d+", text or ""))


def significant_tokens(text: str, min_len: int = 4) -> set[str]:
    pat = rf"[0-9A-Za-zА-Яа-яЁё]{{{min_len},}}"
    return {m.lower() for m in re.findall(pat, text or "")}


def new_details_signal(candidate: str, reference: str) -> tuple[bool, str]:
    c = candidate or ""
    r = reference or ""
    c_norm = normalize_for_fingerprint(c)
    r_norm = normalize_for_fingerprint(r)

    if not c_norm:
        return False, "empty_candidate"
    if not r_norm:
        return True, "empty_reference"

    len_diff = len(c_norm) - len(r_norm)
    num_new = extract_numbers(c) - extract_numbers(r)
    tok_new = significant_tokens(c) - significant_tokens(r)

    if len_diff >= 400:
        return True, "large_length_delta"
    if len_diff >= 250 and len(num_new) >= 4:
        return True, "length_plus_numbers"
    if len_diff >= 200 and len(tok_new) >= 8:
        return True, "length_plus_tokens"
    if len(num_new) >= 4 and len(tok_new) >= 6:
        return True, "numbers_plus_tokens"
    if len(num_new) >= 5:
        return True, "many_new_numbers"
    if len(tok_new) >= 12:
        return True, "many_new_tokens"
    return False, "weak_delta"


def near_duplicate_score(candidate: str, reference: str, k: int = 5) -> float:
    return jaccard(word_shingles(candidate, k), word_shingles(reference, k))


# Канонические AI-сущности: продукты, модели, компании. Каждая запись — пара
# (метка, regex). Если в тексте нашёлся хоть один pattern из группы — метка
# попадает в сет. Сравнение сетов между двумя постами даёт сильный сигнал
# "одна и та же новость" даже когда лексический Jaccard невысокий.
_AI_ENTITY_PATTERNS: tuple[tuple[str, str], ...] = (
    ("openai", r"\bopen\s*ai\b|\bопенай\b|\bопенэйай\b"),
    ("anthropic", r"\banthropic\b|\bантропик\b"),
    ("google", r"\bgoogle\b|\bгугл\w*\b|\balphabet\b"),
    ("deepmind", r"\bdeep\s*mind\b|\bдипмайнд\b"),
    ("meta", r"\bmeta\s*ai\b|\bmeta\s+platforms\b|\bмета\s+ai\b"),
    ("microsoft", r"\bmicrosoft\b|\bмайкрософт\b|\bмс\s+(?:ai|copilot)\b"),
    ("nvidia", r"\bnvidia\b|\bнвидиа\b|\bнвидия\b"),
    ("apple", r"\bapple\s+(?:intelligence|ai)\b|\bэппл\s+(?:ai|интелл\w+)\b"),
    ("amazon", r"\bamazon\s+(?:ai|bedrock|q\b)\b|\bbedrock\b"),
    ("xai", r"\bx\.ai\b|\bxai\b"),
    ("perplexity", r"\bperplexity\b|\bперплекс\w+\b"),
    ("cohere", r"\bcohere\b"),
    ("mistral_co", r"\bmistral\s+ai\b|\bмистраль\s+ai\b"),
    ("deepseek_co", r"\bdeepseek\b|\bдипсик\b"),
    ("alibaba", r"\balibaba\b|\bqwen\s+team\b"),
    ("baidu", r"\bbaidu\b|\bernie\s+bot\b"),
    ("hf", r"\bhugging\s*face\b|\bhf\s+hub\b"),
    ("stability", r"\bstability\s+ai\b"),
    ("runway_co", r"\brunway(?:ml)?\b"),
    ("midjourney", r"\bmidjourney\b|\bмидджорни\b|\bмидджорни\b"),
    ("ollama", r"\bollama\b"),
    # Модели/продукты
    ("gpt", r"\bgpt[-\s]?\d+(?:[.\-]?\d+)?(?:[-\s]?(?:turbo|mini|pro|vision|o))?\b|\bgpt\b|\bchat\s*gpt\b|\bчатгпт\b|\bчат\s*gpt\b"),
    ("claude", r"\bclaude(?:[-\s]?\d+(?:\.\d+)?)?(?:[-\s]?(?:opus|sonnet|haiku))?\b|\bклод\b"),
    ("gemini", r"\bgemini(?:[-\s]?\d+(?:\.\d+)?)?(?:[-\s]?(?:pro|ultra|nano|flash))?\b|\bгемини\b"),
    ("bard", r"\bbard\b"),
    ("llama", r"\bllama[-\s]?\d+(?:\.\d+)?\b|\bllama\b|\bлама\s+\d\b"),
    ("mistral_model", r"\bmistral[-\s]?(?:7b|8x7b|large|small|nemo)\b|\bmixtral\b"),
    ("qwen", r"\bqwen[-\s]?\d*(?:\.\d+)?\b"),
    ("deepseek_model", r"\bdeepseek[-\s]?(?:v\d|coder|r\d|moe)\b"),
    ("grok", r"\bgrok[-\s]?\d*\b"),
    ("copilot", r"\bcopilot\b|\bкопилот\b"),
    ("sora", r"\bsora\b|\bсора\b"),
    ("dalle", r"\bdall[-\s]?e[-\s]?\d?\b"),
    ("stable_diffusion", r"\bstable\s+diffusion\b|\bsdxl\b|\bsd\s*\d\b"),
    ("flux", r"\bflux(?:\.\d+)?(?:[-\s]?(?:dev|pro|schnell))?\b"),
    ("kling", r"\bkling(?:\s*ai)?\b"),
    ("veo", r"\bveo[-\s]?\d?\b"),
    ("lumiere", r"\blumiere\b"),
    ("runway_model", r"\bgen[-\s]?\d\b|\brunway\s+gen\b"),
    ("suno", r"\bsuno(?:\s*ai)?\b"),
    ("elevenlabs", r"\beleven\s*labs\b"),
    ("pika", r"\bpika(?:\s*labs)?\b"),
    ("ideogram", r"\bideogram\b"),
    ("notebooklm", r"\bnotebook\s*lm\b"),
    ("mcp", r"\bmcp\b|\bmodel\s+context\s+protocol\b"),
    # Generic AI-маркеры — ловят посты без брендов (опенсорс-инструменты, локальные ассистенты).
    # Каждый pattern должен быть достаточно узким, чтобы не давать ложноположительных
    # на не-AI текстах (например, "rag" в значении "тряпка" — поэтому только в составных формах).
    ("llm_generic", r"\bllms?\b|\blarge\s+language\s+model\w*\b|\bбольш\w+\s+язык\w+\s+модел\w+\b"),
    ("ai_assistant", r"\b(?:ai|ии)[-\s]?ассистент\w*\b|\bai\s+assistant\b|\b(?:ai|ии)[-\s]?агент\w*\b|\bagentic\b|\bagentic\s+ai\b"),
    ("lm_studio", r"\blm\s*studio\b"),
    ("rag_pipeline", r"\brag[-\s]?(?:систем\w*|pipeline|агент\w*|стек\w*|разработ\w*)\b|\bretrieval[-\s]?augment\w+\b"),
    ("neural_net", r"\bнейросет\w*\b|\bneural\s+network\w*\b"),
    ("embedding", r"\bembedding\w*\b|\bэмбеддинг\w*\b|\bembedding\s+model\w*\b"),
    ("vector_db", r"\bvector\s+(?:db|store|database|search|index\w*)\b|\bвектор\w*\s+(?:бд|поиск|индекс\w*)\b|\bpinecone\b|\bweaviate\b|\bchroma\s*db\b|\bmilvus\b|\bqdrant\b"),
    ("finetune", r"\bfine[-\s]?tun\w+\b|\bфайн[-\s]?тюн\w*\b|\bдообуч\w+\s+(?:модел\w+|нейросет\w+)\b|\blora\s+(?:adapter\w*|train\w*|tuning|тюн\w*)\b"),
    ("inference_engine", r"\binference\s+(?:engine|server|api|stack|runtime)\b|\bинференс\w*\s+(?:сервер\w*|движок|api)\b|\bvllm\b|\bllama\.cpp\b|\bllama-cpp\b|\bonnx\s+runtime\b"),
    ("genai_term", r"\bgen(?:erative)?\s*ai\b|\bgenai\b|\bгенеративн\w+\s+(?:ии|ai|нейросет\w+|модел\w+)\b"),
    ("opensource_ai", r"\bopen[-\s]?weights?\b|\bопенсорс\w*\s+(?:модел\w+|llm|нейросет\w+|ии|ai)\b|\bopen[-\s]?source\s+(?:llm|ai|model\w*|assistant)\b"),
    ("transformer_arch", r"\btransformer\s+(?:model\w*|architecture|архитектур\w*)\b|\bтрансформер\w*\s+(?:модел\w*|архитектур\w*)\b|\bmoe\b|\bmixture\s+of\s+experts\b"),
    ("ai_coding_tool", r"\bcursor[-\s]?(?:ide|editor)?\b|\bwindsurf\b|\baider\b|\bcontinue\.dev\b|\bzed\s+(?:ai|editor)\b|\bcline\b|\bsweep\s+ai\b|\btabnine\b|\bcodeium\b"),
)

_AI_ENTITY_COMPILED = tuple(
    (label, re.compile(pattern, flags=re.IGNORECASE)) for label, pattern in _AI_ENTITY_PATTERNS
)


def extract_ai_entities(text: str) -> set[str]:
    """Возвращает набор канонических AI-сущностей, упомянутых в тексте."""
    if not text:
        return set()
    found: set[str] = set()
    for label, regex in _AI_ENTITY_COMPILED:
        if regex.search(text):
            found.add(label)
    return found
