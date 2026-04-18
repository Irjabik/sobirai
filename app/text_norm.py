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


def has_new_details_vs_reference(candidate: str, reference: str) -> bool:
    """
    Эвристика MVP: есть ли новые детали относительно похожего текста.
    Не фактчек, только чтобы отличить «та же заметка» от «добавили цифры/имена».
    """
    c = candidate or ""
    r = reference or ""
    if len(normalize_for_fingerprint(c)) - len(normalize_for_fingerprint(r)) >= 80:
        return True
    num_new = extract_numbers(c) - extract_numbers(r)
    if num_new:
        return True
    tok_new = significant_tokens(c) - significant_tokens(r)
    if len(tok_new) >= 3:
        return True
    return False


def near_duplicate_score(candidate: str, reference: str, k: int = 5) -> float:
    return jaccard(word_shingles(candidate, k), word_shingles(reference, k))
