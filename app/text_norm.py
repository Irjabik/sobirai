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
    return new_details_signal(candidate, reference)[0]


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

    if len_diff >= 220:
        return True, "large_length_delta"
    if len_diff >= 130 and len(num_new) >= 2:
        return True, "length_plus_numbers"
    if len_diff >= 100 and len(tok_new) >= 5:
        return True, "length_plus_tokens"
    if len(num_new) >= 2 and len(tok_new) >= 3:
        return True, "numbers_plus_tokens"
    if len(num_new) >= 3:
        return True, "many_new_numbers"
    if len(tok_new) >= 8:
        return True, "many_new_tokens"
    return False, "weak_delta"


def near_duplicate_score(candidate: str, reference: str, k: int = 5) -> float:
    return jaccard(word_shingles(candidate, k), word_shingles(reference, k))
