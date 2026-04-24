from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.text_norm import has_new_details_vs_reference, near_duplicate_score, significant_tokens


def _token_jaccard(a: str, b: str) -> float:
    sa = significant_tokens(a, min_len=4)
    sb = significant_tokens(b, min_len=4)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / max(1, len(sa | sb))


def _is_duplicate(candidate: str, published: list[str], threshold: float = 0.38) -> bool:
    for ref in published:
        score = near_duplicate_score(candidate, ref, k=3)
        lexical = _token_jaccard(candidate, ref)
        if (score >= threshold or lexical >= 0.14) and not has_new_details_vs_reference(candidate, ref):
            return True
    return False


def main() -> int:
    fixtures = [
        "Anthropic выпустила Claude 4.1 для API. Улучшили работу с кодом и длинным контекстом.",
        "Anthropic выпустила Claude 4.1 в API: улучшена работа с кодом, длинный контекст стал стабильнее.",
        "Claude 4.1 от Anthropic уже в API. Фокус релиза - качество кода и длинный контекст.",
        (
            "Anthropic выпустила Claude 4.1 для API. Кроме этого, компания добавила batch inference, "
            "новые лимиты скорости и расширила окно контекста до 1M токенов."
        ),
    ]

    published: list[str] = []
    published_count = 0
    duplicate_count = 0
    for item in fixtures:
        if _is_duplicate(item, published):
            duplicate_count += 1
            continue
        published.append(item)
        published_count += 1

    if published_count < 1 or published_count > 2:
        print(
            f"CRIT: bad publish count for same-event fixture, published={published_count}, duplicates={duplicate_count}"
        )
        return 2
    print(f"OK: regression fixture passed, published={published_count}, duplicates={duplicate_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
