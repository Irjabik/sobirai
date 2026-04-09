from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Source:
    username: str
    category: str


SOURCES: tuple[Source, ...] = (
    Source("@opendatascience", "Новости"),
    Source("@neuro_channel", "Новости"),
    Source("@mlunderhood", "Новости"),
    Source("@ai_newz", "Новости"),
    Source("@aioftheday", "Новости"),
    Source("@aimetropolis", "Новости"),
    Source("@ai_machinelearning_big_data", "Новости"),
    Source("@data_secrets", "Новости"),
    Source("@seeallochnaya", "Новости"),
    Source("@deeplearning_ru", "Технические"),
    Source("@llm_under_hood", "Технические"),
    Source("@ii_papka", "Технические"),
    Source("@scientific_opensource", "Технические"),
    Source("@sergeinotevskii", "Технические"),
    Source("@countwithsasha", "Технические"),
    Source("@denpleada", "Авторские"),
    Source("@misha_davai_po_novoi", "Авторские"),
    Source("@rogo3inai", "Авторские"),
    Source("@tg_1red2black", "Авторские"),
    Source("@nerdienatella", "Авторские"),
    Source("@zamesin", "Авторские"),
    Source("@mishasamin", "Авторские"),
    Source("@complete_ai", "Авторские"),
    Source("@NeuralShit", "Креативные"),
    Source("@life2film", "Креативные"),
    Source("@GPTinvest", "Креативные"),
    Source("@cryptoEssay", "Креативные"),
    Source("@neurozeh", "Креативные"),
    Source("@vibecoding_tg", "Креативные"),
)

CATEGORY_KEYS: dict[str, str] = {
    "новости": "news",
    "технические": "tech",
    "авторские": "author",
    "креативные": "creative",
}

KEY_TO_CATEGORY: dict[str, str] = {value: key for key, value in CATEGORY_KEYS.items()}


def grouped_sources() -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for source in SOURCES:
        groups.setdefault(source.category, []).append(source.username)
    return groups


def category_by_username() -> dict[str, str]:
    return {source.username.lower(): source.category for source in SOURCES}


def all_source_usernames() -> set[str]:
    return {source.username.lower() for source in SOURCES}
