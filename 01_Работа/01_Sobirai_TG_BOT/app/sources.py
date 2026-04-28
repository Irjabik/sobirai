from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Source:
    platform: str
    username: str
    category: str

    @property
    def source_key(self) -> str:
        return self.username.strip().lower()


SOURCES: tuple[Source, ...] = (
    Source("tg", "@opendatascience", "Новости"),
    Source("tg", "@neuro_channel", "Новости"),
    Source("tg", "@mlunderhood", "Новости"),
    Source("tg", "@ai_newz", "Новости"),
    Source("tg", "@aioftheday", "Новости"),
    Source("tg", "@aimetropolis", "Новости"),
    Source("tg", "@ai_machinelearning_big_data", "Новости"),
    Source("tg", "@data_secrets", "Новости"),
    Source("tg", "@seeallochnaya", "Новости"),
    Source("tg", "@deeplearning_ru", "Технические"),
    Source("tg", "@llm_under_hood", "Технические"),
    Source("tg", "@ii_papka", "Технические"),
    Source("tg", "@scientific_opensource", "Технические"),
    Source("tg", "@sergeinotevskii", "Технические"),
    Source("tg", "@countwithsasha", "Технические"),
    Source("tg", "@GreenNeuralRobots", "Технические"),
    Source("tg", "@opensource_hub", "Технические"),
    Source("tg", "@denpleada", "Авторские"),
    Source("tg", "@misha_davai_po_novoi", "Авторские"),
    Source("tg", "@rogo3inai", "Авторские"),
    Source("tg", "@tg_1red2black", "Авторские"),
    Source("tg", "@nerdienatella", "Авторские"),
    Source("tg", "@zamesin", "Авторские"),
    Source("tg", "@mishasamin", "Авторские"),
    Source("tg", "@complete_ai", "Авторские"),
    Source("tg", "@strangedalle", "Авторские"),
    Source("tg", "@ai_product", "Авторские"),
    Source("tg", "@sergiobulaev", "Авторские"),
    Source("tg", "@bossofyourboss", "Авторские"),
    Source("tg", "@Sprut_AI", "Авторские"),
    Source("tg", "@NeuralShit", "Креативные"),
    Source("tg", "@life2film", "Креативные"),
    Source("tg", "@GPTinvest", "Креативные"),
    Source("tg", "@cryptoEssay", "Креативные"),
    Source("tg", "@vibecoding_tg", "Креативные"),
    Source("x", "@TheRundownAI", "Новости"),
    Source("x", "@OpenAI", "Новости"),
    Source("x", "@AnthropicAI", "Новости"),
    Source("x", "@karpathy", "Технические"),
    Source("x", "@rasbt", "Технические"),
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
        groups.setdefault(source.category, []).append(f"{source.username} ({source.platform})")
    return groups


def grouped_sources_by_platform() -> dict[str, dict[str, list[str]]]:
    grouped: dict[str, dict[str, list[str]]] = {"tg": {}, "x": {}}
    for source in SOURCES:
        grouped.setdefault(source.platform, {}).setdefault(source.category, []).append(source.username)
    return grouped


def category_by_username() -> dict[str, str]:
    return {source.username.lower(): source.category for source in SOURCES}


def all_source_usernames() -> set[str]:
    return {source.username.lower() for source in SOURCES}
