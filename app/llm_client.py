from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import Settings
from .llm_openrouter import call_openrouter_chat_json


@dataclass(frozen=True)
class RoutedLlmResult:
    ok: bool
    parsed: dict[str, Any] | None
    error_code: str | None
    attempts: int
    provider_used: str
    model_used: str


def call_llm_with_fallback(
    settings: Settings,
    *,
    system_prompt: str,
    user_message: str,
) -> RoutedLlmResult:
    """LLM-вызов через OpenRouter — единственный провайдер.

    OpenRouter сам внутри маршрутизирует запросы между подпровайдерами (DeepSeek, Together,
    Fireworks, ...) — нам отдельный fallback не нужен.
    Имя функции сохранено для совместимости с существующими call sites.
    """
    ok, parsed, err, attempts = call_openrouter_chat_json(
        api_key=settings.openrouter_api_key,
        model=settings.openrouter_model,
        system_prompt=system_prompt,
        user_message=user_message,
        max_output_tokens=settings.llm_max_output_tokens,
        timeout_seconds=settings.llm_timeout_seconds,
        max_retries=settings.llm_max_retries,
    )
    return RoutedLlmResult(
        ok=ok,
        parsed=parsed,
        error_code=err,
        attempts=attempts,
        provider_used="openrouter",
        model_used=settings.openrouter_model,
    )
