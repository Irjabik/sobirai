from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import Settings
from .llm_gemini import call_gemini_chat_json
from .llm_groq import call_groq_chat_json


@dataclass(frozen=True)
class RoutedLlmResult:
    ok: bool
    parsed: dict[str, Any] | None
    error_code: str | None
    attempts: int
    provider_used: str
    model_used: str


def _call_provider(
    provider: str,
    settings: Settings,
    *,
    system_prompt: str,
    user_message: str,
) -> RoutedLlmResult:
    if provider == "gemini":
        ok, parsed, err, attempts = call_gemini_chat_json(
            api_key=settings.gemini_api_key,
            model=settings.gemini_model,
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
            provider_used="gemini",
            model_used=settings.gemini_model,
        )
    if provider == "groq":
        res = call_groq_chat_json(
            api_key=settings.groq_api_key,
            model=settings.llm_model,
            system_prompt=system_prompt,
            user_message=user_message,
            max_output_tokens=settings.llm_max_output_tokens,
            timeout_seconds=settings.llm_timeout_seconds,
            max_retries=settings.llm_max_retries,
        )
        return RoutedLlmResult(
            ok=res.ok,
            parsed=res.parsed,
            error_code=res.error_code,
            attempts=res.attempts,
            provider_used="groq",
            model_used=settings.llm_model,
        )
    return RoutedLlmResult(
        ok=False,
        parsed=None,
        error_code=f"unsupported_provider:{provider}",
        attempts=1,
        provider_used=provider,
        model_used="",
    )


def call_llm_with_fallback(
    settings: Settings,
    *,
    system_prompt: str,
    user_message: str,
) -> RoutedLlmResult:
    primary = settings.llm_primary_provider
    first = _call_provider(primary, settings, system_prompt=system_prompt, user_message=user_message)
    if first.ok:
        return first
    if not settings.llm_fallback_enabled:
        return first
    fallback = settings.llm_fallback_provider
    if fallback == primary:
        return first
    second = _call_provider(fallback, settings, system_prompt=system_prompt, user_message=user_message)
    if second.ok:
        return second
    return RoutedLlmResult(
        ok=False,
        parsed=None,
        error_code=f"primary={first.error_code};fallback={second.error_code}",
        attempts=first.attempts + second.attempts,
        provider_used=f"{first.provider_used}->{second.provider_used}",
        model_used=second.model_used or first.model_used,
    )

