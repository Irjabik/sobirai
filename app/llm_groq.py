from __future__ import annotations

import json
import logging
import re
import socket
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

GROQ_CHAT_COMPLETIONS_URL = "https://api.groq.com/openai/v1/chat/completions"
# Cloudflare у Groq режет urllib с дефолтным User-Agent Python (HTTP 403, error code 1010).
# Нужен явный клиентский UA; см. https://community.groq.com/t/cloudflare-blocking-urllib-request-without-user-agent/860
_DEFAULT_GROQ_USER_AGENT = (
    "Mozilla/5.0 (compatible; SobiraiBot/1.0; +https://github.com/Irjabik/sobirai) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
class GroqLlmResult:
    ok: bool
    content: str | None
    parsed: dict[str, Any] | None
    error_code: str | None
    attempts: int


def _strip_code_fence(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", t)
        t = re.sub(r"\s*```$", "", t)
    return t.strip()


def _parse_json_object(text: str) -> dict[str, Any] | None:
    raw = _strip_code_fence(text)
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}", raw)
        if not m:
            return None
        try:
            obj = json.loads(m.group(0))
        except json.JSONDecodeError:
            return None
    return obj if isinstance(obj, dict) else None


def call_groq_chat_json(
    *,
    api_key: str,
    model: str,
    system_prompt: str,
    user_message: str,
    max_output_tokens: int,
    timeout_seconds: float,
    max_retries: int,
) -> GroqLlmResult:
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "max_tokens": int(max_output_tokens),
        "temperature": 0.35,
        "response_format": {"type": "json_object"},
    }
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "User-Agent": _DEFAULT_GROQ_USER_AGENT,
        "Accept": "application/json",
    }

    attempts = 0
    last_err: str | None = None
    backoff = 1.0

    while attempts <= max(0, int(max_retries)):
        attempts += 1
        req = urllib.request.Request(
            GROQ_CHAT_COMPLETIONS_URL,
            data=body,
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                resp_body = resp.read().decode("utf-8", errors="replace")
            outer = json.loads(resp_body)
            choices = outer.get("choices") or []
            if not choices:
                last_err = "groq_empty_choices"
                logger.warning("Groq: пустой choices, body=%s", resp_body[:500])
            else:
                msg = (choices[0].get("message") or {}) if isinstance(choices[0], dict) else {}
                content = msg.get("content")
                if not isinstance(content, str) or not content.strip():
                    last_err = "groq_empty_content"
                else:
                    parsed = _parse_json_object(content)
                    if parsed is None:
                        return GroqLlmResult(
                            ok=False,
                            content=content,
                            parsed=None,
                            error_code="groq_json_parse_failed",
                            attempts=attempts,
                        )
                    return GroqLlmResult(
                        ok=True,
                        content=content,
                        parsed=parsed,
                        error_code=None,
                        attempts=attempts,
                    )
        except urllib.error.HTTPError as exc:
            body_err = exc.read().decode("utf-8", errors="replace")[:800]
            if exc.code == 429:
                last_err = "groq_rate_limited"
            elif exc.code >= 500:
                last_err = "groq_server_error"
            else:
                last_err = f"groq_http_{exc.code}"
            logger.warning(
                "Groq HTTPError attempt=%s code=%s err=%s body=%s",
                attempts,
                exc.code,
                last_err,
                body_err,
            )
        except urllib.error.URLError as exc:
            last_err = "groq_url_error"
            logger.warning("Groq URLError attempt=%s: %s", attempts, exc)
        except (TimeoutError, socket.timeout) as exc:
            last_err = "groq_timeout"
            logger.warning("Groq timeout attempt=%s: %s", attempts, exc)
        except Exception:
            last_err = "groq_unknown"
            logger.exception("Groq unexpected error attempt=%s", attempts)

        if attempts > max_retries:
            break
        time.sleep(min(8.0, backoff))
        backoff *= 2.0

    return GroqLlmResult(
        ok=False,
        content=None,
        parsed=None,
        error_code=last_err or "groq_failed",
        attempts=attempts,
    )
