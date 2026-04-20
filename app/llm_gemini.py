from __future__ import annotations

import json
import logging
import re
import socket
import time
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; SobiraiBot/1.0; +https://github.com/Irjabik/sobirai) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


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


def _parse_wait_seconds(headers: Any, body: str) -> float | None:
    if headers is not None:
        ra = headers.get("Retry-After") or headers.get("retry-after")
        if ra:
            try:
                return min(120.0, max(0.5, float(str(ra).strip())))
            except ValueError:
                pass
    m = re.search(r"(?:retry|try again).*?([0-9]+(?:\.[0-9]+)?)\s*s", body, flags=re.IGNORECASE)
    if m:
        return min(120.0, max(0.5, float(m.group(1)) + 0.35))
    return None


def call_gemini_chat_json(
    *,
    api_key: str,
    model: str,
    system_prompt: str,
    user_message: str,
    max_output_tokens: int,
    timeout_seconds: float,
    max_retries: int,
) -> tuple[bool, dict[str, Any] | None, str | None, int]:
    """
    Returns (ok, parsed_json, error_code, attempts).
    """
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        f"?key={api_key}"
    )
    payload = {
        "system_instruction": {"parts": [{"text": system_prompt}]},
        "contents": [{"role": "user", "parts": [{"text": user_message}]}],
        "generationConfig": {
            "temperature": 0.35,
            "maxOutputTokens": int(max_output_tokens),
            "responseMimeType": "application/json",
        },
    }
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": _DEFAULT_USER_AGENT,
        "Accept": "application/json",
    }

    attempts = 0
    last_err: str | None = None
    backoff = 1.0
    while attempts <= max(0, int(max_retries)):
        attempts += 1
        sleep_after_error: float | None = None
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                resp_body = resp.read().decode("utf-8", errors="replace")
            outer = json.loads(resp_body)
            candidates = outer.get("candidates") or []
            if not candidates:
                last_err = "gemini_empty_candidates"
            else:
                content = (candidates[0].get("content") or {}) if isinstance(candidates[0], dict) else {}
                parts = content.get("parts") or []
                text = ""
                if isinstance(parts, list) and parts:
                    text = str((parts[0] or {}).get("text") or "")
                parsed = _parse_json_object(text)
                if parsed is None:
                    return (False, None, "gemini_json_parse_failed", attempts)
                return (True, parsed, None, attempts)
        except urllib.error.HTTPError as exc:
            body_err = exc.read().decode("utf-8", errors="replace")[:1200]
            if exc.code == 429:
                last_err = "gemini_rate_limited"
                sleep_after_error = _parse_wait_seconds(getattr(exc, "headers", None), body_err) or 10.0
            elif exc.code >= 500:
                last_err = "gemini_server_error"
            else:
                last_err = f"gemini_http_{exc.code}"
            logger.warning(
                "Gemini HTTPError attempt=%s code=%s err=%s body=%s",
                attempts,
                exc.code,
                last_err,
                body_err,
            )
        except urllib.error.URLError as exc:
            last_err = "gemini_url_error"
            logger.warning("Gemini URLError attempt=%s: %s", attempts, exc)
        except (TimeoutError, socket.timeout) as exc:
            last_err = "gemini_timeout"
            logger.warning("Gemini timeout attempt=%s: %s", attempts, exc)
        except Exception:
            last_err = "gemini_unknown"
            logger.exception("Gemini unexpected error attempt=%s", attempts)

        if attempts > max_retries:
            break
        if sleep_after_error is not None:
            time.sleep(sleep_after_error)
        else:
            time.sleep(min(8.0, backoff))
            backoff *= 2.0
    return (False, None, last_err or "gemini_failed", attempts)

