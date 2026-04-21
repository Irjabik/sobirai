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


def _parse_wait_seconds(http_error: urllib.error.HTTPError, body_snippet: str) -> float | None:
    hdrs = getattr(http_error, "headers", None)
    if hdrs is not None:
        ra = hdrs.get("Retry-After") or hdrs.get("retry-after")
        if ra:
            try:
                return min(120.0, max(0.5, float(str(ra).strip())))
            except ValueError:
                pass
    m = re.search(r"(?:try again|retry).{0,40}?([0-9]+(?:\.[0-9]+)?)\s*s", body_snippet, flags=re.IGNORECASE)
    if m:
        return min(120.0, max(0.5, float(m.group(1)) + 0.35))
    return None


def call_sambanova_chat_json(
    *,
    api_key: str,
    model: str,
    api_base: str,
    system_prompt: str,
    user_message: str,
    max_output_tokens: int,
    timeout_seconds: float,
    max_retries: int,
) -> tuple[bool, dict[str, Any] | None, str | None, int]:
    """
    Returns (ok, parsed_json, error_code, attempts).
    """
    base = api_base.rstrip("/")
    url = f"{base}/chat/completions"
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
            choices = outer.get("choices") or []
            if not choices:
                last_err = "sambanova_empty_choices"
            else:
                msg = (choices[0].get("message") or {}) if isinstance(choices[0], dict) else {}
                content = msg.get("content")
                if not isinstance(content, str) or not content.strip():
                    last_err = "sambanova_empty_content"
                else:
                    parsed = _parse_json_object(content)
                    if parsed is None:
                        return (False, None, "sambanova_json_parse_failed", attempts)
                    return (True, parsed, None, attempts)
        except urllib.error.HTTPError as exc:
            body_err = exc.read().decode("utf-8", errors="replace")[:1200]
            if exc.code == 429:
                last_err = "sambanova_rate_limited"
                sleep_after_error = _parse_wait_seconds(exc, body_err) or 10.0
            elif exc.code >= 500:
                last_err = "sambanova_server_error"
            else:
                last_err = f"sambanova_http_{exc.code}"
            logger.warning(
                "SambaNova HTTPError attempt=%s code=%s err=%s body=%s",
                attempts,
                exc.code,
                last_err,
                body_err,
            )
        except urllib.error.URLError as exc:
            last_err = "sambanova_url_error"
            logger.warning("SambaNova URLError attempt=%s: %s", attempts, exc)
        except (TimeoutError, socket.timeout) as exc:
            last_err = "sambanova_timeout"
            logger.warning("SambaNova timeout attempt=%s: %s", attempts, exc)
        except Exception:
            last_err = "sambanova_unknown"
            logger.exception("SambaNova unexpected error attempt=%s", attempts)

        if attempts > max_retries:
            break
        if sleep_after_error is not None:
            time.sleep(sleep_after_error)
        else:
            time.sleep(min(8.0, backoff))
            backoff *= 2.0
    return (False, None, last_err or "sambanova_failed", attempts)

