from __future__ import annotations

import logging
from typing import Any, cast

import httpx

from app.settings import get_settings

logger = logging.getLogger(__name__)


async def chat_completion(system: str, user: str) -> str:
    """Принимает system и user строки; возвращает строковый ответ LLM (контент первого choice)."""
    settings = get_settings()
    if not settings.openrouter_api_key or not settings.openrouter_model:
        raise RuntimeError("OpenRouter is not configured")

    api_key = settings.openrouter_api_key
    model_main = settings.openrouter_model
    assert api_key is not None
    assert model_main is not None

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    models = [model_main]
    if settings.openrouter_fallback_model and settings.openrouter_fallback_model.strip():
        models.append(settings.openrouter_fallback_model.strip())

    timeout = httpx.Timeout(settings.openrouter_timeout_seconds)
    async with httpx.AsyncClient(base_url=settings.openrouter_base_url, timeout=timeout) as client:
        last_exc: Exception | None = None
        data: dict[str, Any] | None = None
        for model in models:
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "temperature": 0,
            }
            try:
                logger.info("openrouter_request", extra={"model": model})
                resp = await client.post("/chat/completions", headers=headers, json=payload)
                resp.raise_for_status()
                data = cast(dict[str, Any], resp.json())
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                logger.warning("openrouter_failed", extra={"model": model, "error": str(exc)})
        if last_exc is not None:
            raise last_exc

        if data is None:
            raise RuntimeError("No response")

    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("No choices")

    first = choices[0]
    if not isinstance(first, dict):
        raise RuntimeError("Bad choice")
    message = first.get("message")
    if not isinstance(message, dict):
        raise RuntimeError("Bad message")
    msg = message.get("content")
    if not isinstance(msg, str) or not msg.strip():
        raise RuntimeError("Empty response")

    return msg
