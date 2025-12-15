from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from app.dsl import Aggregation, Metric
from app.nlp import _extract_range, _month_num, _parse_date, parse_to_dsl


def test_month_num() -> None:
    assert _month_num("ноября") == 11
    assert _month_num("мая") == 5


def test_parse_date_russian() -> None:
    assert _parse_date("5 ноября 2025") == date(2025, 11, 5)


def test_extract_range_day_only_left() -> None:
    left, right = _extract_range("с 1 по 5 ноября 2025 включительно")
    assert left == date(2025, 11, 1)
    assert right == date(2025, 11, 5)


@pytest.mark.asyncio
async def test_parse_sum_final_likes() -> None:
    dsl = await parse_to_dsl("Сколько лайков за всё время?")
    assert dsl.aggregation == Aggregation.sum_final
    assert dsl.metric == Metric.likes


@pytest.mark.asyncio
async def test_parse_default_fallback() -> None:
    dsl = await parse_to_dsl("привет")
    assert dsl.aggregation == Aggregation.count_videos


@pytest.mark.asyncio
async def test_parse_llm_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENROUTER_API_KEY", "x")
    monkeypatch.setenv("OPENROUTER_MODEL", "x")

    async def fake_chat(system: str, user: str) -> str:
        return '{"aggregation":"count_videos","metric":null,"creator_id":null,"published_from":"2025-11-01T00:00:00+00:00","published_to":"2025-11-06T00:00:00+00:00","day":null,"threshold":null}'

    monkeypatch.setattr("app.nlp.chat_completion", fake_chat)

    dsl = await parse_to_dsl("любой текст")
    assert dsl.aggregation == Aggregation.count_videos
    assert dsl.published_from == datetime(2025, 11, 1, tzinfo=timezone.utc)
