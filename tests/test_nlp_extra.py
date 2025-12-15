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
async def test_parse_negative_delta_snapshots_views() -> None:
    dsl = await parse_to_dsl(
        "Сколько всего есть замеров статистики (по всем видео), в которых число просмотров за час оказалось отрицательным"
    )
    assert dsl.aggregation == Aggregation.count_snapshots_with_delta_lt0
    assert dsl.metric == Metric.views


@pytest.mark.asyncio
async def test_parse_negative_delta_snapshots_creator_id() -> None:
    q = (
        "Сколько всего есть замеров статистики по видео креатора с id abcdefabcdefabcdefabcdefabcdefab, "
        "в которых число жалоб за час оказалось отрицательным"
    )
    dsl = await parse_to_dsl(q)
    assert dsl.aggregation == Aggregation.count_snapshots_with_delta_lt0
    assert dsl.metric == Metric.reports
    assert dsl.creator_id == "abcdefabcdefabcdefabcdefabcdefab"


@pytest.mark.asyncio
async def test_parse_sum_delta_creator_time_window() -> None:
    q = (
        "На сколько просмотров суммарно выросли все видео креатора с id cd87be38b50b4fdd8342bb3c383f3c7d "
        "в промежутке с 10:00 до 15:00 28 ноября 2025 года?"
    )
    dsl = await parse_to_dsl(q)
    assert dsl.aggregation == Aggregation.sum_delta
    assert dsl.metric == Metric.views
    assert dsl.creator_id == "cd87be38b50b4fdd8342bb3c383f3c7d"
    assert dsl.day == date(2025, 11, 28)
    assert dsl.snapshot_from == datetime(2025, 11, 28, 10, 0, tzinfo=timezone.utc)
    assert dsl.snapshot_to == datetime(2025, 11, 28, 15, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_parse_count_distinct_publish_days_month() -> None:
    q = (
        "Для креатора с id aca1061a9d324ecf8c3fa2bb32d7be63 посчитай, "
        "в скольких разных календарных днях ноября 2025 года он публиковал хотя бы одно видео"
    )
    dsl = await parse_to_dsl(q)
    assert dsl.aggregation == Aggregation.count_distinct_publish_days
    assert dsl.creator_id == "aca1061a9d324ecf8c3fa2bb32d7be63"
    assert dsl.published_from == datetime(2025, 11, 1, tzinfo=timezone.utc)
    assert dsl.published_to == datetime(2025, 12, 1, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_parse_count_distinct_creators_with_final_gt() -> None:
    q = "Сколько разных креаторов имеют хотя бы одно видео, которое в итоге набрало больше 10 000 лайков?"
    dsl = await parse_to_dsl(q)
    assert dsl.aggregation == Aggregation.count_distinct_creators_with_final_gt
    assert dsl.threshold is not None
    assert dsl.threshold.metric == Metric.likes
    assert dsl.threshold.op == "gt"
    assert dsl.threshold.value == 10000


@pytest.mark.asyncio
async def test_parse_sum_final_month_range() -> None:
    q = "Какое суммарное количество лайков набрали все видео, опубликованные в декабре 2025 года?"
    dsl = await parse_to_dsl(q)
    assert dsl.aggregation == Aggregation.sum_final
    assert dsl.metric == Metric.likes
    assert dsl.published_from == datetime(2025, 12, 1, tzinfo=timezone.utc)
    assert dsl.published_to == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_extract_threshold_gte_lte() -> None:
    from app.nlp import _extract_threshold

    th = _extract_threshold("Сколько видео набрало не менее 10 000 просмотров?")
    assert th is not None
    assert th.metric == Metric.views
    assert th.op == "gte"
    assert th.value == 10000

    th2 = _extract_threshold("Сколько видео набрало не более 500 лайков?")
    assert th2 is not None
    assert th2.metric == Metric.likes
    assert th2.op == "lte"
    assert th2.value == 500


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
