from datetime import date, datetime, timezone

import pytest

from app.dsl import Aggregation, Metric
from app.nlp import _extract_creator_id, _extract_json_object, _extract_range, _extract_threshold, parse_to_dsl


@pytest.mark.asyncio
async def test_parse_count_videos_total() -> None:
    dsl = await parse_to_dsl("Сколько всего видео есть в системе?")
    assert dsl.aggregation == Aggregation.count_videos


@pytest.mark.asyncio
async def test_parse_count_videos_creator_range() -> None:
    q = "Сколько видео у креатора с id aca1061a9d324ecf8c3fa2bb32d7be63 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?"
    dsl = await parse_to_dsl(q)
    assert dsl.aggregation == Aggregation.count_videos
    assert dsl.creator_id == "aca1061a9d324ecf8c3fa2bb32d7be63"
    assert dsl.published_from == datetime(2025, 11, 1, tzinfo=timezone.utc)
    assert dsl.published_to == datetime(2025, 11, 6, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_parse_sum_delta_day_views() -> None:
    dsl = await parse_to_dsl("На сколько просмотров в сумме выросли все видео 28 ноября 2025?")
    assert dsl.aggregation == Aggregation.sum_delta
    assert dsl.metric == Metric.views
    assert dsl.day == date(2025, 11, 28)


@pytest.mark.asyncio
async def test_parse_distinct_videos_delta() -> None:
    dsl = await parse_to_dsl("Сколько разных видео получали новые просмотры 27 ноября 2025?")
    assert dsl.aggregation == Aggregation.count_distinct_videos_with_delta_gt0
    assert dsl.metric == Metric.views
    assert dsl.day == date(2025, 11, 27)


def test_extract_creator_id() -> None:
    assert _extract_creator_id("креатора с id abcdefabcdefabcdefabcdefabcdefab") == "abcdefabcdefabcdefabcdefabcdefab"


def test_extract_range() -> None:
    left, right = _extract_range("с 1 ноября 2025 по 5 ноября 2025 включительно")
    assert left == date(2025, 11, 1)
    assert right == date(2025, 11, 5)


def test_extract_threshold_spaces() -> None:
    th = _extract_threshold("Сколько видео набрало больше 100 000 просмотров за всё время?")
    assert th is not None
    assert th.metric == Metric.views
    assert th.op == "gt"
    assert th.value == 100000


def test_extract_json_object_from_fenced() -> None:
    raw = "```json\n{\"aggregation\":\"count_videos\",\"metric\":null,\"creator_id\":null,\"published_from\":null,\"published_to\":null,\"day\":null,\"threshold\":null}\n```"
    blob = _extract_json_object(raw)
    assert blob is not None
    assert blob.startswith(b"{")
    assert blob.endswith(b"}")
