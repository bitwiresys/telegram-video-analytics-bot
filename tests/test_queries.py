from datetime import date, datetime, timezone

import pytest

from app.dsl import Aggregation, Metric, QueryDSL, Threshold
from app.queries import execute_dsl


@pytest.mark.asyncio
async def test_execute_count_videos(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, object] = {}

    async def fake_fetch(stmt: str, params: dict[str, object] | None = None) -> int:
        called["stmt"] = stmt
        called["params"] = params or {}
        return 7

    monkeypatch.setattr("app.queries.fetch_scalar", fake_fetch)

    dsl = QueryDSL(
        aggregation=Aggregation.count_videos,
        creator_id="aaa",
        published_from=datetime(2025, 11, 1, tzinfo=timezone.utc),
        published_to=datetime(2025, 11, 2, tzinfo=timezone.utc),
        threshold=Threshold(metric=Metric.views, op="gt", value=100),
    )

    res = await execute_dsl(dsl)
    assert res == 7
    assert "SELECT count(*) FROM videos" in str(called["stmt"])
    assert "creator_id = :creator_id" in str(called["stmt"])
    assert "video_created_at >= :published_from" in str(called["stmt"])
    assert "video_created_at < :published_to" in str(called["stmt"])
    assert "views_count > :threshold_value" in str(called["stmt"])


@pytest.mark.asyncio
async def test_execute_sum_final(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch(stmt: str, params: dict[str, object] | None = None) -> int:
        assert "sum(likes_count)" in stmt
        return 11

    monkeypatch.setattr("app.queries.fetch_scalar", fake_fetch)
    dsl = QueryDSL(aggregation=Aggregation.sum_final, metric=Metric.likes)
    res = await execute_dsl(dsl)
    assert res == 11


@pytest.mark.asyncio
async def test_execute_sum_delta(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch(stmt: str, params: dict[str, object] | None = None) -> int:
        assert "sum(delta_views_count)" in stmt
        assert params is not None
        assert params["start"] == datetime(2025, 11, 28, tzinfo=timezone.utc)
        assert params["end"] == datetime(2025, 11, 29, tzinfo=timezone.utc)
        return 123

    monkeypatch.setattr("app.queries.fetch_scalar", fake_fetch)
    dsl = QueryDSL(aggregation=Aggregation.sum_delta, metric=Metric.views, day=date(2025, 11, 28))
    res = await execute_dsl(dsl)
    assert res == 123


@pytest.mark.asyncio
async def test_execute_distinct_delta(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch(stmt: str, params: dict[str, object] | None = None) -> int:
        assert "count(DISTINCT video_id)" in stmt
        assert "delta_comments_count" in stmt
        return 3

    monkeypatch.setattr("app.queries.fetch_scalar", fake_fetch)
    dsl = QueryDSL(
        aggregation=Aggregation.count_distinct_videos_with_delta_gt0,
        metric=Metric.comments,
        day=date(2025, 11, 27),
    )
    res = await execute_dsl(dsl)
    assert res == 3


@pytest.mark.asyncio
async def test_execute_count_snapshots_with_delta_lt0(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch(stmt: str, params: dict[str, object] | None = None) -> int:
        assert "SELECT count(*) FROM video_snapshots" in stmt
        assert "delta_views_count < 0" in stmt
        assert params is None or params == {}
        return 24

    monkeypatch.setattr("app.queries.fetch_scalar", fake_fetch)
    dsl = QueryDSL(aggregation=Aggregation.count_snapshots_with_delta_lt0, metric=Metric.views)
    res = await execute_dsl(dsl)
    assert res == 24
