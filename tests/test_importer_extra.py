from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from app.importer import ensure_imported, import_videos


class FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    async def execute(self, stmt: Any, params: Any) -> None:
        if isinstance(params, list):
            snap = [dict(x) if isinstance(x, dict) else x for x in params]
        elif isinstance(params, dict):
            snap = dict(params)
        else:
            snap = params
        self.calls.append((str(stmt), snap))


class FakeBegin:
    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> FakeConn:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakeEngine:
    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn

    def begin(self) -> FakeBegin:
        return FakeBegin(self._conn)


class FakeScalarResult:
    def __init__(self, value: int) -> None:
        self._value = value

    def scalar_one_or_none(self) -> int:
        return self._value


class FakeCheckConn:
    def __init__(self, videos: int, snapshots: int) -> None:
        self._videos = videos
        self._snapshots = snapshots

    async def execute(self, stmt: Any) -> FakeScalarResult:
        sql = str(stmt)
        if "FROM videos" in sql:
            return FakeScalarResult(self._videos)
        return FakeScalarResult(self._snapshots)

    async def __aenter__(self) -> FakeCheckConn:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakeCheckEngine:
    def __init__(self, videos: int, snapshots: int) -> None:
        self._videos = videos
        self._snapshots = snapshots

    def connect(self) -> FakeCheckConn:
        return FakeCheckConn(self._videos, self._snapshots)


@pytest.mark.asyncio
async def test_import_videos_no_list(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import orjson

    p = tmp_path / "v.json"
    p.write_bytes(orjson.dumps({"videos": "bad"}))

    conn = FakeConn()
    engine = FakeEngine(conn)
    monkeypatch.setattr("app.importer.get_engine", lambda: engine)

    await import_videos(p)
    assert conn.calls == []


@pytest.mark.asyncio
async def test_import_videos_skips_invalid_uuid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import orjson

    p = tmp_path / "v.json"
    p.write_bytes(
        orjson.dumps(
            {
                "videos": [
                    {
                        "id": "not-a-uuid",
                        "video_created_at": "2025-08-19T08:54:35+00:00",
                        "views_count": 1,
                        "likes_count": 2,
                        "reports_count": 0,
                        "comments_count": 0,
                        "creator_id": "x",
                        "created_at": "2025-11-26T11:00:08.983295+00:00",
                        "updated_at": "2025-12-01T10:00:00.236609+00:00",
                        "snapshots": [],
                    }
                ]
            }
        )
    )

    conn = FakeConn()
    engine = FakeEngine(conn)
    monkeypatch.setattr("app.importer.get_engine", lambda: engine)

    await import_videos(p, batch_size=1)
    assert conn.calls == []


@pytest.mark.asyncio
async def test_import_videos_flushes_snapshots(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import orjson

    snaps = []
    for i in range(25):
        snaps.append(
            {
                "id": f"s{i}",
                "video_id": "ecd8a4e4-1f24-4b97-a944-35d17078ce7c",
                "views_count": 1,
                "likes_count": 2,
                "reports_count": 0,
                "comments_count": 0,
                "delta_views_count": 1,
                "delta_likes_count": 2,
                "delta_reports_count": 0,
                "delta_comments_count": 0,
                "created_at": "2025-11-26T11:00:09.053200+00:00",
                "updated_at": "2025-11-26T11:00:09.053200+00:00",
            }
        )

    payload = {
        "videos": [
            {
                "id": "ecd8a4e4-1f24-4b97-a944-35d17078ce7c",
                "video_created_at": "2025-08-19T08:54:35+00:00",
                "views_count": 1,
                "likes_count": 2,
                "reports_count": 0,
                "comments_count": 0,
                "creator_id": "aca1061a9d324ecf8c3fa2bb32d7be63",
                "created_at": "2025-11-26T11:00:08.983295+00:00",
                "updated_at": "2025-12-01T10:00:00.236609+00:00",
                "snapshots": snaps,
            }
        ]
    }

    p = tmp_path / "v.json"
    p.write_bytes(orjson.dumps(payload))

    conn = FakeConn()
    engine = FakeEngine(conn)
    monkeypatch.setattr("app.importer.get_engine", lambda: engine)

    await import_videos(p, batch_size=1)

    assert conn.calls
    sqls = [sql for sql, _ in conn.calls]
    assert any("INSERT INTO videos" in s for s in sqls)
    assert any("INSERT INTO video_snapshots" in s for s in sqls)

    first_video = next(i for i, s in enumerate(sqls) if "INSERT INTO videos" in s)
    first_snap = next(i for i, s in enumerate(sqls) if "INSERT INTO video_snapshots" in s)
    assert first_video < first_snap


@pytest.mark.asyncio
async def test_ensure_imported_skips_when_present(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    p = tmp_path / "v.json"
    p.write_text("{}", encoding="utf-8")

    engine = FakeCheckEngine(videos=1, snapshots=1)
    monkeypatch.setattr("app.importer.get_engine", lambda: engine)

    called = False

    async def fake_import(_: Path, batch_size: int = 500) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr("app.importer.import_videos", fake_import)

    did_import = await ensure_imported(p)
    assert did_import is False
    assert called is False


@pytest.mark.asyncio
async def test_ensure_imported_imports_when_empty(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    p = tmp_path / "v.json"
    p.write_text("{}", encoding="utf-8")

    engine = FakeCheckEngine(videos=0, snapshots=0)
    monkeypatch.setattr("app.importer.get_engine", lambda: engine)

    called = False

    async def fake_import(_: Path, batch_size: int = 500) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr("app.importer.import_videos", fake_import)

    did_import = await ensure_imported(p)
    assert did_import is True
    assert called is True


@pytest.mark.asyncio
async def test_ensure_imported_expected_counts_skip(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import orjson

    p = tmp_path / "v.json"
    p.write_bytes(
        orjson.dumps(
            {
                "videos": [
                    {
                        "id": "ecd8a4e4-1f24-4b97-a944-35d17078ce7c",
                        "snapshots": [{"id": "s1"}],
                    }
                ]
            }
        )
    )

    engine = FakeCheckEngine(videos=1, snapshots=1)
    monkeypatch.setattr("app.importer.get_engine", lambda: engine)

    called = False

    async def fake_import(_: Path, batch_size: int = 500) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr("app.importer.import_videos", fake_import)

    did_import = await ensure_imported(p)
    assert did_import is False
    assert called is False


@pytest.mark.asyncio
async def test_ensure_imported_expected_counts_import(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import orjson

    p = tmp_path / "v.json"
    p.write_bytes(
        orjson.dumps(
            {
                "videos": [
                    {
                        "id": "ecd8a4e4-1f24-4b97-a944-35d17078ce7c",
                        "snapshots": [{"id": "s1"}],
                    }
                ]
            }
        )
    )

    engine = FakeCheckEngine(videos=0, snapshots=0)
    monkeypatch.setattr("app.importer.get_engine", lambda: engine)

    called = False

    async def fake_import(_: Path, batch_size: int = 500) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr("app.importer.import_videos", fake_import)

    did_import = await ensure_imported(p)
    assert did_import is True
    assert called is True
