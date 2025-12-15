from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from app.importer import import_videos


class FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, Any]] = []

    async def execute(self, stmt: Any, params: Any) -> None:
        if isinstance(params, list):
            snap: list[Any] = []
            for item in params:
                if isinstance(item, dict):
                    snap.append(dict(item))
                else:
                    snap.append(item)
            self.calls.append((stmt, snap))
            return
        if isinstance(params, dict):
            self.calls.append((stmt, dict(params)))
            return
        self.calls.append((stmt, params))


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


@pytest.mark.asyncio
async def test_import_videos_batches(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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
                "snapshots": [
                    {
                        "id": "s1",
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
                ],
            }
        ]
    }

    import orjson

    p = tmp_path / "v.json"
    p.write_bytes(orjson.dumps(payload))

    conn = FakeConn()
    engine = FakeEngine(conn)

    monkeypatch.setattr("app.importer.get_engine", lambda: engine)

    await import_videos(p, batch_size=1)

    assert conn.calls
    has_creator = False
    has_delta = False
    for _, params in conn.calls:
        if not isinstance(params, list):
            continue
        for row in params:
            if not isinstance(row, dict):
                continue
            if "creator_id" in row:
                has_creator = True
            if "delta_views_count" in row:
                has_delta = True

    assert has_creator
    assert has_delta
