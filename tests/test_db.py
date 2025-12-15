from __future__ import annotations

from pathlib import Path

import pytest

from app import db


class FakeResult:
    def __init__(self, value: int | None) -> None:
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, object]] = []

    async def execute(self, stmt, params=None):
        self.executed.append((str(stmt), params))
        return FakeResult(5)


class FakeBegin:
    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> FakeConn:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakeConnect(FakeBegin):
    pass


class FakeEngine:
    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn

    def begin(self) -> FakeBegin:
        return FakeBegin(self._conn)

    def connect(self) -> FakeConnect:
        return FakeConnect(self._conn)


@pytest.mark.asyncio
async def test_run_sql_file_executes_statements(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    engine = FakeEngine(conn)

    monkeypatch.setattr(db, "get_engine", lambda: engine)

    p = tmp_path / "x.sql"
    p.write_text("SELECT 1;\n\nSELECT 2;\n", encoding="utf-8")

    await db.run_sql_file(p)

    assert len(conn.executed) == 2
    assert "SELECT 1" in conn.executed[0][0]
    assert "SELECT 2" in conn.executed[1][0]


@pytest.mark.asyncio
async def test_fetch_scalar_returns_int(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    engine = FakeEngine(conn)

    monkeypatch.setattr(db, "get_engine", lambda: engine)

    v = await db.fetch_scalar("SELECT 1", {"a": 1})
    assert v == 5
    assert conn.executed
