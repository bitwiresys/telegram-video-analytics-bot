import json
import logging
import os

from app.logging_setup import JsonFormatter


def test_json_formatter_includes_custom_fields() -> None:
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="x",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )
    record.chat_id = 1
    record.result = 2
    out = formatter.format(record)
    obj = json.loads(out)
    assert obj["msg"] == "hello"
    assert obj["chat_id"] == 1
    assert obj["result"] == 2


def test_settings_env_mapping_and_db_engine_cache(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/db")
    monkeypatch.setenv("BOT_TOKEN", "x")

    from app import db

    db._engine = None
    e1 = db.get_engine()
    e2 = db.get_engine()
    assert e1 is e2

    os.environ.pop("DATABASE_URL", None)
    os.environ.pop("BOT_TOKEN", None)
