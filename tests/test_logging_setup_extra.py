import logging
import os

from app.logging_setup import JsonFormatter, setup_logging


def test_setup_logging_installs_json_handler(monkeypatch) -> None:
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    setup_logging()

    root = logging.getLogger()
    assert root.level == logging.DEBUG
    assert root.handlers

    handler = root.handlers[0]
    assert isinstance(handler.formatter, JsonFormatter)

    assert logging.getLogger("httpx").level == logging.WARNING
    assert logging.getLogger("sqlalchemy.engine").level == logging.WARNING

    monkeypatch.delenv("LOG_LEVEL", raising=False)


def test_setup_logging_default_level() -> None:
    os.environ.pop("LOG_LEVEL", None)
    setup_logging()
    root = logging.getLogger()
    assert root.level in {logging.INFO, logging.WARNING, logging.ERROR}
