from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.settings import get_settings

_engine: AsyncEngine | None = None

logger = logging.getLogger(__name__)


def get_engine() -> AsyncEngine:
    """Принимает настройки окружения; возвращает кэшированный AsyncEngine для подключения к БД."""
    global _engine
    if _engine is None:
        settings = get_settings()
        if not settings.database_url:
            raise RuntimeError("DATABASE_URL is not set")
        _engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    return _engine


async def init_db() -> None:
    """Принимает ничего; возвращает None после инициализации схемы БД из SQL-файла."""
    sql_path = Path(__file__).resolve().parents[1] / "sql" / "001_init.sql"
    await run_sql_file(sql_path)


async def run_sql_file(path: Path) -> None:
    """Принимает путь к .sql; возвращает None после выполнения всех SQL-стейтментов в транзакции."""
    content = path.read_text(encoding="utf-8")
    statements = [s.strip() for s in content.split(";") if s.strip()]
    engine = get_engine()
    async with engine.begin() as conn:
        logger.info("db_init_start", extra={"path": str(path), "statements": len(statements)})
        for stmt in statements:
            await conn.execute(text(stmt))
        logger.info("db_init_done", extra={"path": str(path)})


async def fetch_scalar(stmt: str, params: dict[str, object] | None = None) -> int:
    """Принимает SQL и параметры; возвращает целое число (0 если результат пустой)."""
    engine = get_engine()
    async with engine.connect() as conn:
        logger.info("db_scalar", extra={"stmt": stmt, "params": params or {}})
        res = await conn.execute(text(stmt), params or {})
        val = res.scalar_one_or_none()
        if val is None:
            return 0
        return int(val)
