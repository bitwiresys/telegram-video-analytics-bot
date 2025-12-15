from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message

from app.db import init_db
from app.importer import ensure_imported, import_videos
from app.nlp import parse_to_dsl
from app.queries import execute_dsl
from app.settings import get_settings

logger = logging.getLogger(__name__)


dp = Dispatcher()


@dp.message(CommandStart())
async def start(message: Message) -> None:
    """Принимает входящее сообщение /start; возвращает None после отправки пользователю ответа."""
    await message.answer("RLT test task from @nomeoffz")


@dp.message()
async def handle_message(message: Message) -> None:
    """Принимает входящее текстовое сообщение; возвращает None после ответа числом (или 0 при ошибке)."""
    text = message.text or ""
    extra = {
        "chat_id": getattr(message.chat, "id", None),
        "user_id": getattr(getattr(message, "from_user", None), "id", None),
        "message_id": getattr(message, "message_id", None),
        "query": text,
    }
    logger.info("incoming_message", extra=extra)
    try:
        dsl = await parse_to_dsl(text)
        result = await execute_dsl(dsl)
        logger.info("query_ok", extra={**extra, "result": result, "dsl": dsl.model_dump(mode="json")})
        await message.answer(str(result))
    except Exception:
        logger.exception("query_failed", extra=extra)
        await message.answer("0")


async def run_bot() -> None:
    """Принимает ничего; возвращает None после старта поллинга бота (не завершается при нормальной работе)."""
    settings = get_settings()
    await init_db()

    if settings.auto_import and settings.videos_json_path:
        await ensure_imported(Path(settings.videos_json_path))

    if not settings.bot_token:
        raise RuntimeError("BOT_TOKEN is not set")
    bot = Bot(token=settings.bot_token)
    logger.info("bot_start")
    await dp.start_polling(bot)


async def run_import_only() -> None:
    """Принимает ничего; возвращает None после запуска импорта JSON в БД (идемпотентно)."""
    settings = get_settings()
    await init_db()
    if not settings.videos_json_path:
        return
    await import_videos(Path(settings.videos_json_path))


def main_import() -> None:
    """Принимает ничего; возвращает None после синхронного запуска import-only режима."""
    asyncio.run(run_import_only())
