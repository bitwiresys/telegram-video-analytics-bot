import asyncio

from app.bot_main import run_bot
from app.logging_setup import setup_logging


def main() -> None:
    """Принимает ничего; возвращает None после запуска бота (инициализация логов и asyncio)."""
    setup_logging()
    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
