import asyncio

from app.bot_main import run_import_only
from app.logging_setup import setup_logging


def main() -> None:
    """Принимает ничего; возвращает None после запуска одноразового импорта в БД."""
    setup_logging()
    asyncio.run(run_import_only())


if __name__ == "__main__":
    main()
