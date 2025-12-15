import asyncio

from app.db import init_db
from app.logging_setup import setup_logging
from app.nlp import parse_to_dsl
from app.queries import execute_dsl

QUESTIONS = [
    "Сколько всего видео есть в системе?",
    "Сколько видео набрало больше 100 000 просмотров за всё время?",
    "На сколько просмотров в сумме выросли все видео 28 ноября 2025?",
    "Сколько разных видео получали новые просмотры 27 ноября 2025?",
    "Сколько видео у креатора с id aca1061a9d324ecf8c3fa2bb32d7be63 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?",
]


async def run() -> None:
    """Принимает ничего; возвращает None после прогона набора вопросов и печати числовых ответов."""
    await init_db()
    for q in QUESTIONS:
        dsl = await parse_to_dsl(q)
        res = await execute_dsl(dsl)
        print(q)
        print(res)


def main() -> None:
    """Принимает ничего; возвращает None после запуска selfcheck в asyncio."""
    setup_logging()
    asyncio.run(run())


if __name__ == "__main__":
    main()
