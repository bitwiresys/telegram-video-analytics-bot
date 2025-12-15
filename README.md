# Telegram Bot Analytics

## RU

### Описание

Telegram-бот принимает русскоязычные вопросы по аналитике видео и возвращает одно число. Данные хранятся в PostgreSQL и загружаются из предоставленного `videos.json`.

### Быстрый запуск локально (Docker)

#### Требования

- Docker + Docker Compose

#### Шаги

1) Положи `videos.json` в корень репозитория.

2) Создай файл окружения:

```bash
cp .env.example .env
```

Windows:

```bat
copy .env.example .env
```

3) Заполни минимум:

- `BOT_TOKEN=...`

Опционально для LLM:

- `OPENROUTER_API_KEY=...`
- `OPENROUTER_MODEL=...`
- `OPENROUTER_FALLBACK_MODEL=...`

4) Запусти:

```bash
docker compose up --build
```

При старте:

- поднимается PostgreSQL;
- выполняется создание таблиц (скрипт `sql/001_init.sql`);
- при `AUTO_IMPORT=1` автоматически импортируются недостающие данные из `videos.json`.

### Запуск локально (без Docker)

#### Требования

- Python 3.10+
- PostgreSQL

#### Шаги

1) Установи зависимости:

```bash
pip install -r requirements.txt
```

2) Создай `.env` из примера и укажи:

- `BOT_TOKEN=...`
- `DATABASE_URL=postgresql+asyncpg://app:app@localhost:5432/app`
- `VIDEOS_JSON_PATH=./videos.json`

3) Создай таблицы:

```bash
python -c "import asyncio; from app.db import init_db; asyncio.run(init_db())"
```

4) Импортируй данные:

```bash
python -m app.import_main
```

5) Запусти бота:

```bash
python -m app
```

### Как задать токен Telegram-бота

Токен задаётся через переменную окружения `BOT_TOKEN` (рекомендуемый способ — `.env`).

### Как подключить LLM

LLM используется как опциональный слой для преобразования текста в JSON-DSL.

- Провайдер: OpenRouter
- Настройки через переменные окружения:

```text
OPENROUTER_API_KEY=...
OPENROUTER_MODEL=...
OPENROUTER_FALLBACK_MODEL=...
```

Если LLM не настроен или вернул невалидный JSON, используется эвристический разбор.

### Как описывается схема данных для LLM (промпт)

Промпт для LLM находится в `app/nlp.py` в переменной `_SYSTEM`. В нём описаны:

- агрегаты (значения `QueryDSL.aggregation`)
- соответствие формулировок пользователя таблицам `videos` (final) и `video_snapshots` (delta/snapshots)
- формат дат/диапазонов (UTC)

### Кратко про архитектуру и преобразование запроса в SQL

Пайплайн обработки сообщения:

1) `app.bot_main.handle_message` получает текст сообщения.
2) `app.nlp.parse_to_dsl(text)` преобразует текст в `QueryDSL`:
   - LLM → JSON → Pydantic-валидация;
   - fallback: эвристики (метрика/агрегация/даты/порог) → `QueryDSL`.
3) `app.queries.execute_dsl(dsl)` превращает DSL в параметризованный SQL.
4) `app.db.fetch_scalar(stmt, params)` выполняет SQL и возвращает `int`.

Инвариант: ответ пользователю всегда число (в т.ч. 0 при ошибках).

### Схема БД и миграции/SQL

Схема создаётся скриптом:

- `sql/001_init.sql`

Таблицы:

- `videos` — финальные метрики по видео.
- `video_snapshots` — снимки метрик и delta-значения по времени.

### Импорт JSON

Импорт выполняется командой:

```bash
python -m app.import_main
```

## EN

### Overview

A Telegram bot that answers analytics questions about videos and returns a single number. Data is stored in PostgreSQL and loaded from the provided `videos.json`.

### Local run (Docker)

#### Requirements

- Docker + Docker Compose

#### Steps

1) Put `videos.json` into the repository root.

2) Create environment file:

```bash
cp .env.example .env
```

Windows:

```bat
copy .env.example .env
```

3) Fill at minimum:

- `BOT_TOKEN=...`

Optional (LLM integration):

- `OPENROUTER_API_KEY=...`
- `OPENROUTER_MODEL=...`
- `OPENROUTER_FALLBACK_MODEL=...`

4) Run:

```bash
docker compose up --build
```

On startup:

- PostgreSQL is started;
- schema is created (`sql/001_init.sql`);
- if `AUTO_IMPORT=1`, missing data is imported from `videos.json`.

### Local run (no Docker)

#### Requirements

- Python 3.10+
- PostgreSQL

#### Steps

1) Install dependencies:

```bash
pip install -r requirements.txt
```

2) Create `.env` from the example and set:

- `BOT_TOKEN=...`
- `DATABASE_URL=postgresql+asyncpg://app:app@localhost:5432/app`
- `VIDEOS_JSON_PATH=./videos.json`

3) Create tables:

```bash
python -c "import asyncio; from app.db import init_db; asyncio.run(init_db())"
```

4) Import data:

```bash
python -m app.import_main
```

5) Run the bot:

```bash
python -m app
```

### Telegram bot token

Set the token via `BOT_TOKEN` (recommended: `.env`).

### LLM integration

LLM is an optional layer that converts natural language into JSON-DSL (OpenRouter):

```text
OPENROUTER_API_KEY=...
OPENROUTER_MODEL=...
OPENROUTER_FALLBACK_MODEL=...
```

If LLM is not configured or returns invalid output, a heuristic parser is used.

### LLM prompt (schema description)

The LLM prompt is defined in `app/nlp.py` as `_SYSTEM`. It describes:

- supported aggregations (`QueryDSL.aggregation`)
- mapping between user intent and tables (`videos` for final metrics, `video_snapshots` for deltas/snapshots)
- date formats and UTC rules

### Architecture (NL → SQL → PostgreSQL)

Request flow:

1) `app.bot_main.handle_message` receives the user text.
2) `app.nlp.parse_to_dsl(text)` produces a `QueryDSL` (LLM first, heuristics fallback).
3) `app.queries.execute_dsl(dsl)` builds parameterized SQL.
4) `app.db.fetch_scalar(stmt, params)` executes SQL and returns an `int`.

Invariant: the bot always replies with a number (including `0` on errors).

### Database schema and SQL

Schema is created from:

- `sql/001_init.sql`

### JSON import

Use:

```bash
python -m app.import_main
```
