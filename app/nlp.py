from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import cast

import dateparser
import orjson
from pydantic import ValidationError

from app.dsl import Aggregation, Metric, QueryDSL, Threshold
from app.openrouter import chat_completion
from app.settings import get_settings

logger = logging.getLogger(__name__)


_MONTHS: dict[str, int] = {
    "январ": 1,
    "феврал": 2,
    "март": 3,
    "апрел": 4,
    "ма": 5,
    "июн": 6,
    "июл": 7,
    "август": 8,
    "сентябр": 9,
    "октябр": 10,
    "ноябр": 11,
    "декабр": 12,
}


def _month_num(word: str) -> int | None:
    """Принимает слово с названием месяца; возвращает номер месяца или None."""
    w = word.strip().lower()
    for prefix, num in _MONTHS.items():
        if w.startswith(prefix):
            return num
    return None


_METRIC_ALIASES: list[tuple[Metric, list[str]]] = [
    (Metric.views, ["просмотр", "просмотры", "просмотров"]),
    (Metric.likes, ["лайк", "лайки", "лайков"]),
    (Metric.comments, ["коммент", "комментар", "комментарии", "комментариев"]),
    (Metric.reports, ["жалоб", "жалоба", "жалобы", "репорт", "репорты"]),
]


def _detect_metric(text: str) -> Metric | None:
    """Принимает текст запроса; возвращает найденную Metric или None."""
    t = text.lower()
    for metric, aliases in _METRIC_ALIASES:
        for a in aliases:
            if a in t:
                return metric
    return None


def _parse_int_with_spaces(s: str) -> int:
    """Принимает строку числа с пробелами; возвращает int."""
    return int(re.sub(r"\s+", "", s))


def _extract_creator_id(text: str) -> str | None:
    """Принимает текст; возвращает creator_id (32 hex) или None."""
    m = re.search(r"креатор[а-я]*\s+с\s+id\s+`?([0-9a-fA-F]{32})`?", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).lower()
    m = re.search(r"creator\s*[_-]?id\s*[:=]\s*`?([0-9a-fA-F]{32})`?", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).lower()
    return None


def _day_bounds_utc(d: date) -> tuple[datetime, datetime]:
    """Принимает дату; возвращает (start,end) границы суток в UTC."""
    start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _parse_date(s: str) -> date | None:
    """Принимает строку даты; возвращает date или None при невозможности разбора."""
    s_norm = s.strip().lower()
    if re.fullmatch(r"\d{1,2}", s_norm):
        return None
    if re.fullmatch(r"\d{1,2}\s+[а-я]+", s_norm):
        return None
    m = re.search(r"(\d{1,2})\s+([а-я]+)\s+(\d{4})", s_norm)
    if m:
        day = int(m.group(1))
        month = _month_num(m.group(2))
        year = int(m.group(3))
        if month is None:
            return None
        try:
            return date(year, month, day)
        except ValueError:
            return None

    dt_any = dateparser.parse(
        s,
        languages=["ru"],
        settings={"TIMEZONE": "UTC", "RETURN_AS_TIMEZONE_AWARE": True},
    )
    dt = cast(datetime | None, dt_any)
    if not dt:
        return None
    return dt.date()


def _extract_time_range(text: str) -> tuple[tuple[int, int], tuple[int, int]] | None:
    """Принимает текст; возвращает ((h1,m1),(h2,m2)) для диапазона времени или None."""
    m = re.search(
        r"с\s*(\d{1,2}):(\d{2})\s*(?:до|по)\s*(\d{1,2}):(\d{2})",
        text,
        flags=re.IGNORECASE,
    )
    if not m:
        return None
    h1, m1, h2, m2 = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
    if not (0 <= h1 <= 23 and 0 <= h2 <= 23 and 0 <= m1 <= 59 and 0 <= m2 <= 59):
        return None
    return (h1, m1), (h2, m2)


def _extract_day(text: str) -> date | None:
    """Принимает текст; возвращает найденную date или None."""
    m = re.search(r"(\d{1,2}\s+[а-яА-Я]+\s+\d{4})", text)
    if m:
        d = _parse_date(m.group(1))
        if d is not None:
            return d

    dt_any = dateparser.parse(
        text,
        languages=["ru"],
        settings={"TIMEZONE": "UTC", "RETURN_AS_TIMEZONE_AWARE": True},
    )
    dt = cast(datetime | None, dt_any)
    if not dt:
        return None
    return dt.date()


def _extract_range(text: str) -> tuple[date | None, date | None]:
    """Принимает текст; возвращает (date_from,date_to) или (None,None)."""
    m = re.search(
        r"(?:^|\s)с\s+(\d{1,2}(?:\s+[а-яА-Я]+)?(?:\s+\d{4})?)\s+по\s+"
        r"(\d{1,2}(?:\s+[а-яА-Я]+)?(?:\s+\d{4})?)(?:\s+включительно)?(?=\s|$)",
        text,
        flags=re.IGNORECASE,
    )
    if m:
        left_raw = m.group(1).strip()
        right_raw = m.group(2).strip()
        right = _parse_date(right_raw)
        if right is None:
            return None, None
        left = _parse_date(left_raw)
        if left is not None:
            return left, right

        m0 = re.search(r"^(\d{1,2})\s+([а-яА-Я]+)\s+(\d{4})$", left_raw)
        if m0:
            left_try = _parse_date(left_raw)
            return left_try, right

        m2 = re.search(r"^(\d{1,2})$", left_raw)
        if m2:
            day_num = int(m2.group(1))
            try:
                left = date(right.year, right.month, day_num)
                return left, right
            except ValueError:
                return None, None

        m3 = re.search(r"^(\d{1,2})\s+([а-яА-Я]+)$", left_raw)
        if m3:
            left_try = _parse_date(f"{m3.group(1)} {m3.group(2)} {right.year}")
            return left_try, right

        return None, None

    return None, None


def _extract_json_object(raw: str) -> bytes | None:
    """Принимает сырой ответ LLM; возвращает JSON-объект в bytes или None."""
    s = raw.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*\n?", "", s)
        s = re.sub(r"```$", "", s.strip())
    start = s.find("{")
    end = s.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    return s[start : end + 1].encode("utf-8")


def _extract_threshold(text: str) -> Threshold | None:
    """Принимает текст; возвращает Threshold (>, число) или None."""
    t = text.lower()
    if "больше" not in t and ">" not in t:
        return None

    metric = _detect_metric(text)
    if metric is None:
        return None

    m = re.search(r"(больше|>)\s*([0-9][0-9\s]*)", text)
    if not m:
        return None

    value = _parse_int_with_spaces(m.group(2))
    return Threshold(metric=metric, op="gt", value=value)


def _heuristic_parse(text: str) -> QueryDSL | None:
    """Принимает текст; возвращает QueryDSL из эвристик или None если не распознано."""
    t = text.lower()

    if "замер" in t or "снапш" in t or "за час" in t:
        metric = _detect_metric(text)
        if metric is not None and ("отриц" in t or "стало меньше" in t or "уменьш" in t):
            d = _extract_day(text)
            return QueryDSL(aggregation=Aggregation.count_snapshots_with_delta_lt0, metric=metric, day=d)

    new_views_intent = (
        "получ" in t
        and "нов" in t
        and ("просмотр" in t or "лайк" in t or "комментар" in t or "жалоб" in t)
    )

    if (
        "сколько" in t
        and "видео" in t
        and "вырос" not in t
        and "прирост" not in t
        and not new_views_intent
        and not ("разн" in t and "креатор" in t)
    ):
        creator_id = _extract_creator_id(text)
        left, right = _extract_range(text)
        published_from = None
        published_to = None
        if left and right:
            start, _ = _day_bounds_utc(left)
            _, end = _day_bounds_utc(right)
            published_from = start
            published_to = end
        threshold = _extract_threshold(text)
        return QueryDSL(
            aggregation=Aggregation.count_videos,
            creator_id=creator_id,
            published_from=published_from,
            published_to=published_to,
            threshold=threshold,
        )

    if ("вырос" in t or "прирост" in t) and "видео" in t:
        creator_id = _extract_creator_id(text)
        metric = _detect_metric(text)
        if metric is None:
            metric = Metric.views
        d = _extract_day(text)
        if d is None:
            return None
        tr = _extract_time_range(text)
        if tr is not None:
            (h1, m1), (h2, m2) = tr
            start_dt = datetime(d.year, d.month, d.day, h1, m1, tzinfo=timezone.utc)
            end_dt = datetime(d.year, d.month, d.day, h2, m2, tzinfo=timezone.utc)
            if end_dt <= start_dt:
                end_dt = end_dt + timedelta(days=1)
            return QueryDSL(
                aggregation=Aggregation.sum_delta,
                metric=metric,
                creator_id=creator_id,
                day=d,
                snapshot_from=start_dt,
                snapshot_to=end_dt,
            )

        return QueryDSL(aggregation=Aggregation.sum_delta, metric=metric, creator_id=creator_id, day=d)

    if "сколько" in t and "разн" in t and "видео" in t and ("нов" in t or "получ" in t):
        metric = _detect_metric(text)
        if metric is None:
            metric = Metric.views
        d = _extract_day(text)
        if d is None:
            return None
        return QueryDSL(aggregation=Aggregation.count_distinct_videos_with_delta_gt0, metric=metric, day=d)

    if "сколько" in t and "разн" in t and "креатор" in t and "видео" in t:
        threshold = _extract_threshold(text)
        if threshold is None:
            return None
        creator_id = _extract_creator_id(text)
        left, right = _extract_range(text)
        published_from = None
        published_to = None
        if left and right:
            start, _ = _day_bounds_utc(left)
            _, end = _day_bounds_utc(right)
            published_from = start
            published_to = end
        return QueryDSL(
            aggregation=Aggregation.count_distinct_creators_with_final_gt,
            creator_id=creator_id,
            published_from=published_from,
            published_to=published_to,
            threshold=threshold,
        )

    if "сколько" in t and ("просмотр" in t or "лайк" in t or "комментар" in t or "жалоб" in t) and "вс" in t:
        metric = _detect_metric(text)
        if metric is None:
            return None
        return QueryDSL(aggregation=Aggregation.sum_final, metric=metric)

    return None


_SYSTEM = """Ты преобразуешь русскоязычный вопрос пользователя к аналитике в JSON по схеме QueryDSL.

Схема QueryDSL:
{
  "aggregation": "count_videos" | "sum_final" | "sum_delta" | "count_distinct_videos_with_delta_gt0" | "count_snapshots_with_delta_lt0" | "count_distinct_creators_with_final_gt",
  "metric": "views" | "likes" | "comments" | "reports" | null,
  "creator_id": string|null,
  "published_from": string|null,
  "published_to": string|null,
  "snapshot_from": string|null,
  "snapshot_to": string|null,
  "day": string|null,
  "threshold": {"metric": "views"|"likes"|"comments"|"reports", "op": "gt"|"gte"|"lt"|"lte", "value": number} | null
}

Правила:
- Все даты в UTC.
- published_from/published_to в ISO8601, published_to должно быть началом следующего дня для включительного диапазона.
- day в формате YYYY-MM-DD.
- snapshot_from/snapshot_to в ISO8601, используются для фильтрации по created_at в video_snapshots.
- "за всё время" означает таблицу videos (final).
- "выросли" и "прирост" означают сумму delta_* по таблице video_snapshots в пределах дня.
- "замеры статистики", "снапшоты", "за час" и "delta" относятся к таблице video_snapshots.
- "отрицательный" или "стало меньше" для просмотров/лайков/комментариев/жалоб означает count_snapshots_with_delta_lt0 по delta_*.
- Ответь только JSON без текста.
"""


async def parse_to_dsl(text: str) -> QueryDSL:
    """Принимает текст запроса; возвращает QueryDSL (LLM при наличии ключей, иначе эвристики)."""
    settings = get_settings()

    heuristic_dsl = _heuristic_parse(text)
    if heuristic_dsl is not None and heuristic_dsl.aggregation in {
        Aggregation.count_snapshots_with_delta_lt0,
        Aggregation.count_distinct_creators_with_final_gt,
    }:
        return heuristic_dsl

    if settings.openrouter_api_key and settings.openrouter_model:
        try:
            raw = await chat_completion(_SYSTEM, text)
            blob = _extract_json_object(raw)
            if blob is None:
                raise ValueError("no json")
            obj = orjson.loads(blob)
            try:
                llm_dsl = QueryDSL.model_validate(obj)
                t = text.lower()
                is_snapshot_intent = "замер" in t or "снапш" in t or "за час" in t
                if (
                    is_snapshot_intent
                    and llm_dsl.aggregation == Aggregation.count_videos
                    and llm_dsl.threshold is not None
                    and llm_dsl.threshold.op == "lt"
                    and llm_dsl.threshold.value == 0
                ):
                    return QueryDSL(
                        aggregation=Aggregation.count_snapshots_with_delta_lt0,
                        metric=llm_dsl.threshold.metric,
                        day=llm_dsl.day,
                    )
                return llm_dsl
            except ValidationError:
                logger.warning("llm_invalid_dsl")
        except Exception as exc:
            logger.warning("llm_failed", extra={"error": str(exc)})

        logger.info("fallback_to_heuristic")

    heuristic_dsl = _heuristic_parse(text)
    if heuristic_dsl is not None:
        return heuristic_dsl

    logger.info("fallback_default")
    return QueryDSL(aggregation=Aggregation.count_videos)
