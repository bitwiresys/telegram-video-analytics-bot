from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from app.db import fetch_scalar
from app.dsl import Aggregation, Metric, QueryDSL

logger = logging.getLogger(__name__)


def _metric_column(metric: Metric) -> str:
    """Принимает Metric; возвращает имя колонки final-метрики в таблице videos."""
    return {
        Metric.views: "views_count",
        Metric.likes: "likes_count",
        Metric.comments: "comments_count",
        Metric.reports: "reports_count",
    }[metric]


def _delta_metric_column(metric: Metric) -> str:
    """Принимает Metric; возвращает имя колонки delta-метрики в таблице video_snapshots."""
    return {
        Metric.views: "delta_views_count",
        Metric.likes: "delta_likes_count",
        Metric.comments: "delta_comments_count",
        Metric.reports: "delta_reports_count",
    }[metric]


async def execute_dsl(dsl: QueryDSL) -> int:
    """Принимает QueryDSL; возвращает числовой результат запроса (0 при ошибке/неполной DSL)."""
    try:
        if dsl.aggregation == Aggregation.count_videos:
            where: list[str] = []
            params: dict[str, object] = {}

            if dsl.creator_id:
                where.append("creator_id = :creator_id")
                params["creator_id"] = dsl.creator_id

            if dsl.published_from:
                where.append("video_created_at >= :published_from")
                params["published_from"] = dsl.published_from

            if dsl.published_to:
                where.append("video_created_at < :published_to")
                params["published_to"] = dsl.published_to

            if dsl.threshold:
                col = _metric_column(dsl.threshold.metric)
                op = dsl.threshold.op
                if op not in {"gt", "gte", "lt", "lte"}:
                    op = "gt"
                sql_op = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[op]
                where.append(f"{col} {sql_op} :threshold_value")
                params["threshold_value"] = dsl.threshold.value

            stmt = "SELECT count(*) FROM videos"
            if where:
                stmt += " WHERE " + " AND ".join(where)

            result = await fetch_scalar(stmt, params)
            logger.info("dsl_ok", extra={"aggregation": dsl.aggregation.value, "result": result})
            return result

        if dsl.aggregation == Aggregation.sum_final:
            metric = dsl.metric or Metric.views
            col = _metric_column(metric)
            stmt = f"SELECT COALESCE(sum({col}), 0) FROM videos"
            result = await fetch_scalar(stmt)
            logger.info("dsl_ok", extra={"aggregation": dsl.aggregation.value, "result": result})
            return result

        if dsl.aggregation == Aggregation.sum_delta:
            metric = dsl.metric or Metric.views
            if dsl.day is None:
                return 0
            start = datetime(dsl.day.year, dsl.day.month, dsl.day.day, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            col = _delta_metric_column(metric)
            stmt = (
                f"SELECT COALESCE(sum({col}), 0) "
                "FROM video_snapshots "
                "WHERE created_at >= :start AND created_at < :end"
            )
            result = await fetch_scalar(stmt, {"start": start, "end": end})
            logger.info("dsl_ok", extra={"aggregation": dsl.aggregation.value, "result": result})
            return result

        if dsl.aggregation == Aggregation.count_distinct_videos_with_delta_gt0:
            metric = dsl.metric or Metric.views
            if dsl.day is None:
                return 0
            start = datetime(dsl.day.year, dsl.day.month, dsl.day.day, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            col = _delta_metric_column(metric)
            stmt = (
                "SELECT count(DISTINCT video_id) "
                "FROM video_snapshots "
                f"WHERE created_at >= :start AND created_at < :end AND {col} > 0"
            )
            result = await fetch_scalar(stmt, {"start": start, "end": end})
            logger.info("dsl_ok", extra={"aggregation": dsl.aggregation.value, "result": result})
            return result

        logger.info("dsl_unknown", extra={"aggregation": dsl.aggregation.value})
        return 0
    except Exception as exc:
        logger.warning("dsl_failed", extra={"error": str(exc), "aggregation": dsl.aggregation.value})
        return 0
