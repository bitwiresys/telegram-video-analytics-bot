from __future__ import annotations

from datetime import date, datetime
from enum import Enum

from pydantic import BaseModel


class Metric(str, Enum):
    views = "views"
    likes = "likes"
    comments = "comments"
    reports = "reports"


class Aggregation(str, Enum):
    count_videos = "count_videos"
    sum_final = "sum_final"
    sum_delta = "sum_delta"
    count_distinct_videos_with_delta_gt0 = "count_distinct_videos_with_delta_gt0"
    count_snapshots_with_delta_lt0 = "count_snapshots_with_delta_lt0"


class Threshold(BaseModel):
    metric: Metric
    op: str
    value: int


class QueryDSL(BaseModel):
    aggregation: Aggregation
    metric: Metric | None = None

    creator_id: str | None = None

    published_from: datetime | None = None
    published_to: datetime | None = None

    day: date | None = None

    threshold: Threshold | None = None
