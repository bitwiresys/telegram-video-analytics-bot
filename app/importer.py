from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from uuid import UUID

import orjson
from sqlalchemy import text

from app.db import get_engine

logger = logging.getLogger(__name__)


def _dt(s: str) -> datetime:
    """Принимает строку ISO8601; возвращает datetime."""
    return datetime.fromisoformat(s)


async def ensure_imported(json_path: Path, batch_size: int = 500) -> bool:
    """Принимает путь к JSON и batch_size; возвращает True если импорт выполнен, иначе False."""
    engine = get_engine()
    expected_videos: int | None = None
    expected_snapshots: int | None = None
    try:
        data = orjson.loads(json_path.read_bytes())
        videos = data.get("videos")
        if isinstance(videos, list):
            expected_videos = 0
            expected_snapshots = 0
            for v in videos:
                if not isinstance(v, dict):
                    continue
                try:
                    video_id = UUID(str(v.get("id")))
                except Exception:
                    continue
                expected_videos += 1
                snaps = v.get("snapshots")
                if isinstance(snaps, list):
                    for s in snaps:
                        if isinstance(s, dict) and s.get("id") is not None:
                            expected_snapshots += 1
                _ = video_id
    except Exception as exc:
        logger.warning("import_expected_failed", extra={"error": str(exc), "path": str(json_path)})

    try:
        async with engine.connect() as conn:
            videos_res = await conn.execute(text("SELECT count(*) FROM videos"))
            snapshots_res = await conn.execute(text("SELECT count(*) FROM video_snapshots"))
            videos_count = int(videos_res.scalar_one_or_none() or 0)
            snapshots_count = int(snapshots_res.scalar_one_or_none() or 0)
    except Exception as exc:
        logger.warning("import_check_failed", extra={"error": str(exc)})
        await import_videos(json_path, batch_size=batch_size)
        return True

    if expected_videos is not None and expected_snapshots is not None:
        if videos_count >= expected_videos and snapshots_count >= expected_snapshots:
            logger.info(
                "import_skip",
                extra={
                    "videos": videos_count,
                    "snapshots": snapshots_count,
                    "expected_videos": expected_videos,
                    "expected_snapshots": expected_snapshots,
                    "path": str(json_path),
                },
            )
            return False
        logger.info(
            "import_needed",
            extra={
                "videos": videos_count,
                "snapshots": snapshots_count,
                "expected_videos": expected_videos,
                "expected_snapshots": expected_snapshots,
                "path": str(json_path),
            },
        )
        await import_videos(json_path, batch_size=batch_size)
        return True

    if videos_count > 0 and snapshots_count > 0:
        logger.info(
            "import_skip",
            extra={"videos": videos_count, "snapshots": snapshots_count, "path": str(json_path)},
        )
        return False

    logger.info(
        "import_needed",
        extra={"videos": videos_count, "snapshots": snapshots_count, "path": str(json_path)},
    )
    await import_videos(json_path, batch_size=batch_size)
    return True


async def import_videos(json_path: Path, batch_size: int = 500) -> None:
    """Принимает путь к JSON и batch_size; возвращает None после UPSERT-импорта videos и snapshots."""
    logger.info("import_start", extra={"path": str(json_path), "batch_size": batch_size})
    data = orjson.loads(json_path.read_bytes())
    videos = data.get("videos")
    if not isinstance(videos, list):
        logger.info("import_no_videos", extra={"path": str(json_path)})
        return

    engine = get_engine()

    video_stmt = text(
        """
        INSERT INTO videos (
          id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, created_at, updated_at
        )
        VALUES (
          :id, :creator_id, :video_created_at, :views_count, :likes_count, :comments_count, :reports_count, :created_at, :updated_at
        )
        ON CONFLICT (id) DO UPDATE SET
          creator_id = EXCLUDED.creator_id,
          video_created_at = EXCLUDED.video_created_at,
          views_count = EXCLUDED.views_count,
          likes_count = EXCLUDED.likes_count,
          comments_count = EXCLUDED.comments_count,
          reports_count = EXCLUDED.reports_count,
          created_at = EXCLUDED.created_at,
          updated_at = EXCLUDED.updated_at
        """
    )

    snapshot_stmt = text(
        """
        INSERT INTO video_snapshots (
          id, video_id, views_count, likes_count, comments_count, reports_count,
          delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
          created_at, updated_at
        )
        VALUES (
          :id, :video_id, :views_count, :likes_count, :comments_count, :reports_count,
          :delta_views_count, :delta_likes_count, :delta_comments_count, :delta_reports_count,
          :created_at, :updated_at
        )
        ON CONFLICT (id) DO UPDATE SET
          video_id = EXCLUDED.video_id,
          views_count = EXCLUDED.views_count,
          likes_count = EXCLUDED.likes_count,
          comments_count = EXCLUDED.comments_count,
          reports_count = EXCLUDED.reports_count,
          delta_views_count = EXCLUDED.delta_views_count,
          delta_likes_count = EXCLUDED.delta_likes_count,
          delta_comments_count = EXCLUDED.delta_comments_count,
          delta_reports_count = EXCLUDED.delta_reports_count,
          created_at = EXCLUDED.created_at,
          updated_at = EXCLUDED.updated_at
        """
    )

    video_batch: list[dict[str, object]] = []
    snapshot_batch: list[dict[str, object]] = []

    inserted_videos = 0
    inserted_snapshots = 0

    async with engine.begin() as conn:
        for v in videos:
            if not isinstance(v, dict):
                continue
            try:
                video_id = UUID(str(v.get("id")))
            except Exception:
                continue
            video_batch.append(
                {
                    "id": video_id,
                    "creator_id": str(v.get("creator_id")),
                    "video_created_at": _dt(str(v.get("video_created_at"))),
                    "views_count": int(v.get("views_count") or 0),
                    "likes_count": int(v.get("likes_count") or 0),
                    "comments_count": int(v.get("comments_count") or 0),
                    "reports_count": int(v.get("reports_count") or 0),
                    "created_at": _dt(str(v.get("created_at"))),
                    "updated_at": _dt(str(v.get("updated_at"))),
                }
            )

            snaps = v.get("snapshots")
            if isinstance(snaps, list):
                for s in snaps:
                    if not isinstance(s, dict):
                        continue
                    snapshot_batch.append(
                        {
                            "id": str(s.get("id")),
                            "video_id": video_id,
                            "views_count": int(s.get("views_count") or 0),
                            "likes_count": int(s.get("likes_count") or 0),
                            "comments_count": int(s.get("comments_count") or 0),
                            "reports_count": int(s.get("reports_count") or 0),
                            "delta_views_count": int(s.get("delta_views_count") or 0),
                            "delta_likes_count": int(s.get("delta_likes_count") or 0),
                            "delta_comments_count": int(s.get("delta_comments_count") or 0),
                            "delta_reports_count": int(s.get("delta_reports_count") or 0),
                            "created_at": _dt(str(s.get("created_at"))),
                            "updated_at": _dt(str(s.get("updated_at"))),
                        }
                    )

            if len(video_batch) >= batch_size:
                await conn.execute(video_stmt, video_batch)
                inserted_videos += len(video_batch)
                video_batch.clear()

            if len(snapshot_batch) >= batch_size * 20:
                if video_batch:
                    await conn.execute(video_stmt, video_batch)
                    inserted_videos += len(video_batch)
                    video_batch.clear()
                await conn.execute(snapshot_stmt, snapshot_batch)
                inserted_snapshots += len(snapshot_batch)
                snapshot_batch.clear()

        if video_batch:
            await conn.execute(video_stmt, video_batch)
            inserted_videos += len(video_batch)

        if snapshot_batch:
            await conn.execute(snapshot_stmt, snapshot_batch)
            inserted_snapshots += len(snapshot_batch)

    logger.info("import_done", extra={"videos": inserted_videos, "snapshots": inserted_snapshots})
