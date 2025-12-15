CREATE TABLE IF NOT EXISTS videos (
  id uuid PRIMARY KEY,
  creator_id text NOT NULL,
  video_created_at timestamptz NOT NULL,
  views_count bigint NOT NULL,
  likes_count bigint NOT NULL,
  comments_count bigint NOT NULL,
  reports_count bigint NOT NULL,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS video_snapshots (
  id text PRIMARY KEY,
  video_id uuid NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
  views_count bigint NOT NULL,
  likes_count bigint NOT NULL,
  comments_count bigint NOT NULL,
  reports_count bigint NOT NULL,
  delta_views_count bigint NOT NULL,
  delta_likes_count bigint NOT NULL,
  delta_comments_count bigint NOT NULL,
  delta_reports_count bigint NOT NULL,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_videos_creator_id ON videos(creator_id);
CREATE INDEX IF NOT EXISTS ix_videos_video_created_at ON videos(video_created_at);
CREATE INDEX IF NOT EXISTS ix_videos_views_count ON videos(views_count);
CREATE INDEX IF NOT EXISTS ix_snapshots_created_at ON video_snapshots(created_at);
CREATE INDEX IF NOT EXISTS ix_snapshots_video_id_created_at ON video_snapshots(video_id, created_at);
