-- Idempotent migration for existing databases. Fresh installs get these from 01-init.sql.
-- Epoch 1 means every row whose stored stage epoch is 0/null is rediscovered on the next poll.
CREATE TABLE IF NOT EXISTS job_epochs (
    job_type   TEXT PRIMARY KEY,
    epoch      BIGINT NOT NULL DEFAULT 1,
    bumped_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO job_epochs (job_type, epoch) VALUES
    ('activity.summits', 1),
    ('activity.visitedPaths', 1),
    ('activity.visitedAreas', 1),
    ('adminBoundary.metrics', 1)
ON CONFLICT (job_type) DO NOTHING;

ALTER TABLE admin_boundaries ADD COLUMN IF NOT EXISTS metrics_generation BIGINT NOT NULL DEFAULT 0;
