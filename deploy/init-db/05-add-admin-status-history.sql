-- Periodic snapshots of the admin dashboard metrics, used for trend charts.
-- One row per metric per snapshot; snapshots are throttled by the caller.
CREATE TABLE IF NOT EXISTS admin_status_history (
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metric_type   TEXT NOT NULL,
    metric_name   TEXT NOT NULL,
    value         BIGINT NOT NULL,
    PRIMARY KEY (recorded_at, metric_type, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_admin_status_history_lookup
    ON admin_status_history (metric_type, metric_name, recorded_at DESC);
