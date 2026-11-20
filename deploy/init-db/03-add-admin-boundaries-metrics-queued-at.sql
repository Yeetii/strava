-- Idempotent migration for databases created before the polling dedup marker existed.
-- Fresh installs get the column from 01-init.sql; migrate.sh applies this file to
-- existing volumes before the workers start.
ALTER TABLE admin_boundaries ADD COLUMN IF NOT EXISTS metrics_queued_at TIMESTAMPTZ;
