-- Resumable simulation runs: fenced leases, per-replication progress, result versions.
-- Run metadata may already have been created by SQLAlchemy metadata; these
-- statements are idempotent so a reviewed upgrade can be applied to an existing DB.

ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS lease_owner VARCHAR NOT NULL DEFAULT '';
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS lease_token VARCHAR NOT NULL DEFAULT '';
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS lease_expires_at DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS progress_completed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS progress_total INTEGER NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS progress_updated_at VARCHAR NOT NULL DEFAULT '';
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS started_at VARCHAR NOT NULL DEFAULT '';
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS updated_at VARCHAR NOT NULL DEFAULT '';
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS completed_at VARCHAR NOT NULL DEFAULT '';
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS result_version VARCHAR NOT NULL DEFAULT '';
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS artifact_path VARCHAR NOT NULL DEFAULT '';
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS input_hash_fingerprint VARCHAR NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS run_replications (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES simulation_runs(id) ON DELETE CASCADE,
    replication INTEGER NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    lease_token VARCHAR(64) NOT NULL DEFAULT '',
    summary JSON NOT NULL,
    sample_customers JSON NOT NULL DEFAULT '[]',
    created_at VARCHAR NOT NULL,
    CONSTRAINT run_replication_unique UNIQUE (run_id, replication)
);

CREATE TABLE IF NOT EXISTS run_events (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES simulation_runs(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL DEFAULT 0,
    event VARCHAR(40) NOT NULL,
    actor VARCHAR NOT NULL DEFAULT '',
    detail VARCHAR NOT NULL DEFAULT '',
    created_at VARCHAR NOT NULL
);

CREATE INDEX IF NOT EXISTS run_event_run_idx ON run_events (run_id, id);
CREATE INDEX IF NOT EXISTS run_replication_run_idx ON run_replications (run_id, replication);
CREATE INDEX IF NOT EXISTS run_reclaim_idx ON simulation_runs (status, lease_expires_at);
CREATE INDEX IF NOT EXISTS run_input_fingerprint_idx ON simulation_runs (input_hash_fingerprint);
-- Only one active run per identical input may be queued/running at a time;
-- completed/failed runs are kept as separate archives.
CREATE UNIQUE INDEX IF NOT EXISTS run_active_input_unique ON simulation_runs (input_hash_fingerprint) WHERE input_hash_fingerprint<>'' AND status IN ('queued','running');

-- Old completed runs keep their result and remain viewable/exportable, but the
-- versioned file name is only produced for new completions.
