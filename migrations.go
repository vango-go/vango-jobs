package jobs

import (
	"context"
	"strings"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS vango_jobs (
    id                  text PRIMARY KEY,
    name                text NOT NULL,
    queue               text NOT NULL,
    status              text NOT NULL,

    tenant_id           text NULL,
    actor_id            text NULL,
    actor_kind          text NULL,

    parent_job_id       text NULL REFERENCES vango_jobs(id) ON DELETE SET NULL,
    retried_from_job_id text NULL REFERENCES vango_jobs(id) ON DELETE SET NULL,
    unique_key          text NULL,
    concurrency_key     text NULL,
    trace_id            text NULL,

    priority            integer NOT NULL DEFAULT 100,
    attempts            integer NOT NULL DEFAULT 0,
    max_attempts        integer NOT NULL,
    timeout_seconds     integer NOT NULL,

    run_at              timestamptz NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    first_started_at    timestamptz NULL,
    last_started_at     timestamptz NULL,
    finished_at         timestamptz NULL,
    cancel_requested_at timestamptz NULL,

    lease_token         text NULL,
    leased_by           text NULL,
    lease_expires_at    timestamptz NULL,

    payload             jsonb NOT NULL,
    output              jsonb NULL,
    progress            jsonb NULL,
    metadata            jsonb NULL,
    tags                text[] NULL,

    safe_error          text NULL,
    error_code          text NULL,
    retain_until        timestamptz NULL
);

CREATE TABLE IF NOT EXISTS vango_job_attempts (
    id               text PRIMARY KEY,
    job_id           text NOT NULL REFERENCES vango_jobs(id) ON DELETE CASCADE,
    attempt          integer NOT NULL,
    worker_id        text NOT NULL,
    lease_token      text NOT NULL,
    status           text NOT NULL,
    started_at       timestamptz NOT NULL,
    ended_at         timestamptz NULL,
    duration_ms      bigint NULL,
    safe_error       text NULL,
    error_code       text NULL,
    retry_delay_ms   bigint NULL,
    created_at       timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_id, attempt)
);

CREATE TABLE IF NOT EXISTS vango_job_events (
    id          bigserial PRIMARY KEY,
    job_id      text NOT NULL REFERENCES vango_jobs(id) ON DELETE CASCADE,
    kind        text NOT NULL,
    at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    payload     jsonb NULL
);

CREATE TABLE IF NOT EXISTS vango_job_queues (
    queue         text PRIMARY KEY,
    paused        boolean NOT NULL DEFAULT false,
    pause_reason  text NULL,
    updated_at    timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS vango_job_concurrency (
    key              text NOT NULL,
    slot             integer NOT NULL,
    job_id           text NOT NULL REFERENCES vango_jobs(id) ON DELETE CASCADE,
    lease_token      text NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (key, slot),
    UNIQUE (job_id)
);

CREATE TABLE IF NOT EXISTS vango_job_schedules (
    name             text PRIMARY KEY,
    target_job_name  text NOT NULL,
    spec             text NOT NULL,
    timezone         text NOT NULL DEFAULT 'UTC',
    paused           boolean NOT NULL DEFAULT false,
    next_run_at      timestamptz NOT NULL,
    last_run_at      timestamptz NULL,
    last_job_id      text NULL REFERENCES vango_jobs(id) ON DELETE SET NULL,
    updated_at       timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS vango_jobs_claim_idx
    ON vango_jobs (queue, status, run_at, priority DESC, created_at)
    WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS vango_jobs_running_lease_idx
    ON vango_jobs (status, lease_expires_at)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS vango_jobs_tenant_idx
    ON vango_jobs (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS vango_jobs_parent_idx
    ON vango_jobs (parent_job_id);

CREATE INDEX IF NOT EXISTS vango_jobs_retried_from_idx
    ON vango_jobs (retried_from_job_id);

CREATE UNIQUE INDEX IF NOT EXISTS vango_jobs_unique_active_idx
    ON vango_jobs (name, unique_key)
    WHERE unique_key IS NOT NULL AND status IN ('queued', 'running');

CREATE INDEX IF NOT EXISTS vango_jobs_terminal_finished_idx
    ON vango_jobs (status, finished_at DESC, id DESC)
    WHERE status IN ('succeeded', 'failed', 'canceled');

CREATE INDEX IF NOT EXISTS vango_job_attempts_job_idx
    ON vango_job_attempts (job_id, attempt DESC);

CREATE INDEX IF NOT EXISTS vango_job_events_job_idx
    ON vango_job_events (job_id, id DESC);

CREATE INDEX IF NOT EXISTS vango_job_concurrency_lease_idx
    ON vango_job_concurrency (lease_expires_at);
`

// Migrate creates or updates the required PostgreSQL tables and indexes.
func Migrate(ctx context.Context, db DB) error {
	statements := strings.Split(schemaSQL, ";\n")
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}
