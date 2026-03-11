package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type postgresStore struct {
	db  DB
	cfg Config
	ids *idGenerator
}

func newPostgresStore(db DB, opts ...RuntimeOption) *postgresStore {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return &postgresStore{db: db, cfg: cfg, ids: newIDGenerator()}
}

func (s *postgresStore) enqueue(ctx context.Context, req enqueueRequest) (Ref, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Ref{}, err
	}
	defer tx.Rollback(ctx)
	ref, err := s.enqueueTx(ctx, tx, req)
	if err != nil {
		return Ref{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Ref{}, err
	}
	return ref, nil
}

func (s *postgresStore) enqueueTx(ctx context.Context, tx pgx.Tx, req enqueueRequest) (Ref, error) {
	inserted, ref, err := s.insertJob(ctx, tx, req, "")
	if err != nil {
		return Ref{}, err
	}
	if inserted {
		if err := s.notifyWakeup(ctx, tx, req.Queue); err != nil {
			return Ref{}, err
		}
	}
	return ref, nil
}

func (s *postgresStore) inspect(ctx context.Context, id string) (*RawSnapshot, error) {
	row := s.db.QueryRow(ctx, `
SELECT id, name, queue, status,
       COALESCE(tenant_id, ''), COALESCE(actor_id, ''), COALESCE(actor_kind, ''),
       attempts, max_attempts, run_at, created_at, updated_at,
       first_started_at, last_started_at, finished_at, cancel_requested_at,
       lease_expires_at, COALESCE(leased_by, ''), priority,
       COALESCE(unique_key, ''), COALESCE(concurrency_key, ''),
       COALESCE(parent_job_id, ''), COALESCE(retried_from_job_id, ''),
       COALESCE(trace_id, ''), COALESCE(tags, ARRAY[]::text[]),
       payload, output, progress, metadata, COALESCE(safe_error, ''), COALESCE(error_code, '')
FROM vango_jobs
WHERE id = $1
`, id)
	snap, err := scanRawSnapshot(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return snap, nil
}

func (s *postgresStore) list(ctx context.Context, filter ListFilter, scope authScope) ([]*RawSnapshot, error) {
	statuses := make([]string, 0, len(filter.Statuses))
	for _, status := range filter.Statuses {
		statuses = append(statuses, status.String())
	}
	query := fmt.Sprintf(`
SELECT id, name, queue, status,
       COALESCE(tenant_id, ''), COALESCE(actor_id, ''), COALESCE(actor_kind, ''),
       attempts, max_attempts, run_at, created_at, updated_at,
       first_started_at, last_started_at, finished_at, cancel_requested_at,
       lease_expires_at, COALESCE(leased_by, ''), priority,
       COALESCE(unique_key, ''), COALESCE(concurrency_key, ''),
       COALESCE(parent_job_id, ''), COALESCE(retried_from_job_id, ''),
       COALESCE(trace_id, ''), COALESCE(tags, ARRAY[]::text[]),
       payload, output, progress, metadata, COALESCE(safe_error, ''), COALESCE(error_code, '')
FROM vango_jobs
WHERE ($1::boolean = true OR status = ANY($2))
  AND ($3 = '' OR queue = $3)
  AND ($4 = '' OR name = $4)
  AND ($5 = '' OR tenant_id = $5)
  AND ($6::timestamptz IS NULL OR created_at >= $6)
  AND ($7::timestamptz IS NULL OR created_at <= $7)
  AND (
      $8::boolean = true
      OR ($9 <> '' AND tenant_id = $9)
      OR ($10 <> '' AND actor_id = $10 AND actor_kind = $11)
  )
ORDER BY %s
LIMIT $12
`, listOrderClause(filter.Order))
	rows, err := s.db.Query(ctx, query, len(statuses) == 0, statuses, filter.Queue, filter.JobName, filter.TenantID, filter.Since, filter.Until, scope.admin, scope.tenantID, scope.actor.ID, scope.actor.Kind, normalizeListLimit(filter.Limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]*RawSnapshot, 0)
	for rows.Next() {
		snap, err := scanRawSnapshot(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func listOrderClause(order ListOrder) string {
	switch normalizeListOrder(order) {
	case ListOrderTerminalTimeDesc:
		return "COALESCE(finished_at, updated_at, created_at) DESC, updated_at DESC, created_at DESC, id DESC"
	default:
		return "created_at DESC, id DESC"
	}
}

func (s *postgresStore) attempts(ctx context.Context, id string) ([]AttemptSnapshot, error) {
	rows, err := s.db.Query(ctx, `
SELECT id, job_id, attempt, worker_id, lease_token, status, started_at, ended_at, duration_ms,
       COALESCE(safe_error, ''), COALESCE(error_code, ''), retry_delay_ms, created_at
FROM vango_job_attempts
WHERE job_id = $1
ORDER BY attempt DESC
`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]AttemptSnapshot, 0)
	for rows.Next() {
		var snapshot AttemptSnapshot
		var durationMS *int64
		var retryDelayMS *int64
		if err := rows.Scan(
			&snapshot.ID,
			&snapshot.JobID,
			&snapshot.Attempt,
			&snapshot.WorkerID,
			&snapshot.LeaseToken,
			&snapshot.Status,
			&snapshot.StartedAt,
			&snapshot.EndedAt,
			&durationMS,
			&snapshot.SafeError,
			&snapshot.ErrorCode,
			&retryDelayMS,
			&snapshot.CreatedAt,
		); err != nil {
			return nil, err
		}
		if durationMS != nil {
			snapshot.Duration = time.Duration(*durationMS) * time.Millisecond
		}
		if retryDelayMS != nil {
			snapshot.RetryDelay = time.Duration(*retryDelayMS) * time.Millisecond
		}
		out = append(out, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *postgresStore) events(ctx context.Context, id string) ([]EventSnapshot, error) {
	rows, err := s.db.Query(ctx, `
SELECT id, job_id, kind, at, payload
FROM vango_job_events
WHERE job_id = $1
ORDER BY id DESC
`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]EventSnapshot, 0)
	for rows.Next() {
		var snapshot EventSnapshot
		if err := rows.Scan(&snapshot.ID, &snapshot.JobID, &snapshot.Kind, &snapshot.At, &snapshot.Payload); err != nil {
			return nil, err
		}
		snapshot.Payload = append([]byte(nil), snapshot.Payload...)
		out = append(out, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type rawSnapshotScanner interface {
	Scan(dest ...any) error
}

func scanRawSnapshot(scanner rawSnapshotScanner) (*RawSnapshot, error) {
	var snap RawSnapshot
	var actorID, actorKind string
	var tags []string
	var progressBytes []byte
	var metadataBytes []byte
	if err := scanner.Scan(
		&snap.Ref.ID,
		&snap.Ref.Name,
		&snap.Queue,
		&snap.Status,
		&snap.TenantID,
		&actorID,
		&actorKind,
		&snap.Attempts,
		&snap.MaxAttempts,
		&snap.RunAt,
		&snap.CreatedAt,
		&snap.UpdatedAt,
		&snap.FirstStartedAt,
		&snap.LastStartedAt,
		&snap.FinishedAt,
		&snap.CancelRequestedAt,
		&snap.LeaseExpiresAt,
		&snap.WorkerID,
		&snap.Priority,
		&snap.UniqueKey,
		&snap.ConcurrencyKey,
		&snap.ParentJobID,
		&snap.RetriedFromJobID,
		&snap.TraceID,
		&tags,
		&snap.Payload,
		&snap.Output,
		&progressBytes,
		&metadataBytes,
		&snap.SafeError,
		&snap.ErrorCode,
	); err != nil {
		return nil, err
	}
	snap.Actor = Actor{ID: actorID, Kind: actorKind}
	snap.Tags = copyTags(tags)
	if len(metadataBytes) > 0 {
		_ = json.Unmarshal(metadataBytes, &snap.Metadata)
	}
	if len(progressBytes) > 0 && string(progressBytes) != "null" {
		var progress Progress
		if err := json.Unmarshal(progressBytes, &progress); err != nil {
			return nil, err
		}
		snap.Progress = &progress
	}
	return snap.Clone(), nil
}

func (s *postgresStore) cancel(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var status Status
	if err := tx.QueryRow(ctx, `SELECT status FROM vango_jobs WHERE id = $1 FOR UPDATE`, id).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}

	switch status {
	case StatusQueued:
		if _, err := tx.Exec(ctx, `
UPDATE vango_jobs
SET status = 'canceled',
    cancel_requested_at = COALESCE(cancel_requested_at, CURRENT_TIMESTAMP),
    finished_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP,
    retain_until = CURRENT_TIMESTAMP + ($2 * INTERVAL '1 second')
WHERE id = $1
`, id, int64(s.cfg.CanceledRetention/time.Second)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO vango_job_events (job_id, kind) VALUES ($1, 'canceled')`, id); err != nil {
			return err
		}
	case StatusRunning:
		if _, err := tx.Exec(ctx, `
UPDATE vango_jobs
SET cancel_requested_at = COALESCE(cancel_requested_at, CURRENT_TIMESTAMP),
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
`, id); err != nil {
			return err
		}
	default:
		return nil
	}

	return tx.Commit(ctx)
}

func (s *postgresStore) retry(ctx context.Context, id string, req retryCloneRequest) (Ref, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Ref{}, err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
SELECT id, name, queue, status, COALESCE(tenant_id, ''), COALESCE(actor_id, ''), COALESCE(actor_kind, ''),
       COALESCE(parent_job_id, ''), COALESCE(unique_key, ''), COALESCE(concurrency_key, ''),
       priority, max_attempts, timeout_seconds, payload, metadata, COALESCE(tags, ARRAY[]::text[]), COALESCE(trace_id, '')
FROM vango_jobs
WHERE id = $1
FOR UPDATE
`, id)
	var snap enqueueRequest
	var status Status
	var actorID, actorKind string
	var timeoutSeconds int
	var metadataBytes []byte
	var tags []string
	if err := row.Scan(
		&snap.ID,
		&snap.Name,
		&snap.Queue,
		&status,
		&snap.TenantID,
		&actorID,
		&actorKind,
		&snap.ParentJobID,
		&snap.UniqueKey,
		&snap.ConcurrencyKey,
		&snap.Priority,
		&snap.MaxAttempts,
		&timeoutSeconds,
		&snap.Payload,
		&metadataBytes,
		&tags,
		&snap.TraceID,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Ref{}, ErrNotFound
		}
		return Ref{}, err
	}
	if status != StatusFailed && status != StatusCanceled {
		return Ref{}, fmt.Errorf("jobs: retry requires failed or canceled job, got %s", status)
	}
	snap.Actor = Actor{ID: actorID, Kind: actorKind}
	snap.Timeout = time.Duration(timeoutSeconds) * time.Second
	snap.Tags = copyTags(tags)
	if len(metadataBytes) > 0 && string(metadataBytes) != "null" {
		if err := json.Unmarshal(metadataBytes, &snap.Metadata); err != nil {
			return Ref{}, err
		}
	}
	snap.ID = req.NewJobID(req.Now)
	if req.OverrideQueue != "" {
		snap.Queue = req.OverrideQueue
	}
	if req.OverrideRunAt != nil {
		snap.RunAt = req.OverrideRunAt.UTC()
	} else if req.OverrideDelay != nil {
		snap.RunAt = req.Now.Add(*req.OverrideDelay).UTC()
	} else {
		snap.RunAt = req.Now
	}
	if !req.PreserveUnique {
		snap.UniqueKey = ""
	}
	inserted, ref, err := s.insertJob(ctx, tx, snap, id)
	if err != nil {
		return Ref{}, err
	}
	if !inserted {
		return Ref{}, ErrDuplicateActiveJob
	}
	if err := s.notifyWakeup(ctx, tx, snap.Queue); err != nil {
		return Ref{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Ref{}, err
	}
	return ref, nil
}

func (s *postgresStore) pauseQueue(ctx context.Context, queue string, reason string) error {
	_, err := s.db.Exec(ctx, `
INSERT INTO vango_job_queues (queue, paused, pause_reason, updated_at)
VALUES ($1, true, NULLIF($2, ''), CURRENT_TIMESTAMP)
ON CONFLICT (queue) DO UPDATE
SET paused = true,
    pause_reason = EXCLUDED.pause_reason,
    updated_at = CURRENT_TIMESTAMP
`, queue, reason)
	return err
}

func (s *postgresStore) resumeQueue(ctx context.Context, queue string) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
INSERT INTO vango_job_queues (queue, paused, pause_reason, updated_at)
VALUES ($1, false, NULL, CURRENT_TIMESTAMP)
ON CONFLICT (queue) DO UPDATE
SET paused = false,
    pause_reason = NULL,
    updated_at = CURRENT_TIMESTAMP
`, queue); err != nil {
		return err
	}
	if err := s.notifyWakeup(ctx, tx, queue); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *postgresStore) queue(ctx context.Context, name string) (*QueueSnapshot, error) {
	row := s.db.QueryRow(ctx, `
WITH stats AS (
    SELECT queue,
           COUNT(*) FILTER (WHERE status = 'queued') AS queued,
           COUNT(*) FILTER (WHERE status = 'running') AS running
    FROM vango_jobs
    WHERE queue = $1
    GROUP BY queue
)
SELECT q.queue,
       q.paused,
       COALESCE(q.pause_reason, ''),
       q.updated_at,
       COALESCE(s.queued, 0),
       COALESCE(s.running, 0)
FROM vango_job_queues q
LEFT JOIN stats s ON s.queue = q.queue
WHERE q.queue = $1
`, name)
	var snapshot QueueSnapshot
	if err := row.Scan(
		&snapshot.Queue,
		&snapshot.Paused,
		&snapshot.PauseReason,
		&snapshot.UpdatedAt,
		&snapshot.Queued,
		&snapshot.Running,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &snapshot, nil
}

func (s *postgresStore) queues(ctx context.Context) ([]QueueSnapshot, error) {
	rows, err := s.db.Query(ctx, `
WITH stats AS (
    SELECT queue,
           COUNT(*) FILTER (WHERE status = 'queued') AS queued,
           COUNT(*) FILTER (WHERE status = 'running') AS running
    FROM vango_jobs
    GROUP BY queue
)
SELECT q.queue,
       q.paused,
       COALESCE(q.pause_reason, ''),
       q.updated_at,
       COALESCE(s.queued, 0),
       COALESCE(s.running, 0)
FROM vango_job_queues q
LEFT JOIN stats s ON s.queue = q.queue
ORDER BY q.queue ASC
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]QueueSnapshot, 0)
	for rows.Next() {
		var snapshot QueueSnapshot
		if err := rows.Scan(
			&snapshot.Queue,
			&snapshot.Paused,
			&snapshot.PauseReason,
			&snapshot.UpdatedAt,
			&snapshot.Queued,
			&snapshot.Running,
		); err != nil {
			return nil, err
		}
		out = append(out, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *postgresStore) replay(ctx context.Context, filter ReplayFilter, req retryCloneRequest) (*ReplaySummary, error) {
	if !req.Scope.admin && req.Scope.tenantID == "" && req.Scope.actor.Empty() {
		return nil, ErrUnauthorized
	}
	statuses := make([]string, 0, len(filter.Statuses))
	if len(filter.Statuses) == 0 {
		statuses = []string{string(StatusFailed), string(StatusCanceled)}
	} else {
		for _, status := range filter.Statuses {
			statuses = append(statuses, status.String())
		}
	}
	limit := filter.Limit
	if limit <= 0 || limit > req.MaxBatch {
		limit = req.MaxBatch
	}

	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
SELECT id, name, queue, COALESCE(tenant_id, ''), COALESCE(actor_id, ''), COALESCE(actor_kind, ''),
       COALESCE(parent_job_id, ''), COALESCE(unique_key, ''), COALESCE(concurrency_key, ''),
       priority, max_attempts, timeout_seconds, payload, metadata, COALESCE(tags, ARRAY[]::text[]), COALESCE(trace_id, '')
FROM vango_jobs
WHERE status = ANY($1)
  AND ($2 = '' OR queue = $2)
  AND ($3 = '' OR name = $3)
  AND ($4 = '' OR tenant_id = $4)
  AND ($5::timestamptz IS NULL OR created_at >= $5)
  AND ($6::timestamptz IS NULL OR created_at <= $6)
  AND (
      $7::boolean = true
      OR ($8 <> '' AND tenant_id = $8)
      OR ($9 <> '' AND actor_id = $9 AND actor_kind = $10)
  )
ORDER BY created_at ASC
LIMIT $11
FOR UPDATE SKIP LOCKED
`, statuses, filter.Queue, filter.JobName, filter.TenantID, filter.Since, filter.Until, req.Scope.admin, req.Scope.tenantID, req.Scope.actor.ID, req.Scope.actor.Kind, limit)
	if err != nil {
		return nil, err
	}

	type replayCandidate struct {
		sourceID       string
		name           string
		queue          string
		tenantID       string
		actorID        string
		actorKind      string
		parentJobID    string
		uniqueKey      string
		concurrencyKey string
		priority       int
		maxAttempts    int
		timeoutSeconds int
		payload        []byte
		metadataBytes  []byte
		tags           []string
		traceID        string
	}

	candidates := make([]replayCandidate, 0, limit)
	for rows.Next() {
		var candidate replayCandidate
		if err := rows.Scan(
			&candidate.sourceID,
			&candidate.name,
			&candidate.queue,
			&candidate.tenantID,
			&candidate.actorID,
			&candidate.actorKind,
			&candidate.parentJobID,
			&candidate.uniqueKey,
			&candidate.concurrencyKey,
			&candidate.priority,
			&candidate.maxAttempts,
			&candidate.timeoutSeconds,
			&candidate.payload,
			&candidate.metadataBytes,
			&candidate.tags,
			&candidate.traceID,
		); err != nil {
			rows.Close()
			return nil, err
		}
		candidate.payload = append([]byte(nil), candidate.payload...)
		candidate.metadataBytes = append([]byte(nil), candidate.metadataBytes...)
		candidate.tags = copyTags(candidate.tags)
		candidates = append(candidates, candidate)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	summary := &ReplaySummary{Selected: len(candidates)}
	for _, candidate := range candidates {
		snap := enqueueRequest{
			ID:             req.NewJobID(req.Now),
			Name:           candidate.name,
			Queue:          candidate.queue,
			TenantID:       candidate.tenantID,
			Actor:          Actor{ID: candidate.actorID, Kind: candidate.actorKind},
			ParentJobID:    candidate.parentJobID,
			UniqueKey:      candidate.uniqueKey,
			ConcurrencyKey: candidate.concurrencyKey,
			Priority:       candidate.priority,
			MaxAttempts:    candidate.maxAttempts,
			Timeout:        time.Duration(candidate.timeoutSeconds) * time.Second,
			Payload:        append([]byte(nil), candidate.payload...),
			Tags:           copyTags(candidate.tags),
			TraceID:        candidate.traceID,
		}
		if len(candidate.metadataBytes) > 0 && string(candidate.metadataBytes) != "null" {
			if err := json.Unmarshal(candidate.metadataBytes, &snap.Metadata); err != nil {
				return nil, err
			}
		}
		if req.OverrideQueue != "" {
			snap.Queue = req.OverrideQueue
		}
		if req.OverrideRunAt != nil {
			snap.RunAt = req.OverrideRunAt.UTC()
		} else if req.OverrideDelay != nil {
			snap.RunAt = req.Now.Add(*req.OverrideDelay).UTC()
		} else {
			snap.RunAt = req.Now
		}
		if !req.PreserveUnique {
			snap.UniqueKey = ""
		}
		inserted, ref, err := s.insertJob(ctx, tx, snap, candidate.sourceID)
		if err != nil {
			summary.Failed++
			continue
		}
		if !inserted {
			summary.Failed++
			continue
		}
		summary.Cloned++
		summary.Refs = append(summary.Refs, ref)
		if err := s.notifyWakeup(ctx, tx, snap.Queue); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return summary, nil
}

func (s *postgresStore) claim(ctx context.Context, req claimRequest) ([]claimedJob, error) {
	if req.Limit <= 0 || len(req.JobNames) == 0 {
		return nil, nil
	}
	overscan := req.OverscanLimit
	if overscan < req.Limit {
		overscan = req.Limit
	}

	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
SELECT j.id, j.name, j.queue,
       COALESCE(j.tenant_id, ''), COALESCE(j.actor_id, ''), COALESCE(j.actor_kind, ''),
       j.attempts, j.max_attempts, j.timeout_seconds, j.created_at, j.payload, j.metadata,
       COALESCE(j.parent_job_id, ''), COALESCE(j.concurrency_key, ''), COALESCE(j.unique_key, ''), COALESCE(j.trace_id, '')
FROM vango_jobs j
WHERE j.status = 'queued'
  AND j.run_at <= CURRENT_TIMESTAMP
  AND NOT EXISTS (
      SELECT 1
      FROM vango_job_queues q
      WHERE q.queue = j.queue AND q.paused = true
  )
  AND (COALESCE(array_length($1::text[], 1), 0) = 0 OR j.queue = ANY($1))
  AND j.name = ANY($2)
ORDER BY j.priority DESC, j.created_at ASC
LIMIT $3
FOR UPDATE SKIP LOCKED
`, req.Queues, req.JobNames, overscan)
	if err != nil {
		return nil, err
	}

	type claimCandidate struct {
		jobID          string
		jobName        string
		queue          string
		tenantID       string
		actorID        string
		actorKind      string
		attempts       int
		maxAttempts    int
		timeoutSeconds int
		createdAt      time.Time
		payload        []byte
		metadataBytes  []byte
		parentJobID    string
		concurrencyKey string
		uniqueKey      string
		traceID        string
	}

	candidates := make([]claimCandidate, 0, overscan)
	for rows.Next() {
		var candidate claimCandidate
		if err := rows.Scan(
			&candidate.jobID,
			&candidate.jobName,
			&candidate.queue,
			&candidate.tenantID,
			&candidate.actorID,
			&candidate.actorKind,
			&candidate.attempts,
			&candidate.maxAttempts,
			&candidate.timeoutSeconds,
			&candidate.createdAt,
			&candidate.payload,
			&candidate.metadataBytes,
			&candidate.parentJobID,
			&candidate.concurrencyKey,
			&candidate.uniqueKey,
			&candidate.traceID,
		); err != nil {
			rows.Close()
			return nil, err
		}
		candidate.payload = append([]byte(nil), candidate.payload...)
		candidate.metadataBytes = append([]byte(nil), candidate.metadataBytes...)
		candidates = append(candidates, candidate)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	claimed := make([]claimedJob, 0, req.Limit)
	for _, candidate := range candidates {
		if len(claimed) >= req.Limit {
			break
		}
		job := claimedJob{
			JobID:          candidate.jobID,
			JobName:        candidate.jobName,
			Queue:          candidate.queue,
			TraceID:        candidate.traceID,
			TenantID:       candidate.tenantID,
			Actor:          Actor{ID: candidate.actorID, Kind: candidate.actorKind},
			Attempts:       candidate.attempts,
			MaxAttempts:    candidate.maxAttempts,
			Timeout:        time.Duration(candidate.timeoutSeconds) * time.Second,
			CreatedAt:      candidate.createdAt,
			Payload:        append([]byte(nil), candidate.payload...),
			ParentJobID:    candidate.parentJobID,
			ConcurrencyKey: candidate.concurrencyKey,
			UniqueKey:      candidate.uniqueKey,
		}
		if len(candidate.metadataBytes) > 0 && string(candidate.metadataBytes) != "null" {
			if err := json.Unmarshal(candidate.metadataBytes, &job.Metadata); err != nil {
				return nil, err
			}
		}

		now := s.cfg.Now().UTC()
		leaseToken := s.ids.newLeaseToken(now)
		if job.ConcurrencyKey != "" {
			var slot int
			if err := tx.QueryRow(ctx, `
INSERT INTO vango_job_concurrency (key, slot, job_id, lease_token, lease_expires_at, updated_at)
VALUES ($1, 1, $2, $3, CURRENT_TIMESTAMP + ($4 * INTERVAL '1 millisecond'), CURRENT_TIMESTAMP)
ON CONFLICT (key, slot) DO UPDATE
SET job_id = EXCLUDED.job_id,
    lease_token = EXCLUDED.lease_token,
    lease_expires_at = EXCLUDED.lease_expires_at,
    updated_at = CURRENT_TIMESTAMP
WHERE vango_job_concurrency.lease_expires_at < CURRENT_TIMESTAMP
RETURNING slot
	`, job.ConcurrencyKey, job.JobID, leaseToken, milliseconds(req.LeaseDuration)).Scan(&slot); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					continue
				}
				return nil, err
			}
		}

		job.Attempts++
		job.LeaseToken = leaseToken
		job.AttemptStartedAt = now
		if _, err := tx.Exec(ctx, `
UPDATE vango_jobs
SET status = 'running',
    attempts = attempts + 1,
    first_started_at = COALESCE(first_started_at, CURRENT_TIMESTAMP),
    last_started_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP,
    lease_token = $2,
    leased_by = $3,
    lease_expires_at = CURRENT_TIMESTAMP + ($4 * INTERVAL '1 millisecond')
WHERE id = $1
	`, job.JobID, leaseToken, req.WorkerID, milliseconds(req.LeaseDuration)); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO vango_job_attempts (id, job_id, attempt, worker_id, lease_token, status, started_at)
VALUES ($1, $2, $3, $4, $5, 'running', CURRENT_TIMESTAMP)
	`, s.ids.newAttemptID(now), job.JobID, job.Attempts, req.WorkerID, leaseToken); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO vango_job_events (job_id, kind, payload)
VALUES ($1, 'started', jsonb_build_object('worker_id', $2::text, 'attempt', $3::integer))
`, job.JobID, req.WorkerID, job.Attempts); err != nil {
			return nil, err
		}
		claimed = append(claimed, job)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claimed, nil
}

func (s *postgresStore) heartbeat(ctx context.Context, req heartbeatRequest) (heartbeatResult, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return heartbeatResult{}, err
	}
	defer tx.Rollback(ctx)
	var result heartbeatResult
	if err := tx.QueryRow(ctx, `
UPDATE vango_jobs
SET lease_expires_at = CURRENT_TIMESTAMP + ($3 * INTERVAL '1 millisecond'),
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND status = 'running' AND lease_token = $2
RETURNING (cancel_requested_at IS NOT NULL)
`, req.JobID, req.LeaseToken, milliseconds(req.LeaseDuration)).Scan(&result.CancelRequested); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return heartbeatResult{LeaseHeld: false}, nil
		}
		return heartbeatResult{}, err
	}
	result.LeaseHeld = true
	if _, err := tx.Exec(ctx, `
UPDATE vango_job_concurrency
SET lease_expires_at = CURRENT_TIMESTAMP + ($3 * INTERVAL '1 millisecond'),
    updated_at = CURRENT_TIMESTAMP
WHERE job_id = $1 AND lease_token = $2
`, req.JobID, req.LeaseToken, milliseconds(req.LeaseDuration)); err != nil {
		return heartbeatResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return heartbeatResult{}, err
	}
	return result, nil
}

func (s *postgresStore) writeProgress(ctx context.Context, req progressWriteRequest) error {
	progressBytes, err := json.Marshal(req.Progress)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
UPDATE vango_jobs
SET progress = $3::jsonb,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND status = 'running' AND lease_token = $2
`, req.JobID, req.LeaseToken, string(progressBytes))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO vango_job_events (job_id, kind, payload)
VALUES ($1, 'progress', $2::jsonb)
`, req.JobID, string(progressBytes)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *postgresStore) complete(ctx context.Context, req completionRequest) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var progressArg any
	if req.Progress != nil {
		progressBytes, err := json.Marshal(req.Progress)
		if err != nil {
			return err
		}
		progressArg = string(progressBytes)
	}

	var affected int64
	switch req.Kind {
	case completionSuccess:
		tag, execErr := tx.Exec(ctx, `
UPDATE vango_jobs
SET status = 'succeeded',
    output = $3::jsonb,
    progress = COALESCE($4::jsonb, progress),
    safe_error = NULL,
    error_code = NULL,
    finished_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP,
    lease_token = NULL,
    leased_by = NULL,
    lease_expires_at = NULL,
    retain_until = CURRENT_TIMESTAMP + ($5 * INTERVAL '1 second')
WHERE id = $1 AND status = 'running' AND lease_token = $2
`, req.JobID, req.LeaseToken, string(req.Output), progressArg, int64(req.Retention/time.Second))
		err = execErr
		affected = tag.RowsAffected()
	case completionRetry:
		tag, execErr := tx.Exec(ctx, `
UPDATE vango_jobs
SET status = 'queued',
    run_at = CURRENT_TIMESTAMP + ($3 * INTERVAL '1 millisecond'),
    output = NULL,
    progress = COALESCE($4::jsonb, progress),
    safe_error = NULLIF($5, ''),
    error_code = NULLIF($6, ''),
    updated_at = CURRENT_TIMESTAMP,
    lease_token = NULL,
    leased_by = NULL,
    lease_expires_at = NULL
WHERE id = $1 AND status = 'running' AND lease_token = $2
`, req.JobID, req.LeaseToken, milliseconds(req.RetryDelay), progressArg, req.SafeError, req.ErrorCode)
		err = execErr
		affected = tag.RowsAffected()
	case completionFailure:
		tag, execErr := tx.Exec(ctx, `
UPDATE vango_jobs
SET status = 'failed',
    output = NULL,
    progress = COALESCE($3::jsonb, progress),
    safe_error = NULLIF($4, ''),
    error_code = NULLIF($5, ''),
    finished_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP,
    lease_token = NULL,
    leased_by = NULL,
    lease_expires_at = NULL,
    retain_until = CURRENT_TIMESTAMP + ($6 * INTERVAL '1 second')
WHERE id = $1 AND status = 'running' AND lease_token = $2
`, req.JobID, req.LeaseToken, progressArg, req.SafeError, req.ErrorCode, int64(req.Retention/time.Second))
		err = execErr
		affected = tag.RowsAffected()
	case completionCanceled:
		tag, execErr := tx.Exec(ctx, `
UPDATE vango_jobs
SET status = 'canceled',
    output = NULL,
    progress = COALESCE($3::jsonb, progress),
    safe_error = NULL,
    error_code = NULL,
    finished_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP,
    lease_token = NULL,
    leased_by = NULL,
    lease_expires_at = NULL,
    retain_until = CURRENT_TIMESTAMP + ($4 * INTERVAL '1 second')
WHERE id = $1 AND status = 'running' AND lease_token = $2
`, req.JobID, req.LeaseToken, progressArg, int64(req.Retention/time.Second))
		err = execErr
		affected = tag.RowsAffected()
	default:
		return fmt.Errorf("jobs: unknown completion kind %q", req.Kind)
	}
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}

	if _, err := tx.Exec(ctx, `
DELETE FROM vango_job_concurrency
WHERE job_id = $1 AND lease_token = $2
`, req.JobID, req.LeaseToken); err != nil {
		return err
	}

	attemptStatus := string(req.Kind)
	if req.Kind == completionRetry {
		attemptStatus = string(AttemptRetryScheduled)
	}
	if req.Kind == completionFailure {
		attemptStatus = string(AttemptFailed)
	}
	if req.Kind == completionCanceled {
		attemptStatus = string(AttemptCanceled)
	}
	if req.Kind == completionSuccess {
		attemptStatus = string(AttemptSucceeded)
	}
	if _, err := tx.Exec(ctx, `
UPDATE vango_job_attempts
SET status = $3,
    ended_at = CURRENT_TIMESTAMP,
    duration_ms = FLOOR(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at)) * 1000),
    safe_error = NULLIF($4, ''),
    error_code = NULLIF($5, ''),
    retry_delay_ms = CASE WHEN $6 > 0 THEN $6 ELSE NULL END
WHERE job_id = $1 AND attempt = $2
`, req.JobID, req.Attempt, attemptStatus, req.SafeError, req.ErrorCode, milliseconds(req.RetryDelay)); err != nil {
		return err
	}

	eventKind := string(req.Kind)
	if req.Kind == completionRetry {
		eventKind = string(EventRetried)
	}
	if req.Kind == completionFailure {
		eventKind = string(EventFailed)
	}
	if req.Kind == completionCanceled {
		eventKind = string(EventCanceled)
	}
	if req.Kind == completionSuccess {
		eventKind = string(EventSucceeded)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO vango_job_events (job_id, kind, payload)
VALUES ($1, $2, CASE WHEN $3::text = '' AND $4::text = '' AND $5::bigint = 0 THEN NULL ELSE jsonb_build_object('code', NULLIF($3::text, ''), 'safe_error', NULLIF($4::text, ''), 'retry_delay_ms', CASE WHEN $5::bigint > 0 THEN $5::bigint ELSE NULL END) END)
`, req.JobID, eventKind, req.ErrorCode, req.SafeError, milliseconds(req.RetryDelay)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *postgresStore) reap(ctx context.Context, req reapRequest) ([]reapedJob, error) {
	if req.Limit <= 0 {
		req.Limit = 32
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
SELECT id, name, queue, attempts, max_attempts, COALESCE(cancel_requested_at, NULL), COALESCE(lease_token, '')
FROM vango_jobs
WHERE status = 'running'
  AND lease_expires_at < CURRENT_TIMESTAMP
ORDER BY lease_expires_at ASC
LIMIT $1
FOR UPDATE SKIP LOCKED
`, req.Limit)
	if err != nil {
		return nil, err
	}

	type reapCandidate struct {
		jobID             string
		name              string
		queue             string
		attempts          int
		maxAttempts       int
		cancelRequestedAt *time.Time
		leaseToken        string
	}

	candidates := make([]reapCandidate, 0, req.Limit)
	for rows.Next() {
		var candidate reapCandidate
		if err := rows.Scan(&candidate.jobID, &candidate.name, &candidate.queue, &candidate.attempts, &candidate.maxAttempts, &candidate.cancelRequestedAt, &candidate.leaseToken); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	results := make([]reapedJob, 0, len(candidates))
	for _, candidate := range candidates {
		if _, err := tx.Exec(ctx, `
UPDATE vango_job_attempts
SET status = 'abandoned',
    ended_at = CURRENT_TIMESTAMP,
    duration_ms = FLOOR(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at)) * 1000)
WHERE job_id = $1 AND attempt = $2
	`, candidate.jobID, candidate.attempts); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO vango_job_events (job_id, kind) VALUES ($1, 'abandoned')
	`, candidate.jobID); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `
DELETE FROM vango_job_concurrency
WHERE job_id = $1 AND lease_token = $2
	`, candidate.jobID, candidate.leaseToken); err != nil {
			return nil, err
		}
		result := reapedJob{
			JobID:   candidate.jobID,
			JobName: candidate.name,
			Queue:   candidate.queue,
		}
		switch {
		case candidate.cancelRequestedAt != nil:
			retention := s.cfg.CanceledRetention
			if req.Retention != nil {
				retention = req.Retention(candidate.name, StatusCanceled)
			}
			if _, err := tx.Exec(ctx, `
UPDATE vango_jobs
SET status = 'canceled',
    finished_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP,
    lease_token = NULL,
    leased_by = NULL,
    lease_expires_at = NULL,
    retain_until = CURRENT_TIMESTAMP + ($2 * INTERVAL '1 second')
WHERE id = $1
	`, candidate.jobID, int64(retention/time.Second)); err != nil {
				return nil, err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO vango_job_events (job_id, kind) VALUES ($1, 'canceled')`, candidate.jobID); err != nil {
				return nil, err
			}
			result.Outcome = EventCanceled
		case candidate.attempts >= candidate.maxAttempts:
			retention := s.cfg.FailedRetention
			if req.Retention != nil {
				retention = req.Retention(candidate.name, StatusFailed)
			}
			if _, err := tx.Exec(ctx, `
UPDATE vango_jobs
SET status = 'failed',
    safe_error = 'job lease expired before completion',
    error_code = 'job_abandoned',
    finished_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP,
    lease_token = NULL,
    leased_by = NULL,
    lease_expires_at = NULL,
    retain_until = CURRENT_TIMESTAMP + ($2 * INTERVAL '1 second')
WHERE id = $1
	`, candidate.jobID, int64(retention/time.Second)); err != nil {
				return nil, err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO vango_job_events (job_id, kind) VALUES ($1, 'failed')`, candidate.jobID); err != nil {
				return nil, err
			}
			result.Outcome = EventFailed
			result.ErrorCode = "job_abandoned"
		default:
			delay := s.cfg.HeartbeatInterval
			if req.Backoff != nil {
				delay = req.Backoff(candidate.name, candidate.jobID, candidate.attempts)
			}
			if _, err := tx.Exec(ctx, `
UPDATE vango_jobs
SET status = 'queued',
    run_at = CURRENT_TIMESTAMP + ($2 * INTERVAL '1 millisecond'),
    safe_error = 'job lease expired before completion',
    error_code = 'job_abandoned',
    updated_at = CURRENT_TIMESTAMP,
    lease_token = NULL,
    leased_by = NULL,
    lease_expires_at = NULL
WHERE id = $1
	`, candidate.jobID, milliseconds(delay)); err != nil {
				return nil, err
			}
			if _, err := tx.Exec(ctx, `
INSERT INTO vango_job_events (job_id, kind, payload)
VALUES ($1, 'retried', jsonb_build_object('retry_delay_ms', $2::bigint, 'code', 'job_abandoned'))
	`, candidate.jobID, milliseconds(delay)); err != nil {
				return nil, err
			}
			result.Outcome = EventRetried
			result.ErrorCode = "job_abandoned"
		}
		results = append(results, result)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *postgresStore) prune(ctx context.Context, req pruneRequest) (*PruneSummary, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(ctx, `
WITH doomed AS (
    SELECT id
    FROM vango_jobs
    WHERE status IN ('succeeded', 'failed', 'canceled')
      AND retain_until IS NOT NULL
      AND retain_until <= CURRENT_TIMESTAMP
    ORDER BY retain_until ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
DELETE FROM vango_jobs j
USING doomed d
WHERE j.id = d.id
RETURNING j.id
`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &PruneSummary{Deleted: len(ids), JobIDs: ids}, nil
}

func (s *postgresStore) ensureSchedules(ctx context.Context, schedules []registeredSchedule) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	now := s.cfg.Now().UTC()
	for _, schedule := range schedules {
		nextRunAt, err := schedule.next(now)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO vango_job_schedules (name, target_job_name, spec, timezone, next_run_at, updated_at)
VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
ON CONFLICT (name) DO UPDATE
SET target_job_name = EXCLUDED.target_job_name,
    spec = EXCLUDED.spec,
    timezone = EXCLUDED.timezone,
    next_run_at = CASE
        WHEN vango_job_schedules.spec <> EXCLUDED.spec OR vango_job_schedules.timezone <> EXCLUDED.timezone
            THEN EXCLUDED.next_run_at
        ELSE vango_job_schedules.next_run_at
    END,
    updated_at = CURRENT_TIMESTAMP
`, schedule.name(), schedule.targetName(), schedule.spec(), schedule.timezone().String(), nextRunAt); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *postgresStore) tickSchedules(ctx context.Context, req scheduleTickRequest) ([]scheduledRef, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
SELECT name, next_run_at
FROM vango_job_schedules
WHERE paused = false
  AND next_run_at <= $1
ORDER BY next_run_at ASC
LIMIT $2
FOR UPDATE SKIP LOCKED
`, req.Now, req.Limit)
	if err != nil {
		return nil, err
	}

	type scheduleCandidate struct {
		name  string
		runAt time.Time
	}

	candidates := make([]scheduleCandidate, 0, req.Limit)
	for rows.Next() {
		var candidate scheduleCandidate
		if err := rows.Scan(&candidate.name, &candidate.runAt); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	refs := make([]scheduledRef, 0, req.Limit)
	for _, candidate := range candidates {
		schedule := req.Schedules[candidate.name]
		if schedule == nil {
			continue
		}
		input, err := schedule.inputFor(ctx, candidate.runAt)
		if err != nil {
			return nil, err
		}
		def := schedule.targetDefinition()
		payload, err := def.encodeInputAny(req.Codec, input)
		if err != nil {
			return nil, err
		}
		if req.CheckBudget != nil {
			if err := req.CheckBudget(BudgetPayload, payload); err != nil {
				return nil, err
			}
		}
		nextRunAt, err := schedule.next(candidate.runAt)
		if err != nil {
			return nil, err
		}
		base := def.base()
		jobID := req.NewJobID(req.Now)
		jobReq := enqueueRequest{
			ID:                jobID,
			Name:              base.name,
			Queue:             base.queue,
			Priority:          base.priority,
			MaxAttempts:       base.maxAttempts,
			Timeout:           base.timeout,
			RunAt:             req.Now,
			Payload:           payload,
			UniqueKey:         fmt.Sprintf("schedule:%s:%s", candidate.name, candidate.runAt.UTC().Format(time.RFC3339Nano)),
			UniquePolicy:      UniqueReturnExisting,
			RetentionOverride: base.retention,
		}
		inserted, ref, err := s.insertJob(ctx, tx, jobReq, "")
		if err != nil {
			return nil, err
		}
		if inserted {
			if err := s.notifyWakeup(ctx, tx, jobReq.Queue); err != nil {
				return nil, err
			}
		}
		if _, err := tx.Exec(ctx, `
UPDATE vango_job_schedules
SET last_run_at = $2,
    last_job_id = $3,
    next_run_at = $4,
    updated_at = CURRENT_TIMESTAMP
WHERE name = $1
	`, candidate.name, candidate.runAt, ref.ID, nextRunAt); err != nil {
			return nil, err
		}
		refs = append(refs, scheduledRef{
			Ref:          ref,
			Queue:        base.queue,
			ScheduleName: candidate.name,
			DueAt:        candidate.runAt,
			Inserted:     inserted && ref.ID == jobID,
		})
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return refs, nil
}

func (s *postgresStore) pauseSchedule(ctx context.Context, name string) error {
	tag, err := s.db.Exec(ctx, `
UPDATE vango_job_schedules
SET paused = true,
    updated_at = CURRENT_TIMESTAMP
WHERE name = $1
`, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *postgresStore) resumeSchedule(ctx context.Context, name string) error {
	tag, err := s.db.Exec(ctx, `
UPDATE vango_job_schedules
SET paused = false,
    updated_at = CURRENT_TIMESTAMP
WHERE name = $1
`, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *postgresStore) schedule(ctx context.Context, name string) (*ScheduleSnapshot, error) {
	row := s.db.QueryRow(ctx, `
SELECT name, target_job_name, spec, timezone, paused, next_run_at, last_run_at, COALESCE(last_job_id, ''), updated_at
FROM vango_job_schedules
WHERE name = $1
`, name)
	var snapshot ScheduleSnapshot
	if err := row.Scan(
		&snapshot.Name,
		&snapshot.TargetJobName,
		&snapshot.Spec,
		&snapshot.Timezone,
		&snapshot.Paused,
		&snapshot.NextRunAt,
		&snapshot.LastRunAt,
		&snapshot.LastJobID,
		&snapshot.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &snapshot, nil
}

func (s *postgresStore) schedules(ctx context.Context) ([]ScheduleSnapshot, error) {
	rows, err := s.db.Query(ctx, `
SELECT name, target_job_name, spec, timezone, paused, next_run_at, last_run_at, COALESCE(last_job_id, ''), updated_at
FROM vango_job_schedules
ORDER BY name ASC
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ScheduleSnapshot, 0)
	for rows.Next() {
		var snapshot ScheduleSnapshot
		if err := rows.Scan(
			&snapshot.Name,
			&snapshot.TargetJobName,
			&snapshot.Spec,
			&snapshot.Timezone,
			&snapshot.Paused,
			&snapshot.NextRunAt,
			&snapshot.LastRunAt,
			&snapshot.LastJobID,
			&snapshot.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *postgresStore) subscribeWakeup(ctx context.Context) (<-chan string, func(), error) {
	acquirer, ok := s.db.(connAcquirer)
	if !ok {
		return nil, func() {}, nil
	}
	listenCtx, cancel := context.WithCancel(ctx)
	conn, err := acquirer.Acquire(listenCtx)
	if err != nil {
		cancel()
		return nil, func() {}, err
	}
	if _, err := conn.Exec(listenCtx, `LISTEN vango_jobs_wakeup`); err != nil {
		conn.Release()
		cancel()
		return nil, func() {}, err
	}
	ch := make(chan string, 1)
	go func() {
		defer close(ch)
		defer conn.Release()
		for {
			notification, err := conn.Conn().WaitForNotification(listenCtx)
			if err != nil {
				return
			}
			select {
			case ch <- notification.Payload:
			default:
			}
		}
	}()
	return ch, cancel, nil
}

func (s *postgresStore) close() error {
	return nil
}

func (s *postgresStore) insertJob(ctx context.Context, q pgx.Tx, req enqueueRequest, retriedFromID string) (bool, Ref, error) {
	runAt := req.RunAt
	if runAt.IsZero() {
		runAt = s.cfg.Now().UTC()
	}
	metadataBytes, err := json.Marshal(req.Metadata)
	if err != nil {
		return false, Ref{}, err
	}
	if _, err := q.Exec(ctx, `
INSERT INTO vango_job_queues (queue, paused, pause_reason, updated_at)
VALUES ($1, false, NULL, CURRENT_TIMESTAMP)
ON CONFLICT (queue) DO NOTHING
`, req.Queue); err != nil {
		return false, Ref{}, err
	}

	if req.UniqueKey == "" {
		if _, err := q.Exec(ctx, `
INSERT INTO vango_jobs (
    id, name, queue, status,
    tenant_id, actor_id, actor_kind,
    parent_job_id, retried_from_job_id, unique_key, concurrency_key, trace_id,
    priority, attempts, max_attempts, timeout_seconds, run_at,
    payload, metadata, tags
) VALUES (
    $1, $2, $3, 'queued',
    NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''),
    NULLIF($7, ''), NULLIF($8, ''), NULLIF($9, ''), NULLIF($10, ''), NULLIF($11, ''),
    $12, 0, $13, $14, $15,
    $16::jsonb, $17::jsonb, $18
)`, req.ID, req.Name, req.Queue, req.TenantID, req.Actor.ID, req.Actor.Kind, req.ParentJobID, retriedFromID, req.UniqueKey, req.ConcurrencyKey, req.TraceID, req.Priority, req.MaxAttempts, int(req.Timeout/time.Second), runAt, string(req.Payload), string(metadataBytes), req.Tags); err != nil {
			return false, Ref{}, err
		}
		if _, err := q.Exec(ctx, `INSERT INTO vango_job_events (job_id, kind) VALUES ($1, 'enqueued')`, req.ID); err != nil {
			return false, Ref{}, err
		}
		return true, Ref{ID: req.ID, Name: req.Name}, nil
	}

	row := q.QueryRow(ctx, `
WITH inserted AS (
    INSERT INTO vango_jobs (
        id, name, queue, status,
        tenant_id, actor_id, actor_kind,
        parent_job_id, retried_from_job_id, unique_key, concurrency_key, trace_id,
        priority, attempts, max_attempts, timeout_seconds, run_at,
        payload, metadata, tags
    ) VALUES (
        $1, $2, $3, 'queued',
        NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''),
        NULLIF($7, ''), NULLIF($8, ''), NULLIF($9, ''), NULLIF($10, ''), NULLIF($11, ''),
        $12, 0, $13, $14, $15,
        $16::jsonb, $17::jsonb, $18
    )
    ON CONFLICT (name, unique_key) WHERE unique_key IS NOT NULL AND status IN ('queued', 'running')
    DO NOTHING
    RETURNING id, name, true
)
SELECT id, name, true FROM inserted
UNION ALL
SELECT id, name, false
FROM vango_jobs
WHERE name = $2 AND unique_key = $9 AND status IN ('queued', 'running')
LIMIT 1
`, req.ID, req.Name, req.Queue, req.TenantID, req.Actor.ID, req.Actor.Kind, req.ParentJobID, retriedFromID, req.UniqueKey, req.ConcurrencyKey, req.TraceID, req.Priority, req.MaxAttempts, int(req.Timeout/time.Second), runAt, string(req.Payload), string(metadataBytes), req.Tags)
	var ref Ref
	var inserted bool
	if err := row.Scan(&ref.ID, &ref.Name, &inserted); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, Ref{}, ErrDuplicateActiveJob
		}
		return false, Ref{}, err
	}
	if inserted {
		if _, err := q.Exec(ctx, `INSERT INTO vango_job_events (job_id, kind) VALUES ($1, 'enqueued')`, ref.ID); err != nil {
			return false, Ref{}, err
		}
		return true, ref, nil
	}
	if req.UniquePolicy == UniqueRejectDuplicate {
		return false, Ref{}, ErrDuplicateActiveJob
	}
	return false, ref, nil
}

func (s *postgresStore) notifyWakeup(ctx context.Context, q pgx.Tx, queue string) error {
	_, err := q.Exec(ctx, `SELECT pg_notify('vango_jobs_wakeup', $1)`, queue)
	return err
}

func milliseconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return d.Milliseconds()
}
