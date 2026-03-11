package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type store interface {
	enqueue(ctx context.Context, req enqueueRequest) (Ref, error)
	enqueueTx(ctx context.Context, tx pgx.Tx, req enqueueRequest) (Ref, error)
	inspect(ctx context.Context, id string) (*RawSnapshot, error)
	list(ctx context.Context, filter ListFilter, scope authScope) ([]*RawSnapshot, error)
	attempts(ctx context.Context, id string) ([]AttemptSnapshot, error)
	events(ctx context.Context, id string) ([]EventSnapshot, error)
	cancel(ctx context.Context, id string) error
	retry(ctx context.Context, id string, req retryCloneRequest) (Ref, error)
	pauseQueue(ctx context.Context, queue string, reason string) error
	resumeQueue(ctx context.Context, queue string) error
	queue(ctx context.Context, name string) (*QueueSnapshot, error)
	queues(ctx context.Context) ([]QueueSnapshot, error)
	replay(ctx context.Context, filter ReplayFilter, req retryCloneRequest) (*ReplaySummary, error)
	claim(ctx context.Context, req claimRequest) ([]claimedJob, error)
	heartbeat(ctx context.Context, req heartbeatRequest) (heartbeatResult, error)
	writeProgress(ctx context.Context, req progressWriteRequest) error
	complete(ctx context.Context, req completionRequest) error
	reap(ctx context.Context, req reapRequest) ([]reapedJob, error)
	prune(ctx context.Context, req pruneRequest) (*PruneSummary, error)
	ensureSchedules(ctx context.Context, schedules []registeredSchedule) error
	tickSchedules(ctx context.Context, req scheduleTickRequest) ([]scheduledRef, error)
	pauseSchedule(ctx context.Context, name string) error
	resumeSchedule(ctx context.Context, name string) error
	schedule(ctx context.Context, name string) (*ScheduleSnapshot, error)
	schedules(ctx context.Context) ([]ScheduleSnapshot, error)
	subscribeWakeup(ctx context.Context) (<-chan string, func(), error)
	close() error
}

// Runtime is the high-level application entrypoint for enqueue, inspection, and workers.
type Runtime struct {
	registry  *Registry
	store     store
	cfg       Config
	ids       *idGenerator
	logger    *slog.Logger
	metrics   MetricsSink
	collector *MetricsCollector
	tracer    trace.Tracer
	updates   *jobUpdateHub
	closeMu   sync.Mutex
	closed    bool
}

// NewRuntime creates a runtime on top of an internal store implementation.
func NewRuntime(st store, reg *Registry, opts ...RuntimeOption) (*Runtime, error) {
	if st == nil {
		return nil, errors.New("jobs: store is nil")
	}
	if reg == nil {
		reg = NewRegistry()
	}
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	var collector *MetricsCollector
	if cfg.Metrics == nil {
		collector = NewMetricsCollector()
		cfg.Metrics = collector
	}
	if cfg.TracerName == "" {
		cfg.TracerName = defaultTracerName
	}
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer(cfg.TracerName)
	}
	rt := &Runtime{
		registry:  reg,
		store:     st,
		cfg:       cfg,
		ids:       newIDGenerator(),
		logger:    cfg.Logger,
		metrics:   cfg.Metrics,
		collector: collector,
		tracer:    cfg.Tracer,
		updates:   newJobUpdateHub(cfg.NotificationBuffer, cfg.Broadcast, cfg.Logger),
	}
	return rt, nil
}

// Enqueue writes a durable job row.
func (r *Runtime) Enqueue(ctx context.Context, def any, input any, opts ...EnqueueOption) (Ref, error) {
	req, err := r.newEnqueueRequest(ctx, def, input, opts...)
	if err != nil {
		return Ref{}, err
	}
	ctx, span := r.startSpan(ctx, "jobs.enqueue",
		attribute.String("jobs.name", req.Name),
		attribute.String("jobs.queue", req.Queue),
	)
	defer span.End()
	ref, err := r.store.enqueue(ctx, req)
	if err != nil {
		r.recordSpanError(span, err)
		return Ref{}, err
	}
	inserted := ref.ID == req.ID
	if inserted {
		r.metrics.RecordEnqueued(req.Name, req.Queue)
		r.refreshQueueMetrics(ctx, req.Queue)
		r.log(slog.LevelInfo, "job_enqueued",
			"job_id", ref.ID,
			"job_name", req.Name,
			"queue", req.Queue,
			"run_at", req.RunAt,
			"trace_id", req.TraceID,
		)
	}
	r.updates.publish(ctx, ref.ID)
	return ref, err
}

// EnqueueTx writes a durable job row inside an existing pgx transaction.
func (r *Runtime) EnqueueTx(ctx context.Context, tx pgx.Tx, def any, input any, opts ...EnqueueOption) (Ref, error) {
	if tx == nil {
		return Ref{}, errors.New("jobs: tx is nil")
	}
	req, err := r.newEnqueueRequest(ctx, def, input, opts...)
	if err != nil {
		return Ref{}, err
	}
	ctx, span := r.startSpan(ctx, "jobs.enqueue_tx",
		attribute.String("jobs.name", req.Name),
		attribute.String("jobs.queue", req.Queue),
	)
	defer span.End()
	ref, err := r.store.enqueueTx(ctx, tx, req)
	if err != nil {
		r.recordSpanError(span, err)
		return Ref{}, err
	}
	if ref.ID == req.ID {
		r.metrics.RecordEnqueued(req.Name, req.Queue)
		r.refreshQueueMetrics(ctx, req.Queue)
		r.log(slog.LevelInfo, "job_enqueued",
			"job_id", ref.ID,
			"job_name", req.Name,
			"queue", req.Queue,
			"run_at", req.RunAt,
			"trace_id", req.TraceID,
			"transactional", true,
		)
	}
	r.updates.publish(ctx, ref.ID)
	return ref, err
}

// MetricsCollector returns the default in-memory collector when one is installed.
func (r *Runtime) MetricsCollector() *MetricsCollector {
	if r == nil {
		return nil
	}
	return r.collector
}

// Inspect reads an immutable raw snapshot.
func (r *Runtime) Inspect(ctx context.Context, id string, opts ...InspectOption) (*RawSnapshot, error) {
	ctx, span := r.startSpan(ctx, "jobs.inspect", attribute.String("jobs.id", id))
	defer span.End()
	cfg := inspectConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	raw, err := r.store.inspect(ctx, id)
	if err != nil {
		r.recordSpanError(span, err)
		return nil, err
	}
	if err := authorizeSnapshot(raw, cfg.scope); err != nil {
		r.recordSpanError(span, err)
		return nil, err
	}
	return raw.Clone(), nil
}

// List returns durable job snapshots for admin/status surfaces.
func (r *Runtime) List(ctx context.Context, filter ListFilter, opts ...InspectOption) ([]*RawSnapshot, error) {
	ctx, span := r.startSpan(ctx, "jobs.list")
	defer span.End()
	cfg := inspectConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.scope.empty() {
		r.recordSpanError(span, ErrUnauthorized)
		return nil, ErrUnauthorized
	}
	items, err := r.store.list(ctx, filter, cfg.scope)
	if err != nil {
		r.recordSpanError(span, err)
		return nil, err
	}
	out := make([]*RawSnapshot, 0, len(items))
	for _, item := range items {
		out = append(out, item.Clone())
	}
	return out, nil
}

// Lookup reads a typed immutable snapshot for a concrete definition.
func (r *Runtime) Lookup(ctx context.Context, def any, id string, opts ...InspectOption) (any, error) {
	ctx, span := r.startSpan(ctx, "jobs.lookup", attribute.String("jobs.id", id))
	defer span.End()
	raw, err := r.Inspect(ctx, id, opts...)
	if err != nil {
		r.recordSpanError(span, err)
		return nil, err
	}
	handler, err := r.resolveDefinition(def)
	if err != nil {
		r.recordSpanError(span, err)
		return nil, err
	}
	if raw.Ref.Name != handler.name() {
		err := fmt.Errorf("jobs: lookup definition %s does not match stored job %s", handler.name(), raw.Ref.Name)
		r.recordSpanError(span, err)
		return nil, err
	}
	return handler.typedSnapshot(r.cfg.Codec, raw)
}

// Attempts returns durable execution-attempt history for one job.
func (r *Runtime) Attempts(ctx context.Context, id string, opts ...InspectOption) ([]AttemptSnapshot, error) {
	cfg := inspectConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	raw, err := r.store.inspect(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := authorizeSnapshot(raw, cfg.scope); err != nil {
		return nil, err
	}
	return r.store.attempts(ctx, id)
}

// Events returns bounded durable lifecycle events for one job.
func (r *Runtime) Events(ctx context.Context, id string, opts ...InspectOption) ([]EventSnapshot, error) {
	cfg := inspectConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	raw, err := r.store.inspect(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := authorizeSnapshot(raw, cfg.scope); err != nil {
		return nil, err
	}
	return r.store.events(ctx, id)
}

// Cancel requests cancellation for a queued or running job.
func (r *Runtime) Cancel(ctx context.Context, id string, opts ...ControlOption) error {
	ctx, span := r.startSpan(ctx, "jobs.cancel", attribute.String("jobs.id", id))
	defer span.End()
	cfg := controlConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	raw, err := r.store.inspect(ctx, id)
	if err != nil {
		r.recordSpanError(span, err)
		return err
	}
	if err := authorizeSnapshot(raw, cfg.scope); err != nil {
		r.recordSpanError(span, err)
		return err
	}
	if err := r.store.cancel(ctx, id); err != nil {
		r.recordSpanError(span, err)
		return err
	}
	if raw.Status == StatusQueued {
		r.metrics.RecordCanceled(raw.Ref.Name, raw.Queue)
		r.refreshQueueMetrics(ctx, raw.Queue)
		r.log(slog.LevelInfo, "job_canceled",
			"job_id", raw.Ref.ID,
			"job_name", raw.Ref.Name,
			"queue", raw.Queue,
		)
	}
	r.updates.publish(ctx, id)
	return nil
}

// Retry clones a historical failed or canceled job into a new queued row.
func (r *Runtime) Retry(ctx context.Context, id string, opts ...RetryOption) (Ref, error) {
	ctx, span := r.startSpan(ctx, "jobs.retry", attribute.String("jobs.id", id))
	defer span.End()
	cfg := retryConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	raw, err := r.store.inspect(ctx, id)
	if err != nil {
		r.recordSpanError(span, err)
		return Ref{}, err
	}
	if err := authorizeSnapshot(raw, cfg.scope); err != nil {
		r.recordSpanError(span, err)
		return Ref{}, err
	}
	req := retryCloneRequest{
		Now:            r.now(),
		NewJobID:       r.ids.newJobID,
		PreserveUnique: cfg.preserveUnique,
		OverrideRunAt:  cfg.overrideRunAt,
		OverrideDelay:  cfg.overrideDelay,
		OverrideQueue:  cfg.overrideQueue,
	}
	ref, err := r.store.retry(ctx, id, req)
	if err != nil {
		r.recordSpanError(span, err)
		return Ref{}, err
	}
	queue := raw.Queue
	if cfg.overrideQueue != "" {
		queue = cfg.overrideQueue
	}
	r.metrics.RecordRetried(raw.Ref.Name, queue, raw.ErrorCode)
	r.metrics.RecordEnqueued(raw.Ref.Name, queue)
	r.refreshQueueMetrics(ctx, raw.Queue, queue)
	r.log(slog.LevelInfo, "job_retried",
		"job_id", raw.Ref.ID,
		"job_name", raw.Ref.Name,
		"queue", raw.Queue,
		"new_job_id", ref.ID,
		"new_queue", queue,
		"error_code", raw.ErrorCode,
	)
	r.log(slog.LevelInfo, "job_enqueued",
		"job_id", ref.ID,
		"job_name", raw.Ref.Name,
		"queue", queue,
		"retried_from_job_id", raw.Ref.ID,
	)
	r.updates.publish(ctx, id)
	r.updates.publish(ctx, ref.ID)
	return ref, err
}

// Replay clones matching historical jobs in bounded batches.
func (r *Runtime) Replay(ctx context.Context, filter ReplayFilter, opts ...RetryOption) (*ReplaySummary, error) {
	ctx, span := r.startSpan(ctx, "jobs.replay")
	defer span.End()
	cfg := retryConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	req := retryCloneRequest{
		Now:            r.now(),
		NewJobID:       r.ids.newJobID,
		PreserveUnique: cfg.preserveUnique,
		OverrideRunAt:  cfg.overrideRunAt,
		OverrideDelay:  cfg.overrideDelay,
		OverrideQueue:  cfg.overrideQueue,
		Scope:          cfg.scope,
		MaxBatch:       r.cfg.MaxReplayBatch,
	}
	summary, err := r.store.replay(ctx, filter, req)
	if err != nil {
		r.recordSpanError(span, err)
		return nil, err
	}
	for _, ref := range summary.Refs {
		r.updates.publish(ctx, ref.ID)
	}
	if cfg.overrideQueue != "" {
		r.refreshQueueMetrics(ctx, cfg.overrideQueue)
	} else if filter.Queue != "" {
		r.refreshQueueMetrics(ctx, filter.Queue)
	}
	r.log(slog.LevelInfo, "jobs_replayed",
		"selected", summary.Selected,
		"cloned", summary.Cloned,
		"failed", summary.Failed,
	)
	return summary, err
}

// PauseQueue pauses claiming for one queue.
func (r *Runtime) PauseQueue(ctx context.Context, queue string, reason string, opts ...ControlOption) error {
	ctx, span := r.startSpan(ctx, "jobs.pause_queue", attribute.String("jobs.queue", normalizeQueue(queue)))
	defer span.End()
	cfg := controlConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if err := authorizeAdmin(cfg.scope); err != nil {
		r.recordSpanError(span, err)
		return err
	}
	queue = normalizeQueue(queue)
	if err := r.store.pauseQueue(ctx, queue, reason); err != nil {
		r.recordSpanError(span, err)
		return err
	}
	r.log(slog.LevelInfo, "queue_paused", "queue", queue, "reason", reason)
	return nil
}

// ResumeQueue resumes claiming for one queue.
func (r *Runtime) ResumeQueue(ctx context.Context, queue string, opts ...ControlOption) error {
	ctx, span := r.startSpan(ctx, "jobs.resume_queue", attribute.String("jobs.queue", normalizeQueue(queue)))
	defer span.End()
	cfg := controlConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if err := authorizeAdmin(cfg.scope); err != nil {
		r.recordSpanError(span, err)
		return err
	}
	queue = normalizeQueue(queue)
	if err := r.store.resumeQueue(ctx, queue); err != nil {
		r.recordSpanError(span, err)
		return err
	}
	r.log(slog.LevelInfo, "queue_resumed", "queue", queue)
	r.updates.publish(ctx, "")
	return nil
}

// Queue returns one queue admin snapshot.
func (r *Runtime) Queue(ctx context.Context, queue string, opts ...ControlOption) (*QueueSnapshot, error) {
	cfg := controlConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if err := authorizeAdmin(cfg.scope); err != nil {
		return nil, err
	}
	return r.store.queue(ctx, normalizeQueue(queue))
}

// Queues returns all known queue admin snapshots.
func (r *Runtime) Queues(ctx context.Context, opts ...ControlOption) ([]QueueSnapshot, error) {
	cfg := controlConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if err := authorizeAdmin(cfg.scope); err != nil {
		return nil, err
	}
	return r.store.queues(ctx)
}

// PauseSchedule pauses one durable schedule.
func (r *Runtime) PauseSchedule(ctx context.Context, name string, opts ...ControlOption) error {
	cfg := controlConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if err := authorizeAdmin(cfg.scope); err != nil {
		return err
	}
	if err := r.syncSchedules(ctx); err != nil {
		return err
	}
	return r.store.pauseSchedule(ctx, normalizeName(name))
}

// ResumeSchedule resumes one durable schedule.
func (r *Runtime) ResumeSchedule(ctx context.Context, name string, opts ...ControlOption) error {
	cfg := controlConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if err := authorizeAdmin(cfg.scope); err != nil {
		return err
	}
	if err := r.syncSchedules(ctx); err != nil {
		return err
	}
	return r.store.resumeSchedule(ctx, normalizeName(name))
}

// Schedule returns one durable schedule snapshot.
func (r *Runtime) Schedule(ctx context.Context, name string, opts ...ControlOption) (*ScheduleSnapshot, error) {
	cfg := controlConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if err := authorizeAdmin(cfg.scope); err != nil {
		return nil, err
	}
	if err := r.syncSchedules(ctx); err != nil {
		return nil, err
	}
	return r.store.schedule(ctx, normalizeName(name))
}

// Schedules returns all durable schedule snapshots.
func (r *Runtime) Schedules(ctx context.Context, opts ...ControlOption) ([]ScheduleSnapshot, error) {
	cfg := controlConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if err := authorizeAdmin(cfg.scope); err != nil {
		return nil, err
	}
	if err := r.syncSchedules(ctx); err != nil {
		return nil, err
	}
	return r.store.schedules(ctx)
}

// Prune deletes terminal jobs whose retention windows have expired.
func (r *Runtime) Prune(ctx context.Context, opts ...PruneOption) (*PruneSummary, error) {
	ctx, span := r.startSpan(ctx, "jobs.prune")
	defer span.End()
	cfg := pruneConfig{limit: 100}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if err := authorizeAdmin(cfg.scope); err != nil {
		r.recordSpanError(span, err)
		return nil, err
	}
	summary, err := r.store.prune(ctx, pruneRequest{Limit: cfg.limit})
	if err != nil {
		r.recordSpanError(span, err)
		return nil, err
	}
	for _, id := range summary.JobIDs {
		r.updates.publish(ctx, id)
	}
	r.log(slog.LevelInfo, "jobs_pruned", "deleted", summary.Deleted)
	return summary, nil
}

// SubscribeJob returns a local notification stream for one job ID.
func (r *Runtime) SubscribeJob(ctx context.Context, jobID string) (<-chan struct{}, error) {
	return r.updates.subscribe(ctx, jobID), nil
}

// Close releases runtime resources.
func (r *Runtime) Close() error {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.updates.close()
	return r.store.close()
}

func (r *Runtime) resolveDefinition(def any) (registeredHandler, error) {
	name, ok := definitionName(def)
	if !ok {
		return nil, fmt.Errorf("%w: unsupported definition type %T", ErrInvalidDefinition, def)
	}
	h, ok := r.registry.resolve(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMissingHandler, name)
	}
	return h, nil
}

func (r *Runtime) syncSchedules(ctx context.Context) error {
	schedules := r.registry.schedulesList()
	if len(schedules) == 0 {
		return nil
	}
	return r.store.ensureSchedules(ctx, schedules)
}
