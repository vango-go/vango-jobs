package jobs

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type jobContext struct {
	context.Context
	runtime *Runtime
	job     claimedJob
	worker  string
	logger  *slog.Logger

	mu                  sync.Mutex
	progress            *Progress
	progressDirty       bool
	cancelRequestedFlag bool
	leaseLostFlag       bool
}

func newJobContext(ctx context.Context, runtime *Runtime, job claimedJob, workerID string) *jobContext {
	logger := runtime.logger.With(
		"job_id", job.JobID,
		"job_name", job.JobName,
		"queue", job.Queue,
		"attempt", job.Attempts,
		"worker_id", workerID,
	)
	if job.TraceID != "" {
		logger = logger.With("trace_id", job.TraceID)
	}
	return &jobContext{
		Context: ctx,
		runtime: runtime,
		job:     job,
		worker:  workerID,
		logger:  logger,
	}
}

func (c *jobContext) JobID() string {
	return c.job.JobID
}

func (c *jobContext) JobName() string {
	return c.job.JobName
}

func (c *jobContext) Queue() string {
	return c.job.Queue
}

func (c *jobContext) Attempt() int {
	return c.job.Attempts
}

func (c *jobContext) MaxAttempts() int {
	return c.job.MaxAttempts
}

func (c *jobContext) TenantID() string {
	return c.job.TenantID
}

func (c *jobContext) Actor() Actor {
	return c.job.Actor
}

func (c *jobContext) Metadata() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return copyMetadata(c.job.Metadata)
}

func (c *jobContext) IdempotencyKey() string {
	return c.job.JobID
}

func (c *jobContext) Logger() *slog.Logger {
	return c.logger
}

func (c *jobContext) Progress(p Progress) error {
	payload, err := c.runtime.cfg.Codec.Marshal(p)
	if err != nil {
		return err
	}
	if err := c.runtime.checkBudget(BudgetProgress, payload); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	copyProgress := p
	copyProgress.Metadata = copyMetadata(p.Metadata)
	c.progress = &copyProgress
	c.progressDirty = true
	return nil
}

func (c *jobContext) Enqueue(def any, input any, opts ...EnqueueOption) (Ref, error) {
	if !hasParentOption(opts) {
		opts = append(opts, WithParent(Ref{ID: c.job.JobID, Name: c.job.JobName}))
	}
	return c.runtime.Enqueue(c.Context, def, input, opts...)
}

func (c *jobContext) latestProgress() *Progress {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneProgress(c.progress)
}

func (c *jobContext) progressForFlush(interval time.Duration, last time.Time, now time.Time) *Progress {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.progressDirty || c.progress == nil {
		return nil
	}
	if interval > 0 && !last.IsZero() && now.Sub(last) < interval {
		return nil
	}
	c.progressDirty = false
	return cloneProgress(c.progress)
}

func (c *jobContext) markCancelRequested() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelRequestedFlag = true
}

func (c *jobContext) cancelRequested() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cancelRequestedFlag
}

func (c *jobContext) markLeaseLost() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leaseLostFlag = true
}

func (c *jobContext) leaseLost() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leaseLostFlag
}

func hasParentOption(opts []EnqueueOption) bool {
	cfg := enqueueConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg.parent != nil
}
