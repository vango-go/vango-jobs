package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// WorkerOption customizes a worker instance.
type WorkerOption func(*WorkerConfig)

// WorkerConfig controls claim/execution behavior.
type WorkerConfig struct {
	ID                  string
	Queues              []string
	Concurrency         int
	ClaimBatchSize      int
	ClaimOverscanFactor int
	LeaseDuration       time.Duration
	HeartbeatInterval   time.Duration
	PollInterval        time.Duration
	DrainTimeout        time.Duration
	ReapInterval        time.Duration
}

func defaultWorkerConfig(cfg Config, ids *idGenerator) WorkerConfig {
	hostname, _ := os.Hostname()
	workerID := cfg.WorkerID
	if workerID == "" {
		now := time.Now().UTC()
		if cfg.Now != nil {
			now = cfg.Now().UTC()
		}
		workerID = fmt.Sprintf("worker:%s:%s", hostname, ids.newLeaseToken(now))
	}
	return WorkerConfig{
		ID:                  workerID,
		Concurrency:         cfg.ClaimBatchSize,
		ClaimBatchSize:      cfg.ClaimBatchSize,
		ClaimOverscanFactor: cfg.ClaimOverscanFactor,
		LeaseDuration:       cfg.LeaseDuration,
		HeartbeatInterval:   cfg.HeartbeatInterval,
		PollInterval:        cfg.ClaimPollInterval,
		DrainTimeout:        10 * time.Second,
		ReapInterval:        cfg.LeaseDuration,
	}
}

// WithWorkerQueues limits the worker to a queue subset.
func WithWorkerQueues(queues ...string) WorkerOption {
	return func(cfg *WorkerConfig) {
		cfg.Queues = copyTags(queues)
	}
}

// WithWorkerConcurrency sets the worker concurrency limit.
func WithWorkerConcurrency(n int) WorkerOption {
	return func(cfg *WorkerConfig) {
		if n > 0 {
			cfg.Concurrency = n
		}
	}
}

// WithWorkerTimings overrides worker timing knobs.
func WithWorkerTimings(poll, heartbeat, lease, reap time.Duration) WorkerOption {
	return func(cfg *WorkerConfig) {
		if poll > 0 {
			cfg.PollInterval = poll
		}
		if heartbeat > 0 {
			cfg.HeartbeatInterval = heartbeat
		}
		if lease > 0 {
			cfg.LeaseDuration = lease
		}
		if reap > 0 {
			cfg.ReapInterval = reap
		}
	}
}

// WithWorkerDrainTimeout sets the graceful drain timeout.
func WithWorkerDrainTimeout(d time.Duration) WorkerOption {
	return func(cfg *WorkerConfig) {
		if d > 0 {
			cfg.DrainTimeout = d
		}
	}
}

// Worker claims and executes durable jobs.
type Worker struct {
	runtime *Runtime
	cfg     WorkerConfig
}

// NewWorker creates a worker bound to the runtime registry and store.
func (r *Runtime) NewWorker(opts ...WorkerOption) *Worker {
	cfg := defaultWorkerConfig(r.cfg, r.ids)
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.ClaimBatchSize < 1 {
		cfg.ClaimBatchSize = cfg.Concurrency
	}
	return &Worker{
		runtime: r,
		cfg:     cfg,
	}
}

// Run starts the claim/execute loop and blocks until shutdown.
func (w *Worker) Run(ctx context.Context) error {
	if w == nil || w.runtime == nil {
		return errors.New("jobs: worker is nil")
	}
	slots := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup

	wakeup, cleanupWakeup, err := w.runtime.store.subscribeWakeup(ctx)
	if err != nil {
		return err
	}
	defer cleanupWakeup()

	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		if w.cfg.ReapInterval <= 0 {
			return
		}
		ticker := time.NewTicker(w.cfg.ReapInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				results, err := w.runtime.store.reap(ctx, reapRequest{
					Limit: 32,
					Backoff: func(name, jobID string, attempt int) time.Duration {
						return w.runtime.backoffFor(name, jobID, attempt)
					},
					Retention: w.runtime.retentionForName,
				})
				if err != nil {
					continue
				}
				for _, result := range results {
					w.runtime.metrics.RecordAbandoned(result.JobName, result.Queue)
					w.runtime.log(slog.LevelWarn, "job_abandoned",
						"job_id", result.JobID,
						"job_name", result.JobName,
						"queue", result.Queue,
					)
					switch result.Outcome {
					case EventRetried:
						w.runtime.metrics.RecordRetried(result.JobName, result.Queue, result.ErrorCode)
						w.runtime.log(slog.LevelInfo, "job_retried",
							"job_id", result.JobID,
							"job_name", result.JobName,
							"queue", result.Queue,
							"error_code", result.ErrorCode,
						)
					case EventFailed:
						w.runtime.metrics.RecordFailed(result.JobName, result.Queue, result.ErrorCode)
						w.runtime.log(slog.LevelError, "job_failed",
							"job_id", result.JobID,
							"job_name", result.JobName,
							"queue", result.Queue,
							"error_code", result.ErrorCode,
						)
					case EventCanceled:
						w.runtime.metrics.RecordCanceled(result.JobName, result.Queue)
						w.runtime.log(slog.LevelInfo, "job_canceled",
							"job_id", result.JobID,
							"job_name", result.JobName,
							"queue", result.Queue,
						)
					}
					w.runtime.refreshQueueMetrics(ctx, result.Queue)
					w.runtime.updates.publish(ctx, result.JobID)
				}
			}
		}
	}()

	wait := func() {
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		timeout := w.cfg.DrainTimeout
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		select {
		case <-done:
		case <-time.After(timeout):
		}
		<-reaperDone
	}

	for {
		if ctx.Err() != nil {
			wait()
			return nil
		}

		available := cap(slots) - len(slots)
		if available < 1 {
			select {
			case <-ctx.Done():
				wait()
				return nil
			case <-time.After(minDuration(w.cfg.PollInterval, 100*time.Millisecond)):
				continue
			}
		}

		limit := minInt(available, w.cfg.ClaimBatchSize)
		claimed, err := w.runtime.store.claim(ctx, claimRequest{
			WorkerID:      w.cfg.ID,
			Queues:        append([]string(nil), w.cfg.Queues...),
			Limit:         limit,
			OverscanLimit: limit * maxInt(1, w.cfg.ClaimOverscanFactor),
			LeaseDuration: w.cfg.LeaseDuration,
			JobNames:      w.runtime.registry.names(),
		})
		if err != nil {
			select {
			case <-ctx.Done():
				wait()
				return nil
			case <-time.After(w.cfg.PollInterval):
				continue
			}
		}
		if len(claimed) == 0 {
			select {
			case <-ctx.Done():
				wait()
				return nil
			case <-time.After(w.cfg.PollInterval):
			case <-wakeup:
			}
			continue
		}

		for _, job := range claimed {
			slots <- struct{}{}
			wg.Add(1)
			go func(job claimedJob) {
				defer wg.Done()
				defer func() { <-slots }()
				w.runOne(ctx, job)
			}(job)
		}
	}
}

// RunOnce claims and executes up to one batch of jobs synchronously.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	if w == nil || w.runtime == nil {
		return 0, errors.New("jobs: worker is nil")
	}
	claimed, err := w.runtime.store.claim(ctx, claimRequest{
		WorkerID:      w.cfg.ID,
		Queues:        append([]string(nil), w.cfg.Queues...),
		Limit:         w.cfg.ClaimBatchSize,
		OverscanLimit: w.cfg.ClaimBatchSize * maxInt(1, w.cfg.ClaimOverscanFactor),
		LeaseDuration: w.cfg.LeaseDuration,
		JobNames:      w.runtime.registry.names(),
	})
	if err != nil {
		return 0, err
	}
	for _, job := range claimed {
		w.runOne(ctx, job)
	}
	return len(claimed), nil
}

func (w *Worker) runOne(workerCtx context.Context, job claimedJob) {
	handler, ok := w.runtime.registry.resolve(job.JobName)
	if !ok {
		w.runtime.logger.Error("jobs: claimed job missing handler", "job_id", job.JobID, "job_name", job.JobName)
		return
	}

	attemptCtx, span := w.runtime.startSpan(workerCtx, "jobs.worker.run", jobSpanAttributes(job)...)
	defer span.End()
	execCtx, cancelExec := context.WithCancel(attemptCtx)
	defer cancelExec()
	handlerCtx := execCtx
	if job.Timeout > 0 {
		var cancelTimeout context.CancelFunc
		handlerCtx, cancelTimeout = context.WithTimeout(execCtx, job.Timeout)
		defer cancelTimeout()
	}

	jobCtx := newJobContext(handlerCtx, w.runtime, job, w.cfg.ID)
	w.runtime.metrics.RecordStarted(job.JobName, job.Queue)
	w.runtime.refreshQueueMetrics(workerCtx, job.Queue)
	w.runtime.log(slog.LevelInfo, "job_started",
		"job_id", job.JobID,
		"job_name", job.JobName,
		"queue", job.Queue,
		"worker_id", w.cfg.ID,
		"attempt", job.Attempts,
		"trace_id", job.TraceID,
	)
	heartbeatDone := make(chan heartbeatResult, 1)
	go func() {
		heartbeatDone <- w.heartbeatLoop(workerCtx, cancelExec, jobCtx, job)
	}()

	output, err := handler.run(jobCtx, w.runtime.cfg.Codec, job.Payload)
	cancelExec()
	heartbeatResult := <-heartbeatDone

	if heartbeatResult.LeaseHeld == false && err == nil {
		// Lease already lost. Fail closed and let reaper recover.
		return
	}
	if heartbeatResult.CancelRequested || jobCtx.cancelRequested() {
		finishedAt := w.runtime.now()
		_ = w.runtime.store.complete(workerCtx, completionRequest{
			JobID:      job.JobID,
			LeaseToken: job.LeaseToken,
			Attempt:    job.Attempts,
			Kind:       completionCanceled,
			Progress:   jobCtx.latestProgress(),
			Retention:  w.runtime.retentionFor(handler.baseDefinition(), StatusCanceled),
		})
		w.runtime.metrics.RecordCanceled(job.JobName, job.Queue)
		if d := maybeDuration(job.AttemptStartedAt, finishedAt); d > 0 {
			w.runtime.metrics.RecordRunDuration(job.JobName, job.Queue, d)
		}
		if d := maybeDuration(job.CreatedAt, finishedAt); d > 0 {
			w.runtime.metrics.RecordEndToEndLatency(job.JobName, job.Queue, d)
		}
		w.runtime.refreshQueueMetrics(workerCtx, job.Queue)
		w.runtime.log(slog.LevelInfo, "job_canceled",
			"job_id", job.JobID,
			"job_name", job.JobName,
			"queue", job.Queue,
			"attempt", job.Attempts,
		)
		w.runtime.updates.publish(workerCtx, job.JobID)
		return
	}
	if errors.Is(workerCtx.Err(), context.Canceled) && errors.Is(err, context.Canceled) {
		// Worker shutdown: do not acknowledge. Lease expiry handles replay.
		return
	}
	if jobCtx.leaseLost() {
		return
	}

	if err == nil {
		if budgetErr := w.runtime.checkBudget(BudgetOutput, output); budgetErr != nil {
			err = Permanent(Safe(budgetErr, budgetErr.Error()))
		}
	}

	if err == nil {
		finishedAt := w.runtime.now()
		_ = w.runtime.store.complete(workerCtx, completionRequest{
			JobID:      job.JobID,
			LeaseToken: job.LeaseToken,
			Attempt:    job.Attempts,
			Kind:       completionSuccess,
			Output:     output,
			Progress:   jobCtx.latestProgress(),
			Retention:  w.runtime.retentionFor(handler.baseDefinition(), StatusSucceeded),
		})
		w.runtime.metrics.RecordSucceeded(job.JobName, job.Queue)
		if d := maybeDuration(job.AttemptStartedAt, finishedAt); d > 0 {
			w.runtime.metrics.RecordRunDuration(job.JobName, job.Queue, d)
		}
		if d := maybeDuration(job.CreatedAt, finishedAt); d > 0 {
			w.runtime.metrics.RecordEndToEndLatency(job.JobName, job.Queue, d)
		}
		w.runtime.refreshQueueMetrics(workerCtx, job.Queue)
		w.runtime.log(slog.LevelInfo, "job_succeeded",
			"job_id", job.JobID,
			"job_name", job.JobName,
			"queue", job.Queue,
			"attempt", job.Attempts,
			"run_duration", maybeDuration(job.AttemptStartedAt, finishedAt),
			"end_to_end_latency", maybeDuration(job.CreatedAt, finishedAt),
		)
		w.runtime.updates.publish(workerCtx, job.JobID)
		return
	}

	if errors.Is(err, context.DeadlineExceeded) && handlerCtx.Err() == context.DeadlineExceeded {
		// Per-attempt timeouts are ordinary retryable failures.
	} else if errors.Is(err, context.Canceled) && workerCtx.Err() != nil {
		return
	}

	if isPermanent(err) || job.Attempts >= job.MaxAttempts {
		finishedAt := w.runtime.now()
		_ = w.runtime.store.complete(workerCtx, completionRequest{
			JobID:      job.JobID,
			LeaseToken: job.LeaseToken,
			Attempt:    job.Attempts,
			Kind:       completionFailure,
			Progress:   jobCtx.latestProgress(),
			SafeError:  safeErrorString(err),
			ErrorCode:  errorCode(err),
			Retention:  w.runtime.retentionFor(handler.baseDefinition(), StatusFailed),
		})
		w.runtime.recordSpanError(span, err)
		w.runtime.metrics.RecordFailed(job.JobName, job.Queue, errorCode(err))
		if d := maybeDuration(job.AttemptStartedAt, finishedAt); d > 0 {
			w.runtime.metrics.RecordRunDuration(job.JobName, job.Queue, d)
		}
		if d := maybeDuration(job.CreatedAt, finishedAt); d > 0 {
			w.runtime.metrics.RecordEndToEndLatency(job.JobName, job.Queue, d)
		}
		w.runtime.refreshQueueMetrics(workerCtx, job.Queue)
		w.runtime.log(slog.LevelError, "job_failed",
			"job_id", job.JobID,
			"job_name", job.JobName,
			"queue", job.Queue,
			"attempt", job.Attempts,
			"error_code", errorCode(err),
			"safe_error", safeErrorString(err),
			"run_duration", maybeDuration(job.AttemptStartedAt, finishedAt),
			"end_to_end_latency", maybeDuration(job.CreatedAt, finishedAt),
		)
		w.runtime.updates.publish(workerCtx, job.JobID)
		return
	}

	delay, ok := explicitRetryDelay(err)
	if !ok {
		delay = w.runtime.backoffFor(job.JobName, job.JobID, job.Attempts)
	}
	_ = w.runtime.store.complete(workerCtx, completionRequest{
		JobID:      job.JobID,
		LeaseToken: job.LeaseToken,
		Attempt:    job.Attempts,
		Kind:       completionRetry,
		Progress:   jobCtx.latestProgress(),
		SafeError:  safeErrorString(err),
		ErrorCode:  errorCode(err),
		RetryDelay: delay,
	})
	w.runtime.recordSpanError(span, err)
	if d := maybeDuration(job.AttemptStartedAt, w.runtime.now()); d > 0 {
		w.runtime.metrics.RecordRunDuration(job.JobName, job.Queue, d)
	}
	w.runtime.metrics.RecordRetried(job.JobName, job.Queue, errorCode(err))
	w.runtime.refreshQueueMetrics(workerCtx, job.Queue)
	w.runtime.log(slog.LevelInfo, "job_retried",
		"job_id", job.JobID,
		"job_name", job.JobName,
		"queue", job.Queue,
		"attempt", job.Attempts,
		"error_code", errorCode(err),
		"safe_error", safeErrorString(err),
		"retry_delay", delay,
	)
	w.runtime.updates.publish(workerCtx, job.JobID)
}

func (w *Worker) heartbeatLoop(workerCtx context.Context, cancel context.CancelFunc, jobCtx *jobContext, job claimedJob) heartbeatResult {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()
	lastProgressFlush := time.Time{}
	for {
		select {
		case <-workerCtx.Done():
			cancel()
			return heartbeatResult{LeaseHeld: true}
		case <-jobCtx.Done():
			return heartbeatResult{LeaseHeld: !jobCtx.leaseLost(), CancelRequested: jobCtx.cancelRequested()}
		case <-ticker.C:
			now := w.runtime.now()
			if progress := jobCtx.progressForFlush(w.runtime.cfg.ProgressFlushInterval, lastProgressFlush, now); progress != nil {
				_ = w.runtime.store.writeProgress(workerCtx, progressWriteRequest{
					JobID:      job.JobID,
					LeaseToken: job.LeaseToken,
					Progress:   *progress,
				})
				lastProgressFlush = now
				w.runtime.log(slog.LevelDebug, "job_progress",
					"job_id", job.JobID,
					"job_name", job.JobName,
					"queue", job.Queue,
					"attempt", job.Attempts,
					"stage", progress.Stage,
					"message", progress.Message,
					"current", progress.Current,
					"total", progress.Total,
					"percent", progress.Percent,
				)
				w.runtime.updates.publish(workerCtx, job.JobID)
			}
			hb, err := w.runtime.store.heartbeat(workerCtx, heartbeatRequest{
				JobID:         job.JobID,
				LeaseToken:    job.LeaseToken,
				LeaseDuration: w.cfg.LeaseDuration,
			})
			if err != nil || !hb.LeaseHeld {
				w.runtime.metrics.RecordLeaseRenewalFailure(job.Queue)
				w.runtime.log(slog.LevelWarn, "job_lease_renewal_failed",
					"job_id", job.JobID,
					"job_name", job.JobName,
					"queue", job.Queue,
					"attempt", job.Attempts,
					"error", err,
				)
				jobCtx.markLeaseLost()
				cancel()
				return heartbeatResult{LeaseHeld: false}
			}
			if hb.CancelRequested {
				jobCtx.markCancelRequested()
				cancel()
				return hb
			}
		}
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= 0 {
		return b
	}
	if b <= 0 {
		return a
	}
	if a < b {
		return a
	}
	return b
}
