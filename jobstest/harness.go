package jobstest

import (
	"context"
	"sync"
	"time"

	jobs "github.com/vango-go/vango-jobs"
)

// Option customizes a Harness.
type Option func(*config)

type config struct {
	start      time.Time
	immediate  bool
	workerOpts []jobs.WorkerOption
	schedOpts  []jobs.SchedulerOption
	runtime    []jobs.RuntimeOption
}

// WithStartTime sets the deterministic harness clock start.
func WithStartTime(t time.Time) Option {
	return func(cfg *config) {
		cfg.start = t.UTC()
	}
}

// WithImmediateExecution runs due scheduled and queued work after each enqueue.
func WithImmediateExecution(enabled bool) Option {
	return func(cfg *config) {
		cfg.immediate = enabled
	}
}

// WithWorkerOptions applies worker options to the harness worker.
func WithWorkerOptions(opts ...jobs.WorkerOption) Option {
	return func(cfg *config) {
		cfg.workerOpts = append(cfg.workerOpts, opts...)
	}
}

// WithSchedulerOptions applies scheduler options to the harness scheduler.
func WithSchedulerOptions(opts ...jobs.SchedulerOption) Option {
	return func(cfg *config) {
		cfg.schedOpts = append(cfg.schedOpts, opts...)
	}
}

// WithRuntimeOptions applies runtime options to the underlying jobs runtime.
func WithRuntimeOptions(opts ...jobs.RuntimeOption) Option {
	return func(cfg *config) {
		cfg.runtime = append(cfg.runtime, opts...)
	}
}

// Harness owns a deterministic in-memory jobs runtime.
type Harness struct {
	Runtime   *jobs.Runtime
	Worker    *jobs.Worker
	Scheduler *jobs.Scheduler

	clock     *clock
	immediate bool
}

// New constructs a deterministic jobs harness.
func New(reg *jobs.Registry, opts ...Option) (*Harness, error) {
	cfg := config{
		start: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	clk := &clock{now: cfg.start}
	runtimeOpts := append([]jobs.RuntimeOption{jobs.WithClock(clk.Now)}, cfg.runtime...)
	rt, err := jobs.NewInMemory(reg, runtimeOpts...)
	if err != nil {
		return nil, err
	}
	h := &Harness{
		Runtime:   rt,
		Worker:    rt.NewWorker(cfg.workerOpts...),
		Scheduler: rt.NewScheduler(cfg.schedOpts...),
		clock:     clk,
		immediate: cfg.immediate,
	}
	return h, nil
}

// Now returns the deterministic harness clock.
func (h *Harness) Now() time.Time {
	return h.clock.Now()
}

// Advance moves the deterministic harness clock forward.
func (h *Harness) Advance(d time.Duration) {
	h.clock.Advance(d)
}

// Enqueue delegates to the runtime and optionally drains due work immediately.
func (h *Harness) Enqueue(ctx context.Context, def any, input any, opts ...jobs.EnqueueOption) (jobs.Ref, error) {
	ref, err := h.Runtime.Enqueue(ctx, def, input, opts...)
	if err != nil || !h.immediate {
		return ref, err
	}
	_, err = h.RunAll(ctx)
	return ref, err
}

// RunNext processes one due scheduler batch and one worker batch.
func (h *Harness) RunNext(ctx context.Context) (bool, error) {
	scheduled, err := h.Scheduler.RunOnce(ctx)
	if err != nil {
		return false, err
	}
	ran, err := h.Worker.RunOnce(ctx)
	if err != nil {
		return false, err
	}
	return scheduled+ran > 0, nil
}

// RunAll drains due scheduled and queued work until quiescent.
func (h *Harness) RunAll(ctx context.Context) (int, error) {
	total := 0
	for {
		ran, err := h.RunNext(ctx)
		if err != nil {
			return total, err
		}
		if !ran {
			return total, nil
		}
		total++
	}
}

// Close closes the underlying runtime.
func (h *Harness) Close() error {
	if h == nil || h.Runtime == nil {
		return nil
	}
	return h.Runtime.Close()
}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
