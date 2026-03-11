package jobs

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

// SchedulerOption customizes the durable schedule runner.
type SchedulerOption func(*SchedulerConfig)

// SchedulerConfig controls scheduler loop behavior.
type SchedulerConfig struct {
	PollInterval time.Duration
	BatchSize    int
}

func defaultSchedulerConfig(cfg Config) SchedulerConfig {
	return SchedulerConfig{
		PollInterval: cfg.SchedulePollInterval,
		BatchSize:    16,
	}
}

// WithSchedulePollInterval sets the scheduler poll interval.
func WithSchedulePollInterval(d time.Duration) SchedulerOption {
	return func(cfg *SchedulerConfig) {
		if d > 0 {
			cfg.PollInterval = d
		}
	}
}

// WithScheduleBatchSize sets the scheduler due-row batch size.
func WithScheduleBatchSize(n int) SchedulerOption {
	return func(cfg *SchedulerConfig) {
		if n > 0 {
			cfg.BatchSize = n
		}
	}
}

// Scheduler emits ordinary jobs from durable schedule rows.
type Scheduler struct {
	runtime *Runtime
	cfg     SchedulerConfig
}

// NewScheduler creates a durable schedule runner.
func (r *Runtime) NewScheduler(opts ...SchedulerOption) *Scheduler {
	cfg := defaultSchedulerConfig(r.cfg)
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return &Scheduler{runtime: r, cfg: cfg}
}

// Run synchronizes registered schedules and continuously fires due rows.
func (s *Scheduler) Run(ctx context.Context) error {
	schedules := s.runtime.registry.schedulesList()
	if err := s.runtime.store.ensureSchedules(ctx, schedules); err != nil {
		return err
	}
	scheduleMap := make(map[string]registeredSchedule, len(schedules))
	for _, schedule := range schedules {
		scheduleMap[schedule.name()] = schedule
	}
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if _, err := s.runTick(ctx, scheduleMap); err != nil && ctx.Err() == nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// RunOnce synchronizes schedules and fires one due batch.
func (s *Scheduler) RunOnce(ctx context.Context) (int, error) {
	schedules := s.runtime.registry.schedulesList()
	if err := s.runtime.store.ensureSchedules(ctx, schedules); err != nil {
		return 0, err
	}
	scheduleMap := make(map[string]registeredSchedule, len(schedules))
	for _, schedule := range schedules {
		scheduleMap[schedule.name()] = schedule
	}
	return s.runTick(ctx, scheduleMap)
}

func (s *Scheduler) runTick(ctx context.Context, scheduleMap map[string]registeredSchedule) (int, error) {
	now := s.runtime.now()
	ctx, span := s.runtime.startSpan(ctx, "jobs.scheduler.tick", attribute.Int("jobs.scheduler.batch_size", s.cfg.BatchSize))
	defer span.End()
	refs, err := s.runtime.store.tickSchedules(ctx, scheduleTickRequest{
		Now:         s.runtime.now(),
		Limit:       s.cfg.BatchSize,
		NewJobID:    s.runtime.ids.newJobID,
		Schedules:   scheduleMap,
		Codec:       s.runtime.cfg.Codec,
		CheckBudget: s.runtime.checkBudget,
	})
	if err != nil {
		s.runtime.recordSpanError(span, err)
		return 0, err
	}
	for _, scheduled := range refs {
		s.runtime.updates.publish(ctx, scheduled.Ref.ID)
		if !scheduled.Inserted {
			continue
		}
		lag := maybeDuration(scheduled.DueAt, now)
		s.runtime.metrics.RecordEnqueued(scheduled.Ref.Name, scheduled.Queue)
		s.runtime.metrics.RecordScheduleLag(scheduled.ScheduleName, lag)
		s.runtime.log(slog.LevelInfo, "job_enqueued",
			"job_id", scheduled.Ref.ID,
			"job_name", scheduled.Ref.Name,
			"queue", scheduled.Queue,
			"schedule", scheduled.ScheduleName,
			"scheduled_for", scheduled.DueAt,
			"schedule_lag", lag,
		)
	}
	for _, scheduled := range refs {
		if scheduled.Inserted {
			s.runtime.refreshQueueMetrics(ctx, scheduled.Queue)
		}
	}
	return len(refs), nil
}
