package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// ScheduleOption customizes durable schedule behavior.
type ScheduleOption func(*scheduleConfig)

type scheduleConfig struct {
	spec      string
	timezone  *time.Location
	paused    bool
	input     any
	inputFunc func(context.Context, time.Time) (any, error)
}

// Schedule is a durable schedule definition that emits ordinary jobs.
type Schedule[In any, Out any] struct {
	scheduleName string
	target       *Definition[In, Out]
	cfg          scheduleConfig
}

// NewSchedule creates a durable schedule definition.
func NewSchedule[In any, Out any](name string, target *Definition[In, Out], opts ...ScheduleOption) *Schedule[In, Out] {
	cfg := scheduleConfig{
		timezone: time.UTC,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return &Schedule[In, Out]{
		scheduleName: normalizeName(name),
		target:       target,
		cfg:          cfg,
	}
}

// Cron configures the cron expression.
func Cron(expr string) ScheduleOption {
	return func(cfg *scheduleConfig) {
		cfg.spec = expr
	}
}

// ScheduleTimezone configures the schedule timezone.
func ScheduleTimezone(loc *time.Location) ScheduleOption {
	return func(cfg *scheduleConfig) {
		if loc != nil {
			cfg.timezone = loc
		}
	}
}

// ScheduleInput configures the static payload for scheduled enqueues.
func ScheduleInput[In any](input In) ScheduleOption {
	return func(cfg *scheduleConfig) {
		cfg.input = input
		cfg.inputFunc = nil
	}
}

// ScheduleInputFunc configures a payload factory run when a schedule fires.
func ScheduleInputFunc[In any](fn func(context.Context, time.Time) (In, error)) ScheduleOption {
	return func(cfg *scheduleConfig) {
		if fn == nil {
			cfg.inputFunc = nil
			return
		}
		cfg.inputFunc = func(ctx context.Context, at time.Time) (any, error) {
			return fn(ctx, at)
		}
	}
}

type registeredSchedule interface {
	name() string
	targetName() string
	targetDefinition() definitionContract
	spec() string
	timezone() *time.Location
	next(after time.Time) (time.Time, error)
	inputFor(ctx context.Context, runAt time.Time) (any, error)
}

// RegisterSchedule registers a durable schedule at startup.
func (r *Registry) RegisterSchedule(schedule any) error {
	if r == nil {
		return errors.New("jobs: registry is nil")
	}
	registered, ok := schedule.(registeredSchedule)
	if !ok || schedule == nil {
		return errors.Join(ErrScheduleNotRegistered, errorString("schedule must not be nil"))
	}
	if registered.targetName() == "" {
		return errors.Join(ErrScheduleNotRegistered, errorString("schedule must not be nil"))
	}
	if registered.name() == "" {
		return errors.Join(ErrScheduleNotRegistered, errorString("schedule name must not be empty"))
	}
	if registered.spec() == "" {
		return errors.Join(ErrScheduleNotRegistered, errorString("schedule spec must not be empty"))
	}
	if _, err := cron.ParseStandard(registered.spec()); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.schedules[registered.name()]; exists {
		return fmt.Errorf("%w: schedule %s", ErrDuplicateHandler, registered.name())
	}
	r.schedules[registered.name()] = registered
	return nil
}

func (r *Registry) schedulesList() []registeredSchedule {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]registeredSchedule, 0, len(r.schedules))
	for _, sched := range r.schedules {
		out = append(out, sched)
	}
	return out
}

func (s *Schedule[In, Out]) name() string {
	return s.scheduleName
}

func (s *Schedule[In, Out]) targetName() string {
	if s.target == nil {
		return ""
	}
	return s.target.Name()
}

func (s *Schedule[In, Out]) targetDefinition() definitionContract {
	if s.target == nil {
		return nil
	}
	return s.target.contract()
}

func (s *Schedule[In, Out]) spec() string {
	return s.cfg.spec
}

func (s *Schedule[In, Out]) timezone() *time.Location {
	return s.cfg.timezone
}

func (s *Schedule[In, Out]) next(after time.Time) (time.Time, error) {
	parser, err := cron.ParseStandard(s.cfg.spec)
	if err != nil {
		return time.Time{}, err
	}
	return parser.Next(after.In(s.cfg.timezone)).UTC(), nil
}

func (s *Schedule[In, Out]) inputFor(ctx context.Context, runAt time.Time) (any, error) {
	if s.cfg.inputFunc != nil {
		return s.cfg.inputFunc(ctx, runAt)
	}
	if s.cfg.input != nil {
		return s.cfg.input, nil
	}
	var zero In
	return zero, nil
}
