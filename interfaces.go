package jobs

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Enqueuer is the low-level enqueue contract.
type Enqueuer interface {
	Enqueue(ctx context.Context, def any, input any, opts ...EnqueueOption) (Ref, error)
	EnqueueTx(ctx context.Context, tx pgx.Tx, def any, input any, opts ...EnqueueOption) (Ref, error)
}

// Reader is the untyped inspection contract.
type Reader interface {
	Inspect(ctx context.Context, id string, opts ...InspectOption) (*RawSnapshot, error)
}

// TypedReader is the typed lookup contract.
type TypedReader interface {
	Lookup(ctx context.Context, def any, id string, opts ...InspectOption) (any, error)
}

// JobReader exposes durable job listings for admin/status surfaces.
type JobReader interface {
	List(ctx context.Context, filter ListFilter, opts ...InspectOption) ([]*RawSnapshot, error)
}

// HistoryReader exposes durable attempt and event history for one job.
type HistoryReader interface {
	Attempts(ctx context.Context, id string, opts ...InspectOption) ([]AttemptSnapshot, error)
	Events(ctx context.Context, id string, opts ...InspectOption) ([]EventSnapshot, error)
}

// Controller is the admin control contract.
type Controller interface {
	Cancel(ctx context.Context, id string, opts ...ControlOption) error
	Retry(ctx context.Context, id string, opts ...RetryOption) (Ref, error)
	PauseQueue(ctx context.Context, queue string, reason string, opts ...ControlOption) error
	ResumeQueue(ctx context.Context, queue string, opts ...ControlOption) error
}

// QueueReader exposes queue-level administrative state.
type QueueReader interface {
	Queue(ctx context.Context, queue string, opts ...ControlOption) (*QueueSnapshot, error)
	Queues(ctx context.Context, opts ...ControlOption) ([]QueueSnapshot, error)
}

// ScheduleController exposes durable schedule administration.
type ScheduleController interface {
	PauseSchedule(ctx context.Context, name string, opts ...ControlOption) error
	ResumeSchedule(ctx context.Context, name string, opts ...ControlOption) error
	Schedule(ctx context.Context, name string, opts ...ControlOption) (*ScheduleSnapshot, error)
	Schedules(ctx context.Context, opts ...ControlOption) ([]ScheduleSnapshot, error)
}

// Pruner deletes terminal jobs whose retention windows have expired.
type Pruner interface {
	Prune(ctx context.Context, opts ...PruneOption) (*PruneSummary, error)
}
