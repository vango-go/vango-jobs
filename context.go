package jobs

import (
	"context"
	"log/slog"
)

// Ctx is the handler context exposed to worker functions.
type Ctx interface {
	context.Context
	JobID() string
	JobName() string
	Queue() string
	Attempt() int
	MaxAttempts() int
	TenantID() string
	Actor() Actor
	Metadata() map[string]string
	IdempotencyKey() string
	Logger() *slog.Logger
	Progress(p Progress) error
	Enqueue(def any, input any, opts ...EnqueueOption) (Ref, error)
}
