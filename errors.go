package jobs

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrNotFound indicates that the job row does not exist.
	ErrNotFound = errors.New("jobs: not found")
	// ErrUnauthorized indicates that the supplied inspection/control scope is insufficient.
	ErrUnauthorized = errors.New("jobs: unauthorized")
	// ErrDuplicateHandler indicates duplicate registry handler registration.
	ErrDuplicateHandler = errors.New("jobs: duplicate handler")
	// ErrMissingHandler indicates that a claimed job has no registered handler.
	ErrMissingHandler = errors.New("jobs: missing handler")
	// ErrInvalidDefinition indicates that a job definition is malformed.
	ErrInvalidDefinition = errors.New("jobs: invalid definition")
	// ErrDuplicateActiveJob indicates a unique-key conflict with an active job.
	ErrDuplicateActiveJob = errors.New("jobs: duplicate active job")
	// ErrQueuePaused indicates an enqueue or control flow rejected due to queue pause rules.
	ErrQueuePaused = errors.New("jobs: queue paused")
	// ErrScheduleNotRegistered indicates an unknown schedule definition.
	ErrScheduleNotRegistered = errors.New("jobs: schedule not registered")
)

// BudgetKind identifies a configured storage budget.
type BudgetKind string

const (
	BudgetPayload  BudgetKind = "payload"
	BudgetMetadata BudgetKind = "metadata"
	BudgetOutput   BudgetKind = "output"
	BudgetProgress BudgetKind = "progress"
)

// BudgetError reports a durable size-budget violation.
type BudgetError struct {
	Kind     BudgetKind
	Observed int
	Limit    int
}

func (e *BudgetError) Error() string {
	switch e.Kind {
	case BudgetPayload, BudgetMetadata:
		return fmt.Sprintf("jobs: %s size %d exceeds limit %d; store durable references instead of embedding large blobs", e.Kind, e.Observed, e.Limit)
	case BudgetOutput:
		return fmt.Sprintf("jobs: %s size %d exceeds limit %d; write large artifacts out-of-band and return a reference", e.Kind, e.Observed, e.Limit)
	case BudgetProgress:
		return fmt.Sprintf("jobs: %s size %d exceeds limit %d; keep durable progress small and move detailed state to logs or external storage", e.Kind, e.Observed, e.Limit)
	default:
		return fmt.Sprintf("jobs: %s size %d exceeds limit %d", e.Kind, e.Observed, e.Limit)
	}
}

// Is allows errors.Is against sentinel typed errors.
func (e *BudgetError) Is(target error) bool {
	t, ok := target.(*BudgetError)
	if !ok {
		return false
	}
	return e.Kind == t.Kind
}

var (
	ErrPayloadTooLarge  error = &BudgetError{Kind: BudgetPayload}
	ErrMetadataTooLarge error = &BudgetError{Kind: BudgetMetadata}
	ErrOutputTooLarge   error = &BudgetError{Kind: BudgetOutput}
	ErrProgressTooLarge error = &BudgetError{Kind: BudgetProgress}
)

type jobError struct {
	err        error
	permanent  bool
	retryAfter *time.Duration
	code       string
	safe       string
}

func (e *jobError) Error() string {
	if e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *jobError) Unwrap() error {
	return e.err
}

// Permanent marks an error as terminal and non-retryable.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &jobError{err: err, permanent: true}
}

// RetryAfter marks an error as retryable with an explicit delay override.
func RetryAfter(err error, d time.Duration) error {
	if err == nil {
		return nil
	}
	return &jobError{err: err, retryAfter: &d}
}

// Code attaches a stable machine-readable error code.
func Code(err error, code string) error {
	if err == nil {
		return nil
	}
	return &jobError{err: err, code: code}
}

// Safe attaches the safe message that may be persisted and shown to operators.
func Safe(err error, safe string) error {
	if err == nil {
		return nil
	}
	return &jobError{err: err, safe: safe}
}

type errorDecision struct {
	permanent  bool
	retryAfter *time.Duration
	code       string
	safe       string
}

func classifyError(err error) errorDecision {
	var decision errorDecision
	for err != nil {
		var wrapped *jobError
		if errors.As(err, &wrapped) {
			if wrapped.permanent {
				decision.permanent = true
			}
			if wrapped.retryAfter != nil {
				d := *wrapped.retryAfter
				decision.retryAfter = &d
			}
			if wrapped.code != "" && decision.code == "" {
				decision.code = wrapped.code
			}
			if wrapped.safe != "" && decision.safe == "" {
				decision.safe = wrapped.safe
			}
			err = wrapped.Unwrap()
			continue
		}
		break
	}
	return decision
}
