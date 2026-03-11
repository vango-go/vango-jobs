package jobs

// AttemptStatus is the status stored for a single execution attempt.
type AttemptStatus string

const (
	AttemptRunning        AttemptStatus = "running"
	AttemptSucceeded      AttemptStatus = "succeeded"
	AttemptFailed         AttemptStatus = "failed"
	AttemptCanceled       AttemptStatus = "canceled"
	AttemptAbandoned      AttemptStatus = "abandoned"
	AttemptRetryScheduled AttemptStatus = "retry_scheduled"
)

// EventKind is the durable lifecycle event kind.
type EventKind string

const (
	EventEnqueued  EventKind = "enqueued"
	EventStarted   EventKind = "started"
	EventProgress  EventKind = "progress"
	EventRetried   EventKind = "retried"
	EventSucceeded EventKind = "succeeded"
	EventFailed    EventKind = "failed"
	EventCanceled  EventKind = "canceled"
	EventAbandoned EventKind = "abandoned"
)
