package jobs

import "time"

// Actor captures durable actor metadata for audit and authorization.
type Actor struct {
	ID   string `json:"id,omitempty"`
	Kind string `json:"kind,omitempty"`
}

// Empty reports whether the actor has no durable identity.
func (a Actor) Empty() bool {
	return a.ID == "" && a.Kind == ""
}

// Progress is the bounded durable progress payload exposed to observers.
type Progress struct {
	Stage    string            `json:"stage,omitempty"`
	Message  string            `json:"message,omitempty"`
	Current  int64             `json:"current,omitempty"`
	Total    int64             `json:"total,omitempty"`
	Percent  float64           `json:"percent,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Ref is the durable handle returned from enqueue.
type Ref struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Snapshot is the typed immutable status view returned to callers.
type Snapshot[Out any] struct {
	Ref               Ref
	Queue             string
	Status            Status
	TenantID          string
	Actor             Actor
	Attempts          int
	MaxAttempts       int
	RunAt             time.Time
	CreatedAt         time.Time
	FirstStartedAt    *time.Time
	LastStartedAt     *time.Time
	FinishedAt        *time.Time
	CancelRequestedAt *time.Time
	Progress          *Progress
	Output            *Out
	SafeError         string
	ErrorCode         string
	ParentJobID       string
	RetriedFromJobID  string
}

// RawSnapshot is the untyped durable job view used by admin and low-level code.
type RawSnapshot struct {
	Ref               Ref
	Queue             string
	Status            Status
	TenantID          string
	Actor             Actor
	Attempts          int
	MaxAttempts       int
	RunAt             time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
	FirstStartedAt    *time.Time
	LastStartedAt     *time.Time
	FinishedAt        *time.Time
	CancelRequestedAt *time.Time
	LeaseExpiresAt    *time.Time
	WorkerID          string
	Priority          int
	UniqueKey         string
	ConcurrencyKey    string
	ParentJobID       string
	RetriedFromJobID  string
	TraceID           string
	Tags              []string
	Metadata          map[string]string
	Payload           []byte
	Output            []byte
	Progress          *Progress
	SafeError         string
	ErrorCode         string
}

// Clone returns a detached copy safe for callers to retain and mutate.
func (s *RawSnapshot) Clone() *RawSnapshot {
	if s == nil {
		return nil
	}
	out := *s
	if len(s.Tags) > 0 {
		out.Tags = append([]string(nil), s.Tags...)
	}
	if len(s.Metadata) > 0 {
		out.Metadata = copyMetadata(s.Metadata)
	}
	if len(s.Payload) > 0 {
		out.Payload = append([]byte(nil), s.Payload...)
	}
	if len(s.Output) > 0 {
		out.Output = append([]byte(nil), s.Output...)
	}
	if s.Progress != nil {
		cp := *s.Progress
		cp.Metadata = copyMetadata(s.Progress.Metadata)
		out.Progress = &cp
	}
	return &out
}

// AttemptSnapshot is a durable execution-attempt history row.
type AttemptSnapshot struct {
	ID         string
	JobID      string
	Attempt    int
	WorkerID   string
	LeaseToken string
	Status     AttemptStatus
	StartedAt  time.Time
	EndedAt    *time.Time
	Duration   time.Duration
	SafeError  string
	ErrorCode  string
	RetryDelay time.Duration
	CreatedAt  time.Time
}

// EventSnapshot is a bounded lifecycle event row.
type EventSnapshot struct {
	ID      int64
	JobID   string
	Kind    EventKind
	At      time.Time
	Payload []byte
}

// QueueSnapshot is the durable admin view of a queue.
type QueueSnapshot struct {
	Queue       string
	Paused      bool
	PauseReason string
	UpdatedAt   time.Time
	Queued      int
	Running     int
}

// ScheduleSnapshot is the durable admin view of a registered schedule row.
type ScheduleSnapshot struct {
	Name          string
	TargetJobName string
	Spec          string
	Timezone      string
	Paused        bool
	NextRunAt     time.Time
	LastRunAt     *time.Time
	LastJobID     string
	UpdatedAt     time.Time
}

// PruneSummary describes one prune pass over retained terminal jobs.
type PruneSummary struct {
	Deleted int
	JobIDs  []string
}
