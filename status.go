package jobs

// Status is the user-facing durable job status.
type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusCanceled  Status = "canceled"
)

func (s Status) String() string {
	return string(s)
}

// Terminal reports whether the status is final.
func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCanceled:
		return true
	default:
		return false
	}
}

func validStatus(s Status) bool {
	switch s {
	case StatusQueued, StatusRunning, StatusSucceeded, StatusFailed, StatusCanceled:
		return true
	default:
		return false
	}
}
