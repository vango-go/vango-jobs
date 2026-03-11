package jobs

import (
	"hash/fnv"
	"math"
	"time"
)

// BackoffStrategy computes the next delay after an attempt failure.
type BackoffStrategy interface {
	Next(jobID string, attempt int, err error) time.Duration
}

type backoffFunc func(jobID string, attempt int, err error) time.Duration

func (f backoffFunc) Next(jobID string, attempt int, err error) time.Duration {
	return f(jobID, attempt, err)
}

// ExponentialJitter returns a deterministic exponential backoff with bounded jitter.
func ExponentialJitter(min, max time.Duration) BackoffStrategy {
	if min <= 0 {
		min = 5 * time.Second
	}
	if max < min {
		max = min
	}
	return backoffFunc(func(jobID string, attempt int, err error) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		base := float64(min)
		pow := math.Pow(2, float64(attempt-1))
		delay := time.Duration(base * pow)
		if delay > max {
			delay = max
		}
		jitter := deterministicJitter(jobID, attempt, delay/2)
		out := delay - (delay / 4) + jitter
		if out < min {
			return min
		}
		if out > max {
			return max
		}
		return out
	})
}

func deterministicJitter(jobID string, attempt int, limit time.Duration) time.Duration {
	if limit <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(jobID))
	_, _ = h.Write([]byte{byte(attempt), byte(attempt >> 8), byte(attempt >> 16), byte(attempt >> 24)})
	return time.Duration(h.Sum64() % uint64(limit))
}
