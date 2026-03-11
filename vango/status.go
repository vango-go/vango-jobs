package jobsvango

import (
	"context"
	"time"

	jobs "github.com/vango-go/vango-jobs"
	corevango "github.com/vango-go/vango/pkg/vango"
	"github.com/vango-go/vango/setup"
)

type statusConfig[K comparable] struct {
	pollInterval time.Duration
	runtime      *jobs.Runtime
	keyToJobID   func(K) string
}

// StatusOption customizes status resource behavior.
type StatusOption[K comparable] func(*statusConfig[K])

// WithPollInterval enables periodic refetch while a job is active.
func WithPollInterval[K comparable](d time.Duration) StatusOption[K] {
	return func(cfg *statusConfig[K]) {
		if d > 0 {
			cfg.pollInterval = d
		}
	}
}

// WithLiveUpdates invalidates the resource when the runtime publishes updates
// for the derived job ID.
func WithLiveUpdates[K comparable](runtime *jobs.Runtime, keyToJobID func(K) string) StatusOption[K] {
	return func(cfg *statusConfig[K]) {
		cfg.runtime = runtime
		cfg.keyToJobID = keyToJobID
	}
}

// StatusKeyed wraps setup.ResourceKeyed for typed job snapshots.
func StatusKeyed[P any, K comparable, Out any](
	s *corevango.SetupCtx[P],
	key func() K,
	load func(context.Context, K) (*jobs.Snapshot[Out], error),
	opts ...StatusOption[K],
) *corevango.Resource[*jobs.Snapshot[Out]] {
	cfg := statusConfig[K]{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	resource := setup.ResourceKeyed(s, key, load)

	if cfg.runtime != nil && cfg.keyToJobID != nil {
		s.Effect(func() corevango.Cleanup {
			jobID := cfg.keyToJobID(key())
			if jobID == "" {
				return nil
			}
			return corevango.Subscribe(jobUpdateStream{runtime: cfg.runtime, jobID: jobID}, func(struct{}) {
				resource.Invalidate()
				resource.Fetch()
			})
		})
	}

	if cfg.pollInterval > 0 {
		s.Effect(func() corevango.Cleanup {
			if !shouldPoll(resource) {
				return nil
			}
			return corevango.Interval(cfg.pollInterval, func() {
				resource.Invalidate()
				resource.Fetch()
			})
		})
	}

	return resource
}

// Status is the string-keyed convenience wrapper over StatusKeyed.
func Status[P any, Out any](
	s *corevango.SetupCtx[P],
	id func() string,
	load func(context.Context, string) (*jobs.Snapshot[Out], error),
	opts ...StatusOption[string],
) *corevango.Resource[*jobs.Snapshot[Out]] {
	return StatusKeyed(s, id, load, opts...)
}

func shouldPoll[Out any](resource *corevango.Resource[*jobs.Snapshot[Out]]) bool {
	if resource == nil || !resource.IsReady() {
		return false
	}
	snapshot := resource.Data()
	if snapshot == nil {
		return false
	}
	return snapshot.Status == jobs.StatusQueued || snapshot.Status == jobs.StatusRunning
}

type jobUpdateStream struct {
	runtime *jobs.Runtime
	jobID   string
}

func (s jobUpdateStream) Subscribe(handler func(struct{})) func() {
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := s.runtime.SubscribeJob(ctx, s.jobID)
	if err != nil {
		cancel()
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
			handler(struct{}{})
		}
	}()
	return func() {
		cancel()
		<-done
	}
}
