package jobs

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStoreReapRequeuesExpiredRunningJob(t *testing.T) {
	current := time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC)
	cfg := defaultConfig()
	cfg.Now = func() time.Time { return current }
	store := newMemoryStore(cfg)

	ref, err := store.enqueue(context.Background(), enqueueRequest{
		ID:             store.ids.newJobID(current),
		Name:           "memory.reap.v1",
		Queue:          "default",
		MaxAttempts:    3,
		Timeout:        time.Minute,
		RunAt:          current,
		Payload:        []byte(`{}`),
		ConcurrencyKey: "org:123",
	})
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := store.claim(context.Background(), claimRequest{
		WorkerID:      "worker:test",
		Limit:         1,
		OverscanLimit: 1,
		LeaseDuration: time.Second,
		JobNames:      []string{"memory.reap.v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed=%d want 1", len(claimed))
	}

	current = current.Add(2 * time.Second)
	reaped, err := store.reap(context.Background(), reapRequest{
		Limit: 1,
		Backoff: func(name, jobID string, attempt int) time.Duration {
			if name != "memory.reap.v1" || jobID != ref.ID || attempt != 1 {
				t.Fatalf("unexpected backoff args name=%s jobID=%s attempt=%d", name, jobID, attempt)
			}
			return 5 * time.Second
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped) != 1 {
		t.Fatalf("reaped=%d want 1", len(reaped))
	}
	if reaped[0].Outcome != EventRetried {
		t.Fatalf("outcome=%s want retried", reaped[0].Outcome)
	}

	snap, err := store.inspect(context.Background(), ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != StatusQueued {
		t.Fatalf("status=%s want queued", snap.Status)
	}
	if got, want := snap.RunAt, current.Add(5*time.Second); !got.Equal(want) {
		t.Fatalf("run_at=%s want %s", got, want)
	}
	if snap.SafeError != "job lease expired before completion" {
		t.Fatalf("safe_error=%q want abandoned message", snap.SafeError)
	}
	if snap.ErrorCode != "job_abandoned" {
		t.Fatalf("error_code=%q want job_abandoned", snap.ErrorCode)
	}
	if snap.WorkerID != "" {
		t.Fatalf("worker_id=%q want empty", snap.WorkerID)
	}
	if snap.LeaseExpiresAt != nil {
		t.Fatalf("lease_expires_at=%v want nil", snap.LeaseExpiresAt)
	}
	if _, ok := store.concurrency["org:123"]; ok {
		t.Fatalf("concurrency slot still held after reap")
	}
	attempts := store.attemptRows[ref.ID]
	if len(attempts) != 1 {
		t.Fatalf("attempt rows=%d want 1", len(attempts))
	}
	if attempts[0].Status != AttemptAbandoned {
		t.Fatalf("attempt status=%s want abandoned", attempts[0].Status)
	}
}

func TestMemoryStoreReapHonorsCancellationOnExpiredRunningJob(t *testing.T) {
	current := time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC)
	cfg := defaultConfig()
	cfg.Now = func() time.Time { return current }
	store := newMemoryStore(cfg)

	ref, err := store.enqueue(context.Background(), enqueueRequest{
		ID:          store.ids.newJobID(current),
		Name:        "memory.cancel.v1",
		Queue:       "default",
		MaxAttempts: 3,
		Timeout:     time.Minute,
		RunAt:       current,
		Payload:     []byte(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := store.claim(context.Background(), claimRequest{
		WorkerID:      "worker:test",
		Limit:         1,
		OverscanLimit: 1,
		LeaseDuration: time.Second,
		JobNames:      []string{"memory.cancel.v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed=%d want 1", len(claimed))
	}
	if err := store.cancel(context.Background(), ref.ID); err != nil {
		t.Fatal(err)
	}

	current = current.Add(2 * time.Second)
	reaped, err := store.reap(context.Background(), reapRequest{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped) != 1 {
		t.Fatalf("reaped=%d want 1", len(reaped))
	}
	if reaped[0].Outcome != EventCanceled {
		t.Fatalf("outcome=%s want canceled", reaped[0].Outcome)
	}

	snap, err := store.inspect(context.Background(), ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != StatusCanceled {
		t.Fatalf("status=%s want canceled", snap.Status)
	}
	if snap.FinishedAt == nil || !snap.FinishedAt.Equal(current) {
		t.Fatalf("finished_at=%v want %s", snap.FinishedAt, current)
	}
	attempts := store.attemptRows[ref.ID]
	if len(attempts) != 1 {
		t.Fatalf("attempt rows=%d want 1", len(attempts))
	}
	if attempts[0].Status != AttemptAbandoned {
		t.Fatalf("attempt status=%s want abandoned", attempts[0].Status)
	}
}
