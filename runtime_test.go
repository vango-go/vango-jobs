package jobs_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	jobs "github.com/vango-go/vango-jobs"
	"github.com/vango-go/vango-jobs/jobstest"
)

type echoIn struct {
	Message string `json:"message"`
}

type echoOut struct {
	Message string `json:"message"`
}

type testBroadcast struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
}

func newTestBroadcast() *testBroadcast {
	return &testBroadcast{subs: make(map[chan []byte]struct{})}
}

func (b *testBroadcast) Publish(ctx context.Context, channel string, data []byte) error {
	b.mu.Lock()
	targets := make([]chan []byte, 0, len(b.subs))
	for ch := range b.subs {
		targets = append(targets, ch)
	}
	b.mu.Unlock()
	for _, ch := range targets {
		select {
		case ch <- append([]byte(nil), data...):
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

func (b *testBroadcast) Subscribe(ctx context.Context, channel string) (<-chan []byte, error) {
	ch := make(chan []byte, 8)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	go func() {
		<-ctx.Done()
		b.mu.Lock()
		if _, ok := b.subs[ch]; ok {
			delete(b.subs, ch)
			close(ch)
		}
		b.mu.Unlock()
	}()
	return ch, nil
}

var (
	echoJob = jobs.New[echoIn, echoOut](
		"test.echo.v1",
		jobs.Queue("emails"),
		jobs.MaxAttempts(3),
		jobs.Timeout(30*time.Second),
	)
	retryJob = jobs.New[echoIn, echoOut](
		"test.retry.v1",
		jobs.Queue("retries"),
		jobs.MaxAttempts(3),
		jobs.Timeout(30*time.Second),
		jobs.Backoff(jobs.ExponentialJitter(time.Second, time.Second)),
	)
	pruneJob = jobs.New[struct{}, struct{}](
		"test.prune.v1",
		jobs.Retention(time.Second),
	)
)

func TestInMemoryWorkerCompletesJob(t *testing.T) {
	reg := jobs.NewRegistry()
	reg.MustHandle(echoJob, func(ctx jobs.Ctx, in echoIn) (echoOut, error) {
		return echoOut{Message: in.Message}, nil
	})

	h, err := jobstest.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	ref, err := h.Enqueue(context.Background(), echoJob, echoIn{Message: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.RunAll(context.Background()); err != nil {
		t.Fatal(err)
	}

	snap, err := jobs.LookupTyped(context.Background(), h.Runtime, echoJob, ref.ID, jobs.InspectAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != jobs.StatusSucceeded {
		t.Fatalf("status=%s want %s", snap.Status, jobs.StatusSucceeded)
	}
	if snap.Output == nil || snap.Output.Message != "hello" {
		t.Fatalf("output=%v want hello", snap.Output)
	}
}

func TestRetryFlowRequeuesAndSucceeds(t *testing.T) {
	reg := jobs.NewRegistry()
	attempts := 0
	reg.MustHandle(retryJob, func(ctx jobs.Ctx, in echoIn) (echoOut, error) {
		attempts++
		if attempts == 1 {
			return echoOut{}, jobs.Safe(errors.New("boom"), "retry later")
		}
		return echoOut{Message: "done"}, nil
	})

	h, err := jobstest.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	ref, err := h.Enqueue(context.Background(), retryJob, echoIn{Message: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if ran, err := h.RunNext(context.Background()); err != nil || !ran {
		t.Fatalf("RunNext ran=%v err=%v", ran, err)
	}

	snap, err := jobs.LookupTyped(context.Background(), h.Runtime, retryJob, ref.ID, jobs.InspectAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != jobs.StatusQueued {
		t.Fatalf("status=%s want queued", snap.Status)
	}
	if snap.Attempts != 1 {
		t.Fatalf("attempts=%d want 1", snap.Attempts)
	}

	h.Advance(time.Second)
	if _, err := h.RunAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	snap, err = jobs.LookupTyped(context.Background(), h.Runtime, retryJob, ref.ID, jobs.InspectAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != jobs.StatusSucceeded {
		t.Fatalf("status=%s want succeeded", snap.Status)
	}
	if snap.Attempts != 2 {
		t.Fatalf("attempts=%d want 2", snap.Attempts)
	}
}

func TestListFiltersRecentJobs(t *testing.T) {
	reg := jobs.NewRegistry()
	reg.MustHandle(echoJob, func(ctx jobs.Ctx, in echoIn) (echoOut, error) {
		return echoOut{Message: in.Message}, nil
	})
	reg.MustHandle(retryJob, func(ctx jobs.Ctx, in echoIn) (echoOut, error) {
		return echoOut{}, jobs.Permanent(jobs.Safe(errors.New("boom"), "permanent failure"))
	})

	h, err := jobstest.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	successRef, err := h.Enqueue(context.Background(), echoJob, echoIn{Message: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	h.Advance(time.Second)
	failedRef, err := h.Enqueue(context.Background(), retryJob, echoIn{Message: "fail"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.RunAll(context.Background()); err != nil {
		t.Fatal(err)
	}

	items, err := h.Runtime.List(context.Background(), jobs.ListFilter{
		Statuses: []jobs.Status{jobs.StatusFailed},
		Limit:    10,
	}, jobs.InspectAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("len(items)=%d want 1", len(items))
	}
	if items[0].Ref.ID != failedRef.ID || items[0].Status != jobs.StatusFailed {
		t.Fatalf("failed item=%+v want id=%s failed", items[0], failedRef.ID)
	}

	items, err = h.Runtime.List(context.Background(), jobs.ListFilter{Limit: 10}, jobs.InspectAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("len(items)=%d want 2", len(items))
	}
	if items[0].Ref.ID != failedRef.ID || items[1].Ref.ID != successRef.ID {
		t.Fatalf("unexpected order: got %s then %s", items[0].Ref.ID, items[1].Ref.ID)
	}
}

func TestListSupportsTerminalTimeOrdering(t *testing.T) {
	reg := jobs.NewRegistry()
	reg.MustHandle(retryJob, func(ctx jobs.Ctx, in echoIn) (echoOut, error) {
		return echoOut{}, jobs.Permanent(jobs.Safe(errors.New(in.Message), in.Message))
	})

	h, err := jobstest.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	olderRef, err := h.Enqueue(context.Background(), retryJob, echoIn{Message: "older-failed-later"}, jobs.WithDelay(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	h.Advance(time.Second)
	newerRef, err := h.Enqueue(context.Background(), retryJob, echoIn{Message: "newer-failed-earlier"})
	if err != nil {
		t.Fatal(err)
	}

	if ran, err := h.RunNext(context.Background()); err != nil || !ran {
		t.Fatalf("RunNext ran=%v err=%v", ran, err)
	}

	h.Advance(time.Second)
	if ran, err := h.RunNext(context.Background()); err != nil || !ran {
		t.Fatalf("second RunNext ran=%v err=%v", ran, err)
	}

	defaultOrder, err := h.Runtime.List(context.Background(), jobs.ListFilter{
		Statuses: []jobs.Status{jobs.StatusFailed},
		Limit:    10,
	}, jobs.InspectAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if len(defaultOrder) != 2 {
		t.Fatalf("len(defaultOrder)=%d want 2", len(defaultOrder))
	}
	if defaultOrder[0].Ref.ID != newerRef.ID || defaultOrder[1].Ref.ID != olderRef.ID {
		t.Fatalf("default order got %s then %s, want newer-created %s then %s", defaultOrder[0].Ref.ID, defaultOrder[1].Ref.ID, newerRef.ID, olderRef.ID)
	}

	terminalOrder, err := h.Runtime.List(context.Background(), jobs.ListFilter{
		Statuses: []jobs.Status{jobs.StatusFailed},
		Limit:    10,
		Order:    jobs.ListOrderTerminalTimeDesc,
	}, jobs.InspectAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if len(terminalOrder) != 2 {
		t.Fatalf("len(terminalOrder)=%d want 2", len(terminalOrder))
	}
	if terminalOrder[0].Ref.ID != olderRef.ID || terminalOrder[1].Ref.ID != newerRef.ID {
		t.Fatalf("terminal order got %s then %s, want older-failed-later %s then %s", terminalOrder[0].Ref.ID, terminalOrder[1].Ref.ID, olderRef.ID, newerRef.ID)
	}
}

func TestUniqueKeyReturnsExistingRef(t *testing.T) {
	reg := jobs.NewRegistry()
	reg.MustHandle(echoJob, func(ctx jobs.Ctx, in echoIn) (echoOut, error) {
		return echoOut{Message: in.Message}, nil
	})

	h, err := jobstest.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	first, err := h.Enqueue(context.Background(), echoJob, echoIn{Message: "a"}, jobs.WithUnique("same", jobs.UniqueReturnExisting))
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.Enqueue(context.Background(), echoJob, echoIn{Message: "b"}, jobs.WithUnique("same", jobs.UniqueReturnExisting))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("first=%s second=%s want same ref", first.ID, second.ID)
	}
}

func TestCancelQueuedJob(t *testing.T) {
	reg := jobs.NewRegistry()
	reg.MustHandle(echoJob, func(ctx jobs.Ctx, in echoIn) (echoOut, error) {
		return echoOut{Message: in.Message}, nil
	})

	h, err := jobstest.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	ref, err := h.Enqueue(context.Background(), echoJob, echoIn{Message: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Runtime.Cancel(context.Background(), ref.ID, jobs.ControlAdmin()); err != nil {
		t.Fatal(err)
	}
	snap, err := h.Runtime.Inspect(context.Background(), ref.ID, jobs.InspectAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != jobs.StatusCanceled {
		t.Fatalf("status=%s want canceled", snap.Status)
	}
}

func TestSchedulerEnqueuesDueJobs(t *testing.T) {
	cleanupJob := jobs.New[struct{}, struct{}]("test.cleanup.v1")
	schedule := jobs.NewSchedule("cleanup.daily.v1", cleanupJob, jobs.Cron("*/1 * * * *"))

	reg := jobs.NewRegistry()
	reg.MustHandle(cleanupJob, func(ctx jobs.Ctx, in struct{}) (struct{}, error) {
		return struct{}{}, nil
	})
	if err := reg.RegisterSchedule(schedule); err != nil {
		t.Fatal(err)
	}

	h, err := jobstest.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if ran, err := h.RunNext(context.Background()); err != nil || ran {
		t.Fatalf("initial RunNext ran=%v err=%v", ran, err)
	}

	h.Advance(time.Minute)
	if _, err := h.RunAll(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAttemptsAndEventsExposeHistory(t *testing.T) {
	reg := jobs.NewRegistry()
	reg.MustHandle(echoJob, func(ctx jobs.Ctx, in echoIn) (echoOut, error) {
		return echoOut{Message: in.Message}, nil
	})

	h, err := jobstest.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	ref, err := h.Enqueue(context.Background(), echoJob, echoIn{Message: "history"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.RunAll(context.Background()); err != nil {
		t.Fatal(err)
	}

	attempts, err := h.Runtime.Attempts(context.Background(), ref.ID, jobs.InspectAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 {
		t.Fatalf("attempts=%d want 1", len(attempts))
	}
	if attempts[0].Status != jobs.AttemptSucceeded {
		t.Fatalf("attempt status=%s want succeeded", attempts[0].Status)
	}

	events, err := h.Runtime.Events(context.Background(), ref.ID, jobs.InspectAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 3 {
		t.Fatalf("events=%d want at least 3", len(events))
	}
	if events[0].Kind != jobs.EventSucceeded {
		t.Fatalf("newest event=%s want succeeded", events[0].Kind)
	}
	if events[len(events)-1].Kind != jobs.EventEnqueued {
		t.Fatalf("oldest event=%s want enqueued", events[len(events)-1].Kind)
	}
}

func TestQueueAdminControls(t *testing.T) {
	reg := jobs.NewRegistry()
	reg.MustHandle(echoJob, func(ctx jobs.Ctx, in echoIn) (echoOut, error) {
		return echoOut{Message: in.Message}, nil
	})

	h, err := jobstest.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	ref, err := h.Enqueue(context.Background(), echoJob, echoIn{Message: "queue"})
	if err != nil {
		t.Fatal(err)
	}

	if err := h.Runtime.PauseQueue(context.Background(), "emails", "maintenance"); !errors.Is(err, jobs.ErrUnauthorized) {
		t.Fatalf("PauseQueue err=%v want ErrUnauthorized", err)
	}
	if err := h.Runtime.PauseQueue(context.Background(), "emails", "maintenance", jobs.ControlAdmin()); err != nil {
		t.Fatal(err)
	}
	queue, err := h.Runtime.Queue(context.Background(), "emails", jobs.ControlAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if !queue.Paused || queue.Queued != 1 {
		t.Fatalf("queue=%+v want paused queued=1", queue)
	}
	if err := h.Runtime.ResumeQueue(context.Background(), "emails", jobs.ControlAdmin()); err != nil {
		t.Fatal(err)
	}
	snap, err := h.Runtime.Inspect(context.Background(), ref.ID, jobs.InspectAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != jobs.StatusQueued {
		t.Fatalf("status=%s want queued", snap.Status)
	}
}

func TestScheduleControls(t *testing.T) {
	cleanupJob := jobs.New[struct{}, struct{}]("test.schedule.control.v1")
	schedule := jobs.NewSchedule("cleanup.control.v1", cleanupJob, jobs.Cron("*/1 * * * *"))

	reg := jobs.NewRegistry()
	reg.MustHandle(cleanupJob, func(ctx jobs.Ctx, in struct{}) (struct{}, error) {
		return struct{}{}, nil
	})
	if err := reg.RegisterSchedule(schedule); err != nil {
		t.Fatal(err)
	}

	h, err := jobstest.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if err := h.Runtime.PauseSchedule(context.Background(), "cleanup.control.v1", jobs.ControlAdmin()); err != nil {
		t.Fatal(err)
	}
	sched, err := h.Runtime.Schedule(context.Background(), "cleanup.control.v1", jobs.ControlAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if !sched.Paused {
		t.Fatalf("schedule paused=%v want true", sched.Paused)
	}
	h.Advance(time.Minute)
	if ran, err := h.RunNext(context.Background()); err != nil || ran {
		t.Fatalf("paused schedule RunNext ran=%v err=%v", ran, err)
	}
	if err := h.Runtime.ResumeSchedule(context.Background(), "cleanup.control.v1", jobs.ControlAdmin()); err != nil {
		t.Fatal(err)
	}
	if ran, err := h.RunNext(context.Background()); err != nil || !ran {
		t.Fatalf("resumed schedule RunNext ran=%v err=%v", ran, err)
	}
}

func TestPruneDeletesExpiredCompletedJobs(t *testing.T) {
	reg := jobs.NewRegistry()
	reg.MustHandle(pruneJob, func(ctx jobs.Ctx, in struct{}) (struct{}, error) {
		return struct{}{}, nil
	})

	h, err := jobstest.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	ref, err := h.Enqueue(context.Background(), pruneJob, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.RunAll(context.Background()); err != nil {
		t.Fatal(err)
	}

	h.Advance(2 * time.Second)
	summary, err := h.Runtime.Prune(context.Background(), jobs.PruneAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if summary.Deleted != 1 {
		t.Fatalf("deleted=%d want 1", summary.Deleted)
	}
	if _, err := h.Runtime.Inspect(context.Background(), ref.ID, jobs.InspectAdmin()); !errors.Is(err, jobs.ErrNotFound) {
		t.Fatalf("Inspect err=%v want ErrNotFound", err)
	}
}

func TestBroadcastDeliversCrossRuntimeJobUpdates(t *testing.T) {
	reg := jobs.NewRegistry()
	reg.MustHandle(echoJob, func(ctx jobs.Ctx, in echoIn) (echoOut, error) {
		return echoOut{Message: in.Message}, nil
	})

	backend := newTestBroadcast()
	rt1, err := jobs.NewInMemory(reg, jobs.WithBroadcast(backend))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt1.Close() })
	rt2, err := jobs.NewInMemory(reg, jobs.WithBroadcast(backend))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt2.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch, err := rt2.SubscribeJob(ctx, "*")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := rt1.Enqueue(ctx, echoJob, echoIn{Message: "broadcast"}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatal("timed out waiting for broadcast update")
	}
}
