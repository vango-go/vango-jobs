package jobscmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/vango-go/vango-jobs"
	"github.com/vango-go/vango-jobs/jobstest"
)

type statusIn struct {
	Message string `json:"message"`
}

type statusOut struct {
	Message string `json:"message"`
}

var (
	statusOKJob   = jobs.New[statusIn, statusOut]("test.status.ok.v1", jobs.Queue("emails"))
	statusFailJob = jobs.New[statusIn, statusOut]("test.status.fail.v1", jobs.Queue("emails"))
)

func TestRunStatusOutputsQueuesAndFailures(t *testing.T) {
	reg := jobs.NewRegistry()
	reg.MustHandle(statusOKJob, func(ctx jobs.Ctx, in statusIn) (statusOut, error) {
		return statusOut{Message: in.Message}, nil
	})
	reg.MustHandle(statusFailJob, func(ctx jobs.Ctx, in statusIn) (statusOut, error) {
		return statusOut{}, jobs.Permanent(jobs.Safe(errors.New("boom"), "send failed"))
	})

	h, err := jobstest.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if _, err := h.Enqueue(context.Background(), statusOKJob, statusIn{Message: "ok"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Enqueue(context.Background(), statusFailJob, statusIn{Message: "fail"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.RunAll(context.Background()); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	if err := Run(context.Background(), Config{
		Runtime: h.Runtime,
		Stdout:  &stdout,
		Stderr:  &bytes.Buffer{},
	}, []string{"status", "--failures=5"}); err != nil {
		t.Fatal(err)
	}

	var payload struct {
		Queues         []jobs.QueueSnapshot   `json:"queues"`
		RecentFailures []statusFailureSummary `json:"recent_failures"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal status output: %v\n%s", err, stdout.String())
	}
	if len(payload.Queues) != 1 {
		t.Fatalf("len(queues)=%d want 1", len(payload.Queues))
	}
	if payload.Queues[0].Queue != "emails" {
		t.Fatalf("queue=%q want emails", payload.Queues[0].Queue)
	}
	if len(payload.RecentFailures) != 1 {
		t.Fatalf("len(recent_failures)=%d want 1", len(payload.RecentFailures))
	}
	if payload.RecentFailures[0].Ref.Name != statusFailJob.Name() {
		t.Fatalf("failure name=%q want %q", payload.RecentFailures[0].Ref.Name, statusFailJob.Name())
	}
	if payload.RecentFailures[0].SafeError != "send failed" {
		t.Fatalf("safe_error=%q want %q", payload.RecentFailures[0].SafeError, "send failed")
	}
	if payload.RecentFailures[0].CreatedAt.IsZero() {
		t.Fatalf("created_at not populated")
	}
}

func TestRunStatusOmitsRawPayloadMetadataAndOutput(t *testing.T) {
	reg := jobs.NewRegistry()
	reg.MustHandle(statusFailJob, func(ctx jobs.Ctx, in statusIn) (statusOut, error) {
		return statusOut{}, jobs.Permanent(jobs.Safe(errors.New("boom"), "send failed"))
	})

	h, err := jobstest.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if _, err := h.Enqueue(
		context.Background(),
		statusFailJob,
		statusIn{Message: "payload-secret"},
		jobs.WithMetadata(map[string]string{"token": "metadata-secret"}),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := h.RunAll(context.Background()); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	if err := Run(context.Background(), Config{
		Runtime: h.Runtime,
		Stdout:  &stdout,
		Stderr:  &bytes.Buffer{},
	}, []string{"status", "--failures=5"}); err != nil {
		t.Fatal(err)
	}

	if strings.Contains(stdout.String(), "payload-secret") || strings.Contains(stdout.String(), "metadata-secret") {
		t.Fatalf("status output leaked raw data: %s", stdout.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal status output: %v\n%s", err, stdout.String())
	}
	recentFailures, ok := payload["recent_failures"].([]any)
	if !ok || len(recentFailures) != 1 {
		t.Fatalf("recent_failures=%#v", payload["recent_failures"])
	}
	item, ok := recentFailures[0].(map[string]any)
	if !ok {
		t.Fatalf("recent failure item=%#v", recentFailures[0])
	}
	for _, forbidden := range []string{"payload", "Payload", "metadata", "Metadata", "output", "Output"} {
		if _, exists := item[forbidden]; exists {
			t.Fatalf("status output exposed forbidden field %q: %#v", forbidden, item)
		}
	}
}

func TestRunStatusOrdersRecentFailuresByTerminalTime(t *testing.T) {
	reg := jobs.NewRegistry()
	reg.MustHandle(statusFailJob, func(ctx jobs.Ctx, in statusIn) (statusOut, error) {
		return statusOut{}, jobs.Permanent(jobs.Safe(errors.New(in.Message), in.Message))
	})

	h, err := jobstest.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	olderRef, err := h.Enqueue(context.Background(), statusFailJob, statusIn{Message: "older-failed-later"}, jobs.WithDelay(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	h.Advance(time.Second)
	newerRef, err := h.Enqueue(context.Background(), statusFailJob, statusIn{Message: "newer-failed-earlier"})
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

	var stdout bytes.Buffer
	if err := Run(context.Background(), Config{
		Runtime: h.Runtime,
		Stdout:  &stdout,
		Stderr:  &bytes.Buffer{},
	}, []string{"status", "--failures=5"}); err != nil {
		t.Fatal(err)
	}

	var payload struct {
		RecentFailures []statusFailureSummary `json:"recent_failures"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal status output: %v\n%s", err, stdout.String())
	}
	if len(payload.RecentFailures) != 2 {
		t.Fatalf("len(recent_failures)=%d want 2", len(payload.RecentFailures))
	}
	if payload.RecentFailures[0].Ref.ID != olderRef.ID || payload.RecentFailures[1].Ref.ID != newerRef.ID {
		t.Fatalf("recent_failures order got %s then %s, want %s then %s", payload.RecentFailures[0].Ref.ID, payload.RecentFailures[1].Ref.ID, olderRef.ID, newerRef.ID)
	}
	if payload.RecentFailures[0].SafeError != "older-failed-later" {
		t.Fatalf("recent_failures[0].safe_error=%q want older-failed-later", payload.RecentFailures[0].SafeError)
	}
}

func TestRunStatusWatchReturnsSnapshotErrors(t *testing.T) {
	origWriteStatusText := writeStatusTextFn
	defer func() { writeStatusTextFn = origWriteStatusText }()

	writeStatusTextFn = func(ctx context.Context, cfg Config, failures int) error {
		return errors.New("snapshot unavailable")
	}

	h, err := jobstest.New(jobs.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	err = Run(context.Background(), Config{
		Runtime: h.Runtime,
		Stdout:  &bytes.Buffer{},
		Stderr:  &bytes.Buffer{},
	}, []string{"status", "--watch", "--interval=1ms"})
	if err == nil {
		t.Fatal("expected status --watch to fail")
	}
	if !strings.Contains(err.Error(), "snapshot unavailable") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunWorkerProcess_StatusSnapshotsAreBestEffort(t *testing.T) {
	origWriteStatusText := writeStatusTextFn
	defer func() { writeStatusTextFn = origWriteStatusText }()

	h, err := jobstest.New(
		jobs.NewRegistry(),
		jobstest.WithWorkerOptions(jobs.WithWorkerTimings(1*time.Millisecond, 1*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writeStatusTextFn = func(ctx context.Context, cfg Config, failures int) error {
		switch calls.Add(1) {
		case 1:
			return errors.New("snapshot unavailable")
		case 2:
			cancel()
			return nil
		default:
			return nil
		}
	}

	var stderr bytes.Buffer
	err = Run(ctx, Config{
		Runtime: h.Runtime,
		Worker:  h.Worker,
		Stdout:  &bytes.Buffer{},
		Stderr:  &stderr,
	}, []string{"run", "--no-scheduler", "--status-interval=1ms"})
	if err != nil {
		t.Fatalf("run worker process: %v", err)
	}
	if got := calls.Load(); got < 2 {
		t.Fatalf("status writer calls=%d want at least 2", got)
	}
	if !strings.Contains(stderr.String(), "jobs status snapshot failed; continuing: snapshot unavailable") {
		t.Fatalf("expected best-effort warning, stderr=%q", stderr.String())
	}
}
