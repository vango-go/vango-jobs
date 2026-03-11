package jobscmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

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
		Queues         []jobs.QueueSnapshot `json:"queues"`
		RecentFailures []*jobs.RawSnapshot  `json:"recent_failures"`
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
}
