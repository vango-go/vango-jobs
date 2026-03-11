package jobstest_test

import (
	"context"
	"testing"

	jobs "github.com/vango-go/vango-jobs"
	"github.com/vango-go/vango-jobs/jobstest"
)

func TestImmediateExecution(t *testing.T) {
	def := jobs.New[string, string]("test.immediate.v1")
	reg := jobs.NewRegistry()
	reg.MustHandle(def, func(ctx jobs.Ctx, in string) (string, error) {
		return in + "!", nil
	})

	h, err := jobstest.New(reg, jobstest.WithImmediateExecution(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	ref, err := h.Enqueue(context.Background(), def, "go")
	if err != nil {
		t.Fatal(err)
	}
	snap, err := jobs.LookupTyped(context.Background(), h.Runtime, def, ref.ID, jobs.InspectAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != jobs.StatusSucceeded {
		t.Fatalf("status=%s want succeeded", snap.Status)
	}
	if snap.Output == nil || *snap.Output != "go!" {
		t.Fatalf("output=%v want go!", snap.Output)
	}
}
