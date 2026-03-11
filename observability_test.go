package jobs_test

import (
	"context"
	"testing"
	"time"

	jobs "github.com/vango-go/vango-jobs"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRuntimeMetricsCaptureLifecycle(t *testing.T) {
	reg := jobs.NewRegistry()
	reg.MustHandle(echoJob, func(ctx jobs.Ctx, in echoIn) (echoOut, error) {
		time.Sleep(10 * time.Millisecond)
		return echoOut{Message: in.Message}, nil
	})

	rt, err := jobs.NewInMemory(reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	collector := rt.MetricsCollector()
	if collector == nil {
		t.Fatal("expected default metrics collector")
	}

	ref, err := rt.Enqueue(context.Background(), echoJob, echoIn{Message: "metrics"})
	if err != nil {
		t.Fatal(err)
	}

	worker := rt.NewWorker()
	ran, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ran != 1 {
		t.Fatalf("ran=%d want 1", ran)
	}

	snap, err := jobs.LookupTyped(context.Background(), rt, echoJob, ref.ID, jobs.InspectAdmin())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != jobs.StatusSucceeded {
		t.Fatalf("status=%s want succeeded", snap.Status)
	}

	metrics := collector.Snapshot()
	key := jobs.JobQueueKey{Job: echoJob.Name(), Queue: "emails"}
	if metrics.Enqueued[key] != 1 {
		t.Fatalf("enqueued=%d want 1", metrics.Enqueued[key])
	}
	if metrics.Started[key] != 1 {
		t.Fatalf("started=%d want 1", metrics.Started[key])
	}
	if metrics.Succeeded[key] != 1 {
		t.Fatalf("succeeded=%d want 1", metrics.Succeeded[key])
	}
	if metrics.QueueDepth["emails"] != 0 {
		t.Fatalf("queue depth=%d want 0", metrics.QueueDepth["emails"])
	}
	if metrics.Inflight["emails"] != 0 {
		t.Fatalf("inflight=%d want 0", metrics.Inflight["emails"])
	}
	if metrics.RunDurationCount[key] != 1 {
		t.Fatalf("run duration count=%d want 1", metrics.RunDurationCount[key])
	}
	if metrics.EndToEndLatencyCount[key] != 1 {
		t.Fatalf("end-to-end latency count=%d want 1", metrics.EndToEndLatencyCount[key])
	}
}

func TestEnqueueDerivesTraceIDFromContext(t *testing.T) {
	reg := jobs.NewRegistry()
	reg.MustHandle(echoJob, func(ctx jobs.Ctx, in echoIn) (echoOut, error) {
		return echoOut{Message: in.Message}, nil
	})

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	rt, err := jobs.NewInMemory(reg, jobs.WithTracer(tp.Tracer("jobs-test")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	ctx, origin := tp.Tracer("origin").Start(context.Background(), "origin-request")
	ref, err := rt.Enqueue(ctx, echoJob, echoIn{Message: "trace"})
	origin.End()
	if err != nil {
		t.Fatal(err)
	}

	raw, err := rt.Inspect(context.Background(), ref.ID, jobs.InspectAdmin())
	if err != nil {
		t.Fatal(err)
	}
	wantTraceID := origin.SpanContext().TraceID().String()
	if raw.TraceID != wantTraceID {
		t.Fatalf("trace_id=%q want %q", raw.TraceID, wantTraceID)
	}

	ended := recorder.Ended()
	found := false
	for _, span := range ended {
		if span.Name() == "jobs.enqueue" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected jobs.enqueue span")
	}
}
