package jobs

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func (r *Runtime) startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil || r.tracer == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	return r.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

func (r *Runtime) recordSpanError(span trace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

func (r *Runtime) refreshQueueMetrics(ctx context.Context, queues ...string) {
	if r == nil || r.metrics == nil {
		return
	}
	seen := make(map[string]struct{}, len(queues))
	for _, queue := range queues {
		queue = normalizeQueue(queue)
		if queue == "" {
			continue
		}
		if _, ok := seen[queue]; ok {
			continue
		}
		seen[queue] = struct{}{}
		snapshot, err := r.store.queue(ctx, queue)
		if err != nil || snapshot == nil {
			continue
		}
		r.metrics.RecordQueueDepth(snapshot.Queue, snapshot.Queued)
		r.metrics.RecordInflight(snapshot.Queue, snapshot.Running)
	}
}

func traceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	spanCtx := trace.SpanContextFromContext(ctx)
	if !spanCtx.IsValid() || !spanCtx.TraceID().IsValid() {
		return ""
	}
	return spanCtx.TraceID().String()
}

func jobSpanAttributes(job claimedJob) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("jobs.id", job.JobID),
		attribute.String("jobs.name", job.JobName),
		attribute.String("jobs.queue", job.Queue),
		attribute.Int("jobs.attempt", job.Attempts),
		attribute.Int("jobs.max_attempts", job.MaxAttempts),
	}
	if job.TenantID != "" {
		attrs = append(attrs, attribute.String("jobs.tenant_id", job.TenantID))
	}
	if !job.Actor.Empty() {
		attrs = append(attrs,
			attribute.String("jobs.actor_id", job.Actor.ID),
			attribute.String("jobs.actor_kind", job.Actor.Kind),
		)
	}
	if job.TraceID != "" {
		attrs = append(attrs, attribute.String("jobs.origin_trace_id", job.TraceID))
	}
	return attrs
}

func maybeDuration(start, end time.Time) time.Duration {
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return 0
	}
	return end.Sub(start)
}

func (r *Runtime) log(level slog.Level, msg string, attrs ...any) {
	if r == nil || r.logger == nil {
		return
	}
	r.logger.Log(context.Background(), level, msg, attrs...)
}
