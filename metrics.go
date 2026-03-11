package jobs

import (
	"sync"
	"time"
)

const defaultTracerName = "github.com/vango-go/vango-jobs"

// JobQueueKey scopes counters and latency summaries by durable job name and queue.
type JobQueueKey struct {
	Job   string
	Queue string
}

// JobQueueCodeKey scopes counters by durable job name, queue, and stable error code.
type JobQueueCodeKey struct {
	Job   string
	Queue string
	Code  string
}

// MetricsSnapshot captures the current in-memory metrics state.
type MetricsSnapshot struct {
	Enqueued             map[JobQueueKey]int64
	Started              map[JobQueueKey]int64
	Succeeded            map[JobQueueKey]int64
	Failed               map[JobQueueCodeKey]int64
	Retried              map[JobQueueCodeKey]int64
	Canceled             map[JobQueueKey]int64
	Abandoned            map[JobQueueKey]int64
	Inflight             map[string]int
	QueueDepth           map[string]int
	ScheduleLagCount     map[string]int64
	ScheduleLagSum       map[string]time.Duration
	RunDurationCount     map[JobQueueKey]int64
	RunDurationSum       map[JobQueueKey]time.Duration
	EndToEndLatencyCount map[JobQueueKey]int64
	EndToEndLatencySum   map[JobQueueKey]time.Duration
	LeaseRenewalFailures map[string]int64
}

// MetricsSink consumes runtime metrics for job lifecycle activity.
type MetricsSink interface {
	RecordEnqueued(job, queue string)
	RecordStarted(job, queue string)
	RecordSucceeded(job, queue string)
	RecordFailed(job, queue, code string)
	RecordRetried(job, queue, code string)
	RecordCanceled(job, queue string)
	RecordAbandoned(job, queue string)
	RecordInflight(queue string, count int)
	RecordQueueDepth(queue string, depth int)
	RecordScheduleLag(schedule string, lag time.Duration)
	RecordRunDuration(job, queue string, d time.Duration)
	RecordEndToEndLatency(job, queue string, d time.Duration)
	RecordLeaseRenewalFailure(queue string)
}

// NoopMetricsSink drops all metrics.
type NoopMetricsSink struct{}

func (NoopMetricsSink) RecordEnqueued(job, queue string)                         {}
func (NoopMetricsSink) RecordStarted(job, queue string)                          {}
func (NoopMetricsSink) RecordSucceeded(job, queue string)                        {}
func (NoopMetricsSink) RecordFailed(job, queue, code string)                     {}
func (NoopMetricsSink) RecordRetried(job, queue, code string)                    {}
func (NoopMetricsSink) RecordCanceled(job, queue string)                         {}
func (NoopMetricsSink) RecordAbandoned(job, queue string)                        {}
func (NoopMetricsSink) RecordInflight(queue string, count int)                   {}
func (NoopMetricsSink) RecordQueueDepth(queue string, depth int)                 {}
func (NoopMetricsSink) RecordScheduleLag(schedule string, lag time.Duration)     {}
func (NoopMetricsSink) RecordRunDuration(job, queue string, d time.Duration)     {}
func (NoopMetricsSink) RecordEndToEndLatency(job, queue string, d time.Duration) {}
func (NoopMetricsSink) RecordLeaseRenewalFailure(queue string)                   {}

// MetricsCollector is the default in-memory metrics sink.
type MetricsCollector struct {
	mu       sync.Mutex
	snapshot MetricsSnapshot
}

// NewMetricsCollector creates an in-memory metrics sink.
func NewMetricsCollector() *MetricsCollector {
	c := &MetricsCollector{}
	c.Reset()
	return c
}

// Reset clears all aggregated metrics.
func (c *MetricsCollector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot = MetricsSnapshot{
		Enqueued:             make(map[JobQueueKey]int64),
		Started:              make(map[JobQueueKey]int64),
		Succeeded:            make(map[JobQueueKey]int64),
		Failed:               make(map[JobQueueCodeKey]int64),
		Retried:              make(map[JobQueueCodeKey]int64),
		Canceled:             make(map[JobQueueKey]int64),
		Abandoned:            make(map[JobQueueKey]int64),
		Inflight:             make(map[string]int),
		QueueDepth:           make(map[string]int),
		ScheduleLagCount:     make(map[string]int64),
		ScheduleLagSum:       make(map[string]time.Duration),
		RunDurationCount:     make(map[JobQueueKey]int64),
		RunDurationSum:       make(map[JobQueueKey]time.Duration),
		EndToEndLatencyCount: make(map[JobQueueKey]int64),
		EndToEndLatencySum:   make(map[JobQueueKey]time.Duration),
		LeaseRenewalFailures: make(map[string]int64),
	}
}

// Snapshot returns a detached copy of the current aggregated metrics.
func (c *MetricsCollector) Snapshot() MetricsSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return MetricsSnapshot{
		Enqueued:             copyJobQueueCountMap(c.snapshot.Enqueued),
		Started:              copyJobQueueCountMap(c.snapshot.Started),
		Succeeded:            copyJobQueueCountMap(c.snapshot.Succeeded),
		Failed:               copyJobQueueCodeCountMap(c.snapshot.Failed),
		Retried:              copyJobQueueCodeCountMap(c.snapshot.Retried),
		Canceled:             copyJobQueueCountMap(c.snapshot.Canceled),
		Abandoned:            copyJobQueueCountMap(c.snapshot.Abandoned),
		Inflight:             copyStringIntMap(c.snapshot.Inflight),
		QueueDepth:           copyStringIntMap(c.snapshot.QueueDepth),
		ScheduleLagCount:     copyStringCountMap(c.snapshot.ScheduleLagCount),
		ScheduleLagSum:       copyStringDurationMap(c.snapshot.ScheduleLagSum),
		RunDurationCount:     copyJobQueueCountMap(c.snapshot.RunDurationCount),
		RunDurationSum:       copyJobQueueDurationMap(c.snapshot.RunDurationSum),
		EndToEndLatencyCount: copyJobQueueCountMap(c.snapshot.EndToEndLatencyCount),
		EndToEndLatencySum:   copyJobQueueDurationMap(c.snapshot.EndToEndLatencySum),
		LeaseRenewalFailures: copyStringCountMap(c.snapshot.LeaseRenewalFailures),
	}
}

func (c *MetricsCollector) RecordEnqueued(job, queue string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot.Enqueued[JobQueueKey{Job: job, Queue: queue}]++
}

func (c *MetricsCollector) RecordStarted(job, queue string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot.Started[JobQueueKey{Job: job, Queue: queue}]++
}

func (c *MetricsCollector) RecordSucceeded(job, queue string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot.Succeeded[JobQueueKey{Job: job, Queue: queue}]++
}

func (c *MetricsCollector) RecordFailed(job, queue, code string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot.Failed[JobQueueCodeKey{Job: job, Queue: queue, Code: normalizeMetricCode(code)}]++
}

func (c *MetricsCollector) RecordRetried(job, queue, code string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot.Retried[JobQueueCodeKey{Job: job, Queue: queue, Code: normalizeMetricCode(code)}]++
}

func (c *MetricsCollector) RecordCanceled(job, queue string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot.Canceled[JobQueueKey{Job: job, Queue: queue}]++
}

func (c *MetricsCollector) RecordAbandoned(job, queue string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot.Abandoned[JobQueueKey{Job: job, Queue: queue}]++
}

func (c *MetricsCollector) RecordInflight(queue string, count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot.Inflight[queue] = count
}

func (c *MetricsCollector) RecordQueueDepth(queue string, depth int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot.QueueDepth[queue] = depth
}

func (c *MetricsCollector) RecordScheduleLag(schedule string, lag time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot.ScheduleLagCount[schedule]++
	c.snapshot.ScheduleLagSum[schedule] += lag
}

func (c *MetricsCollector) RecordRunDuration(job, queue string, d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := JobQueueKey{Job: job, Queue: queue}
	c.snapshot.RunDurationCount[key]++
	c.snapshot.RunDurationSum[key] += d
}

func (c *MetricsCollector) RecordEndToEndLatency(job, queue string, d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := JobQueueKey{Job: job, Queue: queue}
	c.snapshot.EndToEndLatencyCount[key]++
	c.snapshot.EndToEndLatencySum[key] += d
}

func (c *MetricsCollector) RecordLeaseRenewalFailure(queue string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot.LeaseRenewalFailures[queue]++
}

func normalizeMetricCode(code string) string {
	if code == "" {
		return "unknown"
	}
	return code
}

func copyJobQueueCountMap(in map[JobQueueKey]int64) map[JobQueueKey]int64 {
	out := make(map[JobQueueKey]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyJobQueueCodeCountMap(in map[JobQueueCodeKey]int64) map[JobQueueCodeKey]int64 {
	out := make(map[JobQueueCodeKey]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyJobQueueDurationMap(in map[JobQueueKey]time.Duration) map[JobQueueKey]time.Duration {
	out := make(map[JobQueueKey]time.Duration, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyStringCountMap(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyStringIntMap(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyStringDurationMap(in map[string]time.Duration) map[string]time.Duration {
	out := make(map[string]time.Duration, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
