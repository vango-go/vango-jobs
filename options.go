package jobs

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// DefinitionOption customizes a definition's durable metadata.
type DefinitionOption interface {
	applyDefinition(any)
}

type definitionOptionFunc func(any)

func (f definitionOptionFunc) applyDefinition(target any) {
	f(target)
}

// Queue sets the default queue for a definition.
func Queue(name string) DefinitionOption {
	return definitionOptionFunc(func(target any) {
		applyDefinitionField(target, func(base *baseDefinition) {
			base.queue = normalizeQueue(name)
		})
	})
}

// MaxAttempts sets the per-job maximum attempt count.
func MaxAttempts(n int) DefinitionOption {
	return definitionOptionFunc(func(target any) {
		applyDefinitionField(target, func(base *baseDefinition) {
			base.maxAttempts = n
		})
	})
}

// Timeout sets the per-attempt timeout.
func Timeout(d time.Duration) DefinitionOption {
	return definitionOptionFunc(func(target any) {
		applyDefinitionField(target, func(base *baseDefinition) {
			base.timeout = d
		})
	})
}

// Backoff sets the retry backoff strategy.
func Backoff(strategy BackoffStrategy) DefinitionOption {
	return definitionOptionFunc(func(target any) {
		applyDefinitionField(target, func(base *baseDefinition) {
			base.backoff = strategy
		})
	})
}

// Retention sets a terminal retention override for the definition.
func Retention(d time.Duration) DefinitionOption {
	return definitionOptionFunc(func(target any) {
		applyDefinitionField(target, func(base *baseDefinition) {
			base.retention = d
		})
	})
}

// Priority sets the default enqueue priority for the definition.
func Priority(n int) DefinitionOption {
	return definitionOptionFunc(func(target any) {
		applyDefinitionField(target, func(base *baseDefinition) {
			base.priority = n
		})
	})
}

func applyDefinitionField(target any, mutate func(*baseDefinition)) {
	switch d := target.(type) {
	case interface {
		base() baseDefinition
		setBase(baseDefinition)
	}:
		base := d.base()
		mutate(&base)
		d.setBase(base)
	}
}

// EnqueueOption customizes one enqueue operation.
type EnqueueOption func(*enqueueConfig)

// UniquePolicy controls duplicate active-job handling.
type UniquePolicy string

const (
	UniqueReturnExisting  UniquePolicy = "return_existing"
	UniqueRejectDuplicate UniquePolicy = "reject_duplicate"
)

type enqueueConfig struct {
	tenantID       string
	actor          Actor
	metadata       map[string]string
	uniqueKey      string
	uniquePolicy   UniquePolicy
	concurrencyKey string
	delay          *time.Duration
	runAt          time.Time
	priority       *int
	parent         *Ref
	tags           []string
	traceID        string
}

// WithTenant applies durable tenant metadata.
func WithTenant(id string) EnqueueOption {
	return func(cfg *enqueueConfig) {
		cfg.tenantID = id
	}
}

// WithActor applies durable actor metadata.
func WithActor(actor Actor) EnqueueOption {
	return func(cfg *enqueueConfig) {
		cfg.actor = actor
	}
}

// WithMetadata applies safe low-cardinality metadata.
func WithMetadata(metadata map[string]string) EnqueueOption {
	return func(cfg *enqueueConfig) {
		cfg.metadata = copyMetadata(metadata)
	}
}

// WithUnique configures active-job deduplication.
func WithUnique(key string, policy UniquePolicy) EnqueueOption {
	return func(cfg *enqueueConfig) {
		cfg.uniqueKey = key
		if policy == "" {
			policy = UniqueReturnExisting
		}
		cfg.uniquePolicy = policy
	}
}

// WithConcurrencyKey serializes execution for jobs sharing the same key.
func WithConcurrencyKey(key string) EnqueueOption {
	return func(cfg *enqueueConfig) {
		cfg.concurrencyKey = key
	}
}

// WithDelay schedules the job in the future.
func WithDelay(d time.Duration) EnqueueOption {
	return func(cfg *enqueueConfig) {
		value := d
		cfg.delay = &value
	}
}

// WithRunAt sets the durable run-at timestamp.
func WithRunAt(t time.Time) EnqueueOption {
	return func(cfg *enqueueConfig) {
		cfg.runAt = t.UTC()
	}
}

// WithPriority overrides the default enqueue priority.
func WithPriority(n int) EnqueueOption {
	return func(cfg *enqueueConfig) {
		cfg.priority = &n
	}
}

// WithParent records parent-job lineage.
func WithParent(parent Ref) EnqueueOption {
	return func(cfg *enqueueConfig) {
		cfg.parent = &parent
	}
}

// WithTags records durable low-cardinality tags.
func WithTags(tags ...string) EnqueueOption {
	return func(cfg *enqueueConfig) {
		cfg.tags = copyTags(tags)
	}
}

// WithTraceID records a correlation trace identifier.
func WithTraceID(traceID string) EnqueueOption {
	return func(cfg *enqueueConfig) {
		cfg.traceID = traceID
	}
}

type authScope struct {
	admin    bool
	tenantID string
	actor    Actor
}

func (s authScope) empty() bool {
	return !s.admin && s.tenantID == "" && s.actor.Empty()
}

type inspectConfig struct {
	scope authScope
}

// InspectOption scopes inspection authorization.
type InspectOption func(*inspectConfig)

// InspectAdmin authorizes admin-level reads.
func InspectAdmin() InspectOption {
	return func(cfg *inspectConfig) {
		cfg.scope.admin = true
	}
}

// InspectTenant authorizes reads for one tenant.
func InspectTenant(tenantID string) InspectOption {
	return func(cfg *inspectConfig) {
		cfg.scope.tenantID = tenantID
	}
}

// InspectActor authorizes reads for one actor.
func InspectActor(actor Actor) InspectOption {
	return func(cfg *inspectConfig) {
		cfg.scope.actor = actor
	}
}

type controlConfig struct {
	scope authScope
}

// ControlOption scopes admin/control authorization.
type ControlOption func(*controlConfig)

// ControlAdmin authorizes admin-level control operations.
func ControlAdmin() ControlOption {
	return func(cfg *controlConfig) {
		cfg.scope.admin = true
	}
}

// ControlTenant authorizes controls for one tenant.
func ControlTenant(tenantID string) ControlOption {
	return func(cfg *controlConfig) {
		cfg.scope.tenantID = tenantID
	}
}

// ControlActor authorizes controls for one actor.
func ControlActor(actor Actor) ControlOption {
	return func(cfg *controlConfig) {
		cfg.scope.actor = actor
	}
}

type pruneConfig struct {
	scope authScope
	limit int
}

// PruneOption customizes prune authorization and batch size.
type PruneOption func(*pruneConfig)

// PruneAdmin authorizes prune operations as an admin control.
func PruneAdmin() PruneOption {
	return func(cfg *pruneConfig) {
		cfg.scope.admin = true
	}
}

// PruneLimit bounds one prune pass.
func PruneLimit(n int) PruneOption {
	return func(cfg *pruneConfig) {
		if n > 0 {
			cfg.limit = n
		}
	}
}

type retryConfig struct {
	scope          authScope
	preserveUnique bool
	overrideRunAt  *time.Time
	overrideDelay  *time.Duration
	overrideQueue  string
}

// RetryOption customizes admin retry behavior.
type RetryOption func(*retryConfig)

// RetryAdmin authorizes admin-level retries.
func RetryAdmin() RetryOption {
	return func(cfg *retryConfig) {
		cfg.scope.admin = true
	}
}

// RetryTenant authorizes retries for one tenant.
func RetryTenant(tenantID string) RetryOption {
	return func(cfg *retryConfig) {
		cfg.scope.tenantID = tenantID
	}
}

// RetryActor authorizes retries for one actor.
func RetryActor(actor Actor) RetryOption {
	return func(cfg *retryConfig) {
		cfg.scope.actor = actor
	}
}

// RetryPreserveUnique preserves the original unique key on cloned retries.
func RetryPreserveUnique(enabled bool) RetryOption {
	return func(cfg *retryConfig) {
		cfg.preserveUnique = enabled
	}
}

// RetryRunAt overrides the retry clone run-at timestamp.
func RetryRunAt(t time.Time) RetryOption {
	return func(cfg *retryConfig) {
		value := t.UTC()
		cfg.overrideRunAt = &value
	}
}

// RetryDelay overrides the retry clone delay from now.
func RetryDelay(d time.Duration) RetryOption {
	return func(cfg *retryConfig) {
		cfg.overrideDelay = &d
	}
}

// RetryQueue overrides the retry clone queue.
func RetryQueue(queue string) RetryOption {
	return func(cfg *retryConfig) {
		cfg.overrideQueue = normalizeQueue(queue)
	}
}

// ReplayFilter selects historical jobs for bulk replay.
type ReplayFilter struct {
	Queue    string
	JobName  string
	TenantID string
	Statuses []Status
	Since    *time.Time
	Until    *time.Time
	Limit    int
}

// ListOrder controls durable job listing order for admin/status surfaces.
type ListOrder string

const (
	// ListOrderCreatedDesc keeps the historical default ordering by enqueue time.
	ListOrderCreatedDesc ListOrder = "created_desc"
	// ListOrderTerminalTimeDesc orders by the terminal completion time when present,
	// falling back to updated/created time for non-terminal rows.
	ListOrderTerminalTimeDesc ListOrder = "terminal_time_desc"
)

// ListFilter selects durable jobs for admin/status listing.
type ListFilter struct {
	Queue    string
	JobName  string
	TenantID string
	Statuses []Status
	Since    *time.Time
	Until    *time.Time
	Limit    int
	Order    ListOrder
}

func normalizeListLimit(limit int) int {
	switch {
	case limit <= 0:
		return 100
	case limit > 1000:
		return 1000
	default:
		return limit
	}
}

func normalizeListOrder(order ListOrder) ListOrder {
	switch order {
	case ListOrderTerminalTimeDesc:
		return order
	default:
		return ListOrderCreatedDesc
	}
}

// ReplaySummary describes a bulk replay operation.
type ReplaySummary struct {
	Selected int
	Cloned   int
	Failed   int
	Refs     []Ref
}

// RuntimeOption customizes runtime construction.
type RuntimeOption func(*Config)

// Config contains runtime-wide configuration.
type Config struct {
	Logger                *slog.Logger
	Metrics               MetricsSink
	Tracer                trace.Tracer
	TracerName            string
	Codec                 Codec
	Broadcast             BroadcastBackend
	PayloadLimit          int
	OutputLimit           int
	ProgressLimit         int
	MetadataLimit         int
	LeaseDuration         time.Duration
	HeartbeatInterval     time.Duration
	ProgressFlushInterval time.Duration
	ClaimBatchSize        int
	ClaimPollInterval     time.Duration
	ClaimOverscanFactor   int
	WorkerID              string
	SuccessRetention      time.Duration
	FailedRetention       time.Duration
	CanceledRetention     time.Duration
	MaxReplayBatch        int
	NotificationBuffer    int
	SchedulePollInterval  time.Duration
	Now                   func() time.Time
}

func defaultConfig() Config {
	return Config{
		Logger:                slog.Default(),
		TracerName:            defaultTracerName,
		Codec:                 jsonCodec{},
		PayloadLimit:          64 << 10,
		OutputLimit:           64 << 10,
		ProgressLimit:         4 << 10,
		MetadataLimit:         4 << 10,
		LeaseDuration:         30 * time.Second,
		HeartbeatInterval:     10 * time.Second,
		ProgressFlushInterval: 1 * time.Second,
		ClaimBatchSize:        16,
		ClaimPollInterval:     1 * time.Second,
		ClaimOverscanFactor:   4,
		SuccessRetention:      7 * 24 * time.Hour,
		FailedRetention:       30 * 24 * time.Hour,
		CanceledRetention:     7 * 24 * time.Hour,
		MaxReplayBatch:        100,
		NotificationBuffer:    32,
		SchedulePollInterval:  15 * time.Second,
		Now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

// WithLogger configures the runtime logger.
func WithLogger(logger *slog.Logger) RuntimeOption {
	return func(cfg *Config) {
		if logger != nil {
			cfg.Logger = logger
		}
	}
}

// WithMetrics configures the runtime metrics sink.
func WithMetrics(metrics MetricsSink) RuntimeOption {
	return func(cfg *Config) {
		if metrics != nil {
			cfg.Metrics = metrics
		}
	}
}

// WithTracer configures the runtime tracer.
func WithTracer(tracer trace.Tracer) RuntimeOption {
	return func(cfg *Config) {
		if tracer != nil {
			cfg.Tracer = tracer
		}
	}
}

// WithTracerName configures the default tracer name when no explicit tracer is supplied.
func WithTracerName(name string) RuntimeOption {
	return func(cfg *Config) {
		if name != "" {
			cfg.TracerName = name
		}
	}
}

// WithCodec configures the runtime payload codec.
func WithCodec(codec Codec) RuntimeOption {
	return func(cfg *Config) {
		if codec != nil {
			cfg.Codec = codec
		}
	}
}

// WithBroadcast configures the optional cross-instance update broadcaster.
func WithBroadcast(b BroadcastBackend) RuntimeOption {
	return func(cfg *Config) {
		cfg.Broadcast = b
	}
}

// WithWorkerID configures the default worker identifier.
func WithWorkerID(id string) RuntimeOption {
	return func(cfg *Config) {
		cfg.WorkerID = id
	}
}

// WithBudgets overrides the durable size budgets.
func WithBudgets(payload, output, progress, metadata int) RuntimeOption {
	return func(cfg *Config) {
		if payload > 0 {
			cfg.PayloadLimit = payload
		}
		if output > 0 {
			cfg.OutputLimit = output
		}
		if progress > 0 {
			cfg.ProgressLimit = progress
		}
		if metadata > 0 {
			cfg.MetadataLimit = metadata
		}
	}
}

// WithRuntimeTimings configures default runtime claim/lease timings.
func WithRuntimeTimings(lease, heartbeat, poll time.Duration) RuntimeOption {
	return func(cfg *Config) {
		if lease > 0 {
			cfg.LeaseDuration = lease
		}
		if heartbeat > 0 {
			cfg.HeartbeatInterval = heartbeat
		}
		if poll > 0 {
			cfg.ClaimPollInterval = poll
		}
	}
}

// WithRetention configures default terminal retention windows.
func WithRetention(succeeded, failed, canceled time.Duration) RuntimeOption {
	return func(cfg *Config) {
		if succeeded > 0 {
			cfg.SuccessRetention = succeeded
		}
		if failed > 0 {
			cfg.FailedRetention = failed
		}
		if canceled > 0 {
			cfg.CanceledRetention = canceled
		}
	}
}

// WithClock overrides the runtime wall clock. This is primarily useful for deterministic tests.
func WithClock(now func() time.Time) RuntimeOption {
	return func(cfg *Config) {
		if now != nil {
			cfg.Now = now
		}
	}
}

// BroadcastBackend is an optional safe multi-instance update channel.
type BroadcastBackend interface {
	Publish(ctx context.Context, channel string, data []byte) error
	Subscribe(ctx context.Context, channel string) (<-chan []byte, error)
}
