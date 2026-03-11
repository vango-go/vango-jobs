package jobs

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

type enqueueRequest struct {
	ID                string
	Name              string
	Queue             string
	TenantID          string
	Actor             Actor
	ParentJobID       string
	UniqueKey         string
	UniquePolicy      UniquePolicy
	ConcurrencyKey    string
	Priority          int
	MaxAttempts       int
	Timeout           time.Duration
	RunAt             time.Time
	Payload           []byte
	Metadata          map[string]string
	Tags              []string
	TraceID           string
	RetentionOverride time.Duration
}

type retryCloneRequest struct {
	Now            time.Time
	NewJobID       func(time.Time) string
	PreserveUnique bool
	OverrideRunAt  *time.Time
	OverrideDelay  *time.Duration
	OverrideQueue  string
	Scope          authScope
	MaxBatch       int
}

type claimRequest struct {
	WorkerID      string
	Queues        []string
	Limit         int
	OverscanLimit int
	LeaseDuration time.Duration
	JobNames      []string
}

type claimedJob struct {
	JobID            string
	JobName          string
	Queue            string
	LeaseToken       string
	TraceID          string
	TenantID         string
	Actor            Actor
	Metadata         map[string]string
	Attempts         int
	MaxAttempts      int
	Timeout          time.Duration
	CreatedAt        time.Time
	AttemptStartedAt time.Time
	Payload          []byte
	ParentJobID      string
	ConcurrencyKey   string
	UniqueKey        string
}

type heartbeatRequest struct {
	JobID         string
	LeaseToken    string
	LeaseDuration time.Duration
}

type heartbeatResult struct {
	LeaseHeld       bool
	CancelRequested bool
}

type progressWriteRequest struct {
	JobID      string
	LeaseToken string
	Progress   Progress
}

type completionKind string

const (
	completionSuccess  completionKind = "success"
	completionRetry    completionKind = "retry"
	completionFailure  completionKind = "failure"
	completionCanceled completionKind = "canceled"
)

type completionRequest struct {
	JobID      string
	LeaseToken string
	Attempt    int
	Kind       completionKind
	Output     []byte
	Progress   *Progress
	SafeError  string
	ErrorCode  string
	RetryDelay time.Duration
	Retention  time.Duration
}

type reapRequest struct {
	Limit     int
	Backoff   func(name, jobID string, attempt int) time.Duration
	Retention func(name string, status Status) time.Duration
}

type reapedJob struct {
	JobID     string
	JobName   string
	Queue     string
	Outcome   EventKind
	ErrorCode string
}

type pruneRequest struct {
	Limit int
}

type scheduleTickRequest struct {
	Now         time.Time
	Limit       int
	NewJobID    func(time.Time) string
	Schedules   map[string]registeredSchedule
	Codec       Codec
	CheckBudget func(BudgetKind, []byte) error
}

type scheduledRef struct {
	Ref          Ref
	Queue        string
	ScheduleName string
	DueAt        time.Time
	Inserted     bool
}

func (r *Runtime) newEnqueueRequest(ctx context.Context, def any, input any, opts ...EnqueueOption) (enqueueRequest, error) {
	contract, err := definitionFromAny(def)
	if err != nil {
		return enqueueRequest{}, err
	}
	cfg := enqueueConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	now := r.now()
	base := contract.base()
	payload, err := contract.encodeInputAny(r.cfg.Codec, input)
	if err != nil {
		return enqueueRequest{}, err
	}
	if err := r.checkBudget(BudgetPayload, payload); err != nil {
		return enqueueRequest{}, err
	}
	metadata := copyMetadata(cfg.metadata)
	if len(metadata) > 0 {
		rawMeta, err := json.Marshal(metadata)
		if err != nil {
			return enqueueRequest{}, err
		}
		if err := r.checkBudget(BudgetMetadata, rawMeta); err != nil {
			return enqueueRequest{}, err
		}
	}
	priority := base.priority
	if cfg.priority != nil {
		priority = *cfg.priority
	}
	runAt := cfg.runAt
	if runAt.IsZero() {
		if cfg.delay != nil {
			runAt = now.Add(*cfg.delay)
		}
	}
	if cfg.uniqueKey != "" && cfg.uniquePolicy == "" {
		cfg.uniquePolicy = UniqueReturnExisting
	}
	req := enqueueRequest{
		ID:                r.ids.newJobID(now),
		Name:              base.name,
		Queue:             base.queue,
		TenantID:          cfg.tenantID,
		Actor:             cfg.actor,
		UniqueKey:         cfg.uniqueKey,
		UniquePolicy:      cfg.uniquePolicy,
		ConcurrencyKey:    cfg.concurrencyKey,
		Priority:          priority,
		MaxAttempts:       base.maxAttempts,
		Timeout:           base.timeout,
		RunAt:             runAt.UTC(),
		Payload:           payload,
		Metadata:          metadata,
		Tags:              copyTags(cfg.tags),
		TraceID:           cfg.traceID,
		RetentionOverride: base.retention,
	}
	if req.TraceID == "" {
		req.TraceID = traceIDFromContext(ctx)
	}
	if cfg.parent != nil {
		req.ParentJobID = cfg.parent.ID
	}
	return req, nil
}

func definitionName(def any) (string, bool) {
	contract, err := definitionFromAny(def)
	if err != nil {
		return "", false
	}
	return contract.base().name, true
}

func authorizeSnapshot(raw *RawSnapshot, scope authScope) error {
	if raw == nil {
		return ErrNotFound
	}
	if scope.admin {
		return nil
	}
	if scope.tenantID != "" && raw.TenantID != "" && scope.tenantID == raw.TenantID {
		return nil
	}
	if !scope.actor.Empty() && !raw.Actor.Empty() && scope.actor == raw.Actor {
		return nil
	}
	return ErrUnauthorized
}

func authorizeAdmin(scope authScope) error {
	if scope.admin {
		return nil
	}
	return ErrUnauthorized
}

func (r *Runtime) checkBudget(kind BudgetKind, payload []byte) error {
	limit := 0
	switch kind {
	case BudgetPayload:
		limit = r.cfg.PayloadLimit
	case BudgetMetadata:
		limit = r.cfg.MetadataLimit
	case BudgetOutput:
		limit = r.cfg.OutputLimit
	case BudgetProgress:
		limit = r.cfg.ProgressLimit
	}
	if limit > 0 && len(payload) > limit {
		return &BudgetError{Kind: kind, Observed: len(payload), Limit: limit}
	}
	return nil
}

func safeErrorString(err error) string {
	if err == nil {
		return ""
	}
	decision := classifyError(err)
	if decision.safe != "" {
		return decision.safe
	}
	return "job execution failed"
}

func errorCode(err error) string {
	if err == nil {
		return ""
	}
	return classifyError(err).code
}

func isPermanent(err error) bool {
	if err == nil {
		return false
	}
	return classifyError(err).permanent
}

func explicitRetryDelay(err error) (time.Duration, bool) {
	if err == nil {
		return 0, false
	}
	decision := classifyError(err)
	if decision.retryAfter == nil {
		return 0, false
	}
	return *decision.retryAfter, true
}

type jobUpdateHub struct {
	buffer    int
	broadcast BroadcastBackend
	logger    *slog.Logger
	mu        sync.Mutex
	closed    bool
	subs      map[string]map[chan struct{}]struct{}
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

func newJobUpdateHub(buffer int, broadcast BroadcastBackend, logger *slog.Logger) *jobUpdateHub {
	if buffer < 1 {
		buffer = 1
	}
	hub := &jobUpdateHub{
		buffer:    buffer,
		broadcast: broadcast,
		logger:    logger,
		subs:      make(map[string]map[chan struct{}]struct{}),
	}
	hub.start()
	return hub
}

func (h *jobUpdateHub) publish(ctx context.Context, jobID string) {
	h.notify(jobID)
	if h.broadcast != nil {
		payload, err := json.Marshal(map[string]string{"id": jobID})
		if err == nil {
			_ = h.broadcast.Publish(ctx, "vango_jobs_updates", payload)
		}
	}
}

func (h *jobUpdateHub) notify(jobID string) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	targets := make([]chan struct{}, 0)
	if subs := h.subs[jobID]; len(subs) > 0 {
		for ch := range subs {
			targets = append(targets, ch)
		}
	}
	if jobID != "" {
		if subs := h.subs["*"]; len(subs) > 0 {
			for ch := range subs {
				targets = append(targets, ch)
			}
		}
	}
	h.mu.Unlock()
	for _, ch := range targets {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (h *jobUpdateHub) start() {
	if h == nil || h.broadcast == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := h.broadcast.Subscribe(ctx, "vango_jobs_updates")
	if err != nil {
		cancel()
		if h.logger != nil {
			h.logger.Error("jobs: subscribe broadcast updates", "error", err)
		}
		return
	}
	h.cancel = cancel
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case payload, ok := <-ch:
				if !ok {
					return
				}
				var message struct {
					ID string `json:"id"`
				}
				if err := json.Unmarshal(payload, &message); err != nil {
					if h.logger != nil {
						h.logger.Warn("jobs: decode broadcast update", "error", err)
					}
					continue
				}
				h.notify(message.ID)
			}
		}
	}()
}

func (r *Runtime) now() time.Time {
	if r != nil && r.cfg.Now != nil {
		return r.cfg.Now().UTC()
	}
	return time.Now().UTC()
}

func (h *jobUpdateHub) subscribe(ctx context.Context, jobID string) <-chan struct{} {
	ch := make(chan struct{}, h.buffer)
	h.mu.Lock()
	if h.closed {
		close(ch)
		h.mu.Unlock()
		return ch
	}
	if h.subs[jobID] == nil {
		h.subs[jobID] = make(map[chan struct{}]struct{})
	}
	h.subs[jobID][ch] = struct{}{}
	h.mu.Unlock()
	if ctx != nil {
		go func() {
			<-ctx.Done()
			shouldClose := false
			h.mu.Lock()
			if subs := h.subs[jobID]; subs != nil {
				if _, ok := subs[ch]; ok {
					delete(subs, ch)
					shouldClose = true
					if len(subs) == 0 {
						delete(h.subs, jobID)
					}
				}
			}
			h.mu.Unlock()
			if shouldClose {
				close(ch)
			}
		}()
	}
	return ch
}

func (h *jobUpdateHub) close() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	cancel := h.cancel
	subs := h.subs
	h.subs = nil
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	h.wg.Wait()
	for _, subs := range subs {
		for ch := range subs {
			close(ch)
		}
	}
}
