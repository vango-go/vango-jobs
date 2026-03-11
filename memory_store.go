package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type memoryStore struct {
	mu           sync.Mutex
	cfg          Config
	ids          *idGenerator
	jobs         map[string]*memoryJob
	attemptRows  map[string][]AttemptSnapshot
	eventRows    map[string][]EventSnapshot
	queueState   map[string]memoryQueue
	concurrency  map[string]memoryConcurrency
	scheduleRows map[string]memorySchedule
	nextEventID  int64
	wakeupSubs   map[chan string]struct{}
}

type memoryJob struct {
	snapshot    RawSnapshot
	timeout     time.Duration
	retention   time.Duration
	retainUntil *time.Time
}

type memoryQueue struct {
	Paused    bool
	Reason    string
	UpdatedAt time.Time
}

type memoryConcurrency struct {
	JobID      string
	LeaseToken string
	ExpiresAt  time.Time
}

type memorySchedule struct {
	Name      string
	Target    string
	Spec      string
	Timezone  string
	Paused    bool
	NextRunAt time.Time
	LastRunAt *time.Time
	LastJobID string
	UpdatedAt time.Time
}

func newMemoryStore(cfg Config) *memoryStore {
	return &memoryStore{
		cfg:          cfg,
		ids:          newDeterministicIDGenerator(1),
		jobs:         make(map[string]*memoryJob),
		attemptRows:  make(map[string][]AttemptSnapshot),
		eventRows:    make(map[string][]EventSnapshot),
		queueState:   make(map[string]memoryQueue),
		concurrency:  make(map[string]memoryConcurrency),
		scheduleRows: make(map[string]memorySchedule),
		wakeupSubs:   make(map[chan string]struct{}),
	}
}

// NewInMemory creates a deterministic in-memory runtime suitable for tests.
func NewInMemory(reg *Registry, opts ...RuntimeOption) (*Runtime, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	store := newMemoryStore(cfg)
	rt, err := NewRuntime(store, reg, opts...)
	if err != nil {
		return nil, err
	}
	rt.ids = store.ids
	return rt, nil
}

func (s *memoryStore) enqueue(ctx context.Context, req enqueueRequest) (Ref, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enqueueLocked(req, "")
}

func (s *memoryStore) enqueueTx(ctx context.Context, tx pgx.Tx, req enqueueRequest) (Ref, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enqueueLocked(req, "")
}

func (s *memoryStore) enqueueLocked(req enqueueRequest, retriedFromID string) (Ref, error) {
	now := s.cfg.Now()
	if req.RunAt.IsZero() {
		req.RunAt = now
	}
	if req.UniqueKey != "" {
		if existing := s.findActiveUnique(req.Name, req.UniqueKey); existing != nil {
			if req.UniquePolicy == UniqueRejectDuplicate {
				return Ref{}, ErrDuplicateActiveJob
			}
			return existing.snapshot.Ref, nil
		}
	}
	job := &memoryJob{
		snapshot: RawSnapshot{
			Ref:              Ref{ID: req.ID, Name: req.Name},
			Queue:            req.Queue,
			Status:           StatusQueued,
			TenantID:         req.TenantID,
			Actor:            req.Actor,
			MaxAttempts:      req.MaxAttempts,
			RunAt:            req.RunAt,
			CreatedAt:        now,
			UpdatedAt:        now,
			Priority:         req.Priority,
			UniqueKey:        req.UniqueKey,
			ConcurrencyKey:   req.ConcurrencyKey,
			ParentJobID:      req.ParentJobID,
			RetriedFromJobID: retriedFromID,
			TraceID:          req.TraceID,
			Tags:             copyTags(req.Tags),
			Metadata:         copyMetadata(req.Metadata),
			Payload:          append([]byte(nil), req.Payload...),
		},
		timeout:   req.Timeout,
		retention: req.RetentionOverride,
	}
	if _, ok := s.queueState[req.Queue]; !ok {
		s.queueState[req.Queue] = memoryQueue{UpdatedAt: now}
	}
	s.jobs[job.snapshot.Ref.ID] = job
	s.appendEvent(job.snapshot.Ref.ID, EventEnqueued, nil)
	s.broadcastWakeup(job.snapshot.Queue)
	return job.snapshot.Ref, nil
}

func (s *memoryStore) inspect(ctx context.Context, id string) (*RawSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[id]
	if job == nil {
		return nil, ErrNotFound
	}
	out := job.snapshot.Clone()
	return out, nil
}

func (s *memoryStore) list(ctx context.Context, filter ListFilter, scope authScope) ([]*RawSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	limit := normalizeListLimit(filter.Limit)
	statusSet := make(map[Status]struct{}, len(filter.Statuses))
	if len(filter.Statuses) > 0 {
		for _, status := range filter.Statuses {
			statusSet[status] = struct{}{}
		}
	}

	out := make([]*RawSnapshot, 0, limit)
	for _, job := range s.jobs {
		if len(statusSet) > 0 {
			if _, ok := statusSet[job.snapshot.Status]; !ok {
				continue
			}
		}
		if filter.Queue != "" && job.snapshot.Queue != filter.Queue {
			continue
		}
		if filter.JobName != "" && job.snapshot.Ref.Name != filter.JobName {
			continue
		}
		if filter.TenantID != "" && job.snapshot.TenantID != filter.TenantID {
			continue
		}
		if filter.Since != nil && job.snapshot.CreatedAt.Before(*filter.Since) {
			continue
		}
		if filter.Until != nil && job.snapshot.CreatedAt.After(*filter.Until) {
			continue
		}
		if err := authorizeSnapshot(&job.snapshot, scope); err != nil {
			continue
		}
		out = append(out, job.snapshot.Clone())
	}
	sort.Slice(out, func(i, j int) bool {
		order := normalizeListOrder(filter.Order)
		left := snapshotListTime(out[i], order)
		right := snapshotListTime(out[j], order)
		if left.Equal(right) {
			if out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
				if out[i].CreatedAt.Equal(out[j].CreatedAt) {
					return out[i].Ref.ID > out[j].Ref.ID
				}
				return out[i].CreatedAt.After(out[j].CreatedAt)
			}
			return out[i].UpdatedAt.After(out[j].UpdatedAt)
		}
		return left.After(right)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func snapshotListTime(snap *RawSnapshot, order ListOrder) time.Time {
	if snap == nil {
		return time.Time{}
	}
	switch normalizeListOrder(order) {
	case ListOrderTerminalTimeDesc:
		if snap.FinishedAt != nil {
			return snap.FinishedAt.UTC()
		}
		if !snap.UpdatedAt.IsZero() {
			return snap.UpdatedAt.UTC()
		}
		return snap.CreatedAt.UTC()
	default:
		return snap.CreatedAt.UTC()
	}
}

func (s *memoryStore) attempts(ctx context.Context, id string) ([]AttemptSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jobs[id] == nil {
		return nil, ErrNotFound
	}
	attempts := s.attemptRows[id]
	out := make([]AttemptSnapshot, len(attempts))
	for i := range attempts {
		out[i] = attempts[len(attempts)-1-i]
	}
	return out, nil
}

func (s *memoryStore) events(ctx context.Context, id string) ([]EventSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jobs[id] == nil {
		return nil, ErrNotFound
	}
	events := s.eventRows[id]
	out := make([]EventSnapshot, len(events))
	for i := range events {
		event := events[len(events)-1-i]
		out[i] = event
		out[i].Payload = append([]byte(nil), event.Payload...)
	}
	return out, nil
}

func (s *memoryStore) cancel(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[id]
	if job == nil {
		return ErrNotFound
	}
	now := s.cfg.Now()
	switch job.snapshot.Status {
	case StatusQueued:
		job.snapshot.Status = StatusCanceled
		job.snapshot.CancelRequestedAt = &now
		job.snapshot.FinishedAt = &now
		job.snapshot.UpdatedAt = now
		s.setRetainUntil(job, StatusCanceled, now, 0)
		s.appendEvent(id, EventCanceled, nil)
	case StatusRunning:
		job.snapshot.CancelRequestedAt = &now
		job.snapshot.UpdatedAt = now
	}
	return nil
}

func (s *memoryStore) retry(ctx context.Context, id string, req retryCloneRequest) (Ref, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	source := s.jobs[id]
	if source == nil {
		return Ref{}, ErrNotFound
	}
	if source.snapshot.Status != StatusFailed && source.snapshot.Status != StatusCanceled {
		return Ref{}, errors.New("jobs: retry requires failed or canceled job")
	}
	clone := enqueueRequest{
		ID:                req.NewJobID(req.Now),
		Name:              source.snapshot.Ref.Name,
		Queue:             source.snapshot.Queue,
		TenantID:          source.snapshot.TenantID,
		Actor:             source.snapshot.Actor,
		ParentJobID:       source.snapshot.ParentJobID,
		UniqueKey:         source.snapshot.UniqueKey,
		UniquePolicy:      UniqueRejectDuplicate,
		ConcurrencyKey:    source.snapshot.ConcurrencyKey,
		Priority:          source.snapshot.Priority,
		MaxAttempts:       source.snapshot.MaxAttempts,
		Timeout:           source.timeout,
		RunAt:             req.Now,
		Payload:           append([]byte(nil), source.snapshot.Payload...),
		Metadata:          copyMetadata(source.snapshot.Metadata),
		Tags:              copyTags(source.snapshot.Tags),
		TraceID:           source.snapshot.TraceID,
		RetentionOverride: source.retention,
	}
	if req.OverrideQueue != "" {
		clone.Queue = req.OverrideQueue
	}
	if req.OverrideRunAt != nil {
		clone.RunAt = req.OverrideRunAt.UTC()
	} else if req.OverrideDelay != nil {
		clone.RunAt = req.Now.Add(*req.OverrideDelay)
	}
	if !req.PreserveUnique {
		clone.UniqueKey = ""
	}
	return s.enqueueLocked(clone, id)
}

func (s *memoryStore) pauseQueue(ctx context.Context, queue string, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queueState[queue] = memoryQueue{Paused: true, Reason: reason, UpdatedAt: s.cfg.Now()}
	return nil
}

func (s *memoryStore) resumeQueue(ctx context.Context, queue string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queueState[queue] = memoryQueue{UpdatedAt: s.cfg.Now()}
	s.broadcastWakeup(queue)
	return nil
}

func (s *memoryStore) queue(ctx context.Context, name string) (*QueueSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshots := s.queueSnapshotsLocked()
	for _, snapshot := range snapshots {
		if snapshot.Queue == name {
			copySnapshot := snapshot
			return &copySnapshot, nil
		}
	}
	return nil, ErrNotFound
}

func (s *memoryStore) queues(ctx context.Context) ([]QueueSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queueSnapshotsLocked(), nil
}

func (s *memoryStore) replay(ctx context.Context, filter ReplayFilter, req retryCloneRequest) (*ReplaySummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	summary := &ReplaySummary{}
	candidates := s.replayCandidates(filter, req.Scope)
	limit := filter.Limit
	if limit <= 0 || limit > req.MaxBatch {
		limit = req.MaxBatch
	}
	for _, job := range candidates {
		if summary.Selected >= limit {
			break
		}
		summary.Selected++
		clone := enqueueRequest{
			ID:                req.NewJobID(req.Now),
			Name:              job.snapshot.Ref.Name,
			Queue:             job.snapshot.Queue,
			TenantID:          job.snapshot.TenantID,
			Actor:             job.snapshot.Actor,
			ParentJobID:       job.snapshot.ParentJobID,
			UniqueKey:         job.snapshot.UniqueKey,
			UniquePolicy:      UniqueRejectDuplicate,
			ConcurrencyKey:    job.snapshot.ConcurrencyKey,
			Priority:          job.snapshot.Priority,
			MaxAttempts:       job.snapshot.MaxAttempts,
			Timeout:           job.timeout,
			RunAt:             req.Now,
			Payload:           append([]byte(nil), job.snapshot.Payload...),
			Metadata:          copyMetadata(job.snapshot.Metadata),
			Tags:              copyTags(job.snapshot.Tags),
			TraceID:           job.snapshot.TraceID,
			RetentionOverride: job.retention,
		}
		if req.OverrideQueue != "" {
			clone.Queue = req.OverrideQueue
		}
		if req.OverrideRunAt != nil {
			clone.RunAt = req.OverrideRunAt.UTC()
		} else if req.OverrideDelay != nil {
			clone.RunAt = req.Now.Add(*req.OverrideDelay)
		}
		if !req.PreserveUnique {
			clone.UniqueKey = ""
		}
		ref, err := s.enqueueLocked(clone, job.snapshot.Ref.ID)
		if err != nil {
			summary.Failed++
			continue
		}
		summary.Cloned++
		summary.Refs = append(summary.Refs, ref)
	}
	return summary, nil
}

func (s *memoryStore) claim(ctx context.Context, req claimRequest) ([]claimedJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.cfg.Now()
	candidates := s.claimableJobs(now, req)
	claimed := make([]claimedJob, 0, req.Limit)
	for _, job := range candidates {
		if len(claimed) >= req.Limit {
			break
		}
		if job.snapshot.ConcurrencyKey != "" {
			if slot, ok := s.concurrency[job.snapshot.ConcurrencyKey]; ok && slot.ExpiresAt.After(now) {
				continue
			}
		}
		leaseToken := s.ids.newLeaseToken(now)
		job.snapshot.Status = StatusRunning
		job.snapshot.Attempts++
		job.snapshot.UpdatedAt = now
		job.snapshot.WorkerID = req.WorkerID
		job.snapshot.LeaseExpiresAt = ptrTime(now.Add(req.LeaseDuration))
		job.snapshot.FirstStartedAt = coalesceTime(job.snapshot.FirstStartedAt, now)
		job.snapshot.LastStartedAt = &now
		if job.snapshot.ConcurrencyKey != "" {
			s.concurrency[job.snapshot.ConcurrencyKey] = memoryConcurrency{
				JobID:      job.snapshot.Ref.ID,
				LeaseToken: leaseToken,
				ExpiresAt:  now.Add(req.LeaseDuration),
			}
		}
		s.attemptRows[job.snapshot.Ref.ID] = append(s.attemptRows[job.snapshot.Ref.ID], AttemptSnapshot{
			ID:         s.ids.newAttemptID(now),
			JobID:      job.snapshot.Ref.ID,
			Attempt:    job.snapshot.Attempts,
			WorkerID:   req.WorkerID,
			LeaseToken: leaseToken,
			Status:     AttemptRunning,
			StartedAt:  now,
			CreatedAt:  now,
		})
		s.appendEvent(job.snapshot.Ref.ID, EventStarted, []byte(`{}`))
		claimed = append(claimed, claimedJob{
			JobID:            job.snapshot.Ref.ID,
			JobName:          job.snapshot.Ref.Name,
			Queue:            job.snapshot.Queue,
			LeaseToken:       leaseToken,
			TraceID:          job.snapshot.TraceID,
			TenantID:         job.snapshot.TenantID,
			Actor:            job.snapshot.Actor,
			Metadata:         copyMetadata(job.snapshot.Metadata),
			Attempts:         job.snapshot.Attempts,
			MaxAttempts:      job.snapshot.MaxAttempts,
			Timeout:          job.timeout,
			CreatedAt:        job.snapshot.CreatedAt,
			AttemptStartedAt: now,
			Payload:          append([]byte(nil), job.snapshot.Payload...),
			ParentJobID:      job.snapshot.ParentJobID,
			ConcurrencyKey:   job.snapshot.ConcurrencyKey,
			UniqueKey:        job.snapshot.UniqueKey,
		})
	}
	return claimed, nil
}

func (s *memoryStore) heartbeat(ctx context.Context, req heartbeatRequest) (heartbeatResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[req.JobID]
	if job == nil || job.snapshot.Status != StatusRunning {
		return heartbeatResult{LeaseHeld: false}, nil
	}
	if job.snapshot.LeaseExpiresAt == nil || job.snapshot.LeaseExpiresAt.Before(s.cfg.Now()) {
		return heartbeatResult{LeaseHeld: false}, nil
	}
	next := s.cfg.Now().Add(req.LeaseDuration)
	job.snapshot.LeaseExpiresAt = &next
	job.snapshot.UpdatedAt = s.cfg.Now()
	if job.snapshot.ConcurrencyKey != "" {
		slot := s.concurrency[job.snapshot.ConcurrencyKey]
		slot.ExpiresAt = next
		s.concurrency[job.snapshot.ConcurrencyKey] = slot
	}
	return heartbeatResult{
		LeaseHeld:       true,
		CancelRequested: job.snapshot.CancelRequestedAt != nil,
	}, nil
}

func (s *memoryStore) writeProgress(ctx context.Context, req progressWriteRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[req.JobID]
	if job == nil || job.snapshot.Status != StatusRunning {
		return ErrNotFound
	}
	progress := req.Progress
	progress.Metadata = copyMetadata(progress.Metadata)
	job.snapshot.Progress = &progress
	job.snapshot.UpdatedAt = s.cfg.Now()
	payload, _ := json.Marshal(progress)
	s.appendEvent(job.snapshot.Ref.ID, EventProgress, payload)
	return nil
}

func (s *memoryStore) complete(ctx context.Context, req completionRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[req.JobID]
	if job == nil || job.snapshot.Status != StatusRunning {
		return ErrNotFound
	}
	now := s.cfg.Now()
	if req.Progress != nil {
		job.snapshot.Progress = cloneProgress(req.Progress)
	}
	switch req.Kind {
	case completionSuccess:
		job.snapshot.Status = StatusSucceeded
		job.snapshot.Output = append([]byte(nil), req.Output...)
		job.snapshot.SafeError = ""
		job.snapshot.ErrorCode = ""
		job.snapshot.FinishedAt = &now
		s.setRetainUntil(job, StatusSucceeded, now, req.Retention)
		s.appendEvent(req.JobID, EventSucceeded, nil)
	case completionRetry:
		job.snapshot.Status = StatusQueued
		job.snapshot.RunAt = now.Add(req.RetryDelay)
		job.snapshot.Output = nil
		job.snapshot.SafeError = req.SafeError
		job.snapshot.ErrorCode = req.ErrorCode
		job.snapshot.FinishedAt = nil
		job.retainUntil = nil
		s.appendEvent(req.JobID, EventRetried, nil)
	case completionFailure:
		job.snapshot.Status = StatusFailed
		job.snapshot.SafeError = req.SafeError
		job.snapshot.ErrorCode = req.ErrorCode
		job.snapshot.FinishedAt = &now
		s.setRetainUntil(job, StatusFailed, now, req.Retention)
		s.appendEvent(req.JobID, EventFailed, nil)
	case completionCanceled:
		job.snapshot.Status = StatusCanceled
		job.snapshot.FinishedAt = &now
		s.setRetainUntil(job, StatusCanceled, now, req.Retention)
		s.appendEvent(req.JobID, EventCanceled, nil)
	}
	job.snapshot.UpdatedAt = now
	job.snapshot.WorkerID = ""
	job.snapshot.LeaseExpiresAt = nil
	if job.snapshot.ConcurrencyKey != "" {
		delete(s.concurrency, job.snapshot.ConcurrencyKey)
	}
	if attempts := s.attemptRows[req.JobID]; len(attempts) > 0 {
		last := &attempts[len(attempts)-1]
		last.EndedAt = &now
		last.Duration = now.Sub(last.StartedAt)
		last.SafeError = req.SafeError
		last.ErrorCode = req.ErrorCode
		last.RetryDelay = req.RetryDelay
		switch req.Kind {
		case completionSuccess:
			last.Status = AttemptSucceeded
		case completionRetry:
			last.Status = AttemptRetryScheduled
		case completionFailure:
			last.Status = AttemptFailed
		case completionCanceled:
			last.Status = AttemptCanceled
		}
		s.attemptRows[req.JobID] = attempts
	}
	return nil
}

func (s *memoryStore) reap(ctx context.Context, req reapRequest) ([]reapedJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.cfg.Now()
	results := make([]reapedJob, 0)
	for _, job := range s.jobs {
		if job.snapshot.Status != StatusRunning || job.snapshot.LeaseExpiresAt == nil || !job.snapshot.LeaseExpiresAt.Before(now) {
			continue
		}
		if attempts := s.attemptRows[job.snapshot.Ref.ID]; len(attempts) > 0 {
			last := &attempts[len(attempts)-1]
			last.Status = AttemptAbandoned
			last.EndedAt = &now
			last.Duration = now.Sub(last.StartedAt)
			s.attemptRows[job.snapshot.Ref.ID] = attempts
		}
		s.appendEvent(job.snapshot.Ref.ID, EventAbandoned, nil)
		result := reapedJob{
			JobID:   job.snapshot.Ref.ID,
			JobName: job.snapshot.Ref.Name,
			Queue:   job.snapshot.Queue,
		}
		if job.snapshot.ConcurrencyKey != "" {
			delete(s.concurrency, job.snapshot.ConcurrencyKey)
		}
		switch {
		case job.snapshot.CancelRequestedAt != nil:
			job.snapshot.Status = StatusCanceled
			job.snapshot.FinishedAt = &now
			retention := s.cfg.CanceledRetention
			if req.Retention != nil {
				retention = req.Retention(job.snapshot.Ref.Name, StatusCanceled)
			}
			s.setRetainUntil(job, StatusCanceled, now, retention)
			s.appendEvent(job.snapshot.Ref.ID, EventCanceled, nil)
			result.Outcome = EventCanceled
		case job.snapshot.Attempts >= job.snapshot.MaxAttempts:
			job.snapshot.Status = StatusFailed
			job.snapshot.SafeError = "job lease expired before completion"
			job.snapshot.ErrorCode = "job_abandoned"
			job.snapshot.FinishedAt = &now
			retention := s.cfg.FailedRetention
			if req.Retention != nil {
				retention = req.Retention(job.snapshot.Ref.Name, StatusFailed)
			}
			s.setRetainUntil(job, StatusFailed, now, retention)
			s.appendEvent(job.snapshot.Ref.ID, EventFailed, nil)
			result.Outcome = EventFailed
			result.ErrorCode = job.snapshot.ErrorCode
		default:
			delay := s.cfg.HeartbeatInterval
			if req.Backoff != nil {
				delay = req.Backoff(job.snapshot.Ref.Name, job.snapshot.Ref.ID, job.snapshot.Attempts)
			}
			job.snapshot.Status = StatusQueued
			job.snapshot.RunAt = now.Add(delay)
			job.snapshot.SafeError = "job lease expired before completion"
			job.snapshot.ErrorCode = "job_abandoned"
			job.snapshot.FinishedAt = nil
			job.retainUntil = nil
			s.appendEvent(job.snapshot.Ref.ID, EventRetried, nil)
			result.Outcome = EventRetried
			result.ErrorCode = job.snapshot.ErrorCode
		}
		job.snapshot.WorkerID = ""
		job.snapshot.LeaseExpiresAt = nil
		job.snapshot.UpdatedAt = now
		results = append(results, result)
	}
	return results, nil
}

func (s *memoryStore) ensureSchedules(ctx context.Context, schedules []registeredSchedule) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.cfg.Now()
	for _, schedule := range schedules {
		existing, ok := s.scheduleRows[schedule.name()]
		nextRunAt, err := schedule.next(now)
		if err != nil {
			return err
		}
		if !ok {
			s.scheduleRows[schedule.name()] = memorySchedule{
				Name:      schedule.name(),
				Target:    schedule.targetName(),
				Spec:      schedule.spec(),
				Timezone:  schedule.timezone().String(),
				NextRunAt: nextRunAt,
				UpdatedAt: now,
			}
			continue
		}
		existing.Target = schedule.targetName()
		existing.Spec = schedule.spec()
		existing.Timezone = schedule.timezone().String()
		existing.UpdatedAt = now
		s.scheduleRows[schedule.name()] = existing
	}
	return nil
}

func (s *memoryStore) tickSchedules(ctx context.Context, req scheduleTickRequest) ([]scheduledRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.scheduleRows))
	for name, sched := range s.scheduleRows {
		if sched.Paused || sched.NextRunAt.After(req.Now) {
			continue
		}
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return s.scheduleRows[names[i]].NextRunAt.Before(s.scheduleRows[names[j]].NextRunAt)
	})
	if req.Limit > 0 && len(names) > req.Limit {
		names = names[:req.Limit]
	}
	refs := make([]scheduledRef, 0, len(names))
	for _, name := range names {
		sched := req.Schedules[name]
		if sched == nil {
			continue
		}
		state := s.scheduleRows[name]
		input, err := sched.inputFor(ctx, state.NextRunAt)
		if err != nil {
			return nil, err
		}
		def := sched.targetDefinition()
		payload, err := def.encodeInputAny(req.Codec, input)
		if err != nil {
			return nil, err
		}
		if req.CheckBudget != nil {
			if err := req.CheckBudget(BudgetPayload, payload); err != nil {
				return nil, err
			}
		}
		base := def.base()
		jobID := req.NewJobID(req.Now)
		ref, err := s.enqueueLocked(enqueueRequest{
			ID:                jobID,
			Name:              base.name,
			Queue:             base.queue,
			UniqueKey:         "schedule:" + name + ":" + state.NextRunAt.UTC().Format(time.RFC3339Nano),
			UniquePolicy:      UniqueReturnExisting,
			Priority:          base.priority,
			MaxAttempts:       base.maxAttempts,
			Timeout:           base.timeout,
			RunAt:             req.Now,
			Payload:           payload,
			RetentionOverride: base.retention,
		}, "")
		if err != nil {
			return nil, err
		}
		nextRunAt, err := sched.next(state.NextRunAt)
		if err != nil {
			return nil, err
		}
		lastRunAt := state.NextRunAt
		state.LastRunAt = &lastRunAt
		state.LastJobID = ref.ID
		state.NextRunAt = nextRunAt
		state.UpdatedAt = s.cfg.Now()
		s.scheduleRows[name] = state
		refs = append(refs, scheduledRef{
			Ref:          ref,
			Queue:        base.queue,
			ScheduleName: name,
			DueAt:        lastRunAt,
			Inserted:     ref.ID == jobID,
		})
	}
	return refs, nil
}

func (s *memoryStore) pauseSchedule(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.scheduleRows[name]
	if !ok {
		return ErrNotFound
	}
	state.Paused = true
	state.UpdatedAt = s.cfg.Now()
	s.scheduleRows[name] = state
	return nil
}

func (s *memoryStore) resumeSchedule(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.scheduleRows[name]
	if !ok {
		return ErrNotFound
	}
	state.Paused = false
	state.UpdatedAt = s.cfg.Now()
	s.scheduleRows[name] = state
	return nil
}

func (s *memoryStore) schedule(ctx context.Context, name string) (*ScheduleSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.scheduleRows[name]
	if !ok {
		return nil, ErrNotFound
	}
	snapshot := scheduleSnapshotFromMemory(state)
	return &snapshot, nil
}

func (s *memoryStore) schedules(ctx context.Context) ([]ScheduleSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.scheduleRows))
	for name := range s.scheduleRows {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]ScheduleSnapshot, 0, len(names))
	for _, name := range names {
		out = append(out, scheduleSnapshotFromMemory(s.scheduleRows[name]))
	}
	return out, nil
}

func (s *memoryStore) prune(ctx context.Context, req pruneRequest) (*PruneSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.cfg.Now()
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	ids := make([]string, 0, limit)
	for id, job := range s.jobs {
		if len(ids) >= limit {
			break
		}
		if !job.snapshot.Status.Terminal() || job.retainUntil == nil || job.retainUntil.After(now) {
			continue
		}
		ids = append(ids, id)
	}
	for _, id := range ids {
		delete(s.jobs, id)
		delete(s.attemptRows, id)
		delete(s.eventRows, id)
	}
	return &PruneSummary{Deleted: len(ids), JobIDs: ids}, nil
}

func (s *memoryStore) subscribeWakeup(ctx context.Context) (<-chan string, func(), error) {
	ch := make(chan string, 8)
	s.mu.Lock()
	s.wakeupSubs[ch] = struct{}{}
	s.mu.Unlock()
	cleanup := func() {
		s.mu.Lock()
		_, ok := s.wakeupSubs[ch]
		delete(s.wakeupSubs, ch)
		s.mu.Unlock()
		if ok {
			close(ch)
		}
	}
	if ctx != nil {
		go func() {
			<-ctx.Done()
			cleanup()
		}()
	}
	return ch, cleanup, nil
}

func (s *memoryStore) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.wakeupSubs {
		close(ch)
		delete(s.wakeupSubs, ch)
	}
	return nil
}

func (s *memoryStore) findActiveUnique(name, key string) *memoryJob {
	for _, job := range s.jobs {
		if job.snapshot.Ref.Name != name || job.snapshot.UniqueKey != key {
			continue
		}
		if job.snapshot.Status == StatusQueued || job.snapshot.Status == StatusRunning {
			return job
		}
	}
	return nil
}

func (s *memoryStore) claimableJobs(now time.Time, req claimRequest) []*memoryJob {
	queueSet := make(map[string]struct{}, len(req.Queues))
	for _, queue := range req.Queues {
		queueSet[queue] = struct{}{}
	}
	nameSet := make(map[string]struct{}, len(req.JobNames))
	for _, name := range req.JobNames {
		nameSet[name] = struct{}{}
	}
	jobs := make([]*memoryJob, 0)
	for _, job := range s.jobs {
		if job.snapshot.Status != StatusQueued || job.snapshot.RunAt.After(now) {
			continue
		}
		if len(queueSet) > 0 {
			if _, ok := queueSet[job.snapshot.Queue]; !ok {
				continue
			}
		}
		if _, ok := nameSet[job.snapshot.Ref.Name]; !ok {
			continue
		}
		if queue := s.queueState[job.snapshot.Queue]; queue.Paused {
			continue
		}
		jobs = append(jobs, job)
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].snapshot.Priority != jobs[j].snapshot.Priority {
			return jobs[i].snapshot.Priority > jobs[j].snapshot.Priority
		}
		return jobs[i].snapshot.CreatedAt.Before(jobs[j].snapshot.CreatedAt)
	})
	return jobs
}

func (s *memoryStore) replayCandidates(filter ReplayFilter, scope authScope) []*memoryJob {
	candidates := make([]*memoryJob, 0)
	statusSet := make(map[Status]struct{}, len(filter.Statuses))
	if len(filter.Statuses) == 0 {
		statusSet[StatusFailed] = struct{}{}
		statusSet[StatusCanceled] = struct{}{}
	} else {
		for _, status := range filter.Statuses {
			statusSet[status] = struct{}{}
		}
	}
	for _, job := range s.jobs {
		if _, ok := statusSet[job.snapshot.Status]; !ok {
			continue
		}
		if filter.Queue != "" && job.snapshot.Queue != filter.Queue {
			continue
		}
		if filter.JobName != "" && job.snapshot.Ref.Name != filter.JobName {
			continue
		}
		if filter.TenantID != "" && job.snapshot.TenantID != filter.TenantID {
			continue
		}
		if filter.Since != nil && job.snapshot.CreatedAt.Before(*filter.Since) {
			continue
		}
		if filter.Until != nil && job.snapshot.CreatedAt.After(*filter.Until) {
			continue
		}
		if err := authorizeSnapshot(&job.snapshot, scope); err != nil {
			continue
		}
		candidates = append(candidates, job)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].snapshot.CreatedAt.Before(candidates[j].snapshot.CreatedAt)
	})
	return candidates
}

func (s *memoryStore) appendEvent(jobID string, kind EventKind, payload []byte) {
	s.nextEventID++
	s.eventRows[jobID] = append(s.eventRows[jobID], EventSnapshot{
		ID:      s.nextEventID,
		JobID:   jobID,
		Kind:    kind,
		At:      s.cfg.Now(),
		Payload: append([]byte(nil), payload...),
	})
}

func (s *memoryStore) broadcastWakeup(queue string) {
	for ch := range s.wakeupSubs {
		select {
		case ch <- queue:
		default:
		}
	}
}

func (s *memoryStore) queueSnapshotsLocked() []QueueSnapshot {
	queueSet := make(map[string]struct{}, len(s.queueState))
	for name := range s.queueState {
		queueSet[name] = struct{}{}
	}
	for _, job := range s.jobs {
		queueSet[job.snapshot.Queue] = struct{}{}
	}
	names := make([]string, 0, len(queueSet))
	for name := range queueSet {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]QueueSnapshot, 0, len(names))
	for _, name := range names {
		queue := s.queueState[name]
		snapshot := QueueSnapshot{
			Queue:       name,
			Paused:      queue.Paused,
			PauseReason: queue.Reason,
			UpdatedAt:   queue.UpdatedAt,
		}
		for _, job := range s.jobs {
			if job.snapshot.Queue != name {
				continue
			}
			switch job.snapshot.Status {
			case StatusQueued:
				snapshot.Queued++
			case StatusRunning:
				snapshot.Running++
			}
		}
		out = append(out, snapshot)
	}
	return out
}

func scheduleSnapshotFromMemory(state memorySchedule) ScheduleSnapshot {
	return ScheduleSnapshot{
		Name:          state.Name,
		TargetJobName: state.Target,
		Spec:          state.Spec,
		Timezone:      state.Timezone,
		Paused:        state.Paused,
		NextRunAt:     state.NextRunAt,
		LastRunAt:     state.LastRunAt,
		LastJobID:     state.LastJobID,
		UpdatedAt:     state.UpdatedAt,
	}
}

func (s *memoryStore) setRetainUntil(job *memoryJob, status Status, now time.Time, retention time.Duration) {
	if retention <= 0 {
		retention = s.retentionDuration(job, status)
	}
	until := now.Add(retention)
	job.retainUntil = &until
}

func (s *memoryStore) retentionDuration(job *memoryJob, status Status) time.Duration {
	if job != nil && job.retention > 0 {
		return job.retention
	}
	switch status {
	case StatusSucceeded:
		return s.cfg.SuccessRetention
	case StatusCanceled:
		return s.cfg.CanceledRetention
	case StatusFailed:
		return s.cfg.FailedRetention
	default:
		return s.cfg.SuccessRetention
	}
}

func coalesceTime(cur *time.Time, next time.Time) *time.Time {
	if cur != nil {
		return cur
	}
	return &next
}

func ptrTime(t time.Time) *time.Time {
	return &t
}
