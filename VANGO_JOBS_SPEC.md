# vango-jobs Design Specification

## Canonical Status

This document is the canonical design specification for `github.com/vango-go/vango-jobs` (“vango-jobs”).

It defines the intended package boundaries, runtime model, public API, PostgreSQL storage design, Vango integration contract, operational posture, and phased rollout plan.

This is a design and architecture document, not a lightweight getting-started guide.

If implementation and this document diverge, the implementation must either be corrected or this document must be updated deliberately in the same change.

## Status

- Document status: design-spec draft
- Intended module: `github.com/vango-go/vango-jobs`
- Intended audience:
  - Vango maintainers
  - application developers integrating jobs into Vango apps
  - coding agents implementing and reviewing the system
- Initial backend target: PostgreSQL

## Why This Exists

Vango already has a strong model for:

- synchronous session-loop event handling
- structured async reads through `setup.Resource`
- structured async writes through `setup.Action`
- HTTP-boundary integrations such as webhooks and uploads

What is still missing is a **durable detached work model**.

Real applications need to:

- send emails after a mutation succeeds
- generate exports that may take minutes
- fan out webhooks to downstream services
- retry transient failures safely
- run maintenance and reconciliation work outside the request path
- observe progress and failure state from the UI

Vango needs a first-party answer to that problem.

`vango-jobs` is that answer.

## Design Goals

`vango-jobs` MUST optimize for:

1. **Explicitness**
   - job definitions are named, typed, and registered explicitly
   - no reflection-driven handler discovery
   - no hidden enqueue side effects

2. **Type safety**
   - input and output payloads are strongly typed
   - application code should not default to `map[string]any` or raw untyped JSON

3. **Operational correctness**
   - durable enqueue
   - leases and heartbeat-based crash recovery
   - bounded retries
   - cancellation semantics
   - safe observability

4. **Vango model alignment**
   - jobs are detached from the session loop
   - job enqueueing follows the same structured-boundary philosophy as Resources and Actions
   - UI observation happens through Resources or dedicated helpers, not ad hoc concurrency

5. **Boring infrastructure**
   - PostgreSQL first
   - strong defaults
   - no dependency on Redis, Kafka, or RabbitMQ in v1
   - no attempt to become a distributed workflow platform on day one

6. **AI coding agent friendliness**
   - one blessed path
   - explicit stable names
   - predictable file layout
   - small number of orthogonal concepts
   - deterministic test kit

7. **Security**
   - server-authoritative job creation
   - safe errors and logs
   - tenant and actor scoping
   - secrets kept out of durable payloads by default

## Non-Goals

The initial `vango-jobs` design explicitly does **not** aim to provide:

- exactly-once execution semantics
- arbitrary DAG workflow orchestration
- a Temporal-style durable code-execution engine
- cross-language worker support
- browser/client-side job definitions or direct client enqueue
- non-PostgreSQL backends in v1
- large artifact storage inside job rows
- implicit magical binding to Vango session state or signals

The system is an **at-least-once durable job queue** with strong application ergonomics, not a general workflow engine.

## Placement and Package Boundaries

`vango-jobs` should be **first-party and framework-adjacent**, but **not part of the root `vango` package**.

### Why It Is Not in Core `vango`

The root Vango framework should remain focused on:

- component authoring
- render purity
- reactive state
- session lifecycle
- routing
- HTTP integration boundaries

Background jobs introduce unrelated operational concerns:

- worker loops
- polling and wakeups
- leases and heartbeats
- job retention
- queue control
- retry semantics
- scheduler state
- database migrations

That surface area is too large and too infrastructure-heavy for the root framework package.

### Why It Still Feels “Core”

`vango-jobs` is more central than optional integrations like Stripe or S3 because durable work is a common application concern, not a vendor-specific concern.

The intended posture is:

- first-party module
- documented from the main Vango guide
- scaffolded by the Vango CLI
- supported by Vango devtools
- integrated into Vango examples

### Intended Package Layout

Canonical module:

- `github.com/vango-go/vango-jobs`

Conceptual public packages:

- `github.com/vango-go/vango-jobs`
  - package `jobs`
  - job definitions, registry, enqueue/read/admin interfaces, PostgreSQL runtime, worker
- `github.com/vango-go/vango-jobs/vango`
  - package alias used in examples: `jobsvango`
  - thin Vango integration helpers for status resources and live invalidation
- `github.com/vango-go/vango-jobs/jobstest`
  - deterministic test kit and immediate execution helpers

The root package may own the PostgreSQL runtime directly in v1 for simplicity.

If additional backends are added later, the application-facing API should remain stable.

## Core Mental Model

Vango now has three structured async categories:

1. `setup.Resource`
   - async reads tied to the current UI/session/view state

2. `setup.Action`
   - async writes tied to a user action or request/response mutation

3. `jobs`
   - detached durable work that may complete after the originating request, action, or session is gone

The clean distinction is:

- **Action** means “do it now and return”
- **Job** means “do it durably and observe separately”

### Access Contract

All job operations that mutate durable queue state MUST occur only in:

- Action work functions
- HTTP handlers
- webhooks
- CLI/admin commands
- worker handlers

Job status reads MAY occur in:

- Resource loaders
- HTTP handlers
- admin endpoints
- worker handlers

Job enqueueing MUST NOT occur in:

- Setup callbacks
- render closures
- event handlers themselves
- Effects
- `OnMount`
- `OnChange`

In a Vango component, the normal pattern is:

1. event handler triggers an Action
2. Action work enqueues a job
3. render observes the Action result
4. job status is read through a Resource or helper

### Jobs Are Detached from Session State

Jobs MUST NOT:

- read or write reactive signals
- depend on live session auth state
- assume the existence of an active browser session
- assume the initiating tab still exists

Jobs receive a `jobs.Ctx`, not a `vango.Ctx`.

The only durable application context they should rely on is:

- their typed input payload
- optional tenant and actor metadata
- server-owned services resolved at startup

## Delivery Semantics

`vango-jobs` provides **at-least-once** delivery.

This has important consequences:

- a job may run more than once
- a job may partially complete a side effect and still be retried
- worker crashes after a side effect but before acknowledgement are expected

Therefore:

- handlers MUST be idempotent, or
- handlers MUST make downstream operations idempotent

The framework SHOULD make this easier by providing:

- stable job IDs
- stable per-job idempotency keys
- explicit retry and permanence wrappers

It MUST NOT pretend to provide exactly-once semantics.

## Public API Shape

The application-facing API should be explicit and typed.

### Definitions

Jobs are declared as typed definitions with a stable string name:

```go
var GenerateExport = jobs.New[GenerateExportInput, GenerateExportResult](
	"export.generate.v1",
	jobs.Queue("exports"),
	jobs.MaxAttempts(10),
	jobs.Timeout(15*time.Minute),
	jobs.Backoff(jobs.ExponentialJitter(10*time.Second, 30*time.Minute)),
	jobs.Retention(7*24*time.Hour),
)
```

Rules:

- the string name is a durable compatibility contract
- the name MUST be explicit
- names SHOULD include an application domain and version
- payload-incompatible changes MUST create a new name/version

Examples:

- `email.send_receipt.v1`
- `billing.sync_customer.v1`
- `exports.generate_csv.v2`

The definition object is **metadata**, not a bound handler closure.

### Registry and Handler Registration

Handlers are registered explicitly against a registry at startup:

```go
func RegisterJobs(reg *jobs.Registry, deps Deps) {
	reg.Handle(GenerateExport, func(ctx jobs.Ctx, in GenerateExportInput) (GenerateExportResult, error) {
		// durable work
		return GenerateExportResult{FileKey: key}, nil
	})
}
```

This separation is intentional:

- the definition remains a stable typed symbol
- handlers can close over application dependencies at startup
- test code can register alternative handlers

The registry MUST reject duplicate handler registration for the same job name.

The worker MUST fail closed if it encounters a claimed job whose name has no registered handler.

Implementations MAY additionally provide startup validation or operator tooling that checks for queued jobs with missing handlers, but correctness must not depend on a full startup scan.

### Narrow Interfaces

Application code SHOULD depend on the smallest interface needed.

Proposed interfaces:

```go
type Enqueuer interface {
	Enqueue(ctx context.Context, def any, input any, opts ...EnqueueOption) (Ref, error)
	EnqueueTx(ctx context.Context, tx pgx.Tx, def any, input any, opts ...EnqueueOption) (Ref, error)
}

type Reader interface {
	Inspect(ctx context.Context, id string, opts ...InspectOption) (*RawSnapshot, error)
}

type TypedReader interface {
	Lookup(ctx context.Context, def any, id string, opts ...InspectOption) (any, error)
}

type Controller interface {
	Cancel(ctx context.Context, id string, opts ...ControlOption) error
	Retry(ctx context.Context, id string, opts ...RetryOption) (Ref, error)
	PauseQueue(ctx context.Context, queue string, reason string) error
	ResumeQueue(ctx context.Context, queue string) error
}
```

The exact generic interface signatures may vary in implementation, but the principle should hold:

- enqueue and read are separate
- admin control is separate
- queue management is separate

This reduces accidental privilege spread in application services.

### Retry Control Semantics

Admin retry MUST NOT mutate a historical failed job row back into `queued`.

Instead, `Retry(...)` MUST:

- load the original failed or canceled job
- create a new queued job row with a new `id`
- copy the original payload and safe metadata
- record lineage to the original job

This preserves:

- auditability
- accurate attempt history
- debuggability
- replay safety

Direct mutation of historical job payloads for replay SHOULD NOT be supported by default.

If operators need to “retry with changes”, the system should support a deliberate clone-and-override flow that still produces a new job row and preserves the original.

### Bulk Replay

Operationally, large replay events are normal.

Examples:

- an email provider outage creates thousands of failed jobs
- a bad deploy temporarily breaks one handler
- a third-party API returns transient errors for hours

The admin surface SHOULD support filtered bulk replay without requiring one-at-a-time operator loops.

Bulk replay SHOULD:

- select historical jobs by filters such as queue, job name, tenant, status, and time window
- clone them into new queued jobs in bounded batches
- preserve lineage to the original failed rows
- emit summary observability data for the replay operation

Bulk replay MUST NOT:

- mutate historical rows in place
- hold a single giant transaction for thousands of cloned jobs
- block ordinary enqueue/claim traffic for long periods

The CLI and admin APIs should expose this as an operator feature, but it does not need to be part of the narrow app-facing enqueue/read interfaces used by normal application services.

### References

Enqueue returns a lightweight durable reference:

```go
type Ref struct {
	ID   string
	Name string
}
```

Design rules:

- IDs SHOULD be opaque prefixed ULIDs such as `job_01HT...`
- the ref MUST be safe to persist in application tables
- the ref is not, by itself, authorization

### Snapshots

Job status inspection returns immutable snapshots.

Conceptual typed shape:

```go
type Snapshot[Out any] struct {
	Ref               Ref
	Queue             string
	Status            Status
	TenantID          string
	Actor             Actor
	Attempts          int
	MaxAttempts       int
	RunAt             time.Time
	CreatedAt         time.Time
	FirstStartedAt    *time.Time
	LastStartedAt     *time.Time
	FinishedAt        *time.Time
	CancelRequestedAt *time.Time
	Progress          *Progress
	Output            *Out
	SafeError         string
	ErrorCode         string
	ParentJobID       string
	RetriedFromJobID  string
}
```

`Status` is user-facing and intentionally small:

- `queued`
- `running`
- `succeeded`
- `failed`
- `canceled`

Retry delays and scheduled future runs are represented through:

- `Status == queued`
- `RunAt` in the future

This keeps the state model small while still making delayed retries and scheduled jobs observable.

### Job Context

Worker handlers receive a `jobs.Ctx`.

Conceptual capabilities:

```go
type Ctx interface {
	context.Context
	JobID() string
	JobName() string
	Queue() string
	Attempt() int
	MaxAttempts() int
	TenantID() string
	Actor() Actor
	Metadata() map[string]string
	IdempotencyKey() string
	Logger() *slog.Logger
	Progress(p Progress) error
	Enqueue(def any, input any, opts ...EnqueueOption) (Ref, error)
}
```

Important notes:

- `IdempotencyKey()` MUST be stable for the lifetime of the job across retries
- `Attempt()` MUST increment for each execution attempt
- `Progress(...)` MUST be throttled and bounded
- `Enqueue(...)` inside a handler MUST NOT imply workflow semantics; it only creates child lineage metadata when configured

### Progress Flush Semantics

`ctx.Progress(...)` MUST NOT translate directly into a database write on every call.

The worker-side `jobs.Ctx` implementation SHOULD:

- keep the most recent progress value in memory
- coalesce intermediate updates
- flush to durable storage at a bounded rate

Recommended default:

- at most one durable progress write per second per running job

This means a tight loop that calls `ctx.Progress(...)` thousands of times should produce:

- cheap in-memory updates inside the worker
- bounded database writes
- the latest available progress on the durable job row

### Actor and Tenant Metadata

Jobs may carry actor and tenant metadata for:

- authorization checks on status APIs
- audit trails
- safe admin filtering
- lookup of tenant-scoped secrets at execution time

Conceptual actor type:

```go
type Actor struct {
	ID   string
	Kind string
}
```

Rules:

- actor and tenant metadata are descriptive context, not live auth
- jobs MUST NOT carry cookies, bearer tokens, refresh tokens, or session IDs as normal metadata

### Enqueue Options

Canonical enqueue options should include:

- `WithTenant(id string)`
- `WithActor(actor Actor)`
- `WithMetadata(map[string]string)` for safe low-cardinality metadata
- `WithUnique(key string, policy UniquePolicy)`
- `WithConcurrencyKey(key string)` for per-key execution serialization
- `WithDelay(d time.Duration)`
- `WithRunAt(t time.Time)`
- `WithPriority(n int)`
- `WithParent(parent Ref)`
- `WithTags(tags ...string)`

The initial option surface should remain small and orthogonal.

### Unique Keys

Unique keys solve deduplication.

Default v1 semantics:

- uniqueness is scoped to `(job_name, unique_key)`
- uniqueness applies to active jobs only: `queued` and `running`
- default policy returns the existing ref instead of inserting a duplicate

Candidate policies:

- `ReturnExisting`
- `RejectDuplicate`

More complex replace semantics should wait unless clearly needed.

### Concurrency Keys

Concurrency keys solve serialization, not deduplication.

`WithConcurrencyKey("org:123")` means:

- multiple jobs may be queued with that key
- at most one job with that key may execute at a time

For v1:

- concurrency-key execution limit is `1`
- broader semaphore or weighted limits are out of scope

Even though the initial public API only exposes limit `1`, the runtime and storage design MUST NOT paint the system into a corner.

Internal concurrency state should be modeled as leaseable slots rather than as a forever-singleton lock so that future semaphore-style limits can be added without changing the meaning of `concurrency_key`.

### Result Payloads

Typed job output is valuable, but it MUST remain small.

Rules:

- results SHOULD be small JSON values
- large artifacts MUST be stored elsewhere and referenced by key
- jobs that do not need output should use `struct{}`

## Error Model

The runtime needs more than plain “error vs nil”.

### Default Rule

- ordinary `error` is retryable until attempts are exhausted
- exhausted jobs become terminal `failed`

### Explicit Wrappers

The runtime SHOULD expose wrappers like:

- `jobs.Permanent(err)` -> terminal failure, do not retry
- `jobs.RetryAfter(err, d)` -> retryable failure with explicit delay override
- `jobs.Code(err, "mail_rate_limited")` -> stable machine-readable failure code
- `jobs.Safe(err, "email send failed")` -> safe stored/admin-visible message

This lets applications express intent without inventing custom error contracts.

### Safe Failure Storage

Persisted job records MUST store only:

- safe error message
- stable error code

They MUST NOT persist:

- full wrapped error chains
- secrets
- raw HTTP responses
- signed URLs
- access tokens

Full debug details belong in structured logs and traces, not durable job rows.

### Size-Limit Errors

The runtime SHOULD expose typed safe errors for budget violations, including:

- `ErrPayloadTooLarge`
- `ErrMetadataTooLarge`
- `ErrOutputTooLarge`
- `ErrProgressTooLarge`

These errors SHOULD provide structured access to:

- the observed size
- the configured limit
- the violated budget class

The default safe error strings should be explicit and helpful rather than generic.

## Retry Model

Retries are core, not optional sugar.

### Default Behavior

Each definition configures:

- `MaxAttempts`
- `Backoff`
- `Timeout`

Default backoff SHOULD be exponential with jitter.

### Retry Decision Matrix

| Handler outcome | Runtime behavior |
|---|---|
| `nil` | mark succeeded |
| ordinary error + attempts remain | set `queued`, compute next `run_at` |
| ordinary error + exhausted | mark failed |
| `jobs.Permanent(err)` | mark failed immediately |
| `jobs.RetryAfter(err, d)` | set `queued` with explicit delay |
| context canceled due to worker shutdown | do not acknowledge; lease expiry handles replay |
| context canceled due to user/admin cancellation | mark canceled |

### Timeouts

Timeouts are per-attempt.

When timeout is hit:

- handler context is canceled
- if the worker cannot safely acknowledge cancellation, the lease is allowed to expire
- the attempt is treated as retryable unless exhausted

## Cancellation Semantics

Jobs must be cancelable in a predictable way.

### Queued Cancellation

If a queued job is canceled:

- it transitions directly to `canceled`
- it will never be executed

### Running Cancellation

If a running job is canceled:

- `cancel_requested_at` is set
- the worker learns this during heartbeat/lease renewal
- the worker cancels the handler context
- if the handler returns promptly, the job becomes `canceled`
- if the worker dies or the handler ignores context, the reaper marks expired canceled jobs as `canceled`, not retried

This preserves admin intent.

## Parent/Child Lineage

`vango-jobs` SHOULD support simple lineage metadata in v1:

- a job may record `parent_job_id`
- child jobs are ordinary jobs
- parent success/failure does not automatically depend on child outcome

This is intentionally weaker than workflow orchestration.

It enables:

- audit trails
- fan-out visibility
- coarse job trees in admin UI

It does not imply:

- step orchestration
- compensation
- wait-for-all barriers
- DAG scheduling

## Runtime Architecture

The runtime has five main parts:

1. durable store
2. registry of handlers
3. worker loop
4. queue controller/admin surface
5. optional Vango adapter for status observation

### Worker Topology

Production SHOULD run workers in a separate process from the web server.

Development MAY run an embedded worker for convenience.

Reasons to prefer a separate worker in production:

- clearer failure isolation
- easier horizontal scaling
- cleaner resource tuning
- simpler operational control

### Worker Concurrency

Each worker process has:

- global concurrency
- per-queue subscriptions
- optional per-queue concurrency overrides

The worker claim loop should:

- poll for claimable jobs
- use `LISTEN/NOTIFY` as a wakeup hint
- batch claim jobs
- execute jobs under a bounded worker pool

### Leases and Heartbeats

Leases are mandatory.

When a worker claims a job:

- it generates a `lease_token`
- marks the job `running`
- sets `lease_expires_at`
- records the worker identity

While executing:

- the worker heartbeats periodically
- the heartbeat extends both job lease and concurrency-key lease if present

If the worker dies:

- heartbeat stops
- the lease expires
- the reaper can safely recover the job

### Database Time Is Authoritative

All lease computations and lease-expiry comparisons MUST use PostgreSQL’s clock, not the worker process clock.

Examples:

- claim-time lease creation
- heartbeat lease extension
- reaper expiry checks
- retry `run_at` transitions driven by infrastructure failures

This avoids correctness bugs caused by worker/database clock skew.

Implementations SHOULD prefer SQL expressions such as `CURRENT_TIMESTAMP` for lease math rather than computing expiry timestamps from Go `time.Now()`.

### Reaper

A reaper process or loop is required.

The reaper handles:

- expired running jobs with dead leases
- expired concurrency-key leases
- stale canceled-running jobs

For expired jobs:

- if `cancel_requested_at` is set, mark `canceled`
- else if attempts exhausted, mark `failed`
- else requeue with backoff as an abandoned attempt

### Queue Pause/Resume

Queues are first-class admin objects.

Pausing a queue means:

- new jobs may still be enqueued
- workers will not claim jobs from that queue

This is needed for:

- incident response
- maintenance
- controlled rollout

## PostgreSQL Storage Design

PostgreSQL is the canonical v1 runtime backend.

Reasons:

- transactional enqueue is straightforward
- it matches the broader Vango ecosystem posture around Postgres/Neon
- `FOR UPDATE SKIP LOCKED` is sufficient for a durable queue
- `LISTEN/NOTIFY` provides low-latency wakeups
- most Vango apps already depend on Postgres

### Core Tables

#### `vango_jobs`

This is the canonical durable state table.

Conceptual columns:

```sql
CREATE TABLE vango_jobs (
    id                  text PRIMARY KEY,
    name                text NOT NULL,
    queue               text NOT NULL,
    status              text NOT NULL,

    tenant_id           text NULL,
    actor_id            text NULL,
    actor_kind          text NULL,

    parent_job_id       text NULL REFERENCES vango_jobs(id),
    retried_from_job_id text NULL REFERENCES vango_jobs(id),
    unique_key          text NULL,
    concurrency_key     text NULL,
    trace_id            text NULL,

    priority            integer NOT NULL DEFAULT 100,
    attempts            integer NOT NULL DEFAULT 0,
    max_attempts        integer NOT NULL,
    timeout_seconds     integer NOT NULL,

    run_at              timestamptz NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    first_started_at    timestamptz NULL,
    last_started_at     timestamptz NULL,
    finished_at         timestamptz NULL,
    cancel_requested_at timestamptz NULL,

    lease_token         uuid NULL,
    leased_by           text NULL,
    lease_expires_at    timestamptz NULL,

    payload             jsonb NOT NULL,
    output              jsonb NULL,
    progress            jsonb NULL,
    metadata            jsonb NULL,
    tags                text[] NULL,

    safe_error          text NULL,
    error_code          text NULL,
    retain_until        timestamptz NULL
);
```

Rules:

- `status` is always one of the canonical statuses
- `payload` is immutable after enqueue
- `output`, `progress`, and failure fields are mutable
- `retain_until` drives pruning

#### `vango_job_attempts`

Each execution attempt gets its own history row.

```sql
CREATE TABLE vango_job_attempts (
    id               text PRIMARY KEY,
    job_id           text NOT NULL REFERENCES vango_jobs(id) ON DELETE CASCADE,
    attempt          integer NOT NULL,
    worker_id        text NOT NULL,
    lease_token      uuid NOT NULL,
    status           text NOT NULL,
    started_at       timestamptz NOT NULL,
    ended_at         timestamptz NULL,
    duration_ms      bigint NULL,
    safe_error       text NULL,
    error_code       text NULL,
    retry_delay_ms   bigint NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE(job_id, attempt)
);
```

This table exists for:

- debugging
- admin UI
- metrics backfill
- postmortem analysis

#### `vango_job_events`

This table stores bounded, safe lifecycle events.

```sql
CREATE TABLE vango_job_events (
    id          bigserial PRIMARY KEY,
    job_id       text NOT NULL REFERENCES vango_jobs(id) ON DELETE CASCADE,
    kind         text NOT NULL,
    at           timestamptz NOT NULL DEFAULT now(),
    payload      jsonb NULL
);
```

Event kinds SHOULD include:

- `enqueued`
- `started`
- `progress`
- `retried`
- `succeeded`
- `failed`
- `canceled`

Progress event volume MUST be throttled.

#### `vango_job_queues`

```sql
CREATE TABLE vango_job_queues (
    queue         text PRIMARY KEY,
    paused        boolean NOT NULL DEFAULT false,
    pause_reason  text NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);
```

#### `vango_job_concurrency`

This table serializes execution for `concurrency_key`.

The shape is intentionally slot-based so future semaphore-style limits can be added without replacing the lease model.

In v1:

- only slot `1` is used
- public API behavior remains “at most one running job per key”

```sql
CREATE TABLE vango_job_concurrency (
    key              text NOT NULL,
    slot             integer NOT NULL,
    job_id           text NOT NULL REFERENCES vango_jobs(id) ON DELETE CASCADE,
    lease_token      uuid NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (key, slot),
    UNIQUE (job_id)
);
```

For a future semaphore-style limit of `N`, the runtime can allocate slots `1..N` for the same key and claim any free or expired slot.

### Indexes

The following indexes are mandatory:

```sql
CREATE INDEX vango_jobs_claim_idx
    ON vango_jobs (queue, status, run_at, priority, created_at)
    WHERE status = 'queued';

CREATE INDEX vango_jobs_running_lease_idx
    ON vango_jobs (status, lease_expires_at)
    WHERE status = 'running';

CREATE INDEX vango_jobs_tenant_idx
    ON vango_jobs (tenant_id, created_at DESC);

CREATE INDEX vango_jobs_parent_idx
    ON vango_jobs (parent_job_id);

CREATE INDEX vango_jobs_retried_from_idx
    ON vango_jobs (retried_from_job_id);

CREATE UNIQUE INDEX vango_jobs_unique_active_idx
    ON vango_jobs (name, unique_key)
    WHERE unique_key IS NOT NULL AND status IN ('queued', 'running');

CREATE INDEX vango_job_attempts_job_idx
    ON vango_job_attempts (job_id, attempt DESC);

CREATE INDEX vango_job_events_job_idx
    ON vango_job_events (job_id, id DESC);

CREATE INDEX vango_job_concurrency_lease_idx
    ON vango_job_concurrency (lease_expires_at);
```

### Claim Algorithm

Claiming should use a transaction roughly shaped like:

1. select claimable queued jobs for the configured queue set
2. lock them with `FOR UPDATE SKIP LOCKED`
3. reject jobs from paused queues
4. for each job with `concurrency_key`, try to claim a concurrency slot
5. transition claimed rows to `running`
6. write attempt rows
7. commit

Claimability rules:

- `status = 'queued'`
- `run_at <= now()`
- queue is not paused

Sort order SHOULD be:

- higher priority first
- older jobs first within the same priority

For v1 concurrency-key claims:

- the runtime attempts to claim slot `1`
- if slot `1` is leased and not expired, the job remains queued
- if slot `1` is missing or expired, the runtime claims or replaces it transactionally

### Why `LISTEN/NOTIFY` Is Only a Hint

Workers SHOULD use `NOTIFY` on enqueue and queue resume, but correctness MUST NOT depend on it.

Reason:

- notifications can be lost
- listeners can disconnect
- workers may start after the notification was emitted

The worker loop MUST continue periodic polling even when notifications are enabled.

### Retry/Requeue Update

On retryable failure:

- clear lease fields
- clear concurrency lease if held
- set `status = 'queued'`
- set `run_at = computed_retry_time`
- update `safe_error` and `error_code`
- leave `output = NULL`

On terminal failure:

- set `status = 'failed'`
- set `finished_at = now()`

On success:

- set `status = 'succeeded'`
- set `output`
- set `finished_at = now()`

### Reaper Behavior

The reaper MUST identify:

- rows where `status = 'running'`
- `lease_expires_at < now()`

For each:

- mark the attempt as `abandoned`
- release any concurrency row owned by the same lease token
- if `cancel_requested_at IS NOT NULL`, mark `canceled`
- else requeue or fail depending on remaining attempts

## Transaction-Aware Enqueue

`EnqueueTx(...)` is a required first-class feature.

This is critical because many real app mutations are dual-write sensitive:

- create order row
- enqueue receipt email

Without transactional enqueue, applications face a classic bug:

- DB write succeeds
- job enqueue fails
- user-visible mutation exists without its side effect

### Requirements

- `EnqueueTx(...)` MUST write into the same PostgreSQL transaction as the application mutation
- `EnqueueTx(...)` MUST be available against `pgx.Tx`
- if the transaction rolls back, the job MUST NOT exist
- if the transaction commits, the job MUST exist durably

The Vango ecosystem should lean into this rather than hide it.

## Scheduling

Scheduling is important, but it should be layered on top of the durable queue, not replace it.

### v1 Posture

The core queue runtime is the first priority.

Schedule support MAY ship in the initial release if it does not distort the core queue model. Otherwise it should land immediately after the core worker/runtime is stable.

Minimal durable schedules should arrive early enough that teams do not build ad hoc cron side systems outside the blessed jobs runtime.

### Schedule Model

Schedules should be:

- durable
- registered explicitly at startup
- versioned by name
- able to emit ordinary jobs

Conceptual schedule shape:

```go
var DailyCleanup = jobs.NewSchedule(
	"cleanup.daily.v1",
	CleanupJob,
	jobs.Cron("0 3 * * *"),
)
```

The scheduler should:

- compute next run
- enqueue a normal job using a schedule-derived uniqueness key
- persist next-run state durably

The schedule system MUST NOT invent a second execution model.

### Suggested Schedule Storage

If durable schedules are implemented, the canonical runtime table should be conceptually shaped like:

```sql
CREATE TABLE vango_job_schedules (
    name             text PRIMARY KEY,
    target_job_name  text NOT NULL,
    spec             text NOT NULL,
    timezone         text NOT NULL DEFAULT 'UTC',
    paused           boolean NOT NULL DEFAULT false,
    next_run_at      timestamptz NOT NULL,
    last_run_at      timestamptz NULL,
    last_job_id      text NULL REFERENCES vango_jobs(id),
    updated_at       timestamptz NOT NULL DEFAULT now()
);
```

The scheduler loop should:

1. claim due schedule rows
2. enqueue the target job transactionally with a schedule-derived uniqueness key
3. compute and persist the next run
4. emit schedule observability events

## Vango Integration

The Vango integration layer should be intentionally thin.

### Canonical Application Pattern

Enqueue in an Action:

```go
startExport := setup.Action(&s,
	func(ctx context.Context, input ExportInput) (jobs.Ref, error) {
		return deps.Jobs.Enqueue(ctx, GenerateExport, GenerateExportInput{
			ExportID: input.ExportID,
		},
			jobs.WithTenant(input.OrgID),
			jobs.WithActor(jobs.Actor{ID: input.UserID, Kind: "user"}),
			jobs.WithUnique("export:"+input.ExportID),
		)
	},
	vango.DropWhileRunning(),
)
```

Observe status with a Resource:

```go
status := setup.ResourceKeyed(&s,
	func() string { return exportJobID.Get() },
	func(ctx context.Context, id string) (*jobs.Snapshot[GenerateExportResult], error) {
		return deps.Jobs.Lookup(ctx, GenerateExport, id, jobs.InspectTenant(orgID))
	},
)
```

This pattern is already fully aligned with Vango’s boundaries and does not require new session-loop rules.

### Optional Vango Helper Package

The `github.com/vango-go/vango-jobs/vango` package may provide helpers such as:

- `jobsvango.StatusKeyed(...)`
- `jobsvango.Status(...)`
- `jobsvango.WithLiveUpdates(...)`

These helpers should be sugar over `setup.Resource` plus invalidation.

They MUST NOT introduce:

- hidden goroutine patterns in user code
- a second async model
- direct signal mutation from worker threads

### Live Status Updates

If a broadcast backend exists, `vango-jobs` SHOULD be able to publish safe job-update notifications.

The Vango helper MAY use these notifications to invalidate status resources automatically.

Fallback behavior without broadcast support should be:

- polling with backoff
- explicit manual refresh

### Vango Guide Contract

The Vango Developer Guide should add a section that says:

- Use `setup.Action` to enqueue durable work
- Use `setup.Resource` to inspect durable work
- Never block the session loop waiting for a job
- Do not treat jobs as “slow Actions”

## Security Model

### Server-Authoritative Enqueue

The browser MUST NOT choose:

- arbitrary job names
- arbitrary queue names
- arbitrary actor/tenant bindings

Clients may request business actions.

Server code decides:

- whether a job should be created
- which definition to use
- what payload to build
- what tenant or actor metadata applies

### Payload Hygiene

Job payloads SHOULD contain:

- IDs
- durable references
- safe low-cardinality metadata

Job payloads SHOULD NOT contain:

- access tokens
- refresh tokens
- cookie values
- signed URLs
- raw third-party secrets

If a handler needs credentials:

- it should use `tenant_id` or other durable references to resolve them at execution time

### Authorization on Reads and Control

Job IDs are not authorization.

Reader and controller operations MUST support authorization scopes such as:

- tenant scope
- actor scope
- admin scope

Examples:

- a user may inspect their own export job
- an org admin may inspect org jobs
- only privileged operators may pause a queue or retry arbitrary failed jobs

### Safe Logging

Logs SHOULD include:

- job id
- job name
- queue
- worker id
- attempt
- duration
- safe error code

Logs MUST NOT include payload or output bodies by default.

### Optional Encryption

If needed later, `vango-jobs` MAY expose a pluggable payload codec for compression or encryption-at-rest.

This is not a substitute for good payload hygiene.

The default guidance remains:

- do not store secrets in jobs

## Observability

Observability is a v1 requirement, not a later enhancement.

### Structured Logs

The runtime SHOULD emit structured lifecycle logs:

- `job_enqueued`
- `job_started`
- `job_progress`
- `job_retried`
- `job_succeeded`
- `job_failed`
- `job_canceled`
- `job_abandoned`

### Metrics

Canonical metrics SHOULD include:

- `vango_jobs_enqueued_total{job,queue}`
- `vango_jobs_started_total{job,queue}`
- `vango_jobs_succeeded_total{job,queue}`
- `vango_jobs_failed_total{job,queue,code}`
- `vango_jobs_retried_total{job,queue,code}`
- `vango_jobs_canceled_total{job,queue}`
- `vango_jobs_inflight{queue}`
- `vango_jobs_queue_depth{queue}`
- `vango_jobs_schedule_lag_seconds{schedule}`
- `vango_jobs_run_duration_seconds{job,queue}`
- `vango_jobs_end_to_end_latency_seconds{job,queue}`
- `vango_jobs_lease_renewal_failures_total{queue}`

Cardinality guidance:

- `job` and `queue` are safe labels
- `tenant_id` SHOULD NOT be a default metric label
- error `code` should remain bounded

### Tracing

Enqueue SHOULD create spans or span events.

Workers SHOULD:

- continue or link to originating traces when possible
- record enqueue-to-start latency
- record attempt duration

`trace_id` may be stored on the durable row as a safe correlation field.

## CLI and Tooling

Vango CLI support is important for adoption.

### Scaffolding

The Vango CLI SHOULD support:

```bash
vango create myapp --with jobs
```

This should scaffold:

- `cmd/worker/main.go`
- `internal/jobs/defs.go`
- `internal/jobs/register.go`
- SQL migrations for the jobs tables
- optional example admin route or dashboard wiring

### Runtime Commands

Canonical commands SHOULD include:

```bash
vango jobs run
vango jobs inspect <job-id>
vango jobs retry <job-id>
vango jobs retry --queue=emails --status=failed --since=24h
vango jobs cancel <job-id>
vango jobs pause <queue>
vango jobs resume <queue>
vango jobs prune
```

These commands should reuse the same application bootstrap and registry wiring as the main server.

### Dev Experience

`vango dev` SHOULD optionally run an embedded worker and show:

- queue depths
- active jobs
- recent failures

This is especially useful for coding agents and tight local feedback loops.

## Testing

Testing support needs to be first-class.

### Deterministic Test Kit

`jobstest` SHOULD provide:

- in-memory durable queue semantics
- explicit advancement of scheduled time
- immediate execution mode
- deterministic IDs and clocks
- helpers to inspect queued/running/failed jobs

### What Should Be Easy to Test

Application tests should be able to:

- assert that a job was enqueued with the right payload
- run queued jobs immediately
- simulate retries
- simulate permanent failures
- simulate cancellation
- inspect final outputs

### Integration Tests

The PostgreSQL runtime SHOULD have high-confidence integration tests for:

- claim concurrency
- lease expiry and recovery
- unique-key dedupe
- transaction enqueue
- queue pause/resume
- cancellation
- reaper correctness

## Operational Guidance

### Worker Shutdown

Graceful shutdown should:

- stop claiming new jobs
- cancel active handler contexts
- wait up to a configured drain timeout
- leave unacknowledged work to lease expiry recovery

The handler `jobs.Ctx` MUST be derived from the worker process root context so OS-level shutdown signals such as `SIGTERM` immediately trigger `ctx.Done()`.

This gives well-behaved handlers a chance to stop promptly, release local resources, and return cleanly before lease-expiry recovery takes over.

### Payload and Output Budgets

The runtime MUST enforce size limits.

Recommended defaults:

- payload max: 64 KiB
- output max: 64 KiB
- progress max: 4 KiB
- metadata max: 4 KiB

These should be configurable but conservative by default.

### Budget Violation Behavior

Budget violations MUST fail in explicit and helpful ways.

#### Enqueue-Time Violations

If payload or metadata exceeds the configured limit:

- enqueue MUST fail before any row is inserted
- the returned error SHOULD be a typed safe error such as `ErrPayloadTooLarge` or `ErrMetadataTooLarge`
- the error SHOULD expose the observed size and configured limit programmatically
- the safe error message SHOULD point developers toward storing references instead of embedding large blobs

#### Handler Output Violations

If a handler returns output larger than the configured limit:

- the runtime MUST NOT silently truncate it
- the attempt SHOULD fail with `ErrOutputTooLarge`
- that failure SHOULD be treated as permanent by default because retrying the same oversized output is not expected to succeed
- the stored safe error SHOULD make clear that large artifacts must be written out-of-band and referenced from the job result

#### Progress Violations

If `ctx.Progress(...)` exceeds the configured limit:

- the progress update MUST be rejected
- the call SHOULD return `ErrProgressTooLarge`
- the runtime SHOULD leave the last accepted progress value unchanged
- the job MAY continue running if the handler chooses to handle the error

### Retention and Pruning

Completed jobs must not accumulate forever.

Default retention SHOULD be:

- succeeded: 7 days
- failed: 30 days
- canceled: 7 days

The runtime SHOULD ship a prune command or background task.

### Idempotency Guidance

The documentation MUST strongly instruct developers to:

- use `ctx.IdempotencyKey()` when calling downstream idempotent APIs
- guard external writes with application-level uniqueness where necessary
- treat retries as normal, not exceptional

## Recommended Project Layout

Suggested app structure:

```text
myapp/
├── cmd/
│   ├── server/
│   │   └── main.go
│   └── worker/
│       └── main.go
├── internal/
│   ├── jobs/
│   │   ├── defs.go
│   │   ├── register.go
│   │   ├── email.go
│   │   └── exports.go
│   ├── services/
│   └── db/
└── app/
    └── routes/
```

This layout is friendly to:

- explicit registry wiring
- shared dependencies
- isolated worker main package
- coding-agent navigation

## Rollout Plan

### Phase 1: Core Durable Queue

Phase 1 MUST include:

- typed job definitions
- explicit registry
- PostgreSQL runtime
- worker loop
- leases and heartbeats
- retry/backoff/timeouts
- transaction enqueue
- unique keys
- queue pause/resume
- safe snapshots
- cancellation
- logs, metrics, traces
- deterministic test kit

### Phase 2: Vango Ergonomics, Admin Surface, and Schedules

Phase 2 SHOULD include:

- Vango helper package for status resources
- CLI inspect/retry/cancel/pause/resume commands
- devtools jobs panel
- optional broadcast-driven live invalidation
- durable schedules
- scheduler runner
- schedule admin controls

### Phase 3: Advanced Execution Features

Only after the earlier phases are solid should Vango consider:

- richer child-job helpers
- batch helpers
- semaphore-style and weighted concurrency controls
- alternative backends

## Deliberate Design Constraints

The following constraints are intentional and should not be relaxed casually:

1. Explicit names over inferred names
2. Explicit registration over reflection scanning
3. PostgreSQL-first over premature backend abstraction
4. Safe stored errors over raw error chains
5. At-least-once honesty over exactly-once marketing
6. Small payloads and outputs over job-as-document-store
7. Thin Vango integration over framework entanglement

## Summary

`vango-jobs` should be a first-party, typed, PostgreSQL-first, operationally boring durable job system that fits naturally beside `setup.Action` and `setup.Resource`.

It should let Vango applications say:

- do this mutation now in an Action
- enqueue this durable work transactionally
- observe it later through a Resource

without forcing the root Vango framework to absorb worker infrastructure concerns.

If implemented with these constraints, `vango-jobs` becomes:

- easy to reason about
- easy to test
- easy to scaffold
- safe to operate
- friendly to coding agents
- aligned with Vango’s broader philosophy of explicit structured correctness
