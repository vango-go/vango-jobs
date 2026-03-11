# vango-jobs Implementation Progress

## Status

- Start date: 2026-03-11
- Overall status: second-pass completion in progress
- Spec source: `vango-jobs/VANGO_JOBS_SPEC.md`

## Phases

### Phase 1: Foundations

- [completed] Validate repo/module conventions and package layout
- [completed] Create `go.mod` and package skeleton
- [completed] Define core public API types and contracts

### Phase 2: Core Runtime

- [completed] Implement typed definitions, registry, enqueue/read/control APIs
- [completed] Implement PostgreSQL storage schema and SQL runtime
- [completed] Implement worker, heartbeat, claim, retry, cancel, and reaper flows

### Phase 3: Ergonomics

- [completed] Implement Vango integration helpers
- [completed] Implement deterministic `jobstest`
- [completed] Add operator-facing helpers and migration assets

### Phase 4: Verification

- [completed] Add focused unit tests
- [completed] Add PostgreSQL runtime integration tests
- [completed] Run formatting and test suites
- [completed] Final docs pass and closeout

### Phase 5: Spec Completion Pass

- [completed] Close remaining module/spec gaps after implementation review
- [completed] Add missing admin and operator surfaces
- [completed] Complete broadcast-backed live updates and pruning support
- [completed] Complete observability coverage and repo-level CLI wiring
- [completed] Align root `vango` CLI/tests/docs with the shipped jobs module
- [completed] Re-run full verification against the expanded surface

## Work Log

### 2026-03-11

- Created implementation tracker.
- Confirmed `vango-jobs/` currently contains only the design spec.
- Reviewed `vango/DEVELOPER_GUIDE.md` to align jobs boundaries with Vango's existing `setup.Action` and `setup.Resource` model.
- Began inspecting module conventions and neighboring package patterns before creating the new module.
- Created the standalone `vango-jobs` module with a local `replace` back to `../vango`.
- Implemented the core public contracts: statuses, snapshots, actors, progress, errors, backoff, definitions, schedules, options, registry wiring, runtime configuration, and the initial runtime shell.
- Adjusted the registry/schedule API to match Go's actual generics limits while preserving explicit registration and typed definition metadata.
- Verified that the root package compiles with `go test ./...` before starting store and worker implementation.
- Built the PostgreSQL-backed store, worker, scheduler, deterministic in-memory runtime, `jobstest` harness, Vango helper package, and initial unit/integration coverage.
- Identified and began fixing PostgreSQL transaction cursor correctness issues in bulk replay, claiming, reaping, and schedule ticking after integration tests exposed `conn busy` failures.
- Refactored PostgreSQL replay/claim/reap/schedule transactions to fully consume and close row cursors before executing additional statements in the same transaction.
- Tightened PostgreSQL event payload SQL with explicit casts so job lifecycle events work reliably under real PostgreSQL type inference rules.
- Normalized remaining worker, scheduler, and PostgreSQL store time reads onto the configured runtime clock for deterministic behavior.
- Expanded verification to cover transactional enqueue commit/rollback semantics, queue pause/resume behavior, and deterministic reaper recovery for expired running jobs.
- Completed final verification with `go test -count=1 ./...` and `go vet ./...`.
- Reviewed the resulting implementation directly against `VANGO_JOBS_SPEC.md` and identified remaining gaps: queue-control authorization, cross-instance broadcast subscriptions, pruning, missing operator/admin accessors, and incomplete observability/CLI coverage.
- Added missing admin/operator runtime surfaces for attempts, events, queues, schedules, and pruning, and enforced admin-only queue/schedule/prune controls.
- Finished cross-instance broadcast-backed live updates, fixed shutdown safety for update subscriptions, and closed the deterministic progress flush clock gap.
- Added structured lifecycle logs, an internal metrics sink/collector, OTel span wiring, trace-id derivation on enqueue, schedule lag reporting, queue depth/inflight refreshes, and lease-renewal failure metrics.
- Expanded tests to cover history/admin surfaces, broadcast propagation, pruning, metrics collection, and trace-id propagation, then re-ran `go test ./...`.
- Began the repo-level completion pass in the root `vango` CLI: added `vango jobs`, jobs scaffolding in `vango create`, worker/migration templates, and focused tests for the new command paths.
- Identified that the new root CLI tests were using invalid temporary project configs and that the SQLite/create assertion was checking `VangoError.Error()` instead of the structured `Detail` field; correcting those before the next verification run.
- Added durable job listing/status APIs to `vango-jobs`, exposed `vango jobs status`, and extended the worker runner with periodic queue/failure status snapshots for local development.
- Integrated `vango dev --jobs` so local development can run an embedded jobs worker and stream status snapshots without adding direct framework coupling to the jobs runtime.
- Updated the main Vango developer guide and scaffolded jobs docs so the Action/Resource/jobs contract is documented in the canonical framework guide and generated app docs.
- Completed the expanded verification pass with `go test ./...` and `go vet ./...` in `vango-jobs`, plus `go test ./cmd/vango ./internal/templates` in the root `vango` repo.
