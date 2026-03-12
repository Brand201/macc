# Coordinator Cutover Checklist

This checklist defines a safe migration path from shell-driven coordinator logic to a Rust control-plane with SQLite as source of truth.

## PR Order

### PR1 - Contracts & State Machine
- [ ] Lock event contract in `docs/schemas/coordinator-event.v1.schema.json`.
- [ ] Validate event schema in CI.
- [ ] Keep workflow/runtime transitions authoritative in `core/src/coordinator.rs`.
- [ ] Ensure shell wrappers only call Rust transition validation.
- [ ] Add/keep tests:
  - [ ] valid transition table coverage
  - [ ] invalid transition rejection
  - [ ] replay idempotence baseline

### PR2 - Storage Layer Ready
- [x] Implement/verify `Storage` trait with:
  - [x] `JsonStorage`
  - [x] `SqliteStorage`
- [x] Ensure SQLite schema exists for:
  - [x] `tasks`
  - [x] `task_runtime`
  - [x] `resource_locks`
  - [x] `events`
  - [x] `cursors`
  - [x] `jobs`
- [x] Keep JSON files mirrored for CLI/TUI compatibility.
- [x] Add/keep tests:
  - [x] JSON <-> SQLite equivalence
  - [x] cursor persistence
  - [x] crash/restart replay

### PR3 - Dual-Write in Runtime Path
- [x] Enable `COORDINATOR_STORAGE_MODE=dual-write` in coordinator runtime flow.
- [x] Add strict mismatch handling:
  - [x] fail hard
  - [ ] warning with explicit counter during rollout
- [x] Emit storage metrics/events:
  - [x] `storage_sync_ok`
  - [x] `storage_sync_failed`
  - [x] `storage_mismatch_count`
  - [x] `storage_sync_latency_ms`

### PR4 - Rust Control-Plane
- [x] Move scheduler + monitor + event-consumer loop to Rust.
- [x] Keep `automat/coordinator.sh` as compatibility wrapper only.
- [x] Add/keep integration tests:
  - [x] same input event stream => same final state
  - [x] parallel dispatch behavior
  - [x] retry-phase behavior

### PR5 - Runtime Correctness & Recovery
- [x] Persist and reconcile runtime job state from SQLite:
  - [x] performer PID/runtime
  - [x] heartbeat
  - [x] stale detection
  - [x] targeted retry
- [x] Add/keep failure-path tests:
  - [x] dead PID recovery
  - [x] stale heartbeat recovery
  - [x] blocked -> retry -> merged flow
  - [x] merge-worker single-flight
- [x] Emit/track:
  - [x] `task_phase_duration_seconds{phase}`
  - [x] `task_retries_total`
  - [x] `stale_runtime_total`
  - [x] `merge_fail_total`
  - [x] `merge_fix_attempt_total`

### PR6 - SQLite Cutover (Source of Truth)
- [x] Switch default mode to `sqlite`.
- [x] Keep JSON mirror for one release window.
- [x] Enforce cutover gates before merge:
  - [x] `storage_mismatch_count == 0` across rollout window
  - [x] replay/idempotence tests green
  - [x] blocked/stale rates not regressed

### PR7 - Shell Coordinator Decommission
- [x] Remove coordinator logic from `automat/coordinator/*.sh`.
- [x] Keep minimal compatibility launcher only if still required.
- [x] Update references in:
  - [x] `core/src/automation.rs`
  - [x] `core/src/lib.rs`
  - [x] `cli/src/main.rs`
  - [x] docs and tests
- [x] Keep `performer.sh` and adapter runners until their own migration completes.

## Mandatory Test Matrix

### Unit Tests
- [ ] workflow transition validity
- [ ] runtime transition validity
- [ ] runtime status mapping from event
- [ ] storage serialization/deserialization

### Integration Tests
- [ ] dispatch -> advance -> reconcile -> cleanup full cycle
- [ ] crash mid-run + restart from cursor
- [ ] duplicate/reordered event replay safety
- [ ] parallel dispatch with per-tool limits

### Failure Drills
- [ ] local merge failure with pause/retry/skip flow
- [ ] missing PID + stale heartbeat recovery
- [ ] lock contention and orphan cleanup

## Operational Metrics (Rollout Window)

- [ ] blocked ratio
- [ ] stale ratio
- [ ] merge success ratio
- [ ] p50/p95 phase durations (`dev`, `review`, `integrate`, `wait`)
- [ ] event throughput and consumer lag (`events/sec`, `last_event_age`)
- [ ] storage mismatch count
- [ ] cursor rollback/replay anomaly count

## Rollout Gates

### Gate A (post-PR3)
- [ ] dual-write stable for 3-5 days
- [ ] no unresolved storage mismatches

### Gate B (post-PR5)
- [ ] crash/restart and replay drills green
- [ ] no ghost-active runtime states (`running` without live PID unless policy allows)

### Gate C (pre-PR6)
- [ ] production-like runs meet SLOs
- [ ] blocked/stale/error rates not regressed

### Gate D (pre-PR7)
- [ ] SQLite-only runtime stable for one full release cycle
- [ ] CLI/TUI protocol behavior unchanged for operators

## Guardrails (Must Never Regress)

- [ ] Idempotent transitions.
- [ ] Replay-safe event handling.
- [ ] Backward-compatible CLI/TUI protocol during migration.
- [ ] Deterministic migration checks: same event stream => same final state.
