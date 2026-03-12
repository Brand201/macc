# Coordinator Realtime Orchestrator Design (Short)

Status: Implemented (v1 event contract) + Iterating

Scope: make `macc coordinator` a realtime, event-driven orchestrator with strict state transitions, durable recovery, and actionable live UX.

## 1) Goals

- Make task/runtime state unambiguous (no "it already finished?" confusion).
- Replace polling-heavy progression with event-driven progression.
- Expose performer progress/results in near-realtime in TUI.
- Support crash-safe resume with durable cursor + idempotent event handling.
- Keep tool adapters and CLI/TUI tool-agnostic.

Non-goals:

- Replace git/worktree model.
- Introduce external message broker as hard dependency.

## 2) Foundation: strict state model

Use two state dimensions per task:

1) Workflow state (`task.state`):
- `todo`, `claimed`, `in_progress`, `pr_open`, `changes_requested`, `queued`, `merged`, `blocked`, `abandoned`

2) Runtime state (`task.task_runtime.status`):
- `idle`, `dispatched`, `running`, `phase_done`, `failed`, `stale`

Runtime details (`task.task_runtime`):
- `session_id`, `pid`, `started_at`, `last_heartbeat`, `current_phase`, `last_error`, `attempt`

Key rule:
- `in_progress` means business work exists and is tracked.
- `dispatched/running/phase_done/failed` describe process execution lifecycle only.

## 3) Event contract (versioned)

File transport (initial): append-only JSONL at `.macc/log/coordinator/events.jsonl`.
Formal schema (versioned): `docs/schemas/coordinator-event.v1.schema.json`.

Envelope fields:

- `schema_version` (string, required)
- `event_id` (string UUID, required, unique)
- `seq` (integer, required, monotonic per task)
- `ts` (RFC3339 UTC, required)
- `source` (`coordinator` | `performer:<tool>`, required)
- `task_id` (string, required for task events)
- `type` (required): `lifecycle`, `phase`, `progress`, `artifact`, `error`, `heartbeat`
- `phase` (optional): `dev`, `review`, `integrate`, `merge`
- `status` (required): `started`, `running`, `done`, `failed`, `paused`
- `payload` (object, optional)

Minimal JSON Schema excerpt:

```json
{
  "type": "object",
  "required": ["schema_version", "event_id", "seq", "ts", "source", "type", "status"],
  "properties": {
    "schema_version": {"type": "string"},
    "event_id": {"type": "string"},
    "seq": {"type": "integer", "minimum": 0},
    "ts": {"type": "string"},
    "source": {"type": "string"},
    "task_id": {"type": "string"},
    "type": {"type": "string"},
    "phase": {"type": "string"},
    "status": {"type": "string"},
    "payload": {"type": "object"}
  }
}
```

## 4) Event-driven coordinator loops

Split coordinator into logical loops:

1) Scheduler loop:
- picks `todo` tasks eligible for dispatch.

2) Event consumer loop:
- tails JSONL, applies transitions immediately.
- persists cursor in `.macc/state/coordinator.cursor`:
  - `path`, `inode`, `offset`, `last_event_id`, `updated_at`

Current implementation note: heartbeat events are consumed in the native control-plane to update `task_runtime.last_heartbeat` using an in-memory cursor (not yet persisted).

3) Runtime monitor loop:
- heartbeat timeout detection (`running` with stale heartbeat -> `stale` action).

Concurrency model:
- one lock for registry write, no long global lock around process waits.
- event handlers must be idempotent.

## 5) Failure handling and user gating

Blocking failures (example: git merge error) trigger pause mode:

- coordinator sets `paused=true` in runtime state.
- TUI shows red blocking panel with:
  - cause
  - failed step
  - suggested fix command(s)
  - choices: `Retry step`, `Skip`, `Stop`, `Open logs`

Resume behavior:
- `Retry step` reruns the same failed phase only.
- If failure persists, same gate is shown again.

## 6) TUI live behavior target

Coordinator Live becomes event-first:

- Timeline per task: `dispatch -> running -> review -> integrate -> merge`
- Instant fields:
  - `current_phase`
  - `% progress` (if emitted)
  - `last_event_age`
  - `last_error`
  - `next_action`
- Existing spinners remain visual hint only; source of truth is event stream.

## 7) Reliability guardrails

- Event dedup by `event_id`.
- Ordering check by `seq` per task (late/out-of-order events ignored or quarantined).
- Backpressure:
  - periodic rotation of events JSONL (`EVENT_LOG_MAX_BYTES`, `EVENT_LOG_KEEP_FILES`)
  - compaction of `processed_event_ids` (`PROCESSED_EVENT_IDS_MAX`)
- Crash safety:
  - cursor persisted after successful apply
  - replay-safe transition handlers

## 8) Rollout plan (PR sequence)

PR-1: State model groundwork
- Add `task_runtime` structure.
- Add strict transition table tests in core.

PR-2: Event schema + producer library
- Shared event model + validation.
- Performer emits `started/progress/phase_result/failed/heartbeat`.

PR-3: Cursor + consumer
- Implement durable cursor + idempotent apply.
- Add replay/out-of-order tests.

PR-4: Coordinator loop split
- Separate scheduler vs consumer vs monitor.
- Remove long wait paths that hide realtime updates.

PR-5: TUI live event view
- Timeline, phase status, error panel with Retry/Skip/Stop.

PR-6: Pause/resume flow
- Blocking failure gate + targeted retry of failed phase.

PR-7: Cleanup and compatibility
- Deprecate legacy polling-only paths.
- Migration notes for old registry files.

PR-8: Docs + runbook finalization
- Update `README.md`, `MACC.md`, and operational examples.

## 9) Compatibility and migration notes

- Keep `task.state` backward-compatible during migration.
- `task_runtime` is additive first, mandatory later.
- Legacy events without `schema_version` are parsed as `v0` best-effort.
