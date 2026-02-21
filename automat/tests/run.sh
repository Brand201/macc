#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT="${ROOT_DIR}/automat/coordinator_legacy.sh"
FIXTURES="${ROOT_DIR}/automat/tests/fixtures"
COORD_MODULE_DIR="${ROOT_DIR}/automat/legacy_coordinator"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing dependency: $1" >&2
    exit 1
  }
}

assert_eq() {
  local expected="$1"
  local actual="$2"
  local msg="$3"
  if [[ "$expected" != "$actual" ]]; then
    echo "Assertion failed: $msg (expected='$expected', actual='$actual')" >&2
    exit 1
  fi
}

setup_repo() {
  local dir
  dir="$(mktemp -d)"
  git -C "$dir" init -q
  echo "init" >"$dir/README.md"
  git -C "$dir" add README.md
  git -C "$dir" commit -q -m "init"
  echo "$dir"
}

make_stub_macc() {
  local dir="$1"
  local bin_dir="$dir/bin"
  mkdir -p "$bin_dir"
  cat >"$bin_dir/macc" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail

if [[ "$1" == "--cwd" ]]; then
  shift 2
fi

if [[ "$1" == "coordinator" ]]; then
  case "${2:-}" in
    validate-transition|validate-runtime-transition)
      # Keep integration tests independent from core transition-table changes.
      exit 0
      ;;
    runtime-status-from-event)
      # Keep event contract stable for shell wrappers.
      echo "running"
      exit 0
      ;;
    storage-sync)
      # Tests default to json mode; allow non-json mode paths to no-op here.
      exit 0
      ;;
  esac
fi

if [[ "$1" != "worktree" ]]; then
  echo "unsupported" >&2
  exit 1
fi

if [[ "${2:-}" == "run" ]]; then
  # Simulate successful performer execution.
  exit 0
fi

if [[ "${2:-}" == "apply" ]]; then
  # Simulate successful worktree apply.
  exit 0
fi

if [[ "${2:-}" != "create" ]]; then
  echo "unsupported" >&2
  exit 1
fi

slug="$3"
shift 3

tool="codex"
base="main"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tool) tool="$2"; shift 2 ;;
    --base) base="$2"; shift 2 ;;
    --count) shift 2 ;;
    --skip-apply) shift ;;
    *) shift ;;
  esac
done

root="$(pwd)"
suffix="$(date +%s)"
name="${slug}-${suffix}"
path="${root}/.macc/worktree/${name}"
branch="ai/task/${name}"

mkdir -p "$path"
git -C "$path" init -q
echo "worktree" >"$path/README.md"
git -C "$path" add README.md
git -C "$path" commit -q -m "init"

echo "Created 1 worktree(s):"
echo "  ${name}  branch=${branch} base=${base} path=${path}"
EOS
  chmod +x "$bin_dir/macc"
  echo "$bin_dir"
}

load_coordinator_event_modules() {
  : "${EVENT_SEQ_COUNTER:=0}"
  : "${COORD_COMMAND_NAME:=test}"
  : "${COORD_EVENT_SOURCE:=coordinator:test}"
  # shellcheck source=/dev/null
  source "${COORD_MODULE_DIR}/state.sh"
  # shellcheck source=/dev/null
  source "${COORD_MODULE_DIR}/events.sh"
}

load_coordinator_runtime_modules() {
  : "${EVENT_SEQ_COUNTER:=0}"
  : "${COORD_COMMAND_NAME:=test}"
  : "${COORD_EVENT_SOURCE:=coordinator:test}"
  : "${TIMEOUT_SECONDS:=5}"
  : "${STALE_HEARTBEAT_ACTION:=block}"
  : "${STALE_HEARTBEAT_SECONDS:=1800}"
  : "${STALE_ACTION:=blocked}"
  : "${STALE_CLAIMED_SECONDS:=0}"
  : "${STALE_IN_PROGRESS_SECONDS:=0}"
  : "${STALE_CHANGES_REQUESTED_SECONDS:=0}"
  : "${SLO_DEV_SECONDS:=0}"
  : "${SLO_REVIEW_SECONDS:=0}"
  : "${SLO_INTEGRATE_SECONDS:=0}"
  : "${SLO_WAIT_SECONDS:=0}"
  : "${SLO_RETRIES_MAX:=0}"
  : "${COORDINATOR_TOOL:=codex}"
  # shellcheck source=/dev/null
  source "${COORD_MODULE_DIR}/runtime.sh"
  # shellcheck source=/dev/null
  source "${COORD_MODULE_DIR}/state.sh"
  # shellcheck source=/dev/null
  source "${COORD_MODULE_DIR}/events.sh"
  # shellcheck source=/dev/null
  source "${COORD_MODULE_DIR}/vcs.sh"
  # shellcheck source=/dev/null
  source "${COORD_MODULE_DIR}/jobs.sh"
}

test_dependency_gating() {
  local repo registry output
  repo="$(setup_repo)"
  registry="$repo/.macc/automation/task/task_registry.json"
  output="$(
    PATH="$(make_stub_macc "$repo"):$PATH" \
    PRD_FILE="${FIXTURES}/prd_deps_blocked.json" \
    REPO_DIR="$repo" \
    MAX_DISPATCH=2 \
    "$SCRIPT"
  )"
  local claimed_a claimed_b
  claimed_a="$(jq -r '.tasks[] | select(.id=="TASK-A") | .state' "$registry")"
  claimed_b="$(jq -r '.tasks[] | select(.id=="TASK-B") | .state' "$registry")"
  assert_eq "in_progress" "$claimed_a" "base task should dispatch first"
  assert_eq "todo" "$claimed_b" "dependent task should remain todo"
  rm -rf "$repo"
}


test_dependency_ready() {
  local repo registry
  repo="$(setup_repo)"
  registry="$repo/.macc/automation/task/task_registry.json"
  PATH="$(make_stub_macc "$repo"):$PATH" \
  PRD_FILE="${FIXTURES}/prd_deps.json" \
  REPO_DIR="$repo" \
  MAX_DISPATCH=1 \
  "$SCRIPT" >/dev/null
  local in_progress
  in_progress="$(jq -r '.tasks[] | select(.state=="in_progress") | .id' "$registry")"
  assert_eq "TASK-B" "$in_progress" "dependent task should dispatch when deps merged"
  rm -rf "$repo"
}

test_lock_contention() {
  local repo registry
  repo="$(setup_repo)"
  registry="$repo/.macc/automation/task/task_registry.json"
  PATH="$(make_stub_macc "$repo"):$PATH" \
  PRD_FILE="${FIXTURES}/prd_locks.json" \
  REPO_DIR="$repo" \
  MAX_DISPATCH=2 \
  "$SCRIPT" >/dev/null
  local active
  active="$(jq -r '[.tasks[] | select(.state=="in_progress") | .id] | length' "$registry")"
  assert_eq "1" "$active" "exclusive lock should allow only one active task"
  rm -rf "$repo"
}

test_rerun_idempotency() {
  local repo registry
  repo="$(setup_repo)"
  registry="$repo/.macc/automation/task/task_registry.json"
  PATH="$(make_stub_macc "$repo"):$PATH" \
  PRD_FILE="${FIXTURES}/prd_single.json" \
  REPO_DIR="$repo" \
  MAX_DISPATCH=1 \
  "$SCRIPT" >/dev/null 2>&1
  local first_state
  first_state="$(jq -r '.tasks[] | select(.id=="TASK-ONLY") | .state' "$registry")"
  PATH="$(make_stub_macc "$repo"):$PATH" \
  PRD_FILE="${FIXTURES}/prd_single.json" \
  REPO_DIR="$repo" \
  MAX_DISPATCH=1 \
  "$SCRIPT" >/dev/null 2>&1
  local second_state
  second_state="$(jq -r '.tasks[] | select(.id=="TASK-ONLY") | .state' "$registry")"
  assert_eq "$first_state" "$second_state" "rerun should be idempotent"
  rm -rf "$repo"
}

test_failure_rollback() {
  local repo registry
  repo="$(setup_repo)"
  registry="$repo/.macc/automation/task/task_registry.json"
  PATH="$repo/bin:$PATH"
  mkdir -p "$repo/bin"
  cat >"$repo/bin/macc" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == "--cwd" ]]; then
  shift 2
fi
if [[ "$1" == "coordinator" ]]; then
  case "${2:-}" in
    validate-transition|validate-runtime-transition)
      exit 0
      ;;
    runtime-status-from-event)
      echo "running"
      exit 0
      ;;
    storage-sync)
      exit 0
      ;;
  esac
fi
echo "fail"
exit 1
EOS
  chmod +x "$repo/bin/macc"

  PRD_FILE="${FIXTURES}/prd_single.json" \
  REPO_DIR="$repo" \
  MAX_DISPATCH=1 \
  "$SCRIPT" >/dev/null 2>&1

  local state
  state="$(jq -r '.tasks[] | select(.id=="TASK-ONLY") | .state' "$registry")"
  assert_eq "blocked" "$state" "failed worktree create should block task"
  rm -rf "$repo"
}

test_transition_and_cleanup_cycle() {
  local repo registry
  repo="$(setup_repo)"
  registry="$repo/.macc/automation/task/task_registry.json"
  local stub_path
  stub_path="$(make_stub_macc "$repo")"

  PATH="$stub_path:$PATH" \
  PRD_FILE="${FIXTURES}/prd_single.json" \
  REPO_DIR="$repo" \
  MAX_DISPATCH=1 \
  "$SCRIPT" dispatch >/dev/null

  local state
  state="$(jq -r '.tasks[] | select(.id=="TASK-ONLY") | .state' "$registry")"
  assert_eq "in_progress" "$state" "task should be in progress after dispatch"
  jq '.tasks |= map(if .id=="TASK-ONLY" then .worktree = null else . end)' "$registry" >"${registry}.tmp"
  mv "${registry}.tmp" "$registry"

  PATH="$stub_path:$PATH" REPO_DIR="$repo" \
    PRD_FILE="${FIXTURES}/prd_single.json" \
    "$SCRIPT" --transition TASK-ONLY --state pr_open --pr-url "http://example/pr/1" >/dev/null
  PATH="$stub_path:$PATH" REPO_DIR="$repo" \
    PRD_FILE="${FIXTURES}/prd_single.json" \
    "$SCRIPT" --transition TASK-ONLY --state queued >/dev/null
  PATH="$stub_path:$PATH" REPO_DIR="$repo" \
    PRD_FILE="${FIXTURES}/prd_single.json" \
    "$SCRIPT" --transition TASK-ONLY --state merged >/dev/null

  PATH="$stub_path:$PATH" REPO_DIR="$repo" \
    PRD_FILE="${FIXTURES}/prd_single.json" \
    "$SCRIPT" reconcile >/dev/null
  PATH="$stub_path:$PATH" REPO_DIR="$repo" \
    "$SCRIPT" cleanup >/dev/null
  PATH="$stub_path:$PATH" REPO_DIR="$repo" \
    "$SCRIPT" unlock --all >/dev/null

  state="$(jq -r '.tasks[] | select(.id=="TASK-ONLY") | .state' "$registry")"
  assert_eq "merged" "$state" "task should be merged at end of cycle"

  local worktree lock_count
  worktree="$(jq -r '.tasks[] | select(.id=="TASK-ONLY") | .worktree' "$registry")"
  assert_eq "null" "$worktree" "worktree must be cleared after merged"
  lock_count="$(jq -r '(.resource_locks // {}) | length' "$registry")"
  assert_eq "0" "$lock_count" "locks must be released after cleanup/unlock"

  rm -rf "$repo"
}

test_advance_full_cycle_with_vcs_hook() {
  local repo registry hook stub_path
  repo="$(setup_repo)"
  registry="$repo/.macc/automation/task/task_registry.json"
  stub_path="$(make_stub_macc "$repo")"
  mkdir -p "$(dirname "$registry")"

  cat >"$registry" <<'JSON'
{
  "schema_version": 1,
  "tasks": [
    {
      "id": "TASK-FULL",
      "title": "Full cycle task",
      "state": "in_progress",
      "dependencies": [],
      "exclusive_resources": ["r1"],
      "tool": "codex",
      "worktree": {
        "worktree_path": "/tmp/wt-task-full",
        "branch": "ai/task-full",
        "base_branch": "master",
        "last_commit": "abc"
      }
    }
  ],
  "resource_locks": {
    "r1": {
      "task_id": "TASK-FULL",
      "worktree_path": "/tmp/wt-task-full",
      "locked_at": "2026-01-01T00:00:00Z"
    }
  },
  "state_mapping": {}
}
JSON

  hook="$repo/vcs-hook.sh"
  cat >"$hook" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail
mode="${1:-}"
case "$mode" in
  pr_create) echo '{"pr_url":"https://example.test/pr/42"}' ;;
  review_status) echo '{"decision":"approved","reviewer":"ci-bot"}' ;;
  ci_status) echo '{"status":"green"}' ;;
  queue_status) echo '{"status":"ready"}' ;;
  merge_status) echo '{"status":"merged"}' ;;
  *) echo '{}' ;;
esac
EOS
  chmod +x "$hook"

  PATH="$stub_path:$PATH" \
  REPO_DIR="$repo" \
  COORDINATOR_VCS_HOOK="$hook" \
  "$SCRIPT" advance >/dev/null

  local state lock_count pr_url
  state="$(jq -r '.tasks[] | select(.id=="TASK-FULL") | .state' "$registry")"
  assert_eq "merged" "$state" "advance should drive task to merged via hook"
  pr_url="$(jq -r '.tasks[] | select(.id=="TASK-FULL") | (.pr_url // "")' "$registry")"
  assert_eq "https://example.test/pr/42" "$pr_url" "advance should persist PR URL"
  lock_count="$(jq -r '(.resource_locks // {}) | length' "$registry")"
  assert_eq "0" "$lock_count" "locks must be released when merged"

  rm -rf "$repo"
}

test_runtime_event_replay_idempotent() {
  local repo registry events cursor first_state second_state stub_path
  repo="$(setup_repo)"
  stub_path="$(make_stub_macc "$repo")"
  PATH="$stub_path:$PATH"
  registry="$repo/.macc/automation/task/task_registry.json"
  events="$repo/.macc/log/coordinator/events.jsonl"
  cursor="$repo/.macc/state/coordinator.cursor"
  mkdir -p "$(dirname "$registry")" "$(dirname "$events")" "$(dirname "$cursor")"

  cat >"$registry" <<'JSON'
{
  "schema_version": 1,
  "tasks": [
    {
      "id": "TASK-REPLAY",
      "title": "Replay-safe task",
      "state": "in_progress",
      "dependencies": [],
      "exclusive_resources": [],
      "tool": "codex",
      "task_runtime": {}
    }
  ],
  "processed_event_ids": {},
  "resource_locks": {},
  "state_mapping": {},
  "updated_at": "2026-01-01T00:00:00Z"
}
JSON

  cat >"$events" <<'JSONL'
{"schema_version":"1","event_id":"evt-replay-1","seq":1,"ts":"2026-02-19T10:00:00Z","source":"performer:codex:TASK-REPLAY:1","type":"heartbeat","phase":"dev","status":"running","task_id":"TASK-REPLAY","payload":{}}
{"schema_version":"1","event_id":"evt-replay-2","seq":2,"ts":"2026-02-19T10:00:02Z","source":"performer:codex:TASK-REPLAY:1","type":"phase_result","phase":"dev","status":"failed","task_id":"TASK-REPLAY","payload":{"error":"simulated failure"}}
JSONL

  TASK_REGISTRY_FILE="$registry"
  COORD_EVENTS_FILE="$events"
  COORD_CURSOR_FILE="$cursor"
  COORD_LOG_DIR="$(dirname "$events")"
  REPO_DIR="$repo"
  EVENT_LOG_MAX_BYTES=0
  EVENT_LOG_KEEP_FILES=2
  PROCESSED_EVENT_IDS_MAX=100
  load_coordinator_event_modules

  consume_runtime_events_once
  first_state="$(jq -S . "$registry")"

  # Replay exact same stream from offset 0 and ensure final state is unchanged.
  rm -f "$cursor"
  consume_runtime_events_once
  second_state="$(jq -S . "$registry")"

  assert_eq "$first_state" "$second_state" "event replay must be idempotent"
  rm -rf "$repo"
}

test_emit_event_contract_stable_fields() {
  local repo events line has_schema has_type has_event has_task
  repo="$(setup_repo)"
  events="$repo/.macc/log/coordinator/events.jsonl"
  mkdir -p "$(dirname "$events")"

  COORD_EVENTS_FILE="$events"
  COORD_LOG_DIR="$(dirname "$events")"
  COORD_COMMAND_NAME="test"
  COORD_EVENT_SOURCE="coordinator:test"
  REPO_DIR="$repo"
  EVENT_SEQ_COUNTER=0
  load_coordinator_event_modules

  emit_event "task_dispatched" "Task dispatched" "TASK-EVT" "claimed" "tool=codex" "dev" "started"
  line="$(tail -n1 "$events")"
  has_schema="$(jq -r 'has("schema_version")' <<<"$line")"
  has_type="$(jq -r 'has("type")' <<<"$line")"
  has_event="$(jq -r 'has("event")' <<<"$line")"
  has_task="$(jq -r '.task_id == "TASK-EVT"' <<<"$line")"
  assert_eq "true" "$has_schema" "event contract must keep schema_version"
  assert_eq "true" "$has_type" "event contract must keep type"
  assert_eq "true" "$has_event" "event contract must keep legacy event"
  assert_eq "true" "$has_task" "event contract must keep task_id"

  rm -rf "$repo"
}

test_dead_pid_recovery() {
  local repo registry events stub_path state runtime_status
  repo="$(setup_repo)"
  stub_path="$(make_stub_macc "$repo")"
  PATH="$stub_path:$PATH"
  registry="$repo/.macc/automation/task/task_registry.json"
  events="$repo/.macc/log/coordinator/events.jsonl"
  mkdir -p "$(dirname "$registry")" "$(dirname "$events")"

  cat >"$registry" <<'JSON'
{
  "schema_version": 1,
  "tasks": [
    {
      "id": "TASK-DEAD-PID",
      "title": "Dead pid recovery",
      "state": "claimed",
      "tool": "codex",
      "task_runtime": {
        "status": "running",
        "pid": 999999,
        "current_phase": "dev"
      }
    }
  ],
  "processed_event_ids": {},
  "resource_locks": {},
  "state_mapping": {},
  "updated_at": "2026-01-01T00:00:00Z"
}
JSON

  REPO_DIR="$repo"
  TASK_REGISTRY_FILE="$registry"
  COORD_EVENTS_FILE="$events"
  COORD_LOG_DIR="$(dirname "$events")"
  load_coordinator_runtime_modules

  reconcile_orphan_runtime_tasks
  state="$(jq -r '.tasks[] | select(.id=="TASK-DEAD-PID") | .state' "$registry")"
  runtime_status="$(jq -r '.tasks[] | select(.id=="TASK-DEAD-PID") | .task_runtime.status' "$registry")"
  assert_eq "blocked" "$state" "dead pid should force blocked workflow state"
  assert_eq "failed" "$runtime_status" "dead pid should force failed runtime status"
  rm -rf "$repo"
}

test_stale_heartbeat_recovery() {
  local repo registry events stub_path runtime_status retries stale_metric_count
  repo="$(setup_repo)"
  stub_path="$(make_stub_macc "$repo")"
  PATH="$stub_path:$PATH"
  registry="$repo/.macc/automation/task/task_registry.json"
  events="$repo/.macc/log/coordinator/events.jsonl"
  mkdir -p "$(dirname "$registry")" "$(dirname "$events")"

  cat >"$registry" <<'JSON'
{
  "schema_version": 1,
  "tasks": [
    {
      "id": "TASK-STALE",
      "title": "Stale heartbeat recovery",
      "state": "claimed",
      "tool": "codex",
      "task_runtime": {
        "status": "running",
        "current_phase": "dev",
        "last_heartbeat": "2020-01-01T00:00:00Z",
        "metrics": {
          "retries": 0
        }
      }
    }
  ],
  "processed_event_ids": {},
  "resource_locks": {},
  "state_mapping": {},
  "updated_at": "2026-01-01T00:00:00Z"
}
JSON

  REPO_DIR="$repo"
  TASK_REGISTRY_FILE="$registry"
  COORD_EVENTS_FILE="$events"
  COORD_LOG_DIR="$(dirname "$events")"
  STALE_HEARTBEAT_SECONDS=1
  STALE_HEARTBEAT_ACTION=retry
  load_coordinator_runtime_modules

  cleanup_stale_tasks
  runtime_status="$(jq -r '.tasks[] | select(.id=="TASK-STALE") | .task_runtime.status' "$registry")"
  retries="$(jq -r '.tasks[] | select(.id=="TASK-STALE") | (.task_runtime.retries // 0)' "$registry")"
  stale_metric_count="$(jq -s 'map(select(.type=="stale_runtime_total")) | length' "$events" 2>/dev/null || echo 0)"
  assert_eq "dispatched" "$runtime_status" "stale heartbeat retry should set runtime to dispatched"
  assert_eq "1" "$retries" "stale heartbeat retry should increment retries"
  assert_eq "1" "$stale_metric_count" "stale runtime metric event should be emitted"
  rm -rf "$repo"
}

test_blocked_retry_merged_flow() {
  local repo registry events stub_path base_branch state_dir result_file state
  repo="$(setup_repo)"
  stub_path="$(make_stub_macc "$repo")"
  PATH="$stub_path:$PATH"
  state_dir="$(mktemp -d)"
  registry="$state_dir/task_registry.json"
  events="$state_dir/events.jsonl"
  mkdir -p "$(dirname "$registry")" "$(dirname "$events")"

  base_branch="$(git -C "$repo" branch --show-current)"
  cat >"$registry" <<JSON
{
  "schema_version": 1,
  "tasks": [
    {
      "id": "TASK-RETRY-MERGE",
      "title": "Retry merge flow",
      "state": "blocked",
      "tool": "codex",
      "pr_url": "local://feature/retry-merge",
      "worktree": {
        "worktree_path": "$repo",
        "branch": "feature/retry-merge",
        "base_branch": "$base_branch"
      },
      "task_runtime": {
        "status": "failed",
        "current_phase": "integrate",
        "last_error": "failure:local_merge",
        "merge_result_pending": true
      }
    }
  ],
  "processed_event_ids": {},
  "resource_locks": {},
  "state_mapping": {},
  "updated_at": "2026-01-01T00:00:00Z"
}
JSON

  REPO_DIR="$repo"
  TASK_REGISTRY_FILE="$registry"
  COORD_EVENTS_FILE="$events"
  COORD_LOG_DIR="$(dirname "$events")"
  COORDINATOR_AUTOMERGE=true
  COORDINATOR_MERGE_WORKER="${ROOT_DIR}/automat/merge_worker.sh"
  COORDINATOR_MERGE_AI_FIX=false
  COORDINATOR_MERGE_FIX_HOOK=""
  load_coordinator_runtime_modules

  result_file="$state_dir/merge-result.json"
  cat >"$result_file" <<JSON
{"status":"success","task_id":"TASK-RETRY-MERGE","branch":"feature/retry-merge","base_branch":"$base_branch","report_file":"","error":null,"suggestion":null,"conflicts":[],"merge_output":"ok","hook_output":null,"assisted":false}
JSON
  handle_merge_worker_completion "TASK-RETRY-MERGE" "feature/retry-merge" "$base_branch" "local://feature/retry-merge" "$result_file" "0"

  state="$(jq -r '.tasks[] | select(.id=="TASK-RETRY-MERGE") | .state' "$registry")"
  assert_eq "merged" "$state" "blocked integrate flow should recover to merged on successful merge worker result"
  rm -rf "$state_dir"
  rm -rf "$repo"
}

test_merge_worker_single_flight() {
  local repo registry events stub_path worker starts
  repo="$(setup_repo)"
  stub_path="$(make_stub_macc "$repo")"
  PATH="$stub_path:$PATH"
  registry="$repo/.macc/automation/task/task_registry.json"
  events="$repo/.macc/log/coordinator/events.jsonl"
  mkdir -p "$(dirname "$registry")" "$(dirname "$events")"

  worker="$repo/merge-worker-stub.sh"
  cat >"$worker" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail
starts_file="${STARTS_FILE:?}"
echo "start" >>"$starts_file"
sleep 1
exit 0
EOS
  chmod +x "$worker"

  cat >"$registry" <<JSON
{
  "schema_version": 1,
  "tasks": [
    {
      "id": "TASK-MERGE-SF",
      "title": "single flight",
      "state": "queued",
      "tool": "codex",
      "worktree": {
        "worktree_path": "$repo",
        "branch": "feature/x",
        "base_branch": "master"
      },
      "task_runtime": {
        "status": "phase_done",
        "current_phase": "integrate",
        "merge_result_pending": false
      }
    }
  ],
  "processed_event_ids": {},
  "resource_locks": {},
  "state_mapping": {},
  "updated_at": "2026-01-01T00:00:00Z"
}
JSON

  REPO_DIR="$repo"
  TASK_REGISTRY_FILE="$registry"
  COORD_EVENTS_FILE="$events"
  COORD_LOG_DIR="$(dirname "$events")"
  COORDINATOR_MERGE_WORKER="$worker"
  COORDINATOR_MERGE_AI_FIX=false
  COORDINATOR_MERGE_FIX_HOOK=""
  export STARTS_FILE="$repo/starts.log"
  : >"$STARTS_FILE"
  load_coordinator_runtime_modules

  RUN_LOOP_MERGE_JOBS=()
  start_local_merge_worker_async "TASK-MERGE-SF" "feature/x" "master" "local://feature/x"
  start_local_merge_worker_async "TASK-MERGE-SF" "feature/x" "master" "local://feature/x"

  starts="$(wc -l <"$STARTS_FILE" | tr -d ' ')"
  assert_eq "1" "$starts" "merge worker must start only once for same task while pending"
  local entry pid
  for entry in "${RUN_LOOP_MERGE_JOBS[@]}"; do
    IFS='|' read -r pid _ <<<"$entry"
    if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
  rm -rf "$repo"
}

test_cutover_gate_passes_without_mismatches() {
  local repo stub_path events
  repo="$(setup_repo)"
  stub_path="$(make_stub_macc "$repo")"
  PATH="$stub_path:$PATH"
  events="$repo/.macc/log/coordinator/events.jsonl"
  mkdir -p "$(dirname "$events")"

  cat >"$events" <<'JSONL'
{"schema_version":"1","event_id":"evt-1","seq":1,"ts":"2026-02-20T00:00:00Z","source":"coordinator","type":"task_dispatched","status":"started","task_id":"T1","payload":{}}
{"schema_version":"1","event_id":"evt-2","seq":2,"ts":"2026-02-20T00:00:01Z","source":"coordinator","type":"performer_complete","status":"done","task_id":"T1","payload":{}}
JSONL

  CUTOVER_GATE_WINDOW_EVENTS=200 \
  CUTOVER_GATE_MAX_BLOCKED_RATIO=0.50 \
  CUTOVER_GATE_MAX_STALE_RATIO=0.50 \
  REPO_DIR="$repo" \
  "$SCRIPT" cutover-gate >/dev/null

  rm -rf "$repo"
}

test_cutover_gate_fails_on_storage_mismatch() {
  local repo stub_path events
  repo="$(setup_repo)"
  stub_path="$(make_stub_macc "$repo")"
  PATH="$stub_path:$PATH"
  events="$repo/.macc/log/coordinator/events.jsonl"
  mkdir -p "$(dirname "$events")"

  cat >"$events" <<'JSONL'
{"schema_version":"1","event_id":"evt-1","seq":1,"ts":"2026-02-20T00:00:00Z","source":"coordinator","type":"storage_mismatch_count","status":"error","payload":{"count":1}}
JSONL

  set +e
  CUTOVER_GATE_WINDOW_EVENTS=200 \
  REPO_DIR="$repo" \
  "$SCRIPT" cutover-gate >/dev/null 2>&1
  local rc=$?
  set -e
  assert_eq "1" "$rc" "cutover gate should fail when storage mismatch event exists"
  rm -rf "$repo"
}

lint_shell() {
  if command -v shellcheck >/dev/null 2>&1; then
    shellcheck "$SCRIPT"
  else
    echo "shellcheck not installed; skipping lint"
  fi
}

require_cmd git
require_cmd jq
require_cmd bash

lint_shell
test_dependency_gating
test_dependency_ready
test_lock_contention
test_rerun_idempotency
test_failure_rollback
test_transition_and_cleanup_cycle
test_advance_full_cycle_with_vcs_hook
test_runtime_event_replay_idempotent
test_emit_event_contract_stable_fields
test_dead_pid_recovery
test_stale_heartbeat_recovery
test_blocked_retry_merged_flow
test_merge_worker_single_flight
test_cutover_gate_passes_without_mismatches
test_cutover_gate_fails_on_storage_mismatch

echo "All coordinator tests passed."
