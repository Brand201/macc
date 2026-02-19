#!/usr/bin/env bash
set -euo pipefail

# coordinator.sh
# - Reads PRD tasks
# - Maintains .macc/automation/task/task_registry.json
# - Dispatches READY tasks to dedicated MACC worktrees
# - Applies dependency gating + exclusive resource locking
# - Assigns at most one task per worktree

PRD_FILE="${PRD_FILE:-prd.json}"
REPO_DIR="${REPO_DIR:-.}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
TASK_REGISTRY_REL_PATH=".macc/automation/task/task_registry.json"
TASK_REGISTRY_FILE=""
AGENT_ID="${AGENT_ID:-agentA}"
DEFAULT_TOOL="${DEFAULT_TOOL:-codex}"
DEFAULT_BASE_BRANCH="${DEFAULT_BASE_BRANCH:-master}"
MAX_DISPATCH="${MAX_DISPATCH:-10}" # 0 = no cap
MAX_PARALLEL="${MAX_PARALLEL:-3}" # max concurrent performer runs
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-0}" # lock wait timeout, 0 = no timeout
PHASE_RUNNER_MAX_ATTEMPTS="${PHASE_RUNNER_MAX_ATTEMPTS:-1}"
STALE_CLAIMED_SECONDS="${STALE_CLAIMED_SECONDS:-0}"
STALE_IN_PROGRESS_SECONDS="${STALE_IN_PROGRESS_SECONDS:-0}"
STALE_CHANGES_REQUESTED_SECONDS="${STALE_CHANGES_REQUESTED_SECONDS:-0}"
STALE_ACTION="${STALE_ACTION:-abandon}" # abandon | todo | blocked
STALE_HEARTBEAT_SECONDS="${STALE_HEARTBEAT_SECONDS:-0}" # 0 disables runtime heartbeat stale checks
STALE_HEARTBEAT_ACTION="${STALE_HEARTBEAT_ACTION:-block}" # retry | block | requeue
COORDINATOR_TOOL="${COORDINATOR_TOOL:-}"
ENABLED_TOOLS_CSV="${ENABLED_TOOLS_CSV:-}"
TOOL_PRIORITY_CSV="${TOOL_PRIORITY_CSV:-}"
MAX_PARALLEL_PER_TOOL_JSON="${MAX_PARALLEL_PER_TOOL_JSON:-{}}"
TOOL_SPECIALIZATIONS_JSON="${TOOL_SPECIALIZATIONS_JSON:-{}}"
WORKTREE_POOL_MODE="${WORKTREE_POOL_MODE:-true}" # true|false: reuse idle compatible worktrees
COORDINATOR_VCS_HOOK="${COORDINATOR_VCS_HOOK:-}" # optional executable implementing PR/CI/queue/merge actions
COORDINATOR_AUTOMERGE="${COORDINATOR_AUTOMERGE:-true}" # true|false: allow default local merge fallback
COORDINATOR_MERGE_WORKER="${COORDINATOR_MERGE_WORKER:-$SCRIPT_DIR/merge_worker.sh}" # local merge worker script
COORDINATOR_MERGE_AI_FIX="${COORDINATOR_MERGE_AI_FIX:-false}" # true|false: allow AI-assisted merge conflict fixing
COORDINATOR_MERGE_FIX_HOOK="${COORDINATOR_MERGE_FIX_HOOK:-}" # optional hook invoked by merge worker on merge conflicts
EVENT_LOG_MAX_BYTES="${EVENT_LOG_MAX_BYTES:-5242880}" # rotate events.jsonl when above this size
EVENT_LOG_KEEP_FILES="${EVENT_LOG_KEEP_FILES:-5}" # number of rotated event files to keep
PROCESSED_EVENT_IDS_MAX="${PROCESSED_EVENT_IDS_MAX:-10000}" # max dedup IDs retained in registry
SLO_DEV_SECONDS="${SLO_DEV_SECONDS:-0}" # 0 disables; warn when dev runtime exceeds this value
SLO_REVIEW_SECONDS="${SLO_REVIEW_SECONDS:-300}" # 0 disables
SLO_INTEGRATE_SECONDS="${SLO_INTEGRATE_SECONDS:-0}" # 0 disables
SLO_WAIT_SECONDS="${SLO_WAIT_SECONDS:-0}" # 0 disables
SLO_RETRIES_MAX="${SLO_RETRIES_MAX:-0}" # 0 disables

ENABLED_TOOLS_JSON="[]"
TOOL_PRIORITY_JSON="[]"
COORD_LOG_DIR=""
COORD_LOG_FILE=""
COORD_EVENTS_FILE=""
COORD_COMMAND_NAME=""
COORD_CURSOR_FILE=""
COORD_EVENT_SOURCE=""
EVENT_SEQ_COUNTER=0
RUN_LOOP_ACTIVE_JOBS=()
RUN_LOOP_MERGE_JOBS=()

note() {
  local msg="$*"
  if [[ -n "${COORD_TERM_FD:-}" ]]; then
    printf '%s\n' "$msg" >&"$COORD_TERM_FD"
  else
    printf '%s\n' "$msg"
  fi
  printf '%s\n' "$msg"
}

emit_event() {
  local event="$1"
  local msg="${2:-}"
  local task_id="${3:-}"
  local state="${4:-}"
  local detail="${5:-}"
  local phase="${6:-}"
  local status="${7:-}"
  local source="${8:-${COORD_EVENT_SOURCE:-coordinator}}"
  local payload_json="${9:-{}}"
  [[ -n "${COORD_EVENTS_FILE:-}" ]] || return 0
  EVENT_SEQ_COUNTER=$((EVENT_SEQ_COUNTER + 1))
  if [[ -z "$status" ]]; then
    status="${state:-${event}}"
  fi
  if ! jq -e 'type == "object"' <<<"$payload_json" >/dev/null 2>&1; then
    payload_json="$(jq -nc --arg detail "$payload_json" '{detail:$detail}')"
  fi
  jq -nc \
    --arg schema_version "1" \
    --arg event_id "${task_id:-global}-${EVENT_SEQ_COUNTER}-$(date +%s%N)" \
    --argjson seq "$EVENT_SEQ_COUNTER" \
    --arg ts "$(now_iso)" \
    --arg event "$event" \
    --arg command "${COORD_COMMAND_NAME:-}" \
    --arg msg "$msg" \
    --arg task_id "$task_id" \
    --arg source "$source" \
    --arg phase "$phase" \
    --arg status "$status" \
    --arg state "$state" \
    --arg detail "$detail" \
    --argjson payload "$payload_json" \
    '{
      schema_version:$schema_version,
      event_id:$event_id,
      seq:$seq,
      ts:$ts,
      source:$source,
      type:$event,
      phase:($phase|select(length>0)),
      status:$status,
      payload:$payload,
      event:$event,
      command:$command,
      msg:$msg,
      task_id:($task_id|select(length>0)),
      state:($state|select(length>0)),
      detail:($detail|select(length>0))
    }' >>"$COORD_EVENTS_FILE" 2>/dev/null || true
}

spinner_enabled() {
  if [[ -n "${CI:-}" || -n "${MACC_NO_SPINNER:-}" ]]; then
    return 1
  fi
  if [[ -n "${COORD_TERM_FD:-}" ]]; then
    [[ -t "${COORD_TERM_FD}" ]]
  else
    [[ -t 1 ]]
  fi
}

spinner_start() {
  local msg="$1"
  local fd
  if [[ -n "${COORD_TERM_FD:-}" ]]; then
    fd="$COORD_TERM_FD"
  else
    fd=1
  fi
  if ! spinner_enabled; then
    return 0
  fi
  SPINNER_MSG="$msg"
  (
    local frames='|/-\'
    local i=0
    while true; do
      local ch="${frames:i%4:1}"
      printf '\r[%s] %s' "$ch" "$SPINNER_MSG" >&"$fd"
      i=$((i + 1))
      sleep 0.1
    done
  ) &
  SPINNER_PID=$!
}

spinner_stop() {
  local msg="$1"
  local fd
  if [[ -n "${COORD_TERM_FD:-}" ]]; then
    fd="$COORD_TERM_FD"
  else
    fd=1
  fi
  if [[ -n "${SPINNER_PID:-}" ]]; then
    kill "$SPINNER_PID" >/dev/null 2>&1 || true
    wait "$SPINNER_PID" >/dev/null 2>&1 || true
    SPINNER_PID=""
    if spinner_enabled; then
      printf '\r[done] %s\n' "$msg" >&"$fd"
    fi
  fi
}

on_exit() {
  local rc=$?
  spinner_stop "Coordinator stopped."
  if [[ "$rc" -eq 0 ]]; then
    emit_event "command_end" "Coordinator command completed"
  else
    emit_event "command_error" "Coordinator command failed" "" "" "exit_code=${rc}"
  fi
  if [[ "$rc" -ne 0 && -n "${COORD_TERM_FD:-}" && -n "${COORD_LOG_FILE:-}" ]]; then
    printf 'Coordinator failed. See log: %s\n' "$COORD_LOG_FILE" >&"$COORD_TERM_FD"
  fi
}
trap on_exit EXIT

setup_logging() {
  local command_name="${1:-dispatch}"
  COORD_COMMAND_NAME="$command_name"
  mkdir -p "${REPO_DIR}/.macc/log/coordinator"
  mkdir -p "${REPO_DIR}/.macc/state"
  COORD_LOG_DIR="${REPO_DIR}/.macc/log/coordinator"
  COORD_EVENTS_FILE="${COORD_LOG_DIR}/events.jsonl"
  COORD_CURSOR_FILE="${REPO_DIR}/.macc/state/coordinator.cursor"
  local ts
  ts="$(date -u +"%Y%m%dT%H%M%SZ")"
  COORD_EVENT_SOURCE="coordinator:${command_name}:$$:$(date +%s%N)"
  COORD_LOG_FILE="${COORD_LOG_DIR}/${command_name}-${ts}.md"

  exec {COORD_TERM_FD}>&1
  exec >>"$COORD_LOG_FILE" 2>&1

  note "# Coordinator log"
  note ""
  note "- Command: ${command_name}"
  note "- Repository: ${REPO_DIR}"
  note "- Started (UTC): $(now_iso)"
  note ""
  note "Coordinator log file: ${COORD_LOG_FILE}"
  emit_event "command_start" "Coordinator command started"
}

usage() {
  cat <<EOF
Usage:
  AGENT_ID=agentA ./coordinator.sh [command] [options]

Commands:
  run         Realtime coordinator control-plane (scheduler + monitor + event consumer)
  dispatch    Sync, cleanup, and dispatch READY tasks (default)
  advance     Progress active tasks through PR/CI/review/queue/merge states
  retry-phase Retry one failed phase for a task (targeted remediation)
  sync        Sync registry from PRD without dispatching
  status      Show registry summary + lock status
  reconcile   Reconcile registry with worktree state on disk
  unlock      Release locks (task or resource)
  cleanup     Run stale-task cleanup only

Env vars:
  PRD_FILE            Path to PRD JSON (default: prd.json)
  Task registry is fixed at \$REPO_DIR/.macc/automation/task/task_registry.json
  REPO_DIR            Path to git repository (default: .)
  AGENT_ID            Coordinator/agent identifier (default: agentA)
  DEFAULT_TOOL        Tool used when task does not declare one (default: codex)
  DEFAULT_BASE_BRANCH Default base branch when task.base_branch is unset (default: master)
  MAX_DISPATCH        Max tasks to dispatch in one run, 0 for all READY tasks (default: 0)
  MAX_PARALLEL        Max concurrent performer runs (default: 1)
  TIMEOUT_SECONDS     Timeout while waiting for lock (default: 0)
  PHASE_RUNNER_MAX_ATTEMPTS Max attempts for phase runner fallback (default: 1)
  STALE_CLAIMED_SECONDS         Auto-abandon claimed tasks older than this (0 disables)
  STALE_IN_PROGRESS_SECONDS     Auto-abandon in_progress tasks older than this (0 disables)
  STALE_CHANGES_REQUESTED_SECONDS Auto-abandon changes_requested tasks older than this (0 disables)
  STALE_ACTION                  Action for stale tasks: abandon|todo|blocked (default: abandon)
  STALE_HEARTBEAT_SECONDS       Runtime heartbeat stale threshold in seconds (0 disables)
  STALE_HEARTBEAT_ACTION        Action on stale runtime heartbeat: retry|block|requeue (default: block)
  COORDINATOR_TOOL              Optional fixed tool for coordinator phase hooks (review/fix/integrate)
  ENABLED_TOOLS_CSV             Optional allowed tool IDs (comma-separated; usually from macc.yaml tools.enabled)
  TOOL_PRIORITY_CSV             Optional priority order for tool selection (comma-separated)
  MAX_PARALLEL_PER_TOOL_JSON    Optional JSON object {"tool":<cap>} with per-tool concurrency caps
  TOOL_SPECIALIZATIONS_JSON     Optional JSON object {"category":["tool-a","tool-b"]} for category routing
  WORKTREE_POOL_MODE            Reuse idle compatible worktrees when true (default: true)
  COORDINATOR_VCS_HOOK          Optional hook executable for PR/CI/merge integration
  COORDINATOR_AUTOMERGE         Allow local merge fallback when no hook is configured (default: true)
  COORDINATOR_MERGE_WORKER      Path to local merge worker script (default: <coordinator_dir>/merge_worker.sh)
  COORDINATOR_MERGE_AI_FIX      Allow AI-assisted merge conflict fixing in merge worker (default: false)
  COORDINATOR_MERGE_FIX_HOOK    Optional merge-fix hook path; defaults to <coordinator_dir>/hooks/ai-merge-fix.sh when AI fix is enabled
  EVENT_LOG_MAX_BYTES           Rotate .macc/log/coordinator/events.jsonl above this size (default: 5242880)
  EVENT_LOG_KEEP_FILES          Keep this many rotated event log files (default: 5)
  PROCESSED_EVENT_IDS_MAX       Max dedup IDs retained in registry before compaction (default: 10000)
  SLO_DEV_SECONDS               Warn when dev_s exceeds this threshold (0 disables)
  SLO_REVIEW_SECONDS            Warn when review_s exceeds this threshold (default: 300, 0 disables)
  SLO_INTEGRATE_SECONDS         Warn when integrate_s exceeds this threshold (0 disables)
  SLO_WAIT_SECONDS              Warn when wait_s exceeds this threshold (0 disables)
  SLO_RETRIES_MAX               Warn when retries exceeds this threshold (0 disables)

Unlock:
  ./coordinator.sh unlock --task <task_id> [--unlock-state blocked|todo]
  ./coordinator.sh unlock --resource <name>
  ./coordinator.sh unlock --all

Transition:
  ./coordinator.sh --transition <task_id> --state <state> [--pr-url <url>] [--reviewer <name>] [--reason <text>]
Failure handling:
  ./coordinator.sh --failure <task_id> --failure-kind <kind> [--reason <text>]
  kinds: worktree_create | pr_create | ci_red | merge_queue_fail | rebase_required
Signal ingestion:
  ./coordinator.sh --signal <task_id> --signal-json <path>
Retry failed phase:
  ./coordinator.sh retry-phase --retry-task <task_id> --retry-phase <dev|review|fix|integrate> [--skip]

Task readiness rules:
  - state == "todo"
  - all dependencies are in "merged"
  - all exclusive_resources are unlocked by active tasks

Active states:
  claimed, in_progress, pr_open, changes_requested, queued
EOF
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Error: missing required command: $1" >&2
    exit 1
  }
}

csv_to_json_array() {
  local csv="${1:-}"
  jq -Rn --arg csv "$csv" '
    ($csv // "")
    | split(",")
    | map(gsub("^\\s+|\\s+$"; ""))
    | map(select(length > 0))
    | unique
  '
}

now_iso() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

sanitize_slug() {
  echo "$1" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//'
}

safe_slug() {
  local slug
  slug="$(sanitize_slug "$1")"
  [[ -n "$slug" ]] || slug="task"
  echo "$slug"
}

branch_exists() {
  local branch="${1:-}"
  [[ -n "$branch" ]] || return 1
  git -C "$REPO_DIR" rev-parse --verify "${branch}^{commit}" >/dev/null 2>&1
}

current_repo_branch() {
  git -C "$REPO_DIR" symbolic-ref --quiet --short HEAD 2>/dev/null \
    || git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null \
    || true
}

resolve_base_branch() {
  local requested="${1:-}"
  local fallback="${DEFAULT_BASE_BRANCH:-main}"
  local current

  if branch_exists "$requested"; then
    echo "$requested"
    return
  fi

  if branch_exists "$fallback"; then
    if [[ -n "$requested" && "$requested" != "$fallback" ]]; then
      echo "Info: base branch '$requested' not found; using '$fallback'." >&2
    fi
    echo "$fallback"
    return
  fi

  current="$(current_repo_branch)"
  if branch_exists "$current"; then
    if [[ -n "$requested" && "$requested" != "$current" ]]; then
      echo "Info: base branch '$requested' not found; using current branch '$current'." >&2
    fi
    echo "$current"
    return
  fi

  echo "${requested:-$fallback}"
}

is_truthy() {
  local v
  v="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  [[ "$v" == "1" || "$v" == "true" || "$v" == "yes" || "$v" == "on" ]]
}

ensure_prd_valid() {
  [[ -f "$PRD_FILE" ]] || {
    echo "Error: PRD file not found: $PRD_FILE" >&2
    exit 1
  }
  jq -e '.tasks and (.tasks | type == "array")' "$PRD_FILE" >/dev/null 2>&1 || {
    echo "Error: PRD must be valid JSON with a top-level tasks array: $PRD_FILE" >&2
    exit 1
  }
  jq -e '
    .tasks
    | all(
        (has("id") and ((.id|type)=="string" or (.id|type)=="number"))
        and (has("title") and ((.title|type)=="string"))
      )
  ' "$PRD_FILE" >/dev/null 2>&1 || {
    echo "Error: PRD tasks must have id + title fields: $PRD_FILE" >&2
    exit 1
  }
}

ensure_repo_valid() {
  git -C "$REPO_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1 || {
    echo "Error: REPO_DIR is not a git repository: $REPO_DIR" >&2
    exit 1
  }
}

normalize_paths() {
  local script_dir
  script_dir="$SCRIPT_DIR"
  REPO_DIR="$(cd "$REPO_DIR" && pwd -P)"
  if [[ "$PRD_FILE" != /* ]]; then
    PRD_FILE="${REPO_DIR%/}/${PRD_FILE}"
  fi
  TASK_REGISTRY_FILE="${REPO_DIR%/}/${TASK_REGISTRY_REL_PATH}"
  if [[ "$COORDINATOR_MERGE_WORKER" != /* ]]; then
    COORDINATOR_MERGE_WORKER="${REPO_DIR%/}/${COORDINATOR_MERGE_WORKER}"
  fi
  if [[ ! -x "$COORDINATOR_MERGE_WORKER" && -x "${script_dir}/merge_worker.sh" ]]; then
    COORDINATOR_MERGE_WORKER="${script_dir}/merge_worker.sh"
  fi
  if is_truthy "$COORDINATOR_MERGE_AI_FIX" && [[ -z "$COORDINATOR_MERGE_FIX_HOOK" ]]; then
    COORDINATOR_MERGE_FIX_HOOK="${script_dir}/hooks/ai-merge-fix.sh"
  fi
  if [[ -n "$COORDINATOR_MERGE_FIX_HOOK" && "$COORDINATOR_MERGE_FIX_HOOK" != /* ]]; then
    COORDINATOR_MERGE_FIX_HOOK="${REPO_DIR%/}/${COORDINATOR_MERGE_FIX_HOOK}"
  fi
  if [[ -n "$COORDINATOR_MERGE_FIX_HOOK" && ! -x "$COORDINATOR_MERGE_FIX_HOOK" && -x "${script_dir}/hooks/ai-merge-fix.sh" ]]; then
    COORDINATOR_MERGE_FIX_HOOK="${script_dir}/hooks/ai-merge-fix.sh"
  fi
}

lock_acquire() {
  COORD_LOCK_DIR="${TASK_REGISTRY_FILE}.lock"
  mkdir -p "$(dirname "$COORD_LOCK_DIR")"
  local start_ts now_ts elapsed
  start_ts="$(date +%s)"

  while ! mkdir "$COORD_LOCK_DIR" >/dev/null 2>&1; do
    sleep 0.1
    if [[ "$TIMEOUT_SECONDS" -gt 0 ]]; then
      now_ts="$(date +%s)"
      elapsed=$((now_ts - start_ts))
      if [[ "$elapsed" -ge "$TIMEOUT_SECONDS" ]]; then
        echo "Error: timeout waiting for lock: $COORD_LOCK_DIR" >&2
        exit 2
      fi
    fi
  done

  COORD_LOCK_HELD="true"
  trap 'rmdir "${COORD_LOCK_DIR:-}" >/dev/null 2>&1 || true' EXIT
}

lock_release() {
  if [[ "${COORD_LOCK_HELD:-false}" == "true" ]]; then
    rmdir "${COORD_LOCK_DIR:-}" >/dev/null 2>&1 || true
    COORD_LOCK_HELD="false"
  fi
}

ensure_registry_file() {
  mkdir -p "$(dirname "$TASK_REGISTRY_FILE")"
  if [[ -f "$TASK_REGISTRY_FILE" ]]; then
    return
  fi

  local now
  now="$(now_iso)"
  jq -n \
    --arg lot "$(jq -r '.lot // ""' "$PRD_FILE")" \
    --arg version "$(jq -r '.version // "1.0"' "$PRD_FILE")" \
    --arg generated_at "$(jq -r '.generated_at // ""' "$PRD_FILE")" \
    --arg timezone "$(jq -r '.timezone // "UTC"' "$PRD_FILE")" \
    --arg now "$now" \
    '
    {
      schema_version: 1,
      lot: $lot,
      version: $version,
      generated_at: $generated_at,
      timezone: $timezone,
      priority_mapping: {},
      state_mapping: {
        "todo": "todo",
        "claimed": "reserved",
        "in_progress": "dev in progress",
        "pr_open": "awaiting review/CI",
        "changes_requested": "changes requested",
        "queued": "in merge queue",
        "merged": "merged"
      },
      tasks: [],
      processed_event_ids: {},
      resource_locks: {},
      updated_at: $now
    }
    ' >"$TASK_REGISTRY_FILE"
}

ensure_registry_valid() {
  [[ -f "$TASK_REGISTRY_FILE" ]] || {
    echo "Error: task registry not found: $TASK_REGISTRY_FILE" >&2
    exit 1
  }
  jq -e '
    (.tasks | type == "array")
    and ((.processed_event_ids // {}) | type == "object")
    and (.resource_locks | type == "object")
    and (.state_mapping | type == "object")
  ' "$TASK_REGISTRY_FILE" >/dev/null 2>&1 || {
    echo "Error: task registry schema invalid: $TASK_REGISTRY_FILE" >&2
    exit 1
  }
}

status_summary() {
  note "Registry: $TASK_REGISTRY_FILE"
  local total
  total="$(jq -r '(.tasks // []) | length' "$TASK_REGISTRY_FILE")"
  note "Tasks: ${total}"
  jq -r '
    (.tasks // [])
    | sort_by(.state)
    | group_by(.state)
    | map("\(.[0].state): \(length)")
    | .[]
  ' "$TASK_REGISTRY_FILE"
  local locks
  locks="$(jq -r '(.resource_locks // {}) | length' "$TASK_REGISTRY_FILE")"
  note "Locks: ${locks}"
  jq -r '
    (.resource_locks // {})
    | to_entries[]
    | "  \(.key) -> \(.value.task_id)"
  ' "$TASK_REGISTRY_FILE"
  local slo_warn_count
  slo_warn_count="$(jq -r '
    (.tasks // [])
    | map(((.task_runtime.slo_warnings // {}) | length))
    | add // 0
  ' "$TASK_REGISTRY_FILE")"
  note "SLO warnings: ${slo_warn_count}"
  jq -r '
    (.tasks // [])[]
    | .id as $id
    | ((.task_runtime.slo_warnings // {}) | to_entries[])
    | "  task=\($id) metric=\(.key) value=\(.value.value // 0) threshold=\(.value.threshold // 0)"
  ' "$TASK_REGISTRY_FILE"
}

clear_inactive_worktrees() {
  local tmp
  tmp="$(mktemp)"
  jq '
    def is_active($s):
      ($s == "claimed" or $s == "in_progress" or $s == "pr_open" or $s == "changes_requested" or $s == "queued");

    .tasks |= map(
      if (is_active(.state) | not) and (.worktree != null) and (.state == "todo" or .state == "merged" or .state == "abandoned") then
        .worktree = null
      else
        .
      end
    )
  ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
}

reconcile_registry() {
  reconcile_orphan_runtime_tasks
  clear_inactive_worktrees
  while IFS=$'\t' read -r task_id state worktree_path; do
    [[ -n "$task_id" && -n "$worktree_path" ]] || continue
    if ! worktree_exists_on_disk "$worktree_path"; then
      apply_transition "$task_id" "blocked" "" "" "worktree_missing"
      echo "Blocked task due to missing worktree: ${task_id}"
    fi
  done < <(jq -r '
    (.tasks // [])[]
    | select(.worktree != null)
    | [.id, .state, .worktree.worktree_path] | @tsv
  ' "$TASK_REGISTRY_FILE")
}

unlock_locks() {
  local unlock_task="$1"
  local unlock_resource="$2"
  local unlock_all="$3"
  local unlock_state="$4"

  if [[ -n "$unlock_task" ]]; then
    local current
    current="$(task_state "$unlock_task")"
    validate_transition "$current" "$unlock_state"
    apply_transition "$unlock_task" "$unlock_state" "" "" "manual_unlock"
    echo "Unlocked task ${unlock_task} via transition to ${unlock_state}"
    return 0
  fi

  local tmp
  tmp="$(mktemp)"
  if [[ -n "$unlock_resource" ]]; then
    jq --arg res "$unlock_resource" '(.resource_locks // {}) | del(.[$res]) as $locks | .resource_locks = $locks' \
      "$TASK_REGISTRY_FILE" >"$tmp"
    mv "$tmp" "$TASK_REGISTRY_FILE"
    echo "Unlocked resource ${unlock_resource}"
    return 0
  fi

  if [[ "$unlock_all" == "true" ]]; then
    jq '.resource_locks = {}' "$TASK_REGISTRY_FILE" >"$tmp"
    mv "$tmp" "$TASK_REGISTRY_FILE"
    echo "Cleared all resource locks"
    return 0
  fi

  echo "Error: unlock requires --task, --resource, or --all" >&2
  exit 1
}
sync_registry_from_prd() {
  local now tmp
  now="$(now_iso)"
  tmp="$(mktemp)"

  jq --slurpfile prd "$PRD_FILE" \
     --arg now "$now" \
     --arg default_tool "$DEFAULT_TOOL" \
     --arg default_base_branch "$DEFAULT_BASE_BRANCH" \
     '
     def as_array:
       if . == null then []
       elif type == "array" then .
       else [.] end;

     def is_active_state($s):
       ($s == "claimed" or $s == "in_progress" or $s == "pr_open" or $s == "changes_requested" or $s == "queued");

     . as $reg
     | ($prd[0]) as $p
     | .schema_version = 1
     | .lot = ($p.lot // .lot // "")
     | .version = ($p.version // .version // "1.0")
     | .generated_at = ($p.generated_at // .generated_at // "")
     | .timezone = ($p.timezone // .timezone // "UTC")
     | .priority_mapping = ($p.priority_mapping // .priority_mapping // {})
     | .state_mapping = (.state_mapping // {
         "todo": "todo",
         "claimed": "reserved",
         "in_progress": "dev in progress",
         "pr_open": "awaiting review/CI",
         "changes_requested": "changes requested",
         "queued": "in merge queue",
         "merged": "merged"
       })
     | .tasks = (
         ($p.tasks // []) | map(
           . as $t
           | (($reg.tasks // []) | map(select(.id == ($t.id|tostring))) | .[0]) as $old
           | {
               id: ($t.id|tostring),
               title: ($t.title // $old.title // ""),
               priority: ($t.priority // $old.priority // "p4"),
               state: (
                 if ($t.state // "") == "merged" then "merged"
                 else ($old.state // ($t.state // "todo"))
                 end
               ),
               dependencies: (($t.dependencies // $old.dependencies // []) | as_array),
               exclusive_resources: (($t.exclusive_resources // $old.exclusive_resources // []) | as_array),
               assignee: ($old.assignee // null),
               claimed_at: ($old.claimed_at // null),
               tool: ($t.tool // $t.git.tool // $old.tool // null),
               coordinator_tool: ($t.coordinator_tool // $old.coordinator_tool // null),
               category: ($t.category // $old.category // null),
               scope: ($t.scope // $t.git.scope // $old.scope // null),
               base_branch: ($t.git.base_branch // $default_base_branch // $old.base_branch),
               worktree: ($old.worktree // null),
               review: ($old.review // {"reviewer": null, "changed": false, "last_reviewed_at": null}),
               task_runtime: ($old.task_runtime // {
                 status: "idle",
                 pid: null,
                 started_at: null,
                 phase_started_at: null,
                 last_heartbeat: null,
                 wait_started_at: null,
                 current_phase: null,
                 attempt: 0,
                 retries: 0,
                 metrics: {
                   dev_s: 0,
                   review_s: 0,
                   integrate_s: 0,
                   wait_s: 0,
                   retries: 0
                 },
                 slo_warnings: {},
                 last_error: null,
                 last_seq: 0,
                 last_event_id: null,
                 last_seq_source: null,
                 last_commit_created_at: null
               })
             }
         )
       )
     | .tasks |= map(
         if .state == "merged" then
           .assignee = null
           | .claimed_at = null
           | .worktree = null
           | .task_runtime = (.task_runtime // {})
           | .task_runtime.status = "idle"
           | .task_runtime.pid = null
           | .task_runtime.started_at = null
           | .task_runtime.current_phase = null
         else
           .
         end
       )
     | .resource_locks = (
         reduce ((.tasks // [])[] | select(is_active_state(.state) and (.worktree != null))) as $t
           ({};
            reduce ($t.exclusive_resources // [])[] as $r
              (.;
               if .[$r] == null then
                 .[$r] = {
                   task_id: $t.id,
                   worktree_path: ($t.worktree.worktree_path // null),
                   locked_at: ($t.claimed_at // $now)
                 }
               else
                 .
               end
              )
           )
       )
     | .processed_event_ids = (.processed_event_ids // {})
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"

  mv "$tmp" "$TASK_REGISTRY_FILE"
}

task_field() {
  local task_id="$1"
  local field="$2"
  jq -r --arg id "$task_id" "(.tasks // [])[] | select(.id == \$id) | ${field}" "$TASK_REGISTRY_FILE"
}

apply_signal_update() {
  local task_id="$1"
  local signal_path="$2"
  local now tmp
  now="$(now_iso)"
  tmp="$(mktemp)"

  [[ -f "$signal_path" ]] || {
    echo "Error: signal JSON not found: $signal_path" >&2
    exit 1
  }
  jq -e '.' "$signal_path" >/dev/null 2>&1 || {
    echo "Error: signal JSON is invalid: $signal_path" >&2
    exit 1
  }

  jq --arg id "$task_id" \
     --arg now "$now" \
     --slurpfile sig "$signal_path" \
     '
     def merge_files($cur; $new):
       if ($new | type) == "array" then
         ($cur + $new) | unique
       else
         $cur
       end;

     .tasks |= map(
       if .id == $id then
         .updated_at = $now
         | (if ($sig[0].last_commit // "") != "" then .worktree.last_commit = ($sig[0].last_commit) else . end)
         | (if ($sig[0].pr_url // "") != "" then .pr_url = ($sig[0].pr_url) else . end)
         | (if ($sig[0].reviewer // "") != "" then .review.reviewer = ($sig[0].reviewer) else . end)
         | (if ($sig[0].review_notes // "") != "" then .review.notes = ($sig[0].review_notes) else . end)
         | (if ($sig[0].review_changed // null) != null then .review.changed = ($sig[0].review_changed) else . end)
         | (if ($sig[0].files_changed // null) != null then
              .files_changed = merge_files((.files_changed // []); ($sig[0].files_changed))
            else
              .
            end)
       else
         .
       end
     )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"

  mv "$tmp" "$TASK_REGISTRY_FILE"
}
task_exists() {
  local task_id="$1"
  jq -e --arg id "$task_id" '(.tasks // [])[] | select(.id == $id)' "$TASK_REGISTRY_FILE" >/dev/null 2>&1
}

task_has_worktree() {
  local task_id="$1"
  jq -e --arg id "$task_id" '(.tasks // [])[] | select(.id == $id) | (.worktree != null)' "$TASK_REGISTRY_FILE" >/dev/null 2>&1
}

worktree_in_use() {
  local path="$1"
  jq -e --arg path "$path" '
    (.tasks // [])[] | select(.worktree != null) | select(.worktree.worktree_path == $path)
  ' "$TASK_REGISTRY_FILE" >/dev/null 2>&1
}

worktree_task_id() {
  local path="$1"
  jq -r --arg path "$path" '
    (.tasks // [])[] | select(.worktree != null and .worktree.worktree_path == $path) | .id
  ' "$TASK_REGISTRY_FILE"
}

worktree_exists_on_disk() {
  local path="$1"
  [[ -d "$path" ]] && git -C "$path" rev-parse --is-inside-work-tree >/dev/null 2>&1
}

task_state() {
  local task_id="$1"
  task_field "$task_id" '.state'
}

task_scope() {
  local task_id="$1"
  jq -r --arg id "$task_id" '
    ((.tasks // [])[] | select(.id == $id) | (.scope // "")) // ""
  ' "$TASK_REGISTRY_FILE"
}

is_pool_worktree_path() {
  local path="$1"
  local prefix="${REPO_DIR}/.macc/worktree/"
  case "$path" in
    "$prefix"*) return 0 ;;
    *) return 1 ;;
  esac
}

worktree_metadata_file() {
  local worktree_path="$1"
  echo "${worktree_path}/.macc/worktree.json"
}

worktree_metadata_field() {
  local worktree_path="$1"
  local field="$2"
  local default="${3:-}"
  local metadata
  metadata="$(worktree_metadata_file "$worktree_path")"
  if [[ ! -f "$metadata" ]]; then
    echo "$default"
    return 0
  fi
  jq -r --arg def "$default" ".${field} // \$def" "$metadata" 2>/dev/null || echo "$default"
}

worktree_is_clean() {
  local worktree_path="$1"
  [[ -d "$worktree_path" ]] || return 1
  ! git -C "$worktree_path" status --porcelain | awk 'NF' | grep -q .
}

prepare_reused_worktree() {
  local worktree_path="$1"
  local branch="$2"
  local base_branch="$3"

  if ! git -C "$REPO_DIR" rev-parse --verify "$base_branch" >/dev/null 2>&1; then
    return 1
  fi
  if ! git -C "$worktree_path" checkout "$branch" >/dev/null 2>&1; then
    return 1
  fi
  if ! git -C "$worktree_path" reset --hard "$base_branch" >/dev/null 2>&1; then
    return 1
  fi
  worktree_is_clean "$worktree_path"
}

find_idle_compatible_worktree() {
  local selected_tool="$1"
  local base_branch="$2"
  local requested_scope="${3:-}"

  while IFS= read -r path; do
    [[ -n "$path" ]] || continue
    is_pool_worktree_path "$path" || continue
    worktree_exists_on_disk "$path" || continue
    worktree_in_use "$path" && continue
    worktree_is_clean "$path" || continue

    local wt_tool wt_base wt_scope wt_branch
    wt_tool="$(worktree_metadata_field "$path" "tool" "")"
    wt_base="$(worktree_metadata_field "$path" "base" "")"
    wt_scope="$(worktree_metadata_field "$path" "scope" "")"
    wt_branch="$(worktree_metadata_field "$path" "branch" "")"

    [[ -n "$wt_tool" && "$wt_tool" == "$selected_tool" ]] || continue
    [[ -n "$wt_base" && "$wt_base" == "$base_branch" ]] || continue
    if [[ -n "$requested_scope" ]]; then
      [[ "$wt_scope" == "$requested_scope" ]] || continue
    fi
    if [[ -z "$wt_branch" ]]; then
      wt_branch="$(git -C "$path" rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
    fi
    [[ -n "$wt_branch" ]] || continue
    [[ -f "${path}/.macc/tool.json" ]] || continue

    if ! prepare_reused_worktree "$path" "$wt_branch" "$base_branch"; then
      continue
    fi

    local last_commit
    last_commit="$(git -C "$path" rev-parse HEAD 2>/dev/null || true)"
    [[ -n "$last_commit" ]] || continue
    printf '%s\t%s\t%s\n' "$path" "$wt_branch" "$last_commit"
    return 0
  done < <(
    git -C "$REPO_DIR" worktree list --porcelain \
      | awk '/^worktree /{print substr($0,10)}' \
      | LC_ALL=C sort
  )

  return 1
}

validate_transition() {
  local from="$1"
  local to="$2"
  if ! command -v macc >/dev/null 2>&1; then
    echo "Error: macc is required to validate coordinator transitions from core." >&2
    return 1
  fi
  if macc --cwd "$REPO_DIR" coordinator validate-transition -- --from "$from" --to "$to" >/dev/null 2>&1; then
    return 0
  fi
  echo "Error: invalid transition ${from} -> ${to} (core transition table)" >&2
  return 1
}

failure_kind_to_state() {
  local kind="$1"
  case "$kind" in
    worktree_create|pr_create) echo "blocked" ;;
    ci_red) echo "changes_requested" ;;
    merge_queue_fail|rebase_required) echo "pr_open" ;;
    *) echo "" ;;
  esac
}

apply_transition() {
  local task_id="$1"
  local new_state="$2"
  local pr_url="${3:-}"
  local reviewer="${4:-}"
  local reason="${5:-}"
  local old_state=""
  old_state="$(task_state "$task_id" 2>/dev/null || true)"

  local now tmp
  now="$(now_iso)"
  tmp="$(mktemp)"

  jq --arg id "$task_id" \
     --arg state "$new_state" \
     --arg now "$now" \
     --arg pr_url "$pr_url" \
     --arg reviewer "$reviewer" \
     --arg reason "$reason" \
     '
     def is_active($s):
       ($s == "claimed" or $s == "in_progress" or $s == "pr_open" or $s == "changes_requested" or $s == "queued");

     .tasks |= map(
       if .id == $id then
         .state = $state
         | .updated_at = $now
         | .state_changed_at = $now
         | (if $state == "pr_open" and ($pr_url|length) > 0 then .pr_url = $pr_url else . end)
         | (if $state == "changes_requested" then
              .review.changed = true
              | .review.last_reviewed_at = $now
              | (if ($reviewer|length) > 0 then .review.reviewer = $reviewer else . end)
              | (if ($reason|length) > 0 then .review.reason = $reason else . end)
            else
              .
            end)
         | (if ($state == "merged" or $state == "abandoned") then
              .assignee = null
              | .claimed_at = null
              | .worktree = null
              | .task_runtime = (.task_runtime // {})
              | .task_runtime.status = "idle"
              | .task_runtime.pid = null
              | .task_runtime.started_at = null
              | .task_runtime.current_phase = null
              | .task_runtime.merge_result_pending = false
              | .task_runtime.merge_result_file = null
            elif ($state == "todo") then
              .assignee = null
              | .claimed_at = null
              | .worktree = null
              | .task_runtime = (.task_runtime // {})
              | .task_runtime.status = "idle"
              | .task_runtime.pid = null
              | .task_runtime.started_at = null
              | .task_runtime.current_phase = null
              | .task_runtime.merge_result_pending = false
              | .task_runtime.merge_result_file = null
            else
              .
            end)
       else
         .
       end
     )
     | .resource_locks = (
         reduce ((.tasks // [])[] | select(is_active(.state) and (.worktree != null))) as $t
           ({};
            reduce ($t.exclusive_resources // [])[] as $r
              (.;
               if .[$r] == null then
                 .[$r] = {
                   task_id: $t.id,
                   worktree_path: ($t.worktree.worktree_path // null),
                   locked_at: ($t.claimed_at // $now)
                 }
               else
                 .
               end
              )
           )
       )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"

  mv "$tmp" "$TASK_REGISTRY_FILE"
  emit_event "task_transition" "Task state changed" "$task_id" "$new_state" "from=${old_state} reason=${reason}"
}

set_task_runtime() {
  local task_id="$1"
  local runtime_status="$2"
  local phase="${3:-}"
  local pid="${4:-}"
  local last_error="${5:-}"
  local heartbeat_ts="${6:-}"
  local attempt="${7:-}"
  local now tmp
  now="$(now_iso)"
  tmp="$(mktemp)"

  jq --arg id "$task_id" \
     --arg runtime_status "$runtime_status" \
     --arg phase "$phase" \
     --arg pid "$pid" \
     --arg last_error "$last_error" \
     --arg heartbeat_ts "$heartbeat_ts" \
     --arg attempt "$attempt" \
     --arg now "$now" \
     '
     def ts_to_epoch($v):
       if ($v | type) == "string" and ($v | length) > 0 then
         ($v | fromdateiso8601? // 0)
       else
         0
       end;

     def positive_delta($start; $end):
       if $start > 0 and $end > $start then ($end - $start) else 0 end;

     def is_active_state($s):
       ($s == "claimed" or $s == "in_progress" or $s == "pr_open" or $s == "changes_requested" or $s == "queued");

     def should_close_phase($old_phase; $new_phase; $new_status):
       ($old_phase | length) > 0 and (
         (($new_phase | length) > 0 and $new_phase != $old_phase)
         or ($new_status == "phase_done" or $new_status == "failed" or $new_status == "stale" or $new_status == "idle")
       );

     .tasks |= map(
       if .id == $id then
         .task_runtime = (.task_runtime // {})
         | .task_runtime.metrics = (.task_runtime.metrics // {})
         | .task_runtime.metrics.dev_s = (.task_runtime.metrics.dev_s // 0)
         | .task_runtime.metrics.review_s = (.task_runtime.metrics.review_s // 0)
         | .task_runtime.metrics.integrate_s = (.task_runtime.metrics.integrate_s // 0)
         | .task_runtime.metrics.wait_s = (.task_runtime.metrics.wait_s // 0)
         | .task_runtime.metrics.retries = (.task_runtime.metrics.retries // (.task_runtime.retries // 0))
         | .task_runtime.retries = (.task_runtime.metrics.retries // 0)
         | .task_runtime.slo_warnings = (.task_runtime.slo_warnings // {})
         | (ts_to_epoch($now)) as $now_epoch
         | (.task_runtime.current_phase // "") as $old_phase
         | (.task_runtime.status // "") as $old_status
         | (if ($phase | length) > 0 then $phase else $old_phase end) as $new_phase
         | (.task_runtime.phase_started_at // .task_runtime.started_at // "") as $old_phase_started_at
         | (ts_to_epoch($old_phase_started_at)) as $old_phase_started_epoch
         | (positive_delta($old_phase_started_epoch; $now_epoch)) as $phase_elapsed
         | (if should_close_phase($old_phase; $new_phase; $runtime_status) then
              if $old_phase == "dev" then
                .task_runtime.metrics.dev_s = ((.task_runtime.metrics.dev_s // 0) + $phase_elapsed)
              elif $old_phase == "review" then
                .task_runtime.metrics.review_s = ((.task_runtime.metrics.review_s // 0) + $phase_elapsed)
              elif $old_phase == "integrate" then
                .task_runtime.metrics.integrate_s = ((.task_runtime.metrics.integrate_s // 0) + $phase_elapsed)
              else
                .
              end
            else
              .
            end)
         | (.state // "") as $task_state
         | (.task_runtime.wait_started_at // "") as $old_wait_started_at
         | (ts_to_epoch($old_wait_started_at)) as $old_wait_started_epoch
         | (positive_delta($old_wait_started_epoch; $now_epoch)) as $wait_elapsed
         | (if ($old_wait_started_at | length) > 0 and ($runtime_status == "running" or $runtime_status == "idle" or $runtime_status == "failed" or $runtime_status == "stale") then
              .task_runtime.metrics.wait_s = ((.task_runtime.metrics.wait_s // 0) + $wait_elapsed)
              | .task_runtime.wait_started_at = null
            elif (($old_wait_started_at | length) == 0 and is_active_state($task_state) and ($runtime_status != "running") and ($runtime_status != "idle")) then
              .task_runtime.wait_started_at = $now
            else
              .
            end)
         | .task_runtime.status = $runtime_status
         | (if ($phase|length) > 0 then .task_runtime.current_phase = $phase else . end)
         | (if ($pid|length) > 0 then
              .task_runtime.pid = ($pid|tonumber?)
            elif ($runtime_status == "idle" or $runtime_status == "phase_done" or $runtime_status == "failed" or $runtime_status == "stale") then
              .task_runtime.pid = null
            else
              .
            end)
         | (if ($last_error|length) > 0 then .task_runtime.last_error = $last_error else . end)
         | (if ($heartbeat_ts|length) > 0 then .task_runtime.last_heartbeat = $heartbeat_ts else . end)
         | (if ($attempt|length) > 0 then .task_runtime.attempt = ($attempt|tonumber?) else . end)
         | (if (
              (($phase|length) > 0 and $runtime_status == "running" and ($phase != $old_phase or $old_status != "running"))
              or ($runtime_status == "running" and ((.task_runtime.phase_started_at // "") | length) == 0)
            ) then
              .task_runtime.phase_started_at = $now
            elif ($runtime_status == "idle" or $runtime_status == "phase_done" or $runtime_status == "failed" or $runtime_status == "stale") then
              .task_runtime.phase_started_at = null
            else
              .
            end)
         | (if ($runtime_status == "running" and ((.task_runtime.started_at // "") | length) == 0) then
              .task_runtime.started_at = $now
            else
              .
            end)
       else
         .
       end
     )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"

  mv "$tmp" "$TASK_REGISTRY_FILE"
  check_task_slo_and_warn "$task_id"
}

task_has_pending_merge_result() {
  local task_id="$1"
  jq -r --arg id "$task_id" '
    (.tasks // [])
    | map(select(.id == $id))
    | .[0].task_runtime.merge_result_pending // false
    | if . == true then "true" else "false" end
  ' "$TASK_REGISTRY_FILE" 2>/dev/null || echo "false"
}

set_task_merge_result_pending() {
  local task_id="$1"
  local result_file="$2"
  local pid="${3:-}"
  local now tmp
  now="$(now_iso)"
  tmp="$(mktemp)"
  jq --arg id "$task_id" \
     --arg result_file "$result_file" \
     --arg pid "$pid" \
     --arg now "$now" \
     '
     .tasks |= map(
       if .id == $id then
         .task_runtime = (.task_runtime // {})
         | .task_runtime.merge_result_pending = true
         | .task_runtime.merge_result_file = $result_file
         | .task_runtime.merge_worker_pid = ($pid | tonumber?)
         | .task_runtime.merge_result_started_at = $now
       else
         .
       end
     )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
}

set_task_merge_result_processed() {
  local task_id="$1"
  local result_file="$2"
  local status="$3"
  local rc="$4"
  local now tmp
  now="$(now_iso)"
  tmp="$(mktemp)"
  jq --arg id "$task_id" \
     --arg result_file "$result_file" \
     --arg status "$status" \
     --arg rc "$rc" \
     --arg now "$now" \
     '
     .tasks |= map(
       if .id == $id then
         .task_runtime = (.task_runtime // {})
         | .task_runtime.merge_result_pending = false
         | .task_runtime.merge_result_file = null
         | .task_runtime.merge_worker_pid = null
         | .task_runtime.last_merge_result_file = (if ($result_file|length) > 0 then $result_file else (.task_runtime.last_merge_result_file // null) end)
         | .task_runtime.last_merge_result_status = (if ($status|length) > 0 then $status else (.task_runtime.last_merge_result_status // null) end)
         | .task_runtime.last_merge_result_rc = ($rc | tonumber? // .task_runtime.last_merge_result_rc // null)
         | .task_runtime.last_merge_result_at = $now
       else
         .
       end
     )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
}

increment_task_retries() {
  local task_id="$1"
  local reason="${2:-retry}"
  task_exists "$task_id" || return 0
  local tmp now
  now="$(now_iso)"
  tmp="$(mktemp)"
  jq --arg id "$task_id" \
     --arg now "$now" \
     '
     .tasks |= map(
       if .id == $id then
         .task_runtime = (.task_runtime // {})
         | .task_runtime.metrics = (.task_runtime.metrics // {})
         | .task_runtime.metrics.retries = ((.task_runtime.metrics.retries // (.task_runtime.retries // 0)) + 1)
         | .task_runtime.retries = (.task_runtime.metrics.retries // 0)
       else
         .
       end
     )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
  emit_event "task_retry_count" "Incremented task retry counter" "$task_id" "" "reason=${reason}"
  check_task_slo_and_warn "$task_id"
}

upsert_task_slo_warning() {
  local task_id="$1"
  local metric="$2"
  local threshold="$3"
  local value="$4"
  local suggestion="$5"
  local now tmp
  now="$(now_iso)"
  tmp="$(mktemp)"
  jq --arg id "$task_id" \
     --arg metric "$metric" \
     --arg suggestion "$suggestion" \
     --arg now "$now" \
     --argjson threshold "$threshold" \
     --argjson value "$value" \
     '
     .tasks |= map(
       if .id == $id then
         .task_runtime = (.task_runtime // {})
         | .task_runtime.slo_warnings = (.task_runtime.slo_warnings // {})
         | .task_runtime.slo_warnings[$metric] = {
             metric: $metric,
             threshold: $threshold,
             value: $value,
             warned_at: $now,
             suggestion: $suggestion
           }
       else
         .
       end
     )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
}

maybe_warn_task_metric() {
  local task_id="$1"
  local metric="$2"
  local threshold="$3"
  local suggestion="$4"
  [[ "$threshold" =~ ^[0-9]+$ ]] || return 0
  [[ "$threshold" -gt 0 ]] || return 0

  local value warned
  value="$(jq -r --arg id "$task_id" --arg metric "$metric" '
    (.tasks // [])
    | map(select(.id == $id))
    | .[0]
    | (
        if ($metric == "retries") then
          (.task_runtime.retries // .task_runtime.metrics.retries // 0)
        else
          (.task_runtime.metrics[$metric] // 0)
        end
      )
  ' "$TASK_REGISTRY_FILE" 2>/dev/null || echo 0)"
  warned="$(jq -r --arg id "$task_id" --arg metric "$metric" '
    (.tasks // [])
    | map(select(.id == $id))
    | .[0]
    | ((.task_runtime.slo_warnings // {})[$metric] != null)
  ' "$TASK_REGISTRY_FILE" 2>/dev/null || echo false)"

  [[ "$value" =~ ^[0-9]+$ ]] || value=0
  if [[ "$value" -gt "$threshold" && "$warned" != "true" ]]; then
    local unit
    if [[ "$metric" == "retries" ]]; then
      unit=""
    else
      unit="s"
    fi
    upsert_task_slo_warning "$task_id" "$metric" "$threshold" "$value" "$suggestion"
    emit_event "task_slo_warning" \
      "Task exceeded SLO threshold" \
      "$task_id" \
      "$(task_state "$task_id")" \
      "metric=${metric} value=${value} threshold=${threshold} action=${suggestion}"
    note "SLO warning ${task_id}: ${metric}=${value}${unit} threshold=${threshold}${unit}. Suggestion: ${suggestion}"
  fi
}

check_task_slo_and_warn() {
  local task_id="$1"
  task_exists "$task_id" || return 0
  maybe_warn_task_metric "$task_id" "dev_s" "$SLO_DEV_SECONDS" "Check performer logs and split implementation scope before retrying dev phase."
  maybe_warn_task_metric "$task_id" "review_s" "$SLO_REVIEW_SECONDS" "Run macc coordinator retry-phase --retry-task ${task_id} --retry-phase review after fixing review blockers."
  maybe_warn_task_metric "$task_id" "integrate_s" "$SLO_INTEGRATE_SECONDS" "Inspect merge logs and retry integrate phase or run merge worker manually."
  maybe_warn_task_metric "$task_id" "wait_s" "$SLO_WAIT_SECONDS" "Check queue capacity/dependencies and run macc coordinator reconcile."
  maybe_warn_task_metric "$task_id" "retries" "$SLO_RETRIES_MAX" "Inspect repeated failures and consider manual intervention or task decomposition."
}

pid_is_running() {
  local pid="${1:-}"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  [[ "$pid" -gt 0 ]] || return 1
  kill -0 "$pid" >/dev/null 2>&1
}

is_recent_event_ts() {
  local ts="${1:-}"
  local window="${2:-120}"
  [[ -n "$ts" ]] || return 1
  [[ "$window" =~ ^[0-9]+$ ]] || window=120
  local event_epoch now_epoch age
  event_epoch="$(date -d "$ts" +%s 2>/dev/null || echo 0)"
  [[ "$event_epoch" =~ ^[0-9]+$ ]] || event_epoch=0
  [[ "$event_epoch" -gt 0 ]] || return 1
  now_epoch="$(date +%s)"
  age=$((now_epoch - event_epoch))
  if [[ "$age" -lt 0 ]]; then
    age=0
  fi
  [[ "$age" -le "$window" ]]
}

orphan_runtime_target_state() {
  case "$STALE_HEARTBEAT_ACTION" in
    requeue|retry) echo "todo" ;;
    block|*) echo "blocked" ;;
  esac
}

transition_orphan_task_state() {
  local task_id="$1"
  local current_state="$2"
  local target_state="$3"
  local reason="$4"

  if [[ "$target_state" == "todo" ]]; then
    if [[ "$current_state" == "blocked" ]]; then
      apply_transition "$task_id" "todo" "" "" "$reason"
      return 0
    fi
    # Workflow does not support direct transitions from active states to todo.
    apply_transition "$task_id" "blocked" "" "" "$reason"
    apply_transition "$task_id" "todo" "" "" "$reason"
    return 0
  fi

  apply_transition "$task_id" "$target_state" "" "" "$reason"
}

reconcile_orphan_runtime_tasks() {
  local row task_id state runtime_status pid phase last_commit_created_at
  while IFS= read -r row; do
    [[ -n "${row:-}" ]] || continue
    task_id="$(jq -r '.task_id // ""' <<<"$row" 2>/dev/null || true)"
    state="$(jq -r '.state // ""' <<<"$row" 2>/dev/null || true)"
    runtime_status="$(jq -r '.runtime_status // ""' <<<"$row" 2>/dev/null || true)"
    pid="$(jq -r '.pid // ""' <<<"$row" 2>/dev/null || true)"
    phase="$(jq -r '.phase // ""' <<<"$row" 2>/dev/null || true)"
    last_commit_created_at="$(jq -r '.last_commit_created_at // ""' <<<"$row" 2>/dev/null || true)"
    [[ -n "$task_id" ]] || continue

    case "$runtime_status" in
      phase_done)
        # If a task is still claimed but dev phase already completed and the worker is gone,
        # recover the workflow transition so advance can continue.
        if [[ "$state" == "claimed" ]]; then
          if [[ -z "$pid" || ! "$pid" =~ ^[0-9]+$ || "$pid" -le 0 ]] || ! pid_is_running "$pid"; then
            transition_task_and_hooks "$task_id" "in_progress" "" "" "auto:recover_phase_done"
            set_task_runtime "$task_id" "phase_done" "${phase:-dev}" "" "" "$(now_iso)"
            emit_event "performer_complete" "Recovered completed dev phase from orphan runtime" "$task_id" "in_progress" "pid=${pid:-none}"
            note "Recovered task ${task_id}: claimed + phase_done -> in_progress (pid=${pid:-none})"
          fi
        fi
        ;;
      running|dispatched)
        if [[ "$phase" != "integrate" && ( "$state" == "claimed" || "$state" == "in_progress" ) ]] && is_recent_event_ts "$last_commit_created_at" 180; then
          if [[ "$state" == "claimed" ]]; then
            transition_task_and_hooks "$task_id" "in_progress" "" "" "auto:recover_commit_created"
          fi
          set_task_runtime "$task_id" "phase_done" "${phase:-dev}" "" "" "$(now_iso)"
          emit_event "performer_complete" "Recovered recent commit_created from orphan runtime" "$task_id" "$(task_state "$task_id")" "runtime_status=${runtime_status} commit_created_at=${last_commit_created_at}" "${phase:-dev}" "phase_done"
          note "Recovered task ${task_id}: recent commit_created (${last_commit_created_at}) kept workflow on success path"
          continue
        fi

        # Integrate merge workers can finish quickly; keep queued tasks stable while
        # their persisted merge result is pending processing.
        if [[ "$phase" == "integrate" ]] && [[ "$(task_has_pending_merge_result "$task_id")" == "true" ]]; then
          continue
        fi

        # Runtime marked active but worker PID absent/dead means workflow/runtime divergence.
        # Resolve explicitly according to policy to avoid ghost-active tasks.
        local target_state reason runtime_mark msg
        target_state="$(orphan_runtime_target_state)"
        if [[ "$state" == "queued" && "$phase" == "integrate" && ( -z "$pid" || ! "$pid" =~ ^[0-9]+$ || "$pid" -le 0 ) ]]; then
          # queued/integrate may be waiting for async merge worker scheduling.
          continue
        fi
        if [[ -z "$pid" || ! "$pid" =~ ^[0-9]+$ || "$pid" -le 0 ]]; then
          reason="failure:orphaned_runtime_missing_pid"
          runtime_mark="stale"
          msg="runtime ${runtime_status} has no pid"
          if [[ "$state" == "todo" || "$state" == "merged" || "$state" == "abandoned" ]]; then
            set_task_runtime "$task_id" "$runtime_mark" "$phase" "" "$msg" "$(now_iso)"
            emit_event "task_runtime_orphan" "Orphan runtime without PID handled (runtime-only)" "$task_id" "$state" "runtime_status=${runtime_status} policy=runtime_only" "$phase" "stale"
            note "Orphan runtime handled for ${task_id}: runtime=${runtime_status} missing pid (state=${state}, runtime only)"
            continue
          fi
          transition_orphan_task_state "$task_id" "$state" "$target_state" "$reason"
          set_task_runtime "$task_id" "$runtime_mark" "$phase" "" "$msg" "$(now_iso)"
          emit_event "task_runtime_orphan" "Orphan runtime without PID handled" "$task_id" "$target_state" "runtime_status=${runtime_status} policy=${target_state}" "$phase" "stale"
          note "Orphan runtime handled for ${task_id}: runtime=${runtime_status} missing pid -> ${target_state}"
          continue
        fi
        if ! pid_is_running "$pid"; then
          reason="failure:orphaned_runtime_pid"
          runtime_mark="failed"
          msg="runtime pid ${pid} is not running"
          if [[ "$state" == "todo" || "$state" == "merged" || "$state" == "abandoned" ]]; then
            set_task_runtime "$task_id" "$runtime_mark" "$phase" "" "$msg" "$(now_iso)"
            emit_event "task_runtime_orphan" "Orphan runtime with dead PID handled (runtime-only)" "$task_id" "$state" "pid=${pid} runtime_status=${runtime_status} policy=runtime_only" "$phase" "failed"
            note "Orphan runtime handled for ${task_id}: runtime=${runtime_status} dead pid=${pid} (state=${state}, runtime only)"
            continue
          fi
          transition_orphan_task_state "$task_id" "$state" "$target_state" "$reason"
          set_task_runtime "$task_id" "$runtime_mark" "$phase" "" "$msg" "$(now_iso)"
          emit_event "task_runtime_orphan" "Orphan runtime with dead PID handled" "$task_id" "$target_state" "pid=${pid} runtime_status=${runtime_status} policy=${target_state}" "$phase" "failed"
          note "Orphan runtime handled for ${task_id}: runtime=${runtime_status} dead pid=${pid} -> ${target_state}"
        fi
        ;;
    esac
  done < <(jq -c '
    (.tasks // [])[]
    | {
        task_id: .id,
        state: (.state // ""),
        runtime_status: (.task_runtime.status // ""),
        pid: ((.task_runtime.pid // "")|tostring),
        phase: (.task_runtime.current_phase // ""),
        last_commit_created_at: (.task_runtime.last_commit_created_at // "")
      }
  ' "$TASK_REGISTRY_FILE")
}

event_file_inode() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "0"
    return 0
  fi
  stat -c '%i' "$path" 2>/dev/null \
    || stat -f '%i' "$path" 2>/dev/null \
    || echo "0"
}

read_event_cursor() {
  if [[ -z "${COORD_CURSOR_FILE:-}" || ! -f "${COORD_CURSOR_FILE:-}" ]]; then
    echo "0|0||"
    return 0
  fi
  local row
  row="$(jq -r '[.offset // 0, .inode // 0, .path // "", .last_event_id // ""] | @tsv' "$COORD_CURSOR_FILE" 2>/dev/null || true)"
  if [[ -z "$row" ]]; then
    echo "0|0||"
    return 0
  fi
  local offset inode path last_event_id
  IFS=$'\t' read -r offset inode path last_event_id <<<"$row"
  [[ "$offset" =~ ^[0-9]+$ ]] || offset=0
  [[ "$inode" =~ ^[0-9]+$ ]] || inode=0
  echo "${offset}|${inode}|${path}|${last_event_id}"
}

write_event_cursor() {
  local offset="$1"
  local inode="$2"
  local last_event_id="${3:-}"
  [[ -n "${COORD_CURSOR_FILE:-}" ]] || return 0
  mkdir -p "$(dirname "$COORD_CURSOR_FILE")"
  jq -nc \
    --arg path "$COORD_EVENTS_FILE" \
    --arg updated_at "$(now_iso)" \
    --argjson offset "$offset" \
    --argjson inode "$inode" \
    --arg last_event_id "$last_event_id" \
    '{
      path:$path,
      inode:$inode,
      offset:$offset,
      last_event_id:($last_event_id|select(length>0)),
      updated_at:$updated_at
    }' >"$COORD_CURSOR_FILE"
}

rotate_events_log_if_needed() {
  [[ -n "${COORD_EVENTS_FILE:-}" ]] || return 0
  [[ -f "$COORD_EVENTS_FILE" ]] || return 0
  [[ "$EVENT_LOG_MAX_BYTES" =~ ^[0-9]+$ ]] || return 0
  if [[ "$EVENT_LOG_MAX_BYTES" -le 0 ]]; then
    return 0
  fi
  local size
  size="$(wc -c <"$COORD_EVENTS_FILE" 2>/dev/null || echo 0)"
  [[ "$size" =~ ^[0-9]+$ ]] || size=0
  if [[ "$size" -lt "$EVENT_LOG_MAX_BYTES" ]]; then
    return 0
  fi

  local ts rotated
  ts="$(date -u +"%Y%m%dT%H%M%SZ")"
  rotated="${COORD_LOG_DIR}/events-${ts}.jsonl"
  mv "$COORD_EVENTS_FILE" "$rotated"
  : >"$COORD_EVENTS_FILE"
  emit_event "events_rotated" "Rotated coordinator events log" "" "" "from_size=${size} path=${rotated}"

  if [[ "$EVENT_LOG_KEEP_FILES" =~ ^[0-9]+$ ]] && [[ "$EVENT_LOG_KEEP_FILES" -ge 0 ]]; then
    mapfile -t rotated_files < <(ls -1t "${COORD_LOG_DIR}"/events-*.jsonl 2>/dev/null || true)
    local idx
    for idx in "${!rotated_files[@]}"; do
      if [[ "$idx" -ge "$EVENT_LOG_KEEP_FILES" ]]; then
        rm -f "${rotated_files[$idx]}"
      fi
    done
  fi
}

compact_processed_event_ids_if_needed() {
  [[ "$PROCESSED_EVENT_IDS_MAX" =~ ^[0-9]+$ ]] || return 0
  if [[ "$PROCESSED_EVENT_IDS_MAX" -le 0 ]]; then
    return 0
  fi
  local count
  count="$(jq -r '(.processed_event_ids // {}) | length' "$TASK_REGISTRY_FILE" 2>/dev/null || echo 0)"
  [[ "$count" =~ ^[0-9]+$ ]] || count=0
  if [[ "$count" -le "$PROCESSED_EVENT_IDS_MAX" ]]; then
    return 0
  fi
  local tmp now
  now="$(now_iso)"
  tmp="$(mktemp)"
  jq --arg now "$now" '
    . as $root
    | (.tasks // [])
      | map(.task_runtime.last_event_id // "")
      | map(select(length > 0))
      | unique as $keep
    | .processed_event_ids = (
        reduce $keep[] as $id ({};
          if (($root.processed_event_ids // {})[$id] // false) then
            .[$id] = true
          else
            .
          end
        )
      )
    | .updated_at = $now
  ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
  emit_event "events_compacted" "Compacted processed event dedup map" "" "" "previous_count=${count}"
}

apply_runtime_event() {
  local event_id="$1"
  local task_id="$2"
  local seq="$3"
  local event_type="$4"
  local phase="$5"
  local status="$6"
  local ts="$7"
  local payload_json="$8"
  local source="$9"

  [[ -n "$task_id" ]] || return 0
  task_exists "$task_id" || return 0

  local runtime_status
  case "$status" in
    started|dispatched) runtime_status="dispatched" ;;
    running|progress|heartbeat) runtime_status="running" ;;
    done|phase_done) runtime_status="phase_done" ;;
    failed|error) runtime_status="failed" ;;
    stale) runtime_status="stale" ;;
    paused) runtime_status="paused" ;;
    *) runtime_status="running" ;;
  esac

  local tmp now
  now="$(now_iso)"
  tmp="$(mktemp)"
  jq --arg task_id "$task_id" \
     --arg event_id "$event_id" \
     --argjson seq "$seq" \
     --arg runtime_status "$runtime_status" \
     --arg phase "$phase" \
     --arg event_type "$event_type" \
     --arg status "$status" \
     --arg ts "$ts" \
    --argjson payload "$payload_json" \
     --arg source "$source" \
     --arg now "$now" \
     '
     def payload_obj:
       if ($payload | type) == "object" then
         if (($payload.value? // null) | type) == "string" then
           (($payload.value | fromjson?) // $payload)
         else
           $payload
         end
       elif ($payload | type) == "string" then
         (($payload | fromjson?) // {})
       else
         {}
       end;

     def payload_error:
       (payload_obj.error // payload_obj.reason // payload_obj.message // payload_obj.matched_output // "");

     def payload_attempt:
       (payload_obj.attempt // null);

     def heartbeat_ts:
       if ($status == "running" or $status == "heartbeat" or $event_type == "heartbeat") then
         (if ($ts|length) > 0 then $ts else $now end)
       else
         null
       end;

     . as $root
     | (.processed_event_ids // {}) as $seen
     | if ($seen[$event_id] // false) then
         .
       else
         .processed_event_ids = $seen
         | .processed_event_ids[$event_id] = true
         | .tasks |= map(
             if .id == $task_id then
               .task_runtime = (.task_runtime // {})
               | (if (
                    (.task_runtime.last_seq_source // "") == $source
                    and $seq > 0
                    and ((.task_runtime.last_seq // -1) >= 0)
                    and ((.task_runtime.last_seq // -1) >= $seq)
                  ) then
                    .
                  else
                    .task_runtime.last_seq = $seq
                    | .task_runtime.last_event_id = $event_id
                    | .task_runtime.last_seq_source = $source
                    | (if $event_type == "commit_created" then
                         .task_runtime.last_commit_created_at = (if ($ts|length) > 0 then $ts else $now end)
                       else
                         .
                       end)
                    | .task_runtime.status = $runtime_status
                    | (if ($phase|length) > 0 then .task_runtime.current_phase = $phase else . end)
                    | (if heartbeat_ts != null then .task_runtime.last_heartbeat = heartbeat_ts else . end)
                    | (if payload_attempt != null then .task_runtime.attempt = payload_attempt else . end)
                    | .task_runtime.metrics = (.task_runtime.metrics // {})
                    | .task_runtime.metrics.retries = (.task_runtime.metrics.retries // (.task_runtime.retries // 0))
                    | (if payload_attempt != null and payload_attempt > 1 then
                         .task_runtime.metrics.retries = (
                           if (.task_runtime.metrics.retries // 0) > (payload_attempt - 1) then
                             (.task_runtime.metrics.retries // 0)
                           else
                             (payload_attempt - 1)
                           end
                         )
                       else
                         .
                       end)
                    | .task_runtime.retries = (.task_runtime.metrics.retries // 0)
                    | (if ($runtime_status == "failed" and (payload_error|length) > 0) then
                         .task_runtime.last_error = payload_error
                       else
                         .
                       end)
                    | (if ($runtime_status == "running" and ((.task_runtime.started_at // "")|length) == 0) then
                         .task_runtime.started_at = (if ($ts|length) > 0 then $ts else $now end)
                       else
                         .
                       end)
                  end)
             else
               .
             end
           )
         | .updated_at = $now
       end
     ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
}

consume_runtime_events_once() {
  [[ -n "${COORD_EVENTS_FILE:-}" ]] || return 0
  rotate_events_log_if_needed
  [[ -f "$COORD_EVENTS_FILE" ]] || return 0

  local cursor_meta cursor_offset cursor_inode cursor_path cursor_last_event_id
  cursor_meta="$(read_event_cursor)"
  IFS='|' read -r cursor_offset cursor_inode cursor_path cursor_last_event_id <<<"$cursor_meta"

  local file_size file_inode
  file_size="$(wc -c <"$COORD_EVENTS_FILE" 2>/dev/null || echo 0)"
  file_inode="$(event_file_inode "$COORD_EVENTS_FILE")"
  [[ "$cursor_offset" =~ ^[0-9]+$ ]] || cursor_offset=0
  [[ "$cursor_inode" =~ ^[0-9]+$ ]] || cursor_inode=0
  [[ "$file_size" =~ ^[0-9]+$ ]] || file_size=0
  [[ "$file_inode" =~ ^[0-9]+$ ]] || file_inode=0

  if [[ "$cursor_path" != "$COORD_EVENTS_FILE" || "$cursor_inode" != "$file_inode" || "$cursor_offset" -gt "$file_size" ]]; then
    cursor_offset=0
  fi
  if [[ "$cursor_offset" -eq "$file_size" ]]; then
    return 0
  fi

  local lines
  lines="$(tail -c +"$((cursor_offset + 1))" "$COORD_EVENTS_FILE" 2>/dev/null || true)"
  local last_processed_event_id=""
  while IFS= read -r line; do
    [[ -n "${line// }" ]] || continue
    local event_id task_id seq event_type phase status ts payload_json source
    event_id="$(jq -r '.event_id // ""' <<<"$line" 2>/dev/null || true)"
    task_id="$(jq -r '.task_id // ""' <<<"$line" 2>/dev/null || true)"
    seq="$(jq -r '.seq // 0' <<<"$line" 2>/dev/null || echo 0)"
    event_type="$(jq -r '.type // .event // ""' <<<"$line" 2>/dev/null || true)"
    phase="$(jq -r '.phase // ""' <<<"$line" 2>/dev/null || true)"
    status="$(jq -r '.status // .state // ""' <<<"$line" 2>/dev/null || true)"
    ts="$(jq -r '.ts // ""' <<<"$line" 2>/dev/null || true)"
    source="$(jq -r '.source // ""' <<<"$line" 2>/dev/null || true)"
    payload_json="$(jq -c '.payload // {}' <<<"$line" 2>/dev/null || echo '{}')"

    [[ "$seq" =~ ^[0-9]+$ ]] || seq=0
    [[ -n "$event_id" ]] || continue
    case "$event_type" in
      started|progress|phase_result|commit_created|review_done|integrate_done|failed|heartbeat) ;;
      *) continue ;;
    esac
    apply_runtime_event "$event_id" "$task_id" "$seq" "$event_type" "$phase" "$status" "$ts" "$payload_json" "$source"
    last_processed_event_id="$event_id"

    if [[ "$status" == "failed" && "$source" == performer:* ]]; then
      local current
      current="$(task_state "$task_id" 2>/dev/null || true)"
      if [[ -n "$current" && "$current" != "blocked" && "$current" != "merged" && "$current" != "abandoned" ]]; then
        apply_transition "$task_id" "blocked" "" "" "failure:performer_event"
      fi
    fi
  done <<<"$lines"

  write_event_cursor "$file_size" "$file_inode" "$last_processed_event_id"
  compact_processed_event_ids_if_needed
}

cleanup_stale_tasks() {
  local now_epoch task_id state claimed_at threshold
  now_epoch="$(date +%s)"

  while IFS=$'\t' read -r task_id state claimed_at; do
    [[ -n "$task_id" && -n "$claimed_at" ]] || continue
    local claimed_epoch
    claimed_epoch="$(date -d "$claimed_at" +%s 2>/dev/null || echo 0)"
    [[ "$claimed_epoch" -gt 0 ]] || continue
    local age=$((now_epoch - claimed_epoch))

    case "$state" in
      claimed) threshold="$STALE_CLAIMED_SECONDS" ;;
      in_progress) threshold="$STALE_IN_PROGRESS_SECONDS" ;;
      changes_requested) threshold="$STALE_CHANGES_REQUESTED_SECONDS" ;;
      *) threshold="0" ;;
    esac

    if [[ "$threshold" -gt 0 && "$age" -ge "$threshold" ]]; then
      local target_state
      case "$STALE_ACTION" in
        abandon) target_state="abandoned" ;;
        todo) target_state="todo" ;;
        blocked) target_state="blocked" ;;
        *) target_state="abandoned" ;;
      esac
      apply_transition "$task_id" "$target_state" "" "" "stale"
      echo "Auto-${target_state} stale task: ${task_id} (state=${state}, age=${age}s)"
    fi
  done < <(jq -r '
    (.tasks // [])[]
    | select(.state == "claimed" or .state == "in_progress" or .state == "changes_requested")
    | [.id, .state, (.claimed_at // "")] | @tsv
  ' "$TASK_REGISTRY_FILE")

  if [[ "$STALE_HEARTBEAT_SECONDS" -gt 0 ]]; then
    while IFS=$'\t' read -r task_id task_state runtime_status current_phase last_heartbeat started_at; do
      [[ -n "$task_id" ]] || continue
      if [[ "$task_state" == "queued" && "$current_phase" == "integrate" ]]; then
        continue
      fi
      local ref_ts heartbeat_epoch age
      ref_ts="$last_heartbeat"
      [[ -n "$ref_ts" ]] || ref_ts="$started_at"
      [[ -n "$ref_ts" ]] || continue
      heartbeat_epoch="$(date -d "$ref_ts" +%s 2>/dev/null || echo 0)"
      [[ "$heartbeat_epoch" -gt 0 ]] || continue
      age=$((now_epoch - heartbeat_epoch))
      [[ "$age" -ge "$STALE_HEARTBEAT_SECONDS" ]] || continue

      case "$STALE_HEARTBEAT_ACTION" in
        retry)
          increment_task_retries "$task_id" "stale_heartbeat_retry"
          set_task_runtime "$task_id" "dispatched" "" "" "stale heartbeat (${age}s)" "$(now_iso)"
          emit_event "task_runtime_retry" "Runtime stale heartbeat; retry requested" "$task_id" "dispatched" "age=${age}s"
          ;;
        requeue)
          apply_transition "$task_id" "todo" "" "" "stale_heartbeat_requeue"
          set_task_runtime "$task_id" "stale" "" "" "stale heartbeat (${age}s)" "$(now_iso)"
          emit_event "task_runtime_requeue" "Runtime stale heartbeat; task requeued" "$task_id" "todo" "age=${age}s"
          ;;
        block|*)
          apply_transition "$task_id" "blocked" "" "" "stale_heartbeat"
          set_task_runtime "$task_id" "stale" "" "" "stale heartbeat (${age}s)" "$(now_iso)"
          emit_event "task_runtime_stale" "Runtime stale heartbeat; task blocked" "$task_id" "blocked" "age=${age}s"
          ;;
      esac
      echo "Stale heartbeat handled: ${task_id} (state=${task_state}, runtime=${runtime_status}, phase=${current_phase}, age=${age}s)"
    done < <(jq -r '
      (.tasks // [])[]
      | select((.task_runtime.status // "") == "running" or (.task_runtime.status // "") == "dispatched")
      | [
          .id,
          (.state // ""),
          (.task_runtime.status // ""),
          (.task_runtime.current_phase // ""),
          (.task_runtime.last_heartbeat // ""),
          (.task_runtime.started_at // "")
        ] | @tsv
    ' "$TASK_REGISTRY_FILE")
  fi
}

select_next_ready_task_tsv() {
  jq -r \
    --argjson enabled_tools "$ENABLED_TOOLS_JSON" \
    --argjson tool_priority "$TOOL_PRIORITY_JSON" \
    --argjson tool_caps "$MAX_PARALLEL_PER_TOOL_JSON" \
    --argjson tool_specializations "$TOOL_SPECIALIZATIONS_JSON" \
    --argjson max_parallel "$MAX_PARALLEL" \
    --arg default_tool "$DEFAULT_TOOL" \
    --arg default_base_branch "$DEFAULT_BASE_BRANCH" \
    '
    def pkey:
      if type == "number" then .
      elif type == "string" then
        (ascii_downcase) as $v
        | if $v == "p0" then 0
          elif $v == "p1" then 1
          elif $v == "p2" then 2
          elif $v == "p3" then 3
          elif $v == "p4" then 4
          elif ($v | test("^[0-9]+$")) then ($v | tonumber)
          else 99
          end
      else 99
      end;

    def is_active_state($s):
      ($s == "claimed" or $s == "in_progress" or $s == "pr_open" or $s == "changes_requested" or $s == "queued");

    def active_count($root; $tool):
      (($root.tasks // [])
        | map(select(is_active_state(.state // "") and ((.tool // "") == $tool)))
        | length);

    def active_global_count($root):
      (($root.tasks // [])
        | map(select(is_active_state(.state // "")))
        | length);

    def cap_ok($root; $tool):
      (($tool_caps[$tool] // null) as $cap
        | if $cap == null then true
          elif ($cap | type) == "number" then active_count($root; $tool) < $cap
          elif ($cap | type) == "string" then
            (($cap | tonumber?) as $n | if $n == null then true else active_count($root; $tool) < $n end)
          else true
          end);

    def in_enabled($tool):
      if ($enabled_tools | length) == 0 then true else (($enabled_tools | index($tool)) != null) end;

    def category_tools($task):
      (($task.category // "") as $cat | ($tool_specializations[$cat] // []));

    def preference_list($task):
      (
        if (category_tools($task) | length) > 0 then category_tools($task)
        elif (($task.tool // "") | tostring | length) > 0 then [($task.tool | tostring)]
        elif ($tool_priority | length) > 0 then $tool_priority
        else [] end
      )
      | map(tostring)
      | map(select(length > 0));

    def fallback_pool($task):
      (
        if ($enabled_tools | length) > 0 then $enabled_tools
        else (
          preference_list($task)
          + $tool_priority
          + [($task.tool // ""), $default_tool]
          + (($tool_specializations | to_entries | map(.value) | add) // [])
        )
        end
      )
      | map(tostring)
      | map(select(length > 0))
      | unique;

    def candidate_tools($root; $task):
      (
        preference_list($task)
        + fallback_pool($task)
      )
      | unique
      | map(select(in_enabled(.)))
      | map(select(cap_ok($root; .)));

    def pick_tool($root; $task):
      (candidate_tools($root; $task)) as $cands
      | if ($cands | length) == 0 then ""
        else (
          $cands
          | map({
              tool: .,
              rank: (preference_list($task) | index(.) // 999),
              load: active_count($root; .)
            })
          | sort_by(.rank, .load, .tool)
          | .[0].tool
        )
        end;

    . as $root
    | [($root.tasks // [])[]
        | select(($max_parallel | tonumber) <= 0 or (active_global_count($root) < ($max_parallel | tonumber)))
        | select((.state // "todo") == "todo" and (.worktree == null))
        | . as $t
        | select((($t.dependencies // []) | all(
            . as $dep
            | (((($root.tasks // []) | map(select(.id == ($dep|tostring)) | .state) | .[0]) // "") == "merged")
          )))
        | select((($t.exclusive_resources // []) | all(
            . as $r
            | (($root.resource_locks[$r].task_id // "") as $owner | ($owner == "" or $owner == $t.id))
          )))
        | .selected_tool = pick_tool($root; $t)
        | select((.selected_tool // "") != "")
      ]
    | sort_by((.priority | pkey), (.category // "zzz"), .id)
    | .[0]
    | if . == null then "" else [(.id // ""), (.title // ""), (.selected_tool // ""), (.base_branch // $default_base_branch)] | @tsv end
  ' "$TASK_REGISTRY_FILE"
}

create_macc_worktree() {
  local task_id="$1"
  local title="$2"
  local tool="$3"
  local requested_base_branch="$4"
  local base_branch
  base_branch="$(resolve_base_branch "$requested_base_branch")"

  local slug
  slug="$(safe_slug "${task_id}-${title}")"

  local output
  if ! output="$(
    macc --cwd "$REPO_DIR" worktree create "$slug" \
      --tool "$tool" \
      --count 1 \
      --base "$base_branch" 2>&1
  )"; then
    echo "Error: macc worktree create failed for task ${task_id}" >&2
    echo "$output" >&2
    return 1
  fi

  local worktree_path branch
  worktree_path="$(printf '%s\n' "$output" | sed -n 's/.* path=\(.*\)$/\1/p' | tail -n1)"
  branch="$(printf '%s\n' "$output" | sed -n 's/.* branch=\([^ ]*\).*/\1/p' | tail -n1)"

  if [[ -z "$worktree_path" ]]; then
    worktree_path="$(git -C "$REPO_DIR" worktree list --porcelain \
      | awk '/^worktree /{print substr($0,10)}' \
      | grep "/.macc/worktree/" \
      | tail -n1 || true)"
  fi

  [[ -n "$worktree_path" ]] || {
    echo "Error: could not resolve created worktree path for task ${task_id}" >&2
    return 1
  }

  if [[ -z "$branch" ]]; then
    branch="$(git -C "$worktree_path" rev-parse --abbrev-ref HEAD)"
  fi

  local last_commit
  last_commit="$(git -C "$worktree_path" rev-parse HEAD)"

  printf '%s\t%s\t%s\n' "$worktree_path" "$branch" "$last_commit"
}

write_worktree_prd() {
  local worktree_path="$1"
  local task_id="$2"
  local out_path="${worktree_path}/worktree.prd.json"
  local tmp
  tmp="$(mktemp)"

  jq --arg id "$task_id" '
    . as $p
    | {
        lot: ($p.lot // ""),
        version: ($p.version // ""),
        generated_at: ($p.generated_at // ""),
        timezone: ($p.timezone // "UTC"),
        priority_mapping: ($p.priority_mapping // {}),
        assumptions: ($p.assumptions // []),
        tasks: (($p.tasks // []) | map(select((.id|tostring) == $id)))
      }
  ' "$PRD_FILE" >"$tmp"

  if ! jq -e '.tasks | length == 1' "$tmp" >/dev/null 2>&1; then
    rm -f "$tmp"
    echo "Error: could not extract unique task '$task_id' from PRD for worktree.prd.json" >&2
    return 1
  fi

  mv "$tmp" "$out_path"
}

mark_task_claimed() {
  local task_id="$1"
  local worktree_path="$2"
  local branch="$3"
  local base_branch="$4"
  local last_commit="$5"
  local selected_tool="$6"

  local now session_id tmp
  now="$(now_iso)"
  session_id="${AGENT_ID}-${task_id}-$(date -u +%Y%m%dT%H%M%SZ)"
  tmp="$(mktemp)"

  jq --arg id "$task_id" \
     --arg agent "$AGENT_ID" \
     --arg now "$now" \
     --arg wp "$worktree_path" \
     --arg branch "$branch" \
     --arg base "$base_branch" \
     --arg commit "$last_commit" \
     --arg session "$session_id" \
     --arg selected_tool "$selected_tool" \
     '
     . as $root
     | .tasks |= map(
       if .id == $id then
         .state = "claimed"
         | .tool = $selected_tool
         | .assignee = $agent
         | .claimed_at = $now
         | .worktree = {
             worktree_path: $wp,
             branch: $branch,
             base_branch: $base,
             last_commit: $commit,
             session_id: $session
           }
         | .task_runtime = (.task_runtime // {})
         | .task_runtime.status = "dispatched"
         | .task_runtime.current_phase = "dev"
         | .task_runtime.started_at = $now
         | .task_runtime.last_heartbeat = $now
         | .task_runtime.last_error = null
         | .task_runtime.pid = null
         | .task_runtime.merge_result_pending = false
         | .task_runtime.merge_result_file = null
         | .task_runtime.merge_worker_pid = null
       else
         .
       end
     )
     | .resource_locks |= (
         . as $locks
         | ((($root.tasks // []) | map(select(.id == $id) | .exclusive_resources // []) | .[0]) // []) as $res
         | reduce $res[] as $r ($locks; .[$r] = {
             task_id: $id,
             worktree_path: $wp,
             locked_at: $now
           })
       )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"

  mv "$tmp" "$TASK_REGISTRY_FILE"
}

invoke_performer() {
  local task_id="$1"
  local worktree_path="$2"
  local tool="$3"
  local root_config="${REPO_DIR}/.macc/macc.yaml"
  local worktree_config="${worktree_path}/.macc/macc.yaml"

  if [[ -f "$root_config" ]]; then
    cp "$root_config" "$worktree_config"
  fi

  if ! macc --cwd "$REPO_DIR" worktree apply "$worktree_path" >/dev/null 2>&1; then
    echo "Error: failed to apply config before performer run (task=${task_id}, tool=${tool}, worktree=${worktree_path})" >&2
    return 1
  fi

  COORD_EVENTS_FILE="$COORD_EVENTS_FILE" \
  MACC_EVENT_SOURCE="performer:${tool}:${task_id}:$(date +%s%N)" \
  MACC_EVENT_TASK_ID="$task_id" \
  macc --cwd "$REPO_DIR" worktree run "$worktree_path"
}

resolve_tool_runner() {
  local worktree_path="$1"
  local tool="${2:-}"
  if [[ -n "$tool" ]]; then
    local explicit_runner="${worktree_path}/.macc/automation/runners/${tool}.performer.sh"
    if [[ -x "$explicit_runner" ]]; then
      echo "$explicit_runner"
      return 0
    fi
  fi
  local tool_json="${worktree_path}/.macc/tool.json"
  [[ -f "$tool_json" ]] || {
    echo ""
    return 0
  }
  local runner
  runner="$(jq -r '.performer.runner // ""' "$tool_json")"
  [[ -n "$runner" && "$runner" != "null" ]] || {
    echo ""
    return 0
  }
  if [[ "$runner" = /* ]]; then
    echo "$runner"
  else
    echo "${REPO_DIR}/${runner}"
  fi
}

build_phase_prompt() {
  local mode="$1"
  local task_id="$2"
  local tool="$3"

  local task_json
  task_json="$(jq -c --arg id "$task_id" '(.tasks // []) | map(select(.id == $id)) | .[0] // {}' "$TASK_REGISTRY_FILE")"

  cat <<PROMPT
You are the assigned ${tool} performer running inside a MACC worktree.

Mode: ${mode}
Task ID: ${task_id}

Task registry entry (JSON):
${task_json}

Instructions:
1) Execute the ${mode} phase only.
2) Keep changes minimal and focused on this task.
3) Update code/tests/docs as needed for this phase.
4) Do not modify task registry state directly.
PROMPT
}

invoke_tool_phase_runner() {
  local mode="$1"
  local task_id="$2"
  local worktree_path="$3"
  local tool="$4"
  local tool_json="${worktree_path}/.macc/tool.json"

  [[ -f "$tool_json" ]] || {
    echo "Error: missing tool.json for ${mode} fallback: $tool_json" >&2
    return 1
  }

  local runner
  runner="$(resolve_tool_runner "$worktree_path" "$tool")"
  [[ -n "$runner" && -x "$runner" ]] || {
    echo "Error: tool phase runner missing or not executable for ${mode}: ${runner}" >&2
    return 1
  }

  local prompt_file
  prompt_file="$(mktemp)"
  build_phase_prompt "$mode" "$task_id" "$tool" >"$prompt_file"

  COORD_EVENTS_FILE="$COORD_EVENTS_FILE" \
  MACC_EVENT_SOURCE="performer:${tool}:${task_id}:$(date +%s%N)" \
  MACC_EVENT_TASK_ID="$task_id" \
  "$runner" \
    --prompt-file "$prompt_file" \
    --tool-json "$tool_json" \
    --repo "$REPO_DIR" \
    --worktree "$worktree_path" \
    --task-id "$task_id" \
    --attempt 1 \
    --max-attempts "$PHASE_RUNNER_MAX_ATTEMPTS"
  local status=$?
  rm -f "$prompt_file"
  return "$status"
}

maybe_run_phase_hook() {
  local task_id="$1"
  local new_state="$2"
  local worktree_path="$3"
  local tool="$4"

  case "$new_state" in
    pr_open)
      invoke_tool_phase_runner review "$task_id" "$worktree_path" "$tool" || return 1
      ;;
    changes_requested)
      invoke_tool_phase_runner fix "$task_id" "$worktree_path" "$tool" || return 1
      ;;
    queued)
      invoke_tool_phase_runner integrate "$task_id" "$worktree_path" "$tool" || return 1
      ;;
  esac
}

worktree_for_task() {
  local task_id="$1"
  jq -r --arg id "$task_id" '(.tasks // [])[] | select(.id == $id) | .worktree.worktree_path // ""' "$TASK_REGISTRY_FILE"
}

tool_for_task() {
  local task_id="$1"
  jq -r --arg id "$task_id" '(.tasks // [])[] | select(.id == $id) | .tool // ""' "$TASK_REGISTRY_FILE"
}

coordinator_phase_tool_for_task() {
  local task_id="$1"
  if [[ -n "$COORDINATOR_TOOL" ]]; then
    echo "$COORDINATOR_TOOL"
    return 0
  fi
  jq -r --arg id "$task_id" '
    (.tasks // [])[] | select(.id == $id) | (.coordinator_tool // .tool // "")
  ' "$TASK_REGISTRY_FILE"
}

transition_task_and_hooks() {
  local task_id="$1"
  local new_state="$2"
  local pr_url="${3:-}"
  local reviewer="${4:-}"
  local reason="${5:-}"
  local current wt tool

  current="$(task_state "$task_id")"
  validate_transition "$current" "$new_state" || return 1
  apply_transition "$task_id" "$new_state" "$pr_url" "$reviewer" "$reason"
  case "$new_state" in
    claimed)
      set_task_runtime "$task_id" "dispatched" "dev" "" "" "$(now_iso)"
      ;;
    in_progress)
      set_task_runtime "$task_id" "running" "dev" "" "" "$(now_iso)"
      ;;
    pr_open)
      set_task_runtime "$task_id" "running" "review" "" "" "$(now_iso)"
      ;;
    changes_requested)
      set_task_runtime "$task_id" "running" "fix" "" "" "$(now_iso)"
      ;;
    queued)
      set_task_runtime "$task_id" "phase_done" "integrate" "" "" "$(now_iso)"
      ;;
    merged)
      set_task_runtime "$task_id" "idle" "" "" "" "$(now_iso)"
      ;;
    blocked|abandoned)
      set_task_runtime "$task_id" "failed" "" "" "${reason:-workflow transition to ${new_state}}" "$(now_iso)"
      ;;
    todo)
      set_task_runtime "$task_id" "idle" "" "" "" "$(now_iso)"
      ;;
  esac

  if [[ "$new_state" == "pr_open" || "$new_state" == "changes_requested" || "$new_state" == "queued" ]]; then
    wt="$(worktree_for_task "$task_id")"
    tool="$(coordinator_phase_tool_for_task "$task_id")"
    if [[ -n "$wt" && -n "$tool" ]]; then
      if ! maybe_run_phase_hook "$task_id" "$new_state" "$wt" "$tool"; then
        note "Warning: phase hook failed for ${task_id} (${new_state}); continuing state machine."
        emit_event "phase_result" "Coordinator phase hook failed" "$task_id" "$new_state" "tool=${tool}" "$new_state" "failed"
      else
        case "$new_state" in
          pr_open)
            emit_event "review_done" "Review phase completed" "$task_id" "$new_state" "tool=${tool}" "review" "done"
            ;;
          changes_requested)
            emit_event "phase_result" "Fix phase completed" "$task_id" "$new_state" "tool=${tool}" "fix" "done"
            ;;
          queued)
            emit_event "integrate_done" "Integrate phase completed" "$task_id" "$new_state" "tool=${tool}" "integrate" "done"
            ;;
        esac
      fi
    fi
  fi
}

task_worktree_field() {
  local task_id="$1"
  local field="$2"
  jq -r --arg id "$task_id" "(.tasks // [])[] | select(.id == \$id) | (.worktree.${field} // \"\")" "$TASK_REGISTRY_FILE"
}

task_pr_url() {
  local task_id="$1"
  jq -r --arg id "$task_id" '(.tasks // [])[] | select(.id == $id) | (.pr_url // "")' "$TASK_REGISTRY_FILE"
}

run_vcs_hook_json() {
  local mode="$1"
  local task_id="$2"
  local worktree_path="$3"
  local tool="$4"
  local branch="$5"
  local base_branch="$6"
  local pr_url="$7"

  [[ -n "$COORDINATOR_VCS_HOOK" ]] || return 2
  [[ -x "$COORDINATOR_VCS_HOOK" ]] || {
    echo "Error: COORDINATOR_VCS_HOOK is not executable: $COORDINATOR_VCS_HOOK" >&2
    return 1
  }

  local output status
  set +e
  output="$(
    MACC_MODE="$mode" \
    MACC_TASK_ID="$task_id" \
    MACC_TASK_WORKTREE="$worktree_path" \
    MACC_TASK_TOOL="$tool" \
    MACC_TASK_BRANCH="$branch" \
    MACC_TASK_BASE_BRANCH="$base_branch" \
    MACC_TASK_PR_URL="$pr_url" \
    REPO_DIR="$REPO_DIR" \
    "$COORDINATOR_VCS_HOOK" "$mode"
  )"
  status=$?
  set -e

  if [[ "$status" -ne 0 ]]; then
    echo "Error: VCS hook failed (mode=${mode}, task=${task_id}, status=${status})" >&2
    return 1
  fi

  if [[ -z "$output" ]]; then
    echo "{}"
    return 0
  fi

  if ! jq -e 'type == "object"' <<<"$output" >/dev/null 2>&1; then
    echo "Error: VCS hook returned invalid JSON object (mode=${mode}, task=${task_id})" >&2
    return 1
  fi
  printf '%s\n' "$output"
}

local_merge_branch_into_base() {
  local task_id="$1"
  local branch="$2"
  local base_branch="$3"
  LOCAL_MERGE_LAST_ERROR=""
  local safe_task ts ns result_file rc status error suggestion report_file
  safe_task="$(printf '%s' "$task_id" | tr '[:space:]' '-' | tr -cd '[:alnum:]_.-')"
  [[ -n "$safe_task" ]] || safe_task="task"
  ts="$(date -u +"%Y%m%dT%H%M%SZ")"
  ns="$(date +%s%N 2>/dev/null || printf '%s000000000' "$(date +%s)")"
  result_file="${COORD_LOG_DIR}/merge-result-${safe_task}-${ts}-${ns}-pid${BASHPID}.json"

  set +e
  "$COORDINATOR_MERGE_WORKER" \
    --repo "$REPO_DIR" \
    --task-id "$task_id" \
    --branch "$branch" \
    --base-branch "$base_branch" \
    --log-dir "$COORD_LOG_DIR" \
    --result-file "$result_file" \
    --allow-ai-fix "$COORDINATOR_MERGE_AI_FIX" \
    --merge-fix-hook "$COORDINATOR_MERGE_FIX_HOOK" >/dev/null 2>&1
  rc=$?
  set -e

  status="$(jq -r '.status // ""' "$result_file" 2>/dev/null || true)"
  error="$(jq -r '.error // ""' "$result_file" 2>/dev/null || true)"
  suggestion="$(jq -r '.suggestion // ""' "$result_file" 2>/dev/null || true)"
  report_file="$(jq -r '.report_file // ""' "$result_file" 2>/dev/null || true)"

  if [[ "$rc" -eq 0 && "$status" == "success" ]]; then
    return 0
  fi

  LOCAL_MERGE_LAST_ERROR="${error:-failure:local_merge}"
  if [[ -n "$suggestion" ]]; then
    LOCAL_MERGE_LAST_ERROR="${LOCAL_MERGE_LAST_ERROR} suggestion=\"${suggestion}\""
  fi
  if [[ -n "$report_file" ]]; then
    LOCAL_MERGE_LAST_ERROR="${LOCAL_MERGE_LAST_ERROR} report=\"${report_file}\""
  fi
  return 1
}

advance_active_tasks() {
  reconcile_orphan_runtime_tasks
  local progressed="false"
  for _ in $(seq 1 16); do
    local pass_progressed="false"
    while IFS=$'\t' read -r task_id state; do
      [[ -n "$task_id" && -n "$state" ]] || continue
      local worktree_path tool branch base_branch pr_url
      worktree_path="$(worktree_for_task "$task_id")"
      tool="$(coordinator_phase_tool_for_task "$task_id")"
      branch="$(task_worktree_field "$task_id" "branch")"
      base_branch="$(task_worktree_field "$task_id" "base_branch")"
      pr_url="$(task_pr_url "$task_id")"

      case "$state" in
        in_progress)
          local pr_create_json hook_status new_pr_url
          if pr_create_json="$(run_vcs_hook_json "pr_create" "$task_id" "$worktree_path" "$tool" "$branch" "$base_branch" "$pr_url")"; then
            new_pr_url="$(jq -r '.pr_url // ""' <<<"$pr_create_json")"
            if [[ -n "$new_pr_url" ]]; then
              transition_task_and_hooks "$task_id" "pr_open" "$new_pr_url" "" "auto:pr_create"
              note "Advance: ${task_id} in_progress -> pr_open"
              pass_progressed="true"
              progressed="true"
            fi
          else
            hook_status=$?
            if [[ "$hook_status" -eq 2 ]]; then
              transition_task_and_hooks "$task_id" "pr_open" "local://${branch}" "" "auto:local_pr"
              note "Advance: ${task_id} in_progress -> pr_open (local)"
              pass_progressed="true"
              progressed="true"
            else
              transition_task_and_hooks "$task_id" "blocked" "" "" "failure:pr_create"
              note "Blocked task due to PR creation failure: ${task_id}"
              pass_progressed="true"
              progressed="true"
            fi
          fi
          ;;
        pr_open)
          local review_json ci_json review_hook_status ci_hook_status decision ci_status reviewer reason
          decision="approved"
          reviewer=""
          reason=""
          ci_status="green"

          if review_json="$(run_vcs_hook_json "review_status" "$task_id" "$worktree_path" "$tool" "$branch" "$base_branch" "$pr_url")"; then
            decision="$(jq -r '.decision // "approved"' <<<"$review_json")"
            reviewer="$(jq -r '.reviewer // ""' <<<"$review_json")"
            reason="$(jq -r '.reason // ""' <<<"$review_json")"
          else
            review_hook_status=$?
            if [[ "$review_hook_status" -ne 2 ]]; then
              transition_task_and_hooks "$task_id" "blocked" "" "" "failure:review_hook"
              note "Blocked task due to review hook failure: ${task_id}"
              pass_progressed="true"
              progressed="true"
              continue
            fi
          fi

          if ci_json="$(run_vcs_hook_json "ci_status" "$task_id" "$worktree_path" "$tool" "$branch" "$base_branch" "$pr_url")"; then
            ci_status="$(jq -r '.status // "green"' <<<"$ci_json")"
            local ci_reason
            ci_reason="$(jq -r '.reason // ""' <<<"$ci_json")"
            if [[ -n "$ci_reason" ]]; then
              reason="$ci_reason"
            fi
          else
            ci_hook_status=$?
            if [[ "$ci_hook_status" -ne 2 ]]; then
              transition_task_and_hooks "$task_id" "blocked" "" "" "failure:ci_hook"
              note "Blocked task due to CI hook failure: ${task_id}"
              pass_progressed="true"
              progressed="true"
              continue
            fi
          fi

          case "$decision:$ci_status" in
            changes_requested:*)
              transition_task_and_hooks "$task_id" "changes_requested" "" "$reviewer" "${reason:-auto:review_changes_requested}"
              note "Advance: ${task_id} pr_open -> changes_requested"
              pass_progressed="true"
              progressed="true"
              ;;
            *:red|*:failed|*:failure)
              transition_task_and_hooks "$task_id" "changes_requested" "" "$reviewer" "${reason:-failure:ci_red}"
              note "Advance: ${task_id} pr_open -> changes_requested (CI red)"
              pass_progressed="true"
              progressed="true"
              ;;
            *:pending|*:running)
              ;;
            *)
              transition_task_and_hooks "$task_id" "queued" "" "$reviewer" "${reason:-auto:review_ci_ok}"
              note "Advance: ${task_id} pr_open -> queued"
              pass_progressed="true"
              progressed="true"
              ;;
          esac
          ;;
        changes_requested)
          # Keep flow moving: after fix phase, reopen PR for review.
          transition_task_and_hooks "$task_id" "pr_open" "$pr_url" "" "auto:reopen_after_fix"
          note "Advance: ${task_id} changes_requested -> pr_open"
          pass_progressed="true"
          progressed="true"
          ;;
        queued)
          local queue_json merge_json queue_hook_status merge_hook_status queue_status merge_status
          queue_status="queued"
          merge_status="pending"

          if queue_json="$(run_vcs_hook_json "queue_status" "$task_id" "$worktree_path" "$tool" "$branch" "$base_branch" "$pr_url")"; then
            queue_status="$(jq -r '.status // "queued"' <<<"$queue_json")"
          else
            queue_hook_status=$?
            if [[ "$queue_hook_status" -ne 2 ]]; then
              transition_task_and_hooks "$task_id" "blocked" "" "" "failure:merge_queue_hook"
              note "Blocked task due to merge queue hook failure: ${task_id}"
              pass_progressed="true"
              progressed="true"
              continue
            fi
          fi

          case "$queue_status" in
            failed|error)
              transition_task_and_hooks "$task_id" "pr_open" "$pr_url" "" "failure:merge_queue_fail"
              note "Advance: ${task_id} queued -> pr_open (queue failure)"
              pass_progressed="true"
              progressed="true"
              continue
              ;;
            pending|queued|running)
              ;;
            ready|ok|merged)
              ;;
          esac

          if merge_json="$(run_vcs_hook_json "merge_status" "$task_id" "$worktree_path" "$tool" "$branch" "$base_branch" "$pr_url")"; then
            merge_status="$(jq -r '.status // "pending"' <<<"$merge_json")"
            case "$merge_status" in
              merged|ok)
                transition_task_and_hooks "$task_id" "merged" "$pr_url" "" "auto:merged"
                note "Advance: ${task_id} queued -> merged"
                pass_progressed="true"
                progressed="true"
                ;;
              failed|error)
                transition_task_and_hooks "$task_id" "pr_open" "$pr_url" "" "failure:merge_queue_fail"
                note "Advance: ${task_id} queued -> pr_open (merge failed)"
                pass_progressed="true"
                progressed="true"
                ;;
              *)
                ;;
            esac
          else
            merge_hook_status=$?
            if [[ "$merge_hook_status" -eq 2 ]]; then
              if is_truthy "$COORDINATOR_AUTOMERGE"; then
                if [[ "$COORD_COMMAND_NAME" == "run" ]]; then
                  if merge_job_pending_for_task "$task_id"; then
                    :
                  elif [[ "$(task_has_pending_merge_result "$task_id")" == "true" ]]; then
                    :
                  elif merge_job_is_running_for_task "$task_id"; then
                    :
                  elif [[ "$(run_loop_merge_job_count)" -gt 0 ]]; then
                    :
                  else
                    start_local_merge_worker_async "$task_id" "$branch" "$base_branch" "$pr_url"
                    pass_progressed="true"
                    progressed="true"
                  fi
                else
                  if local_merge_branch_into_base "$task_id" "$branch" "$base_branch"; then
                    transition_task_and_hooks "$task_id" "merged" "$pr_url" "" "auto:local_merge"
                    note "Advance: ${task_id} queued -> merged (local merge)"
                    pass_progressed="true"
                    progressed="true"
                  else
                    local merge_error
                    merge_error="${LOCAL_MERGE_LAST_ERROR:-failure:local_merge}"
                    transition_task_and_hooks "$task_id" "blocked" "$pr_url" "" "failure:local_merge"
                    set_task_runtime "$task_id" "failed" "integrate" "" "$merge_error" "$(now_iso)"
                    emit_event "local_merge_failed" "Local merge failed" "$task_id" "blocked" "$merge_error" "integrate" "failed"
                    note "Blocked task due to local merge failure: ${task_id}"
                    if [[ -n "${LOCAL_MERGE_LAST_ERROR:-}" ]]; then
                      note "Local merge detail (${task_id}): ${LOCAL_MERGE_LAST_ERROR}"
                    fi
                    pass_progressed="true"
                    progressed="true"
                  fi
                fi
              fi
            else
              transition_task_and_hooks "$task_id" "blocked" "$pr_url" "" "failure:merge_hook"
              note "Blocked task due to merge hook failure: ${task_id}"
              pass_progressed="true"
              progressed="true"
            fi
          fi
          ;;
      esac
    done < <(jq -r '
      (.tasks // [])[]
      | select(.state == "in_progress" or .state == "pr_open" or .state == "changes_requested" or .state == "queued")
      | [.id, .state] | @tsv
    ' "$TASK_REGISTRY_FILE")
    if [[ "$pass_progressed" != "true" ]]; then
      break
    fi
  done

  if [[ "$progressed" == "true" ]]; then
    note "Advance complete: state transitions applied."
  else
    note "Advance complete: no transitions."
  fi
}

handle_performer_completion() {
  local task_id="$1"
  local tool="$2"
  local rc="$3"
  if [[ "$rc" -ne 0 ]]; then
    local structured_error existing_error runtime_error
    structured_error="$(jq -r --arg id "$task_id" '
      def payload_obj:
        if (.payload | type) == "object" then
          if ((.payload.value? // null) | type) == "string" then
            ((.payload.value | fromjson?) // .payload)
          else
            .payload
          end
        elif (.payload | type) == "string" then
          ((.payload | fromjson?) // {})
        else
          {}
        end;

      select(.task_id == $id)
      | (.type // .event // "") as $event
      | select($event == "failed" or ($event == "phase_result" and ((.status // .state // "") == "failed")))
      | payload_obj as $p
      | ($p.error // $p.reason // $p.message // "") as $reason
      | ($p.matched_output // "") as $match
      | if ($reason | length) > 0 and ($match | length) > 0 and $match != $reason then
          ($reason + " | " + $match)
        elif ($reason | length) > 0 then
          $reason
        elif ($match | length) > 0 then
          $match
        else
          empty
        end
    ' "$COORD_EVENTS_FILE" 2>/dev/null | tail -n1 || true)"
    existing_error="$(jq -r --arg id "$task_id" '
      (.tasks // [])
      | map(select(.id == $id))
      | .[0].task_runtime.last_error // ""
    ' "$TASK_REGISTRY_FILE" 2>/dev/null || true)"

    if [[ -n "$structured_error" ]]; then
      runtime_error="$structured_error"
    elif [[ -n "$existing_error" ]]; then
      runtime_error="$existing_error"
      if [[ "$runtime_error" != *"exit status"* ]]; then
        runtime_error="${runtime_error} (exit status ${rc})"
      fi
    else
      runtime_error="performer exited with status ${rc}"
    fi
    set_task_runtime "$task_id" "failed" "dev" "" "$runtime_error" "$(now_iso)"
    apply_transition "$task_id" "blocked" "" "" "failure:performer"
    note "Blocked task due to performer failure: ${task_id}"
    if [[ -n "$runtime_error" ]]; then
      note "Performer failure detail (${task_id}): ${runtime_error}"
    fi
    emit_event "task_blocked" "Performer failed" "$task_id" "blocked"
  else
    transition_task_and_hooks "$task_id" "in_progress" "" "" "auto:performer_complete"
    set_task_runtime "$task_id" "phase_done" "dev" "" "" "$(now_iso)"
    note "Performer complete: ${task_id} (${tool})"
    emit_event "performer_complete" "Performer completed task phase" "$task_id" "in_progress"
  fi
}

merge_job_is_running_for_task() {
  local task_id="$1"
  local entry pid queued_task
  for entry in "${RUN_LOOP_MERGE_JOBS[@]}"; do
    IFS='|' read -r pid queued_task _ _ _ _ <<<"$entry"
    if [[ "$queued_task" == "$task_id" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      return 0
    fi
  done
  return 1
}

merge_job_pending_for_task() {
  local task_id="$1"
  local entry queued_task
  for entry in "${RUN_LOOP_MERGE_JOBS[@]}"; do
    IFS='|' read -r _ queued_task _ _ _ _ <<<"$entry"
    if [[ "$queued_task" == "$task_id" ]]; then
      return 0
    fi
  done
  return 1
}

start_local_merge_worker_async() {
  local task_id="$1"
  local branch="$2"
  local base_branch="$3"
  local pr_url="$4"
  local safe_task ts ns result_file pid

  safe_task="$(printf '%s' "$task_id" | tr '[:space:]' '-' | tr -cd '[:alnum:]_.-')"
  [[ -n "$safe_task" ]] || safe_task="task"
  ts="$(date -u +"%Y%m%dT%H%M%SZ")"
  ns="$(date +%s%N 2>/dev/null || printf '%s000000000' "$(date +%s)")"
  result_file="${COORD_LOG_DIR}/merge-result-${safe_task}-${ts}-${ns}-pid${BASHPID}.json"

  lock_release
  "$COORDINATOR_MERGE_WORKER" \
    --repo "$REPO_DIR" \
    --task-id "$task_id" \
    --branch "$branch" \
    --base-branch "$base_branch" \
    --log-dir "$COORD_LOG_DIR" \
    --result-file "$result_file" \
    --allow-ai-fix "$COORDINATOR_MERGE_AI_FIX" \
    --merge-fix-hook "$COORDINATOR_MERGE_FIX_HOOK" >/dev/null 2>&1 &
  pid=$!
  lock_acquire
  ensure_registry_valid

  RUN_LOOP_MERGE_JOBS+=("${pid}|${task_id}|${branch}|${base_branch}|${pr_url}|${result_file}")
  set_task_merge_result_pending "$task_id" "$result_file" "$pid"
  set_task_runtime "$task_id" "running" "integrate" "$pid" "" "$(now_iso)"
  emit_event "merge_worker_started" "Started local merge worker" "$task_id" "queued" "pid=${pid} branch=${branch} base=${base_branch}" "integrate" "started"
  note "Merge worker started: ${task_id} (pid=${pid}, branch=${branch}, base=${base_branch})"
}

handle_merge_worker_completion() {
  local task_id="$1"
  local branch="$2"
  local base_branch="$3"
  local pr_url="$4"
  local result_file="$5"
  local rc="$6"
  local status error suggestion report_file

  status="$(jq -r '.status // ""' "$result_file" 2>/dev/null || true)"
  error="$(jq -r '.error // ""' "$result_file" 2>/dev/null || true)"
  suggestion="$(jq -r '.suggestion // ""' "$result_file" 2>/dev/null || true)"
  report_file="$(jq -r '.report_file // ""' "$result_file" 2>/dev/null || true)"

  if [[ "$rc" -eq 0 && "$status" == "success" ]]; then
    local current
    current="$(task_state "$task_id")"
    if [[ "$current" == "merged" ]]; then
      :
    elif [[ "$current" == "blocked" ]]; then
      # Merge already succeeded; keep transition idempotent even if orphan handling blocked first.
      apply_transition "$task_id" "merged" "$pr_url" "" "auto:local_merge_worker_recovered"
    elif ! transition_task_and_hooks "$task_id" "merged" "$pr_url" "" "auto:local_merge_worker"; then
      current="$(task_state "$task_id")"
      if [[ "$current" != "merged" ]]; then
        apply_transition "$task_id" "merged" "$pr_url" "" "auto:local_merge_worker_forced"
      fi
    fi
    set_task_runtime "$task_id" "idle" "" "" "" "$(now_iso)"
    set_task_merge_result_processed "$task_id" "$result_file" "$status" "$rc"
    emit_event "merge_worker_complete" "Local merge worker completed successfully" "$task_id" "merged" "branch=${branch} base=${base_branch}" "integrate" "done"
    note "Advance: ${task_id} queued -> merged (local merge worker)"
    return 0
  fi

  local merge_error
  merge_error="${error:-failure:local_merge branch=${branch} base=${base_branch}}"
  if [[ -n "$suggestion" ]]; then
    merge_error="${merge_error} suggestion=\"${suggestion}\""
  fi
  if [[ -n "$report_file" ]]; then
    merge_error="${merge_error} report=\"${report_file}\""
  fi
  transition_task_and_hooks "$task_id" "blocked" "$pr_url" "" "failure:local_merge"
  set_task_runtime "$task_id" "failed" "integrate" "" "$merge_error" "$(now_iso)"
  set_task_merge_result_processed "$task_id" "$result_file" "${status:-failed}" "$rc"
  emit_event "local_merge_failed" "Local merge worker failed" "$task_id" "blocked" "$merge_error" "integrate" "failed"
  note "Blocked task due to local merge failure: ${task_id}"
  if [[ -n "$report_file" ]]; then
    note "Local merge report (${task_id}): ${report_file}"
  fi
}

monitor_persisted_merge_results_once() {
  local task_id branch base_branch pr_url result_file status inferred_rc
  while IFS=$'\t' read -r task_id branch base_branch pr_url result_file; do
    [[ -n "$task_id" ]] || continue
    merge_job_pending_for_task "$task_id" && continue
    [[ -n "$result_file" && -f "$result_file" ]] || continue
    status="$(jq -r '.status // ""' "$result_file" 2>/dev/null || true)"
    inferred_rc=1
    if [[ "$status" == "success" ]]; then
      inferred_rc=0
    fi
    handle_merge_worker_completion "$task_id" "$branch" "$base_branch" "$pr_url" "$result_file" "$inferred_rc"
  done < <(jq -r '
    (.tasks // [])[]
    | select((.task_runtime.merge_result_pending // false) == true)
    | [
        .id,
        (.worktree.branch // ""),
        (.worktree.base_branch // ""),
        (.pr_url // ""),
        (.task_runtime.merge_result_file // "")
      ]
    | @tsv
  ' "$TASK_REGISTRY_FILE")
}

retry_failed_phase() {
  local task_id="$1"
  local phase="$2"
  local skip_phase="${3:-false}"

  task_exists "$task_id" || {
    echo "Error: task not found in registry: $task_id" >&2
    return 1
  }

  case "$phase" in
    dev|review|fix|integrate) ;;
    *)
      echo "Error: unsupported retry phase '${phase}' (expected: dev|review|fix|integrate)" >&2
      return 1
      ;;
  esac

  if [[ "$skip_phase" == "true" ]]; then
    transition_task_and_hooks "$task_id" "todo" "" "" "manual:skip_phase:${phase}"
    set_task_runtime "$task_id" "idle" "" "" "" "$(now_iso)"
    emit_event "phase_skipped" "Skipped failed phase" "$task_id" "todo" "phase=${phase}" "$phase" "skipped"
    note "Skipped phase '${phase}' for task ${task_id}; task moved back to todo."
    return 0
  fi

  local worktree_path tool current rc performer_pid
  worktree_path="$(worktree_for_task "$task_id")"
  tool="$(coordinator_phase_tool_for_task "$task_id")"
  current="$(task_state "$task_id")"

  [[ -n "$worktree_path" && -d "$worktree_path" ]] || {
    echo "Error: retry-phase requires an existing worktree for task ${task_id}" >&2
    return 1
  }
  [[ -n "$tool" ]] || {
    echo "Error: retry-phase could not resolve tool for task ${task_id}" >&2
    return 1
  }

  increment_task_retries "$task_id" "manual_retry_phase:${phase}"

  case "$phase" in
    dev)
      if [[ "$current" == "blocked" || "$current" == "todo" ]]; then
        transition_task_and_hooks "$task_id" "claimed" "" "" "manual:retry_phase:dev"
      fi
      set_task_runtime "$task_id" "running" "dev" "" "" "$(now_iso)"
      emit_event "phase_retry" "Retrying failed phase" "$task_id" "$current" "phase=dev tool=${tool}" "dev" "started"
      lock_release
      invoke_performer "$task_id" "$worktree_path" "$tool" &
      performer_pid=$!
      rc=0
      wait "$performer_pid" || rc=$?
      lock_acquire
      ensure_registry_valid
      handle_performer_completion "$task_id" "$tool" "$rc"
      [[ "$rc" -eq 0 ]]
      return
      ;;
    review)
      emit_event "phase_retry" "Retrying failed phase" "$task_id" "$current" "phase=review tool=${tool}" "review" "started"
      if invoke_tool_phase_runner review "$task_id" "$worktree_path" "$tool"; then
        local target="pr_open"
        current="$(task_state "$task_id")"
        validate_transition "$current" "$target"
        apply_transition "$task_id" "$target" "" "" "manual:retry_phase:review"
        set_task_runtime "$task_id" "running" "review" "" "" "$(now_iso)"
        emit_event "review_done" "Review phase retried successfully" "$task_id" "pr_open" "tool=${tool}" "review" "done"
        note "Retried review phase for task ${task_id}."
      else
        set_task_runtime "$task_id" "failed" "review" "" "retry review phase failed" "$(now_iso)"
        emit_event "phase_result" "Retry review phase failed" "$task_id" "$current" "tool=${tool}" "review" "failed"
        return 1
      fi
      ;;
    fix)
      emit_event "phase_retry" "Retrying failed phase" "$task_id" "$current" "phase=fix tool=${tool}" "fix" "started"
      if invoke_tool_phase_runner fix "$task_id" "$worktree_path" "$tool"; then
        local target="changes_requested"
        current="$(task_state "$task_id")"
        validate_transition "$current" "$target"
        apply_transition "$task_id" "$target" "" "" "manual:retry_phase:fix"
        set_task_runtime "$task_id" "running" "fix" "" "" "$(now_iso)"
        emit_event "phase_result" "Fix phase retried successfully" "$task_id" "changes_requested" "tool=${tool}" "fix" "done"
        note "Retried fix phase for task ${task_id}."
      else
        set_task_runtime "$task_id" "failed" "fix" "" "retry fix phase failed" "$(now_iso)"
        emit_event "phase_result" "Retry fix phase failed" "$task_id" "$current" "tool=${tool}" "fix" "failed"
        return 1
      fi
      ;;
    integrate)
      emit_event "phase_retry" "Retrying failed phase" "$task_id" "$current" "phase=integrate tool=${tool}" "integrate" "started"
      if invoke_tool_phase_runner integrate "$task_id" "$worktree_path" "$tool"; then
        local target="queued"
        current="$(task_state "$task_id")"
        validate_transition "$current" "$target"
        apply_transition "$task_id" "$target" "" "" "manual:retry_phase:integrate"
        set_task_runtime "$task_id" "running" "integrate" "" "" "$(now_iso)"
        emit_event "integrate_done" "Integrate phase retried successfully" "$task_id" "queued" "tool=${tool}" "integrate" "done"
        note "Retried integrate phase for task ${task_id}."
      else
        set_task_runtime "$task_id" "failed" "integrate" "" "retry integrate phase failed" "$(now_iso)"
        emit_event "phase_result" "Retry integrate phase failed" "$task_id" "$current" "tool=${tool}" "integrate" "failed"
        return 1
      fi
      ;;
  esac
}

registry_counts_tsv() {
  jq -r '
    def is_active($s):
      ($s == "claimed" or $s == "in_progress" or $s == "pr_open" or $s == "changes_requested" or $s == "queued");

    [(.tasks // [])[] | (.state // "todo")] as $states
    | [
        ($states | length),
        ($states | map(select(. == "todo")) | length),
        ($states | map(select(is_active(.))) | length),
        ($states | map(select(. == "blocked")) | length),
        ($states | map(select(. == "merged")) | length)
      ]
    | @tsv
  ' "$TASK_REGISTRY_FILE"
}

run_loop_active_job_count() {
  local count=0
  local entry pid task_id wt tool
  for entry in "${RUN_LOOP_ACTIVE_JOBS[@]}"; do
    IFS='|' read -r pid task_id wt tool <<<"$entry"
    if kill -0 "$pid" >/dev/null 2>&1; then
      count=$((count + 1))
    fi
  done
  echo "$count"
}

run_loop_merge_job_count() {
  local count=0
  local entry pid
  for entry in "${RUN_LOOP_MERGE_JOBS[@]}"; do
    IFS='|' read -r pid _ _ _ _ _ <<<"$entry"
    if kill -0 "$pid" >/dev/null 2>&1; then
      count=$((count + 1))
    fi
  done
  echo "$count"
}

monitor_run_loop_jobs_once() {
  local -a remaining=()
  local entry pid task_id wt tool rc
  for entry in "${RUN_LOOP_ACTIVE_JOBS[@]}"; do
    IFS='|' read -r pid task_id wt tool <<<"$entry"
    if kill -0 "$pid" >/dev/null 2>&1; then
      remaining+=("$entry")
      continue
    fi
    rc=0
    wait "$pid" || rc=$?
    handle_performer_completion "$task_id" "$tool" "$rc"
  done
  RUN_LOOP_ACTIVE_JOBS=("${remaining[@]}")
}

monitor_run_loop_merge_jobs_once() {
  local -a remaining=()
  local entry pid task_id branch base_branch pr_url result_file rc
  for entry in "${RUN_LOOP_MERGE_JOBS[@]}"; do
    IFS='|' read -r pid task_id branch base_branch pr_url result_file <<<"$entry"
    if kill -0 "$pid" >/dev/null 2>&1; then
      remaining+=("$entry")
      continue
    fi
    rc=0
    wait "$pid" || rc=$?
    handle_merge_worker_completion "$task_id" "$branch" "$base_branch" "$pr_url" "$result_file" "$rc"
  done
  RUN_LOOP_MERGE_JOBS=("${remaining[@]}")
  monitor_persisted_merge_results_once
}

stop_run_loop_jobs() {
  local entry pid task_id wt tool
  for entry in "${RUN_LOOP_ACTIVE_JOBS[@]}"; do
    IFS='|' read -r pid task_id wt tool <<<"$entry"
    kill "$pid" >/dev/null 2>&1 || true
  done
  RUN_LOOP_ACTIVE_JOBS=()
  for entry in "${RUN_LOOP_MERGE_JOBS[@]}"; do
    IFS='|' read -r pid _ _ _ _ _ <<<"$entry"
    kill "$pid" >/dev/null 2>&1 || true
  done
  RUN_LOOP_MERGE_JOBS=()
}

dispatch_ready_tasks_nonblocking() {
  local dispatched=0
  local ready_count
  ready_count="$(jq -r '
    . as $root
    | [($root.tasks // [])[]
        | select((.state // "todo") == "todo" and (.worktree == null))
        | . as $t
        | select((($t.dependencies // []) | all(
            . as $dep
            | (((($root.tasks // []) | map(select(.id == ($dep|tostring)) | .state) | .[0]) // "") == "merged")
          )))
        | select((($t.exclusive_resources // []) | all(
            . as $r
            | (($root.resource_locks[$r].task_id // "") as $owner | ($owner == "" or $owner == $t.id))
          )))
      ]
    | length
  ' "$TASK_REGISTRY_FILE")"
  note "Dispatch config: MAX_DISPATCH=${MAX_DISPATCH} MAX_PARALLEL=${MAX_PARALLEL} MAX_PARALLEL_PER_TOOL_JSON=${MAX_PARALLEL_PER_TOOL_JSON}"
  note "Dispatch ready tasks: ${ready_count}"

  while true; do
    local active_jobs
    active_jobs="$(run_loop_active_job_count)"
    if [[ "$MAX_PARALLEL" -gt 0 && "$active_jobs" -ge "$MAX_PARALLEL" ]]; then
      break
    fi
    if [[ "$MAX_DISPATCH" -gt 0 && "$dispatched" -ge "$MAX_DISPATCH" ]]; then
      break
    fi

    local selected_row selected title tool base_branch
    selected_row="$(select_next_ready_task_tsv)"
    [[ -n "$selected_row" ]] || break
    IFS=$'\t' read -r selected title tool base_branch <<<"$selected_row"
    [[ -n "$selected" ]] || break

    local created task_scope_value dispatch_kind
    dispatch_kind="create"
    task_scope_value="$(task_scope "$selected")"

    if is_truthy "$WORKTREE_POOL_MODE"; then
      created="$(find_idle_compatible_worktree "$tool" "$base_branch" "$task_scope_value" || true)"
      if [[ -n "$created" ]]; then
        dispatch_kind="reuse"
      fi
    fi

    if [[ -z "$created" ]]; then
      if ! created="$(create_macc_worktree "$selected" "$title" "$tool" "$base_branch")"; then
        apply_transition "$selected" "blocked" "" "" "failure:worktree_create"
        note "Blocked task due to worktree create failure: ${selected}"
        continue
      fi
    fi
    local worktree_path branch last_commit
    IFS=$'\t' read -r worktree_path branch last_commit <<<"$created"

    if worktree_in_use "$worktree_path"; then
      local other
      other="$(worktree_task_id "$worktree_path")"
      note "Skip: worktree already in use by task ${other}: ${worktree_path}"
      continue
    fi
    if ! worktree_exists_on_disk "$worktree_path"; then
      echo "Error: worktree path not a git worktree on disk: ${worktree_path}" >&2
      apply_transition "$selected" "blocked" "" "" "failure:worktree_create"
      continue
    fi

    write_worktree_prd "$worktree_path" "$selected"
    mark_task_claimed "$selected" "$worktree_path" "$branch" "$base_branch" "$last_commit" "$tool"

    lock_release
    invoke_performer "$selected" "$worktree_path" "$tool" &
    local performer_pid=$!
    lock_acquire
    ensure_registry_valid
    set_task_runtime "$selected" "running" "dev" "$performer_pid" "" "$(now_iso)"
    RUN_LOOP_ACTIVE_JOBS+=("${performer_pid}|${selected}|${worktree_path}|${tool}")

    note "Dispatched: ${selected}"
    note "  tool:      ${tool}"
    note "  branch:    ${branch}"
    note "  worktree:  ${worktree_path}"
    note "  source:    ${dispatch_kind}"
    note "  mode:      async (pid=${performer_pid})"
    note ""
    emit_event "task_dispatched" "Task dispatched" "$selected" "claimed" "tool=${tool} worktree=${worktree_path}" "dev" "started"

    dispatched=$((dispatched + 1))
  done

  note "Dispatch complete. Tasks dispatched: ${dispatched}"
  emit_event "dispatch_complete" "Dispatch finished" "" "" "dispatched=${dispatched}"
}

run_control_plane() {
  local started_epoch cycle no_progress previous_counts
  started_epoch="$(date +%s)"
  cycle=0
  no_progress=0
  previous_counts=""

  while true; do
    cycle=$((cycle + 1))
    lock_acquire
    ensure_registry_file
    ensure_registry_valid
    sync_registry_from_prd
    ensure_registry_valid

    consume_runtime_events_once
    cleanup_stale_tasks
    monitor_run_loop_jobs_once
    monitor_run_loop_merge_jobs_once
    reconcile_orphan_runtime_tasks
    dispatch_ready_tasks_nonblocking
    advance_active_tasks
    reconcile_registry
    cleanup_stale_tasks
    consume_runtime_events_once
    monitor_run_loop_jobs_once
    monitor_run_loop_merge_jobs_once

    local counts total todo active blocked merged
    counts="$(registry_counts_tsv)"
    IFS=$'\t' read -r total todo active blocked merged <<<"$counts"
    lock_release

    note "Coordinator cycle ${cycle}: total=${total} todo=${todo} active=${active} blocked=${blocked} merged=${merged}"

    if [[ "$todo" -eq 0 && "$active" -eq 0 ]]; then
      if [[ "$blocked" -gt 0 ]]; then
        echo "Error: Validation error: Coordinator run finished with blocked tasks: ${blocked}. Run \`macc coordinator status\`, then \`macc coordinator unlock --all\`, and inspect logs with \`macc logs tail --component coordinator\`." >&2
        return 1
      fi
      note "Coordinator run complete."
      return 0
    fi

    if [[ "$active" -gt 0 ]]; then
      no_progress=0
    elif [[ "$counts" == "$previous_counts" ]]; then
      no_progress=$((no_progress + 1))
    else
      no_progress=0
    fi
    previous_counts="$counts"

    if [[ "$no_progress" -ge 2 ]]; then
      stop_run_loop_jobs
      echo "Error: Validation error: Coordinator made no progress for ${no_progress} cycles (todo=${todo}, active=${active}, blocked=${blocked}). Run \`macc coordinator status\`, then \`macc coordinator unlock --all\`, and inspect logs with \`macc logs tail --component coordinator\`." >&2
      return 1
    fi

    if [[ "$TIMEOUT_SECONDS" -gt 0 ]]; then
      local now elapsed
      now="$(date +%s)"
      elapsed=$((now - started_epoch))
      if [[ "$elapsed" -ge "$TIMEOUT_SECONDS" ]]; then
        stop_run_loop_jobs
        echo "Error: Validation error: Coordinator run timed out after ${TIMEOUT_SECONDS} seconds. Run \`macc coordinator status\` and \`macc logs tail --component coordinator\`." >&2
        return 1
      fi
    fi

    sleep 0.2
  done
}

dispatch_ready_tasks() {
  local dispatched=0
  local -a jobs=()
  local ready_count
  ready_count="$(jq -r '
    . as $root
    | [($root.tasks // [])[]
        | select((.state // "todo") == "todo" and (.worktree == null))
        | . as $t
        | select((($t.dependencies // []) | all(
            . as $dep
            | (((($root.tasks // []) | map(select(.id == ($dep|tostring)) | .state) | .[0]) // "") == "merged")
          )))
        | select((($t.exclusive_resources // []) | all(
            . as $r
            | (($root.resource_locks[$r].task_id // "") as $owner | ($owner == "" or $owner == $t.id))
          )))
      ]
    | length
  ' "$TASK_REGISTRY_FILE")"
  note "Dispatch config: MAX_DISPATCH=${MAX_DISPATCH} MAX_PARALLEL=${MAX_PARALLEL} MAX_PARALLEL_PER_TOOL_JSON=${MAX_PARALLEL_PER_TOOL_JSON}"
  note "Dispatch ready tasks: ${ready_count}"

  while true; do
    if [[ "$MAX_PARALLEL" -gt 0 && "${#jobs[@]}" -ge "$MAX_PARALLEL" ]]; then
      local completed_idx="" completed_task="" completed_tool="" completed_rc=0
      lock_release
      while [[ -z "$completed_idx" ]]; do
        local i
        for i in "${!jobs[@]}"; do
          local pid task_id wt tool
          IFS='|' read -r pid task_id wt tool <<<"${jobs[$i]}"
          if ! kill -0 "$pid" >/dev/null 2>&1; then
            completed_idx="$i"
            completed_task="$task_id"
            completed_tool="$tool"
            wait "$pid" || completed_rc=$?
            break
          fi
        done
        if [[ -z "$completed_idx" ]]; then
          lock_acquire
          consume_runtime_events_once
          lock_release
          sleep 0.1
        fi
      done
      lock_acquire
      ensure_registry_valid
      consume_runtime_events_once

      unset "jobs[$completed_idx]"
      jobs=("${jobs[@]}")

      handle_performer_completion "$completed_task" "$completed_tool" "$completed_rc"
    fi

    local selected_row selected title tool base_branch
    selected_row="$(select_next_ready_task_tsv)"
    [[ -n "$selected_row" ]] || break
    IFS=$'\t' read -r selected title tool base_branch <<<"$selected_row"
    [[ -n "$selected" ]] || break

    local created task_scope_value dispatch_kind
    dispatch_kind="create"
    task_scope_value="$(task_scope "$selected")"

    if is_truthy "$WORKTREE_POOL_MODE"; then
      created="$(find_idle_compatible_worktree "$tool" "$base_branch" "$task_scope_value" || true)"
      if [[ -n "$created" ]]; then
        dispatch_kind="reuse"
      fi
    fi

    if [[ -z "$created" ]]; then
      if ! created="$(create_macc_worktree "$selected" "$title" "$tool" "$base_branch")"; then
        apply_transition "$selected" "blocked" "" "" "failure:worktree_create"
        note "Blocked task due to worktree create failure: ${selected}"
        continue
      fi
    fi
    local worktree_path branch last_commit
    IFS=$'\t' read -r worktree_path branch last_commit <<<"$created"

    if worktree_in_use "$worktree_path"; then
      local other
      other="$(worktree_task_id "$worktree_path")"
      note "Skip: worktree already in use by task ${other}: ${worktree_path}"
      continue
    fi
    if ! worktree_exists_on_disk "$worktree_path"; then
      echo "Error: worktree path not a git worktree on disk: ${worktree_path}" >&2
      apply_transition "$selected" "blocked" "" "" "failure:worktree_create"
      continue
    fi

    write_worktree_prd "$worktree_path" "$selected"
    mark_task_claimed "$selected" "$worktree_path" "$branch" "$base_branch" "$last_commit" "$tool"

    lock_release
    invoke_performer "$selected" "$worktree_path" "$tool" &
    local performer_pid=$!
    lock_acquire
    ensure_registry_valid
    set_task_runtime "$selected" "running" "dev" "$performer_pid" "" "$(now_iso)"
    jobs+=("${performer_pid}|${selected}|${worktree_path}|${tool}")

    note "Dispatched: ${selected}"
    note "  tool:      ${tool}"
    note "  branch:    ${branch}"
    note "  worktree:  ${worktree_path}"
    note "  source:    ${dispatch_kind}"
    note "  mode:      async (pid=${performer_pid})"
    note ""
    emit_event "task_dispatched" "Task dispatched" "$selected" "claimed" "tool=${tool} worktree=${worktree_path}" "dev" "started"

    dispatched=$((dispatched + 1))
    if [[ "$MAX_DISPATCH" -gt 0 && "$dispatched" -ge "$MAX_DISPATCH" ]]; then
      break
    fi
  done

  while [[ "${#jobs[@]}" -gt 0 ]]; do
    local completed_idx="" completed_task="" completed_tool="" completed_rc=0
    lock_release
    while [[ -z "$completed_idx" ]]; do
      local i
      for i in "${!jobs[@]}"; do
        local pid task_id wt tool
        IFS='|' read -r pid task_id wt tool <<<"${jobs[$i]}"
        if ! kill -0 "$pid" >/dev/null 2>&1; then
          completed_idx="$i"
          completed_task="$task_id"
          completed_tool="$tool"
          wait "$pid" || completed_rc=$?
          break
        fi
      done
      if [[ -z "$completed_idx" ]]; then
        lock_acquire
        consume_runtime_events_once
        lock_release
        sleep 0.1
      fi
    done
    lock_acquire
    ensure_registry_valid
    consume_runtime_events_once

    unset "jobs[$completed_idx]"
    jobs=("${jobs[@]}")

    handle_performer_completion "$completed_task" "$completed_tool" "$completed_rc"
  done

  note "Dispatch complete. Tasks dispatched: ${dispatched}"
  emit_event "dispatch_complete" "Dispatch finished" "" "" "dispatched=${dispatched}"
}

main() {
  local command="dispatch"
  local transition_task_id=""
  local transition_state=""
  local transition_pr_url=""
  local transition_reviewer=""
  local transition_reason=""
  local failure_task_id=""
  local failure_kind=""
  local signal_task_id=""
  local signal_json=""
  local unlock_task_id=""
  local unlock_resource=""
  local unlock_all="false"
  local unlock_state="blocked"
  local retry_task_id=""
  local retry_phase=""
  local retry_skip="false"
  local requires_prd="false"
  local requires_sync="false"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      run|dispatch|advance|sync|status|reconcile|unlock|cleanup|retry-phase) command="$1"; shift ;;
      --prd) PRD_FILE="$2"; shift 2 ;;
      --repo) REPO_DIR="$2"; shift 2 ;;
      --transition) transition_task_id="$2"; shift 2 ;;
      --state) transition_state="$2"; shift 2 ;;
      --pr-url) transition_pr_url="$2"; shift 2 ;;
      --reviewer) transition_reviewer="$2"; shift 2 ;;
      --reason) transition_reason="$2"; shift 2 ;;
      --failure) failure_task_id="$2"; shift 2 ;;
      --failure-kind) failure_kind="$2"; shift 2 ;;
      --signal) signal_task_id="$2"; shift 2 ;;
      --signal-json) signal_json="$2"; shift 2 ;;
      --task) unlock_task_id="$2"; shift 2 ;;
      --resource) unlock_resource="$2"; shift 2 ;;
      --unlock-state) unlock_state="$2"; shift 2 ;;
      --retry-task) retry_task_id="$2"; shift 2 ;;
      --retry-phase) retry_phase="$2"; shift 2 ;;
      --skip) retry_skip="true"; shift ;;
      --all) unlock_all="true"; shift ;;
      -h|--help) usage; exit 0 ;;
      *) echo "Unknown arg: $1" >&2; usage; exit 1 ;;
    esac
  done

  case "$command" in
    run|dispatch|sync|reconcile|retry-phase)
      requires_prd="true"
      requires_sync="true"
      ;;
  esac

  need_cmd git
  need_cmd jq
  ENABLED_TOOLS_JSON="$(csv_to_json_array "$ENABLED_TOOLS_CSV")"
  TOOL_PRIORITY_JSON="$(csv_to_json_array "$TOOL_PRIORITY_CSV")"

  jq -e 'type == "object"' <<<"$MAX_PARALLEL_PER_TOOL_JSON" >/dev/null 2>&1 || {
    echo "Error: MAX_PARALLEL_PER_TOOL_JSON must be a JSON object." >&2
    exit 1
  }
  jq -e 'type == "object"' <<<"$TOOL_SPECIALIZATIONS_JSON" >/dev/null 2>&1 || {
    echo "Error: TOOL_SPECIALIZATIONS_JSON must be a JSON object." >&2
    exit 1
  }
  jq -e 'all(to_entries[]; ((.value|type) == "number") or ((.value|type) == "string" and (.value | test("^[0-9]+$"))))' \
    <<<"$MAX_PARALLEL_PER_TOOL_JSON" >/dev/null 2>&1 || {
    echo "Error: MAX_PARALLEL_PER_TOOL_JSON values must be integers." >&2
    exit 1
  }
  jq -e 'all(to_entries[]; (.value|type) == "array" and all(.value[]; type == "string"))' \
    <<<"$TOOL_SPECIALIZATIONS_JSON" >/dev/null 2>&1 || {
    echo "Error: TOOL_SPECIALIZATIONS_JSON values must be string arrays." >&2
    exit 1
  }
  [[ "$MAX_DISPATCH" =~ ^[0-9]+$ ]] || {
    echo "Error: MAX_DISPATCH must be a non-negative integer: $MAX_DISPATCH" >&2
    exit 1
  }
  [[ "$MAX_PARALLEL" =~ ^[0-9]+$ ]] || {
    echo "Error: MAX_PARALLEL must be a positive integer: $MAX_PARALLEL" >&2
    exit 1
  }
  [[ "$MAX_PARALLEL" -gt 0 ]] || {
    echo "Error: MAX_PARALLEL must be >= 1: $MAX_PARALLEL" >&2
    exit 1
  }
  [[ "$STALE_HEARTBEAT_SECONDS" =~ ^[0-9]+$ ]] || {
    echo "Error: STALE_HEARTBEAT_SECONDS must be a non-negative integer: $STALE_HEARTBEAT_SECONDS" >&2
    exit 1
  }
  [[ "$EVENT_LOG_MAX_BYTES" =~ ^[0-9]+$ ]] || {
    echo "Error: EVENT_LOG_MAX_BYTES must be a non-negative integer: $EVENT_LOG_MAX_BYTES" >&2
    exit 1
  }
  [[ "$EVENT_LOG_KEEP_FILES" =~ ^[0-9]+$ ]] || {
    echo "Error: EVENT_LOG_KEEP_FILES must be a non-negative integer: $EVENT_LOG_KEEP_FILES" >&2
    exit 1
  }
  [[ "$PROCESSED_EVENT_IDS_MAX" =~ ^[0-9]+$ ]] || {
    echo "Error: PROCESSED_EVENT_IDS_MAX must be a non-negative integer: $PROCESSED_EVENT_IDS_MAX" >&2
    exit 1
  }
  [[ "$SLO_DEV_SECONDS" =~ ^[0-9]+$ ]] || {
    echo "Error: SLO_DEV_SECONDS must be a non-negative integer: $SLO_DEV_SECONDS" >&2
    exit 1
  }
  [[ "$SLO_REVIEW_SECONDS" =~ ^[0-9]+$ ]] || {
    echo "Error: SLO_REVIEW_SECONDS must be a non-negative integer: $SLO_REVIEW_SECONDS" >&2
    exit 1
  }
  [[ "$SLO_INTEGRATE_SECONDS" =~ ^[0-9]+$ ]] || {
    echo "Error: SLO_INTEGRATE_SECONDS must be a non-negative integer: $SLO_INTEGRATE_SECONDS" >&2
    exit 1
  }
  [[ "$SLO_WAIT_SECONDS" =~ ^[0-9]+$ ]] || {
    echo "Error: SLO_WAIT_SECONDS must be a non-negative integer: $SLO_WAIT_SECONDS" >&2
    exit 1
  }
  [[ "$SLO_RETRIES_MAX" =~ ^[0-9]+$ ]] || {
    echo "Error: SLO_RETRIES_MAX must be a non-negative integer: $SLO_RETRIES_MAX" >&2
    exit 1
  }
  case "$STALE_HEARTBEAT_ACTION" in
    retry|block|requeue) ;;
    *)
      echo "Error: STALE_HEARTBEAT_ACTION must be retry|block|requeue: $STALE_HEARTBEAT_ACTION" >&2
      exit 1
      ;;
  esac
  if [[ "$command" == "dispatch" || "$command" == "run" || "$command" == "retry-phase" ]]; then
    need_cmd macc
  fi

  ensure_repo_valid
  normalize_paths
  if is_truthy "$COORDINATOR_AUTOMERGE"; then
    [[ -x "$COORDINATOR_MERGE_WORKER" ]] || {
      echo "Error: COORDINATOR_MERGE_WORKER is not executable: $COORDINATOR_MERGE_WORKER" >&2
      exit 1
    }
    if is_truthy "$COORDINATOR_MERGE_AI_FIX"; then
      [[ -n "$COORDINATOR_MERGE_FIX_HOOK" ]] || {
        echo "Error: COORDINATOR_MERGE_AI_FIX requires COORDINATOR_MERGE_FIX_HOOK." >&2
        exit 1
      }
      [[ -x "$COORDINATOR_MERGE_FIX_HOOK" ]] || {
        echo "Error: COORDINATOR_MERGE_FIX_HOOK is not executable: $COORDINATOR_MERGE_FIX_HOOK" >&2
        exit 1
      }
    fi
  fi
  if [[ "$requires_prd" == "true" ]]; then
    ensure_prd_valid
  fi
  setup_logging "$command"

  if [[ "$command" == "run" ]]; then
    run_control_plane
    exit $?
  fi

  lock_acquire
  ensure_registry_file
  ensure_registry_valid
  if [[ "$requires_sync" == "true" ]]; then
    spinner_start "Syncing registry"
    sync_registry_from_prd
    spinner_stop "Registry synced"
    ensure_registry_valid
  fi

  if [[ "$command" == "sync" ]]; then
    note "Registry synced."
    exit 0
  fi

  spinner_start "Cleaning stale tasks"
  cleanup_stale_tasks
  spinner_stop "Cleanup complete"
  consume_runtime_events_once
  reconcile_orphan_runtime_tasks

  if [[ "$command" == "cleanup" ]]; then
    note "Cleanup complete."
    exit 0
  fi

  if [[ "$command" == "retry-phase" ]]; then
    [[ -n "$retry_task_id" && -n "$retry_phase" ]] || {
      echo "Error: retry-phase requires --retry-task and --retry-phase" >&2
      exit 1
    }
    retry_failed_phase "$retry_task_id" "$retry_phase" "$retry_skip"
    exit $?
  fi

  if [[ -n "$signal_task_id" || -n "$signal_json" ]]; then
    [[ -n "$signal_task_id" && -n "$signal_json" ]] || {
      echo "Error: --signal requires --signal-json and vice versa" >&2
      exit 1
    }
    task_exists "$signal_task_id" || {
      echo "Error: task not found in registry: $signal_task_id" >&2
      exit 1
    }
    apply_signal_update "$signal_task_id" "$signal_json"
    note "Signal ingested for ${signal_task_id}"
    exit 0
  fi

  if [[ -n "$failure_task_id" || -n "$failure_kind" ]]; then
    [[ -n "$failure_task_id" && -n "$failure_kind" ]] || {
      echo "Error: --failure requires --failure-kind and vice versa" >&2
      exit 1
    }
    task_exists "$failure_task_id" || {
      echo "Error: task not found in registry: $failure_task_id" >&2
      exit 1
    }
    local target_state
    target_state="$(failure_kind_to_state "$failure_kind")"
    [[ -n "$target_state" ]] || {
      echo "Error: unknown failure kind: $failure_kind" >&2
      exit 1
    }
    local current
    current="$(task_state "$failure_task_id")"
    validate_transition "$current" "$target_state"
    local reason
    reason="failure:${failure_kind}"
    if [[ -n "$transition_reason" ]]; then
      reason="${reason} ${transition_reason}"
    fi
    apply_transition "$failure_task_id" "$target_state" "" "" "$reason"
    note "Failure handled ${failure_task_id}: ${current} -> ${target_state} (${failure_kind})"
    exit 0
  fi

  if [[ -n "$transition_task_id" || -n "$transition_state" ]]; then
    [[ -n "$transition_task_id" && -n "$transition_state" ]] || {
      echo "Error: --transition requires --state and vice versa" >&2
      exit 1
    }
    task_exists "$transition_task_id" || {
      echo "Error: task not found in registry: $transition_task_id" >&2
      exit 1
    }
    local current
    current="$(task_state "$transition_task_id")"
    transition_task_and_hooks "$transition_task_id" "$transition_state" "$transition_pr_url" "$transition_reviewer" "$transition_reason"
    note "Transitioned ${transition_task_id}: ${current} -> ${transition_state}"
    exit 0
  fi

  if [[ "$command" == "status" ]]; then
    status_summary
    exit 0
  fi

  if [[ "$command" == "reconcile" ]]; then
    spinner_start "Reconciling registry"
    reconcile_registry
    spinner_stop "Reconcile complete"
    note "Reconcile complete."
    exit 0
  fi

  if [[ "$command" == "advance" ]]; then
    spinner_start "Advancing active tasks"
    advance_active_tasks
    spinner_stop "Advance complete"
    exit 0
  fi

  if [[ "$command" == "unlock" ]]; then
    unlock_locks "$unlock_task_id" "$unlock_resource" "$unlock_all" "$unlock_state"
    exit 0
  fi

  spinner_start "Dispatching ready tasks"
  dispatch_ready_tasks
  spinner_stop "Dispatch complete"
}

main "$@"
