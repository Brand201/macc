# shellcheck shell=bash

note() {
  local msg="$*"
  if [[ -n "${COORD_TERM_FD:-}" ]]; then
    printf '%s\n' "$msg" >&"$COORD_TERM_FD"
  else
    printf '%s\n' "$msg"
  fi
  printf '%s\n' "$msg"
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
  local storage_rc=0
  spinner_stop "Coordinator stopped."
  lock_release || true
  if [[ "$rc" -eq 0 ]]; then
    emit_event "command_end" "Coordinator command completed"
  else
    emit_event "command_error" "Coordinator command failed" "" "" "exit_code=${rc}"
  fi
  if [[ "${COORDINATOR_SKIP_STORAGE_SYNC:-0}" != "1" \
        && "${COORDINATOR_STORAGE_PRE_SYNC_DONE:-false}" == "true" \
        && "${COORDINATOR_JSON_PROJECTION_ENABLED:-false}" == "true" ]]; then
    if ! coordinator_storage_sync_phase "post"; then
      storage_rc=1
    fi
  fi
  if [[ "$storage_rc" -ne 0 ]]; then
    rc=1
  fi
  if [[ "$rc" -ne 0 && -n "${COORD_TERM_FD:-}" && -n "${COORD_LOG_FILE:-}" ]]; then
    printf 'Coordinator failed. See log: %s\n' "$COORD_LOG_FILE" >&"$COORD_TERM_FD"
  fi
  return "$rc"
}

normalize_storage_mode() {
  local mode
  mode="$(printf '%s' "${COORDINATOR_STORAGE_MODE:-sqlite}" | tr '[:upper:]' '[:lower:]')"
  case "$mode" in
    json) echo "json" ;;
    dual_write|dual-write) echo "dual-write" ;;
    sqlite) echo "sqlite" ;;
    *)
      echo "Error: COORDINATOR_STORAGE_MODE must be json|dual-write|sqlite, got '${COORDINATOR_STORAGE_MODE}'" >&2
      return 1
      ;;
  esac
}

check_cutover_gates() {
  [[ -f "$COORD_EVENTS_FILE" ]] || {
    note "Cutover gate: no events file found at ${COORD_EVENTS_FILE}."
    return 1
  }

  [[ "$CUTOVER_GATE_WINDOW_EVENTS" =~ ^[0-9]+$ ]] || {
    echo "Error: CUTOVER_GATE_WINDOW_EVENTS must be a non-negative integer: ${CUTOVER_GATE_WINDOW_EVENTS}" >&2
    return 1
  }
  [[ "$CUTOVER_GATE_MAX_BLOCKED_RATIO" =~ ^[0-9]+([.][0-9]+)?$ ]] || {
    echo "Error: CUTOVER_GATE_MAX_BLOCKED_RATIO must be numeric: ${CUTOVER_GATE_MAX_BLOCKED_RATIO}" >&2
    return 1
  }
  [[ "$CUTOVER_GATE_MAX_STALE_RATIO" =~ ^[0-9]+([.][0-9]+)?$ ]] || {
    echo "Error: CUTOVER_GATE_MAX_STALE_RATIO must be numeric: ${CUTOVER_GATE_MAX_STALE_RATIO}" >&2
    return 1
  }

  local events_json mismatch_count task_events blocked_events stale_events blocked_ratio stale_ratio
  events_json="$(tail -n "$CUTOVER_GATE_WINDOW_EVENTS" "$COORD_EVENTS_FILE" | jq -s '.')" || {
    echo "Error: failed to parse coordinator events for cutover gate." >&2
    return 1
  }
  mismatch_count="$(jq -r '[.[] | select((.type // .event // "") == "storage_mismatch_count")] | length' <<<"$events_json")"
  task_events="$(jq -r '[.[] | select((.task_id // "") != "")] | length' <<<"$events_json")"
  blocked_events="$(jq -r '[.[] | select(((.type // .event // "") == "task_blocked") or ((.type // .event // "") == "local_merge_failed"))] | length' <<<"$events_json")"
  stale_events="$(jq -r '[.[] | select(((.type // .event // "") == "stale_runtime_total") or ((.type // .event // "") == "task_runtime_stale"))] | length' <<<"$events_json")"

  if [[ "$task_events" -gt 0 ]]; then
    blocked_ratio="$(awk -v n="$blocked_events" -v d="$task_events" 'BEGIN { printf "%.6f", (n / d) }')"
    stale_ratio="$(awk -v n="$stale_events" -v d="$task_events" 'BEGIN { printf "%.6f", (n / d) }')"
  else
    blocked_ratio="0.000000"
    stale_ratio="0.000000"
  fi

  note "Cutover gate: events_window=${CUTOVER_GATE_WINDOW_EVENTS} task_events=${task_events} mismatches=${mismatch_count} blocked_ratio=${blocked_ratio} stale_ratio=${stale_ratio}"
  if [[ "$mismatch_count" -gt 0 ]]; then
    echo "Error: cutover gate failed: storage_mismatch_count=${mismatch_count} (must be 0)." >&2
    return 1
  fi
  awk -v v="$blocked_ratio" -v max="$CUTOVER_GATE_MAX_BLOCKED_RATIO" 'BEGIN { exit !(v <= max) }' || {
    echo "Error: cutover gate failed: blocked ratio ${blocked_ratio} exceeds ${CUTOVER_GATE_MAX_BLOCKED_RATIO}." >&2
    return 1
  }
  awk -v v="$stale_ratio" -v max="$CUTOVER_GATE_MAX_STALE_RATIO" 'BEGIN { exit !(v <= max) }' || {
    echo "Error: cutover gate failed: stale ratio ${stale_ratio} exceeds ${CUTOVER_GATE_MAX_STALE_RATIO}." >&2
    return 1
  }
  note "Cutover gate passed."
  return 0
}

coordinator_storage_sync_phase() {
  local phase="$1"
  local mode="${COORDINATOR_STORAGE_MODE:-json}"
  if [[ "$mode" == "json" ]]; then
    return 0
  fi
  if ! command -v macc >/dev/null 2>&1; then
    echo "Error: macc is required for coordinator storage mode '${mode}'" >&2
    return 1
  fi
  local output status started_ms ended_ms elapsed_ms
  started_ms="$(date +%s%3N)"
  set +e
  output="$(macc --cwd "$REPO_DIR" coordinator storage-sync -- --mode "$mode" --phase "$phase" 2>&1)"
  status=$?
  set -e
  ended_ms="$(date +%s%3N)"
  elapsed_ms=$((ended_ms - started_ms))
  if [[ "$status" -ne 0 ]]; then
    if [[ "$output" == *"Coordinator storage mismatch:"* ]]; then
      COORD_STORAGE_MISMATCH_COUNT=$((COORD_STORAGE_MISMATCH_COUNT + 1))
      emit_event "storage_mismatch_count" "Coordinator storage mismatch detected" "" "" \
        "mode=${mode} phase=${phase} count=${COORD_STORAGE_MISMATCH_COUNT}" "" "error" \
        "coordinator" "{\"count\":${COORD_STORAGE_MISMATCH_COUNT}}"
    fi
    echo "Error: coordinator storage sync failed (mode=${mode}, phase=${phase}):" >&2
    if [[ -n "$output" ]]; then
      echo "$output" >&2
    fi
    emit_event "storage_sync_failed" "Coordinator storage sync failed" "" "" \
      "mode=${mode} phase=${phase} status=${status} latency_ms=${elapsed_ms}" "" "failed" \
      "coordinator" "{\"latency_ms\":${elapsed_ms},\"status\":${status}}"
    emit_event "storage_sync_latency_ms" "Coordinator storage sync latency" "" "" \
      "mode=${mode} phase=${phase} value=${elapsed_ms}" "" "error" "coordinator" \
      "{\"value\":${elapsed_ms}}"
    return 1
  fi
  emit_event "storage_sync_ok" "Coordinator storage sync complete" "" "" \
    "mode=${mode} phase=${phase} latency_ms=${elapsed_ms}" "" "done" "coordinator" \
    "{\"latency_ms\":${elapsed_ms}}"
  emit_event "storage_sync_latency_ms" "Coordinator storage sync latency" "" "" \
    "mode=${mode} phase=${phase} value=${elapsed_ms}" "" "done" "coordinator" \
    "{\"value\":${elapsed_ms}}"
  return 0
}

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
  cutover-gate Validate PR6 cutover gates from coordinator events/registry

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
  COORDINATOR_STORAGE_MODE      Coordinator storage backend mode: json|dual-write|sqlite (default: sqlite)
  COORDINATOR_JSON_COMPAT       Mirror SQLite state back to task_registry.json (default: 0; status only when disabled)
  CUTOVER_GATE_WINDOW_EVENTS    Number of tail events considered by cutover-gate (default: 2000)
  CUTOVER_GATE_MAX_BLOCKED_RATIO Max blocked event ratio allowed by cutover-gate (default: 0.25)
  CUTOVER_GATE_MAX_STALE_RATIO  Max stale event ratio allowed by cutover-gate (default: 0.25)
  SLO_DEV_SECONDS               Warn when dev_s exceeds this threshold (0 disables)
  SLO_REVIEW_SECONDS            Warn when review_s exceeds this threshold (default: 300, 0 disables)
  SLO_INTEGRATE_SECONDS         Warn when integrate_s exceeds this threshold (0 disables)
  SLO_WAIT_SECONDS              Warn when wait_s exceeds this threshold (0 disables)
  SLO_RETRIES_MAX               Warn when retries exceeds this threshold (0 disables)
  ERROR_CODE_RETRY_LIST         Comma-separated error codes eligible for auto-retry (default: E101,E102,E103,E301,E302,E303)
  ERROR_CODE_RETRY_MAX          Max auto-retries per task for eligible error codes (default: 2)

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

is_truthy() {
  local v
  v="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  [[ "$v" == "1" || "$v" == "true" || "$v" == "yes" || "$v" == "on" ]]
}

should_project_json_for_command() {
  local command_name="${1:-}"
  if is_truthy "${COORDINATOR_JSON_COMPAT:-0}"; then
    return 0
  fi
  case "$command_name" in
    status) return 0 ;;
    *) return 1 ;;
  esac
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

status_summary() {
  note "Registry: $TASK_REGISTRY_FILE"
  local counts total todo active blocked merged
  if command -v macc >/dev/null 2>&1 \
    && counts="$(macc --cwd "$REPO_DIR" coordinator state-counts -- 2>/dev/null)"; then
    IFS=$'\t' read -r total todo active blocked merged <<<"$counts"
    note "Tasks: ${total}"
    note "  todo: ${todo}"
    note "  active: ${active}"
    note "  blocked: ${blocked}"
    note "  merged: ${merged}"
  else
    total="$(jq -r '(.tasks // []) | length' "$TASK_REGISTRY_FILE")"
    note "Tasks: ${total}"
    jq -r '
      (.tasks // [])
      | sort_by(.state)
      | group_by(.state)
      | map("\(.[0].state): \(length)")
      | .[]
    ' "$TASK_REGISTRY_FILE"
  fi
  local locks
  if command -v macc >/dev/null 2>&1 \
    && locks="$(macc --cwd "$REPO_DIR" coordinator state-locks -- --format count 2>/dev/null)"; then
    note "Locks: ${locks}"
    macc --cwd "$REPO_DIR" coordinator state-locks -- --format lines 2>/dev/null \
      | sed 's/^/  /'
  else
    locks="$(jq -r '(.resource_locks // {}) | length' "$TASK_REGISTRY_FILE")"
    note "Locks: ${locks}"
    jq -r '
      (.resource_locks // {})
      | to_entries[]
      | "  \(.key) -> \(.value.task_id)"
    ' "$TASK_REGISTRY_FILE"
  fi
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
    mark_run_blocking_merge_failure_from_registry || true
    if run_should_pause_on_blocking_merge; then
      print_blocking_merge_pause_error
      return 1
    fi

    reconcile_orphan_runtime_tasks
    advance_active_tasks
    monitor_run_loop_merge_jobs_once
    mark_run_blocking_merge_failure_from_registry || true
    if run_should_pause_on_blocking_merge; then
      print_blocking_merge_pause_error
      return 1
    fi

    reconcile_registry
    cleanup_stale_tasks
    consume_runtime_events_once
    monitor_run_loop_jobs_once
    monitor_run_loop_merge_jobs_once
    mark_run_blocking_merge_failure_from_registry || true
    if run_should_pause_on_blocking_merge; then
      print_blocking_merge_pause_error
      return 1
    fi

    dispatch_ready_tasks_nonblocking

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
      run|dispatch|advance|sync|status|reconcile|unlock|cleanup|retry-phase|cutover-gate) command="$1"; shift ;;
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

  if [[ "${COORDINATOR_STORAGE_MODE_DEFAULTED:-false}" == "true" ]]; then
    if [[ "$command" == "run" ]]; then
      COORDINATOR_STORAGE_MODE="sqlite"
    else
      COORDINATOR_STORAGE_MODE="sqlite"
    fi
  fi

  need_cmd git
  need_cmd jq
  COORDINATOR_STORAGE_MODE="$(normalize_storage_mode)"
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
  [[ "$ERROR_CODE_RETRY_MAX" =~ ^[0-9]+$ ]] || {
    echo "Error: ERROR_CODE_RETRY_MAX must be a non-negative integer: $ERROR_CODE_RETRY_MAX" >&2
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
  if [[ "${COORDINATOR_SKIP_STORAGE_SYNC:-0}" != "1" \
        && "$COORDINATOR_STORAGE_MODE" != "json" ]] \
    && should_project_json_for_command "$command"; then
    coordinator_storage_sync_phase "pre"
    COORDINATOR_STORAGE_PRE_SYNC_DONE="true"
    COORDINATOR_JSON_PROJECTION_ENABLED="true"
  fi
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
    if [[ "$command" == "run" ]]; then
      is_truthy "$COORDINATOR_MERGE_AI_FIX" || {
        echo "Error: coordinator run requires COORDINATOR_MERGE_AI_FIX=true to enforce AI-assisted merge handling." >&2
        exit 1
      }
      [[ -n "$COORDINATOR_MERGE_FIX_HOOK" && -x "$COORDINATOR_MERGE_FIX_HOOK" ]] || {
        echo "Error: coordinator run requires an executable COORDINATOR_MERGE_FIX_HOOK." >&2
        exit 1
      }
    fi
  fi
  if [[ "$requires_prd" == "true" ]]; then
    ensure_prd_valid
  fi
  setup_logging "$command"

  if [[ "$command" == "cutover-gate" ]]; then
    check_cutover_gates
    exit $?
  fi

  if [[ "$command" == "run" ]]; then
    if [[ "${MACC_COORD_RUST_RUNNER_ACTIVE:-0}" != "1" ]] && command -v macc >/dev/null 2>&1; then
      MACC_COORD_RUST_RUNNER_ACTIVE=1 \
      macc --cwd "$REPO_DIR" coordinator control-plane-run --no-tui
      exit $?
    fi
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
