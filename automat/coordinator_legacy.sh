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
if [[ -z "${COORDINATOR_STORAGE_MODE+x}" ]]; then
  COORDINATOR_STORAGE_MODE=""
  COORDINATOR_STORAGE_MODE_DEFAULTED="true"
else
  COORDINATOR_STORAGE_MODE="${COORDINATOR_STORAGE_MODE}"
  COORDINATOR_STORAGE_MODE_DEFAULTED="false"
fi
CUTOVER_GATE_WINDOW_EVENTS="${CUTOVER_GATE_WINDOW_EVENTS:-2000}" # number of tail events used by cutover-gate checks
CUTOVER_GATE_MAX_BLOCKED_RATIO="${CUTOVER_GATE_MAX_BLOCKED_RATIO:-0.25}" # max blocked ratio over task events
CUTOVER_GATE_MAX_STALE_RATIO="${CUTOVER_GATE_MAX_STALE_RATIO:-0.25}" # max stale ratio over task events
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
RUN_BLOCKING_MERGE_FAILED="false"
RUN_BLOCKING_MERGE_TASK_ID=""
RUN_BLOCKING_MERGE_ERROR=""
RUN_BLOCKING_MERGE_REPORT=""
COORD_STORAGE_MISMATCH_COUNT=0

# Modular coordinator implementation
source "${SCRIPT_DIR}/legacy_coordinator/runtime.sh"
source "${SCRIPT_DIR}/legacy_coordinator/events.sh"
source "${SCRIPT_DIR}/legacy_coordinator/state.sh"
source "${SCRIPT_DIR}/legacy_coordinator/vcs.sh"
source "${SCRIPT_DIR}/legacy_coordinator/jobs.sh"

trap on_exit EXIT

main "$@"
