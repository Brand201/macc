#!/usr/bin/env bash
set -euo pipefail

# coordinator.sh
# - Reads PRD tasks
# - Maintains task_registry.json
# - Dispatches READY tasks to dedicated MACC worktrees
# - Applies dependency gating + exclusive resource locking
# - Assigns at most one task per worktree

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRD_FILE="${PRD_FILE:-prd.json}"
TASK_REGISTRY_FILE="${TASK_REGISTRY_FILE:-task_registry.json}"
REPO_DIR="${REPO_DIR:-.}"
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
COORDINATOR_TOOL="${COORDINATOR_TOOL:-}"
ENABLED_TOOLS_CSV="${ENABLED_TOOLS_CSV:-}"
TOOL_PRIORITY_CSV="${TOOL_PRIORITY_CSV:-}"
MAX_PARALLEL_PER_TOOL_JSON="${MAX_PARALLEL_PER_TOOL_JSON:-{}}"
TOOL_SPECIALIZATIONS_JSON="${TOOL_SPECIALIZATIONS_JSON:-{}}"
WORKTREE_POOL_MODE="${WORKTREE_POOL_MODE:-true}" # true|false: reuse idle compatible worktrees
COORDINATOR_VCS_HOOK="${COORDINATOR_VCS_HOOK:-}" # optional executable implementing PR/CI/queue/merge actions
COORDINATOR_AUTOMERGE="${COORDINATOR_AUTOMERGE:-true}" # true|false: allow default local merge fallback

ENABLED_TOOLS_JSON="[]"
TOOL_PRIORITY_JSON="[]"
COORD_LOG_DIR=""
COORD_LOG_FILE=""

note() {
  local msg="$*"
  if [[ -n "${COORD_TERM_FD:-}" ]]; then
    printf '%s\n' "$msg" >&"$COORD_TERM_FD"
  else
    printf '%s\n' "$msg"
  fi
  printf '%s\n' "$msg"
}

on_exit() {
  local rc=$?
  if [[ "$rc" -ne 0 && -n "${COORD_TERM_FD:-}" && -n "${COORD_LOG_FILE:-}" ]]; then
    printf 'Coordinator failed. See log: %s\n' "$COORD_LOG_FILE" >&"$COORD_TERM_FD"
  fi
}
trap on_exit EXIT

setup_logging() {
  local command_name="${1:-dispatch}"
  mkdir -p "${REPO_DIR}/.macc/log/coordinator"
  COORD_LOG_DIR="${REPO_DIR}/.macc/log/coordinator"
  local ts
  ts="$(date -u +"%Y%m%dT%H%M%SZ")"
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
}

usage() {
  cat <<EOF
Usage:
  AGENT_ID=agentA ./coordinator.sh [command] [options]

Commands:
  dispatch    Sync, cleanup, and dispatch READY tasks (default)
  advance     Progress active tasks through PR/CI/review/queue/merge states
  sync        Sync registry from PRD without dispatching
  status      Show registry summary + lock status
  reconcile   Reconcile registry with worktree state on disk
  unlock      Release locks (task or resource)
  cleanup     Run stale-task cleanup only

Env vars:
  PRD_FILE            Path to PRD JSON (default: prd.json)
  TASK_REGISTRY_FILE  Path to task registry JSON (default: task_registry.json)
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
  COORDINATOR_TOOL              Optional fixed tool for coordinator phase hooks (review/fix/integrate)
  ENABLED_TOOLS_CSV             Optional allowed tool IDs (comma-separated; usually from macc.yaml tools.enabled)
  TOOL_PRIORITY_CSV             Optional priority order for tool selection (comma-separated)
  MAX_PARALLEL_PER_TOOL_JSON    Optional JSON object {"tool":<cap>} with per-tool concurrency caps
  TOOL_SPECIALIZATIONS_JSON     Optional JSON object {"category":["tool-a","tool-b"]} for category routing
  WORKTREE_POOL_MODE            Reuse idle compatible worktrees when true (default: true)
  COORDINATOR_VCS_HOOK          Optional hook executable for PR/CI/merge integration
  COORDINATOR_AUTOMERGE         Allow local merge fallback when no hook is configured (default: true)

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

lock_acquire() {
  COORD_LOCK_DIR="${TASK_REGISTRY_FILE}.lock"
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
               review: ($old.review // {"reviewer": null, "changed": false, "last_reviewed_at": null})
             }
         )
       )
     | .tasks |= map(
         if .state == "merged" then
           .assignee = null
           | .claimed_at = null
           | .worktree = null
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

  case "$from:$to" in
    todo:claimed) return 0 ;;
    claimed:in_progress|claimed:blocked|claimed:abandoned) return 0 ;;
    in_progress:pr_open|in_progress:blocked|in_progress:abandoned) return 0 ;;
    pr_open:changes_requested|pr_open:queued|pr_open:blocked|pr_open:abandoned) return 0 ;;
    changes_requested:pr_open|changes_requested:blocked|changes_requested:abandoned) return 0 ;;
    queued:merged|queued:pr_open|queued:blocked|queued:abandoned) return 0 ;;
    blocked:todo|blocked:claimed|blocked:in_progress|blocked:pr_open|blocked:changes_requested|blocked:queued|blocked:abandoned) return 0 ;;
    abandoned:todo) return 0 ;;
  esac

  echo "Error: invalid transition ${from} -> ${to}" >&2
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
            elif ($state == "todo") then
              .assignee = null
              | .claimed_at = null
              | .worktree = null
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
}

select_next_ready_task_tsv() {
  jq -r \
    --argjson enabled_tools "$ENABLED_TOOLS_JSON" \
    --argjson tool_priority "$TOOL_PRIORITY_JSON" \
    --argjson tool_caps "$MAX_PARALLEL_PER_TOOL_JSON" \
    --argjson tool_specializations "$TOOL_SPECIALIZATIONS_JSON" \
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

  if [[ "$new_state" == "pr_open" || "$new_state" == "changes_requested" || "$new_state" == "queued" ]]; then
    wt="$(worktree_for_task "$task_id")"
    tool="$(coordinator_phase_tool_for_task "$task_id")"
    if [[ -n "$wt" && -n "$tool" ]]; then
      if ! maybe_run_phase_hook "$task_id" "$new_state" "$wt" "$tool"; then
        note "Warning: phase hook failed for ${task_id} (${new_state}); continuing state machine."
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

  if ! git -C "$REPO_DIR" rev-parse --verify "$branch" >/dev/null 2>&1; then
    echo "Error: merge branch not found for task ${task_id}: ${branch}" >&2
    return 1
  fi
  if ! git -C "$REPO_DIR" rev-parse --verify "$base_branch" >/dev/null 2>&1; then
    echo "Error: base branch not found for task ${task_id}: ${base_branch}" >&2
    return 1
  fi
  if git -C "$REPO_DIR" status --porcelain | awk 'NF' | grep -q .; then
    echo "Error: repository has uncommitted changes; cannot merge task ${task_id}" >&2
    return 1
  fi

  local merge_msg
  merge_msg="macc: merge task ${task_id}"
  if ! git -C "$REPO_DIR" checkout "$base_branch" >/dev/null 2>&1; then
    return 1
  fi
  set +e
  git -C "$REPO_DIR" merge --no-ff -m "$merge_msg" "$branch" >/dev/null 2>&1
  local rc=$?
  set -e
  if [[ "$rc" -ne 0 ]]; then
    git -C "$REPO_DIR" merge --abort >/dev/null 2>&1 || true
    return 1
  fi
  return 0
}

advance_active_tasks() {
  local progressed="false"
  local pass
  for pass in $(seq 1 16); do
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
                if local_merge_branch_into_base "$task_id" "$branch" "$base_branch"; then
                  transition_task_and_hooks "$task_id" "merged" "$pr_url" "" "auto:local_merge"
                  note "Advance: ${task_id} queued -> merged (local merge)"
                  pass_progressed="true"
                  progressed="true"
                else
                  transition_task_and_hooks "$task_id" "blocked" "$pr_url" "" "failure:local_merge"
                  note "Blocked task due to local merge failure: ${task_id}"
                  pass_progressed="true"
                  progressed="true"
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
        [[ -n "$completed_idx" ]] || sleep 0.1
      done
      lock_acquire
      ensure_registry_valid

      unset "jobs[$completed_idx]"
      jobs=("${jobs[@]}")

      if [[ "$completed_rc" -ne 0 ]]; then
        apply_transition "$completed_task" "blocked" "" "" "failure:performer"
        note "Blocked task due to performer failure: ${completed_task}"
      else
        transition_task_and_hooks "$completed_task" "in_progress" "" "" "auto:performer_complete"
        note "Performer complete: ${completed_task} (${completed_tool})"
      fi
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
    jobs+=("${performer_pid}|${selected}|${worktree_path}|${tool}")

    note "Dispatched: ${selected}"
    note "  tool:      ${tool}"
    note "  branch:    ${branch}"
    note "  worktree:  ${worktree_path}"
    note "  source:    ${dispatch_kind}"
    note "  mode:      async (pid=${performer_pid})"
    note ""

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
      [[ -n "$completed_idx" ]] || sleep 0.1
    done
    lock_acquire
    ensure_registry_valid

    unset "jobs[$completed_idx]"
    jobs=("${jobs[@]}")

    if [[ "$completed_rc" -ne 0 ]]; then
      apply_transition "$completed_task" "blocked" "" "" "failure:performer"
      note "Blocked task due to performer failure: ${completed_task}"
    else
      transition_task_and_hooks "$completed_task" "in_progress" "" "" "auto:performer_complete"
      note "Performer complete: ${completed_task} (${completed_tool})"
    fi
  done

  note "Dispatch complete. Tasks dispatched: ${dispatched}"
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
  local requires_prd="false"
  local requires_sync="false"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      dispatch|advance|sync|status|reconcile|unlock|cleanup) command="$1"; shift ;;
      --prd) PRD_FILE="$2"; shift 2 ;;
      --registry) TASK_REGISTRY_FILE="$2"; shift 2 ;;
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
      --all) unlock_all="true"; shift ;;
      -h|--help) usage; exit 0 ;;
      *) echo "Unknown arg: $1" >&2; usage; exit 1 ;;
    esac
  done

  case "$command" in
    dispatch|sync|reconcile)
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
  if [[ "$command" == "dispatch" ]]; then
    need_cmd macc
  fi

  if [[ "$requires_prd" == "true" ]]; then
    ensure_prd_valid
  fi
  ensure_repo_valid
  setup_logging "$command"

  lock_acquire
  ensure_registry_file
  ensure_registry_valid
  if [[ "$requires_sync" == "true" ]]; then
    sync_registry_from_prd
    ensure_registry_valid
  fi

  if [[ "$command" == "sync" ]]; then
    note "Registry synced."
    exit 0
  fi

  cleanup_stale_tasks

  if [[ "$command" == "cleanup" ]]; then
    note "Cleanup complete."
    exit 0
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
    reconcile_registry
    note "Reconcile complete."
    exit 0
  fi

  if [[ "$command" == "advance" ]]; then
    advance_active_tasks
    exit 0
  fi

  if [[ "$command" == "unlock" ]]; then
    unlock_locks "$unlock_task_id" "$unlock_resource" "$unlock_all" "$unlock_state"
    exit 0
  fi

  dispatch_ready_tasks
}

main "$@"
