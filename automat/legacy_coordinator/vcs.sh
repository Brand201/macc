# shellcheck shell=bash

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

