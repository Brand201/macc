#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  merge_worker.sh --repo <path> --task-id <id> --branch <branch> --base-branch <branch> --log-dir <path> --result-file <path> [--allow-ai-fix true|false] [--merge-fix-hook <path>]
EOF
}

REPO_DIR=""
TASK_ID=""
BRANCH=""
BASE_BRANCH=""
LOG_DIR=""
RESULT_FILE=""
ALLOW_AI_FIX="false"
MERGE_FIX_HOOK=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO_DIR="$2"; shift 2 ;;
    --task-id) TASK_ID="$2"; shift 2 ;;
    --branch) BRANCH="$2"; shift 2 ;;
    --base-branch) BASE_BRANCH="$2"; shift 2 ;;
    --log-dir) LOG_DIR="$2"; shift 2 ;;
    --result-file) RESULT_FILE="$2"; shift 2 ;;
    --allow-ai-fix) ALLOW_AI_FIX="$2"; shift 2 ;;
    --merge-fix-hook) MERGE_FIX_HOOK="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$REPO_DIR" || -z "$TASK_ID" || -z "$BRANCH" || -z "$BASE_BRANCH" || -z "$LOG_DIR" || -z "$RESULT_FILE" ]]; then
  echo "Error: missing required args" >&2
  usage
  exit 1
fi

command -v git >/dev/null 2>&1 || { echo "Error: git is required" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "Error: jq is required" >&2; exit 1; }

REPO_DIR="$(cd "$REPO_DIR" && pwd -P)"
mkdir -p "$LOG_DIR"
mkdir -p "$(dirname "$RESULT_FILE")"

safe_task="$(printf '%s' "$TASK_ID" | tr '[:space:]' '-' | tr -cd '[:alnum:]_.-')"
[[ -n "$safe_task" ]] || safe_task="task"
ts="$(date -u +"%Y%m%dT%H%M%SZ")"
REPORT_FILE="${LOG_DIR}/merge-fail-${safe_task}-${ts}.md"

summary_trim() {
  local raw="$1"
  raw="$(printf '%s' "$raw" | tr '\n' ' ' | tr '\r' ' ' | sed -E 's/[[:space:]]+/ /g; s/^ //; s/ $//')"
  if [[ "${#raw}" -gt 1000 ]]; then
    raw="${raw:0:1000}..."
  fi
  printf '%s' "$raw"
}

write_result() {
  local status="$1"
  local error="$2"
  local conflicts_csv="$3"
  local merge_output="$4"
  local hook_output="$5"
  local assisted="$6"
  local suggestion="$7"

  local conflicts_json='[]'
  if [[ -n "$conflicts_csv" ]]; then
    conflicts_json="$(printf '%s' "$conflicts_csv" | jq -R 'split(",") | map(select(length>0))')"
  fi

  jq -nc \
    --arg status "$status" \
    --arg task_id "$TASK_ID" \
    --arg branch "$BRANCH" \
    --arg base_branch "$BASE_BRANCH" \
    --arg report_file "$REPORT_FILE" \
    --arg error "$error" \
    --arg suggestion "$suggestion" \
    --arg merge_output "$(summary_trim "$merge_output")" \
    --arg hook_output "$(summary_trim "$hook_output")" \
    --argjson conflicts "$conflicts_json" \
    --argjson assisted "$assisted" \
    '{
      status:$status,
      task_id:$task_id,
      branch:$branch,
      base_branch:$base_branch,
      report_file:$report_file,
      error:(if ($error|length) > 0 then $error else null end),
      suggestion:(if ($suggestion|length) > 0 then $suggestion else null end),
      conflicts:$conflicts,
      merge_output:(if ($merge_output|length) > 0 then $merge_output else null end),
      hook_output:(if ($hook_output|length) > 0 then $hook_output else null end),
      assisted:$assisted,
      updated_at:(now | todateiso8601)
    }' >"$RESULT_FILE"
}

write_report() {
  local error="$1"
  local conflicts_csv="$2"
  local merge_output="$3"
  local hook_output="$4"
  local suggestion="$5"
  cat >"$REPORT_FILE" <<EOF
# Local merge failure report

- Task: ${TASK_ID}
- Branch: ${BRANCH}
- Base: ${BASE_BRANCH}
- UTC: $(date -u +"%Y-%m-%dT%H:%M:%SZ")

## Error

${error}

## Conflicts

${conflicts_csv:-none}

## Suggested manual command

\`cd "${REPO_DIR}" && ${suggestion}\`

## Merge stdout/stderr

\`\`\`text
${merge_output}
\`\`\`

## Merge-fix hook output

\`\`\`text
${hook_output}
\`\`\`
EOF
}

hook_allowed() {
  local hook="$1"
  [[ -n "$hook" ]] || return 1
  [[ -x "$hook" ]] || return 1
  local hook_real repo_real
  hook_real="$(realpath "$hook" 2>/dev/null || true)"
  repo_real="$(realpath "$REPO_DIR" 2>/dev/null || true)"
  [[ -n "$hook_real" && -n "$repo_real" ]] || return 1
  [[ "$hook_real" == "${repo_real}/.macc/automation/hooks/"* || "$hook_real" == "${repo_real}/automat/hooks/"* ]]
}

in_merge_state() {
  git -C "$REPO_DIR" rev-parse -q --verify MERGE_HEAD >/dev/null 2>&1
}

repo_is_dirty() {
  git -C "$REPO_DIR" status --porcelain | awk 'NF' | grep -q .
}

conflicts_csv() {
  git -C "$REPO_DIR" diff --name-only --diff-filter=U 2>/dev/null | paste -sd, -
}

HOOK_LAST_OUTPUT=""
run_merge_fix_hook() {
  local step="$1"
  local reason="$2"
  local conflicts="$3"
  local merge_output="$4"
  local suggestion="$5"
  HOOK_LAST_OUTPUT=""

  if [[ "$ALLOW_AI_FIX" != "true" || -z "$MERGE_FIX_HOOK" ]]; then
    return 1
  fi
  if ! hook_allowed "$MERGE_FIX_HOOK"; then
    HOOK_LAST_OUTPUT="Merge-fix hook rejected by security policy or not executable: ${MERGE_FIX_HOOK}"
    return 1
  fi

  set +e
  HOOK_LAST_OUTPUT="$(
    REPO_DIR="$REPO_DIR" \
    TASK_ID="$TASK_ID" \
    BRANCH="$BRANCH" \
    BASE_BRANCH="$BASE_BRANCH" \
    REPORT_FILE="$REPORT_FILE" \
    MACC_MERGE_FAILURE_STEP="$step" \
    MACC_MERGE_FAILURE_REASON="$reason" \
    MACC_MERGE_FAILURE_CONFLICTS="$conflicts" \
    MACC_MERGE_FAILURE_OUTPUT="$merge_output" \
    MACC_MERGE_SUGGESTION="$suggestion" \
    MACC_MERGE_REPORT_FILE="$REPORT_FILE" \
    "$MERGE_FIX_HOOK" \
      --repo "$REPO_DIR" \
      --task-id "$TASK_ID" \
      --branch "$BRANCH" \
      --base-branch "$BASE_BRANCH" \
      --failure-step "$step" \
      --failure-reason "$reason" \
      --conflicts "$conflicts" \
      --report-file "$REPORT_FILE" 2>&1
  )"
  local hook_rc=$?
  set -e
  if [[ "$hook_rc" -ne 0 ]]; then
    HOOK_LAST_OUTPUT="${HOOK_LAST_OUTPUT}\nHook exit status: ${hook_rc}"
    return 1
  fi
  return 0
}

suggestion_cmd="git checkout ${BASE_BRANCH} && git merge ${BRANCH}"

if ! git -C "$REPO_DIR" rev-parse --verify "$BRANCH" >/dev/null 2>&1; then
  err="failure:local_merge step=verify_branch branch=${BRANCH} base=${BASE_BRANCH} suggestion=\"${suggestion_cmd}\""
  write_report "$err" "" "branch not found: ${BRANCH}" "" "$suggestion_cmd"
  write_result "failed" "$err" "" "branch not found: ${BRANCH}" "" "false" "$suggestion_cmd"
  exit 1
fi
if ! git -C "$REPO_DIR" rev-parse --verify "$BASE_BRANCH" >/dev/null 2>&1; then
  err="failure:local_merge step=verify_base branch=${BRANCH} base=${BASE_BRANCH} suggestion=\"${suggestion_cmd}\""
  write_report "$err" "" "base branch not found: ${BASE_BRANCH}" "" "$suggestion_cmd"
  write_result "failed" "$err" "" "base branch not found: ${BASE_BRANCH}" "" "false" "$suggestion_cmd"
  exit 1
fi
if repo_is_dirty; then
  precheck_out="repository has uncommitted changes before merge\n$(git -C "$REPO_DIR" status --short 2>&1 || true)"
  precheck_hook_out=""
  precheck_reason="repository dirty before merge"
  if run_merge_fix_hook "precheck_clean" "$precheck_reason" "" "$precheck_out" "$suggestion_cmd"; then
    precheck_hook_out="$HOOK_LAST_OUTPUT"
    if repo_is_dirty; then
      precheck_hook_out="${precheck_hook_out}\nHook returned 0 but repository is still dirty."
    fi
  elif [[ -n "$HOOK_LAST_OUTPUT" ]]; then
    precheck_hook_out="$HOOK_LAST_OUTPUT"
  fi
  if repo_is_dirty; then
  err="failure:local_merge step=precheck_clean branch=${BRANCH} base=${BASE_BRANCH} suggestion=\"${suggestion_cmd}\""
  write_report "$err" "" "$precheck_out" "$precheck_hook_out" "$suggestion_cmd"
  write_result "failed" "$err" "" "$precheck_out" "$precheck_hook_out" "false" "$suggestion_cmd"
  exit 1
  fi
fi

checkout_out="$(git -C "$REPO_DIR" checkout "$BASE_BRANCH" 2>&1)" || {
  err="failure:local_merge step=checkout_base branch=${BRANCH} base=${BASE_BRANCH} suggestion=\"${suggestion_cmd}\""
  write_report "$err" "" "$checkout_out" "" "$suggestion_cmd"
  write_result "failed" "$err" "" "$checkout_out" "" "false" "$suggestion_cmd"
  exit 1
}

merge_msg="macc: merge task ${TASK_ID}"
set +e
merge_out="$(git -C "$REPO_DIR" merge --no-ff -m "$merge_msg" "$BRANCH" 2>&1)"
merge_rc=$?
set -e

if [[ "$merge_rc" -eq 0 ]]; then
  write_result "success" "" "" "$merge_out" "" "false" "$suggestion_cmd"
  exit 0
fi

conflicts="$(conflicts_csv)"
hook_out=""
assisted="false"

if run_merge_fix_hook "merge" "git merge reported conflicts" "$conflicts" "$merge_out" "$suggestion_cmd"; then
  hook_out="$HOOK_LAST_OUTPUT"
  if [[ -z "$(conflicts_csv)" && ! in_merge_state ]]; then
    assisted="true"
    write_result "success" "" "" "$merge_out" "$hook_out" "$assisted" "$suggestion_cmd"
    exit 0
  fi
  hook_out="${hook_out}\nHook returned 0 but merge is still unresolved."
elif [[ -n "$HOOK_LAST_OUTPUT" ]]; then
  hook_out="$HOOK_LAST_OUTPUT"
fi

if in_merge_state; then
  abort_out="$(git -C "$REPO_DIR" merge --abort 2>&1)" || true
  if [[ -n "$abort_out" ]]; then
    hook_out="${hook_out}\nmerge --abort output: ${abort_out}"
  fi
fi

err="failure:local_merge step=merge branch=${BRANCH} base=${BASE_BRANCH}"
if [[ -n "$conflicts" ]]; then
  err="${err} conflicts=[${conflicts}]"
fi
err="${err} git_output=\"$(summary_trim "$merge_out")\" suggestion=\"${suggestion_cmd}\""

write_report "$err" "$conflicts" "$merge_out" "$hook_out" "$suggestion_cmd"
write_result "failed" "$err" "$conflicts" "$merge_out" "$hook_out" "$assisted" "$suggestion_cmd"
exit 1
