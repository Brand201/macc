#!/usr/bin/env bash
set -euo pipefail

################################################################################
# Ralph Wiggum - Codex CLI agent loop (Bash)
#
# Optimizations based on Codex pricing + Codex CLI docs:
# - Keep prompts lean (smaller prompts cost fewer credits and fit higher limits)
# - Truncate AGENTS.md / progress context to avoid wasting context budget
# - Prefer the mini model for simple local tasks; escalate only when needed
# - Use Codex CLI exec options like --output-last-message for reliable scripting
################################################################################

# Loop + safety defaults
MAX_ITERATIONS=10
APPROVAL_POLICY="never"          # Consider "on-request" for safer local execs.
SANDBOX="workspace-write"
SHOW_PROMPT=0

# Cost-aware model routing (local tasks)
PRIMARY_MODEL="gpt-5.1-codex-mini"
RETRY_MODEL="gpt-5.1-codex-max" # "gpt-5.2-codex"      # Better at complex/refactor-y tasks.

# Prompt budget knobs (keep these modest to avoid burning credits)
PROMPT_MAX_BYTES=35000
TASK_JSON_MAX_BYTES=9000
AGENTS_MAX_LINES=160
AGENTS_MAX_BYTES=12000
PATTERNS_HEAD_LINES=450
PATTERNS_MAX_BYTES=9000
PROGRESS_TAIL_LINES=60
PROGRESS_TAIL_MAX_BYTES=9000

# Codex CLI knobs
CODEX_PROFILE=""
CODEX_SEARCH=0
CODEX_CONFIG_OVERRIDES=()   # Repeatable: --config key=value

AI_NAME="codex"

usage() {
  cat <<'USAGE'
Usage:
  ralph_optimized.sh [options]

Core options:
  --max-iterations, -MaxIterations N     Max loop iterations (default: 10)
  --approval-policy, -ApprovalPolicy P  on-request | never | untrusted | on-failure (default: never)
  --sandbox, -Sandbox S                 read-only | workspace-write | danger-full-access (default: workspace-write)
  --show-prompt, -ShowPrompt            Print generated prompt and exit

Model routing:
  --model, -Model M                     Primary local model (default: gpt-5.1-codex-mini)
  --retry-model, -RetryModel M          Model used when the same task is retried (default: gpt-5.2-codex)

Codex CLI passthrough:
  --profile, -Profile NAME              Codex CLI config profile to load (maps to `codex --profile`)
  --search, -Search                     Enable Codex web search (maps to `codex --search`)
  --config, -Config key=value           Override Codex config value (repeatable)

Prompt budgeting:
  --prompt-max-bytes N                  Hard cap for total prompt bytes (default: 35000)
  --agents-max-lines N                  Cap lines included from AGENTS.md (default: 160)
  --agents-max-bytes N                  Cap bytes included from AGENTS.md (default: 12000)
  --task-json-max-bytes N               Cap bytes for inline task JSON (default: 9000)
  --patterns-head-lines N               How many lines of progress.md to scan for patterns (default: 450)
  --patterns-max-bytes N                Cap bytes for extracted patterns section (default: 9000)
  --progress-tail-lines N               Tail lines from progress.md to include (default: 60)
  --progress-tail-max-bytes N           Cap bytes for tail snippet (default: 9000)

Notes:
  - Expects: scripts/Ralph/prd.json, scripts/Ralph/progress.md, scripts/Ralph/AGENTS.md
  - Requires: jq, git, codex
USAGE
}

die() { echo "ERROR: $*" >&2; exit 1; }
require_cmd() { command -v "$1" >/dev/null 2>&1 || die "Missing dependency: '$1'"; }

is_uint() { [[ "${1:-}" =~ ^[0-9]+$ ]]; }

# Parse args (supports both --long and PowerShell-like -Name)
while [[ $# -gt 0 ]]; do
  case "$1" in
    --max-iterations|-MaxIterations)
      [[ $# -ge 2 ]] || die "Missing value for $1"; is_uint "$2" || die "Invalid integer for $1";
      MAX_ITERATIONS="$2"; shift 2;;
    --approval-policy|-ApprovalPolicy)
      [[ $# -ge 2 ]] || die "Missing value for $1";
      APPROVAL_POLICY="$2"; shift 2;;
    --sandbox|-Sandbox)
      [[ $# -ge 2 ]] || die "Missing value for $1";
      SANDBOX="$2"; shift 2;;
    --show-prompt|-ShowPrompt)
      SHOW_PROMPT=1; shift;;

    --model|-Model)
      [[ $# -ge 2 ]] || die "Missing value for $1";
      PRIMARY_MODEL="$2"; shift 2;;
    --retry-model|-RetryModel)
      [[ $# -ge 2 ]] || die "Missing value for $1";
      RETRY_MODEL="$2"; shift 2;;

    --profile|-Profile)
      [[ $# -ge 2 ]] || die "Missing value for $1";
      CODEX_PROFILE="$2"; shift 2;;
    --search|-Search)
      CODEX_SEARCH=1; shift;;
    --config|-Config)
      [[ $# -ge 2 ]] || die "Missing value for $1";
      CODEX_CONFIG_OVERRIDES+=("$2"); shift 2;;

    --prompt-max-bytes)
      [[ $# -ge 2 ]] || die "Missing value for $1"; is_uint "$2" || die "Invalid integer for $1";
      PROMPT_MAX_BYTES="$2"; shift 2;;
    --task-json-max-bytes)
      [[ $# -ge 2 ]] || die "Missing value for $1"; is_uint "$2" || die "Invalid integer for $1";
      TASK_JSON_MAX_BYTES="$2"; shift 2;;
    --agents-max-lines)
      [[ $# -ge 2 ]] || die "Missing value for $1"; is_uint "$2" || die "Invalid integer for $1";
      AGENTS_MAX_LINES="$2"; shift 2;;
    --agents-max-bytes)
      [[ $# -ge 2 ]] || die "Missing value for $1"; is_uint "$2" || die "Invalid integer for $1";
      AGENTS_MAX_BYTES="$2"; shift 2;;
    --patterns-head-lines)
      [[ $# -ge 2 ]] || die "Missing value for $1"; is_uint "$2" || die "Invalid integer for $1";
      PATTERNS_HEAD_LINES="$2"; shift 2;;
    --patterns-max-bytes)
      [[ $# -ge 2 ]] || die "Missing value for $1"; is_uint "$2" || die "Invalid integer for $1";
      PATTERNS_MAX_BYTES="$2"; shift 2;;
    --progress-tail-lines)
      [[ $# -ge 2 ]] || die "Missing value for $1"; is_uint "$2" || die "Invalid integer for $1";
      PROGRESS_TAIL_LINES="$2"; shift 2;;
    --progress-tail-max-bytes)
      [[ $# -ge 2 ]] || die "Missing value for $1"; is_uint "$2" || die "Invalid integer for $1";
      PROGRESS_TAIL_MAX_BYTES="$2"; shift 2;;

    -h|--help)
      usage; exit 0;;
    *)
      die "Unknown argument: $1";;
  esac
done

case "$APPROVAL_POLICY" in
  on-request|never|untrusted|on-failure) ;;
  *) die "Invalid --approval-policy: $APPROVAL_POLICY" ;;
esac

case "$SANDBOX" in
  read-only|workspace-write|danger-full-access) ;;
  *) die "Invalid --sandbox: $SANDBOX" ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRD_PATH="${SCRIPT_DIR}/prd.json"
PROGRESS_PATH="${SCRIPT_DIR}/progress.md"
AGENTS_PATH="${SCRIPT_DIR}/AGENTS.md"

REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$REPO_ROOT" ]]; then
  REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
fi

# Cap text by (lines, bytes). Bytes cap applies after line cap.
cap_file() {
  local path="$1"; local max_lines="$2"; local max_bytes="$3"
  [[ -f "$path" ]] || { echo ""; return 0; }
  head -n "$max_lines" "$path" | head -c "$max_bytes" || true
}

cap_text() {
  local max_bytes="$1"
  head -c "$max_bytes" || true
}

get_current_branch_name() {
  local repo_root="$1"
  local head_path="${repo_root}/.git/HEAD"
  if [[ -f "$head_path" ]]; then
    local head
    head="$(cat "$head_path" 2>/dev/null || true)"
    if [[ "$head" =~ ^ref:\ refs/heads/ ]]; then
      echo "${head#ref: refs/heads/}" | tr -d '\r\n'
      return 0
    fi
    if [[ -n "$head" ]]; then
      echo "$head" | tr -d '\r\n'
      return 0
    fi
  fi
  git -C "$repo_root" rev-parse --abbrev-ref HEAD 2>/dev/null || true
}

# Create a cleaned copy of JSON:
# - Strip UTF-8 BOM on the first line
# - Remove CR characters (CRLF -> LF)
clean_json_to_tmp() {
  local src="$1"
  local tmp
  tmp="$(mktemp)"
  sed '1s/^\xEF\xBB\xBF//' "$src" | tr -d '\r' > "$tmp"
  echo "$tmp"
}

# jq wrapper that retries on a cleaned temp copy if initial parse fails.
# Usage: jq_safe <file> <jq args...>
jq_safe() {
  local file="$1"; shift
  if jq "$@" "$file" 2>/dev/null; then
    return 0
  fi
  local tmp
  tmp="$(clean_json_to_tmp "$file")"
  jq "$@" "$tmp"
  rm -f "$tmp"
}

# Select the tasks array (primary) or userStories (fallback)
JQ_ITEMS='(.tasks // .userStories // [])'

get_next_item_json() {
  local prd_path="$1"
  jq_safe "$prd_path" -c "
    ${JQ_ITEMS}
    | map(select(.passes != true))
    | .[0] // empty
  "
}

get_next_item_pretty_json() {
  local prd_path="$1"
  jq_safe "$prd_path" "
    ${JQ_ITEMS}
    | map(select(.passes != true))
    | .[0] // empty
  "
}

get_next_item_id() {
  local prd_path="$1"
  jq_safe "$prd_path" -r "
    ${JQ_ITEMS}
    | map(select(.passes != true))
    | .[0].id // \"\"
  "
}

get_next_item_title() {
  local prd_path="$1"
  jq_safe "$prd_path" -r "
    ${JQ_ITEMS}
    | map(select(.passes != true))
    | .[0].title // \"\"
  "
}

get_prd_branch_name() {
  local prd_path="$1"
  jq_safe "$prd_path" -r '.branchName // ""'
}

get_prd_project_value() {
  local prd_path="$1"
  jq_safe "$prd_path" -r '(.project // .lot // "")'
}

# Extract a small, reusable "Codebase Patterns" section from progress.md.
extract_codebase_patterns() {
  local progress_path="$1"
  [[ -f "$progress_path" ]] || { echo ""; return 0; }

  # Only scan the head to avoid huge prompts.
  head -n "$PATTERNS_HEAD_LINES" "$progress_path" | awk '
    BEGIN { in_section=0 }
    /^[[:space:]]*##[[:space:]]*Codebase Patterns[[:space:]]*$/ { in_section=1; next }
    in_section && /^[[:space:]]*##[[:space:]]+/ { exit }
    in_section { print }
  ' | sed -e 's/[[:space:]]*$//' | sed -e '/^[[:space:]]*$/d' | head -c "$PATTERNS_MAX_BYTES"
}

get_recent_progress() {
  local progress_path="$1"
  [[ -f "$progress_path" ]] || { echo ""; return 0; }
  tail -n "$PROGRESS_TAIL_LINES" "$progress_path" | head -c "$PROGRESS_TAIL_MAX_BYTES" || true
}

# Minimal task summary to keep context lean when task JSON is huge.
# (We do not assume any schema beyond id/title/description/acceptanceCriteria.)
get_task_summary_block() {
  local prd_path="$1"
  jq_safe "$prd_path" -r "
    ${JQ_ITEMS}
    | map(select(.passes != true))
    | .[0]
    | \"- ID: \(.id // \\\"\\\")\n- Title: \(.title // \\\"\\\")\n- Description: \(.description // \\\"\\\")\n- Acceptance criteria: \((.acceptanceCriteria // .acceptance_criteria // []) | if type==\\\"array\\\" then (join(\\\" | \\\")) else tostring end)\" 
  " 2>/dev/null || true
}

build_ai_prompt() {
  local prd_path="$1"
  local item_pretty_json="$2"
  local repo_root="$3"
  local model_used="$4"

  local branch_name project_value current_branch run_id patterns recent_progress agents_text
  branch_name="$(get_prd_branch_name "$prd_path")"
  project_value="$(get_prd_project_value "$prd_path")"
  current_branch="$(get_current_branch_name "$repo_root")"
  run_id="$AI_NAME-$model_used-$(date -u +%Y%m%dT%H%M%SZ)"

  # Capped context reads
  patterns="$(extract_codebase_patterns "$PROGRESS_PATH")"
  recent_progress="$(get_recent_progress "$PROGRESS_PATH")"
  agents_text="$(cap_file "$AGENTS_PATH" "$AGENTS_MAX_LINES" "$AGENTS_MAX_BYTES")"

  # If task JSON is very large, replace it with a summary block.
  local task_block
  if [[ "${#item_pretty_json}" -gt "$TASK_JSON_MAX_BYTES" ]]; then
    task_block="(Task JSON omitted due to size; summary below.)\n\n$(get_task_summary_block "$prd_path")"
  else
    task_block="$item_pretty_json"
  fi

  # Compose prompt sections, then hard-cap total bytes as a last resort.
  {
    cat <<PROMPT
You are an autonomous coding agent working on a software project.

Cost/efficiency guidelines (important):
- Keep commands and outputs concise.
- Prefer targeted file reads (rg/sed/head/tail) over dumping large files.
- Avoid unnecessary context. Sections below may be truncated to stay within a prompt budget.

IMPORTANT: Do NOT read or print the full contents of scripts/Ralph/prd.json or scripts/Ralph/progress.md.
They may be large and can cause hangs. You already have the relevant information below.

Repository:
- Repo root: ${repo_root}
- Current branch (best-effort): ${current_branch}
- Target branch from PRD: ${branch_name:-"(not specified)"}

Project/Lot: ${project_value:-"(not specified)"}

Next task to implement (ONE task only):
${task_block}

Progress log guidance:
- You must append a progress entry to scripts/Ralph/progress.md (do not replace the file).
- Use a run id like: ${run_id}

Existing Codebase Patterns (from the top of progress.md; may be truncated):
$( [[ -n "$patterns" ]] && echo "$patterns" || echo "(none yet)" )

Recent progress context (tail of progress.md; may be truncated):
$( [[ -n "$recent_progress" ]] && echo "$recent_progress" || echo "(progress.md is empty)" )

Nearby agent notes (scripts/Ralph/AGENTS.md; may be truncated):
$( [[ -n "$agents_text" ]] && printf "%s\n" "$agents_text" || echo "(no AGENTS.md content found)" )

Workflow requirements:
1) Ensure you're on the target branch from PRD if specified (create it from main/default branch if needed).
2) Implement ONLY the task above.
3) Read the macc_resume.md file to understand project context. If it's large, search within it and only read relevant sections.
4) Run the repo's quality checks (typecheck/lint/tests as appropriate).
5) Update scripts/Ralph/prd.json to set this task's passes=true.
6) Append a progress entry to scripts/Ralph/progress.md in this format:
## [Date/Time] - <TASK_ID>
Run: ${run_id}
- What was implemented
- Files changed
- **Learnings for future iterations:**
  - Patterns discovered
  - Gotchas encountered
  - Useful context
---
Include the thread URL so future iterations can use the 'read_thread' tool to reference previous work if needed.
7) Update docs/README/tests if necessary.
8) Git commit ALL changes with message: feat: <TASK_ID> - <TASK_TITLE>

Output discipline to avoid hangs:
- Avoid commands that dump huge output (e.g., printing entire JSON files).
- Prefer targeted commands (e.g., head/tail, rg with limits, git diff --stat).
- If you must show logs, keep them short.

Consolidate Patterns:
If you discover a reusable pattern, add it to '## Codebase Patterns' at the TOP of progress.md.
Only add patterns that are general and reusable.

Stop condition:
Check if ALL stories have 'passes: true'.

If after your changes ALL tasks in scripts/Ralph/prd.json have passes=true, reply with EXACTLY:
<promise>COMPLETE</promise>
(on a single line). Otherwise, end normally.

Now start by implementing the task.
PROMPT
  } | cap_text "$PROMPT_MAX_BYTES"
}

build_codex_base_cmd() {
  local model_used="$1"; local sandbox="$2"; local approval="$3"; local -n _out="$4"
  _out=(codex "--model" "$model_used" "--sandbox" "$sandbox" "--ask-for-approval" "$approval")

  if [[ -n "$CODEX_PROFILE" ]]; then
    _out+=("--profile" "$CODEX_PROFILE")
  fi

  if [[ "$CODEX_SEARCH" -eq 1 ]]; then
    _out+=("--search")
  fi

  for kv in "${CODEX_CONFIG_OVERRIDES[@]}"; do
    _out+=("--config" "$kv")
  done
}

invoke_ai() {
  local prompt_text="$1"
  local repo_root="$2"
  local sandbox="$3"
  local approval="$4"
  local model_used="$5"
  local log_file="$6"
  local assistant_msg_file="$7"

  local -a base_cmd cmd
  build_codex_base_cmd "$model_used" "$sandbox" "$approval" base_cmd

  # Preferred path: use -C/--cd to set workspace root.
  cmd=("${base_cmd[@]}" "-C" "$repo_root" exec "--color" "never" "--output-last-message" "$assistant_msg_file" "-")

  set +e
  printf "%s" "$prompt_text" | "${cmd[@]}" 2>&1 | tee "$log_file"
  local status="${PIPESTATUS[1]}"
  set -e

  # Back-compat: older codex builds may not support -C/--cd; retry with an explicit cd.
  if [[ "$status" -ne 0 ]] && grep -qiE 'unknown (flag|option).* -C|unrecognized option.*-C|flag provided but not defined' "$log_file"; then
    local oldpwd="$PWD"
    cd "$repo_root"

    cmd=("${base_cmd[@]}" exec "--color" "never" "--output-last-message" "$assistant_msg_file" "-")

    set +e
    printf "%s" "$prompt_text" | "${cmd[@]}" 2>&1 | tee "$log_file"
    status="${PIPESTATUS[1]}"
    set -e

    cd "$oldpwd"
  fi

  return "$status"
}

################################################################################
# Main
################################################################################

require_cmd jq
require_cmd git
require_cmd codex

[[ -f "$PRD_PATH" ]] || die "PRD file not found: $PRD_PATH"

# Validate JSON (retry with cleanup if needed)
jq_safe "$PRD_PATH" -e . >/dev/null 2>&1 || die "Invalid JSON in $PRD_PATH (even after cleanup)"

echo "Starting Ralph (optimized) - Max iterations: $MAX_ITERATIONS"
echo "Repo root: $REPO_ROOT"
echo "Codex sandbox: $SANDBOX | approval: $APPROVAL_POLICY"
echo "Primary model: $PRIMARY_MODEL | Retry model: $RETRY_MODEL"
if [[ -n "$CODEX_PROFILE" ]]; then echo "Codex profile: $CODEX_PROFILE"; fi
if [[ "$CODEX_SEARCH" -eq 1 ]]; then echo "Codex web search: enabled"; fi
echo

last_item_id=""
item_attempt=0

for ((i=1; i<=MAX_ITERATIONS; i++)); do
  echo "═══════════════════════════════════════════════════════"
  echo "  Ralph Iteration $i of $MAX_ITERATIONS"
  echo "═══════════════════════════════════════════════════════"

  item_id="$(get_next_item_id "$PRD_PATH")"
  if [[ -z "$item_id" ]]; then
    echo "No pending tasks found (all passes=true)."
    echo "<promise>COMPLETE</promise>"
    exit 0
  fi

  if [[ "$item_id" == "$last_item_id" ]]; then
    item_attempt=$((item_attempt + 1))
  else
    item_attempt=1
    last_item_id="$item_id"
  fi

  item_title="$(get_next_item_title "$PRD_PATH")"
  item_pretty_json="$(get_next_item_pretty_json "$PRD_PATH")"

  # Escalate model on retries for the same task to improve completion odds.
  model_used="$PRIMARY_MODEL"
  if [[ "$item_attempt" -ge 2 ]]; then
    model_used="$RETRY_MODEL"
  fi

  prompt_text="$(build_ai_prompt "$PRD_PATH" "$item_pretty_json" "$REPO_ROOT" "$model_used")"

  if [[ "$SHOW_PROMPT" -eq 1 ]]; then
    printf "%s\n" "$prompt_text"
    exit 0
  fi

  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  log_file="${SCRIPT_DIR}/ai_log.${item_id}.${ts}.md"
  assistant_msg_file="$(mktemp)"
  ln -sf "$(basename "$log_file")" "${SCRIPT_DIR}/ai_log.latest.md" 2>/dev/null || true

  set +e
  invoke_ai "$prompt_text" "$REPO_ROOT" "$SANDBOX" "$APPROVAL_POLICY" "$model_used" "$log_file" "$assistant_msg_file"
  codex_status=$?
  set -e

  # Check completion from the final assistant message (reliable + small).
  if grep -Fqx "<promise>COMPLETE</promise>" "$assistant_msg_file"; then
    echo
    echo "Ralph completed all tasks!"
    echo "Completed at iteration $i of $MAX_ITERATIONS"
    rm -f "$assistant_msg_file"
    exit 0
  fi

  rm -f "$assistant_msg_file"

  if [[ "$codex_status" -ne 0 ]]; then
    echo "Codex exited with status $codex_status (iteration will retry if task still pending)."
  fi

  echo "Iteration $i complete. Continuing..."
  sleep 2

done

echo
echo "Ralph reached max iterations ($MAX_ITERATIONS) without completing all tasks."
exit 1
