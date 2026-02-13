#!/usr/bin/env bash
set -euo pipefail

################################################################################
# Ralph Wiggum - AI agent loop (Bash)
#
# PRD schema support:
# - Primary: top-level "tasks": [ ... ]   (as in prd.json.example)
# - Fallback: top-level "userStories": [ ... ]
#
# Robust JSON reading:
# - If jq fails, retry on a cleaned temp copy (strip UTF-8 BOM + CRLF).
# - ./scripts/Ralph/ralph_gemini.sh --max-iterations 14 --sandbox false --approval-policy yolo
################################################################################

MAX_ITERATIONS=10
APPROVAL_POLICY="auto_edit"
SANDBOX=false
SHOW_PROMPT=0
AI_NAME="gemini"
AI_MODEL="gemini-3-flash-preview"

usage() {
  cat <<'USAGE'
Usage:
  ralph.sh [--max-iterations N] [--approval-policy POLICY] [--sandbox SANDBOX] [--show-prompt]

Options:
  --max-iterations, -MaxIterations     Max loop iterations (default: 10)
  --approval-policy, -ApprovalPolicy  on-request | never | untrusted | on-failure (default: never)
  --sandbox, -Sandbox                 read-only | workspace-write | danger-full-access (default: workspace-write)
  --show-prompt, -ShowPrompt          Print generated prompt and exit
  -h, --help                          Show this help

Notes:
  - Expects: scripts/Ralph/prd.json (required), scripts/Ralph/progress.md (required), scripts/Ralph/AGENTS.md (required)
  - Requires: jq, git, gemini
USAGE
}

die() { echo "ERROR: $*" >&2; exit 1; }
require_cmd() { command -v "$1" >/dev/null 2>&1 || die "Missing dependency: '$1'"; }

# Parse args (supports both --long and PowerShell-like -Name)
while [[ $# -gt 0 ]]; do
  case "$1" in
    --max-iterations|-MaxIterations)
      [[ $# -ge 2 ]] || die "Missing value for $1"
      MAX_ITERATIONS="$2"; shift 2
      ;;
    --approval-policy|-ApprovalPolicy)
      [[ $# -ge 2 ]] || die "Missing value for $1"
      APPROVAL_POLICY="$2"; shift 2
      ;;
    --sandbox|-Sandbox)
      [[ $# -ge 2 ]] || die "Missing value for $1"
      SANDBOX="$2"; shift 2
      ;;
    --show-prompt|-ShowPrompt)
      SHOW_PROMPT=1; shift
      ;;
    -h|--help)
      usage; exit 0
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
done

case "$APPROVAL_POLICY" in
  default|auto_edit|yolo) ;;
  *) die "Invalid --approval-policy: $APPROVAL_POLICY" ;;
esac

case "$SANDBOX" in
  false|true) ;;
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

read_text_safe() { [[ -f "$1" ]] && cat "$1" || true; }

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
  # GNU sed on Ubuntu supports \xNN
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

# Select the tasks array per prd.json.example (primary), fallback to userStories
# Pending means: .passes != true  (covers false or missing)
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

# Read small slices of progress.md to keep prompts light
extract_codebase_patterns() {
  local progress_path="$1"
  [[ -f "$progress_path" ]] || { echo ""; return 0; }
  head -n 800 "$progress_path" | awk '
    BEGIN { in_section=0 }
    /^[[:space:]]*##[[:space:]]*Codebase Patterns[[:space:]]*$/ { in_section=1; next }
    in_section && /^[[:space:]]*##[[:space:]]+/ { exit }
    in_section { print }
  ' | sed -e 's/[[:space:]]*$//' | sed -e '/^[[:space:]]*$/d'
}

get_recent_progress() {
  local progress_path="$1"
  [[ -f "$progress_path" ]] || { echo ""; return 0; }
  tail -n 80 "$progress_path" || true
}

build_AI_prompt() {
  local prd_path="$1"
  local item_json="$2"
  local agents_text="$3"
  local repo_root="$4"

  local branch_name project_label project_value run_id patterns recent_progress current_branch
  branch_name="$(jq_safe "$prd_path" -r '.branchName // ""')"
  project_value="$(jq_safe "$prd_path" -r '(.project // .lot // "")')"
  project_label="$([[ -n "$project_value" ]] && echo "Project/Lot" || echo "Project/Lot")"
  current_branch="$(get_current_branch_name "$repo_root")"
  run_id="$AI_NAME-$AI_MODEL-$(date -u +%Y%m%dT%H%M%SZ)"
  patterns="$(extract_codebase_patterns "$PROGRESS_PATH")"
  recent_progress="$(get_recent_progress "$PROGRESS_PATH")"

  cat <<PROMPT
You are an autonomous coding agent working on a software project.

IMPORTANT: Do NOT read or print the full contents of scripts/Ralph/prd.json or scripts/Ralph/progress.md.
They may be large and can cause hangs. You already have the relevant information below.

Repository:
- Repo root: ${repo_root}
- Current branch (best-effort): ${current_branch}
- Target branch from PRD: ${branch_name:-"(not specified)"}

${project_label}: ${project_value:-"(not specified)"}

Next task to implement (ONE task only):
${item_json}

Progress log guidance:
- You must append a progress entry to scripts/Ralph/progress.md (do not replace the file).
- Use a run id like: ${run_id}

Existing Codebase Patterns (from top of progress.md):
$( [[ -n "$patterns" ]] && echo "$patterns" || echo "(none yet)" )

Recent progress context (tail of progress.md):
$( [[ -n "$recent_progress" ]] && echo "$recent_progress" || echo "(progress.md is empty)" )

Nearby agent notes (scripts/Ralph/AGENTS.md):
$( [[ -n "$agents_text" ]] && printf "%s\n" "$agents_text" || echo "(no AGENTS.md content found)" )

Workflow requirements:
1) Ensure you're on the target branch from PRD if specified (create it from main/default branch if needed).
2) Implement ONLY the task above.
3) Run the repo's quality checks (typecheck/lint/tests as appropriate).
4) Update scripts/Ralph/prd.json to set this task's passes=true.
5) Append a progress entry to scripts/Ralph/progress.md in this format:
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
The learnings section is critical - it helps future iterations avoid repeating mistakes and understand the codebase better.
7) Update the doc; README; tests or any other file if necessary.
6) Git commit ALL changes with message: feat: <TASK_ID> - <TASK_TITLE>

Output discipline to avoid hangs:
- Avoid commands that dump huge output (e.g., printing entire JSON files).
- Prefer targeted commands (e.g., head/tail, rg with limits, git diff --stat).
- If you must show logs, keep them short.

Consolidate Patterns:
If you discover a **reusable pattern** that future iterations should know, add it to the '## Codebase Patterns' section at the TOP of progress.md (create it if it doesn't exist). This section should consolidate the most important learnings:
Codebase Patterns
- Example: Use sql<number> template for aggregations
- Example: Always use IF NOT EXISTS for migrations
- Example: Export types from actions.ts for UI components
Only add patterns that are **general and reusable**, not story-specific details.

Stop condition:
Check if ALL stories have 'passes: true'.

If after your changes ALL tasks in scripts/Ralph/prd.json have passes=true, reply with EXACTLY:
<promise>COMPLETE</promise>
(on a single line). Otherwise, end normally.
If there are still stories with 'passes: false', end your response normally (another iteration will pick up the next story).
Git commit ALL changes with message: feat: <TASK_ID> - <TASK_TITLE>

Important:
- Work on ONE story per iteration
- Commit frequently
- Keep CI green
- Read the Codebase Patterns section in progress.md before starting

Now start by implementing the task.
PROMPT
}

invoke_ai() {
  local prompt_text="$1"
  local repo_root="$2"
  local sandbox="$3"
  local approval="$4"
  local log_file="$5"

  local tmp_out
  tmp_out="$(mktemp)"
  
  # Retry Configuration
  local max_retries=5
  local attempt=1
  local wait_time=60  # Start with 60 seconds (capacity issues take time to clear)

  while [[ $attempt -le $max_retries ]]; do
    
    if [[ $attempt -gt 1 ]]; then
      echo "⚠️  Retry attempt $attempt/$max_retries for Gemini API..." >&2
    fi

    set +e
    # Execute Gemini
    ( cd "$repo_root" && gemini --model $AI_MODEL --sandbox $sandbox --approval-mode "$approval" --prompt "$prompt_text" ) 2>&1 | tee "$log_file" "$tmp_out"
    local status="${PIPESTATUS[1]}"
    set -e

    # Success case
    if [[ "$status" -eq 0 ]]; then
      cat "$tmp_out"
      rm -f "$tmp_out"
      return 0
    fi

    # Failure Analysis: Check if it's a capacity issue
    local output_content
    output_content="$(cat "$tmp_out")"
    
    if echo "$output_content" | grep -qE "RESOURCE_EXHAUSTED|429|No capacity available|rateLimitExceeded"; then
      echo "🛑  Capacity/Rate Limit hit. Waiting ${wait_time}s before retry..." >&2
      sleep "$wait_time"
      
      # Exponential backoff: Increase wait time for next attempt (e.g., 60s -> 120s)
      wait_time=$((wait_time * 2))
      attempt=$((attempt + 1))
    else
      # If it's NOT a capacity error (e.g., invalid args), fail immediately
      echo "❌  Fatal error (non-retriable). Exiting." >&2
      cat "$tmp_out"
      rm -f "$tmp_out"
      return "$status"
    fi
  done

  echo "❌  Max retries ($max_retries) reached. Giving up." >&2
  rm -f "$tmp_out"
  return 1
}

extract_assistant_section() {
  awk '
    BEGIN { seen=0; buf=""; full="" }
    /^[[:space:]]*gemini[[:space:]]*$/ { seen=1; buf=""; next }
    { if (seen) buf = buf $0 "\n"; else full = full $0 "\n" }
    END { if (seen) printf "%s", buf; else printf "%s", full }
  '
}

################################################################################
# Main
################################################################################

require_cmd jq
require_cmd git
require_cmd gemini

[[ -f "$PRD_PATH" ]] || die "PRD file not found: $PRD_PATH"

# Validate JSON (retry with cleanup if needed)
jq_safe "$PRD_PATH" -e . >/dev/null 2>&1 || die "Invalid JSON in $PRD_PATH (even after cleanup)"

echo "Starting Ralph - Max iterations: $MAX_ITERATIONS"
echo "Repo root: $REPO_ROOT"
echo "Gemini sandbox: $SANDBOX | approval: $APPROVAL_POLICY"
echo

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

  item_title="$(get_next_item_title "$PRD_PATH")"
  item_pretty_json="$(get_next_item_pretty_json "$PRD_PATH")"
  agents_text="$(read_text_safe "$AGENTS_PATH")"

  prompt_text="$(build_AI_prompt "$PRD_PATH" "$item_pretty_json" "$agents_text" "$REPO_ROOT")"

  if [[ "$SHOW_PROMPT" -eq 1 ]]; then
    printf "%s\n" "$prompt_text"
    exit 0
  fi

  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  log_file="${SCRIPT_DIR}/ai_log.${item_id}.${ts}.md"
  ln -sf "$(basename "$log_file")" "${SCRIPT_DIR}/ai_log.latest.md" 2>/dev/null || true

  full_output="$(invoke_ai "$prompt_text" "$REPO_ROOT" "$SANDBOX" "$APPROVAL_POLICY" "$log_file" || true)"
  assistant_only="$(printf "%s" "$full_output" | extract_assistant_section)"

  if grep -Fq "<promise>COMPLETE</promise>" <<<"$assistant_only"; then
    echo
    echo "Ralph completed all tasks!"
    echo "Completed at iteration $i of $MAX_ITERATIONS"
    exit 0
  fi

  echo "Iteration $i complete. Continuing..."
  sleep 2
done

echo
echo "Ralph reached max iterations ($MAX_ITERATIONS) without completing all tasks."
exit 1