#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ai-merge-fix.sh --repo <path> --task-id <id> --branch <branch> --base-branch <branch> [--failure-step <step>] [--failure-reason <text>] [--conflicts <csv>] [--report-file <path>]
EOF
}

REPO_DIR=""
TASK_ID=""
BRANCH=""
BASE_BRANCH=""
FAILURE_STEP=""
FAILURE_REASON=""
FAILURE_CONFLICTS=""
REPORT_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO_DIR="$2"; shift 2 ;;
    --task-id) TASK_ID="$2"; shift 2 ;;
    --branch) BRANCH="$2"; shift 2 ;;
    --base-branch) BASE_BRANCH="$2"; shift 2 ;;
    --failure-step) FAILURE_STEP="$2"; shift 2 ;;
    --failure-reason) FAILURE_REASON="$2"; shift 2 ;;
    --conflicts) FAILURE_CONFLICTS="$2"; shift 2 ;;
    --report-file) REPORT_FILE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$REPO_DIR" || -z "$TASK_ID" || -z "$BRANCH" || -z "$BASE_BRANCH" ]]; then
  echo "Error: missing required args" >&2
  usage
  exit 1
fi

command -v jq >/dev/null 2>&1 || { echo "Error: jq is required" >&2; exit 1; }

REPO_DIR="$(cd "$REPO_DIR" && pwd -P)"
registry="${REPO_DIR}/.macc/automation/task/task_registry.json"
[[ -f "$registry" ]] || { echo "Error: registry not found: $registry" >&2; exit 1; }

if [[ -z "$FAILURE_STEP" ]]; then
  FAILURE_STEP="${MACC_MERGE_FAILURE_STEP:-unknown}"
fi
if [[ -z "$FAILURE_REASON" ]]; then
  FAILURE_REASON="${MACC_MERGE_FAILURE_REASON:-unknown}"
fi
if [[ -z "$FAILURE_CONFLICTS" ]]; then
  FAILURE_CONFLICTS="${MACC_MERGE_FAILURE_CONFLICTS:-}"
fi
if [[ -z "$REPORT_FILE" ]]; then
  REPORT_FILE="${MACC_MERGE_REPORT_FILE:-}"
fi
FAILURE_OUTPUT="${MACC_MERGE_FAILURE_OUTPUT:-}"
SUGGESTION="${MACC_MERGE_SUGGESTION:-git checkout ${BASE_BRANCH} && git merge ${BRANCH}}"

task_tool="$(jq -r --arg id "$TASK_ID" '(.tasks // [])[] | select(.id == $id) | (.tool // "")' "$registry")"
task_worktree="$(jq -r --arg id "$TASK_ID" '(.tasks // [])[] | select(.id == $id) | (.worktree.worktree_path // "")' "$registry")"
[[ -n "$task_tool" && -n "$task_worktree" ]] || {
  echo "Error: could not resolve tool/worktree for task $TASK_ID" >&2
  exit 1
}

tool_json="${task_worktree}/.macc/tool.json"
[[ -f "$tool_json" ]] || { echo "Error: tool.json not found: $tool_json" >&2; exit 1; }

runner="$(jq -r '.performer.runner // ""' "$tool_json")"
[[ -n "$runner" && "$runner" != "null" ]] || { echo "Error: performer.runner missing in tool.json" >&2; exit 1; }
if [[ "$runner" != /* ]]; then
  runner="${REPO_DIR}/${runner}"
fi
[[ -x "$runner" ]] || { echo "Error: runner not executable: $runner" >&2; exit 1; }

current_branch="$(git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
status_short="$(git -C "$REPO_DIR" status --short 2>&1 || true)"
in_merge_state="no"
if git -C "$REPO_DIR" rev-parse -q --verify MERGE_HEAD >/dev/null 2>&1; then
  in_merge_state="yes"
fi
report_snippet=""
if [[ -n "$REPORT_FILE" && -f "$REPORT_FILE" ]]; then
  report_snippet="$(sed -n '1,200p' "$REPORT_FILE" 2>/dev/null || true)"
fi

prompt_file="$(mktemp)"
cat >"$prompt_file" <<EOF
You are a git merge operator. Resolve a blocked local merge only.

Context:
- repository: ${REPO_DIR}
- task_id: ${TASK_ID}
- base branch: ${BASE_BRANCH}
- feature branch to merge: ${BRANCH}
- failure step: ${FAILURE_STEP}
- failure reason: ${FAILURE_REASON}
- conflict files (csv): ${FAILURE_CONFLICTS:-none}
- currently checked out branch: ${current_branch:-unknown}
- merge in progress (MERGE_HEAD present): ${in_merge_state}
- suggested merge command: ${SUGGESTION}

Recent git status (short):
\`\`\`text
${status_short}
\`\`\`

Failure output:
\`\`\`text
${FAILURE_OUTPUT}
\`\`\`

Merge report excerpt:
\`\`\`text
${report_snippet}
\`\`\`

Instructions:
1) Work ONLY on the merge of branch '${BRANCH}' into '${BASE_BRANCH}'.
2) Do not edit unrelated files. Do not run formatter/linter outside files touched by merge.
3) If merge is not in progress, build it now with:
   git checkout ${BASE_BRANCH}
   git merge --no-ff ${BRANCH}
4) If conflicts exist, resolve them and finish the merge commit.
5) Verify result with:
   git status --short
   git log --oneline -n 3
6) If unresolved, output the precise blocking reason and stop.
EOF

set +e
"$runner" \
  --prompt-file "$prompt_file" \
  --tool-json "$tool_json" \
  --repo "$REPO_DIR" \
  --worktree "$REPO_DIR" \
  --task-id "$TASK_ID" \
  --attempt 1 \
  --max-attempts 1
rc=$?
set -e

rm -f "$prompt_file"
exit "$rc"
