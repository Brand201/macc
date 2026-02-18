#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ai-merge-fix.sh --repo <path> --task-id <id> --branch <branch> --base-branch <branch>
EOF
}

REPO_DIR=""
TASK_ID=""
BRANCH=""
BASE_BRANCH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO_DIR="$2"; shift 2 ;;
    --task-id) TASK_ID="$2"; shift 2 ;;
    --branch) BRANCH="$2"; shift 2 ;;
    --base-branch) BASE_BRANCH="$2"; shift 2 ;;
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

prompt_file="$(mktemp)"
cat >"$prompt_file" <<EOF
You are fixing a local git merge conflict in repository: ${REPO_DIR}

Context:
- task_id: ${TASK_ID}
- target base branch: ${BASE_BRANCH}
- merged branch: ${BRANCH}

Instructions:
1) Resolve ONLY current git merge conflicts in this repository.
2) Keep changes minimal and do not modify unrelated files.
3) Preserve intended behavior from both branches.
4) Complete the merge commit (git add + git commit) if conflicts are resolved.
5) If merge cannot be resolved safely, explain why and stop.
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
