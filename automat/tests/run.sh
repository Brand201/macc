#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT="${ROOT_DIR}/automat/coordinator.sh"
FIXTURES="${ROOT_DIR}/automat/tests/fixtures"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing dependency: $1" >&2
    exit 1
  }
}

assert_eq() {
  local expected="$1"
  local actual="$2"
  local msg="$3"
  if [[ "$expected" != "$actual" ]]; then
    echo "Assertion failed: $msg (expected='$expected', actual='$actual')" >&2
    exit 1
  fi
}

setup_repo() {
  local dir
  dir="$(mktemp -d)"
  git -C "$dir" init -q
  echo "init" >"$dir/README.md"
  git -C "$dir" add README.md
  git -C "$dir" commit -q -m "init"
  echo "$dir"
}

make_stub_macc() {
  local dir="$1"
  local bin_dir="$dir/bin"
  mkdir -p "$bin_dir"
  cat >"$bin_dir/macc" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail

if [[ "$1" == "--cwd" ]]; then
  shift 2
fi

if [[ "$1" != "worktree" ]]; then
  echo "unsupported" >&2
  exit 1
fi

if [[ "${2:-}" == "run" ]]; then
  # Simulate successful performer execution.
  exit 0
fi

if [[ "${2:-}" == "apply" ]]; then
  # Simulate successful worktree apply.
  exit 0
fi

if [[ "${2:-}" != "create" ]]; then
  echo "unsupported" >&2
  exit 1
fi

slug="$3"
shift 3

tool="codex"
base="main"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tool) tool="$2"; shift 2 ;;
    --base) base="$2"; shift 2 ;;
    --count) shift 2 ;;
    --skip-apply) shift ;;
    *) shift ;;
  esac
done

root="$(pwd)"
suffix="$(date +%s)"
name="${slug}-${suffix}"
path="${root}/.macc/worktree/${name}"
branch="ai/task/${name}"

mkdir -p "$path"
git -C "$path" init -q
echo "worktree" >"$path/README.md"
git -C "$path" add README.md
git -C "$path" commit -q -m "init"

echo "Created 1 worktree(s):"
echo "  ${name}  branch=${branch} base=${base} path=${path}"
EOS
  chmod +x "$bin_dir/macc"
  echo "$bin_dir"
}

test_dependency_gating() {
  local repo registry output
  repo="$(setup_repo)"
  registry="$repo/.macc/automation/task/task_registry.json"
  output="$(
    PATH="$(make_stub_macc "$repo"):$PATH" \
    PRD_FILE="${FIXTURES}/prd_deps_blocked.json" \
    REPO_DIR="$repo" \
    MAX_DISPATCH=2 \
    "$SCRIPT"
  )"
  local claimed_a claimed_b
  claimed_a="$(jq -r '.tasks[] | select(.id=="TASK-A") | .state' "$registry")"
  claimed_b="$(jq -r '.tasks[] | select(.id=="TASK-B") | .state' "$registry")"
  assert_eq "in_progress" "$claimed_a" "base task should dispatch first"
  assert_eq "todo" "$claimed_b" "dependent task should remain todo"
  rm -rf "$repo"
}


test_dependency_ready() {
  local repo registry
  repo="$(setup_repo)"
  registry="$repo/.macc/automation/task/task_registry.json"
  PATH="$(make_stub_macc "$repo"):$PATH" \
  PRD_FILE="${FIXTURES}/prd_deps.json" \
  REPO_DIR="$repo" \
  MAX_DISPATCH=1 \
  "$SCRIPT" >/dev/null
  local in_progress
  in_progress="$(jq -r '.tasks[] | select(.state=="in_progress") | .id' "$registry")"
  assert_eq "TASK-B" "$in_progress" "dependent task should dispatch when deps merged"
  rm -rf "$repo"
}

test_lock_contention() {
  local repo registry
  repo="$(setup_repo)"
  registry="$repo/.macc/automation/task/task_registry.json"
  PATH="$(make_stub_macc "$repo"):$PATH" \
  PRD_FILE="${FIXTURES}/prd_locks.json" \
  REPO_DIR="$repo" \
  MAX_DISPATCH=2 \
  "$SCRIPT" >/dev/null
  local active
  active="$(jq -r '[.tasks[] | select(.state=="in_progress") | .id] | length' "$registry")"
  assert_eq "1" "$active" "exclusive lock should allow only one active task"
  rm -rf "$repo"
}

test_rerun_idempotency() {
  local repo registry
  repo="$(setup_repo)"
  registry="$repo/.macc/automation/task/task_registry.json"
  PATH="$(make_stub_macc "$repo"):$PATH" \
  PRD_FILE="${FIXTURES}/prd_single.json" \
  REPO_DIR="$repo" \
  MAX_DISPATCH=1 \
  "$SCRIPT" >/dev/null 2>&1
  local first_state
  first_state="$(jq -r '.tasks[] | select(.id=="TASK-ONLY") | .state' "$registry")"
  PATH="$(make_stub_macc "$repo"):$PATH" \
  PRD_FILE="${FIXTURES}/prd_single.json" \
  REPO_DIR="$repo" \
  MAX_DISPATCH=1 \
  "$SCRIPT" >/dev/null 2>&1
  local second_state
  second_state="$(jq -r '.tasks[] | select(.id=="TASK-ONLY") | .state' "$registry")"
  assert_eq "$first_state" "$second_state" "rerun should be idempotent"
  rm -rf "$repo"
}

test_failure_rollback() {
  local repo registry
  repo="$(setup_repo)"
  registry="$repo/.macc/automation/task/task_registry.json"
  PATH="$repo/bin:$PATH"
  mkdir -p "$repo/bin"
  cat >"$repo/bin/macc" <<'EOS'
#!/usr/bin/env bash
echo "fail" >&2
exit 1
EOS
  chmod +x "$repo/bin/macc"

  PRD_FILE="${FIXTURES}/prd_single.json" \
  REPO_DIR="$repo" \
  MAX_DISPATCH=1 \
  "$SCRIPT" >/dev/null 2>&1

  local state
  state="$(jq -r '.tasks[] | select(.id=="TASK-ONLY") | .state' "$registry")"
  assert_eq "blocked" "$state" "failed worktree create should block task"
  rm -rf "$repo"
}

test_transition_and_cleanup_cycle() {
  local repo registry
  repo="$(setup_repo)"
  registry="$repo/.macc/automation/task/task_registry.json"
  local stub_path
  stub_path="$(make_stub_macc "$repo")"

  PATH="$stub_path:$PATH" \
  PRD_FILE="${FIXTURES}/prd_single.json" \
  REPO_DIR="$repo" \
  MAX_DISPATCH=1 \
  "$SCRIPT" dispatch >/dev/null

  local state
  state="$(jq -r '.tasks[] | select(.id=="TASK-ONLY") | .state' "$registry")"
  assert_eq "in_progress" "$state" "task should be in progress after dispatch"
  jq '.tasks |= map(if .id=="TASK-ONLY" then .worktree = null else . end)' "$registry" >"${registry}.tmp"
  mv "${registry}.tmp" "$registry"

  PATH="$stub_path:$PATH" REPO_DIR="$repo" \
    PRD_FILE="${FIXTURES}/prd_single.json" \
    "$SCRIPT" --transition TASK-ONLY --state pr_open --pr-url "http://example/pr/1" >/dev/null
  PATH="$stub_path:$PATH" REPO_DIR="$repo" \
    PRD_FILE="${FIXTURES}/prd_single.json" \
    "$SCRIPT" --transition TASK-ONLY --state queued >/dev/null
  PATH="$stub_path:$PATH" REPO_DIR="$repo" \
    PRD_FILE="${FIXTURES}/prd_single.json" \
    "$SCRIPT" --transition TASK-ONLY --state merged >/dev/null

  PATH="$stub_path:$PATH" REPO_DIR="$repo" \
    PRD_FILE="${FIXTURES}/prd_single.json" \
    "$SCRIPT" reconcile >/dev/null
  PATH="$stub_path:$PATH" REPO_DIR="$repo" \
    "$SCRIPT" cleanup >/dev/null
  PATH="$stub_path:$PATH" REPO_DIR="$repo" \
    "$SCRIPT" unlock --all >/dev/null

  state="$(jq -r '.tasks[] | select(.id=="TASK-ONLY") | .state' "$registry")"
  assert_eq "merged" "$state" "task should be merged at end of cycle"

  local worktree lock_count
  worktree="$(jq -r '.tasks[] | select(.id=="TASK-ONLY") | .worktree' "$registry")"
  assert_eq "null" "$worktree" "worktree must be cleared after merged"
  lock_count="$(jq -r '(.resource_locks // {}) | length' "$registry")"
  assert_eq "0" "$lock_count" "locks must be released after cleanup/unlock"

  rm -rf "$repo"
}

test_advance_full_cycle_with_vcs_hook() {
  local repo registry hook
  repo="$(setup_repo)"
  registry="$repo/.macc/automation/task/task_registry.json"
  mkdir -p "$(dirname "$registry")"

  cat >"$registry" <<'JSON'
{
  "schema_version": 1,
  "tasks": [
    {
      "id": "TASK-FULL",
      "title": "Full cycle task",
      "state": "in_progress",
      "dependencies": [],
      "exclusive_resources": ["r1"],
      "tool": "codex",
      "worktree": {
        "worktree_path": "/tmp/wt-task-full",
        "branch": "ai/task-full",
        "base_branch": "master",
        "last_commit": "abc"
      }
    }
  ],
  "resource_locks": {
    "r1": {
      "task_id": "TASK-FULL",
      "worktree_path": "/tmp/wt-task-full",
      "locked_at": "2026-01-01T00:00:00Z"
    }
  },
  "state_mapping": {}
}
JSON

  hook="$repo/vcs-hook.sh"
  cat >"$hook" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail
mode="${1:-}"
case "$mode" in
  pr_create) echo '{"pr_url":"https://example.test/pr/42"}' ;;
  review_status) echo '{"decision":"approved","reviewer":"ci-bot"}' ;;
  ci_status) echo '{"status":"green"}' ;;
  queue_status) echo '{"status":"ready"}' ;;
  merge_status) echo '{"status":"merged"}' ;;
  *) echo '{}' ;;
esac
EOS
  chmod +x "$hook"

  REPO_DIR="$repo" \
  COORDINATOR_VCS_HOOK="$hook" \
  "$SCRIPT" advance >/dev/null

  local state lock_count pr_url
  state="$(jq -r '.tasks[] | select(.id=="TASK-FULL") | .state' "$registry")"
  assert_eq "merged" "$state" "advance should drive task to merged via hook"
  pr_url="$(jq -r '.tasks[] | select(.id=="TASK-FULL") | (.pr_url // "")' "$registry")"
  assert_eq "https://example.test/pr/42" "$pr_url" "advance should persist PR URL"
  lock_count="$(jq -r '(.resource_locks // {}) | length' "$registry")"
  assert_eq "0" "$lock_count" "locks must be released when merged"

  rm -rf "$repo"
}

lint_shell() {
  if command -v shellcheck >/dev/null 2>&1; then
    shellcheck "$SCRIPT"
  else
    echo "shellcheck not installed; skipping lint"
  fi
}

require_cmd git
require_cmd jq
require_cmd bash

lint_shell
test_dependency_gating
test_dependency_ready
test_lock_contention
test_rerun_idempotency
test_failure_rollback
test_transition_and_cleanup_cycle
test_advance_full_cycle_with_vcs_hook

echo "All coordinator tests passed."
