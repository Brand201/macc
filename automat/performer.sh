#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  performer.sh --repo <path> --worktree <path> --task-id <id> --tool <tool> --registry <path> --prd <path>

Env vars:
  PERFORMER_MAX_ITERATIONS  Max tasks to run before stopping (default: 50)
  PERFORMER_TOOL_MAX_ATTEMPTS Max attempts per task (default: 2)
  PERFORMER_SLEEP_SECONDS   Pause between tasks (default: 2)
EOF
}

repo=""
worktree=""
task_id=""
tool=""
registry=""
prd=""
performer_log_dir=""
task_log_file=""

PERFORMER_MAX_ITERATIONS="${PERFORMER_MAX_ITERATIONS:-50}"
PERFORMER_TOOL_MAX_ATTEMPTS="${PERFORMER_TOOL_MAX_ATTEMPTS:-2}"
PERFORMER_SLEEP_SECONDS="${PERFORMER_SLEEP_SECONDS:-2}"
PERFORMER_SPINNER="${PERFORMER_SPINNER:-true}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) repo="$2"; shift 2 ;;
    --worktree) worktree="$2"; shift 2 ;;
    --task-id) task_id="$2"; shift 2 ;;
    --tool) tool="$2"; shift 2 ;;
    --registry) registry="$2"; shift 2 ;;
    --prd) prd="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$repo" || -z "$worktree" || -z "$task_id" || -z "$tool" || -z "$registry" || -z "$prd" ]]; then
  echo "Error: missing required args" >&2
  usage
  exit 1
fi

if [[ ! -d "$worktree" ]]; then
  echo "Error: worktree path does not exist: $worktree" >&2
  exit 1
fi

if [[ ! -f "$prd" ]]; then
  echo "Error: PRD file not found: $prd" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "Error: jq is required" >&2
  exit 1
fi

cd "$worktree"

tool_json="${worktree}/.macc/tool.json"

if [[ ! -f "$tool_json" ]]; then
  echo "Error: tool.json not found in worktree: $tool_json" >&2
  exit 1
fi

performer_log_dir="${worktree}/.macc/log/performer"
mkdir -p "$performer_log_dir"

task_log_path() {
  local id="$1"
  local safe
  safe="$(echo "$id" | tr '[:space:]' '-' | tr -cd '[:alnum:]_.-')"
  if [[ -z "$safe" ]]; then
    safe="task"
  fi
  echo "${performer_log_dir}/${safe}.md"
}

log_task_header_if_needed() {
  local path="$1"
  local id="$2"
  local title="$3"
  if [[ ! -f "$path" ]]; then
    cat >"$path" <<EOF
# Performer log for task ${id}

- Tool: ${tool}
- Worktree: ${worktree}
- PRD: ${prd}

EOF
  fi
}

log_task_line() {
  local msg="$1"
  if [[ -n "$task_log_file" ]]; then
    printf '%s\n' "$msg" >>"$task_log_file"
  fi
}

spinner_enabled() {
  if [[ -n "${CI:-}" || -n "${MACC_NO_SPINNER:-}" ]]; then
    return 1
  fi
  if [[ "${PERFORMER_SPINNER}" != "true" ]]; then
    return 1
  fi
  [[ -t 2 ]]
}

spinner_start() {
  local msg="$1"
  if ! spinner_enabled; then
    return 0
  fi
  SPINNER_MSG="$msg"
  (
    local frames='|/-\'
    local i=0
    while true; do
      local ch="${frames:i%4:1}"
      printf '\r[%s] %s' "$ch" "$SPINNER_MSG" >&2
      i=$((i + 1))
      sleep 0.1
    done
  ) &
  SPINNER_PID=$!
}

spinner_stop() {
  local msg="$1"
  if [[ -n "${SPINNER_PID:-}" ]]; then
    kill "$SPINNER_PID" >/dev/null 2>&1 || true
    wait "$SPINNER_PID" >/dev/null 2>&1 || true
    SPINNER_PID=""
    if spinner_enabled; then
      printf '\r[done] %s\n' "$msg" >&2
    fi
  fi
}

tool_runner_path() {
  local runner
  runner="$(jq -r '.performer.runner // ""' "$tool_json")"
  if [[ -z "$runner" || "$runner" == "null" ]]; then
    echo ""
    return
  fi
  if [[ "$runner" = /* ]]; then
    echo "$runner"
  else
    echo "${repo}/${runner}"
  fi
}

JQ_ITEMS='
def task_items:
  if type == "array" then .
  elif type == "object" then (.tasks // .userStories // [])
  else []
  end;
task_items
'

get_next_task_json() {
  jq -c "${JQ_ITEMS} | map(select(.passes != true)) | .[0] // empty" "$prd"
}

get_next_task_id() {
  jq -r "${JQ_ITEMS} | map(select(.passes != true)) | .[0].id // \"\"" "$prd"
}

get_next_task_title() {
  jq -r "${JQ_ITEMS} | map(select(.passes != true)) | .[0].title // \"\"" "$prd"
}

mark_task_passed() {
  local id="$1"
  local tmp
  tmp="$(mktemp)"
  jq --arg id "$id" '
    def match_id($t):
      (($t.id|tostring) == $id);
    if type == "array" then
      map(if match_id(.) then .passes = true else . end)
    elif type == "object" then
      (if ((.tasks | type) == "array") then
         .tasks |= map(if match_id(.) then .passes = true else . end)
       else
         .
       end)
      | (if ((.userStories | type) == "array") then
           .userStories |= map(if match_id(.) then .passes = true else . end)
         else
           .
         end)
    else
      .
    end
  ' "$prd" >"$tmp"
  mv "$tmp" "$prd"
}

pending_task_count() {
  jq -r "${JQ_ITEMS} | map(select(.passes != true)) | length" "$prd"
}

build_prompt() {
  local task_json="$1"
  local task_id="$2"
  local task_title="$3"
  cat <<PROMPT
You are an autonomous coding agent working inside a MACC worktree.

Context:
- Worktree: ${worktree}
- Task file: ${prd}
- Task ID: ${task_id}
- Task Title: ${task_title}

Task (JSON):
${task_json}

Instructions:
1) Implement ONLY the task above.
2) Do NOT edit ${prd}; the runner will update it.
3) Do NOT commit; the runner will commit if all tasks are done.
4) Keep output concise; avoid dumping large files.

Now implement the task.
PROMPT
}

run_tool() {
  local prompt_file="$1"
  local attempt="$2"
  local max_attempts="$3"
  local script
  script="$(tool_runner_path)"
  if [[ -z "$script" || ! -x "$script" ]]; then
    echo "Error: tool performer not found or not executable: ${script}" >&2
    return 1
  fi

  log_task_line "## Attempt ${attempt}/${max_attempts}"
  log_task_line ""
  log_task_line "- Runner: \`${script}\`"
  log_task_line "- Started: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  log_task_line ""
  log_task_line '```text'
  set +e
  spinner_start "Running ${tool} (attempt ${attempt}/${max_attempts})"
  "$script" \
    --prompt-file "$prompt_file" \
    --tool-json "$tool_json" \
    --repo "$repo" \
    --worktree "$worktree" \
    --task-id "$task_id" \
    --attempt "$attempt" \
    --max-attempts "$max_attempts" >>"$task_log_file" 2>&1
  local status=$?
  spinner_stop "Runner finished (${tool})"
  set -e
  log_task_line '```'
  log_task_line ""
  log_task_line "- Exit status: ${status}"
  log_task_line ""
  return "$status"
}

commit_changes() {
  local last_id="$1"
  local last_title="$2"

  if git status --porcelain | awk 'NF' | grep -q .; then
    git add -A
    local msg="feat: ${last_id}"
    if [[ -n "$last_title" ]]; then
      msg="feat: ${last_id} - ${last_title}"
    fi
    git commit -m "$msg"
    echo "Committed changes: $msg"
  else
    echo "No changes to commit."
  fi
}

last_id=""
last_title=""

for ((i=1; i<=PERFORMER_MAX_ITERATIONS; i++)); do
  next_task_json="$(get_next_task_json)"
  if [[ -z "$next_task_json" ]]; then
    commit_changes "$last_id" "$last_title"
    exit 0
  fi

  next_id="$(get_next_task_id)"
  next_title="$(get_next_task_title)"
  task_log_file="$(task_log_path "$next_id")"
  log_task_header_if_needed "$task_log_file" "$next_id" "$next_title"
  log_task_line "## Processing task ${next_id}"
  log_task_line ""
  log_task_line "- Title: ${next_title}"
  log_task_line "- Started: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  log_task_line ""
  echo "Performer: task ${next_id} (${tool})"

  prompt_file="$(mktemp)"
  build_prompt "$next_task_json" "$next_id" "$next_title" >"$prompt_file"
  log_task_line "### Prompt"
  log_task_line ""
  log_task_line '```text'
  cat "$prompt_file" >>"$task_log_file"
  log_task_line '```'
  log_task_line ""

  tool_success=false
  for ((attempt=1; attempt<=PERFORMER_TOOL_MAX_ATTEMPTS; attempt++)); do
    if run_tool "$prompt_file" "$attempt" "$PERFORMER_TOOL_MAX_ATTEMPTS"; then
      tool_success=true
      break
    fi
    echo "Tool failed for task ${next_id} (attempt ${attempt}/${PERFORMER_TOOL_MAX_ATTEMPTS})" >&2
  done
  if [[ "$tool_success" != "true" ]]; then
    rm -f "$prompt_file"
    echo "Error: tool execution failed for task ${next_id}" >&2
    exit 1
  fi
  rm -f "$prompt_file"

  mark_task_passed "$next_id"
  log_task_line "- Marked as passed in worktree PRD: ${next_id}"
  log_task_line "- Completed: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  log_task_line ""

  last_id="$next_id"
  last_title="$next_title"

  if [[ "$(pending_task_count)" -eq 0 ]]; then
    commit_changes "$last_id" "$last_title"
    exit 0
  fi

  sleep "$PERFORMER_SLEEP_SECONDS"
done

echo "Error: max iterations reached (${PERFORMER_MAX_ITERATIONS})" >&2
exit 1
