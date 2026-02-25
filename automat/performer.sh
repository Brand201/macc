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
EVENT_FILE="${COORD_EVENTS_FILE:-}"
EVENT_SOURCE="${MACC_EVENT_SOURCE:-}"
EVENT_TASK_ID="${MACC_EVENT_TASK_ID:-}"
EVENT_RUN_ID="${COORDINATOR_RUN_ID:-$(date +%s%N)-$$}"
EVENT_SEQ=0
EVENT_SEQ_FILE=""
HEARTBEAT_PID=""
CURRENT_PHASE="dev"
LAST_ERROR_CODE=""
LAST_ERROR_ORIGIN=""
LAST_ERROR_MESSAGE=""
TERMINAL_EVENT_EMITTED="false"

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

if [[ -z "$EVENT_SOURCE" ]]; then
  EVENT_SOURCE="performer:${tool:-unknown}:${EVENT_RUN_ID}"
fi
if [[ -z "$EVENT_TASK_ID" ]]; then
  EVENT_TASK_ID="${task_id:-unknown}"
fi

if [[ -z "$repo" || -z "$worktree" || -z "$task_id" || -z "$tool" || -z "$registry" || -z "$prd" ]]; then
  LAST_ERROR_CODE="E901"
  LAST_ERROR_ORIGIN="performer"
  LAST_ERROR_MESSAGE="missing required args"
  echo "Error: missing required args" >&2
  usage
  exit 1
fi

if [[ ! -d "$worktree" ]]; then
  LAST_ERROR_CODE="E301"
  LAST_ERROR_ORIGIN="performer"
  LAST_ERROR_MESSAGE="worktree path does not exist"
  echo "Error: worktree path does not exist: $worktree" >&2
  exit 1
fi

if [[ ! -f "$prd" ]]; then
  LAST_ERROR_CODE="E302"
  LAST_ERROR_ORIGIN="performer"
  LAST_ERROR_MESSAGE="PRD file not found"
  echo "Error: PRD file not found: $prd" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  LAST_ERROR_CODE="E901"
  LAST_ERROR_ORIGIN="performer"
  LAST_ERROR_MESSAGE="jq is required"
  echo "Error: jq is required" >&2
  exit 1
fi

cd "$worktree"

tool_json="${worktree}/.macc/tool.json"

if [[ ! -f "$tool_json" ]]; then
  LAST_ERROR_CODE="E303"
  LAST_ERROR_ORIGIN="performer"
  LAST_ERROR_MESSAGE="tool.json not found"
  echo "Error: tool.json not found in worktree: $tool_json" >&2
  exit 1
fi

if [[ -n "$EVENT_FILE" ]]; then
  EVENT_SEQ_FILE="${worktree}/.macc/tmp/event-seq-${EVENT_RUN_ID}.txt"
  mkdir -p "$(dirname "$EVENT_SEQ_FILE")"
  printf '0\n' >"$EVENT_SEQ_FILE"
fi

on_exit() {
  local rc=$?
  heartbeat_stop
  if [[ "$rc" -ne 0 && "$TERMINAL_EVENT_EMITTED" != "true" ]]; then
    emit_performer_event "failed" "$CURRENT_PHASE" "failed" "$(build_error_payload "$rc")"
    TERMINAL_EVENT_EMITTED="true"
  fi
  if [[ -n "${EVENT_SEQ_FILE:-}" ]]; then
    rm -f "$EVENT_SEQ_FILE" "${EVENT_SEQ_FILE}.lock" >/dev/null 2>&1 || true
  fi
}
trap on_exit EXIT

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

next_event_seq() {
  if [[ -z "${EVENT_SEQ_FILE:-}" ]]; then
    EVENT_SEQ=$((EVENT_SEQ + 1))
    echo "$EVENT_SEQ"
    return 0
  fi

  local lock_file="${EVENT_SEQ_FILE}.lock"
  local current=0
  while ! mkdir "$lock_file" 2>/dev/null; do
    sleep 0.01
  done
  if [[ -f "$EVENT_SEQ_FILE" ]]; then
    current="$(cat "$EVENT_SEQ_FILE" 2>/dev/null || echo 0)"
  fi
  [[ "$current" =~ ^[0-9]+$ ]] || current=0
  current=$((current + 1))
  printf '%s\n' "$current" >"$EVENT_SEQ_FILE"
  rmdir "$lock_file" >/dev/null 2>&1 || true
  echo "$current"
}

emit_performer_event() {
  local event_type="$1"
  local phase="${2:-}"
  local status="${3:-}"
  local payload_json="${4:-{}}"
  [[ -n "$EVENT_FILE" ]] || return 0
  [[ -n "$EVENT_SOURCE" ]] || EVENT_SOURCE="performer:${tool}:${EVENT_RUN_ID}"
  [[ -n "$EVENT_TASK_ID" ]] || EVENT_TASK_ID="$task_id"
  local seq
  seq="$(next_event_seq)"
  if ! jq -e 'type == "object"' <<<"$payload_json" >/dev/null 2>&1; then
    payload_json="$(jq -nc --arg value "$payload_json" '{value:$value}')"
  fi
  jq -nc \
    --arg schema_version "1" \
    --arg event_id "${EVENT_TASK_ID}-${seq}-$(date +%s%N)" \
    --arg run_id "$EVENT_RUN_ID" \
    --argjson seq "$seq" \
    --arg ts "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
    --arg source "$EVENT_SOURCE" \
    --arg task_id "$EVENT_TASK_ID" \
    --arg type "$event_type" \
    --arg phase "$phase" \
    --arg status "$status" \
    --argjson payload "$payload_json" \
    '{
      schema_version:$schema_version,
      event_id:$event_id,
      run_id:$run_id,
      seq:$seq,
      ts:$ts,
      source:$source,
      task_id:$task_id,
      type:$type,
      phase:($phase|select(length>0)),
      status:$status,
      payload:$payload,
      event:$type,
      state:$status
    }' >>"$EVENT_FILE" 2>/dev/null || true
}

set_last_error() {
  local code="$1"
  local origin="$2"
  local message="$3"
  LAST_ERROR_CODE="$code"
  LAST_ERROR_ORIGIN="$origin"
  LAST_ERROR_MESSAGE="$message"
}

build_error_payload() {
  local exit_code="$1"
  if ! command -v jq >/dev/null 2>&1; then
    printf '{"exit_code":%s}' "${exit_code:-0}"
    return 0
  fi
  jq -nc \
    --arg code "$LAST_ERROR_CODE" \
    --arg origin "$LAST_ERROR_ORIGIN" \
    --arg msg "$LAST_ERROR_MESSAGE" \
    --arg exit "$exit_code" \
    '{
      exit_code:($exit|tonumber?),
      error_code:($code|select(length>0)),
      origin:($origin|select(length>0)),
      message:($msg|select(length>0))
    }'
}

heartbeat_start() {
  [[ -n "$EVENT_FILE" ]] || return 0
  heartbeat_stop
  (
    while true; do
      emit_performer_event "heartbeat" "$CURRENT_PHASE" "running" '{}'
      sleep 2
    done
  ) &
  HEARTBEAT_PID=$!
}

heartbeat_stop() {
  if [[ -n "${HEARTBEAT_PID:-}" ]]; then
    kill "$HEARTBEAT_PID" >/dev/null 2>&1 || true
    wait "$HEARTBEAT_PID" >/dev/null 2>&1 || true
    HEARTBEAT_PID=""
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
  local output_capture
  local script
  script="$(tool_runner_path)"
  if [[ -z "$script" || ! -x "$script" ]]; then
    set_last_error "E102" "performer" "tool runner not found or not executable"
    echo "Error: tool performer not found or not executable: ${script}" >&2
    return 1
  fi
  output_capture="$(mktemp)"

  log_task_line "## Attempt ${attempt}/${max_attempts}"
  log_task_line ""
  log_task_line "- Runner: \`${script}\`"
  log_task_line "- Started: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  log_task_line ""
  log_task_line '```text'
  set +e
  emit_performer_event "progress" "$CURRENT_PHASE" "running" "$(jq -nc --arg attempt "$attempt" --arg max "$max_attempts" '{attempt:($attempt|tonumber?), max_attempts:($max|tonumber?)}')"
  spinner_start "Running ${tool} (attempt ${attempt}/${max_attempts})"
  "$script" \
    --prompt-file "$prompt_file" \
    --tool-json "$tool_json" \
    --repo "$repo" \
    --worktree "$worktree" \
    --task-id "$task_id" \
    --attempt "$attempt" \
    --max-attempts "$max_attempts" 2>&1 | tee "$output_capture" >>"$task_log_file"
  local status=${PIPESTATUS[0]}
  spinner_stop "Runner finished (${tool})"
  set -e

  if [[ "$status" -eq 0 ]]; then
    emit_performer_event "phase_result" "$CURRENT_PHASE" "done" "$(jq -nc --arg attempt "$attempt" '{attempt:($attempt|tonumber?)}')"
  else
    set_last_error "E101" "runner" "runner exited non-zero"
    emit_performer_event "phase_result" "$CURRENT_PHASE" "failed" "$(jq -nc --arg attempt "$attempt" --arg status "$status" --arg code "$LAST_ERROR_CODE" --arg origin "$LAST_ERROR_ORIGIN" --arg message "$LAST_ERROR_MESSAGE" '{attempt:($attempt|tonumber?), exit_status:($status|tonumber?), error_code:($code|select(length>0)), origin:($origin|select(length>0)), message:($message|select(length>0))}')"
  fi
  log_task_line '```'
  log_task_line ""
  log_task_line "- Exit status: ${status}"
  log_task_line ""
  rm -f "$output_capture"
  return "$status"
}

commit_changes() {
  local last_id="$1"
  local last_title="$2"

  if git status --porcelain | awk 'NF' | grep -q .; then
    local git_add_output=""
    local git_commit_output=""
    # Stage everything first; protected files are un-staged right after.
    if ! git_add_output="$(git add -A 2>&1)"; then
      git_add_output="${git_add_output//$'\n'/ }"
      set_last_error "E202" "git" "git add failed: ${git_add_output:0:240}"
      return 1
    fi
    git reset -q HEAD -- performer.sh worktree.prd.json >/dev/null 2>&1 || true
    if git diff --cached --quiet; then
      if git status --porcelain -- performer.sh worktree.prd.json | awk 'NF' | grep -q .; then
        echo "No committable changes (protected files excluded: performer.sh, worktree.prd.json)."
      else
        echo "No changes to commit."
      fi
      return 0
    fi
    local msg="feat: ${last_id}"
    if [[ -n "$last_title" ]]; then
      msg="feat: ${last_id} - ${last_title}"
    fi
    if ! git_commit_output="$(git commit -m "$msg" 2>&1)"; then
      git_commit_output="${git_commit_output//$'\n'/ }"
      set_last_error "E201" "git" "git commit failed: ${git_commit_output:0:240}"
      return 1
    fi
    printf '%s\n' "$git_commit_output"
    local sha
    sha="$(git rev-parse HEAD 2>/dev/null || true)"
    emit_performer_event "commit_created" "$CURRENT_PHASE" "done" "$(jq -nc --arg sha "$sha" --arg message "$msg" '{sha:$sha, message:$message}')"
    echo "Committed changes: $msg"
  else
    echo "No changes to commit."
  fi
}

last_id=""
last_title=""
emit_performer_event "started" "$CURRENT_PHASE" "started" "$(jq -nc --arg tool "$tool" --arg worktree "$worktree" '{tool:$tool, worktree:$worktree}')"
heartbeat_start

for ((i=1; i<=PERFORMER_MAX_ITERATIONS; i++)); do
  next_task_json="$(get_next_task_json)"
  if [[ -z "$next_task_json" ]]; then
    commit_changes "$last_id" "$last_title"
    emit_performer_event "phase_result" "$CURRENT_PHASE" "done" '{}'
    TERMINAL_EVENT_EMITTED="true"
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
  emit_performer_event "progress" "$CURRENT_PHASE" "running" "$(jq -nc --arg task "$next_id" --arg title "$next_title" '{task_id:$task, title:$title}')"

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
    else
      attempt_rc=$?
      echo "Tool failed for task ${next_id} (attempt ${attempt}/${PERFORMER_TOOL_MAX_ATTEMPTS})" >&2
    fi
  done
  if [[ "$tool_success" != "true" ]]; then
    rm -f "$prompt_file"
    if [[ -z "$LAST_ERROR_CODE" ]]; then
      set_last_error "E101" "runner" "tool execution failed"
    fi
    emit_performer_event "failed" "$CURRENT_PHASE" "failed" "$(jq -nc --arg task "$next_id" --arg code "$LAST_ERROR_CODE" --arg origin "$LAST_ERROR_ORIGIN" --arg message "$LAST_ERROR_MESSAGE" '{task_id:$task, reason:"tool execution failed", error_code:($code|select(length>0)), origin:($origin|select(length>0)), message:($message|select(length>0))}')"
    TERMINAL_EVENT_EMITTED="true"
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
    emit_performer_event "phase_result" "$CURRENT_PHASE" "done" "$(jq -nc --arg task "$next_id" '{task_id:$task, final:true}')"
    TERMINAL_EVENT_EMITTED="true"
    exit 0
  fi

  sleep "$PERFORMER_SLEEP_SECONDS"
done

echo "Error: max iterations reached (${PERFORMER_MAX_ITERATIONS})" >&2
exit 1
