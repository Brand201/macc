#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  gemini.performer.sh --prompt-file <path> --tool-json <path> [--repo <path>] [--worktree <path>] [--task-id <id>] [--attempt N] [--max-attempts N]
EOF
}

prompt_file=""
tool_json=""
repo=""
worktree=""
task_id=""
attempt="1"
max_attempts="1"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prompt-file) prompt_file="$2"; shift 2 ;;
    --tool-json) tool_json="$2"; shift 2 ;;
    --repo) repo="$2"; shift 2 ;;
    --worktree) worktree="$2"; shift 2 ;;
    --task-id) task_id="$2"; shift 2 ;;
    --attempt) attempt="$2"; shift 2 ;;
    --max-attempts) max_attempts="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$prompt_file" || ! -f "$prompt_file" ]]; then
  echo "Error: prompt file missing: $prompt_file" >&2
  exit 1
fi

if [[ -z "$tool_json" ]]; then
  tool_json=".macc/tool.json"
fi
if [[ ! -f "$tool_json" ]]; then
  echo "Error: tool.json missing: $tool_json" >&2
  exit 1
fi

if [[ -z "$repo" ]]; then
  repo="$(pwd)"
fi
if [[ -z "$worktree" ]]; then
  worktree="$(pwd)"
fi

command="$(jq -r '.performer.command // empty' "$tool_json")"
if [[ -z "$command" ]]; then
  echo "Error: performer.command missing in tool.json" >&2
  exit 1
fi
if ! command -v "$command" >/dev/null 2>&1; then
  echo "Error: tool command not found in PATH: $command" >&2
  exit 1
fi

tool_id="$(jq -r '.id // empty' "$tool_json")"
if [[ -z "$tool_id" || "$tool_id" == "null" ]]; then
  tool_id="tool"
fi

session_enabled="$(jq -r '.performer.session.enabled // false' "$tool_json")"
session_scope="$(jq -r '.performer.session.scope // "worktree"' "$tool_json")"
session_init_prompt="$(jq -r '.performer.session.init_prompt // "Bonjour"' "$tool_json")"
session_extract_regex="$(jq -r '.performer.session.extract_regex // "session[[:space:]]+id:[[:space:]]*([[:alnum:]-]+)"' "$tool_json")"
session_resume_command="$(jq -r '.performer.session.resume.command // empty' "$tool_json")"
session_discover_command="$(jq -r '.performer.session.discover.command // empty' "$tool_json")"
session_id_strategy="$(jq -r '.performer.session.id_strategy // "discovered"' "$tool_json")"
session_state_file="${repo}/.macc/state/tool-sessions.json"
session_lock_dir="${session_state_file}.lock"
session_lease_ttl="${SESSION_LEASE_TTL_SECONDS:-1800}"
mkdir -p "$(dirname "$session_state_file")"

session_key() {
  if [[ "$session_scope" == "project" ]]; then
    echo "project"
  else
    echo "$worktree"
  fi
}

acquire_session_lock() {
  local attempts=0
  until mkdir "$session_lock_dir" 2>/dev/null; do
    attempts=$((attempts + 1))
    if [[ "$attempts" -ge 80 ]]; then
      echo "Error: timed out acquiring session lock: $session_lock_dir" >&2
      return 1
    fi
    sleep 0.1
  done
}

release_session_lock() {
  rmdir "$session_lock_dir" >/dev/null 2>&1 || true
}

read_session_id() {
  local key
  key="$(session_key)"
  [[ -f "$session_state_file" ]] || { echo ""; return 0; }
  jq -r --arg tool "$tool_id" --arg key "$key" '
    .tools[$tool].sessions[$key].session_id // empty
  ' "$session_state_file"
}

now_iso() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

now_epoch() {
  date -u +%s
}

lease_owner_worktree() {
  local sid="$1"
  [[ -f "$session_state_file" ]] || { echo ""; return 0; }
  jq -r --arg tool "$tool_id" --arg sid "$sid" '
    .tools[$tool].leases[$sid].owner_worktree // empty
  ' "$session_state_file"
}

lease_status() {
  local sid="$1"
  [[ -f "$session_state_file" ]] || { echo ""; return 0; }
  jq -r --arg tool "$tool_id" --arg sid "$sid" '
    .tools[$tool].leases[$sid].status // empty
  ' "$session_state_file"
}

lease_heartbeat_epoch() {
  local sid="$1"
  [[ -f "$session_state_file" ]] || { echo "0"; return 0; }
  jq -r --arg tool "$tool_id" --arg sid "$sid" '
    (.tools[$tool].leases[$sid].heartbeat_epoch // 0) | tostring
  ' "$session_state_file"
}

worktree_is_alive() {
  local wt="$1"
  [[ -n "$wt" ]] && [[ -d "$wt" ]] && [[ -e "$wt/.git" ]]
}

session_occupied_by_other() {
  local sid="$1"
  local owner status hb now age
  [[ -n "$sid" ]] || return 1

  owner="$(lease_owner_worktree "$sid")"
  status="$(lease_status "$sid")"
  hb="$(lease_heartbeat_epoch "$sid")"
  [[ "$hb" =~ ^[0-9]+$ ]] || hb=0

  if [[ -z "$owner" || "$owner" == "$worktree" ]]; then
    return 1
  fi
  if [[ "$status" != "active" ]]; then
    return 1
  fi
  if ! worktree_is_alive "$owner"; then
    return 1
  fi

  now="$(now_epoch)"
  age=$((now - hb))
  if (( age > session_lease_ttl )); then
    return 1
  fi
  return 0
}

write_active_lease() {
  local sid="$1"
  local key now ts tmp
  key="$(session_key)"
  now="$(now_iso)"
  ts="$(now_epoch)"
  tmp="$(mktemp)"

  if [[ -f "$session_state_file" ]]; then
    jq \
      --arg tool "$tool_id" \
      --arg key "$key" \
      --arg sid "$sid" \
      --arg now "$now" \
      --arg wt "$worktree" \
      --arg tid "$task_id" \
      --arg pid "$$" \
      --argjson hb "$ts" '
      .tools = (.tools // {}) |
      .tools[$tool] = (.tools[$tool] // {}) |
      .tools[$tool].sessions = (.tools[$tool].sessions // {}) |
      .tools[$tool].leases = (.tools[$tool].leases // {}) |
      .tools[$tool].sessions[$key] = { session_id: $sid, updated_at: $now } |
      .tools[$tool].leases[$sid] = {
        owner_worktree: $wt,
        owner_task_id: $tid,
        owner_pid: $pid,
        status: "active",
        heartbeat_epoch: $hb,
        updated_at: $now
      }
      ' "$session_state_file" >"$tmp"
  else
    jq -n \
      --arg tool "$tool_id" \
      --arg key "$key" \
      --arg sid "$sid" \
      --arg now "$now" \
      --arg wt "$worktree" \
      --arg tid "$task_id" \
      --arg pid "$$" \
      --argjson hb "$ts" '
      {
        tools: {
          ($tool): {
            sessions: {
              ($key): { session_id: $sid, updated_at: $now }
            },
            leases: {
              ($sid): {
                owner_worktree: $wt,
                owner_task_id: $tid,
                owner_pid: $pid,
                status: "active",
                heartbeat_epoch: $hb,
                updated_at: $now
              }
            }
          }
        }
      }
      ' >"$tmp"
  fi

  mv "$tmp" "$session_state_file"
}

mark_lease_status() {
  local sid="$1"
  local status="$2"
  local now ts tmp
  [[ -n "$sid" ]] || return 0
  [[ -f "$session_state_file" ]] || return 0
  now="$(now_iso)"
  ts="$(now_epoch)"
  tmp="$(mktemp)"
  jq \
    --arg tool "$tool_id" \
    --arg sid "$sid" \
    --arg status "$status" \
    --arg now "$now" \
    --argjson hb "$ts" '
    .tools = (.tools // {}) |
    .tools[$tool] = (.tools[$tool] // {}) |
    .tools[$tool].leases = (.tools[$tool].leases // {}) |
    if (.tools[$tool].leases[$sid] // null) != null then
      .tools[$tool].leases[$sid].status = $status |
      .tools[$tool].leases[$sid].heartbeat_epoch = $hb |
      .tools[$tool].leases[$sid].updated_at = $now
    else
      .
    end
    ' "$session_state_file" >"$tmp"
  mv "$tmp" "$session_state_file"
}

extract_session_id_from_output() {
  local output_file="$1"
  local regex="$2"
  local found=""
  shopt -s nocasematch
  while IFS= read -r line; do
    if [[ "$line" =~ $regex ]]; then
      found="${BASH_REMATCH[1]}"
    fi
  done <"$output_file"
  shopt -u nocasematch
  printf "%s" "$found"
}

run_and_capture() {
  local output_file="$1"
  shift
  local rc=0
  "$@" 2>&1 | tee "$output_file"
  rc=${PIPESTATUS[0]}
  return "$rc"
}

run_resume_and_capture() {
  local output_file="$1"
  local sid="$2"
  local prompt="$3"
  local resume_args=()
  local arg

  while IFS= read -r arg; do
    resume_args+=("${arg//\{session_id\}/$sid}")
  done < <(jq -r '.performer.session.resume.args[]?' "$tool_json")

  if [[ "$prompt_mode" == "arg" && -n "$prompt_arg" ]]; then
    run_and_capture "$output_file" "$session_resume_command" "${resume_args[@]}" "$prompt_arg" "$prompt"
  else
    run_and_capture "$output_file" "$session_resume_command" "${resume_args[@]}" "$prompt"
  fi
}

discover_session_id() {
  local output_file="$1"
  local discover_args=()
  local arg
  local sid=""
  local last_line=""

  if [[ -z "$session_discover_command" ]]; then
    echo ""
    return 0
  fi

  while IFS= read -r arg; do
    discover_args+=("$arg")
  done < <(jq -r '.performer.session.discover.args[]?' "$tool_json")

  run_and_capture "$output_file" "$session_discover_command" "${discover_args[@]}" >/dev/null || true
  sid="$(extract_session_id_from_output "$output_file" "$session_extract_regex")"
  if [[ -n "$sid" ]]; then
    echo "$sid"
    return 0
  fi

  last_line="$(awk 'NF{line=$0} END{print line}' "$output_file" | tr -d '\r')"
  echo "$last_line"
}

generate_session_id() {
  if command -v uuidgen >/dev/null 2>&1; then
    uuidgen | tr -d '\r'
    return 0
  fi
  if [[ -r /proc/sys/kernel/random/uuid ]]; then
    cat /proc/sys/kernel/random/uuid | tr -d '\r'
    return 0
  fi
  date -u +%Y%m%dT%H%M%S%N
}

reserve_generated_session_id() {
  local attempts=0
  local sid=""
  while [[ "$attempts" -lt 10 ]]; do
    sid="$(generate_session_id)"
    [[ -n "$sid" ]] || {
      attempts=$((attempts + 1))
      continue
    }
    if ! session_occupied_by_other "$sid"; then
      write_active_lease "$sid"
      active_session_id="$sid"
      printf "%s" "$sid"
      return 0
    fi
    attempts=$((attempts + 1))
  done
  return 1
}

args=()
if [[ "$attempt" -gt 1 ]] && jq -e '.performer.retry' "$tool_json" >/dev/null 2>&1; then
  command="$(jq -r '.performer.retry.command // .performer.command' "$tool_json")"
  while IFS= read -r arg; do
    args+=("$arg")
  done < <(jq -r '.performer.retry.args[]?' "$tool_json")
else
  while IFS= read -r arg; do
    args+=("$arg")
  done < <(jq -r '.performer.args[]?' "$tool_json")
fi

prompt_mode="$(jq -r '.performer.prompt.mode // "stdin"' "$tool_json")"
prompt_arg="$(jq -r '.performer.prompt.arg // empty' "$tool_json")"
prompt_text="$(cat "$prompt_file")"
output_capture="$(mktemp)"
active_session_id=""

cleanup_runner() {
  if [[ -n "$active_session_id" ]]; then
    if acquire_session_lock; then
      mark_lease_status "$active_session_id" "released" || true
      release_session_lock
    fi
  fi
  rm -f "$output_capture"
}
trap cleanup_runner EXIT

run_default_call() {
  if [[ "$prompt_mode" == "arg" ]]; then
    if [[ -z "$prompt_arg" ]]; then
      echo "Error: performer.prompt.arg required for arg mode" >&2
      return 1
    fi
    run_and_capture "$output_capture" "$command" "${args[@]}" "$prompt_arg" "$prompt_text"
  else
    local rc=0
    printf "%s" "$prompt_text" | "$command" "${args[@]}" 2>&1 | tee "$output_capture"
    rc=${PIPESTATUS[1]}
    return "$rc"
  fi
}

if [[ "$session_enabled" == "true" && -n "$session_resume_command" ]]; then
  sid=""
  rc=0

  if acquire_session_lock; then
    sid="$(read_session_id)"
    if session_occupied_by_other "$sid"; then
      sid=""
    fi
    if [[ -n "$sid" ]]; then
      write_active_lease "$sid"
      active_session_id="$sid"
    fi
    release_session_lock
  fi

  if [[ -z "$sid" && "$session_id_strategy" == "generated" ]]; then
    sid=""
    if acquire_session_lock; then
      sid="$(reserve_generated_session_id || true)"
      release_session_lock
    fi
  fi

  if [[ -n "$sid" ]]; then
    if ! run_resume_and_capture "$output_capture" "$sid" "$prompt_text"; then
      rc=$?
      if [[ "$attempt" -eq 1 ]]; then
        run_default_call || rc=$?
      fi
    fi
  else
    run_default_call || rc=$?
  fi

  new_sid="$(extract_session_id_from_output "$output_capture" "$session_extract_regex")"
  if [[ -z "$new_sid" && "$attempt" -eq 1 && "$session_id_strategy" == "discovered" ]]; then
    discovery_capture="$(mktemp)"
    new_sid="$(discover_session_id "$discovery_capture")"
    rm -f "$discovery_capture"
  fi
  if [[ -z "$new_sid" && -n "$sid" && "$session_id_strategy" == "generated" ]]; then
    new_sid="$sid"
  fi
  if [[ -n "$new_sid" ]]; then
    if acquire_session_lock; then
      if ! session_occupied_by_other "$new_sid"; then
        write_active_lease "$new_sid"
        active_session_id="$new_sid"
      fi
      release_session_lock
    fi
  fi
  exit "$rc"
else
  run_default_call
fi
