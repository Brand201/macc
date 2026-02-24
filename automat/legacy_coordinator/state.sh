# shellcheck shell=bash

now_iso() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

coordinator_storage_mode_is_json() {
  [[ "${COORDINATOR_STORAGE_MODE:-sqlite}" == "json" ]]
}

sanitize_slug() {
  echo "$1" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//'
}

safe_slug() {
  local slug
  slug="$(sanitize_slug "$1")"
  [[ -n "$slug" ]] || slug="task"
  echo "$slug"
}

ensure_prd_valid() {
  [[ -f "$PRD_FILE" ]] || {
    echo "Error: PRD file not found: $PRD_FILE" >&2
    exit 1
  }
  jq -e '.tasks and (.tasks | type == "array")' "$PRD_FILE" >/dev/null 2>&1 || {
    echo "Error: PRD must be valid JSON with a top-level tasks array: $PRD_FILE" >&2
    exit 1
  }
  jq -e '
    .tasks
    | all(
        (has("id") and ((.id|type)=="string" or (.id|type)=="number"))
        and (has("title") and ((.title|type)=="string"))
      )
  ' "$PRD_FILE" >/dev/null 2>&1 || {
    echo "Error: PRD tasks must have id + title fields: $PRD_FILE" >&2
    exit 1
  }
}

lock_acquire() {
  COORD_LOCK_DIR="${TASK_REGISTRY_FILE}.lock"
  mkdir -p "$(dirname "$COORD_LOCK_DIR")"
  local start_ts now_ts elapsed
  start_ts="$(date +%s)"

  while ! mkdir "$COORD_LOCK_DIR" >/dev/null 2>&1; do
    sleep 0.1
    if [[ "$TIMEOUT_SECONDS" -gt 0 ]]; then
      now_ts="$(date +%s)"
      elapsed=$((now_ts - start_ts))
      if [[ "$elapsed" -ge "$TIMEOUT_SECONDS" ]]; then
        echo "Error: timeout waiting for lock: $COORD_LOCK_DIR" >&2
        exit 2
      fi
    fi
  done

  COORD_LOCK_HELD="true"
}

lock_release() {
  if [[ "${COORD_LOCK_HELD:-false}" == "true" ]]; then
    rmdir "${COORD_LOCK_DIR:-}" >/dev/null 2>&1 || true
    COORD_LOCK_HELD="false"
  fi
}

ensure_registry_file() {
  mkdir -p "$(dirname "$TASK_REGISTRY_FILE")"
  if [[ -f "$TASK_REGISTRY_FILE" ]]; then
    return
  fi

  local now
  now="$(now_iso)"
  jq -n \
    --arg lot "$(jq -r '.lot // ""' "$PRD_FILE")" \
    --arg version "$(jq -r '.version // "1.0"' "$PRD_FILE")" \
    --arg generated_at "$(jq -r '.generated_at // ""' "$PRD_FILE")" \
    --arg timezone "$(jq -r '.timezone // "UTC"' "$PRD_FILE")" \
    --arg now "$now" \
    '
    {
      schema_version: 1,
      lot: $lot,
      version: $version,
      generated_at: $generated_at,
      timezone: $timezone,
      priority_mapping: {},
      state_mapping: {
        "todo": "todo",
        "claimed": "reserved",
        "in_progress": "dev in progress",
        "pr_open": "awaiting review/CI",
        "changes_requested": "changes requested",
        "queued": "in merge queue",
        "merged": "merged"
      },
      tasks: [],
      processed_event_ids: {},
      resource_locks: {},
      updated_at: $now
    }
    ' >"$TASK_REGISTRY_FILE"
}

ensure_registry_valid() {
  [[ -f "$TASK_REGISTRY_FILE" ]] || {
    echo "Error: task registry not found: $TASK_REGISTRY_FILE" >&2
    exit 1
  }
  jq -e '
    (.tasks | type == "array")
    and ((.processed_event_ids // {}) | type == "object")
    and (.resource_locks | type == "object")
    and (.state_mapping | type == "object")
  ' "$TASK_REGISTRY_FILE" >/dev/null 2>&1 || {
    echo "Error: task registry schema invalid: $TASK_REGISTRY_FILE" >&2
    exit 1
  }
}

clear_inactive_worktrees() {
  local tmp
  tmp="$(mktemp)"
  jq '
    def is_active($s):
      ($s == "claimed" or $s == "in_progress" or $s == "pr_open" or $s == "changes_requested" or $s == "queued");

    .tasks |= map(
      if (is_active(.state) | not) and (.worktree != null) and (.state == "todo" or .state == "merged" or .state == "abandoned") then
        .worktree = null
      else
        .
      end
    )
  ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
}

reconcile_registry() {
  reconcile_orphan_runtime_tasks
  clear_inactive_worktrees
  while IFS=$'\t' read -r task_id state worktree_path; do
    [[ -n "$task_id" && -n "$worktree_path" ]] || continue
    if ! worktree_exists_on_disk "$worktree_path"; then
      apply_transition "$task_id" "blocked" "" "" "worktree_missing"
      echo "Blocked task due to missing worktree: ${task_id}"
    fi
  done < <(jq -r '
    (.tasks // [])[]
    | select(.worktree != null)
    | [.id, .state, .worktree.worktree_path] | @tsv
  ' "$TASK_REGISTRY_FILE")
}

unlock_locks() {
  local unlock_task="$1"
  local unlock_resource="$2"
  local unlock_all="$3"
  local unlock_state="$4"

  if [[ -n "$unlock_task" ]]; then
    local current
    current="$(task_state "$unlock_task")"
    validate_transition "$current" "$unlock_state"
    apply_transition "$unlock_task" "$unlock_state" "" "" "manual_unlock"
    echo "Unlocked task ${unlock_task} via transition to ${unlock_state}"
    return 0
  fi

  local tmp
  tmp="$(mktemp)"
  if [[ -n "$unlock_resource" ]]; then
    jq --arg res "$unlock_resource" '(.resource_locks // {}) | del(.[$res]) as $locks | .resource_locks = $locks' \
      "$TASK_REGISTRY_FILE" >"$tmp"
    mv "$tmp" "$TASK_REGISTRY_FILE"
    echo "Unlocked resource ${unlock_resource}"
    return 0
  fi

  if [[ "$unlock_all" == "true" ]]; then
    jq '.resource_locks = {}' "$TASK_REGISTRY_FILE" >"$tmp"
    mv "$tmp" "$TASK_REGISTRY_FILE"
    echo "Cleared all resource locks"
    return 0
  fi

  echo "Error: unlock requires --task, --resource, or --all" >&2
  exit 1
}

sync_registry_from_prd() {
  local now tmp
  now="$(now_iso)"
  tmp="$(mktemp)"

  jq --slurpfile prd "$PRD_FILE" \
     --arg now "$now" \
     --arg default_tool "$DEFAULT_TOOL" \
     --arg default_base_branch "$DEFAULT_BASE_BRANCH" \
     '
     def as_array:
       if . == null then []
       elif type == "array" then .
       else [.] end;

     def is_active_state($s):
       ($s == "claimed" or $s == "in_progress" or $s == "pr_open" or $s == "changes_requested" or $s == "queued");

     . as $reg
     | ($prd[0]) as $p
     | .schema_version = 1
     | .lot = ($p.lot // .lot // "")
     | .version = ($p.version // .version // "1.0")
     | .generated_at = ($p.generated_at // .generated_at // "")
     | .timezone = ($p.timezone // .timezone // "UTC")
     | .priority_mapping = ($p.priority_mapping // .priority_mapping // {})
     | .state_mapping = (.state_mapping // {
         "todo": "todo",
         "claimed": "reserved",
         "in_progress": "dev in progress",
         "pr_open": "awaiting review/CI",
         "changes_requested": "changes requested",
         "queued": "in merge queue",
         "merged": "merged"
       })
     | .tasks = (
         ($p.tasks // []) | map(
           . as $t
           | (($reg.tasks // []) | map(select(.id == ($t.id|tostring))) | .[0]) as $old
           | {
               id: ($t.id|tostring),
               title: ($t.title // $old.title // ""),
               priority: ($t.priority // $old.priority // "p4"),
               state: (
                 if ($t.state // "") == "merged" then "merged"
                 else ($old.state // ($t.state // "todo"))
                 end
               ),
               dependencies: (($t.dependencies // $old.dependencies // []) | as_array),
               exclusive_resources: (($t.exclusive_resources // $old.exclusive_resources // []) | as_array),
               assignee: ($old.assignee // null),
               claimed_at: ($old.claimed_at // null),
               tool: ($t.tool // $t.git.tool // $old.tool // null),
               coordinator_tool: ($t.coordinator_tool // $old.coordinator_tool // null),
               category: ($t.category // $old.category // null),
               scope: ($t.scope // $t.git.scope // $old.scope // null),
               base_branch: ($t.git.base_branch // $default_base_branch // $old.base_branch),
               worktree: ($old.worktree // null),
               review: ($old.review // {"reviewer": null, "changed": false, "last_reviewed_at": null}),
               task_runtime: ($old.task_runtime // {
                 status: "idle",
                 pid: null,
                 started_at: null,
                 phase_started_at: null,
                 last_heartbeat: null,
                 wait_started_at: null,
                 current_phase: null,
                 attempt: 0,
                 retries: 0,
                 metrics: {
                   dev_s: 0,
                   review_s: 0,
                   integrate_s: 0,
                   wait_s: 0,
                   retries: 0
                 },
                 slo_warnings: {},
                 last_error: null,
                 last_seq: 0,
                 last_event_id: null,
                 last_seq_source: null,
                 last_commit_created_at: null
               })
             }
         )
       )
     | .tasks |= map(
         if .state == "merged" then
           .assignee = null
           | .claimed_at = null
           | .worktree = null
           | .task_runtime = (.task_runtime // {})
           | .task_runtime.status = "idle"
           | .task_runtime.pid = null
           | .task_runtime.started_at = null
           | .task_runtime.current_phase = null
         else
           .
         end
       )
     | .resource_locks = (
         reduce ((.tasks // [])[] | select(is_active_state(.state) and (.worktree != null))) as $t
           ({};
            reduce ($t.exclusive_resources // [])[] as $r
              (.;
               if .[$r] == null then
                 .[$r] = {
                   task_id: $t.id,
                   worktree_path: ($t.worktree.worktree_path // null),
                   locked_at: ($t.claimed_at // $now)
                 }
               else
                 .
               end
              )
           )
       )
     | .processed_event_ids = (.processed_event_ids // {})
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"

  mv "$tmp" "$TASK_REGISTRY_FILE"
}

task_field() {
  local task_id="$1"
  local field="$2"
  if command -v macc >/dev/null 2>&1; then
    local value
    if value="$(macc --cwd "$REPO_DIR" coordinator state-task-field -- --task-id "$task_id" --field "$field" 2>/dev/null)"; then
      printf '%s\n' "$value"
      return 0
    fi
  fi
  jq -r --arg id "$task_id" "(.tasks // [])[] | select(.id == \$id) | ${field}" "$TASK_REGISTRY_FILE"
}

apply_signal_update() {
  local task_id="$1"
  local signal_path="$2"
  local now tmp
  now="$(now_iso)"
  tmp="$(mktemp)"

  [[ -f "$signal_path" ]] || {
    echo "Error: signal JSON not found: $signal_path" >&2
    exit 1
  }
  jq -e '.' "$signal_path" >/dev/null 2>&1 || {
    echo "Error: signal JSON is invalid: $signal_path" >&2
    exit 1
  }

  jq --arg id "$task_id" \
     --arg now "$now" \
     --slurpfile sig "$signal_path" \
     '
     def merge_files($cur; $new):
       if ($new | type) == "array" then
         ($cur + $new) | unique
       else
         $cur
       end;

     .tasks |= map(
       if .id == $id then
         .updated_at = $now
         | (if ($sig[0].last_commit // "") != "" then .worktree.last_commit = ($sig[0].last_commit) else . end)
         | (if ($sig[0].pr_url // "") != "" then .pr_url = ($sig[0].pr_url) else . end)
         | (if ($sig[0].reviewer // "") != "" then .review.reviewer = ($sig[0].reviewer) else . end)
         | (if ($sig[0].review_notes // "") != "" then .review.notes = ($sig[0].review_notes) else . end)
         | (if ($sig[0].review_changed // null) != null then .review.changed = ($sig[0].review_changed) else . end)
         | (if ($sig[0].files_changed // null) != null then
              .files_changed = merge_files((.files_changed // []); ($sig[0].files_changed))
            else
              .
            end)
       else
         .
       end
     )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"

  mv "$tmp" "$TASK_REGISTRY_FILE"
}

task_exists() {
  local task_id="$1"
  if command -v macc >/dev/null 2>&1; then
    if macc --cwd "$REPO_DIR" coordinator state-task-exists -- --task-id "$task_id" >/dev/null 2>&1; then
      return 0
    fi
  fi
  jq -e --arg id "$task_id" '(.tasks // [])[] | select(.id == $id)' "$TASK_REGISTRY_FILE" >/dev/null 2>&1
}

task_has_worktree() {
  local task_id="$1"
  jq -e --arg id "$task_id" '(.tasks // [])[] | select(.id == $id) | (.worktree != null)' "$TASK_REGISTRY_FILE" >/dev/null 2>&1
}

worktree_in_use() {
  local path="$1"
  jq -e --arg path "$path" '
    (.tasks // [])[] | select(.worktree != null) | select(.worktree.worktree_path == $path)
  ' "$TASK_REGISTRY_FILE" >/dev/null 2>&1
}

worktree_task_id() {
  local path="$1"
  jq -r --arg path "$path" '
    (.tasks // [])[] | select(.worktree != null and .worktree.worktree_path == $path) | .id
  ' "$TASK_REGISTRY_FILE"
}

worktree_exists_on_disk() {
  local path="$1"
  [[ -d "$path" ]] && git -C "$path" rev-parse --is-inside-work-tree >/dev/null 2>&1
}

task_state() {
  local task_id="$1"
  task_field "$task_id" '.state'
}

task_scope() {
  local task_id="$1"
  if command -v macc >/dev/null 2>&1; then
    local scope
    if scope="$(macc --cwd "$REPO_DIR" coordinator state-task-field -- --task-id "$task_id" --field '.scope // ""' 2>/dev/null)"; then
      printf '%s\n' "$scope"
      return 0
    fi
  fi
  jq -r --arg id "$task_id" '
    ((.tasks // [])[] | select(.id == $id) | (.scope // "")) // ""
  ' "$TASK_REGISTRY_FILE"
}

validate_transition() {
  local from="$1"
  local to="$2"
  if ! command -v macc >/dev/null 2>&1; then
    echo "Error: macc is required to validate coordinator transitions from core." >&2
    return 1
  fi
  if macc --cwd "$REPO_DIR" coordinator validate-transition -- --from "$from" --to "$to" >/dev/null 2>&1; then
    return 0
  fi
  echo "Error: invalid transition ${from} -> ${to} (core transition table)" >&2
  return 1
}

validate_runtime_transition() {
  local from="$1"
  local to="$2"
  if [[ "$from" == "$to" ]]; then
    return 0
  fi
  if ! command -v macc >/dev/null 2>&1; then
    echo "Error: macc is required to validate coordinator runtime transitions from core." >&2
    return 1
  fi
  if macc --cwd "$REPO_DIR" coordinator validate-runtime-transition -- --from "$from" --to "$to" >/dev/null 2>&1; then
    return 0
  fi
  echo "Error: invalid runtime transition ${from} -> ${to} (core transition table)" >&2
  return 1
}

runtime_status_from_event() {
  local event_type="$1"
  local status="$2"
  if ! command -v macc >/dev/null 2>&1; then
    echo "Error: macc is required to map coordinator runtime status from events in core." >&2
    return 1
  fi
  local mapped
  mapped="$(macc --cwd "$REPO_DIR" coordinator runtime-status-from-event -- --type "$event_type" --status "$status" 2>/dev/null || true)"
  case "$mapped" in
    idle|dispatched|running|phase_done|failed|stale|paused)
      echo "$mapped"
      return 0
      ;;
    *)
      echo "Error: failed to map runtime status from event type='${event_type}' status='${status}'." >&2
      return 1
      ;;
  esac
}

failure_kind_to_state() {
  local kind="$1"
  case "$kind" in
    worktree_create|pr_create) echo "blocked" ;;
    ci_red) echo "changes_requested" ;;
    merge_queue_fail|rebase_required) echo "pr_open" ;;
    *) echo "" ;;
  esac
}

apply_transition() {
  local task_id="$1"
  local new_state="$2"
  local pr_url="${3:-}"
  local reviewer="${4:-}"
  local reason="${5:-}"
  local old_state=""
  old_state="$(task_state "$task_id" 2>/dev/null || true)"

  local now tmp
  now="$(now_iso)"

  if command -v macc >/dev/null 2>&1; then
    if macc --cwd "$REPO_DIR" coordinator state-apply-transition -- \
      --task-id "$task_id" \
      --state "$new_state" \
      --pr-url "$pr_url" \
      --reviewer "$reviewer" \
      --reason "$reason" >/dev/null 2>&1; then
      emit_event "task_transition" "Task state changed" "$task_id" "$new_state" "from=${old_state} reason=${reason}"
      return 0
    fi
    if ! coordinator_storage_mode_is_json; then
      echo "Error: state-apply-transition failed while COORDINATOR_STORAGE_MODE=${COORDINATOR_STORAGE_MODE:-sqlite}; jq fallback is disabled." >&2
      return 1
    fi
  elif ! coordinator_storage_mode_is_json; then
    echo "Error: macc command is required for transition mutation in COORDINATOR_STORAGE_MODE=${COORDINATOR_STORAGE_MODE:-sqlite}." >&2
    return 1
  fi

  tmp="$(mktemp)"

  jq --arg id "$task_id" \
     --arg state "$new_state" \
     --arg now "$now" \
     --arg pr_url "$pr_url" \
     --arg reviewer "$reviewer" \
     --arg reason "$reason" \
     '
     def is_active($s):
       ($s == "claimed" or $s == "in_progress" or $s == "pr_open" or $s == "changes_requested" or $s == "queued");

     .tasks |= map(
       if .id == $id then
         .state = $state
         | .updated_at = $now
         | .state_changed_at = $now
         | (if $state == "pr_open" and ($pr_url|length) > 0 then .pr_url = $pr_url else . end)
         | (if $state == "changes_requested" then
              .review.changed = true
              | .review.last_reviewed_at = $now
              | (if ($reviewer|length) > 0 then .review.reviewer = $reviewer else . end)
              | (if ($reason|length) > 0 then .review.reason = $reason else . end)
            else
              .
            end)
         | (if ($state == "merged" or $state == "abandoned") then
              .assignee = null
              | .claimed_at = null
              | .worktree = null
              | .task_runtime = (.task_runtime // {})
              | .task_runtime.status = "idle"
              | .task_runtime.pid = null
              | .task_runtime.started_at = null
              | .task_runtime.current_phase = null
              | .task_runtime.merge_result_pending = false
              | .task_runtime.merge_result_file = null
            elif ($state == "todo") then
              .assignee = null
              | .claimed_at = null
              | .worktree = null
              | .task_runtime = (.task_runtime // {})
              | .task_runtime.status = "idle"
              | .task_runtime.pid = null
              | .task_runtime.started_at = null
              | .task_runtime.current_phase = null
              | .task_runtime.merge_result_pending = false
              | .task_runtime.merge_result_file = null
            else
              .
            end)
       else
         .
       end
     )
     | .resource_locks = (
         reduce ((.tasks // [])[] | select(is_active(.state) and (.worktree != null))) as $t
           ({};
            reduce ($t.exclusive_resources // [])[] as $r
              (.;
               if .[$r] == null then
                 .[$r] = {
                   task_id: $t.id,
                   worktree_path: ($t.worktree.worktree_path // null),
                   locked_at: ($t.claimed_at // $now)
                 }
               else
                 .
               end
              )
           )
       )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"

  mv "$tmp" "$TASK_REGISTRY_FILE"
  emit_event "task_transition" "Task state changed" "$task_id" "$new_state" "from=${old_state} reason=${reason}"
}

set_task_runtime() {
  local task_id="$1"
  local runtime_status="$2"
  local phase="${3:-}"
  local pid="${4:-}"
  local last_error="${5:-}"
  local heartbeat_ts="${6:-}"
  local attempt="${7:-}"
  local old_runtime_status old_phase new_phase phase_closed metric_value metric_key
  old_runtime_status="$(task_field "$task_id" '.task_runtime.status // "idle"' 2>/dev/null || true)"
  if [[ -z "$old_runtime_status" || "$old_runtime_status" == "null" ]]; then
    old_runtime_status="idle"
  fi
  old_phase="$(task_field "$task_id" '.task_runtime.current_phase // ""' 2>/dev/null || true)"
  if [[ "$old_phase" == "null" ]]; then
    old_phase=""
  fi
  new_phase="$old_phase"
  if [[ -n "$phase" ]]; then
    new_phase="$phase"
  fi
  phase_closed="false"
  if [[ -n "$old_phase" ]]; then
    if [[ "$runtime_status" == "phase_done" || "$runtime_status" == "failed" || "$runtime_status" == "stale" || "$runtime_status" == "idle" ]]; then
      phase_closed="true"
    elif [[ -n "$new_phase" && "$new_phase" != "$old_phase" ]]; then
      phase_closed="true"
    fi
  fi
  if ! validate_runtime_transition "$old_runtime_status" "$runtime_status"; then
    return 1
  fi

  if command -v macc >/dev/null 2>&1; then
    if macc --cwd "$REPO_DIR" coordinator state-set-runtime -- \
      --task-id "$task_id" \
      --runtime-status "$runtime_status" \
      --phase "$phase" \
      --pid "$pid" \
      --last-error "$last_error" \
      --heartbeat-ts "$heartbeat_ts" \
      --attempt "$attempt" >/dev/null 2>&1; then
      check_task_slo_and_warn "$task_id"
      if [[ "$phase_closed" == "true" ]]; then
        case "$old_phase" in
          dev) metric_key="dev_s" ;;
          review) metric_key="review_s" ;;
          integrate) metric_key="integrate_s" ;;
          *) metric_key="" ;;
        esac
        if [[ -n "$metric_key" ]]; then
          metric_value="$(task_field "$task_id" ".task_runtime.metrics.${metric_key} // 0" 2>/dev/null || true)"
          [[ "$metric_value" =~ ^[0-9]+$ ]] || metric_value=0
          emit_event "task_phase_duration_seconds" \
            "Task phase duration updated" \
            "$task_id" \
            "$(task_state "$task_id")" \
            "phase=${old_phase} seconds=${metric_value}" \
            "$old_phase" \
            "$runtime_status" \
            "" \
            "$(jq -nc --arg phase "$old_phase" --argjson seconds "$metric_value" '{phase:$phase,seconds:$seconds}')"
        fi
      fi
      return 0
    fi
    if ! coordinator_storage_mode_is_json; then
      echo "Error: state-set-runtime failed while COORDINATOR_STORAGE_MODE=${COORDINATOR_STORAGE_MODE:-sqlite}; jq fallback is disabled." >&2
      return 1
    fi
  elif ! coordinator_storage_mode_is_json; then
    echo "Error: macc command is required for runtime mutation in COORDINATOR_STORAGE_MODE=${COORDINATOR_STORAGE_MODE:-sqlite}." >&2
    return 1
  fi

  local now tmp
  now="$(now_iso)"
  tmp="$(mktemp)"

  jq --arg id "$task_id" \
     --arg runtime_status "$runtime_status" \
     --arg phase "$phase" \
     --arg pid "$pid" \
     --arg last_error "$last_error" \
     --arg heartbeat_ts "$heartbeat_ts" \
     --arg attempt "$attempt" \
     --arg now "$now" \
     '
     def ts_to_epoch($v):
       if ($v | type) == "string" and ($v | length) > 0 then
         ($v | fromdateiso8601? // 0)
       else
         0
       end;

     def positive_delta($start; $end):
       if $start > 0 and $end > $start then ($end - $start) else 0 end;

     def is_active_state($s):
       ($s == "claimed" or $s == "in_progress" or $s == "pr_open" or $s == "changes_requested" or $s == "queued");

     def should_close_phase($old_phase; $new_phase; $new_status):
       ($old_phase | length) > 0 and (
         (($new_phase | length) > 0 and $new_phase != $old_phase)
         or ($new_status == "phase_done" or $new_status == "failed" or $new_status == "stale" or $new_status == "idle")
       );

     .tasks |= map(
       if .id == $id then
         .task_runtime = (.task_runtime // {})
         | .task_runtime.metrics = (.task_runtime.metrics // {})
         | .task_runtime.metrics.dev_s = (.task_runtime.metrics.dev_s // 0)
         | .task_runtime.metrics.review_s = (.task_runtime.metrics.review_s // 0)
         | .task_runtime.metrics.integrate_s = (.task_runtime.metrics.integrate_s // 0)
         | .task_runtime.metrics.wait_s = (.task_runtime.metrics.wait_s // 0)
         | .task_runtime.metrics.retries = (.task_runtime.metrics.retries // (.task_runtime.retries // 0))
         | .task_runtime.retries = (.task_runtime.metrics.retries // 0)
         | .task_runtime.slo_warnings = (.task_runtime.slo_warnings // {})
         | (ts_to_epoch($now)) as $now_epoch
         | (.task_runtime.current_phase // "") as $old_phase
         | (.task_runtime.status // "") as $old_status
         | (if ($phase | length) > 0 then $phase else $old_phase end) as $new_phase
         | (.task_runtime.phase_started_at // .task_runtime.started_at // "") as $old_phase_started_at
         | (ts_to_epoch($old_phase_started_at)) as $old_phase_started_epoch
         | (positive_delta($old_phase_started_epoch; $now_epoch)) as $phase_elapsed
         | (if should_close_phase($old_phase; $new_phase; $runtime_status) then
              if $old_phase == "dev" then
                .task_runtime.metrics.dev_s = ((.task_runtime.metrics.dev_s // 0) + $phase_elapsed)
              elif $old_phase == "review" then
                .task_runtime.metrics.review_s = ((.task_runtime.metrics.review_s // 0) + $phase_elapsed)
              elif $old_phase == "integrate" then
                .task_runtime.metrics.integrate_s = ((.task_runtime.metrics.integrate_s // 0) + $phase_elapsed)
              else
                .
              end
            else
              .
            end)
         | (.state // "") as $task_state
         | (.task_runtime.wait_started_at // "") as $old_wait_started_at
         | (ts_to_epoch($old_wait_started_at)) as $old_wait_started_epoch
         | (positive_delta($old_wait_started_epoch; $now_epoch)) as $wait_elapsed
         | (if ($old_wait_started_at | length) > 0 and ($runtime_status == "running" or $runtime_status == "idle" or $runtime_status == "failed" or $runtime_status == "stale") then
              .task_runtime.metrics.wait_s = ((.task_runtime.metrics.wait_s // 0) + $wait_elapsed)
              | .task_runtime.wait_started_at = null
            elif (($old_wait_started_at | length) == 0 and is_active_state($task_state) and ($runtime_status != "running") and ($runtime_status != "idle")) then
              .task_runtime.wait_started_at = $now
            else
              .
            end)
         | .task_runtime.status = $runtime_status
         | (if ($phase|length) > 0 then .task_runtime.current_phase = $phase else . end)
         | (if ($pid|length) > 0 then
              .task_runtime.pid = ($pid|tonumber?)
            elif ($runtime_status == "idle" or $runtime_status == "phase_done" or $runtime_status == "failed" or $runtime_status == "stale") then
              .task_runtime.pid = null
            else
              .
            end)
         | (if ($last_error|length) > 0 then .task_runtime.last_error = $last_error else . end)
         | (if ($heartbeat_ts|length) > 0 then .task_runtime.last_heartbeat = $heartbeat_ts else . end)
         | (if ($attempt|length) > 0 then .task_runtime.attempt = ($attempt|tonumber?) else . end)
         | (if (
              (($phase|length) > 0 and $runtime_status == "running" and ($phase != $old_phase or $old_status != "running"))
              or ($runtime_status == "running" and ((.task_runtime.phase_started_at // "") | length) == 0)
            ) then
              .task_runtime.phase_started_at = $now
            elif ($runtime_status == "idle" or $runtime_status == "phase_done" or $runtime_status == "failed" or $runtime_status == "stale") then
              .task_runtime.phase_started_at = null
            else
              .
            end)
         | (if ($runtime_status == "running" and ((.task_runtime.started_at // "") | length) == 0) then
              .task_runtime.started_at = $now
            else
              .
            end)
       else
         .
       end
     )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"

  mv "$tmp" "$TASK_REGISTRY_FILE"
  check_task_slo_and_warn "$task_id"
  if [[ "$phase_closed" == "true" ]]; then
    case "$old_phase" in
      dev) metric_key="dev_s" ;;
      review) metric_key="review_s" ;;
      integrate) metric_key="integrate_s" ;;
      *) metric_key="" ;;
    esac
    if [[ -n "$metric_key" ]]; then
      metric_value="$(task_field "$task_id" ".task_runtime.metrics.${metric_key} // 0" 2>/dev/null || true)"
      [[ "$metric_value" =~ ^[0-9]+$ ]] || metric_value=0
      emit_event "task_phase_duration_seconds" \
        "Task phase duration updated" \
        "$task_id" \
        "$(task_state "$task_id")" \
        "phase=${old_phase} seconds=${metric_value}" \
        "$old_phase" \
        "$runtime_status" \
        "" \
        "$(jq -nc --arg phase "$old_phase" --argjson seconds "$metric_value" '{phase:$phase,seconds:$seconds}')"
    fi
  fi
}

task_has_pending_merge_result() {
  local task_id="$1"
  jq -r --arg id "$task_id" '
    (.tasks // [])
    | map(select(.id == $id))
    | .[0].task_runtime.merge_result_pending // false
    | if . == true then "true" else "false" end
  ' "$TASK_REGISTRY_FILE" 2>/dev/null || echo "false"
}

set_task_merge_result_pending() {
  local task_id="$1"
  local result_file="$2"
  local pid="${3:-}"
  if command -v macc >/dev/null 2>&1; then
    if macc --cwd "$REPO_DIR" coordinator state-set-merge-pending -- \
      --task-id "$task_id" \
      --result-file "$result_file" \
      --pid "$pid" \
      --now "$(now_iso)" >/dev/null 2>&1; then
      return 0
    fi
    if ! coordinator_storage_mode_is_json; then
      echo "Error: state-set-merge-pending failed while COORDINATOR_STORAGE_MODE=${COORDINATOR_STORAGE_MODE:-sqlite}; jq fallback is disabled." >&2
      return 1
    fi
  elif ! coordinator_storage_mode_is_json; then
    echo "Error: macc command is required for merge pending mutation in COORDINATOR_STORAGE_MODE=${COORDINATOR_STORAGE_MODE:-sqlite}." >&2
    return 1
  fi
  local now tmp
  now="$(now_iso)"
  tmp="$(mktemp)"
  jq --arg id "$task_id" \
     --arg result_file "$result_file" \
     --arg pid "$pid" \
     --arg now "$now" \
     '
     .tasks |= map(
       if .id == $id then
         .task_runtime = (.task_runtime // {})
         | .task_runtime.merge_result_pending = true
         | .task_runtime.merge_result_file = $result_file
         | .task_runtime.merge_worker_pid = ($pid | tonumber?)
         | .task_runtime.merge_result_started_at = $now
       else
         .
       end
     )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
}

set_task_merge_result_processed() {
  local task_id="$1"
  local result_file="$2"
  local status="$3"
  local rc="$4"
  if command -v macc >/dev/null 2>&1; then
    if macc --cwd "$REPO_DIR" coordinator state-set-merge-processed -- \
      --task-id "$task_id" \
      --result-file "$result_file" \
      --status "$status" \
      --rc "$rc" \
      --now "$(now_iso)" >/dev/null 2>&1; then
      return 0
    fi
    if ! coordinator_storage_mode_is_json; then
      echo "Error: state-set-merge-processed failed while COORDINATOR_STORAGE_MODE=${COORDINATOR_STORAGE_MODE:-sqlite}; jq fallback is disabled." >&2
      return 1
    fi
  elif ! coordinator_storage_mode_is_json; then
    echo "Error: macc command is required for merge processed mutation in COORDINATOR_STORAGE_MODE=${COORDINATOR_STORAGE_MODE:-sqlite}." >&2
    return 1
  fi
  local now tmp
  now="$(now_iso)"
  tmp="$(mktemp)"
  jq --arg id "$task_id" \
     --arg result_file "$result_file" \
     --arg status "$status" \
     --arg rc "$rc" \
     --arg now "$now" \
     '
     .tasks |= map(
       if .id == $id then
         .task_runtime = (.task_runtime // {})
         | .task_runtime.merge_result_pending = false
         | .task_runtime.merge_result_file = null
         | .task_runtime.merge_worker_pid = null
         | .task_runtime.last_merge_result_file = (if ($result_file|length) > 0 then $result_file else (.task_runtime.last_merge_result_file // null) end)
         | .task_runtime.last_merge_result_status = (if ($status|length) > 0 then $status else (.task_runtime.last_merge_result_status // null) end)
         | .task_runtime.last_merge_result_rc = ($rc | tonumber? // .task_runtime.last_merge_result_rc // null)
         | .task_runtime.last_merge_result_at = $now
       else
         .
       end
     )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
}

set_task_runtime_error_details() {
  local task_id="$1"
  local error_code="$2"
  local error_origin="$3"
  local error_message="$4"
  task_exists "$task_id" || return 0
  local now tmp
  now="$(now_iso)"
  tmp="$(mktemp)"
  jq --arg id "$task_id" \
     --arg code "$error_code" \
     --arg origin "$error_origin" \
     --arg message "$error_message" \
     --arg now "$now" \
     '
     .tasks |= map(
       if .id == $id then
         .task_runtime = (.task_runtime // {})
         | (if ($code|length) > 0 then .task_runtime.last_error_code = $code else . end)
         | (if ($origin|length) > 0 then .task_runtime.last_error_origin = $origin else . end)
         | (if ($message|length) > 0 then .task_runtime.last_error_message = $message else . end)
       else
         .
       end
     )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
}

increment_task_retries() {
  local task_id="$1"
  local reason="${2:-retry}"
  task_exists "$task_id" || return 0
  if command -v macc >/dev/null 2>&1; then
    if macc --cwd "$REPO_DIR" coordinator state-increment-retries -- \
      --task-id "$task_id" \
      --now "$(now_iso)" >/dev/null 2>&1; then
      emit_event "task_retry_count" "Incremented task retry counter" "$task_id" "" "reason=${reason}"
      local retries_total
      retries_total="$(task_field "$task_id" '.task_runtime.retries // .task_runtime.metrics.retries // 0' 2>/dev/null || true)"
      [[ "$retries_total" =~ ^[0-9]+$ ]] || retries_total=0
      emit_event "task_retries_total" "Task retries total updated" "$task_id" "" "retries=${retries_total} reason=${reason}" "" "ok" "" \
        "$(jq -nc --argjson retries "$retries_total" --arg reason "$reason" '{retries:$retries,reason:$reason}')"
      check_task_slo_and_warn "$task_id"
      return 0
    fi
    if ! coordinator_storage_mode_is_json; then
      echo "Error: state-increment-retries failed while COORDINATOR_STORAGE_MODE=${COORDINATOR_STORAGE_MODE:-sqlite}; jq fallback is disabled." >&2
      return 1
    fi
  elif ! coordinator_storage_mode_is_json; then
    echo "Error: macc command is required for retries mutation in COORDINATOR_STORAGE_MODE=${COORDINATOR_STORAGE_MODE:-sqlite}." >&2
    return 1
  fi
  local tmp now
  now="$(now_iso)"
  tmp="$(mktemp)"
  jq --arg id "$task_id" \
     --arg now "$now" \
     '
     .tasks |= map(
       if .id == $id then
         .task_runtime = (.task_runtime // {})
         | .task_runtime.metrics = (.task_runtime.metrics // {})
         | .task_runtime.metrics.retries = ((.task_runtime.metrics.retries // (.task_runtime.retries // 0)) + 1)
         | .task_runtime.retries = (.task_runtime.metrics.retries // 0)
       else
         .
       end
     )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
  emit_event "task_retry_count" "Incremented task retry counter" "$task_id" "" "reason=${reason}"
  local retries_total
  retries_total="$(task_field "$task_id" '.task_runtime.retries // .task_runtime.metrics.retries // 0' 2>/dev/null || true)"
  [[ "$retries_total" =~ ^[0-9]+$ ]] || retries_total=0
  emit_event "task_retries_total" "Task retries total updated" "$task_id" "" "retries=${retries_total} reason=${reason}" "" "ok" "" \
    "$(jq -nc --argjson retries "$retries_total" --arg reason "$reason" '{retries:$retries,reason:$reason}')"
  check_task_slo_and_warn "$task_id"
}

upsert_task_slo_warning() {
  local task_id="$1"
  local metric="$2"
  local threshold="$3"
  local value="$4"
  local suggestion="$5"
  if command -v macc >/dev/null 2>&1; then
    if macc --cwd "$REPO_DIR" coordinator state-upsert-slo-warning -- \
      --task-id "$task_id" \
      --metric "$metric" \
      --threshold "$threshold" \
      --value "$value" \
      --suggestion "$suggestion" \
      --now "$(now_iso)" >/dev/null 2>&1; then
      return 0
    fi
    if ! coordinator_storage_mode_is_json; then
      echo "Error: state-upsert-slo-warning failed while COORDINATOR_STORAGE_MODE=${COORDINATOR_STORAGE_MODE:-sqlite}; jq fallback is disabled." >&2
      return 1
    fi
  elif ! coordinator_storage_mode_is_json; then
    echo "Error: macc command is required for SLO warning mutation in COORDINATOR_STORAGE_MODE=${COORDINATOR_STORAGE_MODE:-sqlite}." >&2
    return 1
  fi
  local now tmp
  now="$(now_iso)"
  tmp="$(mktemp)"
  jq --arg id "$task_id" \
     --arg metric "$metric" \
     --arg suggestion "$suggestion" \
     --arg now "$now" \
     --argjson threshold "$threshold" \
     --argjson value "$value" \
     '
     .tasks |= map(
       if .id == $id then
         .task_runtime = (.task_runtime // {})
         | .task_runtime.slo_warnings = (.task_runtime.slo_warnings // {})
         | .task_runtime.slo_warnings[$metric] = {
             metric: $metric,
             threshold: $threshold,
             value: $value,
             warned_at: $now,
             suggestion: $suggestion
           }
       else
         .
       end
     )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
}

maybe_warn_task_metric() {
  local task_id="$1"
  local metric="$2"
  local threshold="$3"
  local suggestion="$4"
  [[ "$threshold" =~ ^[0-9]+$ ]] || return 0
  [[ "$threshold" -gt 0 ]] || return 0

  local value warned
  if command -v macc >/dev/null 2>&1; then
    local metric_row
    if metric_row="$(macc --cwd "$REPO_DIR" coordinator state-slo-metric -- --task-id "$task_id" --metric "$metric" 2>/dev/null)"; then
      IFS=$'\t' read -r value warned <<<"$metric_row"
    else
      if ! coordinator_storage_mode_is_json; then
        echo "Error: state-slo-metric failed while COORDINATOR_STORAGE_MODE=${COORDINATOR_STORAGE_MODE:-sqlite}; jq fallback is disabled." >&2
        return 1
      fi
      value=0
      warned=false
    fi
  elif ! coordinator_storage_mode_is_json; then
    echo "Error: macc command is required for SLO metric reads in COORDINATOR_STORAGE_MODE=${COORDINATOR_STORAGE_MODE:-sqlite}." >&2
    return 1
  else
    value="$(jq -r --arg id "$task_id" --arg metric "$metric" '
      (.tasks // [])
      | map(select(.id == $id))
      | .[0]
      | (
          if ($metric == "retries") then
            (.task_runtime.retries // .task_runtime.metrics.retries // 0)
          else
            (.task_runtime.metrics[$metric] // 0)
          end
        )
    ' "$TASK_REGISTRY_FILE" 2>/dev/null || echo 0)"
    warned="$(jq -r --arg id "$task_id" --arg metric "$metric" '
      (.tasks // [])
      | map(select(.id == $id))
      | .[0]
      | ((.task_runtime.slo_warnings // {})[$metric] != null)
    ' "$TASK_REGISTRY_FILE" 2>/dev/null || echo false)"
  fi

  [[ "$value" =~ ^[0-9]+$ ]] || value=0
  if [[ "$value" -gt "$threshold" && "$warned" != "true" ]]; then
    local unit
    if [[ "$metric" == "retries" ]]; then
      unit=""
    else
      unit="s"
    fi
    upsert_task_slo_warning "$task_id" "$metric" "$threshold" "$value" "$suggestion"
    emit_event "task_slo_warning" \
      "Task exceeded SLO threshold" \
      "$task_id" \
      "$(task_state "$task_id")" \
      "metric=${metric} value=${value} threshold=${threshold} action=${suggestion}"
    note "SLO warning ${task_id}: ${metric}=${value}${unit} threshold=${threshold}${unit}. Suggestion: ${suggestion}"
  fi
}

mark_run_blocking_merge_failure() {
  local task_id="$1"
  local merge_error="$2"
  local report_file="$3"
  [[ "$COORD_COMMAND_NAME" == "run" ]] || return 0
  RUN_BLOCKING_MERGE_FAILED="true"
  RUN_BLOCKING_MERGE_TASK_ID="$task_id"
  RUN_BLOCKING_MERGE_ERROR="$merge_error"
  RUN_BLOCKING_MERGE_REPORT="$report_file"
}

extract_report_file_from_error() {
  local merge_error="${1:-}"
  if [[ "$merge_error" =~ report=\"([^\"]+)\" ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
  fi
}

find_blocking_local_merge_task_tsv() {
  jq -r '
    (.tasks // [])
    | map(
        select(
          (.state // "") == "blocked"
          and (
            (((.reason // "") + " " + (.task_runtime.last_error // "")) | test("failure:local_merge"))
          )
        )
      )
    | .[0]
    | if . == null then
        empty
      else
        [
          (.id // ""),
          (.task_runtime.last_error // .reason // "failure:local_merge"),
          (.task_runtime.last_merge_result_file // .task_runtime.merge_result_file // "")
        ] | @tsv
      end
  ' "$TASK_REGISTRY_FILE"
}

mark_run_blocking_merge_failure_from_registry() {
  local row task_id merge_error report_file
  row="$(find_blocking_local_merge_task_tsv)"
  [[ -n "$row" ]] || return 1
  IFS=$'\t' read -r task_id merge_error report_file <<<"$row"
  [[ -n "$task_id" ]] || return 1
  if [[ -z "$report_file" ]]; then
    report_file="$(extract_report_file_from_error "$merge_error")"
  fi
  mark_run_blocking_merge_failure "$task_id" "$merge_error" "$report_file"
  return 0
}

run_should_pause_on_blocking_merge() {
  [[ "$RUN_BLOCKING_MERGE_FAILED" == "true" ]]
}

print_blocking_merge_pause_error() {
  local task_id="${RUN_BLOCKING_MERGE_TASK_ID:-unknown}"
  local merge_error="${RUN_BLOCKING_MERGE_ERROR:-failure:local_merge}"
  local report_file="${RUN_BLOCKING_MERGE_REPORT:-}"
  echo "Error: coordinator paused by blocking merge failure on task ${task_id} (phase=integrate)." >&2
  echo "Reason: ${merge_error}" >&2
  if [[ -n "$report_file" ]]; then
    echo "Merge report: ${report_file}" >&2
  fi
  echo "Resolve the merge issue, then run:" >&2
  echo "  macc coordinator retry-phase --retry-task ${task_id} --retry-phase integrate" >&2
  echo "After integrate retry succeeds, resume orchestration with:" >&2
  echo "  macc coordinator run" >&2
}

check_task_slo_and_warn() {
  local task_id="$1"
  task_exists "$task_id" || return 0
  maybe_warn_task_metric "$task_id" "dev_s" "$SLO_DEV_SECONDS" "Check performer logs and split implementation scope before retrying dev phase."
  maybe_warn_task_metric "$task_id" "review_s" "$SLO_REVIEW_SECONDS" "Run macc coordinator retry-phase --retry-task ${task_id} --retry-phase review after fixing review blockers."
  maybe_warn_task_metric "$task_id" "integrate_s" "$SLO_INTEGRATE_SECONDS" "Inspect merge logs and retry integrate phase or run merge worker manually."
  maybe_warn_task_metric "$task_id" "wait_s" "$SLO_WAIT_SECONDS" "Check queue capacity/dependencies and run macc coordinator reconcile."
  maybe_warn_task_metric "$task_id" "retries" "$SLO_RETRIES_MAX" "Inspect repeated failures and consider manual intervention or task decomposition."
}

pid_is_running() {
  local pid="${1:-}"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  [[ "$pid" -gt 0 ]] || return 1
  kill -0 "$pid" >/dev/null 2>&1
}

is_recent_event_ts() {
  local ts="${1:-}"
  local window="${2:-120}"
  [[ -n "$ts" ]] || return 1
  [[ "$window" =~ ^[0-9]+$ ]] || window=120
  local event_epoch now_epoch age
  event_epoch="$(date -d "$ts" +%s 2>/dev/null || echo 0)"
  [[ "$event_epoch" =~ ^[0-9]+$ ]] || event_epoch=0
  [[ "$event_epoch" -gt 0 ]] || return 1
  now_epoch="$(date +%s)"
  age=$((now_epoch - event_epoch))
  if [[ "$age" -lt 0 ]]; then
    age=0
  fi
  [[ "$age" -le "$window" ]]
}

orphan_runtime_target_state() {
  case "$STALE_HEARTBEAT_ACTION" in
    requeue|retry) echo "todo" ;;
    block|*) echo "blocked" ;;
  esac
}

transition_orphan_task_state() {
  local task_id="$1"
  local current_state="$2"
  local target_state="$3"
  local reason="$4"

  if [[ "$target_state" == "todo" ]]; then
    if [[ "$current_state" == "blocked" ]]; then
      apply_transition "$task_id" "todo" "" "" "$reason"
      return 0
    fi
    # Workflow does not support direct transitions from active states to todo.
    apply_transition "$task_id" "blocked" "" "" "$reason"
    apply_transition "$task_id" "todo" "" "" "$reason"
    return 0
  fi

  apply_transition "$task_id" "$target_state" "" "" "$reason"
}

reconcile_orphan_runtime_tasks() {
  local row task_id state runtime_status pid phase last_commit_created_at
  while IFS= read -r row; do
    [[ -n "${row:-}" ]] || continue
    task_id="$(jq -r '.task_id // ""' <<<"$row" 2>/dev/null || true)"
    state="$(jq -r '.state // ""' <<<"$row" 2>/dev/null || true)"
    runtime_status="$(jq -r '.runtime_status // ""' <<<"$row" 2>/dev/null || true)"
    pid="$(jq -r '.pid // ""' <<<"$row" 2>/dev/null || true)"
    phase="$(jq -r '.phase // ""' <<<"$row" 2>/dev/null || true)"
    last_commit_created_at="$(jq -r '.last_commit_created_at // ""' <<<"$row" 2>/dev/null || true)"
    [[ -n "$task_id" ]] || continue

    case "$runtime_status" in
      phase_done)
        # If a task is still claimed but dev phase already completed and the worker is gone,
        # recover the workflow transition so advance can continue.
        if [[ "$state" == "claimed" ]]; then
          if [[ -z "$pid" || ! "$pid" =~ ^[0-9]+$ || "$pid" -le 0 ]] || ! pid_is_running "$pid"; then
            transition_task_and_hooks "$task_id" "in_progress" "" "" "auto:recover_phase_done"
            set_task_runtime "$task_id" "phase_done" "${phase:-dev}" "" "" "$(now_iso)"
            emit_event "performer_complete" "Recovered completed dev phase from orphan runtime" "$task_id" "in_progress" "pid=${pid:-none}"
            note "Recovered task ${task_id}: claimed + phase_done -> in_progress (pid=${pid:-none})"
          fi
        fi
        ;;
      running|dispatched)
        if [[ "$phase" != "integrate" && ( "$state" == "claimed" || "$state" == "in_progress" ) ]] && is_recent_event_ts "$last_commit_created_at" 180; then
          if [[ "$state" == "claimed" ]]; then
            transition_task_and_hooks "$task_id" "in_progress" "" "" "auto:recover_commit_created"
          fi
          set_task_runtime "$task_id" "phase_done" "${phase:-dev}" "" "" "$(now_iso)"
          emit_event "performer_complete" "Recovered recent commit_created from orphan runtime" "$task_id" "$(task_state "$task_id")" "runtime_status=${runtime_status} commit_created_at=${last_commit_created_at}" "${phase:-dev}" "phase_done"
          note "Recovered task ${task_id}: recent commit_created (${last_commit_created_at}) kept workflow on success path"
          continue
        fi

        # Integrate merge workers can finish quickly; keep queued tasks stable while
        # their persisted merge result is pending processing.
        if [[ "$phase" == "integrate" ]] && [[ "$(task_has_pending_merge_result "$task_id")" == "true" ]]; then
          continue
        fi

        # Runtime marked active but worker PID absent/dead means workflow/runtime divergence.
        # Resolve explicitly according to policy to avoid ghost-active tasks.
        local target_state reason runtime_mark msg
        target_state="$(orphan_runtime_target_state)"
        if [[ "$state" == "queued" && "$phase" == "integrate" && ( -z "$pid" || ! "$pid" =~ ^[0-9]+$ || "$pid" -le 0 ) ]]; then
          # queued/integrate may be waiting for async merge worker scheduling.
          continue
        fi
        if [[ -z "$pid" || ! "$pid" =~ ^[0-9]+$ || "$pid" -le 0 ]]; then
          reason="failure:orphaned_runtime_missing_pid"
          runtime_mark="stale"
          msg="runtime ${runtime_status} has no pid"
          if [[ "$state" == "todo" || "$state" == "merged" || "$state" == "abandoned" ]]; then
            set_task_runtime "$task_id" "$runtime_mark" "$phase" "" "$msg" "$(now_iso)"
            emit_event "task_runtime_orphan" "Orphan runtime without PID handled (runtime-only)" "$task_id" "$state" "runtime_status=${runtime_status} policy=runtime_only" "$phase" "stale"
            note "Orphan runtime handled for ${task_id}: runtime=${runtime_status} missing pid (state=${state}, runtime only)"
            continue
          fi
          transition_orphan_task_state "$task_id" "$state" "$target_state" "$reason"
          set_task_runtime "$task_id" "$runtime_mark" "$phase" "" "$msg" "$(now_iso)"
          emit_event "task_runtime_orphan" "Orphan runtime without PID handled" "$task_id" "$target_state" "runtime_status=${runtime_status} policy=${target_state}" "$phase" "stale"
          note "Orphan runtime handled for ${task_id}: runtime=${runtime_status} missing pid -> ${target_state}"
          continue
        fi
        if ! pid_is_running "$pid"; then
          reason="failure:orphaned_runtime_pid"
          runtime_mark="failed"
          msg="runtime pid ${pid} is not running"
          if [[ "$state" == "todo" || "$state" == "merged" || "$state" == "abandoned" ]]; then
            set_task_runtime "$task_id" "$runtime_mark" "$phase" "" "$msg" "$(now_iso)"
            emit_event "task_runtime_orphan" "Orphan runtime with dead PID handled (runtime-only)" "$task_id" "$state" "pid=${pid} runtime_status=${runtime_status} policy=runtime_only" "$phase" "failed"
            note "Orphan runtime handled for ${task_id}: runtime=${runtime_status} dead pid=${pid} (state=${state}, runtime only)"
            continue
          fi
          transition_orphan_task_state "$task_id" "$state" "$target_state" "$reason"
          set_task_runtime "$task_id" "$runtime_mark" "$phase" "" "$msg" "$(now_iso)"
          emit_event "task_runtime_orphan" "Orphan runtime with dead PID handled" "$task_id" "$target_state" "pid=${pid} runtime_status=${runtime_status} policy=${target_state}" "$phase" "failed"
          note "Orphan runtime handled for ${task_id}: runtime=${runtime_status} dead pid=${pid} -> ${target_state}"
        fi
        ;;
    esac
  done < <(jq -c '
    (.tasks // [])[]
    | {
        task_id: .id,
        state: (.state // ""),
        runtime_status: (.task_runtime.status // ""),
        pid: ((.task_runtime.pid // "")|tostring),
        phase: (.task_runtime.current_phase // ""),
        last_commit_created_at: (.task_runtime.last_commit_created_at // "")
      }
  ' "$TASK_REGISTRY_FILE")
}

cleanup_stale_tasks() {
  local now_epoch task_id state claimed_at threshold
  now_epoch="$(date +%s)"

  while IFS=$'\t' read -r task_id state claimed_at; do
    [[ -n "$task_id" && -n "$claimed_at" ]] || continue
    local claimed_epoch
    claimed_epoch="$(date -d "$claimed_at" +%s 2>/dev/null || echo 0)"
    [[ "$claimed_epoch" -gt 0 ]] || continue
    local age=$((now_epoch - claimed_epoch))

    case "$state" in
      claimed) threshold="$STALE_CLAIMED_SECONDS" ;;
      in_progress) threshold="$STALE_IN_PROGRESS_SECONDS" ;;
      changes_requested) threshold="$STALE_CHANGES_REQUESTED_SECONDS" ;;
      *) threshold="0" ;;
    esac

    if [[ "$threshold" -gt 0 && "$age" -ge "$threshold" ]]; then
      local target_state
      case "$STALE_ACTION" in
        abandon) target_state="abandoned" ;;
        todo) target_state="todo" ;;
        blocked) target_state="blocked" ;;
        *) target_state="abandoned" ;;
      esac
      apply_transition "$task_id" "$target_state" "" "" "stale"
      echo "Auto-${target_state} stale task: ${task_id} (state=${state}, age=${age}s)"
    fi
  done < <(jq -r '
    (.tasks // [])[]
    | select(.state == "claimed" or .state == "in_progress" or .state == "changes_requested")
    | [.id, .state, (.claimed_at // "")] | @tsv
  ' "$TASK_REGISTRY_FILE")

  if [[ "$STALE_HEARTBEAT_SECONDS" -gt 0 ]]; then
    while IFS=$'\t' read -r task_id task_state runtime_status current_phase last_heartbeat started_at; do
      [[ -n "$task_id" ]] || continue
      if [[ "$task_state" == "queued" && "$current_phase" == "integrate" ]]; then
        continue
      fi
      local ref_ts heartbeat_epoch age
      ref_ts="$last_heartbeat"
      [[ -n "$ref_ts" ]] || ref_ts="$started_at"
      [[ -n "$ref_ts" ]] || continue
      heartbeat_epoch="$(date -d "$ref_ts" +%s 2>/dev/null || echo 0)"
      [[ "$heartbeat_epoch" -gt 0 ]] || continue
      age=$((now_epoch - heartbeat_epoch))
      [[ "$age" -ge "$STALE_HEARTBEAT_SECONDS" ]] || continue

      case "$STALE_HEARTBEAT_ACTION" in
        retry)
          increment_task_retries "$task_id" "stale_heartbeat_retry"
          set_task_runtime "$task_id" "dispatched" "" "" "stale heartbeat (${age}s)" "$(now_iso)"
          emit_event "task_runtime_retry" "Runtime stale heartbeat; retry requested" "$task_id" "dispatched" "age=${age}s"
          emit_event "stale_runtime_total" "Stale runtime detected" "$task_id" "dispatched" "age=${age}s action=retry" "$current_phase" "stale"
          ;;
        requeue)
          apply_transition "$task_id" "todo" "" "" "stale_heartbeat_requeue"
          set_task_runtime "$task_id" "stale" "" "" "stale heartbeat (${age}s)" "$(now_iso)"
          emit_event "task_runtime_requeue" "Runtime stale heartbeat; task requeued" "$task_id" "todo" "age=${age}s"
          emit_event "stale_runtime_total" "Stale runtime detected" "$task_id" "todo" "age=${age}s action=requeue" "$current_phase" "stale"
          ;;
        block|*)
          apply_transition "$task_id" "blocked" "" "" "stale_heartbeat"
          set_task_runtime "$task_id" "stale" "" "" "stale heartbeat (${age}s)" "$(now_iso)"
          emit_event "task_runtime_stale" "Runtime stale heartbeat; task blocked" "$task_id" "blocked" "age=${age}s"
          emit_event "stale_runtime_total" "Stale runtime detected" "$task_id" "blocked" "age=${age}s action=block" "$current_phase" "stale"
          ;;
      esac
      echo "Stale heartbeat handled: ${task_id} (state=${task_state}, runtime=${runtime_status}, phase=${current_phase}, age=${age}s)"
    done < <(jq -r '
      (.tasks // [])[]
      | select((.task_runtime.status // "") == "running" or (.task_runtime.status // "") == "dispatched")
      | [
          .id,
          (.state // ""),
          (.task_runtime.status // ""),
          (.task_runtime.current_phase // ""),
          (.task_runtime.last_heartbeat // ""),
          (.task_runtime.started_at // "")
        ] | @tsv
    ' "$TASK_REGISTRY_FILE")
  fi
}
