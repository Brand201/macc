# shellcheck shell=bash

is_pool_worktree_path() {
  local path="$1"
  local prefix="${REPO_DIR}/.macc/worktree/"
  case "$path" in
    "$prefix"*) return 0 ;;
    *) return 1 ;;
  esac
}

worktree_metadata_file() {
  local worktree_path="$1"
  echo "${worktree_path}/.macc/worktree.json"
}

worktree_metadata_field() {
  local worktree_path="$1"
  local field="$2"
  local default="${3:-}"
  local metadata
  metadata="$(worktree_metadata_file "$worktree_path")"
  if [[ ! -f "$metadata" ]]; then
    echo "$default"
    return 0
  fi
  jq -r --arg def "$default" ".${field} // \$def" "$metadata" 2>/dev/null || echo "$default"
}

worktree_is_clean() {
  local worktree_path="$1"
  [[ -d "$worktree_path" ]] || return 1
  ! git -C "$worktree_path" status --porcelain | awk 'NF' | grep -q .
}

prepare_reused_worktree() {
  local worktree_path="$1"
  local branch="$2"
  local base_branch="$3"

  if ! git -C "$REPO_DIR" rev-parse --verify "$base_branch" >/dev/null 2>&1; then
    return 1
  fi
  if ! git -C "$worktree_path" checkout "$branch" >/dev/null 2>&1; then
    return 1
  fi
  if ! git -C "$worktree_path" reset --hard "$base_branch" >/dev/null 2>&1; then
    return 1
  fi
  worktree_is_clean "$worktree_path"
}

find_idle_compatible_worktree() {
  local selected_tool="$1"
  local base_branch="$2"
  local requested_scope="${3:-}"

  while IFS= read -r path; do
    [[ -n "$path" ]] || continue
    is_pool_worktree_path "$path" || continue
    worktree_exists_on_disk "$path" || continue
    worktree_in_use "$path" && continue
    worktree_is_clean "$path" || continue

    local wt_tool wt_base wt_scope wt_branch
    wt_tool="$(worktree_metadata_field "$path" "tool" "")"
    wt_base="$(worktree_metadata_field "$path" "base" "")"
    wt_scope="$(worktree_metadata_field "$path" "scope" "")"
    wt_branch="$(worktree_metadata_field "$path" "branch" "")"

    [[ -n "$wt_tool" && "$wt_tool" == "$selected_tool" ]] || continue
    [[ -n "$wt_base" && "$wt_base" == "$base_branch" ]] || continue
    if [[ -n "$requested_scope" ]]; then
      [[ "$wt_scope" == "$requested_scope" ]] || continue
    fi
    if [[ -z "$wt_branch" ]]; then
      wt_branch="$(git -C "$path" rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
    fi
    [[ -n "$wt_branch" ]] || continue
    [[ -f "${path}/.macc/tool.json" ]] || continue

    if ! prepare_reused_worktree "$path" "$wt_branch" "$base_branch"; then
      continue
    fi

    local last_commit
    last_commit="$(git -C "$path" rev-parse HEAD 2>/dev/null || true)"
    [[ -n "$last_commit" ]] || continue
    printf '%s\t%s\t%s\n' "$path" "$wt_branch" "$last_commit"
    return 0
  done < <(
    git -C "$REPO_DIR" worktree list --porcelain \
      | awk '/^worktree /{print substr($0,10)}' \
      | LC_ALL=C sort
  )

  return 1
}

select_next_ready_task_tsv() {
  macc --cwd "$REPO_DIR" coordinator select-ready-task -- \
    --registry "$TASK_REGISTRY_FILE" \
    --enabled-tools-json "$ENABLED_TOOLS_JSON" \
    --tool-priority-json "$TOOL_PRIORITY_JSON" \
    --max-parallel-per-tool-json "$MAX_PARALLEL_PER_TOOL_JSON" \
    --tool-specializations-json "$TOOL_SPECIALIZATIONS_JSON" \
    --max-parallel "$MAX_PARALLEL" \
    --default-tool "$DEFAULT_TOOL" \
    --default-base-branch "$DEFAULT_BASE_BRANCH"
}

create_macc_worktree() {
  local task_id="$1"
  local title="$2"
  local tool="$3"
  local requested_base_branch="$4"
  local base_branch
  base_branch="$(resolve_base_branch "$requested_base_branch")"

  local slug
  slug="$(safe_slug "${task_id}-${title}")"

  local output
  if ! output="$(
    macc --cwd "$REPO_DIR" worktree create "$slug" \
      --tool "$tool" \
      --count 1 \
      --base "$base_branch" 2>&1
  )"; then
    echo "Error: macc worktree create failed for task ${task_id}" >&2
    echo "$output" >&2
    return 1
  fi

  local worktree_path branch
  worktree_path="$(printf '%s\n' "$output" | sed -n 's/.* path=\(.*\)$/\1/p' | tail -n1)"
  branch="$(printf '%s\n' "$output" | sed -n 's/.* branch=\([^ ]*\).*/\1/p' | tail -n1)"

  if [[ -z "$worktree_path" ]]; then
    worktree_path="$(git -C "$REPO_DIR" worktree list --porcelain \
      | awk '/^worktree /{print substr($0,10)}' \
      | grep "/.macc/worktree/" \
      | tail -n1 || true)"
  fi

  [[ -n "$worktree_path" ]] || {
    echo "Error: could not resolve created worktree path for task ${task_id}" >&2
    return 1
  }

  if [[ -z "$branch" ]]; then
    branch="$(git -C "$worktree_path" rev-parse --abbrev-ref HEAD)"
  fi

  local last_commit
  last_commit="$(git -C "$worktree_path" rev-parse HEAD)"

  printf '%s\t%s\t%s\n' "$worktree_path" "$branch" "$last_commit"
}

write_worktree_prd() {
  local worktree_path="$1"
  local task_id="$2"
  local out_path="${worktree_path}/worktree.prd.json"
  local tmp
  tmp="$(mktemp)"

  jq --arg id "$task_id" '
    . as $p
    | {
        lot: ($p.lot // ""),
        version: ($p.version // ""),
        generated_at: ($p.generated_at // ""),
        timezone: ($p.timezone // "UTC"),
        priority_mapping: ($p.priority_mapping // {}),
        assumptions: ($p.assumptions // []),
        tasks: (($p.tasks // []) | map(select((.id|tostring) == $id)))
      }
  ' "$PRD_FILE" >"$tmp"

  if ! jq -e '.tasks | length == 1' "$tmp" >/dev/null 2>&1; then
    rm -f "$tmp"
    echo "Error: could not extract unique task '$task_id' from PRD for worktree.prd.json" >&2
    return 1
  fi

  mv "$tmp" "$out_path"
}

mark_task_claimed() {
  local task_id="$1"
  local worktree_path="$2"
  local branch="$3"
  local base_branch="$4"
  local last_commit="$5"
  local selected_tool="$6"

  local now session_id tmp
  now="$(now_iso)"
  session_id="${AGENT_ID}-${task_id}-$(date -u +%Y%m%dT%H%M%SZ)"
  tmp="$(mktemp)"

  jq --arg id "$task_id" \
     --arg agent "$AGENT_ID" \
     --arg now "$now" \
     --arg wp "$worktree_path" \
     --arg branch "$branch" \
     --arg base "$base_branch" \
     --arg commit "$last_commit" \
     --arg session "$session_id" \
     --arg selected_tool "$selected_tool" \
     '
     . as $root
     | .tasks |= map(
       if .id == $id then
         .state = "claimed"
         | .tool = $selected_tool
         | .assignee = $agent
         | .claimed_at = $now
         | .worktree = {
             worktree_path: $wp,
             branch: $branch,
             base_branch: $base,
             last_commit: $commit,
             session_id: $session
           }
         | .task_runtime = (.task_runtime // {})
         | .task_runtime.status = "dispatched"
         | .task_runtime.current_phase = "dev"
         | .task_runtime.started_at = $now
         | .task_runtime.last_heartbeat = $now
         | .task_runtime.last_error = null
         | .task_runtime.pid = null
         | .task_runtime.merge_result_pending = false
         | .task_runtime.merge_result_file = null
         | .task_runtime.merge_worker_pid = null
       else
         .
       end
     )
     | .resource_locks |= (
         . as $locks
         | ((($root.tasks // []) | map(select(.id == $id) | .exclusive_resources // []) | .[0]) // []) as $res
         | reduce $res[] as $r ($locks; .[$r] = {
             task_id: $id,
             worktree_path: $wp,
             locked_at: $now
           })
       )
     | .updated_at = $now
     ' "$TASK_REGISTRY_FILE" >"$tmp"

  mv "$tmp" "$TASK_REGISTRY_FILE"
}

invoke_performer() {
  local task_id="$1"
  local worktree_path="$2"
  local tool="$3"
  local root_config="${REPO_DIR}/.macc/macc.yaml"
  local worktree_config="${worktree_path}/.macc/macc.yaml"

  if [[ -f "$root_config" ]]; then
    cp "$root_config" "$worktree_config"
  fi

  if ! macc --cwd "$REPO_DIR" worktree apply "$worktree_path" >/dev/null 2>&1; then
    echo "Error: failed to apply config before performer run (task=${task_id}, tool=${tool}, worktree=${worktree_path})" >&2
    return 1
  fi

  COORD_EVENTS_FILE="$COORD_EVENTS_FILE" \
  MACC_EVENT_SOURCE="performer:${tool}:${task_id}:$(date +%s%N)" \
  MACC_EVENT_TASK_ID="$task_id" \
  macc --cwd "$REPO_DIR" worktree run "$worktree_path"
}

resolve_tool_runner() {
  local worktree_path="$1"
  local tool="${2:-}"
  if [[ -n "$tool" ]]; then
    local explicit_runner="${worktree_path}/.macc/automation/runners/${tool}.performer.sh"
    if [[ -x "$explicit_runner" ]]; then
      echo "$explicit_runner"
      return 0
    fi
  fi
  local tool_json="${worktree_path}/.macc/tool.json"
  [[ -f "$tool_json" ]] || {
    echo ""
    return 0
  }
  local runner
  runner="$(jq -r '.performer.runner // ""' "$tool_json")"
  [[ -n "$runner" && "$runner" != "null" ]] || {
    echo ""
    return 0
  }
  if [[ "$runner" = /* ]]; then
    echo "$runner"
  else
    echo "${REPO_DIR}/${runner}"
  fi
}

build_phase_prompt() {
  local mode="$1"
  local task_id="$2"
  local tool="$3"

  local task_json
  task_json="$(jq -c --arg id "$task_id" '(.tasks // []) | map(select(.id == $id)) | .[0] // {}' "$TASK_REGISTRY_FILE")"

  cat <<PROMPT
You are the assigned ${tool} performer running inside a MACC worktree.

Mode: ${mode}
Task ID: ${task_id}

Task registry entry (JSON):
${task_json}

Instructions:
1) Execute the ${mode} phase only.
2) Keep changes minimal and focused on this task.
3) Update code/tests/docs as needed for this phase.
4) Do not modify task registry state directly.
PROMPT
}

invoke_tool_phase_runner() {
  local mode="$1"
  local task_id="$2"
  local worktree_path="$3"
  local tool="$4"
  local tool_json="${worktree_path}/.macc/tool.json"

  [[ -f "$tool_json" ]] || {
    echo "Error: missing tool.json for ${mode} fallback: $tool_json" >&2
    return 1
  }

  local runner
  runner="$(resolve_tool_runner "$worktree_path" "$tool")"
  [[ -n "$runner" && -x "$runner" ]] || {
    echo "Error: tool phase runner missing or not executable for ${mode}: ${runner}" >&2
    return 1
  }

  local prompt_file
  prompt_file="$(mktemp)"
  build_phase_prompt "$mode" "$task_id" "$tool" >"$prompt_file"

  COORD_EVENTS_FILE="$COORD_EVENTS_FILE" \
  MACC_EVENT_SOURCE="performer:${tool}:${task_id}:$(date +%s%N)" \
  MACC_EVENT_TASK_ID="$task_id" \
  "$runner" \
    --prompt-file "$prompt_file" \
    --tool-json "$tool_json" \
    --repo "$REPO_DIR" \
    --worktree "$worktree_path" \
    --task-id "$task_id" \
    --attempt 1 \
    --max-attempts "$PHASE_RUNNER_MAX_ATTEMPTS"
  local status=$?
  rm -f "$prompt_file"
  return "$status"
}

maybe_run_phase_hook() {
  local task_id="$1"
  local new_state="$2"
  local worktree_path="$3"
  local tool="$4"

  case "$new_state" in
    pr_open)
      invoke_tool_phase_runner review "$task_id" "$worktree_path" "$tool" || return 1
      ;;
    changes_requested)
      invoke_tool_phase_runner fix "$task_id" "$worktree_path" "$tool" || return 1
      ;;
    queued)
      invoke_tool_phase_runner integrate "$task_id" "$worktree_path" "$tool" || return 1
      ;;
  esac
}

worktree_for_task() {
  local task_id="$1"
  jq -r --arg id "$task_id" '(.tasks // [])[] | select(.id == $id) | .worktree.worktree_path // ""' "$TASK_REGISTRY_FILE"
}

tool_for_task() {
  local task_id="$1"
  jq -r --arg id "$task_id" '(.tasks // [])[] | select(.id == $id) | .tool // ""' "$TASK_REGISTRY_FILE"
}

coordinator_phase_tool_for_task() {
  local task_id="$1"
  if [[ -n "$COORDINATOR_TOOL" ]]; then
    echo "$COORDINATOR_TOOL"
    return 0
  fi
  jq -r --arg id "$task_id" '
    (.tasks // [])[] | select(.id == $id) | (.coordinator_tool // .tool // "")
  ' "$TASK_REGISTRY_FILE"
}

transition_task_and_hooks() {
  local task_id="$1"
  local new_state="$2"
  local pr_url="${3:-}"
  local reviewer="${4:-}"
  local reason="${5:-}"
  local current wt tool

  current="$(task_state "$task_id")"
  validate_transition "$current" "$new_state" || return 1
  apply_transition "$task_id" "$new_state" "$pr_url" "$reviewer" "$reason"
  case "$new_state" in
    claimed)
      set_task_runtime "$task_id" "dispatched" "dev" "" "" "$(now_iso)"
      ;;
    in_progress)
      set_task_runtime "$task_id" "running" "dev" "" "" "$(now_iso)"
      ;;
    pr_open)
      set_task_runtime "$task_id" "running" "review" "" "" "$(now_iso)"
      ;;
    changes_requested)
      set_task_runtime "$task_id" "running" "fix" "" "" "$(now_iso)"
      ;;
    queued)
      set_task_runtime "$task_id" "phase_done" "integrate" "" "" "$(now_iso)"
      ;;
    merged)
      set_task_runtime "$task_id" "idle" "" "" "" "$(now_iso)"
      ;;
    blocked|abandoned)
      set_task_runtime "$task_id" "failed" "" "" "${reason:-workflow transition to ${new_state}}" "$(now_iso)"
      ;;
    todo)
      set_task_runtime "$task_id" "idle" "" "" "" "$(now_iso)"
      ;;
  esac

  if [[ "$new_state" == "pr_open" || "$new_state" == "changes_requested" || "$new_state" == "queued" ]]; then
    wt="$(worktree_for_task "$task_id")"
    tool="$(coordinator_phase_tool_for_task "$task_id")"
    if [[ -n "$wt" && -n "$tool" ]]; then
      if ! maybe_run_phase_hook "$task_id" "$new_state" "$wt" "$tool"; then
        note "Warning: phase hook failed for ${task_id} (${new_state}); continuing state machine."
        emit_event "phase_result" "Coordinator phase hook failed" "$task_id" "$new_state" "tool=${tool}" "$new_state" "failed"
      else
        case "$new_state" in
          pr_open)
            emit_event "review_done" "Review phase completed" "$task_id" "$new_state" "tool=${tool}" "review" "done"
            ;;
          changes_requested)
            emit_event "phase_result" "Fix phase completed" "$task_id" "$new_state" "tool=${tool}" "fix" "done"
            ;;
          queued)
            emit_event "integrate_done" "Integrate phase completed" "$task_id" "$new_state" "tool=${tool}" "integrate" "done"
            ;;
        esac
      fi
    fi
  fi
}

task_worktree_field() {
  local task_id="$1"
  local field="$2"
  jq -r --arg id "$task_id" "(.tasks // [])[] | select(.id == \$id) | (.worktree.${field} // \"\")" "$TASK_REGISTRY_FILE"
}

task_pr_url() {
  local task_id="$1"
  jq -r --arg id "$task_id" '(.tasks // [])[] | select(.id == $id) | (.pr_url // "")' "$TASK_REGISTRY_FILE"
}

advance_active_tasks() {
  reconcile_orphan_runtime_tasks
  local progressed="false"
  for _ in $(seq 1 16); do
    local pass_progressed="false"
    while IFS=$'\t' read -r task_id state; do
      [[ -n "$task_id" && -n "$state" ]] || continue
      local worktree_path tool branch base_branch pr_url
      worktree_path="$(worktree_for_task "$task_id")"
      tool="$(coordinator_phase_tool_for_task "$task_id")"
      branch="$(task_worktree_field "$task_id" "branch")"
      base_branch="$(task_worktree_field "$task_id" "base_branch")"
      pr_url="$(task_pr_url "$task_id")"

      case "$state" in
        in_progress)
          local pr_create_json hook_status new_pr_url
          if pr_create_json="$(run_vcs_hook_json "pr_create" "$task_id" "$worktree_path" "$tool" "$branch" "$base_branch" "$pr_url")"; then
            new_pr_url="$(jq -r '.pr_url // ""' <<<"$pr_create_json")"
            if [[ -n "$new_pr_url" ]]; then
              transition_task_and_hooks "$task_id" "pr_open" "$new_pr_url" "" "auto:pr_create"
              note "Advance: ${task_id} in_progress -> pr_open"
              pass_progressed="true"
              progressed="true"
            fi
          else
            hook_status=$?
            if [[ "$hook_status" -eq 2 ]]; then
              transition_task_and_hooks "$task_id" "pr_open" "local://${branch}" "" "auto:local_pr"
              note "Advance: ${task_id} in_progress -> pr_open (local)"
              pass_progressed="true"
              progressed="true"
            else
              transition_task_and_hooks "$task_id" "blocked" "" "" "failure:pr_create"
              note "Blocked task due to PR creation failure: ${task_id}"
              pass_progressed="true"
              progressed="true"
            fi
          fi
          ;;
        pr_open)
          local review_json ci_json review_hook_status ci_hook_status decision ci_status reviewer reason
          decision="approved"
          reviewer=""
          reason=""
          ci_status="green"

          if review_json="$(run_vcs_hook_json "review_status" "$task_id" "$worktree_path" "$tool" "$branch" "$base_branch" "$pr_url")"; then
            decision="$(jq -r '.decision // "approved"' <<<"$review_json")"
            reviewer="$(jq -r '.reviewer // ""' <<<"$review_json")"
            reason="$(jq -r '.reason // ""' <<<"$review_json")"
          else
            review_hook_status=$?
            if [[ "$review_hook_status" -ne 2 ]]; then
              transition_task_and_hooks "$task_id" "blocked" "" "" "failure:review_hook"
              note "Blocked task due to review hook failure: ${task_id}"
              pass_progressed="true"
              progressed="true"
              continue
            fi
          fi

          if ci_json="$(run_vcs_hook_json "ci_status" "$task_id" "$worktree_path" "$tool" "$branch" "$base_branch" "$pr_url")"; then
            ci_status="$(jq -r '.status // "green"' <<<"$ci_json")"
            local ci_reason
            ci_reason="$(jq -r '.reason // ""' <<<"$ci_json")"
            if [[ -n "$ci_reason" ]]; then
              reason="$ci_reason"
            fi
          else
            ci_hook_status=$?
            if [[ "$ci_hook_status" -ne 2 ]]; then
              transition_task_and_hooks "$task_id" "blocked" "" "" "failure:ci_hook"
              note "Blocked task due to CI hook failure: ${task_id}"
              pass_progressed="true"
              progressed="true"
              continue
            fi
          fi

          case "$decision:$ci_status" in
            changes_requested:*)
              transition_task_and_hooks "$task_id" "changes_requested" "" "$reviewer" "${reason:-auto:review_changes_requested}"
              note "Advance: ${task_id} pr_open -> changes_requested"
              pass_progressed="true"
              progressed="true"
              ;;
            *:red|*:failed|*:failure)
              transition_task_and_hooks "$task_id" "changes_requested" "" "$reviewer" "${reason:-failure:ci_red}"
              note "Advance: ${task_id} pr_open -> changes_requested (CI red)"
              pass_progressed="true"
              progressed="true"
              ;;
            *:pending|*:running)
              ;;
            *)
              transition_task_and_hooks "$task_id" "queued" "" "$reviewer" "${reason:-auto:review_ci_ok}"
              note "Advance: ${task_id} pr_open -> queued"
              pass_progressed="true"
              progressed="true"
              ;;
          esac
          ;;
        changes_requested)
          # Keep flow moving: after fix phase, reopen PR for review.
          transition_task_and_hooks "$task_id" "pr_open" "$pr_url" "" "auto:reopen_after_fix"
          note "Advance: ${task_id} changes_requested -> pr_open"
          pass_progressed="true"
          progressed="true"
          ;;
        queued)
          local queue_json merge_json queue_hook_status merge_hook_status queue_status merge_status
          queue_status="queued"
          merge_status="pending"

          if queue_json="$(run_vcs_hook_json "queue_status" "$task_id" "$worktree_path" "$tool" "$branch" "$base_branch" "$pr_url")"; then
            queue_status="$(jq -r '.status // "queued"' <<<"$queue_json")"
          else
            queue_hook_status=$?
            if [[ "$queue_hook_status" -ne 2 ]]; then
              transition_task_and_hooks "$task_id" "blocked" "" "" "failure:merge_queue_hook"
              note "Blocked task due to merge queue hook failure: ${task_id}"
              pass_progressed="true"
              progressed="true"
              continue
            fi
          fi

          case "$queue_status" in
            failed|error)
              transition_task_and_hooks "$task_id" "pr_open" "$pr_url" "" "failure:merge_queue_fail"
              note "Advance: ${task_id} queued -> pr_open (queue failure)"
              pass_progressed="true"
              progressed="true"
              continue
              ;;
            pending|queued|running)
              ;;
            ready|ok|merged)
              ;;
          esac

          if merge_json="$(run_vcs_hook_json "merge_status" "$task_id" "$worktree_path" "$tool" "$branch" "$base_branch" "$pr_url")"; then
            merge_status="$(jq -r '.status // "pending"' <<<"$merge_json")"
            case "$merge_status" in
              merged|ok)
                transition_task_and_hooks "$task_id" "merged" "$pr_url" "" "auto:merged"
                note "Advance: ${task_id} queued -> merged"
                pass_progressed="true"
                progressed="true"
                ;;
              failed|error)
                transition_task_and_hooks "$task_id" "pr_open" "$pr_url" "" "failure:merge_queue_fail"
                note "Advance: ${task_id} queued -> pr_open (merge failed)"
                pass_progressed="true"
                progressed="true"
                ;;
              *)
                ;;
            esac
          else
            merge_hook_status=$?
            if [[ "$merge_hook_status" -eq 2 ]]; then
              if is_truthy "$COORDINATOR_AUTOMERGE"; then
                if [[ "$COORD_COMMAND_NAME" == "run" ]]; then
                  if merge_job_pending_for_task "$task_id"; then
                    :
                  elif [[ "$(task_has_pending_merge_result "$task_id")" == "true" ]]; then
                    :
                  elif merge_job_is_running_for_task "$task_id"; then
                    :
                  elif [[ "$(run_loop_merge_job_count)" -gt 0 ]]; then
                    :
                  else
                    start_local_merge_worker_async "$task_id" "$branch" "$base_branch" "$pr_url"
                    pass_progressed="true"
                    progressed="true"
                  fi
                else
                  if local_merge_branch_into_base "$task_id" "$branch" "$base_branch"; then
                    transition_task_and_hooks "$task_id" "merged" "$pr_url" "" "auto:local_merge"
                    note "Advance: ${task_id} queued -> merged (local merge)"
                    pass_progressed="true"
                    progressed="true"
                  else
                    local merge_error
                    merge_error="${LOCAL_MERGE_LAST_ERROR:-failure:local_merge}"
                    transition_task_and_hooks "$task_id" "blocked" "$pr_url" "" "failure:local_merge"
                    set_task_runtime "$task_id" "failed" "integrate" "" "$merge_error" "$(now_iso)"
                    emit_event "local_merge_failed" "Local merge failed" "$task_id" "blocked" "$merge_error" "integrate" "failed"
                    emit_event "merge_fail_total" "Merge failure recorded" "$task_id" "blocked" "$merge_error" "integrate" "failed"
                    note "Blocked task due to local merge failure: ${task_id}"
                    if [[ -n "${LOCAL_MERGE_LAST_ERROR:-}" ]]; then
                      note "Local merge detail (${task_id}): ${LOCAL_MERGE_LAST_ERROR}"
                    fi
                    pass_progressed="true"
                    progressed="true"
                  fi
                fi
              fi
            else
              transition_task_and_hooks "$task_id" "blocked" "$pr_url" "" "failure:merge_hook"
              note "Blocked task due to merge hook failure: ${task_id}"
              pass_progressed="true"
              progressed="true"
            fi
          fi
          ;;
      esac
    done < <(jq -r '
      (.tasks // [])[]
      | select(.state == "in_progress" or .state == "pr_open" or .state == "changes_requested" or .state == "queued")
      | [.id, .state] | @tsv
    ' "$TASK_REGISTRY_FILE")
    if [[ "$pass_progressed" != "true" ]]; then
      break
    fi
  done

  if [[ "$progressed" == "true" ]]; then
    note "Advance complete: state transitions applied."
  else
    note "Advance complete: no transitions."
  fi
}

handle_performer_completion() {
  local task_id="$1"
  local tool="$2"
  local rc="$3"
  if [[ "$rc" -ne 0 ]]; then
    local structured_error existing_error runtime_error
    structured_error="$(jq -r --arg id "$task_id" '
      def payload_obj:
        if (.payload | type) == "object" then
          if ((.payload.value? // null) | type) == "string" then
            ((.payload.value | fromjson?) // .payload)
          else
            .payload
          end
        elif (.payload | type) == "string" then
          ((.payload | fromjson?) // {})
        else
          {}
        end;

      select(.task_id == $id)
      | (.type // .event // "") as $event
      | select($event == "failed" or ($event == "phase_result" and ((.status // .state // "") == "failed")))
      | payload_obj as $p
      | ($p.error // $p.reason // $p.message // "") as $reason
      | ($p.matched_output // "") as $match
      | if ($reason | length) > 0 and ($match | length) > 0 and $match != $reason then
          ($reason + " | " + $match)
        elif ($reason | length) > 0 then
          $reason
        elif ($match | length) > 0 then
          $match
        else
          empty
        end
    ' "$COORD_EVENTS_FILE" 2>/dev/null | tail -n1 || true)"
    existing_error="$(jq -r --arg id "$task_id" '
      (.tasks // [])
      | map(select(.id == $id))
      | .[0].task_runtime.last_error // ""
    ' "$TASK_REGISTRY_FILE" 2>/dev/null || true)"

    if [[ -n "$structured_error" ]]; then
      runtime_error="$structured_error"
    elif [[ -n "$existing_error" ]]; then
      runtime_error="$existing_error"
      if [[ "$runtime_error" != *"exit status"* ]]; then
        runtime_error="${runtime_error} (exit status ${rc})"
      fi
    else
      runtime_error="performer exited with status ${rc}"
    fi
    set_task_runtime "$task_id" "failed" "dev" "" "$runtime_error" "$(now_iso)"
    apply_transition "$task_id" "blocked" "" "" "failure:performer"
    note "Blocked task due to performer failure: ${task_id}"
    if [[ -n "$runtime_error" ]]; then
      note "Performer failure detail (${task_id}): ${runtime_error}"
    fi
    emit_event "task_blocked" "Performer failed" "$task_id" "blocked"
  else
    transition_task_and_hooks "$task_id" "in_progress" "" "" "auto:performer_complete"
    set_task_runtime "$task_id" "phase_done" "dev" "" "" "$(now_iso)"
    note "Performer complete: ${task_id} (${tool})"
    emit_event "performer_complete" "Performer completed task phase" "$task_id" "in_progress"
  fi
}

merge_job_is_running_for_task() {
  local task_id="$1"
  local entry pid queued_task
  for entry in "${RUN_LOOP_MERGE_JOBS[@]}"; do
    IFS='|' read -r pid queued_task _ _ _ _ <<<"$entry"
    if [[ "$queued_task" == "$task_id" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      return 0
    fi
  done
  return 1
}

merge_job_pending_for_task() {
  local task_id="$1"
  local entry queued_task
  for entry in "${RUN_LOOP_MERGE_JOBS[@]}"; do
    IFS='|' read -r _ queued_task _ _ _ _ <<<"$entry"
    if [[ "$queued_task" == "$task_id" ]]; then
      return 0
    fi
  done
  return 1
}

start_local_merge_worker_async() {
  local task_id="$1"
  local branch="$2"
  local base_branch="$3"
  local pr_url="$4"
  local safe_task ts ns result_file pid

  if merge_job_pending_for_task "$task_id" || merge_job_is_running_for_task "$task_id" || [[ "$(task_has_pending_merge_result "$task_id")" == "true" ]]; then
    note "Merge worker already pending/running for ${task_id}; skipping duplicate start."
    return 0
  fi

  safe_task="$(printf '%s' "$task_id" | tr '[:space:]' '-' | tr -cd '[:alnum:]_.-')"
  [[ -n "$safe_task" ]] || safe_task="task"
  ts="$(date -u +"%Y%m%dT%H%M%SZ")"
  ns="$(date +%s%N 2>/dev/null || printf '%s000000000' "$(date +%s)")"
  result_file="${COORD_LOG_DIR}/merge-result-${safe_task}-${ts}-${ns}-pid${BASHPID}.json"

  lock_release
  "$COORDINATOR_MERGE_WORKER" \
    --repo "$REPO_DIR" \
    --task-id "$task_id" \
    --branch "$branch" \
    --base-branch "$base_branch" \
    --log-dir "$COORD_LOG_DIR" \
    --result-file "$result_file" \
    --allow-ai-fix "$COORDINATOR_MERGE_AI_FIX" \
    --merge-fix-hook "$COORDINATOR_MERGE_FIX_HOOK" >/dev/null 2>&1 &
  pid=$!
  lock_acquire
  ensure_registry_valid

  RUN_LOOP_MERGE_JOBS+=("${pid}|${task_id}|${branch}|${base_branch}|${pr_url}|${result_file}")
  set_task_merge_result_pending "$task_id" "$result_file" "$pid"
  set_task_runtime "$task_id" "running" "integrate" "$pid" "" "$(now_iso)"
  emit_event "merge_worker_started" "Started local merge worker" "$task_id" "queued" "pid=${pid} branch=${branch} base=${base_branch}" "integrate" "started"
  note "Merge worker started: ${task_id} (pid=${pid}, branch=${branch}, base=${base_branch})"
}

handle_merge_worker_completion() {
  local task_id="$1"
  local branch="$2"
  local base_branch="$3"
  local pr_url="$4"
  local result_file="$5"
  local rc="$6"
  local status error suggestion report_file assisted hook_output

  status="$(jq -r '.status // ""' "$result_file" 2>/dev/null || true)"
  error="$(jq -r '.error // ""' "$result_file" 2>/dev/null || true)"
  suggestion="$(jq -r '.suggestion // ""' "$result_file" 2>/dev/null || true)"
  report_file="$(jq -r '.report_file // ""' "$result_file" 2>/dev/null || true)"
  assisted="$(jq -r '.assisted // false' "$result_file" 2>/dev/null || true)"
  hook_output="$(jq -r '.hook_output // ""' "$result_file" 2>/dev/null || true)"
  if [[ "$assisted" == "true" || -n "$hook_output" ]]; then
    emit_event "merge_fix_attempt_total" "AI merge-fix attempt detected" "$task_id" "$(task_state "$task_id")" "status=${status:-unknown} assisted=${assisted}" "integrate" "attempted"
  fi

  if [[ "$rc" -eq 0 && "$status" == "success" ]]; then
    local current
    current="$(task_state "$task_id")"
    if [[ "$current" == "merged" ]]; then
      :
    elif [[ "$current" == "blocked" ]]; then
      # Merge already succeeded; keep transition idempotent even if orphan handling blocked first.
      apply_transition "$task_id" "merged" "$pr_url" "" "auto:local_merge_worker_recovered"
    elif ! transition_task_and_hooks "$task_id" "merged" "$pr_url" "" "auto:local_merge_worker"; then
      current="$(task_state "$task_id")"
      if [[ "$current" != "merged" ]]; then
        apply_transition "$task_id" "merged" "$pr_url" "" "auto:local_merge_worker_forced"
      fi
    fi
    set_task_runtime "$task_id" "idle" "" "" "" "$(now_iso)"
    set_task_merge_result_processed "$task_id" "$result_file" "$status" "$rc"
    emit_event "merge_worker_complete" "Local merge worker completed successfully" "$task_id" "merged" "branch=${branch} base=${base_branch}" "integrate" "done"
    note "Advance: ${task_id} queued -> merged (local merge worker)"
    return 0
  fi

  local merge_error
  merge_error="${error:-failure:local_merge branch=${branch} base=${base_branch}}"
  if [[ -n "$suggestion" && "$merge_error" != *"suggestion="* ]]; then
    merge_error="${merge_error} suggestion=\"${suggestion}\""
  fi
  if [[ -n "$report_file" && "$merge_error" != *"report="* ]]; then
    merge_error="${merge_error} report=\"${report_file}\""
  fi
  transition_task_and_hooks "$task_id" "blocked" "$pr_url" "" "failure:local_merge"
  set_task_runtime "$task_id" "failed" "integrate" "" "$merge_error" "$(now_iso)"
  set_task_merge_result_processed "$task_id" "$result_file" "${status:-failed}" "$rc"
  emit_event "local_merge_failed" "Local merge worker failed" "$task_id" "blocked" "$merge_error" "integrate" "failed"
  emit_event "merge_fail_total" "Merge failure recorded" "$task_id" "blocked" "$merge_error" "integrate" "failed"
  note "Blocked task due to local merge failure: ${task_id}"
  if [[ -n "$report_file" ]]; then
    note "Local merge report (${task_id}): ${report_file}"
  fi
  mark_run_blocking_merge_failure "$task_id" "$merge_error" "$report_file"
}

monitor_persisted_merge_results_once() {
  local task_id branch base_branch pr_url result_file status inferred_rc
  while IFS=$'\t' read -r task_id branch base_branch pr_url result_file; do
    [[ -n "$task_id" ]] || continue
    merge_job_pending_for_task "$task_id" && continue
    [[ -n "$result_file" && -f "$result_file" ]] || continue
    status="$(jq -r '.status // ""' "$result_file" 2>/dev/null || true)"
    inferred_rc=1
    if [[ "$status" == "success" ]]; then
      inferred_rc=0
    fi
    handle_merge_worker_completion "$task_id" "$branch" "$base_branch" "$pr_url" "$result_file" "$inferred_rc"
  done < <(jq -r '
    (.tasks // [])[]
    | select((.task_runtime.merge_result_pending // false) == true)
    | [
        .id,
        (.worktree.branch // ""),
        (.worktree.base_branch // ""),
        (.pr_url // ""),
        (.task_runtime.merge_result_file // "")
      ]
    | @tsv
  ' "$TASK_REGISTRY_FILE")
}

retry_failed_phase() {
  local task_id="$1"
  local phase="$2"
  local skip_phase="${3:-false}"

  task_exists "$task_id" || {
    echo "Error: task not found in registry: $task_id" >&2
    return 1
  }

  case "$phase" in
    dev|review|fix|integrate) ;;
    *)
      echo "Error: unsupported retry phase '${phase}' (expected: dev|review|fix|integrate)" >&2
      return 1
      ;;
  esac

  if [[ "$skip_phase" == "true" ]]; then
    transition_task_and_hooks "$task_id" "todo" "" "" "manual:skip_phase:${phase}"
    set_task_runtime "$task_id" "idle" "" "" "" "$(now_iso)"
    emit_event "phase_skipped" "Skipped failed phase" "$task_id" "todo" "phase=${phase}" "$phase" "skipped"
    note "Skipped phase '${phase}' for task ${task_id}; task moved back to todo."
    return 0
  fi

  local worktree_path tool current rc performer_pid branch base_branch pr_url
  worktree_path="$(worktree_for_task "$task_id")"
  tool="$(coordinator_phase_tool_for_task "$task_id")"
  current="$(task_state "$task_id")"
  branch="$(task_worktree_field "$task_id" "branch")"
  base_branch="$(task_worktree_field "$task_id" "base_branch")"
  pr_url="$(task_pr_url "$task_id")"

  [[ -n "$worktree_path" && -d "$worktree_path" ]] || {
    echo "Error: retry-phase requires an existing worktree for task ${task_id}" >&2
    return 1
  }
  [[ -n "$tool" ]] || {
    echo "Error: retry-phase could not resolve tool for task ${task_id}" >&2
    return 1
  }

  increment_task_retries "$task_id" "manual_retry_phase:${phase}"

  case "$phase" in
    dev)
      if [[ "$current" == "blocked" || "$current" == "todo" ]]; then
        transition_task_and_hooks "$task_id" "claimed" "" "" "manual:retry_phase:dev"
      fi
      set_task_runtime "$task_id" "running" "dev" "" "" "$(now_iso)"
      emit_event "phase_retry" "Retrying failed phase" "$task_id" "$current" "phase=dev tool=${tool}" "dev" "started"
      lock_release
      invoke_performer "$task_id" "$worktree_path" "$tool" &
      performer_pid=$!
      rc=0
      wait "$performer_pid" || rc=$?
      lock_acquire
      ensure_registry_valid
      handle_performer_completion "$task_id" "$tool" "$rc"
      [[ "$rc" -eq 0 ]]
      return
      ;;
    review)
      emit_event "phase_retry" "Retrying failed phase" "$task_id" "$current" "phase=review tool=${tool}" "review" "started"
      if invoke_tool_phase_runner review "$task_id" "$worktree_path" "$tool"; then
        local target="pr_open"
        current="$(task_state "$task_id")"
        validate_transition "$current" "$target"
        apply_transition "$task_id" "$target" "" "" "manual:retry_phase:review"
        set_task_runtime "$task_id" "running" "review" "" "" "$(now_iso)"
        emit_event "review_done" "Review phase retried successfully" "$task_id" "pr_open" "tool=${tool}" "review" "done"
        note "Retried review phase for task ${task_id}."
      else
        set_task_runtime "$task_id" "failed" "review" "" "retry review phase failed" "$(now_iso)"
        emit_event "phase_result" "Retry review phase failed" "$task_id" "$current" "tool=${tool}" "review" "failed"
        return 1
      fi
      ;;
    fix)
      emit_event "phase_retry" "Retrying failed phase" "$task_id" "$current" "phase=fix tool=${tool}" "fix" "started"
      if invoke_tool_phase_runner fix "$task_id" "$worktree_path" "$tool"; then
        local target="changes_requested"
        current="$(task_state "$task_id")"
        validate_transition "$current" "$target"
        apply_transition "$task_id" "$target" "" "" "manual:retry_phase:fix"
        set_task_runtime "$task_id" "running" "fix" "" "" "$(now_iso)"
        emit_event "phase_result" "Fix phase retried successfully" "$task_id" "changes_requested" "tool=${tool}" "fix" "done"
        note "Retried fix phase for task ${task_id}."
      else
        set_task_runtime "$task_id" "failed" "fix" "" "retry fix phase failed" "$(now_iso)"
        emit_event "phase_result" "Retry fix phase failed" "$task_id" "$current" "tool=${tool}" "fix" "failed"
        return 1
      fi
      ;;
    integrate)
      local runtime_last_error merge_error
      runtime_last_error="$(task_field "$task_id" '.task_runtime.last_error // ""')"
      emit_event "phase_retry" "Retrying failed phase" "$task_id" "$current" "phase=integrate tool=${tool}" "integrate" "started"
      if [[ "$current" == "blocked" ]] && [[ "$runtime_last_error" == *"failure:local_merge"* ]] && is_truthy "$COORDINATOR_AUTOMERGE"; then
        if local_merge_branch_into_base "$task_id" "$branch" "$base_branch"; then
          transition_task_and_hooks "$task_id" "merged" "$pr_url" "" "manual:retry_phase:integrate_local_merge"
          set_task_runtime "$task_id" "idle" "" "" "" "$(now_iso)"
          emit_event "integrate_done" "Integrate phase retried successfully (local merge)" "$task_id" "merged" "tool=${tool}" "integrate" "done"
          note "Retried integrate phase for task ${task_id} (local merge success)."
          return 0
        fi
        merge_error="${LOCAL_MERGE_LAST_ERROR:-failure:local_merge}"
        transition_task_and_hooks "$task_id" "blocked" "$pr_url" "" "failure:local_merge"
        set_task_runtime "$task_id" "failed" "integrate" "" "$merge_error" "$(now_iso)"
        emit_event "local_merge_failed" "Retry integrate phase failed (local merge)" "$task_id" "blocked" "$merge_error" "integrate" "failed"
        emit_event "merge_fail_total" "Merge failure recorded" "$task_id" "blocked" "$merge_error" "integrate" "failed"
        note "Retried integrate phase for task ${task_id} failed (local merge still blocked)."
        return 1
      fi
      if invoke_tool_phase_runner integrate "$task_id" "$worktree_path" "$tool"; then
        local target="queued"
        current="$(task_state "$task_id")"
        validate_transition "$current" "$target"
        apply_transition "$task_id" "$target" "" "" "manual:retry_phase:integrate"
        set_task_runtime "$task_id" "running" "integrate" "" "" "$(now_iso)"
        emit_event "integrate_done" "Integrate phase retried successfully" "$task_id" "queued" "tool=${tool}" "integrate" "done"
        note "Retried integrate phase for task ${task_id}."
      else
        set_task_runtime "$task_id" "failed" "integrate" "" "retry integrate phase failed" "$(now_iso)"
        emit_event "phase_result" "Retry integrate phase failed" "$task_id" "$current" "tool=${tool}" "integrate" "failed"
        return 1
      fi
      ;;
  esac
}

registry_counts_tsv() {
  if command -v macc >/dev/null 2>&1; then
    local counts
    if counts="$(macc --cwd "$REPO_DIR" coordinator state-counts -- 2>/dev/null)"; then
      printf '%s\n' "$counts"
      return 0
    fi
  fi
  jq -r '
    def is_active($s):
      ($s == "claimed" or $s == "in_progress" or $s == "pr_open" or $s == "changes_requested" or $s == "queued");

    [(.tasks // [])[] | (.state // "todo")] as $states
    | [
        ($states | length),
        ($states | map(select(. == "todo")) | length),
        ($states | map(select(is_active(.))) | length),
        ($states | map(select(. == "blocked")) | length),
        ($states | map(select(. == "merged")) | length)
      ]
    | @tsv
  ' "$TASK_REGISTRY_FILE"
}

run_loop_active_job_count() {
  local -A running=()
  local pid
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && running["$pid"]=1
  done < <(jobs -pr)

  local count=0
  local entry task_pid
  for entry in "${RUN_LOOP_ACTIVE_JOBS[@]}"; do
    IFS='|' read -r task_pid _ _ _ <<<"$entry"
    if [[ -n "${running[$task_pid]+x}" ]]; then
      count=$((count + 1))
    fi
  done
  echo "$count"
}

wait_supports_n_with_p() {
  [[ "${WAIT_SUPPORTS_N_WITH_P:-}" != "" ]] && [[ "$WAIT_SUPPORTS_N_WITH_P" == "1" ]]
}

detect_wait_support() {
  if help wait 2>/dev/null | grep -q -- '-n' && help wait 2>/dev/null | grep -q -- '-p'; then
    WAIT_SUPPORTS_N_WITH_P=1
  else
    WAIT_SUPPORTS_N_WITH_P=0
  fi
}

wait_for_any_job_entry() {
  local -n _jobs_ref=$1
  WAIT_JOB_INDEX=""
  WAIT_JOB_TASK=""
  WAIT_JOB_TOOL=""
  WAIT_JOB_RC=0

  if [[ "${WAIT_SUPPORTS_N_WITH_P:-}" == "" ]]; then
    detect_wait_support
  fi

  local -a pids=()
  local i pid task_id wt tool
  for i in "${!_jobs_ref[@]}"; do
    IFS='|' read -r pid task_id wt tool <<<"${_jobs_ref[$i]}"
    [[ -n "$pid" ]] && pids+=("$pid")
  done
  [[ "${#pids[@]}" -gt 0 ]] || return 1

  if wait_supports_n_with_p; then
    local finished_pid="" wait_rc=0
    set +e
    wait -n -p finished_pid "${pids[@]}"
    wait_rc=$?
    set -e
    if [[ -z "$finished_pid" ]]; then
      return 1
    fi
    for i in "${!_jobs_ref[@]}"; do
      IFS='|' read -r pid task_id wt tool <<<"${_jobs_ref[$i]}"
      if [[ "$pid" == "$finished_pid" ]]; then
        WAIT_JOB_INDEX="$i"
        WAIT_JOB_TASK="$task_id"
        WAIT_JOB_TOOL="$tool"
        WAIT_JOB_RC="$wait_rc"
        return 0
      fi
    done
    return 1
  fi

  # Compatibility fallback for older bash: scan known pids and reap first finished.
  local -A running=()
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && running["$pid"]=1
  done < <(jobs -pr)
  for i in "${!_jobs_ref[@]}"; do
    IFS='|' read -r pid task_id wt tool <<<"${_jobs_ref[$i]}"
    if [[ -z "${running[$pid]+x}" ]]; then
      local rc=0
      wait "$pid" || rc=$?
      WAIT_JOB_INDEX="$i"
      WAIT_JOB_TASK="$task_id"
      WAIT_JOB_TOOL="$tool"
      WAIT_JOB_RC="$rc"
      return 0
    fi
  done
  return 1
}

run_loop_merge_job_count() {
  local -A running=()
  local pid
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && running["$pid"]=1
  done < <(jobs -pr)

  local count=0
  local entry merge_pid
  for entry in "${RUN_LOOP_MERGE_JOBS[@]}"; do
    IFS='|' read -r merge_pid _ _ _ _ _ <<<"$entry"
    if [[ -n "${running[$merge_pid]+x}" ]]; then
      count=$((count + 1))
    fi
  done
  echo "$count"
}

monitor_run_loop_jobs_once() {
  local -A running=()
  local pid
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && running["$pid"]=1
  done < <(jobs -pr)

  local -a remaining=()
  local entry task_pid task_id wt tool rc
  for entry in "${RUN_LOOP_ACTIVE_JOBS[@]}"; do
    IFS='|' read -r task_pid task_id wt tool <<<"$entry"
    if [[ -n "${running[$task_pid]+x}" ]]; then
      remaining+=("$entry")
      continue
    fi
    rc=0
    wait "$task_pid" || rc=$?
    handle_performer_completion "$task_id" "$tool" "$rc"
  done
  RUN_LOOP_ACTIVE_JOBS=("${remaining[@]}")
}

monitor_run_loop_merge_jobs_once() {
  local -A running=()
  local pid
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && running["$pid"]=1
  done < <(jobs -pr)

  local -a remaining=()
  local entry merge_pid task_id branch base_branch pr_url result_file rc
  for entry in "${RUN_LOOP_MERGE_JOBS[@]}"; do
    IFS='|' read -r merge_pid task_id branch base_branch pr_url result_file <<<"$entry"
    if [[ -n "${running[$merge_pid]+x}" ]]; then
      remaining+=("$entry")
      continue
    fi
    rc=0
    wait "$merge_pid" || rc=$?
    handle_merge_worker_completion "$task_id" "$branch" "$base_branch" "$pr_url" "$result_file" "$rc"
  done
  RUN_LOOP_MERGE_JOBS=("${remaining[@]}")
  monitor_persisted_merge_results_once
}

stop_run_loop_jobs() {
  local entry pid task_id wt tool
  for entry in "${RUN_LOOP_ACTIVE_JOBS[@]}"; do
    IFS='|' read -r pid task_id wt tool <<<"$entry"
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  done
  RUN_LOOP_ACTIVE_JOBS=()
  for entry in "${RUN_LOOP_MERGE_JOBS[@]}"; do
    IFS='|' read -r pid _ _ _ _ _ <<<"$entry"
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  done
  RUN_LOOP_MERGE_JOBS=()
}

dispatch_ready_tasks_nonblocking() {
  local dispatched=0
  local ready_count
  ready_count="$(jq -r '
    . as $root
    | [($root.tasks // [])[]
        | select((.state // "todo") == "todo" and (.worktree == null))
        | . as $t
        | select((($t.dependencies // []) | all(
            . as $dep
            | (((($root.tasks // []) | map(select(.id == ($dep|tostring)) | .state) | .[0]) // "") == "merged")
          )))
        | select((($t.exclusive_resources // []) | all(
            . as $r
            | (($root.resource_locks[$r].task_id // "") as $owner | ($owner == "" or $owner == $t.id))
          )))
      ]
    | length
  ' "$TASK_REGISTRY_FILE")"
  note "Dispatch config: MAX_DISPATCH=${MAX_DISPATCH} MAX_PARALLEL=${MAX_PARALLEL} MAX_PARALLEL_PER_TOOL_JSON=${MAX_PARALLEL_PER_TOOL_JSON}"
  note "Dispatch ready tasks: ${ready_count}"

  while true; do
    local active_jobs
    active_jobs="$(run_loop_active_job_count)"
    if [[ "$MAX_PARALLEL" -gt 0 && "$active_jobs" -ge "$MAX_PARALLEL" ]]; then
      break
    fi
    if [[ "$MAX_DISPATCH" -gt 0 && "$dispatched" -ge "$MAX_DISPATCH" ]]; then
      break
    fi

    local selected_row selected title tool base_branch
    selected_row="$(select_next_ready_task_tsv)"
    [[ -n "$selected_row" ]] || break
    IFS=$'\t' read -r selected title tool base_branch <<<"$selected_row"
    [[ -n "$selected" ]] || break

    local created task_scope_value dispatch_kind
    dispatch_kind="create"
    task_scope_value="$(task_scope "$selected")"

    if is_truthy "$WORKTREE_POOL_MODE"; then
      created="$(find_idle_compatible_worktree "$tool" "$base_branch" "$task_scope_value" || true)"
      if [[ -n "$created" ]]; then
        dispatch_kind="reuse"
      fi
    fi

    if [[ -z "$created" ]]; then
      if ! created="$(create_macc_worktree "$selected" "$title" "$tool" "$base_branch")"; then
        apply_transition "$selected" "blocked" "" "" "failure:worktree_create"
        note "Blocked task due to worktree create failure: ${selected}"
        continue
      fi
    fi
    local worktree_path branch last_commit
    IFS=$'\t' read -r worktree_path branch last_commit <<<"$created"

    if worktree_in_use "$worktree_path"; then
      local other
      other="$(worktree_task_id "$worktree_path")"
      note "Skip: worktree already in use by task ${other}: ${worktree_path}"
      continue
    fi
    if ! worktree_exists_on_disk "$worktree_path"; then
      echo "Error: worktree path not a git worktree on disk: ${worktree_path}" >&2
      apply_transition "$selected" "blocked" "" "" "failure:worktree_create"
      continue
    fi

    write_worktree_prd "$worktree_path" "$selected"
    mark_task_claimed "$selected" "$worktree_path" "$branch" "$base_branch" "$last_commit" "$tool"

    lock_release
    invoke_performer "$selected" "$worktree_path" "$tool" &
    local performer_pid=$!
    lock_acquire
    ensure_registry_valid
    set_task_runtime "$selected" "running" "dev" "$performer_pid" "" "$(now_iso)"
    RUN_LOOP_ACTIVE_JOBS+=("${performer_pid}|${selected}|${worktree_path}|${tool}")

    note "Dispatched: ${selected}"
    note "  tool:      ${tool}"
    note "  branch:    ${branch}"
    note "  worktree:  ${worktree_path}"
    note "  source:    ${dispatch_kind}"
    note "  mode:      async (pid=${performer_pid})"
    note ""
    emit_event "task_dispatched" "Task dispatched" "$selected" "claimed" "tool=${tool} worktree=${worktree_path}" "dev" "started"

    dispatched=$((dispatched + 1))
  done

  note "Dispatch complete. Tasks dispatched: ${dispatched}"
  emit_event "dispatch_complete" "Dispatch finished" "" "" "dispatched=${dispatched}"
}

dispatch_ready_tasks() {
  local dispatched=0
  local -a jobs=()
  local ready_count
  ready_count="$(jq -r '
    . as $root
    | [($root.tasks // [])[]
        | select((.state // "todo") == "todo" and (.worktree == null))
        | . as $t
        | select((($t.dependencies // []) | all(
            . as $dep
            | (((($root.tasks // []) | map(select(.id == ($dep|tostring)) | .state) | .[0]) // "") == "merged")
          )))
        | select((($t.exclusive_resources // []) | all(
            . as $r
            | (($root.resource_locks[$r].task_id // "") as $owner | ($owner == "" or $owner == $t.id))
          )))
      ]
    | length
  ' "$TASK_REGISTRY_FILE")"
  note "Dispatch config: MAX_DISPATCH=${MAX_DISPATCH} MAX_PARALLEL=${MAX_PARALLEL} MAX_PARALLEL_PER_TOOL_JSON=${MAX_PARALLEL_PER_TOOL_JSON}"
  note "Dispatch ready tasks: ${ready_count}"

  while true; do
    if [[ "$MAX_PARALLEL" -gt 0 && "${#jobs[@]}" -ge "$MAX_PARALLEL" ]]; then
      lock_release
      wait_for_any_job_entry jobs || true
      lock_acquire
      ensure_registry_valid
      consume_runtime_events_once

      if [[ -n "${WAIT_JOB_INDEX:-}" ]]; then
        unset "jobs[$WAIT_JOB_INDEX]"
        jobs=("${jobs[@]}")
        handle_performer_completion "$WAIT_JOB_TASK" "$WAIT_JOB_TOOL" "$WAIT_JOB_RC"
      fi
    fi

    local selected_row selected title tool base_branch
    selected_row="$(select_next_ready_task_tsv)"
    [[ -n "$selected_row" ]] || break
    IFS=$'\t' read -r selected title tool base_branch <<<"$selected_row"
    [[ -n "$selected" ]] || break

    local created task_scope_value dispatch_kind
    dispatch_kind="create"
    task_scope_value="$(task_scope "$selected")"

    if is_truthy "$WORKTREE_POOL_MODE"; then
      created="$(find_idle_compatible_worktree "$tool" "$base_branch" "$task_scope_value" || true)"
      if [[ -n "$created" ]]; then
        dispatch_kind="reuse"
      fi
    fi

    if [[ -z "$created" ]]; then
      if ! created="$(create_macc_worktree "$selected" "$title" "$tool" "$base_branch")"; then
        apply_transition "$selected" "blocked" "" "" "failure:worktree_create"
        note "Blocked task due to worktree create failure: ${selected}"
        continue
      fi
    fi
    local worktree_path branch last_commit
    IFS=$'\t' read -r worktree_path branch last_commit <<<"$created"

    if worktree_in_use "$worktree_path"; then
      local other
      other="$(worktree_task_id "$worktree_path")"
      note "Skip: worktree already in use by task ${other}: ${worktree_path}"
      continue
    fi
    if ! worktree_exists_on_disk "$worktree_path"; then
      echo "Error: worktree path not a git worktree on disk: ${worktree_path}" >&2
      apply_transition "$selected" "blocked" "" "" "failure:worktree_create"
      continue
    fi

    write_worktree_prd "$worktree_path" "$selected"
    mark_task_claimed "$selected" "$worktree_path" "$branch" "$base_branch" "$last_commit" "$tool"

    lock_release
    invoke_performer "$selected" "$worktree_path" "$tool" &
    local performer_pid=$!
    lock_acquire
    ensure_registry_valid
    set_task_runtime "$selected" "running" "dev" "$performer_pid" "" "$(now_iso)"
    jobs+=("${performer_pid}|${selected}|${worktree_path}|${tool}")

    note "Dispatched: ${selected}"
    note "  tool:      ${tool}"
    note "  branch:    ${branch}"
    note "  worktree:  ${worktree_path}"
    note "  source:    ${dispatch_kind}"
    note "  mode:      async (pid=${performer_pid})"
    note ""
    emit_event "task_dispatched" "Task dispatched" "$selected" "claimed" "tool=${tool} worktree=${worktree_path}" "dev" "started"

    dispatched=$((dispatched + 1))
    if [[ "$MAX_DISPATCH" -gt 0 && "$dispatched" -ge "$MAX_DISPATCH" ]]; then
      break
    fi
  done

  while [[ "${#jobs[@]}" -gt 0 ]]; do
    lock_release
    wait_for_any_job_entry jobs || true
    lock_acquire
    ensure_registry_valid
    consume_runtime_events_once

    if [[ -n "${WAIT_JOB_INDEX:-}" ]]; then
      unset "jobs[$WAIT_JOB_INDEX]"
      jobs=("${jobs[@]}")
      handle_performer_completion "$WAIT_JOB_TASK" "$WAIT_JOB_TOOL" "$WAIT_JOB_RC"
    fi
  done

  note "Dispatch complete. Tasks dispatched: ${dispatched}"
  emit_event "dispatch_complete" "Dispatch finished" "" "" "dispatched=${dispatched}"
}
