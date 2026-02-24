# shellcheck shell=bash

emit_event() {
  local event="$1"
  local msg="${2:-}"
  local task_id="${3:-}"
  local state="${4:-}"
  local detail="${5:-}"
  local phase="${6:-}"
  local status="${7:-}"
  local source="${8:-${COORD_EVENT_SOURCE:-coordinator}}"
  local payload_json="${9:-{}}"
  [[ -n "${COORD_EVENTS_FILE:-}" ]] || return 0
  EVENT_SEQ_COUNTER=$((EVENT_SEQ_COUNTER + 1))
  if [[ -z "$status" ]]; then
    status="${state:-${event}}"
  fi
  if ! jq -e 'type == "object"' <<<"$payload_json" >/dev/null 2>&1; then
    payload_json="$(jq -nc --arg detail "$payload_json" '{detail:$detail}')"
  fi
  jq -nc \
    --arg schema_version "1" \
    --arg event_id "${task_id:-global}-${EVENT_SEQ_COUNTER}-$(date +%s%N)" \
    --argjson seq "$EVENT_SEQ_COUNTER" \
    --arg ts "$(now_iso)" \
    --arg event "$event" \
    --arg command "${COORD_COMMAND_NAME:-}" \
    --arg msg "$msg" \
    --arg task_id "$task_id" \
    --arg source "$source" \
    --arg phase "$phase" \
    --arg status "$status" \
    --arg state "$state" \
    --arg detail "$detail" \
    --argjson payload "$payload_json" \
    '{
      schema_version:$schema_version,
      event_id:$event_id,
      seq:$seq,
      ts:$ts,
      source:$source,
      type:$event,
      phase:($phase|select(length>0)),
      status:$status,
      payload:$payload,
      event:$event,
      command:$command,
      msg:$msg,
      task_id:($task_id|select(length>0)),
      state:($state|select(length>0)),
      detail:($detail|select(length>0))
    }' >>"$COORD_EVENTS_FILE" 2>/dev/null || true
}

event_file_inode() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "0"
    return 0
  fi
  stat -c '%i' "$path" 2>/dev/null \
    || stat -f '%i' "$path" 2>/dev/null \
    || echo "0"
}

read_event_cursor() {
  if [[ -z "${COORD_CURSOR_FILE:-}" || ! -f "${COORD_CURSOR_FILE:-}" ]]; then
    echo "0|0||"
    return 0
  fi
  local row
  row="$(jq -r '[.offset // 0, .inode // 0, .path // "", .last_event_id // ""] | @tsv' "$COORD_CURSOR_FILE" 2>/dev/null || true)"
  if [[ -z "$row" ]]; then
    echo "0|0||"
    return 0
  fi
  local offset inode path last_event_id
  IFS=$'\t' read -r offset inode path last_event_id <<<"$row"
  [[ "$offset" =~ ^[0-9]+$ ]] || offset=0
  [[ "$inode" =~ ^[0-9]+$ ]] || inode=0
  echo "${offset}|${inode}|${path}|${last_event_id}"
}

write_event_cursor() {
  local offset="$1"
  local inode="$2"
  local last_event_id="${3:-}"
  [[ -n "${COORD_CURSOR_FILE:-}" ]] || return 0
  mkdir -p "$(dirname "$COORD_CURSOR_FILE")"
  jq -nc \
    --arg path "$COORD_EVENTS_FILE" \
    --arg updated_at "$(now_iso)" \
    --argjson offset "$offset" \
    --argjson inode "$inode" \
    --arg last_event_id "$last_event_id" \
    '{
      path:$path,
      inode:$inode,
      offset:$offset,
      last_event_id:($last_event_id|select(length>0)),
      updated_at:$updated_at
    }' >"$COORD_CURSOR_FILE"
}

rotate_events_log_if_needed() {
  [[ -n "${COORD_EVENTS_FILE:-}" ]] || return 0
  [[ -f "$COORD_EVENTS_FILE" ]] || return 0
  [[ "$EVENT_LOG_MAX_BYTES" =~ ^[0-9]+$ ]] || return 0
  if [[ "$EVENT_LOG_MAX_BYTES" -le 0 ]]; then
    return 0
  fi
  local size
  size="$(wc -c <"$COORD_EVENTS_FILE" 2>/dev/null || echo 0)"
  [[ "$size" =~ ^[0-9]+$ ]] || size=0
  if [[ "$size" -lt "$EVENT_LOG_MAX_BYTES" ]]; then
    return 0
  fi

  local ts rotated
  ts="$(date -u +"%Y%m%dT%H%M%SZ")"
  rotated="${COORD_LOG_DIR}/events-${ts}.jsonl"
  mv "$COORD_EVENTS_FILE" "$rotated"
  : >"$COORD_EVENTS_FILE"
  emit_event "events_rotated" "Rotated coordinator events log" "" "" "from_size=${size} path=${rotated}"

  if [[ "$EVENT_LOG_KEEP_FILES" =~ ^[0-9]+$ ]] && [[ "$EVENT_LOG_KEEP_FILES" -ge 0 ]]; then
    mapfile -t rotated_files < <(ls -1t "${COORD_LOG_DIR}"/events-*.jsonl 2>/dev/null || true)
    local idx
    for idx in "${!rotated_files[@]}"; do
      if [[ "$idx" -ge "$EVENT_LOG_KEEP_FILES" ]]; then
        rm -f "${rotated_files[$idx]}"
      fi
    done
  fi
}

compact_processed_event_ids_if_needed() {
  [[ "$PROCESSED_EVENT_IDS_MAX" =~ ^[0-9]+$ ]] || return 0
  if [[ "$PROCESSED_EVENT_IDS_MAX" -le 0 ]]; then
    return 0
  fi
  local count
  count="$(jq -r '(.processed_event_ids // {}) | length' "$TASK_REGISTRY_FILE" 2>/dev/null || echo 0)"
  [[ "$count" =~ ^[0-9]+$ ]] || count=0
  if [[ "$count" -le "$PROCESSED_EVENT_IDS_MAX" ]]; then
    return 0
  fi
  local tmp now
  now="$(now_iso)"
  tmp="$(mktemp)"
  jq --arg now "$now" '
    . as $root
    | (.tasks // [])
      | map(.task_runtime.last_event_id // "")
      | map(select(length > 0))
      | unique as $keep
    | .processed_event_ids = (
        reduce $keep[] as $id ({};
          if (($root.processed_event_ids // {})[$id] // false) then
            .[$id] = true
          else
            .
          end
        )
      )
    | .updated_at = $now
  ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
  emit_event "events_compacted" "Compacted processed event dedup map" "" "" "previous_count=${count}"
}

apply_runtime_event() {
  local event_id="$1"
  local task_id="$2"
  local seq="$3"
  local event_type="$4"
  local phase="$5"
  local status="$6"
  local ts="$7"
  local payload_json="$8"
  local source="$9"

  [[ -n "$task_id" ]] || return 0
  task_exists "$task_id" || return 0

  local runtime_status current_runtime_status
  runtime_status="$(runtime_status_from_event "$event_type" "$status")"
  current_runtime_status="$(task_field "$task_id" '.task_runtime.status // "idle"' 2>/dev/null || true)"
  if [[ -z "$current_runtime_status" || "$current_runtime_status" == "null" ]]; then
    current_runtime_status="idle"
  fi
  if ! validate_runtime_transition "$current_runtime_status" "$runtime_status"; then
    runtime_status="$current_runtime_status"
  fi

  local tmp now
  now="$(now_iso)"
  tmp="$(mktemp)"
  jq --arg task_id "$task_id" \
     --arg event_id "$event_id" \
     --argjson seq "$seq" \
     --arg runtime_status "$runtime_status" \
     --arg phase "$phase" \
     --arg event_type "$event_type" \
     --arg status "$status" \
     --arg ts "$ts" \
    --argjson payload "$payload_json" \
     --arg source "$source" \
     --arg now "$now" \
     '
     def payload_obj:
       if ($payload | type) == "object" then
         if (($payload.value? // null) | type) == "string" then
           (($payload.value | fromjson?) // $payload)
         else
           $payload
         end
       elif ($payload | type) == "string" then
         (($payload | fromjson?) // {})
       else
         {}
       end;

     def payload_error_message:
       (payload_obj.message // payload_obj.reason // payload_obj.error // payload_obj.matched_output // "");

     def payload_error_code:
       (payload_obj.error_code // payload_obj.code // "");

     def payload_error_origin:
       (payload_obj.origin // "");

     def payload_attempt:
       (payload_obj.attempt // null);

     def heartbeat_ts:
       if ($status == "running" or $status == "heartbeat" or $event_type == "heartbeat") then
         (if ($ts|length) > 0 then $ts else $now end)
       else
         null
       end;

     . as $root
     | (.processed_event_ids // {}) as $seen
     | if ($seen[$event_id] // false) then
         .
       else
         .processed_event_ids = $seen
         | .processed_event_ids[$event_id] = true
         | .tasks |= map(
             if .id == $task_id then
               .task_runtime = (.task_runtime // {})
               | (if (
                    (.task_runtime.last_seq_source // "") == $source
                    and $seq > 0
                    and ((.task_runtime.last_seq // -1) >= 0)
                    and ((.task_runtime.last_seq // -1) >= $seq)
                  ) then
                    .
                  else
                    .task_runtime.last_seq = $seq
                    | .task_runtime.last_event_id = $event_id
                    | .task_runtime.last_seq_source = $source
                    | (if $event_type == "commit_created" then
                         .task_runtime.last_commit_created_at = (if ($ts|length) > 0 then $ts else $now end)
                       else
                         .
                       end)
                    | .task_runtime.status = $runtime_status
                    | (if ($phase|length) > 0 then .task_runtime.current_phase = $phase else . end)
                    | (if heartbeat_ts != null then .task_runtime.last_heartbeat = heartbeat_ts else . end)
                    | (if payload_attempt != null then .task_runtime.attempt = payload_attempt else . end)
                    | .task_runtime.metrics = (.task_runtime.metrics // {})
                    | .task_runtime.metrics.retries = (.task_runtime.metrics.retries // (.task_runtime.retries // 0))
                    | (if payload_attempt != null and payload_attempt > 1 then
                         .task_runtime.metrics.retries = (
                           if (.task_runtime.metrics.retries // 0) > (payload_attempt - 1) then
                             (.task_runtime.metrics.retries // 0)
                           else
                             (payload_attempt - 1)
                           end
                         )
                       else
                         .
                       end)
                    | .task_runtime.retries = (.task_runtime.metrics.retries // 0)
                    | (if ($runtime_status == "failed" and (payload_error_message|length) > 0) then
                         .task_runtime.last_error = payload_error_message
                       else
                         .
                       end)
                    | (if ($runtime_status == "failed" and (payload_error_code|length) > 0) then
                         .task_runtime.last_error_code = payload_error_code
                       else
                         .
                       end)
                    | (if ($runtime_status == "failed" and (payload_error_origin|length) > 0) then
                         .task_runtime.last_error_origin = payload_error_origin
                       else
                         .
                       end)
                    | (if ($runtime_status == "failed" and (payload_error_message|length) > 0) then
                         .task_runtime.last_error_message = payload_error_message
                       else
                         .
                       end)
                    | (if ($runtime_status == "running" and ((.task_runtime.started_at // "")|length) == 0) then
                         .task_runtime.started_at = (if ($ts|length) > 0 then $ts else $now end)
                       else
                         .
                       end)
                  end)
             else
               .
             end
           )
         | .updated_at = $now
       end
     ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
}

consume_runtime_events_once() {
  [[ -n "${COORD_EVENTS_FILE:-}" ]] || return 0
  rotate_events_log_if_needed
  [[ -f "$COORD_EVENTS_FILE" ]] || return 0

  local cursor_meta cursor_offset cursor_inode cursor_path cursor_last_event_id
  cursor_meta="$(read_event_cursor)"
  IFS='|' read -r cursor_offset cursor_inode cursor_path cursor_last_event_id <<<"$cursor_meta"

  local file_size file_inode
  file_size="$(wc -c <"$COORD_EVENTS_FILE" 2>/dev/null || echo 0)"
  file_inode="$(event_file_inode "$COORD_EVENTS_FILE")"
  [[ "$cursor_offset" =~ ^[0-9]+$ ]] || cursor_offset=0
  [[ "$cursor_inode" =~ ^[0-9]+$ ]] || cursor_inode=0
  [[ "$file_size" =~ ^[0-9]+$ ]] || file_size=0
  [[ "$file_inode" =~ ^[0-9]+$ ]] || file_inode=0

  if [[ "$cursor_path" != "$COORD_EVENTS_FILE" || "$cursor_inode" != "$file_inode" || "$cursor_offset" -gt "$file_size" ]]; then
    cursor_offset=0
  fi
  if [[ "$cursor_offset" -eq "$file_size" ]]; then
    return 0
  fi

  local lines
  lines="$(tail -c +"$((cursor_offset + 1))" "$COORD_EVENTS_FILE" 2>/dev/null || true)"
  local last_processed_event_id=""
  while IFS= read -r line; do
    [[ -n "${line// }" ]] || continue
    local event_id task_id seq event_type phase status ts payload_json source
    event_id="$(jq -r '.event_id // ""' <<<"$line" 2>/dev/null || true)"
    task_id="$(jq -r '.task_id // ""' <<<"$line" 2>/dev/null || true)"
    seq="$(jq -r '.seq // 0' <<<"$line" 2>/dev/null || echo 0)"
    event_type="$(jq -r '.type // .event // ""' <<<"$line" 2>/dev/null || true)"
    phase="$(jq -r '.phase // ""' <<<"$line" 2>/dev/null || true)"
    status="$(jq -r '.status // .state // ""' <<<"$line" 2>/dev/null || true)"
    ts="$(jq -r '.ts // ""' <<<"$line" 2>/dev/null || true)"
    source="$(jq -r '.source // ""' <<<"$line" 2>/dev/null || true)"
    payload_json="$(jq -c '.payload // {}' <<<"$line" 2>/dev/null || echo '{}')"

    [[ "$seq" =~ ^[0-9]+$ ]] || seq=0
    [[ -n "$event_id" ]] || continue
    case "$event_type" in
      started|progress|phase_result|commit_created|review_done|integrate_done|failed|heartbeat) ;;
      *) continue ;;
    esac
    apply_runtime_event "$event_id" "$task_id" "$seq" "$event_type" "$phase" "$status" "$ts" "$payload_json" "$source"
    last_processed_event_id="$event_id"

    if [[ "$status" == "failed" && "$source" == performer:* ]]; then
      local current
      current="$(task_state "$task_id" 2>/dev/null || true)"
      if [[ -n "$current" && "$current" != "blocked" && "$current" != "merged" && "$current" != "abandoned" ]]; then
        apply_transition "$task_id" "blocked" "" "" "failure:performer_event"
      fi
    fi
  done <<<"$lines"

  write_event_cursor "$file_size" "$file_inode" "$last_processed_event_id"
  compact_processed_event_ids_if_needed
}
