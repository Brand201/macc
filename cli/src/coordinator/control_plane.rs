use crate::{
    append_coordinator_event, build_non_task_worker_slug, build_phase_prompt_native,
    count_pool_worktrees, ensure_tool_json, find_reusable_worktree_native, now_iso_coordinator,
    recompute_resource_locks_from_tasks, resolve_phase_runner_native, set_registry_updated_at,
    spawn_merge_job_native, spawn_performer_job_native, summarize_output,
    write_worktree_prd_for_task, CoordinatorEnvConfig, CoordinatorMergeJob, CoordinatorRunState,
    NativeCoordinatorLogger,
};
use macc_core::coordinator::{engine as coordinator_engine, runtime as coordinator_runtime};
use macc_core::{MaccError, Result};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

pub fn sync_registry_from_prd_native(
    repo_root: &Path,
    prd_file: &Path,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<()> {
    let mut registry =
        crate::coordinator::state::coordinator_state_registry_load(repo_root, &BTreeMap::new())?;
    let raw_prd = std::fs::read_to_string(prd_file).map_err(|e| MaccError::Io {
        path: prd_file.to_string_lossy().into(),
        action: "read coordinator prd".into(),
        source: e,
    })?;
    let prd: serde_json::Value = serde_json::from_str(&raw_prd).map_err(|e| {
        MaccError::Validation(format!("Failed to parse PRD {}: {}", prd_file.display(), e))
    })?;
    let prd_tasks = prd
        .get("tasks")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();

    if !registry
        .get("tasks")
        .map(serde_json::Value::is_array)
        .unwrap_or(false)
    {
        registry["tasks"] = serde_json::Value::Array(Vec::new());
    }

    let existing_tasks = registry["tasks"].as_array().cloned().unwrap_or_default();
    let mut by_id: HashMap<String, serde_json::Value> = HashMap::new();
    for task in existing_tasks {
        if let Some(id) = task
            .get("id")
            .and_then(serde_json::Value::as_str)
            .map(|s| s.to_string())
        {
            by_id.insert(id, task);
        }
    }

    let mut merged = Vec::new();
    for prd_task in prd_tasks {
        let id = if let Some(v) = prd_task.get("id").and_then(serde_json::Value::as_str) {
            v.to_string()
        } else if let Some(v) = prd_task.get("id").and_then(serde_json::Value::as_i64) {
            v.to_string()
        } else {
            String::new()
        };
        if id.is_empty() {
            continue;
        }
        let mut task = by_id.remove(&id).unwrap_or_else(|| {
            serde_json::json!({
                "id": id,
                "state": "todo",
                "dependencies": [],
                "exclusive_resources": [],
                "task_runtime": {
                    "status": "idle",
                    "pid": null,
                    "current_phase": null,
                    "merge_result_pending": false,
                    "merge_result_file": null
                }
            })
        });

        for key in [
            "title",
            "description",
            "objective",
            "result",
            "steps",
            "notes",
            "category",
            "priority",
            "dependencies",
            "exclusive_resources",
            "base_branch",
            "scope",
        ] {
            if let Some(v) = prd_task.get(key) {
                task[key] = v.clone();
            }
        }
        coordinator_engine::ensure_runtime_object(&mut task);
        task["updated_at"] = serde_json::Value::String(now_iso_coordinator());
        merged.push(task);
    }

    registry["tasks"] = serde_json::Value::Array(merged);
    recompute_resource_locks_from_tasks(&mut registry);
    set_registry_updated_at(&mut registry);
    crate::coordinator::state::coordinator_state_registry_save(
        repo_root,
        &BTreeMap::new(),
        &registry,
    )?;
    if let Some(log) = logger {
        let count = registry
            .get("tasks")
            .and_then(serde_json::Value::as_array)
            .map(|v| v.len())
            .unwrap_or(0);
        let _ = log.note(format!("Registry synced from PRD (tasks={})", count));
    }
    Ok(())
}

struct NativePhaseExecutor<'a> {
    repo_root: &'a Path,
    logger: Option<&'a NativeCoordinatorLogger>,
}

impl coordinator_runtime::PhaseExecutor for NativePhaseExecutor<'_> {
    fn run_phase(
        &self,
        task: &serde_json::Value,
        mode: &str,
        coordinator_tool_override: Option<&str>,
        max_attempts: usize,
    ) -> Result<std::result::Result<String, String>> {
        let task_id = task
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let worktree_path = task
            .get("worktree")
            .and_then(|w| w.get("worktree_path"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if task_id.is_empty() || worktree_path.is_empty() {
            return Ok(Err(format!(
                "phase '{}' cannot run: missing task id or worktree path",
                mode
            )));
        }
        let phase_tool = coordinator_tool_override
            .filter(|v| !v.trim().is_empty())
            .or_else(|| {
                task.get("coordinator_tool")
                    .and_then(serde_json::Value::as_str)
                    .filter(|v| !v.trim().is_empty())
            })
            .or_else(|| {
                task.get("tool")
                    .and_then(serde_json::Value::as_str)
                    .filter(|v| !v.trim().is_empty())
            })
            .unwrap_or_default()
            .to_string();
        if phase_tool.is_empty() {
            return Ok(Err(format!(
                "phase '{}' cannot run for task {}: missing coordinator tool",
                mode, task_id
            )));
        }
        let worktree = std::path::PathBuf::from(worktree_path);
        let tool_json = worktree.join(".macc").join("tool.json");
        if !tool_json.exists() {
            return Ok(Err(format!(
                "phase '{}' cannot run for task {}: missing {}",
                mode,
                task_id,
                tool_json.display()
            )));
        }
        let Some(runner_path) =
            resolve_phase_runner_native(self.repo_root, &worktree, &phase_tool)?
        else {
            return Ok(Err(format!(
                "phase '{}' cannot run for task {}: missing runner for tool '{}'",
                mode, task_id, phase_tool
            )));
        };
        if !runner_path.exists() {
            return Ok(Err(format!(
                "phase '{}' cannot run for task {}: runner path not found {}",
                mode,
                task_id,
                runner_path.display()
            )));
        }
        let prompt = build_phase_prompt_native(mode, task_id, &phase_tool, task)?;
        let prompt_dir = worktree.join(".macc").join("tmp");
        std::fs::create_dir_all(&prompt_dir).map_err(|e| MaccError::Io {
            path: prompt_dir.to_string_lossy().into(),
            action: "create coordinator phase prompt directory".into(),
            source: e,
        })?;
        let prompt_path = prompt_dir.join(format!(
            "coordinator-phase-{}-{}.prompt.txt",
            mode,
            task_id.replace('/', "-")
        ));
        std::fs::write(&prompt_path, prompt).map_err(|e| MaccError::Io {
            path: prompt_path.to_string_lossy().into(),
            action: "write coordinator phase prompt".into(),
            source: e,
        })?;
        let events_file = self
            .repo_root
            .join(".macc")
            .join("log")
            .join("coordinator")
            .join("events.jsonl");
        let attempts = max_attempts.max(1);
        if let Some(log) = self.logger {
            let _ = log.note(format!(
                "- Phase {} start task={} tool={} attempts={}",
                mode, task_id, phase_tool, attempts
            ));
        }
        let mut last_reason = String::new();
        for attempt in 1..=attempts {
            let output = std::process::Command::new(&runner_path)
                .current_dir(&worktree)
                .env(
                    "COORD_EVENTS_FILE",
                    events_file.to_string_lossy().to_string(),
                )
                .env(
                    "MACC_EVENT_SOURCE",
                    format!(
                        "coordinator-phase:{}:{}:{}:{}",
                        mode,
                        phase_tool,
                        task_id,
                        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
                    ),
                )
                .env("MACC_EVENT_TASK_ID", task_id)
                .arg("--prompt-file")
                .arg(&prompt_path)
                .arg("--tool-json")
                .arg(&tool_json)
                .arg("--repo")
                .arg(self.repo_root)
                .arg("--worktree")
                .arg(&worktree)
                .arg("--task-id")
                .arg(task_id)
                .arg("--attempt")
                .arg(attempt.to_string())
                .arg("--max-attempts")
                .arg(attempts.to_string())
                .output();
            let Ok(out) = output else {
                last_reason = format!(
                    "phase '{}' failed to execute runner '{}'",
                    mode,
                    runner_path.display()
                );
                continue;
            };
            let combined_output = format!(
                "{}\n{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            if out.status.success() {
                let _ = std::fs::remove_file(&prompt_path);
                if let Some(log) = self.logger {
                    let _ = log.note(format!(
                        "- Phase {} done task={} attempt={}",
                        mode, task_id, attempt
                    ));
                }
                return Ok(Ok(combined_output));
            }
            last_reason = format!(
                "phase '{}' failed for task {} on attempt {}/{}: status={} stdout=\"{}\" stderr=\"{}\"",
                mode,
                task_id,
                attempt,
                attempts,
                out.status,
                summarize_output(&String::from_utf8_lossy(&out.stdout)),
                summarize_output(&String::from_utf8_lossy(&out.stderr))
            );
        }
        let _ = std::fs::remove_file(&prompt_path);
        if let Some(log) = self.logger {
            let _ = log.note(format!(
                "- Phase {} failed task={} reason={}",
                mode, task_id, last_reason
            ));
        }
        Ok(Err(last_reason))
    }
}

pub fn run_phase_for_task_native(
    repo_root: &Path,
    task: &serde_json::Value,
    mode: &str,
    coordinator_tool_override: Option<&str>,
    max_attempts: usize,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<std::result::Result<String, String>> {
    let executor = NativePhaseExecutor { repo_root, logger };
    coordinator_runtime::run_phase(&executor, task, mode, coordinator_tool_override, max_attempts)
}

pub fn run_review_phase_for_task_native(
    repo_root: &Path,
    task: &serde_json::Value,
    coordinator_tool_override: Option<&str>,
    max_attempts: usize,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<std::result::Result<coordinator_engine::ReviewVerdict, String>> {
    let executor = NativePhaseExecutor { repo_root, logger };
    coordinator_runtime::run_review_phase(&executor, task, coordinator_tool_override, max_attempts)
}

pub async fn advance_tasks_native(
    repo_root: &Path,
    coordinator_tool_override: Option<&str>,
    phase_runner_max_attempts: usize,
    state: &mut CoordinatorRunState,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<coordinator_engine::AdvanceResult> {
    let mut registry =
        crate::coordinator::state::coordinator_state_registry_load(repo_root, &BTreeMap::new())?;
    let mut progressed = false;
    let blocked_merge: Option<(String, String)> = None;
    let now = now_iso_coordinator();
    let active_merge_ids = state
        .active_merge_jobs
        .keys()
        .cloned()
        .collect::<HashSet<_>>();
    let actions = coordinator_engine::build_advance_actions(&registry, &active_merge_ids)?;
    for action in actions {
        match action {
            coordinator_engine::AdvanceTaskAction::RunPhase {
                task_id,
                mode,
                transition,
            } => {
                let task_snapshot = registry
                    .get("tasks")
                    .and_then(serde_json::Value::as_array)
                    .and_then(|tasks| {
                        tasks.iter().find(|t| {
                            t.get("id")
                                .and_then(serde_json::Value::as_str)
                                .map(|id| id == task_id)
                                .unwrap_or(false)
                        })
                    })
                    .cloned()
                    .ok_or_else(|| {
                        MaccError::Validation(format!(
                            "Task '{}' not found while advancing phase",
                            task_id
                        ))
                    })?;
                let executor = NativePhaseExecutor { repo_root, logger };
                if mode == "review" {
                    match coordinator_runtime::run_review_phase(
                        &executor,
                        &task_snapshot,
                        coordinator_tool_override,
                        phase_runner_max_attempts,
                    )? {
                        Ok(verdict) => {
                            let verdict_status = match verdict {
                                coordinator_engine::ReviewVerdict::Ok => "ok",
                                coordinator_engine::ReviewVerdict::ChangesRequested => {
                                    "changes_requested"
                                }
                            };
                            append_coordinator_event(
                                repo_root,
                                "review_done",
                                &task_id,
                                "review",
                                verdict_status,
                                &format!("Review verdict for task {}: {}", task_id, verdict_status),
                            )?;
                            coordinator_engine::apply_phase_outcome_in_registry(
                                &mut registry,
                                &task_id,
                                mode,
                                transition,
                                Some(verdict),
                                None,
                                &now,
                            )?
                        }
                        Err(reason) => coordinator_engine::apply_phase_outcome_in_registry(
                            &mut registry,
                            &task_id,
                            mode,
                            transition,
                            None,
                            Some(&reason),
                            &now,
                        )?,
                    }
                } else {
                    match coordinator_runtime::run_phase(
                        &executor,
                        &task_snapshot,
                        mode,
                        coordinator_tool_override,
                        phase_runner_max_attempts,
                    )? {
                        Ok(_) => coordinator_engine::apply_phase_outcome_in_registry(
                            &mut registry,
                            &task_id,
                            mode,
                            transition,
                            None,
                            None,
                            &now,
                        )?,
                        Err(reason) => coordinator_engine::apply_phase_outcome_in_registry(
                            &mut registry,
                            &task_id,
                            mode,
                            transition,
                            None,
                            Some(&reason),
                            &now,
                        )?,
                    }
                }
                progressed = true;
            }
            coordinator_engine::AdvanceTaskAction::QueueMerge {
                task_id,
                branch,
                base,
            } => {
                if let Some(log) = logger {
                    let _ = log.note(format!(
                        "- Merge start task={} branch={} base={}",
                        task_id, branch, base
                    ));
                }
                spawn_merge_job_native(
                    repo_root,
                    &task_id,
                    &branch,
                    &base,
                    &state.merge_event_tx,
                    &mut state.merge_join_set,
                )
                .await?;
                state.active_merge_jobs.insert(
                    task_id.clone(),
                    CoordinatorMergeJob {
                        started_at: std::time::Instant::now(),
                    },
                );
                if let Some(log) = logger {
                    let _ = log.note(format!("- Merge queued task={}", task_id));
                }
                progressed = true;
            }
        }
    }
    recompute_resource_locks_from_tasks(&mut registry);
    set_registry_updated_at(&mut registry);
    crate::coordinator::state::coordinator_state_registry_save(
        repo_root,
        &BTreeMap::new(),
        &registry,
    )?;
    Ok(coordinator_engine::AdvanceResult {
        progressed,
        blocked_merge,
    })
}

pub async fn monitor_active_jobs_native(
    repo_root: &Path,
    env_cfg: &CoordinatorEnvConfig,
    state: &mut CoordinatorRunState,
    max_attempts: usize,
    phase_timeout_seconds: usize,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<()> {
    consume_heartbeat_events(repo_root, state, logger)?;
    apply_stale_heartbeat_policy(repo_root, env_cfg, logger)?;
    let retry_codes = resolve_error_code_retry_list(env_cfg);
    let retry_max = resolve_error_code_retry_max(env_cfg);
    loop {
        match state.event_rx.try_recv() {
            Ok(evt) => {
                let maybe_job = state.active_jobs.remove(&evt.task_id);
                let Some(job) = maybe_job else {
                    continue;
                };
                let mut registry = crate::coordinator::state::coordinator_state_registry_load(
                    repo_root,
                    &BTreeMap::new(),
                )?;
                let completion = coordinator_engine::apply_job_completion_in_registry(
                    &mut registry,
                    &evt.task_id,
                    &coordinator_engine::JobCompletionInput {
                        success: evt.success,
                        attempt: job.attempt,
                        max_attempts: max_attempts.max(1),
                        timed_out: evt.timed_out,
                        phase_timeout_seconds,
                        elapsed_seconds: job.started_at.elapsed().as_secs(),
                        status_text: evt.status_text.clone(),
                        error_code: evt.error_code.clone(),
                        error_origin: evt.error_origin.clone(),
                        error_message: evt.error_message.clone(),
                        auto_retry_error_codes: retry_codes.clone(),
                        auto_retry_max: retry_max,
                    },
                    &now_iso_coordinator(),
                )?;
                recompute_resource_locks_from_tasks(&mut registry);
                set_registry_updated_at(&mut registry);
                crate::coordinator::state::coordinator_state_registry_save(
                    repo_root,
                    &BTreeMap::new(),
                    &registry,
                )?;
                if !completion.should_retry && completion.status_label == "phase_done" {
                    let sealed =
                        macc_core::coordinator::session_manager::seal_worktree_scoped_session(
                            repo_root,
                            &job.tool,
                            &job.worktree_path,
                            &evt.task_id,
                            &now_iso_coordinator(),
                        )?;
                    if sealed.sealed {
                        if let Some(log) = logger {
                            let sid = sealed.session_id.as_deref().unwrap_or("unknown");
                            let _ = log.note(format!(
                                "- Session sealed task={} tool={} session_id={}",
                                evt.task_id, job.tool, sid
                            ));
                        }
                    }
                }
                if completion.status_label == "auto_retry" {
                    if let Some(log) = logger {
                        let _ = log.note(format!(
                            "- Task {} auto-retry queued detail={}",
                            evt.task_id, completion.detail
                        ));
                    }
                } else if completion.should_retry {
                    let task_id = evt.task_id.clone();
                    let retry_pid = spawn_performer_job_native(
                        repo_root,
                        &task_id,
                        &job.worktree_path,
                        &state.event_tx,
                        &mut state.join_set,
                        phase_timeout_seconds,
                    )?;
                    state.active_jobs.insert(
                        task_id,
                        crate::CoordinatorJob {
                            tool: job.tool,
                            worktree_path: job.worktree_path,
                            attempt: job.attempt + 1,
                            started_at: std::time::Instant::now(),
                            pid: retry_pid,
                        },
                    );
                    if let Some(log) = logger {
                        let _ = log.note(format!(
                            "- Task {} retry scheduled attempt={}",
                            evt.task_id,
                            job.attempt + 1
                        ));
                    }
                } else if let Some(log) = logger {
                    let status = if evt.success { "phase_done" } else { "failed" };
                    let _ = log.note(format!(
                        "- Task {} completion status={} attempt={} detail={}",
                        evt.task_id, status, job.attempt, evt.status_text
                    ));
                }
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
    while let Some(joined) = state.join_set.try_join_next() {
        let _ = joined;
    }
    Ok(())
}

fn consume_heartbeat_events(
    repo_root: &Path,
    state: &mut CoordinatorRunState,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<usize> {
    let events_file = repo_root
        .join(".macc")
        .join("log")
        .join("coordinator")
        .join("events.jsonl");
    if !events_file.exists() {
        return Ok(0);
    }
    let mut file = File::open(&events_file).map_err(|e| MaccError::Io {
        path: events_file.to_string_lossy().into(),
        action: "open coordinator events for heartbeat scan".into(),
        source: e,
    })?;
    let len = file.metadata().map_err(|e| MaccError::Io {
        path: events_file.to_string_lossy().into(),
        action: "read coordinator events metadata".into(),
        source: e,
    })?.len();
    if len < state.events_cursor_offset {
        state.events_cursor_offset = 0;
    }
    file.seek(SeekFrom::Start(state.events_cursor_offset))
        .map_err(|e| MaccError::Io {
            path: events_file.to_string_lossy().into(),
            action: "seek coordinator events file".into(),
            source: e,
        })?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).map_err(|e| MaccError::Io {
        path: events_file.to_string_lossy().into(),
        action: "read coordinator events file".into(),
        source: e,
    })?;
    state.events_cursor_offset = len;
    if buf.is_empty() {
        return Ok(0);
    }

    let mut heartbeat_updates: HashMap<String, String> = HashMap::new();
    for line in buf.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(event) = serde_json::from_str::<serde_json::Value>(trimmed) else {
            continue;
        };
        let event_type = event
            .get("type")
            .or_else(|| event.get("event"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if event_type != "heartbeat" {
            continue;
        }
        let task_id = event
            .get("task_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let ts = event
            .get("ts")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if task_id.is_empty() || ts.is_empty() {
            continue;
        }
        heartbeat_updates.insert(task_id.to_string(), ts.to_string());
    }
    if heartbeat_updates.is_empty() {
        return Ok(0);
    }

    let mut registry =
        crate::coordinator::state::coordinator_state_registry_load(repo_root, &BTreeMap::new())?;
    let mut updated = 0usize;
    if let Some(tasks) = registry.get_mut("tasks").and_then(serde_json::Value::as_array_mut) {
        for task in tasks {
            let id = task
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let Some(ts) = heartbeat_updates.get(id) else {
                continue;
            };
            coordinator_engine::ensure_runtime_object(task);
            task["task_runtime"]["last_heartbeat"] = serde_json::Value::String(ts.clone());
            updated += 1;
        }
    }
    if updated > 0 {
        set_registry_updated_at(&mut registry);
        crate::coordinator::state::coordinator_state_registry_save(
            repo_root,
            &BTreeMap::new(),
            &registry,
        )?;
        if let Some(log) = logger {
            state.heartbeat_updates_since_log += updated;
            let should_log = state
                .last_heartbeat_log_at
                .map(|last| last.elapsed() >= std::time::Duration::from_secs(30))
                .unwrap_or(true);
            if should_log {
                let _ = log.note(format!(
                    "- Heartbeat updates applied count={} (30s window)",
                    state.heartbeat_updates_since_log
                ));
                state.last_heartbeat_log_at = Some(std::time::Instant::now());
                state.heartbeat_updates_since_log = 0;
            }
        }
    }
    Ok(updated)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaleHeartbeatAction {
    Retry,
    Block,
    Requeue,
}

fn apply_stale_heartbeat_policy(
    repo_root: &Path,
    env_cfg: &CoordinatorEnvConfig,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<usize> {
    let stale_seconds = resolve_stale_heartbeat_seconds(env_cfg);
    if stale_seconds == 0 {
        return Ok(0);
    }
    let action = resolve_stale_heartbeat_action(env_cfg, logger);
    let now = chrono::Utc::now();
    let now_ts = now.timestamp();
    let now_iso = now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let mut registry =
        crate::coordinator::state::coordinator_state_registry_load(repo_root, &BTreeMap::new())?;
    let Some(tasks) = registry.get_mut("tasks").and_then(serde_json::Value::as_array_mut) else {
        return Ok(0);
    };

    let mut stale_ids = Vec::new();
    for task in tasks.iter_mut() {
        coordinator_engine::ensure_runtime_object(task);
        let status = task["task_runtime"]["status"]
            .as_str()
            .unwrap_or_default();
        if status != "running" {
            continue;
        }
        let phase = task["task_runtime"]["current_phase"]
            .as_str()
            .unwrap_or("dev")
            .to_string();
        let last_ts = task["task_runtime"]["last_heartbeat"]
            .as_str()
            .filter(|v| !v.is_empty())
            .or_else(|| {
                task["task_runtime"]["started_at"]
                    .as_str()
                    .filter(|v| !v.is_empty())
            })
            .or_else(|| task.get("updated_at").and_then(serde_json::Value::as_str));
        let Some(last_ts) = last_ts else {
            continue;
        };
        let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(last_ts) else {
            continue;
        };
        let age = now_ts.saturating_sub(parsed.timestamp());
        if age <= stale_seconds as i64 {
            continue;
        }

        let task_id = task
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        if task_id.is_empty() {
            continue;
        }

        let detail = format!(
            "stale heartbeat: last={} age={}s threshold={}s action={}",
            last_ts,
            age,
            stale_seconds,
            match action {
                StaleHeartbeatAction::Retry => "retry",
                StaleHeartbeatAction::Block => "block",
                StaleHeartbeatAction::Requeue => "requeue",
            }
        );

        match action {
            StaleHeartbeatAction::Block => {
                task["task_runtime"]["status"] =
                    serde_json::Value::String("stale".to_string());
                task["task_runtime"]["pid"] = serde_json::Value::Null;
                task["task_runtime"]["last_error"] =
                    serde_json::Value::String(detail.clone());
                task["state"] = serde_json::Value::String("blocked".to_string());
            }
            StaleHeartbeatAction::Requeue => {
                crate::coordinator::state::reset_runtime_to_idle(task);
                task["task_runtime"]["last_error"] =
                    serde_json::Value::String(detail.clone());
                task["state"] = serde_json::Value::String("todo".to_string());
                task["assignee"] = serde_json::Value::Null;
                task["claimed_at"] = serde_json::Value::Null;
                task["worktree"] = serde_json::Value::Null;
            }
            StaleHeartbeatAction::Retry => {
                increment_runtime_retries(task);
                crate::coordinator::state::reset_runtime_to_idle(task);
                task["task_runtime"]["last_error"] =
                    serde_json::Value::String(detail.clone());
                task["state"] = serde_json::Value::String("todo".to_string());
                task["assignee"] = serde_json::Value::Null;
                task["claimed_at"] = serde_json::Value::Null;
                task["worktree"] = serde_json::Value::Null;
            }
        }

        task["updated_at"] = serde_json::Value::String(now_iso.clone());
        task["state_changed_at"] = serde_json::Value::String(now_iso.clone());
        stale_ids.push((task_id, phase));
    }

    if stale_ids.is_empty() {
        return Ok(0);
    }

    recompute_resource_locks_from_tasks(&mut registry);
    set_registry_updated_at(&mut registry);
    crate::coordinator::state::coordinator_state_registry_save(
        repo_root,
        &BTreeMap::new(),
        &registry,
    )?;

    for (task_id, phase) in &stale_ids {
        let _ = append_coordinator_event(
            repo_root,
            "task_runtime_stale",
            task_id,
            phase,
            "stale",
            "stale heartbeat detected",
        );
        if action == StaleHeartbeatAction::Retry {
            let _ = append_coordinator_event(
                repo_root,
                "task_runtime_retry",
                task_id,
                phase,
                "queued",
                "stale heartbeat retry queued",
            );
        } else if action == StaleHeartbeatAction::Requeue {
            let _ = append_coordinator_event(
                repo_root,
                "task_runtime_requeue",
                task_id,
                phase,
                "queued",
                "stale heartbeat requeue queued",
            );
        }
    }

    if let Some(log) = logger {
        let _ = log.note(format!(
            "- Stale heartbeat policy applied count={} action={:?}",
            stale_ids.len(),
            action
        ));
    }

    Ok(stale_ids.len())
}

fn resolve_stale_heartbeat_seconds(_env_cfg: &CoordinatorEnvConfig) -> usize {
    if let Ok(raw) = std::env::var("STALE_HEARTBEAT_SECONDS") {
        if let Ok(value) = raw.trim().parse::<usize>() {
            return value;
        }
    }
    0
}

fn resolve_stale_heartbeat_action(
    _env_cfg: &CoordinatorEnvConfig,
    logger: Option<&NativeCoordinatorLogger>,
) -> StaleHeartbeatAction {
    let raw = std::env::var("STALE_HEARTBEAT_ACTION")
        .unwrap_or_else(|_| "block".to_string())
        .trim()
        .to_ascii_lowercase();
    match raw.as_str() {
        "retry" => StaleHeartbeatAction::Retry,
        "requeue" => StaleHeartbeatAction::Requeue,
        "block" => StaleHeartbeatAction::Block,
        other => {
            if let Some(log) = logger {
                let _ = log.note(format!(
                    "- Unknown STALE_HEARTBEAT_ACTION='{}', defaulting to block",
                    other
                ));
            }
            StaleHeartbeatAction::Block
        }
    }
}

fn increment_runtime_retries(task: &mut serde_json::Value) {
    coordinator_engine::ensure_runtime_object(task);
    if !task
        .get("task_runtime")
        .and_then(|v| v.get("metrics"))
        .map(serde_json::Value::is_object)
        .unwrap_or(false)
    {
        task["task_runtime"]["metrics"] = serde_json::json!({});
    }
    let current = task["task_runtime"]["metrics"]["retries"]
        .as_u64()
        .unwrap_or(0);
    let next = current.saturating_add(1);
    task["task_runtime"]["metrics"]["retries"] = serde_json::Value::from(next);
    task["task_runtime"]["retries"] = serde_json::Value::from(next);
}

fn resolve_error_code_retry_list(env_cfg: &CoordinatorEnvConfig) -> Vec<String> {
    let raw = env_cfg
        .error_code_retry_list
        .clone()
        .unwrap_or_else(|| "E101,E102,E103,E301,E302,E303".to_string());
    raw.split(',')
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect()
}

fn resolve_error_code_retry_max(env_cfg: &CoordinatorEnvConfig) -> usize {
    env_cfg.error_code_retry_max.unwrap_or(2)
}

pub async fn monitor_merge_jobs_native(
    repo_root: &Path,
    state: &mut CoordinatorRunState,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<Option<(String, String)>> {
    let mut blocked_merge: Option<(String, String)> = None;
    loop {
        match state.merge_event_rx.try_recv() {
            Ok(evt) => {
                let maybe_job = state.active_merge_jobs.remove(&evt.task_id);
                let elapsed = maybe_job
                    .as_ref()
                    .map(|j| j.started_at.elapsed().as_secs())
                    .unwrap_or(0);
                let mut registry = crate::coordinator::state::coordinator_state_registry_load(
                    repo_root,
                    &BTreeMap::new(),
                )?;
                let now = now_iso_coordinator();
                coordinator_engine::apply_merge_result_in_registry(
                    &mut registry,
                    &evt.task_id,
                    evt.success,
                    &evt.reason,
                    &now,
                )?;
                if evt.success {
                    if let Some(log) = logger {
                        let _ = log.note(format!(
                            "- Merge done task={} elapsed={}s",
                            evt.task_id, elapsed
                        ));
                    }
                } else {
                    blocked_merge = Some((evt.task_id.clone(), evt.reason.clone()));
                    if let Some(log) = logger {
                        let _ = log.note(format!(
                            "- Merge failed task={} elapsed={}s reason={}",
                            evt.task_id, elapsed, evt.reason
                        ));
                    }
                }
                recompute_resource_locks_from_tasks(&mut registry);
                set_registry_updated_at(&mut registry);
                crate::coordinator::state::coordinator_state_registry_save(
                    repo_root,
                    &BTreeMap::new(),
                    &registry,
                )?;
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
    while let Some(joined) = state.merge_join_set.try_join_next() {
        let _ = joined;
    }
    Ok(blocked_merge)
}

pub async fn dispatch_ready_tasks_native(
    repo_root: &Path,
    canonical: &macc_core::config::CanonicalConfig,
    coordinator: Option<&macc_core::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
    prd_file: &Path,
    state: &mut CoordinatorRunState,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<usize> {
    let mut dispatched = 0usize;
    let max_dispatch = env_cfg
        .max_dispatch
        .or_else(|| coordinator.and_then(|c| c.max_dispatch))
        .unwrap_or(10);
    let max_parallel = env_cfg
        .max_parallel
        .or_else(|| coordinator.and_then(|c| c.max_parallel))
        .unwrap_or(3);

    while max_dispatch == 0 || dispatched < max_dispatch {
        if max_parallel > 0 && state.active_jobs.len() >= max_parallel {
            break;
        }

        let mut registry = crate::coordinator::state::coordinator_state_registry_load(
            repo_root,
            &BTreeMap::new(),
        )?;
        let config = macc_core::coordinator::task_selector::TaskSelectorConfig {
            enabled_tools: canonical.tools.enabled.clone(),
            tool_priority: env_cfg
                .tool_priority
                .clone()
                .map(|csv| {
                    csv.split(',')
                        .map(|v| v.trim().to_string())
                        .filter(|v| !v.is_empty())
                        .collect::<Vec<_>>()
                })
                .or_else(|| coordinator.map(|c| c.tool_priority.clone()))
                .unwrap_or_default(),
            max_parallel_per_tool: env_cfg
                .max_parallel_per_tool_json
                .clone()
                .and_then(|raw| serde_json::from_str::<HashMap<String, usize>>(&raw).ok())
                .or_else(|| {
                    coordinator.map(|c| {
                        c.max_parallel_per_tool
                            .clone()
                            .into_iter()
                            .collect::<HashMap<_, _>>()
                    })
                })
                .unwrap_or_default(),
            tool_specializations: env_cfg
                .tool_specializations_json
                .clone()
                .and_then(|raw| serde_json::from_str::<HashMap<String, Vec<String>>>(&raw).ok())
                .or_else(|| {
                    coordinator.map(|c| {
                        c.tool_specializations
                            .clone()
                            .into_iter()
                            .collect::<HashMap<_, _>>()
                    })
                })
                .unwrap_or_default(),
            max_parallel,
            default_tool: canonical
                .tools
                .enabled
                .first()
                .cloned()
                .unwrap_or_else(|| "codex".to_string()),
            default_base_branch: env_cfg
                .reference_branch
                .clone()
                .or_else(|| coordinator.and_then(|c| c.reference_branch.clone()))
                .unwrap_or_else(|| "master".to_string()),
        };

        let Some(selected) =
            macc_core::coordinator::task_selector::select_next_ready_task(&registry, &config)
        else {
            break;
        };
        if let Some(log) = logger {
            let _ = log.note(format!(
                "- Dispatch candidate task={} tool={} base={}",
                selected.id, selected.tool, selected.base_branch
            ));
        }

        let reusable = find_reusable_worktree_native(
            repo_root,
            &registry,
            &selected.tool,
            &selected.base_branch,
        )?;

        let (worktree_path, branch, last_commit) = if let Some(reused) = reusable {
            let (path, branch, last_commit, skipped_reset) = reused;
            if let Some(log) = logger {
                let _ = log.note(format!(
                    "- reused_worktree path={} skipped_reset={}",
                    path.display(),
                    skipped_reset
                ));
            }
            (path, branch, last_commit)
        } else {
            let pool_count = count_pool_worktrees(repo_root)?;
            if max_parallel > 0 && pool_count >= max_parallel {
                break;
            }
            let create_spec = macc_core::WorktreeCreateSpec {
                slug: build_non_task_worker_slug(pool_count),
                tool: selected.tool.clone(),
                count: 1,
                base: selected.base_branch.clone(),
                dir: std::path::PathBuf::from(".macc/worktree"),
                scope: None,
                feature: None,
            };
            let mut created = macc_core::create_worktrees(repo_root, &create_spec)?;
            let created = created
                .pop()
                .ok_or_else(|| MaccError::Validation("No worktree created".into()))?;
            let last_commit = std::process::Command::new("git")
                .current_dir(&created.path)
                .args(["rev-parse", "HEAD"])
                .output()
                .ok()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                .unwrap_or_default();
            (created.path, created.branch, last_commit)
        };
        write_worktree_prd_for_task(prd_file, &selected.id, &worktree_path)?;
        ensure_tool_json(repo_root, &worktree_path, &selected.tool)?;
        let worktree_paths = macc_core::ProjectPaths::from_root(&worktree_path);
        macc_core::init(&worktree_paths, false)?;
        let canonical_yaml = canonical.to_yaml().map_err(|e| {
            MaccError::Validation(format!(
                "Failed to serialize canonical config for worktree dispatch apply: {}",
                e
            ))
        })?;
        macc_core::atomic_write(
            &worktree_paths,
            &worktree_paths.config_path,
            canonical_yaml.as_bytes(),
        )?;

        let mut apply_cmd = tokio::process::Command::new(std::env::current_exe().map_err(|e| {
            MaccError::Validation(format!("Failed to resolve current executable path: {}", e))
        })?);
        apply_cmd
            .current_dir(repo_root)
            .arg("--cwd")
            .arg(repo_root)
            .arg("worktree")
            .arg("apply")
            .arg(worktree_path.to_string_lossy().to_string())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let apply_output = apply_cmd.output().await.map_err(|e| MaccError::Io {
            path: worktree_path.to_string_lossy().into(),
            action: "run worktree apply for coordinator dispatch".into(),
            source: e,
        })?;
        if !apply_output.status.success() {
            let detail = format!(
                "stdout=\"{}\" stderr=\"{}\"",
                summarize_output(&String::from_utf8_lossy(&apply_output.stdout)),
                summarize_output(&String::from_utf8_lossy(&apply_output.stderr))
            );
            if let Some(log) = logger {
                let _ = log.note(format!(
                    "- Worktree apply failed task={} status={} {}",
                    selected.id, apply_output.status, detail
                ));
            }
            return Err(MaccError::Validation(format!(
                "worktree apply failed for {} with status {} ({})",
                selected.id, apply_output.status, detail
            )));
        }
        if let Some(log) = logger {
            let _ = log.note(format!(
                "- Worktree ready task={} path={}",
                selected.id,
                worktree_path.display()
            ));
        }

        let dispatch_now = now_iso_coordinator();
        let dispatch_session_id = format!("coordinator-{}-{}", selected.id, dispatch_now);
        let update = coordinator_engine::DispatchClaimUpdate {
            task_id: selected.id.clone(),
            tool: selected.tool.clone(),
            worktree_path: worktree_path.to_string_lossy().to_string(),
            branch: branch.clone(),
            base_branch: selected.base_branch.clone(),
            last_commit: last_commit.clone(),
            session_id: dispatch_session_id.clone(),
            pid: None,
            phase: "dev".to_string(),
            now: dispatch_now.clone(),
        };
        coordinator_engine::apply_dispatch_claim_in_registry(&mut registry, &update)?;
        recompute_resource_locks_from_tasks(&mut registry);
        set_registry_updated_at(&mut registry);
        crate::coordinator::state::coordinator_state_registry_save(
            repo_root,
            &BTreeMap::new(),
            &registry,
        )?;

        let phase_timeout_seconds = env_cfg
            .stale_in_progress_seconds
            .or_else(|| coordinator.and_then(|c| c.stale_in_progress_seconds))
            .unwrap_or(0);
        let pid = spawn_performer_job_native(
            repo_root,
            &selected.id,
            &worktree_path,
            &state.event_tx,
            &mut state.join_set,
            phase_timeout_seconds,
        )?;
        let mut registry = crate::coordinator::state::coordinator_state_registry_load(
            repo_root,
            &BTreeMap::new(),
        )?;
        coordinator_engine::apply_dispatch_pid_in_registry(&mut registry, &selected.id, pid)?;
        set_registry_updated_at(&mut registry);
        crate::coordinator::state::coordinator_state_registry_save(
            repo_root,
            &BTreeMap::new(),
            &registry,
        )?;

        state.active_jobs.insert(
            selected.id.clone(),
            crate::CoordinatorJob {
                tool: selected.tool,
                worktree_path,
                attempt: 1,
                started_at: std::time::Instant::now(),
                pid,
            },
        );
        if let Some(log) = logger {
            let _ = log.note(format!(
                "- Task dispatched task={} pid={}",
                selected.id,
                pid.map(|v| v.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            ));
        }
        dispatched += 1;
    }
    Ok(dispatched)
}
