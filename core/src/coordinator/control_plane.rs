use crate::coordinator::helpers::{
    append_coordinator_event, append_coordinator_event_with_severity, build_non_task_worker_slug,
    count_pool_worktrees, find_reusable_worktree_native, now_iso_coordinator,
    recompute_resource_locks_from_tasks, set_registry_updated_at, write_worktree_prd_for_task,
};
use crate::coordinator::runtime::{CoordinatorJob, CoordinatorMergeJob, CoordinatorRunState};
use crate::coordinator::types::CoordinatorEnvConfig;
use crate::coordinator::{engine as coordinator_engine, runtime as coordinator_runtime};
use crate::{MaccError, Result};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::time::{Duration, Instant};

pub trait CoordinatorLog: Sync {
    fn note(&self, line: String) -> Result<()>;
}

fn resolve_dispatch_cooldown_seconds() -> u64 {
    std::env::var("COORDINATOR_DISPATCH_COOLDOWN_SECONDS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(10)
}

fn resolve_merge_timeout_seconds() -> usize {
    std::env::var("COORDINATOR_MERGE_JOB_TIMEOUT_SECONDS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

async fn sanitize_worktree_to_base(worktree_path: &Path, base_branch: &str) -> Result<bool> {
    if !crate::git::reset_hard_async(worktree_path, "HEAD").await? {
        return Ok(false);
    }
    if !crate::git::clean_fd_async(worktree_path).await? {
        return Ok(false);
    }
    if !crate::git::checkout_async(worktree_path, base_branch, false).await?
        && !crate::git::checkout_reset_branch_async(worktree_path, base_branch, false).await?
    {
        return Ok(false);
    }
    if !crate::git::fetch_async(worktree_path, "origin").await? {
        return Ok(false);
    }
    if !crate::git::reset_hard_async(worktree_path, base_branch).await? {
        return Ok(false);
    }
    if !crate::git::reset_hard_async(worktree_path, "HEAD").await? {
        return Ok(false);
    }
    if !crate::git::clean_fd_async(worktree_path).await? {
        return Ok(false);
    }
    Ok(true)
}

fn emit_dispatch_skipped(
    repo_root: &Path,
    logger: Option<&dyn CoordinatorLog>,
    task_id: &str,
    reason: &str,
    detail: &str,
) {
    let msg = format!(
        "dispatch skipped task={} reason={} detail={}",
        task_id, reason, detail
    );
    let _ = append_coordinator_event_with_severity(
        repo_root,
        "dispatch_skipped",
        task_id,
        "dev",
        "skipped",
        &msg,
        "warning",
    );
    if let Some(log) = logger {
        let _ = log.note(format!("- {}", msg));
    }
}

async fn switch_worktree_to_base_after_merge(
    repo_root: &Path,
    task: &serde_json::Value,
    logger: Option<&dyn CoordinatorLog>,
) -> Result<()> {
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
        return Ok(());
    }
    let base_branch = task
        .get("worktree")
        .and_then(|w| w.get("base_branch"))
        .and_then(serde_json::Value::as_str)
        .filter(|v| !v.trim().is_empty())
        .or_else(|| {
            task.get("base_branch")
                .and_then(serde_json::Value::as_str)
                .filter(|v| !v.trim().is_empty())
        })
        .unwrap_or("master");

    let wt = Path::new(worktree_path);

    // First action after merge success: force checkout base to release task branch immediately.
    let switched = if crate::git::checkout_async(wt, base_branch, true).await? {
        true
    } else {
        crate::git::checkout_reset_branch_async(wt, base_branch, true).await?
    };
    if !switched {
        let msg = format!(
            "worktree switch skipped task={} path={} base={} reason=checkout_failed",
            task_id, worktree_path, base_branch
        );
        let _ = append_coordinator_event_with_severity(
            repo_root,
            "worktree_switch",
            task_id,
            "integrate",
            "failed",
            &msg,
            "warning",
        );
        if let Some(log) = logger {
            let _ = log.note(format!("- {}", msg));
        }
        return Ok(());
    }
    // Continue with sanitization now that the worker branch is no longer checked out.
    let _ = crate::git::reset_hard_async(wt, "HEAD").await?;
    let _ = crate::git::clean_fd_async(wt).await?;
    // Stateless policy: fetch origin refs then hard reset to base.
    if !crate::git::fetch_async(wt, "origin").await? {
        let msg = format!(
            "worktree switch warning task={} path={} base={} reason=fetch_failed",
            task_id, worktree_path, base_branch
        );
        let _ = append_coordinator_event_with_severity(
            repo_root,
            "worktree_switch",
            task_id,
            "integrate",
            "warning",
            &msg,
            "warning",
        );
        if let Some(log) = logger {
            let _ = log.note(format!("- {}", msg));
        }
        return Ok(());
    }
    if !crate::git::reset_hard_async(wt, base_branch).await? {
        let msg = format!(
            "worktree switch warning task={} path={} base={} reason=reset_hard_failed",
            task_id, worktree_path, base_branch
        );
        let _ = append_coordinator_event_with_severity(
            repo_root,
            "worktree_switch",
            task_id,
            "integrate",
            "warning",
            &msg,
            "warning",
        );
        if let Some(log) = logger {
            let _ = log.note(format!("- {}", msg));
        }
        return Ok(());
    }
    let msg = format!(
        "worktree switched to base task={} path={} base={}",
        task_id, worktree_path, base_branch
    );
    let _ = append_coordinator_event_with_severity(
        repo_root,
        "worktree_switch",
        task_id,
        "integrate",
        "success",
        &msg,
        "info",
    );
    if let Some(log) = logger {
        let _ = log.note(format!("- {}", msg));
    }
    Ok(())
}

pub fn sync_registry_from_prd_native(
    repo_root: &Path,
    prd_file: &Path,
    logger: Option<&dyn CoordinatorLog>,
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
    logger: Option<&'a dyn CoordinatorLog>,
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
            coordinator_runtime::resolve_phase_runner(self.repo_root, &worktree, &phase_tool)?
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
        let prompt = coordinator_runtime::build_phase_prompt(mode, task_id, &phase_tool, task)?;
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
                coordinator_runtime::summarize_output(&String::from_utf8_lossy(&out.stdout)),
                coordinator_runtime::summarize_output(&String::from_utf8_lossy(&out.stderr))
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
    logger: Option<&dyn CoordinatorLog>,
) -> Result<std::result::Result<String, String>> {
    let executor = NativePhaseExecutor { repo_root, logger };
    coordinator_runtime::run_phase(
        &executor,
        task,
        mode,
        coordinator_tool_override,
        max_attempts,
    )
}

pub fn run_review_phase_for_task_native(
    repo_root: &Path,
    task: &serde_json::Value,
    coordinator_tool_override: Option<&str>,
    max_attempts: usize,
    logger: Option<&dyn CoordinatorLog>,
) -> Result<std::result::Result<coordinator_engine::ReviewVerdict, String>> {
    let executor = NativePhaseExecutor { repo_root, logger };
    coordinator_runtime::run_review_phase(&executor, task, coordinator_tool_override, max_attempts)
}

pub async fn advance_tasks_native(
    repo_root: &Path,
    coordinator_tool_override: Option<&str>,
    phase_runner_max_attempts: usize,
    state: &mut CoordinatorRunState,
    logger: Option<&dyn CoordinatorLog>,
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
                let repo = repo_root.to_path_buf();
                let task_for_worker = task_id.clone();
                let branch_for_worker = branch.clone();
                let base_for_worker = base.clone();
                coordinator_runtime::spawn_merge_job(
                    &task_id,
                    &state.merge_event_tx,
                    &mut state.merge_join_set,
                    resolve_merge_timeout_seconds(),
                    move || {
                        coordinator_runtime::merge_task_with_policy_native(
                            &repo,
                            &task_for_worker,
                            &branch_for_worker,
                            &base_for_worker,
                            |event_type, task_id, phase, status, message, severity| {
                                let _ = append_coordinator_event_with_severity(
                                    &repo, event_type, task_id, phase, status, message, severity,
                                );
                            },
                        )
                    },
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
    logger: Option<&dyn CoordinatorLog>,
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
                    let sealed = crate::coordinator::session_manager::seal_worktree_scoped_session(
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
                    let current_exe = std::env::current_exe().map_err(|e| {
                        MaccError::Validation(format!(
                            "Failed to resolve current executable path: {}",
                            e
                        ))
                    })?;
                    let retry_pid = coordinator_runtime::spawn_performer_job(
                        &current_exe,
                        repo_root,
                        &task_id,
                        &job.worktree_path,
                        &state.event_tx,
                        &mut state.join_set,
                        phase_timeout_seconds,
                    )?;
                    state.active_jobs.insert(
                        task_id,
                        CoordinatorJob {
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
    logger: Option<&dyn CoordinatorLog>,
) -> Result<usize> {
    let current_run_id = std::env::var("COORDINATOR_RUN_ID").ok();
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
    let len = file
        .metadata()
        .map_err(|e| MaccError::Io {
            path: events_file.to_string_lossy().into(),
            action: "read coordinator events metadata".into(),
            source: e,
        })?
        .len();
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

    let project_paths = crate::ProjectPaths::from_root(repo_root);
    let mut heartbeat_updates: HashMap<String, String> = HashMap::new();
    let mut terminal_success_sources: HashSet<(String, String)> = HashSet::new();
    for line in buf.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(event) = serde_json::from_str::<serde_json::Value>(trimmed) else {
            continue;
        };
        if let Some(expected_run_id) = current_run_id.as_deref() {
            let event_run_id = event
                .get("run_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if event_run_id != expected_run_id {
                continue;
            }
        }
        let event_type = event
            .get("type")
            .or_else(|| event.get("event"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let event_status = event
            .get("status")
            .or_else(|| event.get("state"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let event_task_id = event
            .get("task_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let event_source = event
            .get("source")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let payload = event.get("payload").unwrap_or(&serde_json::Value::Null);
        let payload_attempt = payload
            .get("attempt")
            .and_then(serde_json::Value::as_i64)
            .is_some();
        let is_terminal_success = event_type == "commit_created"
            || (event_type == "phase_result" && event_status == "done" && !payload_attempt);
        if is_terminal_success && !event_task_id.is_empty() && !event_source.is_empty() {
            terminal_success_sources.insert((event_task_id.to_string(), event_source.to_string()));
        }
        let is_failed =
            event_type == "failed" || (event_type == "phase_result" && event_status == "failed");
        if is_failed
            && !event_task_id.is_empty()
            && !event_source.is_empty()
            && terminal_success_sources
                .contains(&(event_task_id.to_string(), event_source.to_string()))
        {
            if let Some(log) = logger {
                let _ = log.note(format!(
                    "- Ignored late inconsistent failed event task={} source={}",
                    event_task_id, event_source
                ));
            }
            continue;
        }
        // Ingest performer/runtime events into SQLite source-of-truth.
        let _ = crate::coordinator_storage::append_event_sqlite(&project_paths, &event)?;
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
    if let Some(tasks) = registry
        .get_mut("tasks")
        .and_then(serde_json::Value::as_array_mut)
    {
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
    logger: Option<&dyn CoordinatorLog>,
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
    let Some(tasks) = registry
        .get_mut("tasks")
        .and_then(serde_json::Value::as_array_mut)
    else {
        return Ok(0);
    };

    let mut stale_ids = Vec::new();
    for task in tasks.iter_mut() {
        coordinator_engine::ensure_runtime_object(task);
        let status = task["task_runtime"]["status"].as_str().unwrap_or_default();
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
                task["task_runtime"]["status"] = serde_json::Value::String("stale".to_string());
                task["task_runtime"]["pid"] = serde_json::Value::Null;
                task["task_runtime"]["last_error"] = serde_json::Value::String(detail.clone());
                task["state"] = serde_json::Value::String("blocked".to_string());
            }
            StaleHeartbeatAction::Requeue => {
                crate::coordinator::state::reset_runtime_to_idle(task);
                task["task_runtime"]["last_error"] = serde_json::Value::String(detail.clone());
                task["state"] = serde_json::Value::String("todo".to_string());
                task["assignee"] = serde_json::Value::Null;
                task["claimed_at"] = serde_json::Value::Null;
                task["worktree"] = serde_json::Value::Null;
            }
            StaleHeartbeatAction::Retry => {
                increment_runtime_retries(task);
                crate::coordinator::state::reset_runtime_to_idle(task);
                task["task_runtime"]["last_error"] = serde_json::Value::String(detail.clone());
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
    logger: Option<&dyn CoordinatorLog>,
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
    logger: Option<&dyn CoordinatorLog>,
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
                    if let Some(task_snapshot) = registry
                        .get("tasks")
                        .and_then(serde_json::Value::as_array)
                        .and_then(|tasks| {
                            tasks.iter().find(|task| {
                                task.get("id")
                                    .and_then(serde_json::Value::as_str)
                                    .unwrap_or_default()
                                    == evt.task_id
                            })
                        })
                        .cloned()
                    {
                        // Post-merge order is strict:
                        // 1) switch worktree to base (release task branch)
                        // 2) cleanup merged task branch
                        let _ =
                            switch_worktree_to_base_after_merge(repo_root, &task_snapshot, logger)
                                .await;
                        let branch = task_snapshot
                            .get("worktree")
                            .and_then(|w| w.get("branch"))
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default();
                        let base = task_snapshot
                            .get("worktree")
                            .and_then(|w| w.get("base_branch"))
                            .and_then(serde_json::Value::as_str)
                            .or_else(|| {
                                task_snapshot
                                    .get("base_branch")
                                    .and_then(serde_json::Value::as_str)
                            })
                            .unwrap_or("master");
                        if !branch.is_empty() && branch != base {
                            coordinator_runtime::report_branch_cleanup_outcome(
                                repo_root,
                                Some(&evt.task_id),
                                "integrate",
                                branch,
                                base,
                                "merge_success_post_switch",
                                coordinator_runtime::cleanup_merged_local_branch(
                                    repo_root, branch, base,
                                ),
                                |event_type, task_id, phase, status, message, severity| {
                                    let _ = append_coordinator_event_with_severity(
                                        repo_root, event_type, task_id, phase, status, message,
                                        severity,
                                    );
                                },
                                |msg| tracing::warn!("{}", msg),
                            );
                        }
                    }
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
    canonical: &crate::config::CanonicalConfig,
    coordinator: Option<&crate::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
    prd_file: &Path,
    state: &mut CoordinatorRunState,
    logger: Option<&dyn CoordinatorLog>,
) -> Result<usize> {
    let mut dispatched = 0usize;
    let mut dispatch_failed_this_cycle: HashSet<String> = HashSet::new();
    let cooldown_seconds = resolve_dispatch_cooldown_seconds();
    state
        .dispatch_retry_not_before
        .retain(|_, until| *until > Instant::now());
    let max_dispatch_total = env_cfg
        .max_dispatch
        .or_else(|| coordinator.and_then(|c| c.max_dispatch))
        .unwrap_or(10);
    let max_parallel = env_cfg
        .max_parallel
        .or_else(|| coordinator.and_then(|c| c.max_parallel))
        .unwrap_or(3);

    if max_dispatch_total > 0 && state.dispatched_total_run >= max_dispatch_total {
        if !state.dispatch_limit_event_emitted {
            let msg = format!(
                "dispatch limit reached run_total={} max_dispatch={}",
                state.dispatched_total_run, max_dispatch_total
            );
            let _ = append_coordinator_event_with_severity(
                repo_root,
                "dispatch_limit_reached",
                "-",
                "dev",
                "done",
                &msg,
                "info",
            );
            if let Some(log) = logger {
                let _ = log.note(format!("- {}", msg));
            }
            state.dispatch_limit_event_emitted = true;
        }
        return Ok(0);
    }
    let remaining_budget = if max_dispatch_total == 0 {
        usize::MAX
    } else {
        max_dispatch_total.saturating_sub(state.dispatched_total_run)
    };

    while dispatched < remaining_budget {
        if max_parallel > 0 && state.active_jobs.len() >= max_parallel {
            break;
        }

        let mut registry = crate::coordinator::state::coordinator_state_registry_load(
            repo_root,
            &BTreeMap::new(),
        )?;
        let config = crate::coordinator::task_selector::TaskSelectorConfig {
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
            crate::coordinator::task_selector::select_next_ready_task(&registry, &config)
        else {
            break;
        };
        if let Some(until) = state.dispatch_retry_not_before.get(&selected.id) {
            let now = Instant::now();
            if *until > now {
                let remaining = until.duration_since(now).as_secs();
                emit_dispatch_skipped(
                    repo_root,
                    logger,
                    &selected.id,
                    "cooldown_active",
                    &format!("retry in {}s", remaining),
                );
                break;
            }
        }
        if dispatch_failed_this_cycle.contains(&selected.id) {
            if let Some(log) = logger {
                let _ = log.note(format!(
                    "- Dispatch stop: task {} already failed worktree preparation in this cycle",
                    selected.id
                ));
            }
            break;
        }
        if let Some(log) = logger {
            let _ = log.note(format!("- Lifecycle task={} stage=claim", selected.id));
        }
        if let Some(log) = logger {
            let _ = log.note(format!(
                "- Dispatch candidate task={} tool={} base={}",
                selected.id, selected.tool, selected.base_branch
            ));
        }

        let reuse_scan_started = Instant::now();
        let (reusable, reuse_prepare_error) = find_reusable_worktree_native(
            repo_root,
            &registry,
            &selected.tool,
            &selected.base_branch,
        )?;
        let reuse_scan_elapsed_ms = reuse_scan_started.elapsed().as_millis();

        let (worktree_path, branch, last_commit) = if let Some(reused) = reusable {
            let (path, branch, last_commit, skipped_reset, dirty_before) = reused;
            let sanitize_msg = format!(
                "sanitize done task={} mode=reused path={} duration_ms={} dirty_before={} skipped_reset={}",
                selected.id,
                path.display(),
                reuse_scan_elapsed_ms,
                dirty_before,
                skipped_reset
            );
            let _ = append_coordinator_event_with_severity(
                repo_root,
                "sanitize_done",
                &selected.id,
                "dev",
                "success",
                &sanitize_msg,
                "info",
            );
            if let Some(log) = logger {
                let _ = log.note(format!(
                    "- Lifecycle task={} stage=sanitize path={} dirty_before={} skipped_reset={}",
                    selected.id,
                    path.display(),
                    dirty_before,
                    skipped_reset
                ));
            }
            (path, branch, last_commit)
        } else {
            let pool_count = count_pool_worktrees(repo_root)?;
            if max_parallel > 0 && pool_count >= max_parallel {
                if let Some((reason, detail)) = reuse_prepare_error {
                    emit_dispatch_skipped(repo_root, logger, &selected.id, &reason, &detail);
                    if cooldown_seconds > 0 {
                        state.dispatch_retry_not_before.insert(
                            selected.id.clone(),
                            Instant::now() + Duration::from_secs(cooldown_seconds),
                        );
                    }
                    dispatch_failed_this_cycle.insert(selected.id.clone());
                }
                break;
            }
            let create_spec = crate::WorktreeCreateSpec {
                slug: build_non_task_worker_slug(pool_count),
                tool: selected.tool.clone(),
                count: 1,
                base: selected.base_branch.clone(),
                dir: std::path::PathBuf::from(".macc/worktree"),
                scope: None,
                feature: None,
            };
            let mut created = match crate::create_worktrees(repo_root, &create_spec) {
                Ok(v) => v,
                Err(e) => {
                    let msg = format!(
                        "dispatch failed for task {}: create worktree failed ({})",
                        selected.id, e
                    );
                    let _ = append_coordinator_event_with_severity(
                        repo_root,
                        "dispatch_failed",
                        &selected.id,
                        "dev",
                        "failed",
                        &msg,
                        "warning",
                    );
                    if let Some(log) = logger {
                        let _ = log.note(format!("- {}", msg));
                    }
                    emit_dispatch_skipped(
                        repo_root,
                        logger,
                        &selected.id,
                        "create_worktree_failed",
                        &e.to_string(),
                    );
                    if cooldown_seconds > 0 {
                        state.dispatch_retry_not_before.insert(
                            selected.id.clone(),
                            Instant::now() + Duration::from_secs(cooldown_seconds),
                        );
                    }
                    dispatch_failed_this_cycle.insert(selected.id.clone());
                    break;
                }
            };
            let created = created
                .pop()
                .ok_or_else(|| MaccError::Validation("No worktree created".into()))?;
            let sanitize_started = Instant::now();
            if !sanitize_worktree_to_base(&created.path, &selected.base_branch).await? {
                let msg = format!(
                    "dispatch failed for task {}: sanitize new worktree failed ({})",
                    selected.id,
                    created.path.display()
                );
                let _ = append_coordinator_event_with_severity(
                    repo_root,
                    "dispatch_failed",
                    &selected.id,
                    "dev",
                    "failed",
                    &msg,
                    "warning",
                );
                if let Some(log) = logger {
                    let _ = log.note(format!("- {}", msg));
                }
                emit_dispatch_skipped(
                    repo_root,
                    logger,
                    &selected.id,
                    "sanitize_new_worktree_failed",
                    &created.path.to_string_lossy(),
                );
                if cooldown_seconds > 0 {
                    state.dispatch_retry_not_before.insert(
                        selected.id.clone(),
                        Instant::now() + Duration::from_secs(cooldown_seconds),
                    );
                }
                dispatch_failed_this_cycle.insert(selected.id.clone());
                break;
            }
            let sanitize_elapsed_ms = sanitize_started.elapsed().as_millis();
            let sanitize_msg = format!(
                "sanitize done task={} mode=new path={} duration_ms={} dirty_before=false skipped_reset=false",
                selected.id,
                created.path.display(),
                sanitize_elapsed_ms
            );
            let _ = append_coordinator_event_with_severity(
                repo_root,
                "sanitize_done",
                &selected.id,
                "dev",
                "success",
                &sanitize_msg,
                "info",
            );
            let last_commit = crate::git::head_commit_async(&created.path)
                .await
                .unwrap_or_default();
            if let Some(log) = logger {
                let _ = log.note(format!(
                    "- Lifecycle task={} stage=sanitize path={} dirty_before=false skipped_reset=false",
                    selected.id,
                    created.path.display()
                ));
            }
            (created.path, created.branch, last_commit)
        };
        let dispatch_now = now_iso_coordinator();
        let dispatch_session_id = format!("coordinator-{}-{}", selected.id, dispatch_now);
        let claim_update = coordinator_engine::DispatchClaimUpdate {
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
        coordinator_engine::apply_dispatch_claim_in_registry(&mut registry, &claim_update)?;
        recompute_resource_locks_from_tasks(&mut registry);
        set_registry_updated_at(&mut registry);
        crate::coordinator::state::coordinator_state_registry_save(
            repo_root,
            &BTreeMap::new(),
            &registry,
        )?;
        if let Some(log) = logger {
            let _ = log.note(format!(
                "- Lifecycle task={} stage=claim persisted session_id={}",
                selected.id, dispatch_session_id
            ));
        }

        let rollback_claim = |detail: &str| -> Result<()> {
            let mut rollback_registry = crate::coordinator::state::coordinator_state_registry_load(
                repo_root,
                &BTreeMap::new(),
            )?;
            if let Some(tasks) = rollback_registry
                .get_mut("tasks")
                .and_then(serde_json::Value::as_array_mut)
            {
                for task in tasks.iter_mut() {
                    if task
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        == selected.id
                    {
                        task["state"] = serde_json::Value::String("todo".to_string());
                        task["assignee"] = serde_json::Value::Null;
                        task["claimed_at"] = serde_json::Value::Null;
                        task["worktree"] = serde_json::Value::Null;
                        coordinator_engine::ensure_runtime_object(task);
                        task["task_runtime"]["status"] =
                            serde_json::Value::String("idle".to_string());
                        task["task_runtime"]["pid"] = serde_json::Value::Null;
                        task["task_runtime"]["current_phase"] = serde_json::Value::Null;
                        task["task_runtime"]["last_error"] =
                            serde_json::Value::String(detail.to_string());
                        task["updated_at"] = serde_json::Value::String(now_iso_coordinator());
                        task["state_changed_at"] = serde_json::Value::String(now_iso_coordinator());
                        break;
                    }
                }
            }
            recompute_resource_locks_from_tasks(&mut rollback_registry);
            set_registry_updated_at(&mut rollback_registry);
            crate::coordinator::state::coordinator_state_registry_save(
                repo_root,
                &BTreeMap::new(),
                &rollback_registry,
            )
        };

        if let Some(log) = logger {
            let _ = log.note(format!("- Lifecycle task={} stage=setup", selected.id));
        }
        if let Err(err) = write_worktree_prd_for_task(prd_file, &selected.id, &worktree_path) {
            let msg = format!(
                "dispatch failed for task {}: write worktree.prd.json failed ({})",
                selected.id, err
            );
            let _ = append_coordinator_event_with_severity(
                repo_root,
                "dispatch_failed",
                &selected.id,
                "dev",
                "failed",
                &msg,
                "warning",
            );
            emit_dispatch_skipped(
                repo_root,
                logger,
                &selected.id,
                "write_worktree_prd_failed",
                &err.to_string(),
            );
            let _ = rollback_claim(&msg);
            if let Some(log) = logger {
                let _ = log.note(format!("- {}", msg));
            }
            if cooldown_seconds > 0 {
                state.dispatch_retry_not_before.insert(
                    selected.id.clone(),
                    Instant::now() + Duration::from_secs(cooldown_seconds),
                );
            }
            dispatch_failed_this_cycle.insert(selected.id.clone());
            break;
        }
        let tool_json_path = worktree_path.join(".macc").join("tool.json");
        if !tool_json_path.exists() {
            if let Err(err) =
                crate::worktree::write_tool_json(repo_root, &worktree_path, &selected.tool)
            {
                let msg = format!(
                    "dispatch failed for task {}: ensure tool.json failed ({})",
                    selected.id, err
                );
                let _ = append_coordinator_event_with_severity(
                    repo_root,
                    "dispatch_failed",
                    &selected.id,
                    "dev",
                    "failed",
                    &msg,
                    "warning",
                );
                emit_dispatch_skipped(
                    repo_root,
                    logger,
                    &selected.id,
                    "ensure_tool_json_failed",
                    &err.to_string(),
                );
                let _ = rollback_claim(&msg);
                if let Some(log) = logger {
                    let _ = log.note(format!("- {}", msg));
                }
                if cooldown_seconds > 0 {
                    state.dispatch_retry_not_before.insert(
                        selected.id.clone(),
                        Instant::now() + Duration::from_secs(cooldown_seconds),
                    );
                }
                dispatch_failed_this_cycle.insert(selected.id.clone());
                break;
            }
        }
        let worktree_paths = crate::ProjectPaths::from_root(&worktree_path);
        if let Err(err) = crate::init(&worktree_paths, false) {
            let msg = format!(
                "dispatch failed for task {}: initialize worktree failed ({})",
                selected.id, err
            );
            let _ = append_coordinator_event_with_severity(
                repo_root,
                "dispatch_failed",
                &selected.id,
                "dev",
                "failed",
                &msg,
                "warning",
            );
            emit_dispatch_skipped(
                repo_root,
                logger,
                &selected.id,
                "worktree_init_failed",
                &err.to_string(),
            );
            let _ = rollback_claim(&msg);
            if let Some(log) = logger {
                let _ = log.note(format!("- {}", msg));
            }
            if cooldown_seconds > 0 {
                state.dispatch_retry_not_before.insert(
                    selected.id.clone(),
                    Instant::now() + Duration::from_secs(cooldown_seconds),
                );
            }
            dispatch_failed_this_cycle.insert(selected.id.clone());
            break;
        }
        let canonical_yaml = canonical.to_yaml().map_err(|e| {
            MaccError::Validation(format!(
                "Failed to serialize canonical config for worktree dispatch apply: {}",
                e
            ))
        })?;
        if let Err(err) = crate::atomic_write(
            &worktree_paths,
            &worktree_paths.config_path,
            canonical_yaml.as_bytes(),
        ) {
            let msg = format!(
                "dispatch failed for task {}: write canonical config failed ({})",
                selected.id, err
            );
            let _ = append_coordinator_event_with_severity(
                repo_root,
                "dispatch_failed",
                &selected.id,
                "dev",
                "failed",
                &msg,
                "warning",
            );
            emit_dispatch_skipped(
                repo_root,
                logger,
                &selected.id,
                "write_canonical_config_failed",
                &err.to_string(),
            );
            let _ = rollback_claim(&msg);
            if let Some(log) = logger {
                let _ = log.note(format!("- {}", msg));
            }
            if cooldown_seconds > 0 {
                state.dispatch_retry_not_before.insert(
                    selected.id.clone(),
                    Instant::now() + Duration::from_secs(cooldown_seconds),
                );
            }
            dispatch_failed_this_cycle.insert(selected.id.clone());
            break;
        }

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
                coordinator_runtime::summarize_output(&String::from_utf8_lossy(
                    &apply_output.stdout
                )),
                coordinator_runtime::summarize_output(&String::from_utf8_lossy(
                    &apply_output.stderr
                ))
            );
            let msg = format!(
                "dispatch failed for task {}: worktree apply failed status={} {}",
                selected.id, apply_output.status, detail
            );
            let _ = append_coordinator_event_with_severity(
                repo_root,
                "dispatch_failed",
                &selected.id,
                "dev",
                "failed",
                &msg,
                "warning",
            );
            if let Some(log) = logger {
                let _ = log.note(format!("- {}", msg));
            }
            emit_dispatch_skipped(
                repo_root,
                logger,
                &selected.id,
                "worktree_apply_failed",
                &detail,
            );
            let _ = rollback_claim(&msg);
            if cooldown_seconds > 0 {
                state.dispatch_retry_not_before.insert(
                    selected.id.clone(),
                    Instant::now() + Duration::from_secs(cooldown_seconds),
                );
            }
            dispatch_failed_this_cycle.insert(selected.id.clone());
            break;
        }
        if let Some(log) = logger {
            let _ = log.note(format!(
                "- Worktree ready task={} path={}",
                selected.id,
                worktree_path.display()
            ));
        }

        let phase_timeout_seconds = env_cfg
            .stale_in_progress_seconds
            .or_else(|| coordinator.and_then(|c| c.stale_in_progress_seconds))
            .unwrap_or(0);
        let current_exe = std::env::current_exe().map_err(|e| {
            MaccError::Validation(format!("Failed to resolve current executable path: {}", e))
        })?;
        let pid = match coordinator_runtime::spawn_performer_job(
            &current_exe,
            repo_root,
            &selected.id,
            &worktree_path,
            &state.event_tx,
            &mut state.join_set,
            phase_timeout_seconds,
        ) {
            Ok(pid) => pid,
            Err(err) => {
                let msg = format!(
                    "dispatch failed for task {}: performer spawn failed ({})",
                    selected.id, err
                );
                let _ = append_coordinator_event_with_severity(
                    repo_root,
                    "dispatch_failed",
                    &selected.id,
                    "dev",
                    "failed",
                    &msg,
                    "warning",
                );
                let _ = rollback_claim(&msg);
                if let Some(log) = logger {
                    let _ = log.note(format!("- {}", msg));
                }
                emit_dispatch_skipped(
                    repo_root,
                    logger,
                    &selected.id,
                    "spawn_performer_failed",
                    &err.to_string(),
                );
                dispatch_failed_this_cycle.insert(selected.id.clone());
                if cooldown_seconds > 0 {
                    state.dispatch_retry_not_before.insert(
                        selected.id.clone(),
                        Instant::now() + Duration::from_secs(cooldown_seconds),
                    );
                }
                break;
            }
        };
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
        if let Some(log) = logger {
            let _ = log.note(format!(
                "- Lifecycle task={} stage=run pid_persisted={}",
                selected.id,
                pid.map(|v| v.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            ));
        }

        state.active_jobs.insert(
            selected.id.clone(),
            CoordinatorJob {
                tool: selected.tool,
                worktree_path,
                attempt: 1,
                started_at: std::time::Instant::now(),
                pid,
            },
        );
        if let Some(log) = logger {
            let _ = log.note(format!("- Lifecycle task={} stage=run", selected.id));
            let _ = log.note(format!(
                "- Task dispatched task={} pid={}",
                selected.id,
                pid.map(|v| v.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            ));
        }
        dispatched += 1;
        state.dispatched_total_run += 1;
        if max_dispatch_total > 0 && state.dispatched_total_run >= max_dispatch_total {
            if !state.dispatch_limit_event_emitted {
                let msg = format!(
                    "dispatch limit reached run_total={} max_dispatch={}",
                    state.dispatched_total_run, max_dispatch_total
                );
                let _ = append_coordinator_event_with_severity(
                    repo_root,
                    "dispatch_limit_reached",
                    "-",
                    "dev",
                    "done",
                    &msg,
                    "info",
                );
                if let Some(log) = logger {
                    let _ = log.note(format!("- {}", msg));
                }
                state.dispatch_limit_event_emitted = true;
            }
            break;
        }
    }
    Ok(dispatched)
}
