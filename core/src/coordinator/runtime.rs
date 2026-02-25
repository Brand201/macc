use crate::coordinator::engine::ReviewVerdict;
use crate::{MaccError, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct CoordinatorJob {
    pub tool: String,
    pub worktree_path: PathBuf,
    pub attempt: usize,
    pub started_at: std::time::Instant,
    pub pid: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct CoordinatorMergeJob {
    pub started_at: std::time::Instant,
}

#[derive(Debug, Clone)]
pub struct CoordinatorJobEvent {
    pub task_id: String,
    pub success: bool,
    pub status_text: String,
    pub timed_out: bool,
    pub error_code: Option<String>,
    pub error_origin: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CoordinatorMergeEvent {
    pub task_id: String,
    pub success: bool,
    pub reason: String,
}

pub struct CoordinatorRunState {
    pub active_jobs: HashMap<String, CoordinatorJob>,
    pub join_set: tokio::task::JoinSet<()>,
    pub event_tx: tokio::sync::mpsc::UnboundedSender<CoordinatorJobEvent>,
    pub event_rx: tokio::sync::mpsc::UnboundedReceiver<CoordinatorJobEvent>,
    pub active_merge_jobs: HashMap<String, CoordinatorMergeJob>,
    pub merge_join_set: tokio::task::JoinSet<()>,
    pub merge_event_tx: tokio::sync::mpsc::UnboundedSender<CoordinatorMergeEvent>,
    pub merge_event_rx: tokio::sync::mpsc::UnboundedReceiver<CoordinatorMergeEvent>,
    pub events_cursor_offset: u64,
    pub last_heartbeat_log_at: Option<std::time::Instant>,
    pub heartbeat_updates_since_log: usize,
    pub dispatch_retry_not_before: HashMap<String, std::time::Instant>,
    pub dispatched_total_run: usize,
    pub dispatch_limit_event_emitted: bool,
}

pub trait PhaseExecutor {
    fn run_phase(
        &self,
        task: &serde_json::Value,
        mode: &str,
        coordinator_tool_override: Option<&str>,
        max_attempts: usize,
    ) -> Result<std::result::Result<String, String>>;
}

impl CoordinatorRunState {
    pub fn new() -> Self {
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
        let (merge_event_tx, merge_event_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            active_jobs: HashMap::new(),
            join_set: tokio::task::JoinSet::new(),
            event_tx,
            event_rx,
            active_merge_jobs: HashMap::new(),
            merge_join_set: tokio::task::JoinSet::new(),
            merge_event_tx,
            merge_event_rx,
            events_cursor_offset: 0,
            last_heartbeat_log_at: None,
            heartbeat_updates_since_log: 0,
            dispatch_retry_not_before: HashMap::new(),
            dispatched_total_run: 0,
            dispatch_limit_event_emitted: false,
        }
    }
}

pub fn parse_review_verdict(output: &str) -> Option<ReviewVerdict> {
    for line in output.lines().rev() {
        let trimmed = line.trim();
        if let Some(raw) = trimmed.strip_prefix("REVIEW_VERDICT:") {
            let verdict = raw.trim().to_ascii_uppercase();
            if verdict == "OK" {
                return Some(ReviewVerdict::Ok);
            }
            if verdict == "CHANGES_REQUESTED" {
                return Some(ReviewVerdict::ChangesRequested);
            }
            return None;
        }
    }
    None
}

fn git_status_clean(worktree: &Path) -> Result<bool> {
    let output = std::process::Command::new("git")
        .current_dir(worktree)
        .args(["status", "--porcelain"])
        .output()
        .map_err(|e| MaccError::Io {
            path: worktree.to_string_lossy().into(),
            action: "check git status for review pre/post check".into(),
            source: e,
        })?;
    Ok(output.status.success() && String::from_utf8_lossy(&output.stdout).trim().is_empty())
}

fn git_head_commit(worktree: &Path) -> Result<String> {
    let output = std::process::Command::new("git")
        .current_dir(worktree)
        .args(["rev-parse", "HEAD"])
        .output()
        .map_err(|e| MaccError::Io {
            path: worktree.to_string_lossy().into(),
            action: "read git head for review checks".into(),
            source: e,
        })?;
    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "Failed to resolve HEAD for review checks in {}",
            worktree.display()
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_ahead_count(worktree: &Path, base: &str) -> Result<usize> {
    let range = format!("{}..HEAD", base);
    let output = std::process::Command::new("git")
        .current_dir(worktree)
        .args(["rev-list", "--count", &range])
        .output()
        .map_err(|e| MaccError::Io {
            path: worktree.to_string_lossy().into(),
            action: "count ahead commits for review checks".into(),
            source: e,
        })?;
    if !output.status.success() {
        return Ok(0);
    }
    let raw = String::from_utf8_lossy(&output.stdout);
    Ok(raw.trim().parse::<usize>().unwrap_or(0))
}

pub fn run_phase<E: PhaseExecutor>(
    executor: &E,
    task: &serde_json::Value,
    mode: &str,
    coordinator_tool_override: Option<&str>,
    max_attempts: usize,
) -> Result<std::result::Result<String, String>> {
    executor.run_phase(task, mode, coordinator_tool_override, max_attempts)
}

pub fn run_review_phase<E: PhaseExecutor>(
    executor: &E,
    task: &serde_json::Value,
    coordinator_tool_override: Option<&str>,
    max_attempts: usize,
) -> Result<std::result::Result<ReviewVerdict, String>> {
    let task_id = task
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let worktree_path = task
        .get("worktree")
        .and_then(|w| w.get("worktree_path"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let base_branch = task
        .get("worktree")
        .and_then(|w| w.get("base_branch"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("master");
    if task_id.is_empty() || worktree_path.is_empty() {
        return Ok(Err(
            "review cannot run: missing task id or worktree path".to_string()
        ));
    }
    let worktree = PathBuf::from(worktree_path);
    let clean_before = git_status_clean(&worktree)?;
    if !clean_before {
        return Ok(Err(format!(
            "review precheck failed for task {}: worktree not clean before review",
            task_id
        )));
    }
    let ahead = git_ahead_count(&worktree, base_branch)?;
    if ahead == 0 {
        return Ok(Err(format!(
            "review precheck failed for task {}: no committed diff to review against base '{}'",
            task_id, base_branch
        )));
    }
    let head_before = git_head_commit(&worktree)?;
    let phase = run_phase(
        executor,
        task,
        "review",
        coordinator_tool_override,
        max_attempts,
    )?;
    let output = match phase {
        Ok(out) => out,
        Err(reason) => return Ok(Err(reason)),
    };
    let clean_after = git_status_clean(&worktree)?;
    if !clean_after {
        return Ok(Err(format!(
            "review postcheck failed for task {}: worktree not clean after review",
            task_id
        )));
    }
    let head_after = git_head_commit(&worktree)?;
    if head_after != head_before {
        return Ok(Err(format!(
            "review postcheck failed for task {}: review changed commit {} -> {}",
            task_id, head_before, head_after
        )));
    }
    let Some(verdict) = parse_review_verdict(&output) else {
        return Ok(Err(format!(
            "review verdict parse failed for task {}: missing final REVIEW_VERDICT line",
            task_id
        )));
    };
    Ok(Ok(verdict))
}

pub fn spawn_performer_job(
    executable_path: &Path,
    repo_root: &Path,
    task_id: &str,
    worktree_path: &Path,
    event_tx: &tokio::sync::mpsc::UnboundedSender<CoordinatorJobEvent>,
    join_set: &mut tokio::task::JoinSet<()>,
    phase_timeout_seconds: usize,
) -> Result<Option<i64>> {
    let mut run_cmd = tokio::process::Command::new(executable_path);
    let event_source = format!(
        "coordinator-worktree:{}:{}",
        task_id,
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let events_file = repo_root
        .join(".macc")
        .join("log")
        .join("coordinator")
        .join("events.jsonl");
    run_cmd
        .current_dir(repo_root)
        .env("COORD_EVENTS_FILE", events_file.to_string_lossy().to_string())
        .env(
            "COORDINATOR_RUN_ID",
            std::env::var("COORDINATOR_RUN_ID").unwrap_or_else(|_| {
                format!(
                    "run-{}-{}",
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
                    std::process::id()
                )
            }),
        )
        .env("MACC_EVENT_SOURCE", event_source.clone())
        .env("MACC_EVENT_TASK_ID", task_id)
        .arg("--cwd")
        .arg(repo_root)
        .arg("worktree")
        .arg("run")
        .arg(worktree_path.to_string_lossy().to_string());
    let mut child = run_cmd.spawn().map_err(|e| MaccError::Io {
        path: worktree_path.to_string_lossy().into(),
        action: "spawn performer process".into(),
        source: e,
    })?;
    let pid = child.id().map(|v| v as i64);
    let task_id_owned = task_id.to_string();
    let event_source_owned = event_source.clone();
    let tx = event_tx.clone();
    join_set.spawn(async move {
        let (success, status_text, timed_out) = if phase_timeout_seconds > 0 {
            match tokio::time::timeout(
                std::time::Duration::from_secs(phase_timeout_seconds as u64),
                child.wait(),
            )
            .await
            {
                Ok(Ok(status)) => (status.success(), status.to_string(), false),
                Ok(Err(err)) => (false, err.to_string(), false),
                Err(_) => {
                    let _ = child.kill().await;
                    (false, "timeout".to_string(), true)
                }
            }
        } else {
            match child.wait().await {
                Ok(status) => (status.success(), status.to_string(), false),
                Err(err) => (false, err.to_string(), false),
            }
        };
        let mut error_code = None;
        let mut error_origin = None;
        let mut error_message = None;
        if !success {
            if let Some(details) =
                read_last_error_details(&events_file, &task_id_owned, &event_source_owned)
            {
                error_code = details.error_code;
                error_origin = details.error_origin;
                error_message = details.error_message;
            }
        }
        let _ = tx.send(CoordinatorJobEvent {
            task_id: task_id_owned,
            success,
            status_text,
            timed_out,
            error_code,
            error_origin,
            error_message,
        });
    });
    Ok(pid)
}

#[derive(Debug, Clone)]
struct ErrorDetails {
    error_code: Option<String>,
    error_origin: Option<String>,
    error_message: Option<String>,
}

fn read_last_error_details(events_file: &Path, task_id: &str, event_source: &str) -> Option<ErrorDetails> {
    let content = std::fs::read_to_string(events_file).ok()?;
    let mut failed_candidate: Option<ErrorDetails> = None;
    let mut saw_terminal_success_before_failed = false;
    for line in content.lines().rev() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(event) = serde_json::from_str::<serde_json::Value>(trimmed) else {
            continue;
        };
        let Some(event_task_id) = event.get("task_id").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if event_task_id != task_id {
            continue;
        }
        let source = event
            .get("source")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if !event_source.is_empty() && source != event_source {
            continue;
        }
        let event_type = event
            .get("type")
            .or_else(|| event.get("event"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let status = event
            .get("status")
            .or_else(|| event.get("state"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let payload = event.get("payload").unwrap_or(&serde_json::Value::Null);
        let payload = normalize_payload_object(payload);
        let is_terminal_success = event_type == "commit_created"
            || (event_type == "phase_result"
                && status == "done"
                && payload
                    .get("attempt")
                    .and_then(serde_json::Value::as_i64)
                    .is_none());
        if is_terminal_success && failed_candidate.is_some() {
            saw_terminal_success_before_failed = true;
            continue;
        }
        if event_type != "failed" && !(event_type == "phase_result" && status == "failed") {
            continue;
        }
        let error_code = payload
            .get("error_code")
            .or_else(|| payload.get("code"))
            .and_then(serde_json::Value::as_str)
            .map(|v| v.to_string());
        let error_origin = payload
            .get("origin")
            .and_then(serde_json::Value::as_str)
            .map(|v| v.to_string());
        let error_message = payload
            .get("message")
            .or_else(|| payload.get("reason"))
            .or_else(|| payload.get("error"))
            .and_then(serde_json::Value::as_str)
            .map(|v| v.to_string());
        if failed_candidate.is_none() {
            failed_candidate = Some(ErrorDetails {
                error_code,
                error_origin,
                error_message,
            });
        }
    }
    if saw_terminal_success_before_failed {
        return None;
    }
    failed_candidate.and_then(|details| {
        if details.error_code.is_some() || details.error_origin.is_some() || details.error_message.is_some() {
            Some(details)
        } else {
            None
        }
    })
}

fn normalize_payload_object(payload: &serde_json::Value) -> serde_json::Value {
    if payload.is_object() {
        return payload.clone();
    }
    if let Some(raw) = payload.as_str() {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(raw) {
            if parsed.is_object() {
                return parsed;
            }
        }
    }
    serde_json::json!({})
}

pub async fn spawn_merge_job<F>(
    task_id: &str,
    event_tx: &tokio::sync::mpsc::UnboundedSender<CoordinatorMergeEvent>,
    join_set: &mut tokio::task::JoinSet<()>,
    merge_timeout_seconds: usize,
    merge_runner: F,
) -> Result<()>
where
    F: FnOnce() -> Result<std::result::Result<(), String>> + Send + 'static,
{
    let merge_timeout_seconds = if merge_timeout_seconds > 0 {
        merge_timeout_seconds as u64
    } else {
        std::env::var("COORDINATOR_MERGE_JOB_TIMEOUT_SECONDS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0)
    };
    let task_id_owned = task_id.to_string();
    let tx = event_tx.clone();
    join_set.spawn(async move {
        let worker = tokio::task::spawn_blocking(merge_runner);
        let outcome = if merge_timeout_seconds > 0 {
            match tokio::time::timeout(
                std::time::Duration::from_secs(merge_timeout_seconds),
                worker,
            )
            .await
            {
                Ok(joined) => joined,
                Err(_) => {
                    let _ = tx.send(CoordinatorMergeEvent {
                        task_id: task_id_owned,
                        success: false,
                        reason: format!(
                            "failure:local_merge step=timeout timeout_s={}",
                            merge_timeout_seconds
                        ),
                    });
                    return;
                }
            }
        } else {
            worker.await
        };
        let evt = match outcome {
            Ok(Ok(Ok(()))) => CoordinatorMergeEvent {
                task_id: task_id_owned,
                success: true,
                reason: "merge completed".to_string(),
            },
            Ok(Ok(Err(reason))) => CoordinatorMergeEvent {
                task_id: task_id_owned,
                success: false,
                reason,
            },
            Ok(Err(err)) => CoordinatorMergeEvent {
                task_id: task_id_owned,
                success: false,
                reason: err.to_string(),
            },
            Err(join_err) => CoordinatorMergeEvent {
                task_id: task_id_owned,
                success: false,
                reason: format!("merge worker join error: {}", join_err),
            },
        };
        let _ = tx.send(evt);
    });
    Ok(())
}

pub fn terminate_active_jobs(state: &CoordinatorRunState) -> Vec<(String, i64)> {
    let mut terminated = Vec::new();
    for (task_id, job) in &state.active_jobs {
        let Some(pid) = job.pid else {
            continue;
        };
        let _ = std::process::Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status();
        let _ = std::process::Command::new("kill")
            .arg("-TERM")
            .arg(format!("-{}", pid))
            .status();
        terminated.push((task_id.clone(), pid));
    }
    terminated
}
