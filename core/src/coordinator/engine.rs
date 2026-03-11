use super::{RuntimeStatus, WorkflowState};
use crate::config::{CanonicalConfig, CoordinatorConfig};
use crate::coordinator::control_plane::CoordinatorLog;
use crate::coordinator::runtime::{
    process_branch_cleanup_queue, terminate_active_jobs, CoordinatorRunState,
};
use crate::coordinator::state_runtime::{
    cleanup_dead_runtime_tasks, clear_coordinator_pause_file, coordinator_pause_file_path,
    resume_paused_task_integrate, set_task_paused_for_integrate, write_coordinator_pause_file,
};
use crate::coordinator_storage::{
    coordinator_storage_bootstrap_sqlite_from_json, coordinator_storage_export_sqlite_to_json,
    CoordinatorStorageMode,
};
use crate::{MaccError, Result};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhaseTransition {
    pub mode: &'static str,
    pub next_state: WorkflowState,
    pub runtime_phase: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdvancePlan {
    RunPhase(PhaseTransition),
    Merge,
    Noop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewVerdict {
    Ok,
    ChangesRequested,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkflowEvent {
    PhaseSucceeded(&'static str),
    PhaseFailed(&'static str),
    ReviewChangesRequested,
    MergeSucceeded,
    MergeFailed,
}

#[derive(Debug, Clone)]
pub struct AdvanceResult {
    pub progressed: bool,
    pub blocked_merge: Option<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorCounts {
    pub total: usize,
    pub todo: usize,
    pub active: usize,
    pub blocked: usize,
    pub merged: usize,
}

#[derive(Debug, Clone)]
pub struct DispatchClaimUpdate {
    pub task_id: String,
    pub tool: String,
    pub worktree_path: String,
    pub branch: String,
    pub base_branch: String,
    pub last_commit: String,
    pub session_id: String,
    pub pid: Option<i64>,
    pub phase: String,
    pub now: String,
}

#[derive(Debug, Clone)]
pub struct JobCompletionInput {
    pub success: bool,
    pub attempt: usize,
    pub max_attempts: usize,
    pub timed_out: bool,
    pub phase_timeout_seconds: usize,
    pub elapsed_seconds: u64,
    pub status_text: String,
    pub error_code: Option<String>,
    pub error_origin: Option<String>,
    pub error_message: Option<String>,
    pub auto_retry_error_codes: Vec<String>,
    pub auto_retry_max: usize,
}

#[derive(Debug, Clone)]
pub struct JobCompletionResult {
    pub should_retry: bool,
    pub status_label: &'static str,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub enum AdvanceTaskAction {
    RunPhase {
        task_id: String,
        mode: &'static str,
        transition: PhaseTransition,
    },
    QueueMerge {
        task_id: String,
        branch: String,
        base: String,
    },
}

#[derive(Debug, Clone)]
pub struct DeadRuntimeCleanupEntry {
    pub task_id: String,
    pub old_state: String,
    pub phase: String,
    pub pid: i64,
    pub new_state: String,
}

#[derive(Debug, Clone)]
pub struct ControlPlaneLoopConfig {
    pub timeout: Option<Duration>,
    pub max_no_progress_cycles: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPlaneDecision {
    Continue,
    Complete,
}

pub struct CoordinatorRunController {
    cfg: ControlPlaneLoopConfig,
    started: Instant,
    no_progress_cycles: usize,
    previous_counts: Option<CoordinatorCounts>,
}

#[async_trait]
pub trait ControlPlaneBackend {
    async fn on_cycle_start(&mut self, cycle: usize) -> Result<()>;
    async fn monitor_active_jobs(&mut self) -> Result<()>;
    async fn monitor_merge_jobs(&mut self) -> Result<Option<(String, String)>>;
    async fn on_blocked_merge(&mut self, task_id: &str, reason: &str) -> Result<()>;
    async fn advance_tasks(&mut self) -> Result<AdvanceResult>;
    async fn dispatch_ready_tasks(&mut self) -> Result<usize>;
    async fn on_cycle_end(
        &mut self,
        cycle: usize,
        advance: &AdvanceResult,
        dispatched: usize,
    ) -> Result<CoordinatorCounts>;
    async fn sleep_between_cycles(&mut self) -> Result<()>;
    fn should_terminate_run(&self, _counts: &CoordinatorCounts) -> bool {
        false
    }
}

pub fn plan_advance(state: WorkflowState) -> AdvancePlan {
    match state {
        WorkflowState::InProgress => AdvancePlan::RunPhase(PhaseTransition {
            mode: "review",
            next_state: WorkflowState::PrOpen,
            runtime_phase: "review",
        }),
        WorkflowState::PrOpen => AdvancePlan::RunPhase(PhaseTransition {
            mode: "integrate",
            next_state: WorkflowState::Queued,
            runtime_phase: "integrate",
        }),
        WorkflowState::ChangesRequested => AdvancePlan::RunPhase(PhaseTransition {
            mode: "fix",
            next_state: WorkflowState::PrOpen,
            runtime_phase: "fix",
        }),
        WorkflowState::Queued => AdvancePlan::Merge,
        _ => AdvancePlan::Noop,
    }
}

fn transition_workflow_state(from: WorkflowState, event: WorkflowEvent) -> Result<WorkflowState> {
    let to = match (from, event) {
        (WorkflowState::InProgress, WorkflowEvent::PhaseSucceeded("review")) => {
            WorkflowState::PrOpen
        }
        (WorkflowState::InProgress, WorkflowEvent::ReviewChangesRequested) => {
            WorkflowState::ChangesRequested
        }
        (WorkflowState::PrOpen, WorkflowEvent::PhaseSucceeded("integrate")) => {
            WorkflowState::Queued
        }
        (WorkflowState::ChangesRequested, WorkflowEvent::PhaseSucceeded("fix")) => {
            WorkflowState::PrOpen
        }
        (WorkflowState::InProgress, WorkflowEvent::PhaseFailed("review"))
        | (WorkflowState::PrOpen, WorkflowEvent::PhaseFailed("integrate"))
        | (WorkflowState::ChangesRequested, WorkflowEvent::PhaseFailed("fix"))
        | (WorkflowState::Queued, WorkflowEvent::MergeFailed) => WorkflowState::Blocked,
        (WorkflowState::Queued, WorkflowEvent::MergeSucceeded) => WorkflowState::Merged,
        _ => {
            return Err(MaccError::Validation(format!(
                "Invalid coordinator FSM transition: from={} event={:?}",
                from.as_str(),
                event
            )));
        }
    };

    if !super::is_valid_workflow_transition(from, to) {
        return Err(MaccError::Validation(format!(
            "Coordinator FSM produced invalid workflow transition {} -> {}",
            from.as_str(),
            to.as_str()
        )));
    }

    Ok(to)
}

fn task_workflow_state(task: &Value) -> Result<WorkflowState> {
    task.get("state")
        .and_then(Value::as_str)
        .unwrap_or("todo")
        .parse::<WorkflowState>()
        .map_err(MaccError::Validation)
}

fn tasks_array_mut(registry: &mut Value) -> Result<&mut Vec<Value>> {
    registry
        .get_mut("tasks")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| MaccError::Validation("Registry missing .tasks array".into()))
}

fn find_task_mut<'a>(registry: &'a mut Value, task_id: &str) -> Result<&'a mut Value> {
    tasks_array_mut(registry)?
        .iter_mut()
        .find(|task| {
            task.get("id")
                .and_then(Value::as_str)
                .map(|id| id == task_id)
                .unwrap_or(false)
        })
        .ok_or_else(|| MaccError::Validation(format!("Task '{}' not found in registry", task_id)))
}

pub fn apply_dispatch_claim_in_registry(
    registry: &mut Value,
    update: &DispatchClaimUpdate,
) -> Result<()> {
    let task = find_task_mut(registry, &update.task_id)?;
    apply_dispatch_claim(task, update);
    Ok(())
}

pub fn apply_dispatch_pid_in_registry(
    registry: &mut Value,
    task_id: &str,
    pid: Option<i64>,
) -> Result<()> {
    let task = find_task_mut(registry, task_id)?;
    apply_dispatch_pid(task, pid);
    Ok(())
}

pub fn build_advance_actions(
    registry: &Value,
    active_merge_jobs: &HashSet<String>,
) -> Result<Vec<AdvanceTaskAction>> {
    let tasks = registry
        .get("tasks")
        .and_then(Value::as_array)
        .ok_or_else(|| MaccError::Validation("Registry missing .tasks array".into()))?;
    let mut actions = Vec::new();
    for task in tasks {
        let task_id = task
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let workflow_raw = task
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("todo")
            .to_string();
        let workflow_state = workflow_raw.parse::<WorkflowState>().ok();
        match workflow_state
            .map(plan_advance)
            .unwrap_or(AdvancePlan::Noop)
        {
            AdvancePlan::RunPhase(transition) => {
                actions.push(AdvanceTaskAction::RunPhase {
                    task_id,
                    mode: transition.mode,
                    transition,
                });
            }
            AdvancePlan::Merge => {
                if active_merge_jobs.contains(&task_id) {
                    continue;
                }
                let branch = task
                    .get("worktree")
                    .and_then(|w| w.get("branch"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if branch.is_empty() {
                    continue;
                }
                let base = task
                    .get("worktree")
                    .and_then(|w| w.get("base_branch"))
                    .and_then(Value::as_str)
                    .unwrap_or("master")
                    .to_string();
                actions.push(AdvanceTaskAction::QueueMerge {
                    task_id,
                    branch,
                    base,
                });
            }
            AdvancePlan::Noop => {}
        }
    }
    Ok(actions)
}

pub fn apply_phase_outcome_in_registry(
    registry: &mut Value,
    task_id: &str,
    mode: &'static str,
    transition: PhaseTransition,
    review_verdict: Option<ReviewVerdict>,
    phase_error: Option<&str>,
    now: &str,
) -> Result<()> {
    let task = find_task_mut(registry, task_id)?;
    if let Some(reason) = phase_error {
        return apply_phase_failure(task, mode, reason, now);
    }
    if mode == "review" {
        let verdict = review_verdict.ok_or_else(|| {
            MaccError::Validation(format!(
                "Missing review verdict for task '{}' during review phase",
                task_id
            ))
        })?;
        let next = apply_review_phase_success(task, verdict, now)?;
        if next == WorkflowState::PrOpen
            && task
                .get("pr_url")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .is_empty()
        {
            let branch = task
                .get("worktree")
                .and_then(|w| w.get("branch"))
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            task["pr_url"] = Value::String(format!("local://{}", branch));
        }
        return Ok(());
    }
    apply_phase_success(task, transition, now)?;
    if transition.next_state == WorkflowState::PrOpen
        && task
            .get("pr_url")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .is_empty()
    {
        let branch = task
            .get("worktree")
            .and_then(|w| w.get("branch"))
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        task["pr_url"] = Value::String(format!("local://{}", branch));
    }
    Ok(())
}

pub fn apply_job_completion_in_registry(
    registry: &mut Value,
    task_id: &str,
    input: &JobCompletionInput,
    now: &str,
) -> Result<JobCompletionResult> {
    let task = find_task_mut(registry, task_id)?;
    Ok(apply_job_completion(task, input, now))
}

pub fn apply_merge_result_in_registry(
    registry: &mut Value,
    task_id: &str,
    success: bool,
    reason: &str,
    now: &str,
) -> Result<()> {
    let task = find_task_mut(registry, task_id)?;
    if success {
        apply_merge_success(task, now)
    } else {
        apply_merge_failure(task, reason, now)
    }
}

pub fn ensure_runtime_object(task: &mut Value) {
    if !task
        .get("task_runtime")
        .map(Value::is_object)
        .unwrap_or(false)
    {
        task["task_runtime"] = json!({});
    }
}

pub fn apply_dispatch_claim(task: &mut Value, update: &DispatchClaimUpdate) {
    task["state"] = Value::String(WorkflowState::Claimed.as_str().to_string());
    task["tool"] = Value::String(update.tool.clone());
    task["worktree"] = json!({
        "worktree_path": update.worktree_path,
        "branch": update.branch,
        "base_branch": update.base_branch,
        "last_commit": update.last_commit,
        "session_id": update.session_id,
    });
    ensure_runtime_object(task);
    task["task_runtime"]["status"] = Value::String(RuntimeStatus::Running.as_str().to_string());
    task["task_runtime"]["current_phase"] = Value::String(update.phase.clone());
    task["task_runtime"]["started_at"] = Value::String(update.now.clone());
    task["task_runtime"]["pid"] = update.pid.map(Value::from).unwrap_or(Value::Null);
    task["state_changed_at"] = Value::String(update.now.clone());
}

pub fn apply_dispatch_pid(task: &mut Value, pid: Option<i64>) {
    ensure_runtime_object(task);
    task["task_runtime"]["pid"] = pid.map(Value::from).unwrap_or(Value::Null);
}

pub fn apply_phase_success(task: &mut Value, transition: PhaseTransition, now: &str) -> Result<()> {
    let from = task_workflow_state(task)?;
    let to = transition_workflow_state(from, WorkflowEvent::PhaseSucceeded(transition.mode))?;
    if to != transition.next_state {
        return Err(MaccError::Validation(format!(
            "Coordinator FSM mismatch for mode='{}': expected next={} got {}",
            transition.mode,
            transition.next_state.as_str(),
            to.as_str()
        )));
    }
    task["state"] = Value::String(to.as_str().to_string());
    ensure_runtime_object(task);
    task["task_runtime"]["status"] = Value::String(RuntimeStatus::PhaseDone.as_str().to_string());
    task["task_runtime"]["current_phase"] = Value::String(transition.runtime_phase.to_string());
    task["task_runtime"]["pid"] = Value::Null;
    task["state_changed_at"] = Value::String(now.to_string());
    Ok(())
}

pub fn apply_review_phase_success(
    task: &mut Value,
    verdict: ReviewVerdict,
    now: &str,
) -> Result<WorkflowState> {
    let from = task_workflow_state(task)?;
    let to = match verdict {
        ReviewVerdict::Ok => {
            transition_workflow_state(from, WorkflowEvent::PhaseSucceeded("review"))?
        }
        ReviewVerdict::ChangesRequested => {
            transition_workflow_state(from, WorkflowEvent::ReviewChangesRequested)?
        }
    };
    task["state"] = Value::String(to.as_str().to_string());
    ensure_runtime_object(task);
    task["task_runtime"]["status"] = Value::String(RuntimeStatus::PhaseDone.as_str().to_string());
    task["task_runtime"]["current_phase"] = Value::String("review".to_string());
    task["task_runtime"]["pid"] = Value::Null;
    task["state_changed_at"] = Value::String(now.to_string());
    Ok(to)
}

pub fn apply_phase_failure(
    task: &mut Value,
    phase_mode: &'static str,
    reason: &str,
    now: &str,
) -> Result<()> {
    let from = task_workflow_state(task)?;
    let to = transition_workflow_state(from, WorkflowEvent::PhaseFailed(phase_mode))?;
    task["state"] = Value::String(to.as_str().to_string());
    ensure_runtime_object(task);
    task["task_runtime"]["status"] = Value::String(RuntimeStatus::Failed.as_str().to_string());
    task["task_runtime"]["current_phase"] = Value::String(phase_mode.to_string());
    task["task_runtime"]["last_error"] = Value::String(reason.to_string());
    task["task_runtime"]["pid"] = Value::Null;
    task["state_changed_at"] = Value::String(now.to_string());
    Ok(())
}

pub fn apply_merge_success(task: &mut Value, now: &str) -> Result<()> {
    let from = task_workflow_state(task)?;
    let to = transition_workflow_state(from, WorkflowEvent::MergeSucceeded)?;
    task["state"] = Value::String(to.as_str().to_string());
    ensure_runtime_object(task);
    task["task_runtime"]["status"] = Value::String(RuntimeStatus::Idle.as_str().to_string());
    task["task_runtime"]["pid"] = Value::Null;
    task["state_changed_at"] = Value::String(now.to_string());
    Ok(())
}

pub fn apply_merge_failure(task: &mut Value, reason: &str, now: &str) -> Result<()> {
    let from = task_workflow_state(task)?;
    let to = transition_workflow_state(from, WorkflowEvent::MergeFailed)?;
    task["state"] = Value::String(to.as_str().to_string());
    ensure_runtime_object(task);
    task["task_runtime"]["status"] = Value::String(RuntimeStatus::Paused.as_str().to_string());
    task["task_runtime"]["current_phase"] = Value::String("integrate".to_string());
    task["task_runtime"]["last_error"] = Value::String(reason.to_string());
    task["task_runtime"]["pid"] = Value::Null;
    task["state_changed_at"] = Value::String(now.to_string());
    Ok(())
}

pub fn apply_job_completion(
    task: &mut Value,
    input: &JobCompletionInput,
    now: &str,
) -> JobCompletionResult {
    ensure_runtime_object(task);
    let error_code = input
        .error_code
        .clone()
        .unwrap_or_else(|| "E101".to_string());
    let error_origin = input
        .error_origin
        .clone()
        .unwrap_or_else(|| "runner".to_string());
    let error_message = input
        .error_message
        .clone()
        .unwrap_or_else(|| input.status_text.clone());
    if input.attempt == 0 || input.max_attempts == 0 {
        task["state"] = Value::String(WorkflowState::Blocked.as_str().to_string());
        task["task_runtime"]["status"] = Value::String(RuntimeStatus::Failed.as_str().to_string());
        task["task_runtime"]["pid"] = Value::Null;
        let detail = "performer completion received with invalid attempt counters".to_string();
        task["task_runtime"]["last_error_code"] = Value::String("E901".to_string());
        task["task_runtime"]["last_error_origin"] = Value::String("coordinator".to_string());
        task["task_runtime"]["last_error_message"] = Value::String(detail.clone());
        task["task_runtime"]["last_error"] = Value::String(detail.clone());
        task["state_changed_at"] = Value::String(now.to_string());
        return JobCompletionResult {
            should_retry: false,
            status_label: "failed",
            detail,
        };
    }

    if input.status_text.is_empty() {
        task["state"] = Value::String(WorkflowState::Blocked.as_str().to_string());
        task["task_runtime"]["status"] = Value::String(RuntimeStatus::Failed.as_str().to_string());
        task["task_runtime"]["pid"] = Value::Null;
        let detail = "performer completion received without status detail".to_string();
        task["task_runtime"]["last_error_code"] = Value::String("E901".to_string());
        task["task_runtime"]["last_error_origin"] = Value::String("coordinator".to_string());
        task["task_runtime"]["last_error_message"] = Value::String(detail.clone());
        task["task_runtime"]["last_error"] = Value::String(detail.clone());
        task["state_changed_at"] = Value::String(now.to_string());
        return JobCompletionResult {
            should_retry: false,
            status_label: "failed",
            detail,
        };
    }

    if input.success {
        task["state"] = Value::String(WorkflowState::InProgress.as_str().to_string());
        task["task_runtime"]["status"] =
            Value::String(RuntimeStatus::PhaseDone.as_str().to_string());
        task["task_runtime"]["current_phase"] = Value::String("dev".to_string());
        task["task_runtime"]["pid"] = Value::Null;
        task["state_changed_at"] = Value::String(now.to_string());
        return JobCompletionResult {
            should_retry: false,
            status_label: "phase_done",
            detail: input.status_text.clone(),
        };
    }

    if input.attempt < input.max_attempts {
        task["state"] = Value::String(WorkflowState::Claimed.as_str().to_string());
        task["task_runtime"]["status"] = Value::String(RuntimeStatus::Running.as_str().to_string());
        task["task_runtime"]["current_phase"] = Value::String("dev".to_string());
        task["task_runtime"]["pid"] = Value::Null;
        let reason = if input.timed_out {
            format!(
                "performer timed out after {}s on attempt {} (elapsed={}s)",
                input.phase_timeout_seconds, input.attempt, input.elapsed_seconds
            )
        } else {
            format!(
                "performer failed on attempt {}: {}",
                input.attempt, input.status_text
            )
        };
        task["task_runtime"]["last_error_code"] = Value::String(error_code.clone());
        task["task_runtime"]["last_error_origin"] = Value::String(error_origin.clone());
        task["task_runtime"]["last_error_message"] = Value::String(error_message.clone());
        task["task_runtime"]["last_error"] = Value::String(reason.clone());
        task["state_changed_at"] = Value::String(now.to_string());
        return JobCompletionResult {
            should_retry: true,
            status_label: "retry",
            detail: reason,
        };
    }

    let reason = if input.timed_out {
        format!(
            "performer timed out after {}s (max attempts reached: {}, elapsed={}s)",
            input.phase_timeout_seconds, input.max_attempts, input.elapsed_seconds
        )
    } else {
        format!(
            "performer failed after {} attempts: {}",
            input.attempt, input.status_text
        )
    };
    let retries_total = task_retry_count(task);
    if should_auto_retry_error_code(
        &error_code,
        &input.auto_retry_error_codes,
        input.auto_retry_max,
        retries_total,
    ) {
        increment_task_retries(task);
        task["state"] = Value::String(WorkflowState::Todo.as_str().to_string());
        task["task_runtime"]["status"] = Value::String(RuntimeStatus::Idle.as_str().to_string());
        task["task_runtime"]["pid"] = Value::Null;
        task["task_runtime"]["current_phase"] = Value::String("dev".to_string());
        task["task_runtime"]["last_error_code"] = Value::String(error_code.clone());
        task["task_runtime"]["last_error_origin"] = Value::String(error_origin.clone());
        task["task_runtime"]["last_error_message"] = Value::String(error_message.clone());
        task["task_runtime"]["last_error"] = Value::String(reason.clone());
        task["state_changed_at"] = Value::String(now.to_string());
        return JobCompletionResult {
            should_retry: false,
            status_label: "auto_retry",
            detail: format!("auto-retry scheduled for error code {}", error_code),
        };
    }

    task["state"] = Value::String(WorkflowState::Blocked.as_str().to_string());
    task["task_runtime"]["status"] = Value::String(RuntimeStatus::Failed.as_str().to_string());
    task["task_runtime"]["pid"] = Value::Null;
    task["task_runtime"]["last_error_code"] = Value::String(error_code);
    task["task_runtime"]["last_error_origin"] = Value::String(error_origin);
    task["task_runtime"]["last_error_message"] = Value::String(error_message);
    task["task_runtime"]["last_error"] = Value::String(reason.clone());
    task["state_changed_at"] = Value::String(now.to_string());
    JobCompletionResult {
        should_retry: false,
        status_label: "failed",
        detail: reason,
    }
}

fn task_retry_count(task: &Value) -> usize {
    task.get("task_runtime")
        .and_then(|v| v.get("retries"))
        .and_then(Value::as_u64)
        .or_else(|| {
            task.get("task_runtime")
                .and_then(|v| v.get("metrics"))
                .and_then(|v| v.get("retries"))
                .and_then(Value::as_u64)
        })
        .unwrap_or(0) as usize
}

fn increment_task_retries(task: &mut Value) -> usize {
    ensure_runtime_object(task);
    if !task
        .get("task_runtime")
        .and_then(|v| v.get("metrics"))
        .map(Value::is_object)
        .unwrap_or(false)
    {
        task["task_runtime"]["metrics"] = json!({});
    }
    let current = task_retry_count(task);
    let next = current.saturating_add(1);
    task["task_runtime"]["metrics"]["retries"] = Value::from(next as i64);
    task["task_runtime"]["retries"] = Value::from(next as i64);
    next
}

fn should_auto_retry_error_code(
    code: &str,
    list: &[String],
    max_retries: usize,
    current_retries: usize,
) -> bool {
    if code.is_empty() || max_retries == 0 {
        return false;
    }
    if current_retries >= max_retries {
        return false;
    }
    list.iter().any(|entry| entry.trim() == code)
}

pub fn cleanup_dead_runtime_tasks_in_registry_with<F>(
    registry: &mut Value,
    now: &str,
    heartbeat_grace_seconds: i64,
    mut is_pid_running: F,
) -> Result<Vec<DeadRuntimeCleanupEntry>>
where
    F: FnMut(i64) -> bool,
{
    let now_ts = chrono::DateTime::parse_from_rfc3339(now)
        .ok()
        .map(|dt| dt.timestamp())
        .unwrap_or_default();
    let mut cleaned = Vec::new();
    let tasks = registry
        .get_mut("tasks")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| MaccError::Validation("Registry missing .tasks array".into()))?;
    for task in tasks.iter_mut() {
        ensure_runtime_object(task);
        let Some(pid) = task["task_runtime"]["pid"].as_i64() else {
            continue;
        };
        let runtime_status = task["task_runtime"]["status"].as_str().unwrap_or_default();
        if runtime_status != RuntimeStatus::Running.as_str() || is_pid_running(pid) {
            continue;
        }
        if heartbeat_grace_seconds > 0 {
            let within_grace = task["task_runtime"]["last_heartbeat"]
                .as_str()
                .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
                .map(|dt| now_ts.saturating_sub(dt.timestamp()) <= heartbeat_grace_seconds)
                .unwrap_or(false);
            if within_grace {
                continue;
            }
        }

        let task_id = task
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let phase = task["task_runtime"]["current_phase"]
            .as_str()
            .unwrap_or("dev")
            .to_string();
        let old_state = task
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or(WorkflowState::Todo.as_str())
            .to_string();

        task["task_runtime"]["pid"] = Value::Null;
        task["task_runtime"]["status"] = Value::String(RuntimeStatus::Stale.as_str().to_string());
        task["task_runtime"]["last_error"] =
            Value::String(format!("runtime pid {} is not running; auto-reset", pid));
        task["updated_at"] = Value::String(now.to_string());
        task["state_changed_at"] = Value::String(now.to_string());
        let new_state = if old_state == WorkflowState::Claimed.as_str() && phase == "dev" {
            task["state"] = Value::String(WorkflowState::Todo.as_str().to_string());
            task["assignee"] = Value::Null;
            WorkflowState::Todo.as_str().to_string()
        } else {
            task["state"] = Value::String(WorkflowState::Blocked.as_str().to_string());
            WorkflowState::Blocked.as_str().to_string()
        };

        cleaned.push(DeadRuntimeCleanupEntry {
            task_id,
            old_state,
            phase,
            pid,
            new_state,
        });
    }
    Ok(cleaned)
}

impl CoordinatorRunController {
    pub fn new(cfg: ControlPlaneLoopConfig) -> Self {
        Self {
            cfg,
            started: Instant::now(),
            no_progress_cycles: 0,
            previous_counts: None,
        }
    }

    pub fn on_cycle_counts(&mut self, counts: CoordinatorCounts) -> Result<ControlPlaneDecision> {
        if counts.todo == 0 && counts.active == 0 {
            if counts.blocked > 0 {
                return Err(MaccError::Validation(format!(
                    "Coordinator run finished with blocked tasks: {}. Run `macc coordinator status`, then `macc coordinator unlock --all`, and inspect logs with `macc logs tail --component coordinator`.",
                    counts.blocked
                )));
            }
            return Ok(ControlPlaneDecision::Complete);
        }

        if counts.active > 0 {
            self.no_progress_cycles = 0;
        } else if self.previous_counts == Some(counts) {
            self.no_progress_cycles += 1;
        } else {
            self.no_progress_cycles = 0;
        }
        self.previous_counts = Some(counts);

        if self.no_progress_cycles >= self.cfg.max_no_progress_cycles {
            return Err(MaccError::Validation(format!(
                "Coordinator made no progress for {} cycles (todo={}, active={}, blocked={}). Run `macc coordinator status`, then `macc coordinator unlock --all`, and inspect logs with `macc logs tail --component coordinator`.",
                self.no_progress_cycles, counts.todo, counts.active, counts.blocked
            )));
        }

        if let Some(timeout) = self.cfg.timeout {
            if self.started.elapsed() >= timeout {
                return Err(MaccError::Validation(format!(
                    "Coordinator run timed out after {} seconds. Run `macc coordinator status` and `macc logs tail --component coordinator`.",
                    timeout.as_secs()
                )));
            }
        }

        Ok(ControlPlaneDecision::Continue)
    }
}

pub async fn run_control_plane<B: ControlPlaneBackend + ?Sized>(
    backend: &mut B,
    cfg: ControlPlaneLoopConfig,
) -> Result<()> {
    let mut controller = CoordinatorRunController::new(cfg);
    let mut cycle: usize = 0;
    loop {
        cycle += 1;
        backend.on_cycle_start(cycle).await?;

        backend.monitor_active_jobs().await?;
        if let Some((task_id, reason)) = backend.monitor_merge_jobs().await? {
            backend.on_blocked_merge(&task_id, &reason).await?;
        }

        let advance = backend.advance_tasks().await?;

        backend.monitor_active_jobs().await?;
        if let Some((task_id, reason)) = backend
            .monitor_merge_jobs()
            .await?
            .or_else(|| advance.blocked_merge.clone())
        {
            backend.on_blocked_merge(&task_id, &reason).await?;
        }

        let dispatched = backend.dispatch_ready_tasks().await?;

        if let Some((task_id, reason)) = backend.monitor_merge_jobs().await? {
            backend.on_blocked_merge(&task_id, &reason).await?;
        }

        let counts = backend.on_cycle_end(cycle, &advance, dispatched).await?;
        if backend.should_terminate_run(&counts) {
            return Ok(());
        }
        match controller.on_cycle_counts(counts) {
            Ok(ControlPlaneDecision::Continue) => {}
            Ok(ControlPlaneDecision::Complete) => return Ok(()),
            Err(err) => return Err(err),
        }

        backend.sleep_between_cycles().await?;
    }
}

struct NativeControlPlaneBackend<'a> {
    repo_root: &'a Path,
    canonical: &'a CanonicalConfig,
    coordinator: Option<&'a CoordinatorConfig>,
    env_cfg: &'a super::types::CoordinatorEnvConfig,
    logger: Option<&'a dyn CoordinatorLog>,
    prd_file: PathBuf,
    run_state: CoordinatorRunState,
    phase_runner_max_attempts: usize,
    coordinator_tool_override: Option<String>,
    phase_timeout_seconds: usize,
    last_logged_counts: Option<CoordinatorCounts>,
}

#[async_trait]
impl ControlPlaneBackend for NativeControlPlaneBackend<'_> {
    async fn on_cycle_start(&mut self, _cycle: usize) -> Result<()> {
        crate::coordinator::control_plane::sync_registry_from_prd_native(
            self.repo_root,
            &self.prd_file,
            self.logger,
        )?;
        let logger = self.logger;
        let _ = process_branch_cleanup_queue(
            self.repo_root,
            |event_type, task_id, phase, status, message, severity| {
                let _ = crate::coordinator::helpers::append_coordinator_event_with_severity(
                    self.repo_root,
                    event_type,
                    task_id,
                    phase,
                    status,
                    message,
                    severity,
                );
            },
            Some(move |msg| {
                if let Some(log) = logger {
                    let _ = log.note(msg);
                }
            }),
        )?;
        let cleaned = if let Some(log) = self.logger {
            let note = |line: String| {
                let _ = log.note(line);
            };
            cleanup_dead_runtime_tasks(self.repo_root, "run-cycle", Some(&note))?
        } else {
            cleanup_dead_runtime_tasks(self.repo_root, "run-cycle", None)?
        };
        if cleaned > 0 {
            if let Some(log) = self.logger {
                let _ = log.note(format!("- Runtime cleanup fixed {} ghost task(s)", cleaned));
            }
        }
        Ok(())
    }

    async fn monitor_active_jobs(&mut self) -> Result<()> {
        crate::coordinator::control_plane::monitor_active_jobs_native(
            self.repo_root,
            self.env_cfg,
            &mut self.run_state,
            self.phase_runner_max_attempts,
            self.phase_timeout_seconds,
            self.logger,
        )
        .await
    }

    async fn monitor_merge_jobs(&mut self) -> Result<Option<(String, String)>> {
        crate::coordinator::control_plane::monitor_merge_jobs_native(
            self.repo_root,
            &mut self.run_state,
            self.logger,
        )
        .await
    }

    async fn on_blocked_merge(&mut self, task_id: &str, reason: &str) -> Result<()> {
        for (tid, pid) in terminate_active_jobs(&self.run_state) {
            if let Some(log) = self.logger {
                let _ = log.note(format!("- Sent TERM to active task={} pid={}", tid, pid));
            }
        }
        self.run_state.merge_join_set.abort_all();
        self.run_state.active_merge_jobs.clear();
        set_task_paused_for_integrate(self.repo_root, task_id, reason)?;
        write_coordinator_pause_file(self.repo_root, task_id, "integrate", reason)?;
        if let Some(log) = self.logger {
            let _ = log.note(format!(
                "- Run paused task={} phase=integrate reason={}",
                task_id, reason
            ));
        }
        loop {
            if !coordinator_pause_file_path(self.repo_root).exists() {
                if let Some(log) = self.logger {
                    let _ = log.note("- Resume signal received; continuing run loop".to_string());
                }
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        resume_paused_task_integrate(self.repo_root, task_id)?;
        let _ = clear_coordinator_pause_file(self.repo_root)?;
        Ok(())
    }

    async fn advance_tasks(&mut self) -> Result<AdvanceResult> {
        crate::coordinator::control_plane::advance_tasks_native(
            self.repo_root,
            self.coordinator_tool_override.as_deref(),
            self.phase_runner_max_attempts,
            &mut self.run_state,
            self.logger,
        )
        .await
    }

    async fn dispatch_ready_tasks(&mut self) -> Result<usize> {
        crate::coordinator::control_plane::dispatch_ready_tasks_native(
            self.repo_root,
            self.canonical,
            self.coordinator,
            self.env_cfg,
            &self.prd_file,
            &mut self.run_state,
            self.logger,
        )
        .await
    }

    async fn on_cycle_end(
        &mut self,
        _cycle: usize,
        _advance: &AdvanceResult,
        _dispatched: usize,
    ) -> Result<CoordinatorCounts> {
        let snapshot = crate::coordinator::state::coordinator_state_snapshot(
            self.repo_root,
            &std::collections::BTreeMap::new(),
        )?;
        let tasks = snapshot
            .registry
            .get("tasks")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut counts = CoordinatorCounts {
            total: tasks.len(),
            todo: 0,
            active: 0,
            blocked: 0,
            merged: 0,
        };
        for task in tasks {
            let state = task
                .get("state")
                .and_then(|s| s.as_str())
                .unwrap_or_default();
            match state {
                "todo" => counts.todo += 1,
                "blocked" => counts.blocked += 1,
                "merged" => counts.merged += 1,
                "claimed" | "in_progress" | "pr_open" | "changes_requested" | "queued" => {
                    counts.active += 1
                }
                _ => {}
            }
        }
        self.last_logged_counts = Some(counts);
        Ok(counts)
    }

    async fn sleep_between_cycles(&mut self) -> Result<()> {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(())
    }

    fn should_terminate_run(&self, counts: &CoordinatorCounts) -> bool {
        let max_dispatch_total = self
            .env_cfg
            .max_dispatch
            .or_else(|| self.coordinator.and_then(|c| c.max_dispatch))
            .unwrap_or(10);
        if max_dispatch_total == 0 {
            return false;
        }
        self.run_state.dispatched_total_run >= max_dispatch_total
            && counts.active == 0
            && self.run_state.active_jobs.is_empty()
            && self.run_state.active_merge_jobs.is_empty()
    }
}

pub fn resolve_storage_mode(
    env_cfg: &super::types::CoordinatorEnvConfig,
    coordinator: Option<&CoordinatorConfig>,
) -> Result<CoordinatorStorageMode> {
    let raw = env_cfg
        .storage_mode
        .clone()
        .or_else(|| coordinator.and_then(|c| c.storage_mode.clone()))
        .unwrap_or_else(|| "sqlite".to_string());
    raw.parse::<CoordinatorStorageMode>()
        .map_err(MaccError::Validation)
}

pub fn sync_storage_with_startup_reconcile(
    project_paths: &crate::ProjectPaths,
    storage_mode: CoordinatorStorageMode,
    logger: Option<&dyn CoordinatorLog>,
) -> Result<()> {
    if storage_mode == CoordinatorStorageMode::Json {
        return Ok(());
    }
    let imported = coordinator_storage_bootstrap_sqlite_from_json(project_paths)?;
    if imported {
        if let Some(log) = logger {
            let _ = log.note("- Storage bootstrap: imported JSON snapshot into SQLite".to_string());
        }
    }
    if std::env::var("COORDINATOR_JSON_COMPAT")
        .ok()
        .map(|raw| {
            !matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(false)
    {
        coordinator_storage_export_sqlite_to_json(project_paths)?;
    }
    Ok(())
}

pub async fn run_native_control_plane(
    repo_root: &Path,
    canonical: &CanonicalConfig,
    coordinator: Option<&CoordinatorConfig>,
    env_cfg: &super::types::CoordinatorEnvConfig,
    logger: Option<&dyn CoordinatorLog>,
) -> Result<()> {
    let run_id = if let Ok(existing) = std::env::var("COORDINATOR_RUN_ID") {
        let trimmed = existing.trim();
        if trimmed.is_empty() {
            let generated = format!(
                "run-{}-{}",
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
                std::process::id()
            );
            std::env::set_var("COORDINATOR_RUN_ID", &generated);
            generated
        } else {
            trimmed.to_string()
        }
    } else {
        let generated = format!(
            "run-{}-{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
            std::process::id()
        );
        std::env::set_var("COORDINATOR_RUN_ID", &generated);
        generated
    };

    let _ = crate::coordinator::helpers::append_coordinator_event_with_severity(
        repo_root,
        "command_start",
        "-",
        "run",
        "started",
        &format!("Coordinator run started (run_id={})", run_id),
        "info",
    );

    let prd_file = env_cfg
        .prd
        .as_ref()
        .map(PathBuf::from)
        .or_else(|| {
            coordinator
                .and_then(|c| c.prd_file.clone())
                .map(PathBuf::from)
        })
        .unwrap_or_else(|| repo_root.join("prd.json"));
    if !prd_file.exists() {
        return Err(MaccError::Validation(format!(
            "Coordinator PRD file not found: {}. Configure `automation.coordinator.prd_file` or pass `--prd`.",
            prd_file.display()
        )));
    }

    let phase_runner_max_attempts = env_cfg
        .phase_runner_max_attempts
        .or_else(|| coordinator.and_then(|c| c.phase_runner_max_attempts))
        .unwrap_or(1)
        .max(1);
    let coordinator_tool_override = env_cfg
        .coordinator_tool
        .clone()
        .or_else(|| coordinator.and_then(|c| c.coordinator_tool.clone()));
    let phase_timeout_seconds = env_cfg
        .stale_in_progress_seconds
        .or_else(|| coordinator.and_then(|c| c.stale_in_progress_seconds))
        .unwrap_or(0);

    let storage_mode = resolve_storage_mode(env_cfg, coordinator)?;
    let storage_paths = crate::ProjectPaths::from_root(repo_root);
    sync_storage_with_startup_reconcile(&storage_paths, storage_mode, logger)?;
    let startup_cleaned = if let Some(log) = logger {
        let note = |line: String| {
            let _ = log.note(line);
        };
        cleanup_dead_runtime_tasks(repo_root, "run-startup", Some(&note))?
    } else {
        cleanup_dead_runtime_tasks(repo_root, "run-startup", None)?
    };
    if startup_cleaned > 0 {
        if let Some(log) = logger {
            let _ = log.note(format!(
                "- Startup runtime cleanup fixed {} ghost task(s)",
                startup_cleaned
            ));
        }
    }

    let mut backend = NativeControlPlaneBackend {
        repo_root,
        canonical,
        coordinator,
        env_cfg,
        logger,
        prd_file,
        run_state: CoordinatorRunState::new(),
        phase_runner_max_attempts,
        coordinator_tool_override,
        phase_timeout_seconds,
        last_logged_counts: None,
    };

    let timeout_seconds = env_cfg
        .timeout_seconds
        .or_else(|| coordinator.and_then(|c| c.timeout_seconds))
        .unwrap_or(0);
    let loop_cfg = ControlPlaneLoopConfig {
        timeout: if timeout_seconds > 0 {
            Some(Duration::from_secs(timeout_seconds as u64))
        } else {
            None
        },
        max_no_progress_cycles: 2,
    };
    let run_result = run_control_plane(&mut backend, loop_cfg).await;
    if run_result.is_err() {
        if let Err(err) = &run_result {
            let _ = crate::coordinator::helpers::append_coordinator_event_with_severity(
                repo_root,
                "command_end",
                "-",
                "run",
                "failed",
                &format!("Coordinator run failed: {}", err),
                "blocking",
            );
        }
        for (_tid, _pid) in terminate_active_jobs(&backend.run_state) {}
        backend.run_state.active_jobs.clear();
        backend.run_state.join_set.abort_all();
        backend.run_state.active_merge_jobs.clear();
        backend.run_state.merge_join_set.abort_all();
        return run_result;
    }

    let _ = crate::coordinator::helpers::append_coordinator_event_with_severity(
        repo_root,
        "command_end",
        "-",
        "run",
        "done",
        "Coordinator run complete",
        "info",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_advance_maps_states() {
        assert!(matches!(
            plan_advance(WorkflowState::InProgress),
            AdvancePlan::RunPhase(PhaseTransition { mode: "review", .. })
        ));
        assert!(matches!(
            plan_advance(WorkflowState::PrOpen),
            AdvancePlan::RunPhase(PhaseTransition {
                mode: "integrate",
                ..
            })
        ));
        assert!(matches!(
            plan_advance(WorkflowState::ChangesRequested),
            AdvancePlan::RunPhase(PhaseTransition { mode: "fix", .. })
        ));
        assert!(matches!(
            plan_advance(WorkflowState::Queued),
            AdvancePlan::Merge
        ));
        assert!(matches!(
            plan_advance(WorkflowState::Todo),
            AdvancePlan::Noop
        ));
    }

    #[test]
    fn apply_phase_failure_sets_blocked_failed() {
        let mut task = json!({ "id": "T1", "state": "in_progress" });
        apply_phase_failure(&mut task, "review", "boom", "2026-02-20T00:00:00Z").unwrap();
        assert_eq!(task["state"], "blocked");
        assert_eq!(task["task_runtime"]["status"], "failed");
        assert_eq!(task["task_runtime"]["current_phase"], "review");
    }

    #[test]
    fn apply_dispatch_claim_sets_runtime_and_worktree() {
        let mut task = json!({ "id": "T2", "state": "todo" });
        let update = DispatchClaimUpdate {
            task_id: "T2".to_string(),
            tool: "codex".to_string(),
            worktree_path: "/tmp/wt".to_string(),
            branch: "ai/codex/x".to_string(),
            base_branch: "main".to_string(),
            last_commit: "abc".to_string(),
            session_id: "s-1".to_string(),
            pid: Some(123),
            phase: "dev".to_string(),
            now: "2026-02-20T00:00:00Z".to_string(),
        };
        apply_dispatch_claim(&mut task, &update);
        assert_eq!(task["state"], "claimed");
        assert_eq!(task["task_runtime"]["status"], "running");
        assert_eq!(task["task_runtime"]["pid"], 123);
    }

    #[test]
    fn apply_job_completion_success_sets_in_progress() {
        let mut task =
            json!({"id":"T3","state":"claimed","task_runtime":{"status":"running","pid":123}});
        let out = apply_job_completion(
            &mut task,
            &JobCompletionInput {
                success: true,
                attempt: 1,
                max_attempts: 1,
                timed_out: false,
                phase_timeout_seconds: 0,
                elapsed_seconds: 2,
                status_text: "exit status: 0".to_string(),
                error_code: None,
                error_origin: None,
                error_message: None,
                auto_retry_error_codes: Vec::new(),
                auto_retry_max: 0,
            },
            "2026-02-21T00:00:00Z",
        );
        assert!(!out.should_retry);
        assert_eq!(task["state"], "in_progress");
        assert_eq!(task["task_runtime"]["status"], "phase_done");
        assert!(task["task_runtime"]["pid"].is_null());
    }

    #[test]
    fn cleanup_dead_runtime_tasks_resets_claimed_dev_to_todo() {
        let mut registry = json!({
            "tasks": [{
                "id":"T4",
                "state":"claimed",
                "assignee":"agentA",
                "task_runtime":{
                    "status":"running",
                    "current_phase":"dev",
                    "pid":999
                }
            }]
        });
        let cleaned = cleanup_dead_runtime_tasks_in_registry_with(
            &mut registry,
            "2026-02-21T00:00:00Z",
            0,
            |_| false,
        )
        .unwrap();
        assert_eq!(cleaned.len(), 1);
        assert_eq!(registry["tasks"][0]["state"], "todo");
        assert!(registry["tasks"][0]["assignee"].is_null());
        assert_eq!(registry["tasks"][0]["task_runtime"]["status"], "stale");
        assert!(registry["tasks"][0]["task_runtime"]["pid"].is_null());
    }

    #[test]
    fn cleanup_dead_runtime_tasks_respects_recent_heartbeat_grace() {
        let mut registry = json!({
            "tasks": [{
                "id":"T4b",
                "state":"claimed",
                "assignee":"agentA",
                "task_runtime":{
                    "status":"running",
                    "current_phase":"dev",
                    "pid":999,
                    "last_heartbeat":"2026-02-21T00:00:30Z"
                }
            }]
        });
        let cleaned = cleanup_dead_runtime_tasks_in_registry_with(
            &mut registry,
            "2026-02-21T00:01:00Z",
            60,
            |_| false,
        )
        .unwrap();
        assert_eq!(cleaned.len(), 0);
        assert_eq!(registry["tasks"][0]["state"], "claimed");
        assert_eq!(registry["tasks"][0]["task_runtime"]["status"], "running");
        assert_eq!(registry["tasks"][0]["task_runtime"]["pid"], 999);
    }

    #[test]
    fn fsm_rejects_skipping_review_phase() {
        let mut task = json!({"id":"T5","state":"in_progress","task_runtime":{"status":"running"}});
        let err = apply_phase_success(
            &mut task,
            PhaseTransition {
                mode: "integrate",
                next_state: WorkflowState::Queued,
                runtime_phase: "integrate",
            },
            "2026-02-21T00:00:00Z",
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid coordinator FSM transition"),
            "unexpected error: {}",
            err
        );
    }
}
