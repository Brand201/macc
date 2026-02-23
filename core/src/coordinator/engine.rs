use super::{RuntimeStatus, WorkflowState};
use crate::{MaccError, Result};
use serde_json::{json, Value};
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
}

#[derive(Debug, Clone)]
pub struct JobCompletionResult {
    pub should_retry: bool,
    pub status_label: &'static str,
    pub detail: String,
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

pub fn apply_phase_success(task: &mut Value, transition: PhaseTransition, now: &str) {
    task["state"] = Value::String(transition.next_state.as_str().to_string());
    ensure_runtime_object(task);
    task["task_runtime"]["status"] = Value::String(RuntimeStatus::PhaseDone.as_str().to_string());
    task["task_runtime"]["current_phase"] = Value::String(transition.runtime_phase.to_string());
    task["task_runtime"]["pid"] = Value::Null;
    task["state_changed_at"] = Value::String(now.to_string());
}

pub fn apply_phase_failure(task: &mut Value, runtime_phase: &str, reason: &str, now: &str) {
    task["state"] = Value::String(WorkflowState::Blocked.as_str().to_string());
    ensure_runtime_object(task);
    task["task_runtime"]["status"] = Value::String(RuntimeStatus::Failed.as_str().to_string());
    task["task_runtime"]["current_phase"] = Value::String(runtime_phase.to_string());
    task["task_runtime"]["last_error"] = Value::String(reason.to_string());
    task["task_runtime"]["pid"] = Value::Null;
    task["state_changed_at"] = Value::String(now.to_string());
}

pub fn apply_merge_success(task: &mut Value, now: &str) {
    task["state"] = Value::String(WorkflowState::Merged.as_str().to_string());
    ensure_runtime_object(task);
    task["task_runtime"]["status"] = Value::String(RuntimeStatus::Idle.as_str().to_string());
    task["task_runtime"]["pid"] = Value::Null;
    task["state_changed_at"] = Value::String(now.to_string());
}

pub fn apply_merge_failure(task: &mut Value, reason: &str, now: &str) {
    task["state"] = Value::String(WorkflowState::Blocked.as_str().to_string());
    ensure_runtime_object(task);
    task["task_runtime"]["status"] = Value::String(RuntimeStatus::Paused.as_str().to_string());
    task["task_runtime"]["current_phase"] = Value::String("integrate".to_string());
    task["task_runtime"]["last_error"] = Value::String(reason.to_string());
    task["task_runtime"]["pid"] = Value::Null;
    task["state_changed_at"] = Value::String(now.to_string());
}

pub fn apply_job_completion(
    task: &mut Value,
    input: &JobCompletionInput,
    now: &str,
) -> JobCompletionResult {
    ensure_runtime_object(task);
    if input.attempt == 0 || input.max_attempts == 0 {
        task["state"] = Value::String(WorkflowState::Blocked.as_str().to_string());
        task["task_runtime"]["status"] = Value::String(RuntimeStatus::Failed.as_str().to_string());
        task["task_runtime"]["pid"] = Value::Null;
        let detail = "performer completion received with invalid attempt counters".to_string();
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
        task["task_runtime"]["last_error"] = Value::String(reason.clone());
        task["state_changed_at"] = Value::String(now.to_string());
        return JobCompletionResult {
            should_retry: true,
            status_label: "retry",
            detail: reason,
        };
    }

    task["state"] = Value::String(WorkflowState::Blocked.as_str().to_string());
    task["task_runtime"]["status"] = Value::String(RuntimeStatus::Failed.as_str().to_string());
    task["task_runtime"]["pid"] = Value::Null;
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
    task["task_runtime"]["last_error"] = Value::String(reason.clone());
    task["state_changed_at"] = Value::String(now.to_string());
    JobCompletionResult {
        should_retry: false,
        status_label: "failed",
        detail: reason,
    }
}

pub fn cleanup_dead_runtime_tasks_in_registry_with<F>(
    registry: &mut Value,
    now: &str,
    mut is_pid_running: F,
) -> Result<Vec<DeadRuntimeCleanupEntry>>
where
    F: FnMut(i64) -> bool,
{
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
        apply_phase_failure(&mut task, "review", "boom", "2026-02-20T00:00:00Z");
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
            |_| false,
        )
        .unwrap();
        assert_eq!(cleaned.len(), 1);
        assert_eq!(registry["tasks"][0]["state"], "todo");
        assert!(registry["tasks"][0]["assignee"].is_null());
        assert_eq!(registry["tasks"][0]["task_runtime"]["status"], "stale");
        assert!(registry["tasks"][0]["task_runtime"]["pid"].is_null());
    }
}
