use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;

pub mod args;
pub mod control_plane;
pub mod engine;
pub mod helpers;
pub mod logs;
pub mod model;
pub mod runtime;
pub mod session_manager;
pub mod state;
pub mod state_runtime;
pub mod task_selector;
pub mod types;

pub const COORDINATOR_TASK_REGISTRY_REL_PATH: &str = ".macc/automation/task/task_registry.json";
pub const COORDINATOR_PAUSE_FILE_REL_PATH: &str = ".macc/automation/task/coordinator.pause.json";

pub const COORDINATOR_EVENT_SCHEMA_VERSION: &str = "1";
pub const COORDINATOR_EVENT_TYPES_V1: &[&str] = &[
    "command_start",
    "command_end",
    "command_error",
    "task_transition",
    "task_dispatched",
    "performer_complete",
    "task_blocked",
    "dispatch_complete",
    "started",
    "progress",
    "phase_result",
    "commit_created",
    "review_done",
    "integrate_done",
    "failed",
    "heartbeat",
    "task_runtime_retry",
    "task_runtime_requeue",
    "task_runtime_stale",
    "phase_retry",
    "phase_skipped",
    "events_rotated",
    "events_compacted",
    "storage_sync",
    "storage_sync_ok",
    "storage_sync_failed",
    "storage_sync_latency_ms",
    "storage_mismatch_count",
    "task_phase_duration_seconds",
    "task_retries_total",
    "stale_runtime_total",
    "merge_fail_total",
    "merge_fix_attempt_total",
    "task_retry_count",
    "task_slo_warning",
    "task_runtime_orphan",
    "local_merge_failed",
    "merge_worker_started",
    "merge_worker_complete",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    Todo,
    Claimed,
    InProgress,
    PrOpen,
    ChangesRequested,
    Queued,
    Merged,
    Blocked,
    Abandoned,
}

impl WorkflowState {
    pub fn as_str(self) -> &'static str {
        match self {
            WorkflowState::Todo => "todo",
            WorkflowState::Claimed => "claimed",
            WorkflowState::InProgress => "in_progress",
            WorkflowState::PrOpen => "pr_open",
            WorkflowState::ChangesRequested => "changes_requested",
            WorkflowState::Queued => "queued",
            WorkflowState::Merged => "merged",
            WorkflowState::Blocked => "blocked",
            WorkflowState::Abandoned => "abandoned",
        }
    }
}

impl FromStr for WorkflowState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "todo" => Ok(WorkflowState::Todo),
            "claimed" => Ok(WorkflowState::Claimed),
            "in_progress" => Ok(WorkflowState::InProgress),
            "pr_open" => Ok(WorkflowState::PrOpen),
            "changes_requested" => Ok(WorkflowState::ChangesRequested),
            "queued" => Ok(WorkflowState::Queued),
            "merged" => Ok(WorkflowState::Merged),
            "blocked" => Ok(WorkflowState::Blocked),
            "abandoned" => Ok(WorkflowState::Abandoned),
            other => Err(format!("unknown workflow state: {}", other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeStatus {
    Idle,
    Dispatched,
    Running,
    WaitingForUser,
    PhaseDone,
    Failed,
    Stale,
    Paused,
}

impl RuntimeStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            RuntimeStatus::Idle => "idle",
            RuntimeStatus::Dispatched => "dispatched",
            RuntimeStatus::Running => "running",
            RuntimeStatus::WaitingForUser => "waiting_for_user",
            RuntimeStatus::PhaseDone => "phase_done",
            RuntimeStatus::Failed => "failed",
            RuntimeStatus::Stale => "stale",
            RuntimeStatus::Paused => "paused",
        }
    }
}

impl FromStr for RuntimeStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "idle" => Ok(RuntimeStatus::Idle),
            "dispatched" => Ok(RuntimeStatus::Dispatched),
            "running" => Ok(RuntimeStatus::Running),
            "waiting_for_user" => Ok(RuntimeStatus::WaitingForUser),
            "phase_done" => Ok(RuntimeStatus::PhaseDone),
            "failed" => Ok(RuntimeStatus::Failed),
            "stale" => Ok(RuntimeStatus::Stale),
            "paused" => Ok(RuntimeStatus::Paused),
            other => Err(format!("unknown runtime status: {}", other)),
        }
    }
}

pub fn is_valid_workflow_transition(from: WorkflowState, to: WorkflowState) -> bool {
    matches!(
        (from, to),
        (WorkflowState::Todo, WorkflowState::Claimed)
            | (WorkflowState::Claimed, WorkflowState::InProgress)
            | (WorkflowState::Claimed, WorkflowState::Blocked)
            | (WorkflowState::Claimed, WorkflowState::Abandoned)
            | (WorkflowState::InProgress, WorkflowState::PrOpen)
            | (WorkflowState::InProgress, WorkflowState::ChangesRequested)
            | (WorkflowState::InProgress, WorkflowState::Blocked)
            | (WorkflowState::InProgress, WorkflowState::Abandoned)
            | (WorkflowState::PrOpen, WorkflowState::ChangesRequested)
            | (WorkflowState::PrOpen, WorkflowState::Queued)
            | (WorkflowState::PrOpen, WorkflowState::Blocked)
            | (WorkflowState::PrOpen, WorkflowState::Abandoned)
            | (WorkflowState::ChangesRequested, WorkflowState::PrOpen)
            | (WorkflowState::ChangesRequested, WorkflowState::Blocked)
            | (WorkflowState::ChangesRequested, WorkflowState::Abandoned)
            | (WorkflowState::Queued, WorkflowState::Merged)
            | (WorkflowState::Queued, WorkflowState::PrOpen)
            | (WorkflowState::Queued, WorkflowState::Blocked)
            | (WorkflowState::Queued, WorkflowState::Abandoned)
            | (WorkflowState::Blocked, WorkflowState::Todo)
            | (WorkflowState::Blocked, WorkflowState::Claimed)
            | (WorkflowState::Blocked, WorkflowState::InProgress)
            | (WorkflowState::Blocked, WorkflowState::PrOpen)
            | (WorkflowState::Blocked, WorkflowState::ChangesRequested)
            | (WorkflowState::Blocked, WorkflowState::Queued)
            | (WorkflowState::Blocked, WorkflowState::Abandoned)
            | (WorkflowState::Abandoned, WorkflowState::Todo)
    )
}

pub fn is_valid_runtime_transition(from: RuntimeStatus, to: RuntimeStatus) -> bool {
    matches!(
        (from, to),
        (RuntimeStatus::Idle, RuntimeStatus::Dispatched)
            | (RuntimeStatus::Idle, RuntimeStatus::Running)
            | (RuntimeStatus::Dispatched, RuntimeStatus::Running)
            | (RuntimeStatus::Dispatched, RuntimeStatus::Failed)
            | (RuntimeStatus::Dispatched, RuntimeStatus::Stale)
            | (RuntimeStatus::Running, RuntimeStatus::PhaseDone)
            | (RuntimeStatus::Running, RuntimeStatus::Failed)
            | (RuntimeStatus::Running, RuntimeStatus::Stale)
            | (RuntimeStatus::Running, RuntimeStatus::Paused)
            | (RuntimeStatus::Running, RuntimeStatus::WaitingForUser)
            | (RuntimeStatus::WaitingForUser, RuntimeStatus::Running)
            | (RuntimeStatus::WaitingForUser, RuntimeStatus::Failed)
            | (RuntimeStatus::WaitingForUser, RuntimeStatus::Paused)
            | (RuntimeStatus::WaitingForUser, RuntimeStatus::Idle)
            | (RuntimeStatus::PhaseDone, RuntimeStatus::Running)
            | (RuntimeStatus::PhaseDone, RuntimeStatus::Idle)
            | (RuntimeStatus::PhaseDone, RuntimeStatus::Failed)
            | (RuntimeStatus::Failed, RuntimeStatus::Dispatched)
            | (RuntimeStatus::Failed, RuntimeStatus::Paused)
            | (RuntimeStatus::Failed, RuntimeStatus::Idle)
            | (RuntimeStatus::Stale, RuntimeStatus::Dispatched)
            | (RuntimeStatus::Stale, RuntimeStatus::Failed)
            | (RuntimeStatus::Stale, RuntimeStatus::Paused)
            | (RuntimeStatus::Paused, RuntimeStatus::Dispatched)
            | (RuntimeStatus::Paused, RuntimeStatus::Running)
            | (RuntimeStatus::Paused, RuntimeStatus::Failed)
            | (RuntimeStatus::Paused, RuntimeStatus::Idle)
    )
}

pub fn runtime_status_from_event(event_type: &str, status: &str) -> RuntimeStatus {
    let status_norm = status.trim().to_ascii_lowercase();
    let event_norm = event_type.trim().to_ascii_lowercase();
    match status_norm.as_str() {
        "started" | "dispatched" => RuntimeStatus::Dispatched,
        "running" | "progress" | "heartbeat" => RuntimeStatus::Running,
        "waiting_for_user" | "input_required" => RuntimeStatus::WaitingForUser,
        "done" | "phase_done" => RuntimeStatus::PhaseDone,
        "failed" | "error" => RuntimeStatus::Failed,
        "stale" => RuntimeStatus::Stale,
        "paused" => RuntimeStatus::Paused,
        _ => match event_norm.as_str() {
            "started" => RuntimeStatus::Dispatched,
            "progress" | "heartbeat" => RuntimeStatus::Running,
            "input_required" => RuntimeStatus::WaitingForUser,
            "phase_result" => RuntimeStatus::Running,
            "failed" => RuntimeStatus::Failed,
            _ => RuntimeStatus::Running,
        },
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorEvent {
    pub schema_version: String,
    pub event_id: String,
    pub seq: u64,
    pub ts: String,
    pub source: String,
    pub task_id: Option<String>,
    #[serde(rename = "type")]
    pub event_type: String,
    pub phase: Option<String>,
    pub status: String,
    #[serde(default)]
    pub payload: Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn workflow_transition_table_has_expected_edges() {
        assert!(is_valid_workflow_transition(
            WorkflowState::Todo,
            WorkflowState::Claimed
        ));
        assert!(is_valid_workflow_transition(
            WorkflowState::Queued,
            WorkflowState::Merged
        ));
        assert!(!is_valid_workflow_transition(
            WorkflowState::Todo,
            WorkflowState::Merged
        ));
    }

    #[test]
    fn runtime_transition_table_has_expected_edges() {
        assert!(is_valid_runtime_transition(
            RuntimeStatus::Idle,
            RuntimeStatus::Dispatched
        ));
        assert!(is_valid_runtime_transition(
            RuntimeStatus::Running,
            RuntimeStatus::PhaseDone
        ));
        assert!(is_valid_runtime_transition(
            RuntimeStatus::Failed,
            RuntimeStatus::Dispatched
        ));
        assert!(!is_valid_runtime_transition(
            RuntimeStatus::Idle,
            RuntimeStatus::PhaseDone
        ));
    }

    #[test]
    fn runtime_status_parsing_roundtrips() {
        let status = "phase_done".parse::<RuntimeStatus>().unwrap();
        assert_eq!(status, RuntimeStatus::PhaseDone);
        assert_eq!(status.as_str(), "phase_done");
    }

    #[test]
    fn runtime_status_from_event_maps_stable_values() {
        assert_eq!(
            runtime_status_from_event("heartbeat", "running"),
            RuntimeStatus::Running
        );
        assert_eq!(
            runtime_status_from_event("input_required", "waiting_for_user"),
            RuntimeStatus::WaitingForUser
        );
        assert_eq!(
            runtime_status_from_event("phase_result", "phase_done"),
            RuntimeStatus::PhaseDone
        );
        assert_eq!(
            runtime_status_from_event("failed", "error"),
            RuntimeStatus::Failed
        );
        assert_eq!(
            runtime_status_from_event("unknown", ""),
            RuntimeStatus::Running
        );
    }

    #[test]
    fn workflow_state_parsing_roundtrips() {
        let state = "in_progress".parse::<WorkflowState>().unwrap();
        assert_eq!(state, WorkflowState::InProgress);
        assert_eq!(state.as_str(), "in_progress");
    }

    #[test]
    fn event_schema_matches_core_event_types() {
        let schema_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../docs/schemas/coordinator-event.v1.schema.json");
        let schema_raw = std::fs::read_to_string(&schema_path).expect("read schema");
        let schema: serde_json::Value = serde_json::from_str(&schema_raw).expect("parse schema");

        let schema_version = schema
            .get("properties")
            .and_then(|p| p.get("schema_version"))
            .and_then(|s| s.get("const"))
            .and_then(|v| v.as_str())
            .expect("schema_version const");
        assert_eq!(schema_version, COORDINATOR_EVENT_SCHEMA_VERSION);

        let schema_types: BTreeSet<String> = schema
            .get("properties")
            .and_then(|p| p.get("type"))
            .and_then(|t| t.get("enum"))
            .and_then(|e| e.as_array())
            .expect("type enum")
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect();
        let core_types: BTreeSet<String> = COORDINATOR_EVENT_TYPES_V1
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(schema_types, core_types);
    }
}
