use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;

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
    PhaseDone,
    Failed,
    Stale,
    Paused,
}

pub fn is_valid_workflow_transition(from: WorkflowState, to: WorkflowState) -> bool {
    matches!(
        (from, to),
        (WorkflowState::Todo, WorkflowState::Claimed)
            | (WorkflowState::Claimed, WorkflowState::InProgress)
            | (WorkflowState::Claimed, WorkflowState::Blocked)
            | (WorkflowState::Claimed, WorkflowState::Abandoned)
            | (WorkflowState::InProgress, WorkflowState::PrOpen)
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
    fn workflow_state_parsing_roundtrips() {
        let state = "in_progress".parse::<WorkflowState>().unwrap();
        assert_eq!(state, WorkflowState::InProgress);
        assert_eq!(state.as_str(), "in_progress");
    }
}
