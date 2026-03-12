use crate::{MaccError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct TaskRegistry {
    #[serde(default)]
    pub tasks: Vec<Task>,
    #[serde(default)]
    pub resource_locks: BTreeMap<String, ResourceLock>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct Task {
    #[serde(default)]
    pub id: String,
    #[serde(default = "default_todo_state")]
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignee: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree: Option<TaskWorktree>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclusive_resources: Vec<String>,
    #[serde(default, skip_serializing_if = "is_default_task_runtime")]
    pub task_runtime: TaskRuntime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_changed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct TaskWorktree {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_branch: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct TaskRuntime {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_phase: Option<String>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slo_warnings: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_result_pending: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_result_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_worker_pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_result_started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_merge_result_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_merge_result_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_merge_result_rc: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_merge_result_at: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ResourceLock {
    #[serde(default)]
    pub task_id: String,
    #[serde(default)]
    pub worktree_path: String,
    #[serde(default)]
    pub locked_at: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

fn default_todo_state() -> String {
    "todo".to_string()
}

fn is_default_task_runtime(runtime: &TaskRuntime) -> bool {
    runtime == &TaskRuntime::default()
}

impl TaskRegistry {
    pub fn from_value(value: &Value) -> Result<Self> {
        serde_json::from_value::<Self>(value.clone()).map_err(|e| {
            MaccError::Validation(format!("Failed to parse typed coordinator registry: {}", e))
        })
    }

    pub fn to_value(&self) -> Result<Value> {
        serde_json::to_value(self).map_err(|e| {
            MaccError::Validation(format!(
                "Failed to serialize typed coordinator registry: {}",
                e
            ))
        })
    }

    pub fn set_updated_at(&mut self, ts: String) {
        self.updated_at = Some(ts);
    }

    pub fn recompute_resource_locks(&mut self, now_iso: &str) {
        self.resource_locks.clear();
        for task in &self.tasks {
            if task.id.is_empty() {
                continue;
            }
            if !matches!(
                task.state.as_str(),
                "claimed" | "in_progress" | "pr_open" | "changes_requested" | "queued"
            ) {
                continue;
            }
            let worktree_path = task
                .worktree
                .as_ref()
                .and_then(|w| w.worktree_path.clone())
                .unwrap_or_default();
            for resource in &task.exclusive_resources {
                if resource.is_empty() {
                    continue;
                }
                self.resource_locks.insert(
                    resource.clone(),
                    ResourceLock {
                        task_id: task.id.clone(),
                        worktree_path: worktree_path.clone(),
                        locked_at: now_iso.to_string(),
                        extra: BTreeMap::new(),
                    },
                );
            }
        }
    }

    pub fn active_task_worktree_paths(&self) -> HashSet<String> {
        let mut out = HashSet::new();
        for task in &self.tasks {
            if !matches!(
                task.state.as_str(),
                "claimed" | "in_progress" | "pr_open" | "changes_requested" | "queued"
            ) {
                continue;
            }
            if let Some(path) = task
                .worktree
                .as_ref()
                .and_then(|w| w.worktree_path.as_ref())
            {
                if !path.is_empty() {
                    out.insert(path.to_string());
                }
            }
        }
        out
    }

    pub fn can_reuse_worktree_slot(&self, worktree_path: &str) -> bool {
        let mut seen = false;
        let mut all_merged = true;
        for task in &self.tasks {
            let Some(path) = task
                .worktree
                .as_ref()
                .and_then(|w| w.worktree_path.as_ref())
            else {
                continue;
            };
            if path != worktree_path {
                continue;
            }
            seen = true;
            if task.state != "merged" {
                all_merged = false;
            }
        }
        seen && all_merged
    }

    pub fn has_in_progress_or_queued_on_worktree(&self, worktree_path: &str) -> bool {
        self.tasks.iter().any(|task| {
            matches!(task.state.as_str(), "in_progress" | "queued")
                && task
                    .worktree
                    .as_ref()
                    .and_then(|w| w.worktree_path.as_ref())
                    .map(|p| p == worktree_path)
                    .unwrap_or(false)
        })
    }
}
