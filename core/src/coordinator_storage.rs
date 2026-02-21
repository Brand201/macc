use crate::{MaccError, ProjectPaths, Result};
use chrono::Utc;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorStorageMode {
    Json,
    DualWrite,
    Sqlite,
}

impl FromStr for CoordinatorStorageMode {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "dual-write" | "dual_write" => Ok(Self::DualWrite),
            "sqlite" => Ok(Self::Sqlite),
            other => Err(format!(
                "Unknown coordinator storage mode '{}'. Expected json|dual-write|sqlite.",
                other
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorStoragePhase {
    Pre,
    Post,
}

impl FromStr for CoordinatorStoragePhase {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "pre" => Ok(Self::Pre),
            "post" => Ok(Self::Post),
            other => Err(format!(
                "Unknown coordinator storage phase '{}'. Expected pre|post.",
                other
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoordinatorStoragePaths {
    pub registry_json_path: PathBuf,
    pub events_jsonl_path: PathBuf,
    pub cursor_json_path: PathBuf,
    pub sqlite_path: PathBuf,
}

impl CoordinatorStoragePaths {
    pub fn from_project_paths(paths: &ProjectPaths) -> Self {
        Self {
            registry_json_path: paths
                .root
                .join(".macc")
                .join("automation")
                .join("task")
                .join("task_registry.json"),
            events_jsonl_path: paths
                .root
                .join(".macc")
                .join("log")
                .join("coordinator")
                .join("events.jsonl"),
            cursor_json_path: paths
                .root
                .join(".macc")
                .join("state")
                .join("coordinator.cursor"),
            sqlite_path: paths
                .root
                .join(".macc")
                .join("state")
                .join("coordinator.sqlite"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoordinatorSnapshot {
    pub registry: Value,
    pub events: Vec<Value>,
    pub cursor: Option<Value>,
}

impl CoordinatorSnapshot {
    pub fn empty() -> Self {
        Self {
            registry: default_registry_value(),
            events: Vec::new(),
            cursor: None,
        }
    }
}

pub trait CoordinatorStorage {
    fn load_snapshot(&self) -> Result<CoordinatorSnapshot>;
    fn save_snapshot(&self, snapshot: &CoordinatorSnapshot) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct TransitionMutation {
    pub task_id: String,
    pub new_state: String,
    pub pr_url: String,
    pub reviewer: String,
    pub reason: String,
    pub now: String,
}

#[derive(Debug, Clone)]
pub struct RuntimeMutation {
    pub task_id: String,
    pub runtime_status: String,
    pub phase: String,
    pub pid: Option<i64>,
    pub last_error: String,
    pub heartbeat_ts: String,
    pub attempt: Option<i64>,
    pub now: String,
}

#[derive(Debug, Clone)]
pub struct MergePendingMutation {
    pub task_id: String,
    pub result_file: String,
    pub pid: Option<i64>,
    pub now: String,
}

#[derive(Debug, Clone)]
pub struct MergeProcessedMutation {
    pub task_id: String,
    pub result_file: String,
    pub status: String,
    pub rc: Option<i64>,
    pub now: String,
}

#[derive(Debug, Clone)]
pub struct RetryIncrementMutation {
    pub task_id: String,
    pub now: String,
}

#[derive(Debug, Clone)]
pub struct SloWarningMutation {
    pub task_id: String,
    pub metric: String,
    pub threshold: i64,
    pub value: i64,
    pub suggestion: String,
    pub now: String,
}

#[derive(Debug, Clone)]
pub struct JsonStorage {
    paths: CoordinatorStoragePaths,
}

impl JsonStorage {
    pub fn new(paths: CoordinatorStoragePaths) -> Self {
        Self { paths }
    }
}

impl CoordinatorStorage for JsonStorage {
    fn load_snapshot(&self) -> Result<CoordinatorSnapshot> {
        let registry = read_json_or_default(
            &self.paths.registry_json_path,
            "read coordinator registry json",
            default_registry_value(),
        )?;

        let mut events = Vec::new();
        if self.paths.events_jsonl_path.exists() {
            let raw =
                fs::read_to_string(&self.paths.events_jsonl_path).map_err(|e| MaccError::Io {
                    path: self.paths.events_jsonl_path.to_string_lossy().into(),
                    action: "read coordinator events jsonl".into(),
                    source: e,
                })?;
            for line in raw.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok(v) = serde_json::from_str::<Value>(line) {
                    events.push(v);
                }
            }
        }

        let cursor = if self.paths.cursor_json_path.exists() {
            Some(read_json_or_default(
                &self.paths.cursor_json_path,
                "read coordinator cursor json",
                json!({}),
            )?)
        } else {
            None
        };

        Ok(CoordinatorSnapshot {
            registry,
            events,
            cursor,
        })
    }

    fn save_snapshot(&self, snapshot: &CoordinatorSnapshot) -> Result<()> {
        ensure_parent_dir(&self.paths.registry_json_path)?;
        ensure_parent_dir(&self.paths.events_jsonl_path)?;
        ensure_parent_dir(&self.paths.cursor_json_path)?;

        write_json_atomic(&self.paths.registry_json_path, &snapshot.registry)?;

        let mut events_buf = String::new();
        for event in &snapshot.events {
            let line = serde_json::to_string(event).map_err(|e| {
                MaccError::Validation(format!("Failed to serialize event json: {}", e))
            })?;
            events_buf.push_str(&line);
            events_buf.push('\n');
        }
        write_text_atomic(&self.paths.events_jsonl_path, &events_buf)?;

        match &snapshot.cursor {
            Some(cursor) => write_json_atomic(&self.paths.cursor_json_path, cursor)?,
            None => {
                if self.paths.cursor_json_path.exists() {
                    fs::remove_file(&self.paths.cursor_json_path).map_err(|e| MaccError::Io {
                        path: self.paths.cursor_json_path.to_string_lossy().into(),
                        action: "remove coordinator cursor json".into(),
                        source: e,
                    })?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SqliteStorage {
    paths: CoordinatorStoragePaths,
}

impl SqliteStorage {
    pub fn new(paths: CoordinatorStoragePaths) -> Self {
        Self { paths }
    }

    pub fn has_snapshot_data(&self) -> Result<bool> {
        let conn = self.open()?;
        self.init_schema(&conn)?;
        let registry_meta_exists: i64 = conn
            .query_row(
                "SELECT COUNT(1) FROM metadata WHERE key='registry_json'",
                [],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        let task_count: i64 = conn
            .query_row("SELECT COUNT(1) FROM tasks", [], |row| row.get(0))
            .map_err(sql_err)?;
        Ok(registry_meta_exists > 0 || task_count > 0)
    }

    fn open(&self) -> Result<Connection> {
        ensure_parent_dir(&self.paths.sqlite_path)?;
        Connection::open(&self.paths.sqlite_path).map_err(sql_err)
    }

    fn init_schema(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS metadata (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tasks (
              task_id TEXT PRIMARY KEY,
              state TEXT,
              title TEXT,
              priority TEXT,
              tool TEXT,
              payload_json TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS task_runtime (
              task_id TEXT PRIMARY KEY,
              status TEXT,
              current_phase TEXT,
              pid INTEGER,
              last_error TEXT,
              last_heartbeat TEXT,
              payload_json TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS resource_locks (
              resource TEXT PRIMARY KEY,
              task_id TEXT,
              worktree_path TEXT,
              locked_at TEXT,
              payload_json TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
              event_id TEXT PRIMARY KEY,
              seq INTEGER,
              ts TEXT,
              source TEXT,
              task_id TEXT,
              event_type TEXT,
              phase TEXT,
              status TEXT,
              payload_json TEXT NOT NULL,
              raw_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cursors (
              name TEXT PRIMARY KEY,
              path TEXT,
              inode INTEGER,
              offset INTEGER,
              last_event_id TEXT,
              updated_at TEXT,
              payload_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS jobs (
              job_key TEXT PRIMARY KEY,
              task_id TEXT,
              job_type TEXT NOT NULL,
              pid INTEGER,
              status TEXT,
              payload_json TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            ",
        )
        .map_err(sql_err)?;
        Ok(())
    }

    pub fn apply_transition(&self, change: &TransitionMutation) -> Result<()> {
        let mut conn = self.open()?;
        self.init_schema(&conn)?;
        let tx = conn.transaction().map_err(sql_err)?;

        let task_raw: String = tx
            .query_row(
                "SELECT payload_json FROM tasks WHERE task_id=?1",
                params![change.task_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        let mut task: Value = serde_json::from_str(&task_raw).map_err(|e| {
            MaccError::Validation(format!(
                "Failed to parse task payload for '{}': {}",
                change.task_id, e
            ))
        })?;

        task["state"] = Value::String(change.new_state.clone());
        task["updated_at"] = Value::String(change.now.clone());
        task["state_changed_at"] = Value::String(change.now.clone());

        if change.new_state == "pr_open" && !change.pr_url.is_empty() {
            task["pr_url"] = Value::String(change.pr_url.clone());
        }

        if change.new_state == "changes_requested" {
            ensure_object_value(&mut task, "review");
            task["review"]["changed"] = Value::Bool(true);
            task["review"]["last_reviewed_at"] = Value::String(change.now.clone());
            if !change.reviewer.is_empty() {
                task["review"]["reviewer"] = Value::String(change.reviewer.clone());
            }
            if !change.reason.is_empty() {
                task["review"]["reason"] = Value::String(change.reason.clone());
            }
        }

        if matches!(change.new_state.as_str(), "merged" | "abandoned" | "todo") {
            task["assignee"] = Value::Null;
            task["claimed_at"] = Value::Null;
            task["worktree"] = Value::Null;
            ensure_object_value(&mut task, "task_runtime");
            task["task_runtime"]["status"] = Value::String("idle".to_string());
            task["task_runtime"]["pid"] = Value::Null;
            task["task_runtime"]["started_at"] = Value::Null;
            task["task_runtime"]["current_phase"] = Value::Null;
            task["task_runtime"]["merge_result_pending"] = Value::Bool(false);
            task["task_runtime"]["merge_result_file"] = Value::Null;
        }

        let title = task
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let priority = task
            .get("priority")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let tool = task
            .get("tool")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let payload = serde_json::to_string(&task).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize task payload: {}", e))
        })?;
        tx.execute(
            "UPDATE tasks SET state=?2, title=?3, priority=?4, tool=?5, payload_json=?6, updated_at=?7 WHERE task_id=?1",
            params![
                change.task_id,
                change.new_state,
                title,
                priority,
                tool,
                payload,
                change.now
            ],
        )
        .map_err(sql_err)?;

        let runtime = task
            .get("task_runtime")
            .cloned()
            .unwrap_or_else(|| json!({}));
        self.upsert_task_runtime_row(&tx, &change.task_id, &runtime, &change.now)?;
        self.recompute_resource_locks(&tx, &change.now)?;
        tx.commit().map_err(sql_err)?;
        Ok(())
    }

    pub fn set_runtime(&self, change: &RuntimeMutation) -> Result<()> {
        let mut conn = self.open()?;
        self.init_schema(&conn)?;
        let tx = conn.transaction().map_err(sql_err)?;

        let task_raw: String = tx
            .query_row(
                "SELECT payload_json FROM tasks WHERE task_id=?1",
                params![change.task_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        let mut task: Value = serde_json::from_str(&task_raw).map_err(|e| {
            MaccError::Validation(format!(
                "Failed to parse task payload for '{}': {}",
                change.task_id, e
            ))
        })?;

        ensure_object_value(&mut task, "task_runtime");
        ensure_object_value(&mut task["task_runtime"], "metrics");
        ensure_object_value(&mut task["task_runtime"], "slo_warnings");

        task["task_runtime"]["status"] = Value::String(change.runtime_status.clone());
        if !change.phase.is_empty() {
            task["task_runtime"]["current_phase"] = Value::String(change.phase.clone());
        }
        match change.pid {
            Some(pid) => task["task_runtime"]["pid"] = Value::from(pid),
            None => {
                if matches!(
                    change.runtime_status.as_str(),
                    "idle" | "phase_done" | "failed" | "stale"
                ) {
                    task["task_runtime"]["pid"] = Value::Null;
                }
            }
        }
        if !change.last_error.is_empty() {
            task["task_runtime"]["last_error"] = Value::String(change.last_error.clone());
        }
        if !change.heartbeat_ts.is_empty() {
            task["task_runtime"]["last_heartbeat"] = Value::String(change.heartbeat_ts.clone());
        }
        if let Some(attempt) = change.attempt {
            task["task_runtime"]["attempt"] = Value::from(attempt);
        }
        if change.runtime_status == "running"
            && task["task_runtime"]["started_at"]
                .as_str()
                .unwrap_or_default()
                .is_empty()
        {
            task["task_runtime"]["started_at"] = Value::String(change.now.clone());
        }
        if matches!(
            change.runtime_status.as_str(),
            "idle" | "phase_done" | "failed" | "stale"
        ) {
            task["task_runtime"]["phase_started_at"] = Value::Null;
        } else if change.runtime_status == "running" {
            task["task_runtime"]["phase_started_at"] = Value::String(change.now.clone());
        }

        task["updated_at"] = Value::String(change.now.clone());

        let state = task
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let title = task
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let priority = task
            .get("priority")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let tool = task
            .get("tool")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let payload = serde_json::to_string(&task).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize task payload: {}", e))
        })?;
        tx.execute(
            "UPDATE tasks SET state=?2, title=?3, priority=?4, tool=?5, payload_json=?6, updated_at=?7 WHERE task_id=?1",
            params![change.task_id, state, title, priority, tool, payload, change.now],
        )
        .map_err(sql_err)?;

        let runtime = task
            .get("task_runtime")
            .cloned()
            .unwrap_or_else(|| json!({}));
        self.upsert_task_runtime_row(&tx, &change.task_id, &runtime, &change.now)?;
        tx.commit().map_err(sql_err)?;
        Ok(())
    }

    pub fn set_merge_pending(&self, change: &MergePendingMutation) -> Result<()> {
        let mut conn = self.open()?;
        self.init_schema(&conn)?;
        let tx = conn.transaction().map_err(sql_err)?;

        let task_raw: String = tx
            .query_row(
                "SELECT payload_json FROM tasks WHERE task_id=?1",
                params![change.task_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        let mut task: Value = serde_json::from_str(&task_raw).map_err(|e| {
            MaccError::Validation(format!(
                "Failed to parse task payload for '{}': {}",
                change.task_id, e
            ))
        })?;
        ensure_object_value(&mut task, "task_runtime");
        task["task_runtime"]["merge_result_pending"] = Value::Bool(true);
        task["task_runtime"]["merge_result_file"] = Value::String(change.result_file.clone());
        task["task_runtime"]["merge_worker_pid"] = match change.pid {
            Some(pid) => Value::from(pid),
            None => Value::Null,
        };
        task["task_runtime"]["merge_result_started_at"] = Value::String(change.now.clone());
        task["updated_at"] = Value::String(change.now.clone());

        let state = task
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let title = task
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let priority = task
            .get("priority")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let tool = task
            .get("tool")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let payload = serde_json::to_string(&task).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize task payload: {}", e))
        })?;
        tx.execute(
            "UPDATE tasks SET state=?2, title=?3, priority=?4, tool=?5, payload_json=?6, updated_at=?7 WHERE task_id=?1",
            params![change.task_id, state, title, priority, tool, payload, change.now],
        )
        .map_err(sql_err)?;

        let runtime = task
            .get("task_runtime")
            .cloned()
            .unwrap_or_else(|| json!({}));
        self.upsert_task_runtime_row(&tx, &change.task_id, &runtime, &change.now)?;
        tx.commit().map_err(sql_err)?;
        Ok(())
    }

    pub fn set_merge_processed(&self, change: &MergeProcessedMutation) -> Result<()> {
        let mut conn = self.open()?;
        self.init_schema(&conn)?;
        let tx = conn.transaction().map_err(sql_err)?;

        let task_raw: String = tx
            .query_row(
                "SELECT payload_json FROM tasks WHERE task_id=?1",
                params![change.task_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        let mut task: Value = serde_json::from_str(&task_raw).map_err(|e| {
            MaccError::Validation(format!(
                "Failed to parse task payload for '{}': {}",
                change.task_id, e
            ))
        })?;
        ensure_object_value(&mut task, "task_runtime");
        task["task_runtime"]["merge_result_pending"] = Value::Bool(false);
        task["task_runtime"]["merge_result_file"] = Value::Null;
        task["task_runtime"]["merge_worker_pid"] = Value::Null;
        if !change.result_file.is_empty() {
            task["task_runtime"]["last_merge_result_file"] =
                Value::String(change.result_file.clone());
        }
        if !change.status.is_empty() {
            task["task_runtime"]["last_merge_result_status"] = Value::String(change.status.clone());
        }
        if let Some(rc) = change.rc {
            task["task_runtime"]["last_merge_result_rc"] = Value::from(rc);
        }
        task["task_runtime"]["last_merge_result_at"] = Value::String(change.now.clone());
        task["updated_at"] = Value::String(change.now.clone());

        let state = task
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let title = task
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let priority = task
            .get("priority")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let tool = task
            .get("tool")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let payload = serde_json::to_string(&task).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize task payload: {}", e))
        })?;
        tx.execute(
            "UPDATE tasks SET state=?2, title=?3, priority=?4, tool=?5, payload_json=?6, updated_at=?7 WHERE task_id=?1",
            params![change.task_id, state, title, priority, tool, payload, change.now],
        )
        .map_err(sql_err)?;

        let runtime = task
            .get("task_runtime")
            .cloned()
            .unwrap_or_else(|| json!({}));
        self.upsert_task_runtime_row(&tx, &change.task_id, &runtime, &change.now)?;
        tx.commit().map_err(sql_err)?;
        Ok(())
    }

    pub fn increment_retries(&self, change: &RetryIncrementMutation) -> Result<()> {
        let mut conn = self.open()?;
        self.init_schema(&conn)?;
        let tx = conn.transaction().map_err(sql_err)?;

        let task_raw: String = tx
            .query_row(
                "SELECT payload_json FROM tasks WHERE task_id=?1",
                params![change.task_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        let mut task: Value = serde_json::from_str(&task_raw).map_err(|e| {
            MaccError::Validation(format!(
                "Failed to parse task payload for '{}': {}",
                change.task_id, e
            ))
        })?;
        ensure_object_value(&mut task, "task_runtime");
        ensure_object_value(&mut task["task_runtime"], "metrics");
        let current = task["task_runtime"]["metrics"]["retries"]
            .as_i64()
            .unwrap_or_else(|| task["task_runtime"]["retries"].as_i64().unwrap_or(0));
        let next = current + 1;
        task["task_runtime"]["metrics"]["retries"] = Value::from(next);
        task["task_runtime"]["retries"] = Value::from(next);
        task["updated_at"] = Value::String(change.now.clone());

        let state = task
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let title = task
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let priority = task
            .get("priority")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let tool = task
            .get("tool")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let payload = serde_json::to_string(&task).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize task payload: {}", e))
        })?;
        tx.execute(
            "UPDATE tasks SET state=?2, title=?3, priority=?4, tool=?5, payload_json=?6, updated_at=?7 WHERE task_id=?1",
            params![change.task_id, state, title, priority, tool, payload, change.now],
        )
        .map_err(sql_err)?;

        let runtime = task
            .get("task_runtime")
            .cloned()
            .unwrap_or_else(|| json!({}));
        self.upsert_task_runtime_row(&tx, &change.task_id, &runtime, &change.now)?;
        tx.commit().map_err(sql_err)?;
        Ok(())
    }

    pub fn upsert_slo_warning(&self, change: &SloWarningMutation) -> Result<()> {
        let mut conn = self.open()?;
        self.init_schema(&conn)?;
        let tx = conn.transaction().map_err(sql_err)?;

        let task_raw: String = tx
            .query_row(
                "SELECT payload_json FROM tasks WHERE task_id=?1",
                params![change.task_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        let mut task: Value = serde_json::from_str(&task_raw).map_err(|e| {
            MaccError::Validation(format!(
                "Failed to parse task payload for '{}': {}",
                change.task_id, e
            ))
        })?;
        ensure_object_value(&mut task, "task_runtime");
        ensure_object_value(&mut task["task_runtime"], "slo_warnings");
        task["task_runtime"]["slo_warnings"][&change.metric] = json!({
            "metric": change.metric,
            "threshold": change.threshold,
            "value": change.value,
            "warned_at": change.now,
            "suggestion": change.suggestion,
        });
        task["updated_at"] = Value::String(change.now.clone());

        let state = task
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let title = task
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let priority = task
            .get("priority")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let tool = task
            .get("tool")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let payload = serde_json::to_string(&task).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize task payload: {}", e))
        })?;
        tx.execute(
            "UPDATE tasks SET state=?2, title=?3, priority=?4, tool=?5, payload_json=?6, updated_at=?7 WHERE task_id=?1",
            params![change.task_id, state, title, priority, tool, payload, change.now],
        )
        .map_err(sql_err)?;

        let runtime = task
            .get("task_runtime")
            .cloned()
            .unwrap_or_else(|| json!({}));
        self.upsert_task_runtime_row(&tx, &change.task_id, &runtime, &change.now)?;
        tx.commit().map_err(sql_err)?;
        Ok(())
    }

    fn upsert_task_runtime_row(
        &self,
        tx: &rusqlite::Transaction<'_>,
        task_id: &str,
        runtime: &Value,
        now: &str,
    ) -> Result<()> {
        let runtime_status = runtime.get("status").and_then(Value::as_str).unwrap_or("");
        let current_phase = runtime
            .get("current_phase")
            .and_then(Value::as_str)
            .unwrap_or("");
        let pid = runtime.get("pid").and_then(Value::as_i64);
        let last_error = runtime
            .get("last_error")
            .and_then(Value::as_str)
            .unwrap_or("");
        let last_heartbeat = runtime
            .get("last_heartbeat")
            .and_then(Value::as_str)
            .unwrap_or("");
        let runtime_raw = serde_json::to_string(runtime).map_err(|e| {
            MaccError::Validation(format!(
                "Failed to serialize task_runtime payload for '{}': {}",
                task_id, e
            ))
        })?;
        tx.execute(
            "INSERT INTO task_runtime (task_id, status, current_phase, pid, last_error, last_heartbeat, payload_json, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(task_id) DO UPDATE SET
               status=excluded.status,
               current_phase=excluded.current_phase,
               pid=excluded.pid,
               last_error=excluded.last_error,
               last_heartbeat=excluded.last_heartbeat,
               payload_json=excluded.payload_json,
               updated_at=excluded.updated_at",
            params![
                task_id,
                runtime_status,
                current_phase,
                pid,
                last_error,
                last_heartbeat,
                runtime_raw,
                now
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    fn recompute_resource_locks(&self, tx: &rusqlite::Transaction<'_>, now: &str) -> Result<()> {
        tx.execute("DELETE FROM resource_locks", [])
            .map_err(sql_err)?;
        let mut stmt = tx
            .prepare("SELECT payload_json FROM tasks ORDER BY task_id")
            .map_err(sql_err)?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sql_err)?;
        let mut existing = std::collections::BTreeSet::new();
        for row in rows {
            let raw = row.map_err(sql_err)?;
            let task: Value = match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let state = task.get("state").and_then(Value::as_str).unwrap_or("");
            if !is_active_state(state) {
                continue;
            }
            if task.get("worktree").is_none() || task.get("worktree").unwrap().is_null() {
                continue;
            }
            let task_id = task.get("id").and_then(Value::as_str).unwrap_or("");
            if task_id.is_empty() {
                continue;
            }
            let worktree_path = task
                .get("worktree")
                .and_then(|v| v.get("worktree_path"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let locked_at = task
                .get("claimed_at")
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
                .unwrap_or(now);
            if let Some(resources) = task.get("exclusive_resources").and_then(Value::as_array) {
                for resource in resources {
                    let Some(resource_name) = resource.as_str() else {
                        continue;
                    };
                    if resource_name.is_empty() || existing.contains(resource_name) {
                        continue;
                    }
                    let payload = json!({
                        "task_id": task_id,
                        "worktree_path": if worktree_path.is_empty() { Value::Null } else { Value::String(worktree_path.to_string()) },
                        "locked_at": locked_at,
                    });
                    tx.execute(
                        "INSERT INTO resource_locks (resource, task_id, worktree_path, locked_at, payload_json, updated_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                        params![
                            resource_name,
                            task_id,
                            worktree_path,
                            locked_at,
                            serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string()),
                            now
                        ],
                    )
                    .map_err(sql_err)?;
                    existing.insert(resource_name.to_string());
                }
            }
        }
        Ok(())
    }

    fn load_registry_from_tables(&self, conn: &Connection) -> Result<Value> {
        let mut tasks = Vec::new();
        let mut stmt = conn
            .prepare("SELECT payload_json FROM tasks ORDER BY task_id")
            .map_err(sql_err)?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sql_err)?;
        for row in rows {
            let raw = row.map_err(sql_err)?;
            if let Ok(v) = serde_json::from_str::<Value>(&raw) {
                tasks.push(v);
            }
        }

        let mut locks = serde_json::Map::new();
        let mut stmt = conn
            .prepare("SELECT resource, payload_json FROM resource_locks ORDER BY resource")
            .map_err(sql_err)?;
        let rows = stmt
            .query_map([], |row| {
                let resource: String = row.get(0)?;
                let payload: String = row.get(1)?;
                Ok((resource, payload))
            })
            .map_err(sql_err)?;
        for row in rows {
            let (resource, payload) = row.map_err(sql_err)?;
            if let Ok(v) = serde_json::from_str::<Value>(&payload) {
                locks.insert(resource, v);
            }
        }

        Ok(json!({
            "schema_version": 1,
            "tasks": tasks,
            "resource_locks": locks,
            "processed_event_ids": {},
            "state_mapping": {},
            "updated_at": now_iso_string(),
        }))
    }
}

impl CoordinatorStorage for SqliteStorage {
    fn load_snapshot(&self) -> Result<CoordinatorSnapshot> {
        let conn = self.open()?;
        self.init_schema(&conn)?;

        let metadata_registry = match conn.query_row(
            "SELECT value FROM metadata WHERE key='registry_json'",
            [],
            |row| row.get::<_, String>(0),
        ) {
            Ok(raw) => Some(serde_json::from_str::<Value>(&raw).map_err(|e| {
                MaccError::Validation(format!("Failed to parse registry_json metadata: {}", e))
            })?),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(e) => return Err(sql_err(e)),
        };
        let mut registry = self.load_registry_from_tables(&conn)?;
        if let Some(meta) = metadata_registry {
            let table_tasks_len = registry
                .get("tasks")
                .and_then(Value::as_array)
                .map(|v| v.len())
                .unwrap_or(0);
            let meta_tasks_len = meta
                .get("tasks")
                .and_then(Value::as_array)
                .map(|v| v.len())
                .unwrap_or(0);
            if table_tasks_len == 0 && meta_tasks_len > 0 {
                registry = meta;
            } else {
                for key in [
                    "lot",
                    "version",
                    "generated_at",
                    "timezone",
                    "priority_mapping",
                    "state_mapping",
                    "processed_event_ids",
                    "updated_at",
                ] {
                    if let Some(value) = meta.get(key) {
                        registry[key] = value.clone();
                    }
                }
            }
        }

        let mut events = Vec::new();
        let mut stmt = conn
            .prepare("SELECT raw_json FROM events ORDER BY seq ASC, event_id ASC")
            .map_err(sql_err)?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sql_err)?;
        for row in rows {
            let raw = row.map_err(sql_err)?;
            if let Ok(v) = serde_json::from_str::<Value>(&raw) {
                events.push(v);
            }
        }

        let cursor = match conn.query_row(
            "SELECT payload_json FROM cursors WHERE name='coordinator'",
            [],
            |row| row.get::<_, String>(0),
        ) {
            Ok(raw) => Some(serde_json::from_str::<Value>(&raw).map_err(|e| {
                MaccError::Validation(format!("Failed to parse cursor payload_json: {}", e))
            })?),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(e) => return Err(sql_err(e)),
        };

        Ok(CoordinatorSnapshot {
            registry,
            events,
            cursor,
        })
    }

    fn save_snapshot(&self, snapshot: &CoordinatorSnapshot) -> Result<()> {
        let mut conn = self.open()?;
        self.init_schema(&conn)?;
        let tx = conn.transaction().map_err(sql_err)?;

        tx.execute("DELETE FROM tasks", []).map_err(sql_err)?;
        tx.execute("DELETE FROM task_runtime", [])
            .map_err(sql_err)?;
        tx.execute("DELETE FROM resource_locks", [])
            .map_err(sql_err)?;
        tx.execute("DELETE FROM events", []).map_err(sql_err)?;
        tx.execute("DELETE FROM cursors", []).map_err(sql_err)?;
        tx.execute("DELETE FROM jobs", []).map_err(sql_err)?;

        let now = now_iso_string();
        let registry_raw = serde_json::to_string(&snapshot.registry).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize registry json: {}", e))
        })?;
        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value, updated_at) VALUES ('registry_json', ?1, ?2)",
            params![registry_raw, now],
        )
        .map_err(sql_err)?;

        if let Some(tasks) = snapshot.registry.get("tasks").and_then(|v| v.as_array()) {
            for task in tasks {
                let task_id = task
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if task_id.is_empty() {
                    continue;
                }
                let state = task.get("state").and_then(|v| v.as_str()).unwrap_or("");
                let title = task.get("title").and_then(|v| v.as_str()).unwrap_or("");
                let priority = task.get("priority").and_then(|v| v.as_str()).unwrap_or("");
                let tool = task.get("tool").and_then(|v| v.as_str()).unwrap_or("");
                let task_updated = task
                    .get("updated_at")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| now.as_str());
                let task_raw = serde_json::to_string(task).map_err(|e| {
                    MaccError::Validation(format!("Failed to serialize task payload: {}", e))
                })?;
                tx.execute(
                    "INSERT INTO tasks (task_id, state, title, priority, tool, payload_json, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![task_id, state, title, priority, tool, task_raw, task_updated],
                )
                .map_err(sql_err)?;

                let runtime = task
                    .get("task_runtime")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let runtime_status = runtime.get("status").and_then(|v| v.as_str()).unwrap_or("");
                let current_phase = runtime
                    .get("current_phase")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let pid = runtime.get("pid").and_then(|v| v.as_i64());
                let last_error = runtime
                    .get("last_error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let last_heartbeat = runtime
                    .get("last_heartbeat")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let runtime_raw = serde_json::to_string(&runtime).map_err(|e| {
                    MaccError::Validation(format!(
                        "Failed to serialize task_runtime payload: {}",
                        e
                    ))
                })?;
                let runtime_updated = runtime
                    .get("updated_at")
                    .and_then(|v| v.as_str())
                    .unwrap_or(task_updated);
                tx.execute(
                    "INSERT INTO task_runtime (task_id, status, current_phase, pid, last_error, last_heartbeat, payload_json, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        task_id,
                        runtime_status,
                        current_phase,
                        pid,
                        last_error,
                        last_heartbeat,
                        runtime_raw,
                        runtime_updated
                    ],
                )
                .map_err(sql_err)?;

                if let Some(pid) = pid {
                    let job_payload = json!({
                        "task_id": task_id,
                        "job_type": "performer",
                        "pid": pid,
                        "status": runtime_status,
                    });
                    tx.execute(
                        "INSERT INTO jobs (job_key, task_id, job_type, pid, status, payload_json, updated_at)
                         VALUES (?1, ?2, 'performer', ?3, ?4, ?5, ?6)",
                        params![
                            format!("{}:performer", task_id),
                            task_id,
                            pid,
                            runtime_status,
                            serde_json::to_string(&job_payload).unwrap_or_else(|_| "{}".to_string()),
                            runtime_updated
                        ],
                    )
                    .map_err(sql_err)?;
                }
                if let Some(merge_pid) = runtime.get("merge_worker_pid").and_then(|v| v.as_i64()) {
                    let job_payload = json!({
                        "task_id": task_id,
                        "job_type": "merge_worker",
                        "pid": merge_pid,
                        "status": runtime_status,
                    });
                    tx.execute(
                        "INSERT INTO jobs (job_key, task_id, job_type, pid, status, payload_json, updated_at)
                         VALUES (?1, ?2, 'merge_worker', ?3, ?4, ?5, ?6)",
                        params![
                            format!("{}:merge", task_id),
                            task_id,
                            merge_pid,
                            runtime_status,
                            serde_json::to_string(&job_payload).unwrap_or_else(|_| "{}".to_string()),
                            runtime_updated
                        ],
                    )
                    .map_err(sql_err)?;
                }
            }
        }

        if let Some(locks) = snapshot
            .registry
            .get("resource_locks")
            .and_then(|v| v.as_object())
        {
            for (resource, lock_value) in locks {
                let task_id = lock_value
                    .get("task_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let worktree_path = lock_value
                    .get("worktree_path")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let locked_at = lock_value
                    .get("locked_at")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let lock_raw = serde_json::to_string(lock_value).map_err(|e| {
                    MaccError::Validation(format!(
                        "Failed to serialize resource lock payload: {}",
                        e
                    ))
                })?;
                tx.execute(
                    "INSERT INTO resource_locks (resource, task_id, worktree_path, locked_at, payload_json, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![resource, task_id, worktree_path, locked_at, lock_raw, now],
                )
                .map_err(sql_err)?;
            }
        }

        for (idx, event) in snapshot.events.iter().enumerate() {
            let event_id = event
                .get("event_id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("event-{}", idx + 1));
            let seq = event
                .get("seq")
                .and_then(|v| v.as_i64())
                .unwrap_or((idx + 1) as i64);
            let ts = event.get("ts").and_then(|v| v.as_str()).unwrap_or("");
            let source = event.get("source").and_then(|v| v.as_str()).unwrap_or("");
            let task_id = event.get("task_id").and_then(|v| v.as_str()).unwrap_or("");
            let event_type = event
                .get("type")
                .or_else(|| event.get("event"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let phase = event.get("phase").and_then(|v| v.as_str()).unwrap_or("");
            let status = event
                .get("status")
                .or_else(|| event.get("state"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let payload = event.get("payload").cloned().unwrap_or_else(|| json!({}));
            let payload_raw = serde_json::to_string(&payload).map_err(|e| {
                MaccError::Validation(format!("Failed to serialize event payload: {}", e))
            })?;
            let event_raw = serde_json::to_string(event).map_err(|e| {
                MaccError::Validation(format!("Failed to serialize raw event: {}", e))
            })?;
            tx.execute(
                "INSERT INTO events (event_id, seq, ts, source, task_id, event_type, phase, status, payload_json, raw_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    event_id,
                    seq,
                    ts,
                    source,
                    task_id,
                    event_type,
                    phase,
                    status,
                    payload_raw,
                    event_raw
                ],
            )
            .map_err(sql_err)?;
        }

        if let Some(cursor) = &snapshot.cursor {
            let cursor_raw = serde_json::to_string(cursor).map_err(|e| {
                MaccError::Validation(format!("Failed to serialize cursor payload: {}", e))
            })?;
            let path = cursor.get("path").and_then(|v| v.as_str()).unwrap_or("");
            let inode = cursor.get("inode").and_then(|v| v.as_i64()).unwrap_or(0);
            let offset = cursor.get("offset").and_then(|v| v.as_i64()).unwrap_or(0);
            let last_event_id = cursor
                .get("last_event_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let updated_at = cursor
                .get("updated_at")
                .and_then(|v| v.as_str())
                .unwrap_or(now.as_str());
            tx.execute(
                "INSERT INTO cursors (name, path, inode, offset, last_event_id, updated_at, payload_json)
                 VALUES ('coordinator', ?1, ?2, ?3, ?4, ?5, ?6)",
                params![path, inode, offset, last_event_id, updated_at, cursor_raw],
            )
            .map_err(sql_err)?;
        }

        tx.commit().map_err(sql_err)?;
        Ok(())
    }
}

pub fn sync_coordinator_storage(
    project_paths: &ProjectPaths,
    mode: CoordinatorStorageMode,
    phase: CoordinatorStoragePhase,
) -> Result<()> {
    if mode == CoordinatorStorageMode::Json {
        return Ok(());
    }

    let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
    let json_store = JsonStorage::new(paths.clone());
    let sqlite_store = SqliteStorage::new(paths);

    match (mode, phase) {
        (CoordinatorStorageMode::DualWrite, _) => {
            let json_snapshot = json_store.load_snapshot()?;
            sqlite_store.save_snapshot(&json_snapshot)?;
        }
        (CoordinatorStorageMode::Sqlite, CoordinatorStoragePhase::Pre) => {
            if sqlite_store.has_snapshot_data()? {
                let sqlite_snapshot = sqlite_store.load_snapshot()?;
                json_store.save_snapshot(&sqlite_snapshot)?;
            } else {
                let json_snapshot = json_store.load_snapshot()?;
                sqlite_store.save_snapshot(&json_snapshot)?;
            }
        }
        (CoordinatorStorageMode::Sqlite, CoordinatorStoragePhase::Post) => {
            let sqlite_snapshot = sqlite_store.load_snapshot()?;
            json_store.save_snapshot(&sqlite_snapshot)?;
        }
        (CoordinatorStorageMode::Json, _) => {}
    }
    Ok(())
}

pub fn apply_transition_sqlite(
    project_paths: &ProjectPaths,
    change: &TransitionMutation,
) -> Result<()> {
    let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
    let sqlite = SqliteStorage::new(paths);
    sqlite.apply_transition(change)
}

pub fn set_runtime_sqlite(project_paths: &ProjectPaths, change: &RuntimeMutation) -> Result<()> {
    let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
    let sqlite = SqliteStorage::new(paths);
    sqlite.set_runtime(change)
}

pub fn set_merge_pending_sqlite(
    project_paths: &ProjectPaths,
    change: &MergePendingMutation,
) -> Result<()> {
    let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
    let sqlite = SqliteStorage::new(paths);
    sqlite.set_merge_pending(change)
}

pub fn set_merge_processed_sqlite(
    project_paths: &ProjectPaths,
    change: &MergeProcessedMutation,
) -> Result<()> {
    let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
    let sqlite = SqliteStorage::new(paths);
    sqlite.set_merge_processed(change)
}

pub fn increment_retries_sqlite(
    project_paths: &ProjectPaths,
    change: &RetryIncrementMutation,
) -> Result<()> {
    let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
    let sqlite = SqliteStorage::new(paths);
    sqlite.increment_retries(change)
}

pub fn upsert_slo_warning_sqlite(
    project_paths: &ProjectPaths,
    change: &SloWarningMutation,
) -> Result<()> {
    let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
    let sqlite = SqliteStorage::new(paths);
    sqlite.upsert_slo_warning(change)
}

fn read_json_or_default(path: &Path, action: &str, default: Value) -> Result<Value> {
    if !path.exists() {
        return Ok(default);
    }
    let raw = fs::read_to_string(path).map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: action.into(),
        source: e,
    })?;
    serde_json::from_str::<Value>(&raw)
        .map_err(|e| MaccError::Validation(format!("Failed to parse {}: {}", path.display(), e)))
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| MaccError::Io {
            path: parent.to_string_lossy().into(),
            action: "create parent directory".into(),
            source: e,
        })?;
    }
    Ok(())
}

fn write_text_atomic(path: &Path, content: &str) -> Result<()> {
    ensure_parent_dir(path)?;
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, content).map_err(|e| MaccError::Io {
        path: tmp.to_string_lossy().into(),
        action: "write temp file".into(),
        source: e,
    })?;
    fs::rename(&tmp, path).map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "replace destination file".into(),
        source: e,
    })?;
    Ok(())
}

fn write_json_atomic(path: &Path, value: &Value) -> Result<()> {
    let content = serde_json::to_string_pretty(value)
        .map_err(|e| MaccError::Validation(format!("Failed to serialize json: {}", e)))?;
    write_text_atomic(path, &content)
}

fn now_iso_string() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn ensure_object_value(node: &mut Value, key: &str) {
    if !node.get(key).map(Value::is_object).unwrap_or(false) {
        node[key] = json!({});
    }
}

fn is_active_state(state: &str) -> bool {
    matches!(
        state,
        "claimed" | "in_progress" | "pr_open" | "changes_requested" | "queued"
    )
}

fn default_registry_value() -> Value {
    json!({
        "schema_version": 1,
        "tasks": [],
        "processed_event_ids": {},
        "resource_locks": {},
        "state_mapping": {},
        "updated_at": now_iso_string(),
    })
}

fn sql_err(e: rusqlite::Error) -> MaccError {
    MaccError::Validation(format!("SQLite coordinator storage error: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_project_root(prefix: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("{}_{}", prefix, nonce));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn seed_files(paths: &CoordinatorStoragePaths) {
        ensure_parent_dir(&paths.registry_json_path).unwrap();
        ensure_parent_dir(&paths.events_jsonl_path).unwrap();
        ensure_parent_dir(&paths.cursor_json_path).unwrap();
        let registry = json!({
            "schema_version": 1,
            "tasks": [
                {
                    "id": "TASK-1",
                    "title": "Task One",
                    "state": "in_progress",
                    "tool": "codex",
                    "task_runtime": {
                        "status": "running",
                        "pid": 1234,
                        "current_phase": "dev",
                        "last_heartbeat": "2026-02-20T00:00:05Z",
                        "metrics": {
                            "retries": 2
                        },
                        "last_error": null
                    }
                }
            ],
            "resource_locks": {
                "service-skeleton": {
                    "task_id": "TASK-1",
                    "worktree_path": "/tmp/wt-1",
                    "locked_at": "2026-02-20T00:00:00Z"
                }
            },
            "processed_event_ids": {},
            "state_mapping": {},
            "updated_at": "2026-02-20T00:00:00Z"
        });
        write_json_atomic(&paths.registry_json_path, &registry).unwrap();
        write_text_atomic(
            &paths.events_jsonl_path,
            "{\"event_id\":\"evt-1\",\"seq\":1,\"ts\":\"2026-02-20T00:00:01Z\",\"source\":\"coordinator\",\"type\":\"task_dispatched\",\"task_id\":\"TASK-1\",\"status\":\"started\",\"payload\":{}}\n",
        )
        .unwrap();
        write_json_atomic(
            &paths.cursor_json_path,
            &json!({
                "path": paths.events_jsonl_path.to_string_lossy().to_string(),
                "inode": 1,
                "offset": 100,
                "last_event_id": "evt-1",
                "updated_at": "2026-02-20T00:00:01Z"
            }),
        )
        .unwrap();
    }

    #[test]
    fn dual_write_preserves_equivalence() {
        let root = temp_project_root("macc_coord_storage_dual");
        let project_paths = ProjectPaths::from_root(&root);
        let storage_paths = CoordinatorStoragePaths::from_project_paths(&project_paths);
        seed_files(&storage_paths);

        sync_coordinator_storage(
            &project_paths,
            CoordinatorStorageMode::DualWrite,
            CoordinatorStoragePhase::Post,
        )
        .unwrap();

        let json_snapshot = JsonStorage::new(storage_paths.clone())
            .load_snapshot()
            .unwrap();
        let sqlite_snapshot = SqliteStorage::new(storage_paths).load_snapshot().unwrap();
        assert_eq!(json_snapshot, sqlite_snapshot);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sqlite_pre_phase_exports_existing_sqlite_snapshot() {
        let root = temp_project_root("macc_coord_storage_sqlite");
        let project_paths = ProjectPaths::from_root(&root);
        let storage_paths = CoordinatorStoragePaths::from_project_paths(&project_paths);
        seed_files(&storage_paths);

        sync_coordinator_storage(
            &project_paths,
            CoordinatorStorageMode::DualWrite,
            CoordinatorStoragePhase::Post,
        )
        .unwrap();

        write_json_atomic(&storage_paths.registry_json_path, &json!({"broken": true})).unwrap();

        sync_coordinator_storage(
            &project_paths,
            CoordinatorStorageMode::Sqlite,
            CoordinatorStoragePhase::Pre,
        )
        .unwrap();

        let restored = read_json_or_default(
            &storage_paths.registry_json_path,
            "read restored registry",
            json!({}),
        )
        .unwrap();
        assert!(restored.get("tasks").is_some());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sqlite_roundtrip_preserves_cursor_payload() {
        let root = temp_project_root("macc_coord_storage_cursor");
        let project_paths = ProjectPaths::from_root(&root);
        let storage_paths = CoordinatorStoragePaths::from_project_paths(&project_paths);
        seed_files(&storage_paths);

        sync_coordinator_storage(
            &project_paths,
            CoordinatorStorageMode::DualWrite,
            CoordinatorStoragePhase::Post,
        )
        .unwrap();

        let sqlite_snapshot = SqliteStorage::new(storage_paths.clone())
            .load_snapshot()
            .unwrap();
        let cursor = sqlite_snapshot.cursor.expect("cursor from sqlite");
        assert_eq!(
            cursor.get("offset").and_then(|v| v.as_i64()),
            Some(100),
            "cursor offset must roundtrip",
        );
        assert_eq!(
            cursor.get("inode").and_then(|v| v.as_i64()),
            Some(1),
            "cursor inode must roundtrip",
        );
        assert_eq!(
            cursor.get("last_event_id").and_then(|v| v.as_str()),
            Some("evt-1"),
            "cursor last_event_id must roundtrip",
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sqlite_roundtrip_preserves_runtime_fields() {
        let root = temp_project_root("macc_coord_storage_runtime");
        let project_paths = ProjectPaths::from_root(&root);
        let storage_paths = CoordinatorStoragePaths::from_project_paths(&project_paths);
        seed_files(&storage_paths);

        sync_coordinator_storage(
            &project_paths,
            CoordinatorStorageMode::DualWrite,
            CoordinatorStoragePhase::Post,
        )
        .unwrap();

        // Simulate restart in sqlite mode: restore JSON from sqlite snapshot.
        write_json_atomic(&storage_paths.registry_json_path, &json!({"broken": true})).unwrap();
        sync_coordinator_storage(
            &project_paths,
            CoordinatorStorageMode::Sqlite,
            CoordinatorStoragePhase::Pre,
        )
        .unwrap();

        let restored = read_json_or_default(
            &storage_paths.registry_json_path,
            "read restored registry",
            json!({}),
        )
        .unwrap();
        let task = restored["tasks"][0].clone();
        assert_eq!(
            task["task_runtime"]["status"].as_str(),
            Some("running"),
            "runtime status should survive sqlite roundtrip"
        );
        assert_eq!(
            task["task_runtime"]["pid"].as_i64(),
            Some(1234),
            "runtime pid should survive sqlite roundtrip"
        );
        assert_eq!(
            task["task_runtime"]["current_phase"].as_str(),
            Some("dev"),
            "runtime current phase should survive sqlite roundtrip"
        );
        assert_eq!(
            task["task_runtime"]["last_heartbeat"].as_str(),
            Some("2026-02-20T00:00:05Z"),
            "runtime heartbeat should survive sqlite roundtrip"
        );
        assert_eq!(
            task["task_runtime"]["metrics"]["retries"].as_i64(),
            Some(2),
            "runtime retries metric should survive sqlite roundtrip"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn migration_same_event_stream_same_final_state() {
        let root_json = temp_project_root("macc_coord_storage_json_baseline");
        let paths_json = ProjectPaths::from_root(&root_json);
        let storage_paths_json = CoordinatorStoragePaths::from_project_paths(&paths_json);
        seed_files(&storage_paths_json);

        let root_sqlite = temp_project_root("macc_coord_storage_sqlite_migration");
        let paths_sqlite = ProjectPaths::from_root(&root_sqlite);
        let storage_paths_sqlite = CoordinatorStoragePaths::from_project_paths(&paths_sqlite);
        seed_files(&storage_paths_sqlite);

        let baseline_snapshot = JsonStorage::new(storage_paths_json)
            .load_snapshot()
            .unwrap();

        sync_coordinator_storage(
            &paths_sqlite,
            CoordinatorStorageMode::Sqlite,
            CoordinatorStoragePhase::Pre,
        )
        .unwrap();
        sync_coordinator_storage(
            &paths_sqlite,
            CoordinatorStorageMode::Sqlite,
            CoordinatorStoragePhase::Post,
        )
        .unwrap();

        let migrated_snapshot = JsonStorage::new(storage_paths_sqlite.clone())
            .load_snapshot()
            .unwrap();
        assert_eq!(baseline_snapshot.registry, migrated_snapshot.registry);
        assert_eq!(baseline_snapshot.events, migrated_snapshot.events);
        assert_eq!(
            baseline_snapshot
                .cursor
                .as_ref()
                .and_then(|c| c.get("offset"))
                .cloned(),
            migrated_snapshot
                .cursor
                .as_ref()
                .and_then(|c| c.get("offset"))
                .cloned()
        );
        assert_eq!(
            baseline_snapshot
                .cursor
                .as_ref()
                .and_then(|c| c.get("inode"))
                .cloned(),
            migrated_snapshot
                .cursor
                .as_ref()
                .and_then(|c| c.get("inode"))
                .cloned()
        );
        assert_eq!(
            baseline_snapshot
                .cursor
                .as_ref()
                .and_then(|c| c.get("last_event_id"))
                .cloned(),
            migrated_snapshot
                .cursor
                .as_ref()
                .and_then(|c| c.get("last_event_id"))
                .cloned()
        );

        sync_coordinator_storage(
            &paths_sqlite,
            CoordinatorStorageMode::Sqlite,
            CoordinatorStoragePhase::Post,
        )
        .unwrap();
        let replay_snapshot = JsonStorage::new(storage_paths_sqlite)
            .load_snapshot()
            .unwrap();
        assert_eq!(migrated_snapshot, replay_snapshot);

        let _ = fs::remove_dir_all(root_json);
        let _ = fs::remove_dir_all(root_sqlite);
    }

    #[test]
    fn sqlite_pre_recovers_after_json_loss() {
        let root = temp_project_root("macc_coord_storage_restart");
        let project_paths = ProjectPaths::from_root(&root);
        let storage_paths = CoordinatorStoragePaths::from_project_paths(&project_paths);
        seed_files(&storage_paths);

        // Persist canonical snapshot into SQLite.
        sync_coordinator_storage(
            &project_paths,
            CoordinatorStorageMode::DualWrite,
            CoordinatorStoragePhase::Post,
        )
        .unwrap();

        let baseline = JsonStorage::new(storage_paths.clone())
            .load_snapshot()
            .unwrap();

        // Simulate crash/file loss on JSON side.
        std::fs::remove_file(&storage_paths.registry_json_path).unwrap();
        std::fs::remove_file(&storage_paths.events_jsonl_path).unwrap();
        std::fs::remove_file(&storage_paths.cursor_json_path).unwrap();

        // Restart: pre-phase should rehydrate JSON from SQLite source.
        sync_coordinator_storage(
            &project_paths,
            CoordinatorStorageMode::Sqlite,
            CoordinatorStoragePhase::Pre,
        )
        .unwrap();

        let restored = JsonStorage::new(storage_paths).load_snapshot().unwrap();
        assert_eq!(baseline.registry, restored.registry);
        assert_eq!(baseline.events, restored.events);
        assert_eq!(baseline.cursor, restored.cursor);

        let _ = fs::remove_dir_all(root);
    }
}
