use crate::coordinator::model::{ResourceLock, Task, TaskRegistry};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorStorageTransfer {
    ImportJsonToSqlite,
    ExportSqliteToJson,
    VerifyParity,
}

impl FromStr for CoordinatorStorageTransfer {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "import" | "json-to-sqlite" | "json_to_sqlite" => Ok(Self::ImportJsonToSqlite),
            "export" | "sqlite-to-json" | "sqlite_to_json" => Ok(Self::ExportSqliteToJson),
            "verify" | "parity" => Ok(Self::VerifyParity),
            other => Err(format!(
                "Unknown coordinator storage transfer '{}'. Expected import|export|verify.",
                other
            )),
        }
    }
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
pub struct EventMutation {
    pub event_id: Option<String>,
    pub run_id: Option<String>,
    pub seq: Option<i64>,
    pub ts: Option<String>,
    pub source: String,
    pub task_id: String,
    pub event_type: String,
    pub phase: String,
    pub status: String,
    pub payload: Value,
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

    pub fn append_event(&self, event: &Value) -> Result<bool> {
        let mut conn = self.open()?;
        self.init_schema(&conn)?;
        let tx = conn.transaction().map_err(sql_err)?;
        let mutation = EventMutation {
            event_id: event
                .get("event_id")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string()),
            run_id: event
                .get("run_id")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string()),
            seq: event.get("seq").and_then(|v| v.as_i64()),
            ts: event
                .get("ts")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string()),
            source: event
                .get("source")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            task_id: event
                .get("task_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            event_type: event
                .get("type")
                .or_else(|| event.get("event"))
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            phase: event
                .get("phase")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            status: event
                .get("status")
                .or_else(|| event.get("state"))
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            payload: event.get("payload").cloned().unwrap_or_else(|| json!({})),
        };
        let inserted = self.append_event_in_tx(&tx, &mutation)?;
        tx.commit().map_err(sql_err)?;
        Ok(inserted)
    }

    fn append_event_in_tx(
        &self,
        tx: &rusqlite::Transaction<'_>,
        event: &EventMutation,
    ) -> Result<bool> {
        let now = now_iso_string();
        let seq = event
            .seq
            .unwrap_or_else(|| Utc::now().timestamp_nanos_opt().unwrap_or_default());
        let ts = event.ts.as_deref().unwrap_or(now.as_str());
        let run_id = event
            .run_id
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.to_string())
            .or_else(|| std::env::var("COORDINATOR_RUN_ID").ok())
            .unwrap_or_else(|| {
                format!(
                    "run-{}-{}",
                    Utc::now().timestamp_nanos_opt().unwrap_or_default(),
                    std::process::id()
                )
            });
        let event_id = event
            .event_id
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("evt-{}-{}-{}", event.event_type, event.task_id, seq));
        let payload_raw = serde_json::to_string(&event.payload).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize event payload: {}", e))
        })?;
        let raw_json = serde_json::to_string(&json!({
            "schema_version":"1",
            "event_id": event_id,
            "run_id": run_id,
            "seq": seq,
            "ts": ts,
            "source": event.source,
            "task_id": event.task_id,
            "type": event.event_type,
            "phase": event.phase,
            "status": event.status,
            "payload": event.payload
        }))
        .map_err(|e| MaccError::Validation(format!("Failed to serialize event json: {}", e)))?;

        let inserted = tx
            .execute(
                "INSERT OR IGNORE INTO events (event_id, seq, ts, source, task_id, event_type, phase, status, payload_json, raw_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    event_id,
                    seq,
                    ts,
                    event.source,
                    event.task_id,
                    event.event_type,
                    event.phase,
                    event.status,
                    payload_raw,
                    raw_json
                ],
            )
            .map_err(sql_err)?;
        Ok(inserted > 0)
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
        self.apply_transition_with_event(change, None)
    }

    pub fn apply_transition_with_event(
        &self,
        change: &TransitionMutation,
        event: Option<&EventMutation>,
    ) -> Result<()> {
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
        let mut task = parse_task_payload(&change.task_id, &task_raw)?;

        task.state = change.new_state.clone();
        task.updated_at = Some(change.now.clone());
        task.state_changed_at = Some(change.now.clone());

        if change.new_state == "pr_open" && !change.pr_url.is_empty() {
            task.pr_url = Some(change.pr_url.clone());
        }

        if change.new_state == "changes_requested" {
            let mut review = task.review.take().unwrap_or_else(|| json!({}));
            if !review.is_object() {
                review = json!({});
            }
            review["changed"] = Value::Bool(true);
            review["last_reviewed_at"] = Value::String(change.now.clone());
            if !change.reviewer.is_empty() {
                review["reviewer"] = Value::String(change.reviewer.clone());
            }
            if !change.reason.is_empty() {
                review["reason"] = Value::String(change.reason.clone());
            }
            task.review = Some(review);
        }

        if matches!(change.new_state.as_str(), "merged" | "abandoned" | "todo") {
            task.assignee = None;
            task.claimed_at = None;
            task.worktree = None;
            task.task_runtime.status = Some("idle".to_string());
            task.task_runtime.pid = None;
            task.task_runtime.started_at = None;
            task.task_runtime.current_phase = None;
            task.task_runtime.merge_result_pending = Some(false);
            task.task_runtime.merge_result_file = None;
        }

        let title = task.title.clone().unwrap_or_default();
        let priority = task.priority.clone().unwrap_or_default();
        let tool = task.tool.clone().unwrap_or_default();
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

        let runtime = parse_task_runtime_value(&task);
        self.upsert_task_runtime_row(&tx, &change.task_id, &runtime, &change.now)?;
        self.recompute_resource_locks(&tx, &change.now)?;
        if let Some(event) = event {
            self.append_event_in_tx(&tx, event)?;
        }
        tx.commit().map_err(sql_err)?;
        Ok(())
    }

    pub fn set_runtime(&self, change: &RuntimeMutation) -> Result<()> {
        self.set_runtime_with_event(change, None)
    }

    pub fn set_runtime_with_event(
        &self,
        change: &RuntimeMutation,
        event: Option<&EventMutation>,
    ) -> Result<()> {
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
        let mut task = parse_task_payload(&change.task_id, &task_raw)?;

        if task.task_runtime.metrics.is_none() {
            task.task_runtime.metrics = Some(json!({}));
        }
        if task.task_runtime.slo_warnings.is_none() {
            task.task_runtime.slo_warnings = Some(json!({}));
        }

        task.task_runtime.status = Some(change.runtime_status.clone());
        if !change.phase.is_empty() {
            task.task_runtime.current_phase = Some(change.phase.clone());
        }
        match change.pid {
            Some(pid) => task.task_runtime.pid = Some(pid),
            None => {
                if matches!(
                    change.runtime_status.as_str(),
                    "idle" | "phase_done" | "failed" | "stale"
                ) {
                    task.task_runtime.pid = None;
                }
            }
        }
        if !change.last_error.is_empty() {
            task.task_runtime.last_error = Some(change.last_error.clone());
        }
        if !change.heartbeat_ts.is_empty() {
            task.task_runtime.last_heartbeat = Some(change.heartbeat_ts.clone());
        }
        if let Some(attempt) = change.attempt {
            task.task_runtime.attempt = Some(attempt);
        }
        if change.runtime_status == "running"
            && task
                .task_runtime
                .started_at
                .as_deref()
                .unwrap_or_default()
                .is_empty()
        {
            task.task_runtime.started_at = Some(change.now.clone());
        }
        if matches!(
            change.runtime_status.as_str(),
            "idle" | "phase_done" | "failed" | "stale"
        ) {
            task.task_runtime.phase_started_at = None;
        } else if change.runtime_status == "running" {
            task.task_runtime.phase_started_at = Some(change.now.clone());
        }

        task.updated_at = Some(change.now.clone());

        let state = task.state.clone();
        let title = task.title.clone().unwrap_or_default();
        let priority = task.priority.clone().unwrap_or_default();
        let tool = task.tool.clone().unwrap_or_default();
        let payload = serde_json::to_string(&task).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize task payload: {}", e))
        })?;
        tx.execute(
            "UPDATE tasks SET state=?2, title=?3, priority=?4, tool=?5, payload_json=?6, updated_at=?7 WHERE task_id=?1",
            params![change.task_id, state, title, priority, tool, payload, change.now],
        )
        .map_err(sql_err)?;

        let runtime = parse_task_runtime_value(&task);
        self.upsert_task_runtime_row(&tx, &change.task_id, &runtime, &change.now)?;
        if let Some(event) = event {
            self.append_event_in_tx(&tx, event)?;
        }
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
        let mut task = parse_task_payload(&change.task_id, &task_raw)?;
        task.task_runtime.merge_result_pending = Some(true);
        task.task_runtime.merge_result_file = Some(change.result_file.clone());
        task.task_runtime.merge_worker_pid = change.pid;
        task.task_runtime.merge_result_started_at = Some(change.now.clone());
        task.updated_at = Some(change.now.clone());

        let state = task.state.clone();
        let title = task.title.clone().unwrap_or_default();
        let priority = task.priority.clone().unwrap_or_default();
        let tool = task.tool.clone().unwrap_or_default();
        let payload = serde_json::to_string(&task).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize task payload: {}", e))
        })?;
        tx.execute(
            "UPDATE tasks SET state=?2, title=?3, priority=?4, tool=?5, payload_json=?6, updated_at=?7 WHERE task_id=?1",
            params![change.task_id, state, title, priority, tool, payload, change.now],
        )
        .map_err(sql_err)?;

        let runtime = parse_task_runtime_value(&task);
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
        let mut task = parse_task_payload(&change.task_id, &task_raw)?;
        task.task_runtime.merge_result_pending = Some(false);
        task.task_runtime.merge_result_file = None;
        task.task_runtime.merge_worker_pid = None;
        if !change.result_file.is_empty() {
            task.task_runtime.last_merge_result_file = Some(change.result_file.clone());
        }
        if !change.status.is_empty() {
            task.task_runtime.last_merge_result_status = Some(change.status.clone());
        }
        if let Some(rc) = change.rc {
            task.task_runtime.last_merge_result_rc = Some(rc);
        }
        task.task_runtime.last_merge_result_at = Some(change.now.clone());
        task.updated_at = Some(change.now.clone());

        let state = task.state.clone();
        let title = task.title.clone().unwrap_or_default();
        let priority = task.priority.clone().unwrap_or_default();
        let tool = task.tool.clone().unwrap_or_default();
        let payload = serde_json::to_string(&task).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize task payload: {}", e))
        })?;
        tx.execute(
            "UPDATE tasks SET state=?2, title=?3, priority=?4, tool=?5, payload_json=?6, updated_at=?7 WHERE task_id=?1",
            params![change.task_id, state, title, priority, tool, payload, change.now],
        )
        .map_err(sql_err)?;

        let runtime = parse_task_runtime_value(&task);
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
        let mut task = parse_task_payload(&change.task_id, &task_raw)?;
        let mut metrics = task
            .task_runtime
            .metrics
            .take()
            .unwrap_or_else(|| json!({}));
        if !metrics.is_object() {
            metrics = json!({});
        }
        let current = metrics
            .get("retries")
            .and_then(Value::as_i64)
            .or(task.task_runtime.retries)
            .unwrap_or(0);
        let next = current + 1;
        metrics["retries"] = Value::from(next);
        task.task_runtime.metrics = Some(metrics);
        task.task_runtime.retries = Some(next);
        task.updated_at = Some(change.now.clone());

        let state = task.state.clone();
        let title = task.title.clone().unwrap_or_default();
        let priority = task.priority.clone().unwrap_or_default();
        let tool = task.tool.clone().unwrap_or_default();
        let payload = serde_json::to_string(&task).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize task payload: {}", e))
        })?;
        tx.execute(
            "UPDATE tasks SET state=?2, title=?3, priority=?4, tool=?5, payload_json=?6, updated_at=?7 WHERE task_id=?1",
            params![change.task_id, state, title, priority, tool, payload, change.now],
        )
        .map_err(sql_err)?;
        let runtime = parse_task_runtime_value(&task);
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
        let mut task = parse_task_payload(&change.task_id, &task_raw)?;
        let mut slo_warnings = task
            .task_runtime
            .slo_warnings
            .take()
            .unwrap_or_else(|| json!({}));
        if !slo_warnings.is_object() {
            slo_warnings = json!({});
        }
        slo_warnings[&change.metric] = json!({
            "metric": change.metric,
            "threshold": change.threshold,
            "value": change.value,
            "warned_at": change.now,
            "suggestion": change.suggestion,
        });
        task.task_runtime.slo_warnings = Some(slo_warnings);
        task.updated_at = Some(change.now.clone());

        let state = task.state.clone();
        let title = task.title.clone().unwrap_or_default();
        let priority = task.priority.clone().unwrap_or_default();
        let tool = task.tool.clone().unwrap_or_default();
        let payload = serde_json::to_string(&task).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize task payload: {}", e))
        })?;
        tx.execute(
            "UPDATE tasks SET state=?2, title=?3, priority=?4, tool=?5, payload_json=?6, updated_at=?7 WHERE task_id=?1",
            params![change.task_id, state, title, priority, tool, payload, change.now],
        )
        .map_err(sql_err)?;
        let runtime = parse_task_runtime_value(&task);
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
            let task: Task = match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if !is_active_state(&task.state) {
                continue;
            }
            if task.worktree.is_none() {
                continue;
            }
            if task.id.is_empty() {
                continue;
            }
            let worktree_path = task
                .worktree
                .as_ref()
                .and_then(|v| v.worktree_path.clone())
                .unwrap_or_default();
            let locked_at = task
                .claimed_at
                .as_deref()
                .filter(|v| !v.is_empty())
                .unwrap_or(now)
                .to_string();
            for resource_name in &task.exclusive_resources {
                if resource_name.is_empty() || existing.contains(resource_name) {
                    continue;
                }
                let lock = ResourceLock {
                    task_id: task.id.clone(),
                    worktree_path: worktree_path.clone(),
                    locked_at: locked_at.clone(),
                    extra: Default::default(),
                };
                let lock_json = serde_json::to_value(&lock).map_err(|e| {
                    MaccError::Validation(format!(
                        "Failed to serialize typed resource lock '{}': {}",
                        resource_name, e
                    ))
                })?;
                let payload = if worktree_path.is_empty() {
                    // Keep historical shape where missing worktree_path becomes null in payload.
                    let mut with_null = lock_json;
                    with_null["worktree_path"] = Value::Null;
                    with_null
                } else {
                    lock_json
                };
                tx.execute(
                    "INSERT INTO resource_locks (resource, task_id, worktree_path, locked_at, payload_json, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        resource_name,
                        task.id,
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
        Ok(())
    }

    fn load_registry_from_tables(&self, conn: &Connection) -> Result<Value> {
        let mut registry = TaskRegistry::default();
        let mut stmt = conn
            .prepare("SELECT payload_json FROM tasks ORDER BY task_id")
            .map_err(sql_err)?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sql_err)?;
        for row in rows {
            let raw = row.map_err(sql_err)?;
            if let Ok(task) = serde_json::from_str::<Task>(&raw) {
                registry.tasks.push(task);
            }
        }

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
            if let Ok(lock) = serde_json::from_str::<ResourceLock>(&payload) {
                registry.resource_locks.insert(resource, lock);
            }
        }
        registry.updated_at = Some(now_iso_string());
        registry
            .extra
            .insert("schema_version".into(), Value::from(1));
        registry
            .extra
            .entry("processed_event_ids".into())
            .or_insert_with(|| json!({}));
        registry
            .extra
            .entry("state_mapping".into())
            .or_insert_with(|| json!({}));
        registry.to_value()
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

        let registry = TaskRegistry::from_value(&snapshot.registry)?;
        for task in &registry.tasks {
            let task_id = task.id.clone();
            if task_id.is_empty() {
                continue;
            }
            let state = task.state.clone();
            let title = task.title.clone().unwrap_or_default();
            let priority = task.priority.clone().unwrap_or_default();
            let tool = task.tool.clone().unwrap_or_default();
            let task_updated = task.updated_at.as_deref().unwrap_or(now.as_str());
            let task_raw = serde_json::to_string(task).map_err(|e| {
                MaccError::Validation(format!("Failed to serialize task payload: {}", e))
            })?;
            tx.execute(
                "INSERT INTO tasks (task_id, state, title, priority, tool, payload_json, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![task_id, state, title, priority, tool, task_raw, task_updated],
            )
            .map_err(sql_err)?;

            let runtime = parse_task_runtime_value(task);
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
            let runtime_raw = serde_json::to_string(&runtime).map_err(|e| {
                MaccError::Validation(format!("Failed to serialize task_runtime payload: {}", e))
            })?;
            let runtime_updated = runtime
                .get("updated_at")
                .and_then(Value::as_str)
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
            if let Some(merge_pid) = runtime.get("merge_worker_pid").and_then(Value::as_i64) {
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

        for (resource, lock_value) in &registry.resource_locks {
            let task_id = lock_value.task_id.as_str();
            let worktree_path = lock_value.worktree_path.as_str();
            let locked_at = lock_value.locked_at.as_str();
            let lock_raw = serde_json::to_string(lock_value).map_err(|e| {
                MaccError::Validation(format!("Failed to serialize resource lock payload: {}", e))
            })?;
            tx.execute(
                "INSERT INTO resource_locks (resource, task_id, worktree_path, locked_at, payload_json, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![resource, task_id, worktree_path, locked_at, lock_raw, now],
            )
            .map_err(sql_err)?;
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
    // Backward-compatible entry point. New code should call explicit transfer APIs.
    match (mode, phase) {
        (CoordinatorStorageMode::Json, _) => Ok(()),
        (CoordinatorStorageMode::DualWrite, _) => {
            coordinator_storage_import_json_to_sqlite(project_paths)
        }
        (CoordinatorStorageMode::Sqlite, CoordinatorStoragePhase::Pre) => {
            let imported = coordinator_storage_bootstrap_sqlite_from_json(project_paths)?;
            if !imported {
                coordinator_storage_export_sqlite_to_json(project_paths)?;
            }
            Ok(())
        }
        (CoordinatorStorageMode::Sqlite, CoordinatorStoragePhase::Post) => {
            coordinator_storage_export_sqlite_to_json(project_paths)
        }
    }
}

pub fn coordinator_storage_bootstrap_sqlite_from_json(
    project_paths: &ProjectPaths,
) -> Result<bool> {
    let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
    let json_store = JsonStorage::new(paths.clone());
    let sqlite_store = SqliteStorage::new(paths);
    if sqlite_store.has_snapshot_data()? {
        return Ok(false);
    }
    let json_snapshot = json_store.load_snapshot()?;
    sqlite_store.save_snapshot(&json_snapshot)?;
    Ok(true)
}

pub fn coordinator_storage_import_json_to_sqlite(project_paths: &ProjectPaths) -> Result<()> {
    let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
    let json_store = JsonStorage::new(paths.clone());
    let sqlite_store = SqliteStorage::new(paths);
    let json_snapshot = json_store.load_snapshot()?;
    sqlite_store.save_snapshot(&json_snapshot)
}

pub fn coordinator_storage_export_sqlite_to_json(project_paths: &ProjectPaths) -> Result<()> {
    let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
    let json_store = JsonStorage::new(paths.clone());
    let sqlite_store = SqliteStorage::new(paths);
    let sqlite_snapshot = sqlite_store.load_snapshot()?;
    json_store.save_snapshot(&sqlite_snapshot)
}

pub fn coordinator_storage_verify_parity(project_paths: &ProjectPaths) -> Result<()> {
    let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
    let json_store = JsonStorage::new(paths.clone());
    let sqlite_store = SqliteStorage::new(paths);
    let json_snapshot = json_store.load_snapshot()?;
    let sqlite_snapshot = sqlite_store.load_snapshot()?;
    if json_snapshot != sqlite_snapshot {
        return Err(MaccError::Validation(
            "Coordinator storage mismatch: json and sqlite snapshots differ".into(),
        ));
    }
    Ok(())
}

pub fn append_event_sqlite(project_paths: &ProjectPaths, event: &Value) -> Result<bool> {
    let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
    let sqlite = SqliteStorage::new(paths);
    sqlite.append_event(event)
}

pub fn apply_transition_sqlite(
    project_paths: &ProjectPaths,
    change: &TransitionMutation,
) -> Result<()> {
    apply_transition_sqlite_with_event(project_paths, change, None)
}

pub fn apply_transition_sqlite_with_event(
    project_paths: &ProjectPaths,
    change: &TransitionMutation,
    event: Option<&EventMutation>,
) -> Result<()> {
    let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
    let sqlite = SqliteStorage::new(paths);
    sqlite.apply_transition_with_event(change, event)
}

pub fn set_runtime_sqlite(project_paths: &ProjectPaths, change: &RuntimeMutation) -> Result<()> {
    set_runtime_sqlite_with_event(project_paths, change, None)
}

pub fn set_runtime_sqlite_with_event(
    project_paths: &ProjectPaths,
    change: &RuntimeMutation,
    event: Option<&EventMutation>,
) -> Result<()> {
    let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
    let sqlite = SqliteStorage::new(paths);
    sqlite.set_runtime_with_event(change, event)
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

fn parse_task_payload(task_id: &str, raw: &str) -> Result<Task> {
    serde_json::from_str::<Task>(raw).map_err(|e| {
        MaccError::Validation(format!(
            "Failed to parse typed task payload for '{}': {}",
            task_id, e
        ))
    })
}

fn parse_task_runtime_value(task: &Task) -> Value {
    serde_json::to_value(&task.task_runtime).unwrap_or_else(|_| json!({}))
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

    #[test]
    fn set_runtime_with_event_is_atomic_and_idempotent() {
        let root = temp_project_root("macc_coord_storage_runtime_event");
        let project_paths = ProjectPaths::from_root(&root);
        let storage_paths = CoordinatorStoragePaths::from_project_paths(&project_paths);
        seed_files(&storage_paths);

        coordinator_storage_import_json_to_sqlite(&project_paths).unwrap();
        let now = "2026-02-25T00:00:00Z".to_string();
        let runtime = RuntimeMutation {
            task_id: "TASK-1".to_string(),
            runtime_status: "phase_done".to_string(),
            phase: "dev".to_string(),
            pid: None,
            last_error: "".to_string(),
            heartbeat_ts: now.clone(),
            attempt: Some(1),
            now: now.clone(),
        };
        let event = EventMutation {
            event_id: Some("evt-runtime-done-1".to_string()),
            run_id: Some("run-test".to_string()),
            seq: Some(42),
            ts: Some(now.clone()),
            source: "test".to_string(),
            task_id: "TASK-1".to_string(),
            event_type: "phase_result".to_string(),
            phase: "dev".to_string(),
            status: "done".to_string(),
            payload: json!({"message":"phase done"}),
        };
        set_runtime_sqlite_with_event(&project_paths, &runtime, Some(&event)).unwrap();
        set_runtime_sqlite_with_event(&project_paths, &runtime, Some(&event)).unwrap();

        let snapshot = SqliteStorage::new(storage_paths).load_snapshot().unwrap();
        let task = snapshot
            .registry
            .get("tasks")
            .and_then(Value::as_array)
            .and_then(|tasks| {
                tasks.iter().find(|t| {
                    t.get("id")
                        .and_then(Value::as_str)
                        .map(|id| id == "TASK-1")
                        .unwrap_or(false)
                })
            })
            .cloned()
            .unwrap();
        assert_eq!(
            task.get("task_runtime")
                .and_then(|v| v.get("status"))
                .and_then(Value::as_str),
            Some("phase_done")
        );
        let matching_events = snapshot
            .events
            .iter()
            .filter(|e| {
                e.get("event_id")
                    .and_then(Value::as_str)
                    .map(|id| id == "evt-runtime-done-1")
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(matching_events, 1, "event insert must stay idempotent");

        let _ = fs::remove_dir_all(root);
    }
}
