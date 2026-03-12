use crate::coordinator::helpers::{
    now_iso_coordinator, recompute_resource_locks_from_tasks, set_registry_updated_at,
};
use crate::coordinator::model::TaskRegistry;
use crate::coordinator::{engine as coordinator_engine, RuntimeStatus};
use crate::{MaccError, Result};
use std::collections::BTreeMap;
use std::path::Path;

fn coordinator_registry_path(repo_root: &Path) -> std::path::PathBuf {
    repo_root.join(crate::coordinator::COORDINATOR_TASK_REGISTRY_REL_PATH)
}

pub fn coordinator_pause_file_path(repo_root: &Path) -> std::path::PathBuf {
    repo_root.join(crate::coordinator::COORDINATOR_PAUSE_FILE_REL_PATH)
}

pub fn write_coordinator_pause_file(
    repo_root: &Path,
    task_id: &str,
    phase: &str,
    reason: &str,
) -> Result<()> {
    let path = coordinator_pause_file_path(repo_root);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| MaccError::Io {
            path: parent.to_string_lossy().into(),
            action: "create coordinator pause file parent".into(),
            source: e,
        })?;
    }
    let payload = serde_json::json!({
        "paused": true,
        "task_id": task_id,
        "phase": phase,
        "reason": reason,
        "updated_at": now_iso_coordinator(),
    });
    let body = serde_json::to_string_pretty(&payload).map_err(|e| {
        MaccError::Validation(format!(
            "Failed to serialize coordinator pause file '{}': {}",
            path.display(),
            e
        ))
    })?;
    std::fs::write(&path, body).map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "write coordinator pause file".into(),
        source: e,
    })
}

pub fn clear_coordinator_pause_file(repo_root: &Path) -> Result<bool> {
    let path = coordinator_pause_file_path(repo_root);
    if !path.exists() {
        return Ok(false);
    }
    std::fs::remove_file(&path).map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "remove coordinator pause file".into(),
        source: e,
    })?;
    Ok(true)
}

pub fn read_coordinator_pause_file(repo_root: &Path) -> Result<Option<serde_json::Value>> {
    let path = coordinator_pause_file_path(repo_root);
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&path).map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "read coordinator pause file".into(),
        source: e,
    })?;
    let value: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
        MaccError::Validation(format!(
            "Failed to parse coordinator pause file '{}': {}",
            path.display(),
            e
        ))
    })?;
    Ok(Some(value))
}

pub fn set_task_paused_for_integrate(repo_root: &Path, task_id: &str, reason: &str) -> Result<()> {
    let mut args = BTreeMap::new();
    args.insert("task-id".to_string(), task_id.to_string());
    args.insert("runtime-status".to_string(), "paused".to_string());
    args.insert("phase".to_string(), "integrate".to_string());
    args.insert("last-error".to_string(), reason.to_string());
    args.insert("pid".to_string(), "".to_string());
    crate::coordinator::state::coordinator_state_set_runtime(repo_root, &args)
}

pub fn resume_paused_task_integrate(repo_root: &Path, task_id: &str) -> Result<()> {
    let mut transition_args = BTreeMap::new();
    transition_args.insert("task-id".to_string(), task_id.to_string());
    transition_args.insert("state".to_string(), "queued".to_string());
    transition_args.insert("reason".to_string(), "resume:integrate_pause".to_string());
    crate::coordinator::state::coordinator_state_apply_transition(repo_root, &transition_args)?;

    let mut runtime_args = BTreeMap::new();
    runtime_args.insert("task-id".to_string(), task_id.to_string());
    runtime_args.insert("runtime-status".to_string(), "phase_done".to_string());
    runtime_args.insert("phase".to_string(), "integrate".to_string());
    runtime_args.insert("pid".to_string(), "".to_string());
    crate::coordinator::state::coordinator_state_set_runtime(repo_root, &runtime_args)
}

fn is_pid_running(pid: i64) -> bool {
    if pid <= 0 {
        return false;
    }
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn ensure_runtime_object(task: &mut serde_json::Value) {
    if !task
        .get("task_runtime")
        .map(serde_json::Value::is_object)
        .unwrap_or(false)
    {
        task["task_runtime"] = serde_json::json!({});
    }
}

pub fn cleanup_dead_runtime_tasks_in_registry(
    registry: &mut serde_json::Value,
    reason: &str,
    logger: Option<&dyn Fn(String)>,
    repo_root: Option<&Path>,
) -> Result<usize> {
    let now = now_iso_coordinator();
    let heartbeat_grace_seconds = std::env::var("COORDINATOR_GHOST_HEARTBEAT_GRACE_SECONDS")
        .ok()
        .and_then(|raw| raw.trim().parse::<i64>().ok())
        .unwrap_or(60);
    if let Some(root) = repo_root {
        let refreshed = refresh_candidate_heartbeats_from_events(
            registry,
            root,
            heartbeat_grace_seconds,
            logger,
        )?;
        if refreshed > 0 {
            set_registry_updated_at(registry);
        }
    }
    let cleaned = coordinator_engine::cleanup_dead_runtime_tasks_in_registry_with(
        registry,
        &now,
        heartbeat_grace_seconds,
        is_pid_running,
    )?;
    let fixed = cleaned.len();
    for entry in cleaned {
        if let Some(log) = logger {
            log(format!(
                "- Runtime ghost cleanup task={} state={} phase={} pid={} -> {} ({})",
                entry.task_id, entry.old_state, entry.phase, entry.pid, entry.new_state, reason
            ));
        }
    }
    if fixed > 0 {
        recompute_resource_locks_from_tasks(registry);
        set_registry_updated_at(registry);
        if let (Some(log), Some(root)) = (logger, repo_root) {
            log(format!(
                "- Runtime ghost cleanup applied count={} registry={}",
                fixed,
                coordinator_registry_path(root).display()
            ));
        }
    }
    Ok(fixed)
}

fn refresh_candidate_heartbeats_from_events(
    registry: &mut serde_json::Value,
    repo_root: &Path,
    heartbeat_grace_seconds: i64,
    logger: Option<&dyn Fn(String)>,
) -> Result<usize> {
    if heartbeat_grace_seconds <= 0 {
        return Ok(0);
    }
    let Some(tasks) = registry.get("tasks").and_then(serde_json::Value::as_array) else {
        return Ok(0);
    };
    let mut candidates: std::collections::HashSet<String> = std::collections::HashSet::new();
    for task in tasks {
        let id = task
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let pid = task
            .get("task_runtime")
            .and_then(|v| v.get("pid"))
            .and_then(serde_json::Value::as_i64);
        let runtime_status = task
            .get("task_runtime")
            .and_then(|v| v.get("status"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if !id.is_empty()
            && pid.is_some()
            && runtime_status == RuntimeStatus::Running.as_str()
            && !is_pid_running(pid.unwrap_or_default())
        {
            candidates.insert(id.to_string());
        }
    }
    if candidates.is_empty() {
        return Ok(0);
    }

    let events_path = repo_root
        .join(".macc")
        .join("log")
        .join("coordinator")
        .join("events.jsonl");
    if !events_path.exists() {
        return Ok(0);
    }
    let run_id = std::env::var("COORDINATOR_RUN_ID").ok();
    let now_ts = chrono::DateTime::parse_from_rfc3339(&now_iso_coordinator())
        .ok()
        .map(|dt| dt.timestamp())
        .unwrap_or_default();
    let raw = std::fs::read_to_string(&events_path).map_err(|e| MaccError::Io {
        path: events_path.to_string_lossy().into(),
        action: "read coordinator events for heartbeat grace".into(),
        source: e,
    })?;
    let mut latest_by_task: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(event) = serde_json::from_str::<serde_json::Value>(trimmed) else {
            continue;
        };
        if let Some(expected_run_id) = run_id.as_deref() {
            let event_run_id = event
                .get("run_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if !event_run_id.is_empty() && event_run_id != expected_run_id {
                continue;
            }
        }
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
        if !candidates.contains(task_id) {
            continue;
        }
        let ts = event
            .get("ts")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let Some(parsed) = chrono::DateTime::parse_from_rfc3339(ts).ok() else {
            continue;
        };
        if now_ts.saturating_sub(parsed.timestamp()) > heartbeat_grace_seconds {
            continue;
        }
        let entry = latest_by_task
            .entry(task_id.to_string())
            .or_insert_with(|| ts.to_string());
        let existing_ts = chrono::DateTime::parse_from_rfc3339(entry)
            .ok()
            .map(|dt| dt.timestamp())
            .unwrap_or_default();
        if parsed.timestamp() > existing_ts {
            *entry = ts.to_string();
        }
    }
    if latest_by_task.is_empty() {
        return Ok(0);
    }
    let mut updated = 0usize;
    if let Some(tasks_mut) = registry
        .get_mut("tasks")
        .and_then(serde_json::Value::as_array_mut)
    {
        for task in tasks_mut.iter_mut() {
            let id = task
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            let Some(ts) = latest_by_task.get(&id) else {
                continue;
            };
            coordinator_engine::ensure_runtime_object(task);
            task["task_runtime"]["last_heartbeat"] = serde_json::Value::String(ts.clone());
            updated += 1;
        }
    }
    if updated > 0 {
        if let Some(log) = logger {
            log(format!(
                "- Refreshed {} candidate heartbeat(s) from events before ghost cleanup",
                updated
            ));
        }
    }
    Ok(updated)
}

pub fn cleanup_dead_runtime_tasks(
    repo_root: &Path,
    reason: &str,
    logger: Option<&dyn Fn(String)>,
) -> Result<usize> {
    let mut registry =
        crate::coordinator::state::coordinator_state_registry_load(repo_root, &BTreeMap::new())?;
    let fixed =
        cleanup_dead_runtime_tasks_in_registry(&mut registry, reason, logger, Some(repo_root))?;
    if fixed > 0 {
        crate::coordinator::state::coordinator_state_registry_save(
            repo_root,
            &BTreeMap::new(),
            &registry,
        )?;
    }
    Ok(fixed)
}

pub fn reconcile_registry_native(repo_root: &Path) -> Result<()> {
    let mut registry =
        crate::coordinator::state::coordinator_state_registry_load(repo_root, &BTreeMap::new())?;
    let _ =
        cleanup_dead_runtime_tasks_in_registry(&mut registry, "reconcile", None, Some(repo_root))?;
    recompute_resource_locks_from_tasks(&mut registry);
    set_registry_updated_at(&mut registry);
    crate::coordinator::state::coordinator_state_registry_save(
        repo_root,
        &BTreeMap::new(),
        &registry,
    )
}

pub fn cleanup_registry_native(repo_root: &Path) -> Result<()> {
    let mut registry =
        crate::coordinator::state::coordinator_state_registry_load(repo_root, &BTreeMap::new())?;
    let mut changed = false;
    if let Ok(mut typed) = TaskRegistry::from_value(&registry) {
        for task in typed.tasks.iter_mut() {
            match task.state.as_str() {
                "abandoned" | "todo" => {
                    if task.worktree.is_some() {
                        task.worktree = None;
                        changed = true;
                    }
                    if task.assignee.is_some() {
                        task.assignee = None;
                        changed = true;
                    }
                    if task.task_runtime.pid.is_some() {
                        task.task_runtime.pid = None;
                        changed = true;
                    }
                }
                "merged" => {
                    if task.assignee.is_some() {
                        task.assignee = None;
                        changed = true;
                    }
                    if task.task_runtime.pid.is_some() {
                        task.task_runtime.pid = None;
                        changed = true;
                    }
                }
                _ => {}
            }
        }
        if changed {
            typed.recompute_resource_locks(&now_iso_coordinator());
            typed.set_updated_at(now_iso_coordinator());
            registry = typed.to_value()?;
        }
    } else if let Some(tasks) = registry
        .get_mut("tasks")
        .and_then(serde_json::Value::as_array_mut)
    {
        for task in tasks.iter_mut() {
            let state = task
                .get("state")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("todo");
            if state == "abandoned" || state == "todo" {
                if task.get("worktree").is_some() && !task.get("worktree").unwrap().is_null() {
                    task["worktree"] = serde_json::Value::Null;
                    changed = true;
                }
                if task.get("assignee").is_some() && !task.get("assignee").unwrap().is_null() {
                    task["assignee"] = serde_json::Value::Null;
                    changed = true;
                }
                ensure_runtime_object(task);
                if task["task_runtime"]["pid"].is_number() {
                    task["task_runtime"]["pid"] = serde_json::Value::Null;
                    changed = true;
                }
            } else if state == "merged" {
                if task.get("assignee").is_some() && !task.get("assignee").unwrap().is_null() {
                    task["assignee"] = serde_json::Value::Null;
                    changed = true;
                }
                ensure_runtime_object(task);
                if task["task_runtime"]["pid"].is_number() {
                    task["task_runtime"]["pid"] = serde_json::Value::Null;
                    changed = true;
                }
            }
        }
    }
    if changed {
        if TaskRegistry::from_value(&registry).is_err() {
            recompute_resource_locks_from_tasks(&mut registry);
            set_registry_updated_at(&mut registry);
        }
        crate::coordinator::state::coordinator_state_registry_save(
            repo_root,
            &BTreeMap::new(),
            &registry,
        )?;
    }
    Ok(())
}
