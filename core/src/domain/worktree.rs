use crate::{MaccError, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub fn coordinator_task_registry_path(root: &Path) -> PathBuf {
    root.join(crate::coordinator::COORDINATOR_TASK_REGISTRY_REL_PATH)
}

pub fn canonicalize_path_fallback(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

pub fn truncate_cell(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    if max <= 1 {
        return ".".to_string();
    }
    let keep = max.saturating_sub(3);
    let trimmed = value.chars().take(keep).collect::<String>();
    format!("{}...", trimmed)
}

pub fn git_worktree_is_dirty(worktree: &Path) -> Result<bool> {
    crate::git::is_dirty(worktree)
}

fn unix_timestamp_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn load_worktree_session_labels(
    project_paths: Option<&crate::ProjectPaths>,
) -> Result<BTreeMap<PathBuf, String>> {
    let mut map = BTreeMap::new();
    let Some(paths) = project_paths else {
        return Ok(map);
    };

    let sessions_path = paths.macc_dir.join("state/tool-sessions.json");
    if !sessions_path.exists() {
        return Ok(map);
    }

    let now = unix_timestamp_secs() as i64;
    let content = std::fs::read_to_string(&sessions_path).map_err(|e| MaccError::Io {
        path: sessions_path.to_string_lossy().into(),
        action: "read tool sessions state".into(),
        source: e,
    })?;
    let root: serde_json::Value = serde_json::from_str(&content).map_err(|e| {
        MaccError::Validation(format!(
            "Failed to parse sessions file '{}': {}",
            sessions_path.display(),
            e
        ))
    })?;

    let tools = root
        .get("tools")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    for (tool_id, tool_value) in tools {
        let leases = tool_value
            .get("leases")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        for (session_id, lease) in leases {
            let status = lease
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            if status != "active" {
                continue;
            }
            let owner = lease
                .get("owner_worktree")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            if owner.is_empty() {
                continue;
            }
            let heartbeat = lease
                .get("heartbeat_epoch")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let stale = heartbeat <= 0 || (now - heartbeat) > 1800;
            let owner_path = canonicalize_path_fallback(Path::new(owner));
            let label = if stale {
                format!("stale:{}:{}", tool_id, session_id)
            } else {
                format!("occupied:{}:{}", tool_id, session_id)
            };
            map.insert(owner_path, label);
        }
    }

    Ok(map)
}

pub fn resolve_worktree_path(root: &Path, id: &str) -> Result<PathBuf> {
    let candidate = Path::new(id);
    Ok(
        if candidate.is_absolute() || id.contains(std::path::MAIN_SEPARATOR) {
            PathBuf::from(id)
        } else {
            root.join(".macc/worktree").join(id)
        },
    )
}

pub fn delete_branch(root: &Path, branch: Option<&str>, force: bool) -> Result<()> {
    let Some(branch) = branch else {
        return Ok(());
    };
    crate::git::delete_local_branch(root, branch, force)
}

pub fn remove_all_worktrees(root: &Path, remove_branches: bool) -> Result<usize> {
    let entries = crate::list_worktrees(root)?;
    let root_canon = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let mut removed = 0usize;

    for entry in entries {
        if entry.path == root_canon {
            continue;
        }
        let branch = entry.branch.clone();
        crate::remove_worktree(root, &entry.path, true)?;
        if remove_branches {
            delete_branch(root, branch.as_deref(), true)?;
        }
        removed += 1;
    }
    Ok(removed)
}

pub fn write_tool_json(repo_root: &Path, worktree_path: &Path, tool_id: &str) -> Result<PathBuf> {
    crate::write_tool_json(repo_root, worktree_path, tool_id)
}

pub fn ensure_tool_json(repo_root: &Path, worktree_path: &Path, tool_id: &str) -> Result<PathBuf> {
    let tool_json_path = worktree_path.join(".macc").join("tool.json");
    if tool_json_path.exists() {
        return Ok(tool_json_path);
    }
    write_tool_json(repo_root, worktree_path, tool_id)
}

pub fn ensure_performer(worktree_path: &Path) -> Result<PathBuf> {
    crate::ensure_performer(worktree_path)
}

pub fn resolve_worktree_task_context(
    repo_root: &Path,
    worktree_path: &Path,
    fallback_id: &str,
) -> Result<(String, PathBuf)> {
    crate::resolve_worktree_task_context(repo_root, worktree_path, fallback_id)
}
