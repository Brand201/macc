use macc_core::engine::Engine;
use macc_core::{MaccError, Result};
use std::collections::BTreeMap;

const COORDINATOR_TASK_REGISTRY_REL_PATH: &str = ".macc/automation/task/task_registry.json";

pub fn coordinator_task_registry_path(root: &std::path::Path) -> std::path::PathBuf {
    root.join(COORDINATOR_TASK_REGISTRY_REL_PATH)
}

pub fn canonicalize_path_fallback(path: &std::path::Path) -> std::path::PathBuf {
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

pub fn git_worktree_is_dirty(worktree: &std::path::Path) -> Result<bool> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(["status", "--porcelain"])
        .output()
        .map_err(|e| MaccError::Io {
            path: worktree.to_string_lossy().into(),
            action: "read git worktree status".into(),
            source: e,
        })?;
    if !output.status.success() {
        return Ok(false);
    }
    Ok(!String::from_utf8_lossy(&output.stdout).trim().is_empty())
}

pub fn load_worktree_session_labels(
    project_paths: Option<&macc_core::ProjectPaths>,
) -> Result<BTreeMap<std::path::PathBuf, String>> {
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
            let owner_path = canonicalize_path_fallback(std::path::Path::new(owner));
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

pub fn resolve_worktree_path(root: &std::path::Path, id: &str) -> Result<std::path::PathBuf> {
    let candidate = std::path::Path::new(id);
    Ok(if candidate.is_absolute() || id.contains(std::path::MAIN_SEPARATOR) {
        std::path::PathBuf::from(id)
    } else {
        root.join(".macc/worktree").join(id)
    })
}

pub fn delete_branch(root: &std::path::Path, branch: Option<&str>, force: bool) -> Result<()> {
    let Some(branch) = branch else {
        return Ok(());
    };
    let branch = branch.strip_prefix("refs/heads/").unwrap_or(branch);
    if branch.is_empty() {
        return Ok(());
    }

    let mut cmd = std::process::Command::new("git");
    cmd.arg("branch");
    if force {
        cmd.arg("-D");
    } else {
        cmd.arg("-d");
    }
    let output = cmd
        .arg(branch)
        .current_dir(root)
        .output()
        .map_err(|e| MaccError::Io {
            path: root.to_string_lossy().into(),
            action: "run git branch delete".into(),
            source: e,
        })?;

    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "git branch delete failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

pub fn remove_all_worktrees(root: &std::path::Path, remove_branches: bool) -> Result<usize> {
    let entries = macc_core::list_worktrees(root)?;
    let root_canon = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let mut removed = 0usize;

    for entry in entries {
        if entry.path == root_canon {
            continue;
        }
        let branch = entry.branch.clone();
        macc_core::remove_worktree(root, &entry.path, true)?;
        if remove_branches {
            delete_branch(root, branch.as_deref(), true)?;
        }
        removed += 1;
    }
    Ok(removed)
}

pub fn write_tool_json(
    repo_root: &std::path::Path,
    worktree_path: &std::path::Path,
    tool_id: &str,
) -> Result<std::path::PathBuf> {
    let search_paths = macc_core::tool::ToolSpecLoader::default_search_paths(repo_root);
    let loader = macc_core::tool::ToolSpecLoader::new(search_paths);
    let (specs, diagnostics) = loader.load_all_with_embedded();
    crate::services::project::report_diagnostics(&diagnostics);

    let spec = specs
        .into_iter()
        .find(|spec| spec.id == tool_id)
        .ok_or_else(|| MaccError::Validation(format!("Tool spec not found: {}", tool_id)))?;
    let mut runtime = spec.to_runtime_config().ok_or_else(|| {
        MaccError::Validation(format!("Tool spec missing performer section: {}", tool_id))
    })?;

    let worktree_paths = macc_core::ProjectPaths::from_root(worktree_path);
    let _ = macc_core::ensure_embedded_automation_scripts(&worktree_paths)?;
    if let Some(runner_path) =
        macc_core::embedded_runner_path_for_ref(&worktree_paths, &runtime.performer.runner)?
    {
        runtime.performer.runner = runner_path.to_string_lossy().into_owned();
    }

    let macc_dir = worktree_path.join(".macc");
    std::fs::create_dir_all(&macc_dir).map_err(|e| MaccError::Io {
        path: macc_dir.to_string_lossy().into(),
        action: "create .macc directory".into(),
        source: e,
    })?;

    let tool_json_path = macc_dir.join("tool.json");
    let content = serde_json::to_string_pretty(&runtime)
        .map_err(|e| MaccError::Validation(format!("Failed to serialize tool.json: {}", e)))?;
    std::fs::write(&tool_json_path, content).map_err(|e| MaccError::Io {
        path: tool_json_path.to_string_lossy().into(),
        action: "write tool.json".into(),
        source: e,
    })?;
    Ok(tool_json_path)
}

pub fn ensure_tool_json(
    repo_root: &std::path::Path,
    worktree_path: &std::path::Path,
    tool_id: &str,
) -> Result<std::path::PathBuf> {
    let tool_json_path = worktree_path.join(".macc").join("tool.json");
    if tool_json_path.exists() {
        return Ok(tool_json_path);
    }
    write_tool_json(repo_root, worktree_path, tool_id)
}

pub fn ensure_performer(worktree_path: &std::path::Path) -> Result<std::path::PathBuf> {
    let target = worktree_path.join("performer.sh");
    if target.exists() {
        return Ok(target);
    }

    let worktree_paths = macc_core::ProjectPaths::from_root(worktree_path);
    let _ = macc_core::ensure_embedded_automation_scripts(&worktree_paths)?;
    let source = worktree_paths.automation_performer_path();

    std::fs::copy(&source, &target).map_err(|e| MaccError::Io {
        path: target.to_string_lossy().into(),
        action: "copy performer.sh".into(),
        source: e,
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&target)
            .map_err(|e| MaccError::Io {
                path: target.to_string_lossy().into(),
                action: "read performer permissions".into(),
                source: e,
            })?
            .permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&target, perms).map_err(|e| MaccError::Io {
            path: target.to_string_lossy().into(),
            action: "set performer permissions".into(),
            source: e,
        })?;
    }

    Ok(target)
}

pub fn resolve_worktree_task_context(
    repo_root: &std::path::Path,
    worktree_path: &std::path::Path,
    fallback_id: &str,
) -> Result<(String, std::path::PathBuf)> {
    let prd_path = worktree_path.join("worktree.prd.json");
    if prd_path.exists() {
        let content = std::fs::read_to_string(&prd_path).map_err(|e| MaccError::Io {
            path: prd_path.to_string_lossy().into(),
            action: "read worktree.prd.json".into(),
            source: e,
        })?;
        let json: serde_json::Value = serde_json::from_str(&content).map_err(|e| {
            MaccError::Validation(format!("Failed to parse worktree.prd.json: {}", e))
        })?;
        let task_id = json
            .get("tasks")
            .and_then(|tasks| tasks.get(0))
            .and_then(|task| task.get("id"))
            .and_then(|id| match id {
                serde_json::Value::String(s) => Some(s.clone()),
                serde_json::Value::Number(n) => Some(n.to_string()),
                _ => None,
            })
            .ok_or_else(|| {
                MaccError::Validation("worktree.prd.json is missing tasks[0].id".into())
            })?;
        return Ok((task_id, prd_path));
    }

    let fallback_prd = repo_root.join("prd.json");
    if !fallback_prd.exists() {
        return Err(MaccError::Validation(
            "Missing worktree.prd.json and prd.json".into(),
        ));
    }
    Ok((fallback_id.to_string(), fallback_prd))
}

pub fn apply_worktree<E: Engine>(
    engine: &E,
    repo_root: &std::path::Path,
    worktree_root: &std::path::Path,
    allow_user_scope: bool,
) -> Result<()> {
    crate::apply_worktree(engine, repo_root, worktree_root, allow_user_scope)
}

pub fn open_in_editor(path: &std::path::Path, command: &str) -> Result<()> {
    let mut parts = command.split_whitespace();
    let Some(bin) = parts.next() else {
        return Ok(());
    };
    let mut cmd = std::process::Command::new(bin);
    for arg in parts {
        cmd.arg(arg);
    }
    let status = cmd.arg(path).status().map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "launch editor".into(),
        source: e,
    })?;
    if !status.success() {
        return Err(MaccError::Validation(format!(
            "Editor command failed with status: {}",
            status
        )));
    }
    Ok(())
}

pub fn open_in_terminal(path: &std::path::Path) -> Result<()> {
    if let Ok(term) = std::env::var("TERMINAL") {
        launch_terminal(&term, path)?;
        return Ok(());
    }

    let candidates = [
        ("x-terminal-emulator", &["-e", "bash", "-lc"]),
        ("gnome-terminal", &["--", "bash", "-lc"]),
        ("konsole", &["-e", "bash", "-lc"]),
        ("xterm", &["-e", "bash", "-lc"]),
    ];
    for (bin, prefix) in candidates {
        if launch_terminal_with_prefix(bin, prefix, path).is_ok() {
            return Ok(());
        }
    }

    Err(MaccError::Validation(
        "No terminal launcher found (set $TERMINAL)".into(),
    ))
}

fn launch_terminal(command: &str, path: &std::path::Path) -> Result<()> {
    let mut parts = command.split_whitespace();
    let Some(bin) = parts.next() else {
        return Ok(());
    };
    let mut cmd = std::process::Command::new(bin);
    for arg in parts {
        cmd.arg(arg);
    }
    cmd.arg("--");
    cmd.arg("bash");
    cmd.arg("-lc");
    cmd.arg(format!("cd {}; exec $SHELL", path.display()));
    cmd.spawn().map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "launch terminal".into(),
        source: e,
    })?;
    Ok(())
}

fn launch_terminal_with_prefix(bin: &str, prefix: &[&str], path: &std::path::Path) -> Result<()> {
    let mut cmd = std::process::Command::new(bin);
    for arg in prefix {
        cmd.arg(arg);
    }
    cmd.arg(format!("cd {}; exec $SHELL", path.display()));
    cmd.spawn().map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "launch terminal".into(),
        source: e,
    })?;
    Ok(())
}

fn unix_timestamp_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
