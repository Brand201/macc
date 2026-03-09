use macc_core::{MaccError, Result};
use std::collections::BTreeMap;

pub fn coordinator_task_registry_path(root: &std::path::Path) -> std::path::PathBuf {
    macc_core::domain::worktree::coordinator_task_registry_path(root)
}

pub fn canonicalize_path_fallback(path: &std::path::Path) -> std::path::PathBuf {
    macc_core::domain::worktree::canonicalize_path_fallback(path)
}

pub fn truncate_cell(value: &str, max: usize) -> String {
    macc_core::domain::worktree::truncate_cell(value, max)
}

pub fn git_worktree_is_dirty(worktree: &std::path::Path) -> Result<bool> {
    macc_core::domain::worktree::git_worktree_is_dirty(worktree)
}

pub fn load_worktree_session_labels(
    project_paths: Option<&macc_core::ProjectPaths>,
) -> Result<BTreeMap<std::path::PathBuf, String>> {
    macc_core::domain::worktree::load_worktree_session_labels(project_paths)
}

pub fn resolve_worktree_path(root: &std::path::Path, id: &str) -> Result<std::path::PathBuf> {
    macc_core::domain::worktree::resolve_worktree_path(root, id)
}

pub fn delete_branch(root: &std::path::Path, branch: Option<&str>, force: bool) -> Result<()> {
    macc_core::domain::worktree::delete_branch(root, branch, force)
}

pub fn remove_all_worktrees(root: &std::path::Path, remove_branches: bool) -> Result<usize> {
    macc_core::domain::worktree::remove_all_worktrees(root, remove_branches)
}

pub fn write_tool_json(
    repo_root: &std::path::Path,
    worktree_path: &std::path::Path,
    tool_id: &str,
) -> Result<std::path::PathBuf> {
    macc_core::domain::worktree::write_tool_json(repo_root, worktree_path, tool_id)
}

pub fn ensure_tool_json(
    repo_root: &std::path::Path,
    worktree_path: &std::path::Path,
    tool_id: &str,
) -> Result<std::path::PathBuf> {
    macc_core::domain::worktree::ensure_tool_json(repo_root, worktree_path, tool_id)
}

pub fn ensure_performer(worktree_path: &std::path::Path) -> Result<std::path::PathBuf> {
    macc_core::domain::worktree::ensure_performer(worktree_path)
}

pub fn resolve_worktree_task_context(
    repo_root: &std::path::Path,
    worktree_path: &std::path::Path,
    fallback_id: &str,
) -> Result<(String, std::path::PathBuf)> {
    macc_core::domain::worktree::resolve_worktree_task_context(repo_root, worktree_path, fallback_id)
}

pub fn apply_worktree(
    engine: &crate::services::engine_provider::SharedEngine,
    repo_root: &std::path::Path,
    worktree_root: &std::path::Path,
    allow_user_scope: bool,
) -> Result<()> {
    crate::apply_worktree(engine.as_ref(), repo_root, worktree_root, allow_user_scope)
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
