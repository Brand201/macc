use crate::service::worktree::{
    coordinator_task_registry_path, ensure_performer, ensure_tool_json, resolve_worktree_path,
    resolve_worktree_task_context,
};
use crate::{read_worktree_metadata, MaccError, ProjectPaths, Result};
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

pub fn worktree_run_task(paths: &ProjectPaths, id: &str) -> Result<()> {
    let worktree_path = resolve_worktree_path(&paths.root, id)?;
    if !worktree_path.exists() {
        return Err(MaccError::Validation(format!(
            "Worktree path does not exist: {}",
            worktree_path.display()
        )));
    }

    let metadata = read_worktree_metadata(&worktree_path)?
        .ok_or_else(|| MaccError::Validation("Missing .macc/worktree.json".into()))?;
    ensure_tool_json(&paths.root, &worktree_path, &metadata.tool)?;
    let (task_id, prd_path) =
        resolve_worktree_task_context(&paths.root, &worktree_path, &metadata.id)?;
    let performer_path = ensure_performer(&worktree_path)?;
    let registry_path = coordinator_task_registry_path(&paths.root);
    let events_file = paths
        .root
        .join(".macc")
        .join("log")
        .join("coordinator")
        .join("events.jsonl");
    if let Some(parent) = events_file.parent() {
        std::fs::create_dir_all(parent).map_err(|e| MaccError::Io {
            path: parent.to_string_lossy().into(),
            action: "create coordinator log dir for events".into(),
            source: e,
        })?;
    }

    let status = Command::new(&performer_path)
        .current_dir(&worktree_path)
        .env(
            "COORD_EVENTS_FILE",
            events_file.to_string_lossy().to_string(),
        )
        .env(
            "COORDINATOR_RUN_ID",
            crate::service::project::ensure_coordinator_run_id(),
        )
        .env(
            "MACC_EVENT_SOURCE",
            format!("worktree-run:{}:{}", task_id, now_nanos()),
        )
        .env("MACC_EVENT_TASK_ID", &task_id)
        .arg("--repo")
        .arg(&paths.root)
        .arg("--worktree")
        .arg(&worktree_path)
        .arg("--task-id")
        .arg(&task_id)
        .arg("--tool")
        .arg(&metadata.tool)
        .arg("--registry")
        .arg(&registry_path)
        .arg("--prd")
        .arg(&prd_path)
        .status()
        .map_err(|e| MaccError::Io {
            path: performer_path.to_string_lossy().into(),
            action: "run worktree performer".into(),
            source: e,
        })?;

    if !status.success() {
        return Err(MaccError::Validation(format!(
            "Performer failed with status: {}. Inspect logs with `macc logs tail --component performer --worktree {}` and if the task is stuck run `macc coordinator unlock --task {}`.",
            status, metadata.id, task_id
        )));
    }
    Ok(())
}

pub fn worktree_exec(paths: &ProjectPaths, id: &str, cmd: &[String]) -> Result<()> {
    let worktree_path = resolve_worktree_path(&paths.root, id)?;
    if !worktree_path.exists() {
        return Err(MaccError::Validation(format!(
            "Worktree path does not exist: {}",
            worktree_path.display()
        )));
    }
    if cmd.is_empty() {
        return Err(MaccError::Validation(
            "worktree exec requires a command after --".into(),
        ));
    }

    let mut command = Command::new(&cmd[0]);
    if cmd.len() > 1 {
        command.args(&cmd[1..]);
    }
    let status = command
        .current_dir(&worktree_path)
        .status()
        .map_err(|e| MaccError::Io {
            path: worktree_path.to_string_lossy().into(),
            action: "run worktree exec".into(),
            source: e,
        })?;
    if !status.success() {
        return Err(MaccError::Validation(format!(
            "Command failed with status: {}",
            status
        )));
    }
    Ok(())
}

pub fn worktree_path_for_id(paths: &ProjectPaths, id: &str) -> Result<PathBuf> {
    resolve_worktree_path(&paths.root, id)
}

pub fn open_in_editor(path: &std::path::Path, command: &str) -> Result<()> {
    let mut parts = command.split_whitespace();
    let Some(bin) = parts.next() else {
        return Ok(());
    };
    let mut cmd = Command::new(bin);
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
        ("x-terminal-emulator", &["-e", "bash", "-lc"][..]),
        ("gnome-terminal", &["--", "bash", "-lc"][..]),
        ("konsole", &["-e", "bash", "-lc"][..]),
        ("xterm", &["-e", "bash", "-lc"][..]),
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
    let mut cmd = Command::new(bin);
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
    let mut cmd = Command::new(bin);
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
