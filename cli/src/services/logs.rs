use macc_core::{MaccError, Result};

pub fn select_log_file(
    paths: &macc_core::ProjectPaths,
    component: &str,
    worktree_filter: Option<&str>,
    task_filter: Option<&str>,
) -> Result<std::path::PathBuf> {
    let normalized = component.to_ascii_lowercase();
    let mut files = Vec::new();

    if normalized == "all" || normalized == "coordinator" {
        files.extend(collect_log_files(&paths.macc_dir.join("log/coordinator"), None)?);
    }
    if normalized == "all" || normalized == "performer" {
        files.extend(collect_log_files(
            &paths.macc_dir.join("log/performer"),
            task_filter,
        )?);
        files.extend(collect_performer_worktree_logs(
            &paths.root,
            worktree_filter,
            task_filter,
        )?);
    }

    if files.is_empty() {
        return Err(MaccError::Validation(
            "No logs found. Run `macc coordinator run` or `macc worktree run <id>` first.".into(),
        ));
    }

    files.sort_by(|a, b| {
        let am = std::fs::metadata(a)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let bm = std::fs::metadata(b)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        bm.cmp(&am)
    });
    Ok(files[0].clone())
}

pub fn print_file_tail(path: &std::path::Path, lines: usize) -> Result<()> {
    let content = std::fs::read_to_string(path).map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "read log file".into(),
        source: e,
    })?;
    let all = content.lines().collect::<Vec<_>>();
    let start = all.len().saturating_sub(lines);
    for line in &all[start..] {
        println!("{}", line);
    }
    Ok(())
}

pub fn tail_file_follow(path: &std::path::Path, lines: usize) -> Result<()> {
    let status = std::process::Command::new("tail")
        .arg("-n")
        .arg(lines.to_string())
        .arg("-F")
        .arg(path)
        .status()
        .map_err(|e| MaccError::Io {
            path: "tail".into(),
            action: "follow log file".into(),
            source: e,
        })?;
    if !status.success() {
        return Err(MaccError::Validation(format!(
            "tail failed with status: {}",
            status
        )));
    }
    Ok(())
}

fn collect_log_files(
    dir: &std::path::Path,
    task_filter: Option<&str>,
) -> Result<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();
    if !dir.exists() {
        return Ok(files);
    }
    for entry in std::fs::read_dir(dir).map_err(|e| MaccError::Io {
        path: dir.to_string_lossy().into(),
        action: "read log directory".into(),
        source: e,
    })? {
        let path = entry
            .map_err(|e| MaccError::Io {
                path: dir.to_string_lossy().into(),
                action: "iterate log directory".into(),
                source: e,
            })?
            .path();
        if !path.is_file() {
            continue;
        }
        if let Some(filter) = task_filter {
            let name = path
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default();
            if !name.contains(filter) {
                continue;
            }
        }
        files.push(path);
    }
    Ok(files)
}

fn collect_performer_worktree_logs(
    root: &std::path::Path,
    worktree_filter: Option<&str>,
    task_filter: Option<&str>,
) -> Result<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();
    let base = root.join(".macc/worktree");
    if !base.exists() {
        return Ok(files);
    }
    for entry in std::fs::read_dir(&base).map_err(|e| MaccError::Io {
        path: base.to_string_lossy().into(),
        action: "read worktree log base".into(),
        source: e,
    })? {
        let wt = entry
            .map_err(|e| MaccError::Io {
                path: base.to_string_lossy().into(),
                action: "iterate worktree log base".into(),
                source: e,
            })?
            .path();
        if !wt.is_dir() {
            continue;
        }
        if let Some(filter) = worktree_filter {
            let needle = filter.to_ascii_lowercase();
            let text = wt.display().to_string().to_ascii_lowercase();
            if !text.contains(&needle) {
                continue;
            }
        }
        let log_dir = wt.join(".macc/log/performer");
        files.extend(collect_log_files(&log_dir, task_filter)?);
    }
    Ok(files)
}
