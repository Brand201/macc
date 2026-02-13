use crate::{MaccError, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeEntry {
    pub path: PathBuf,
    pub head: Option<String>,
    pub branch: Option<String>,
    pub locked: bool,
    pub prunable: bool,
}

#[derive(Debug, Clone)]
pub struct WorktreeCreateSpec {
    pub slug: String,
    pub tool: String,
    pub count: usize,
    pub base: String,
    pub dir: PathBuf,
    pub scope: Option<String>,
    pub feature: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WorktreeCreateResult {
    pub id: String,
    pub path: PathBuf,
    pub branch: String,
    pub base: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorktreeMetadata {
    pub id: String,
    pub tool: String,
    pub scope: Option<String>,
    pub feature: Option<String>,
    pub base: String,
    pub branch: String,
}

pub fn list_worktrees(cwd: &Path) -> Result<Vec<WorktreeEntry>> {
    let output = Command::new("git")
        .args(["worktree", "list", "--porcelain"])
        .current_dir(cwd)
        .output()
        .map_err(|e| MaccError::Io {
            path: cwd.to_string_lossy().into(),
            action: "run git worktree list".into(),
            source: e,
        })?;

    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "git worktree list failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    let text = String::from_utf8_lossy(&output.stdout);
    Ok(parse_porcelain(&text))
}

pub fn current_worktree(cwd: &Path, entries: &[WorktreeEntry]) -> Option<WorktreeEntry> {
    let cwd = cwd.canonicalize().unwrap_or_else(|_| cwd.to_path_buf());
    entries.iter().find(|entry| entry.path == cwd).cloned()
}

pub fn create_worktrees(
    root: &Path,
    spec: &WorktreeCreateSpec,
) -> Result<Vec<WorktreeCreateResult>> {
    if spec.count == 0 {
        return Err(MaccError::Validation("worktree count must be >= 1".into()));
    }

    let base_dir = root.join(&spec.dir);
    std::fs::create_dir_all(&base_dir).map_err(|e| MaccError::Io {
        path: base_dir.to_string_lossy().into(),
        action: "create worktree base dir".into(),
        source: e,
    })?;

    let mut results = Vec::new();
    let suffix = generate_suffix();
    for idx in 1..=spec.count {
        let id = if spec.count == 1 {
            format!("{}-{}", spec.slug, suffix)
        } else {
            format!("{}-{}-{:02}", spec.slug, suffix, idx)
        };
        let branch = format!("ai/{}/{}", spec.tool, id);
        let path = base_dir.join(&id);

        let output = Command::new("git")
            .args([
                "worktree",
                "add",
                "-b",
                &branch,
                path.to_string_lossy().as_ref(),
                &spec.base,
            ])
            .current_dir(root)
            .output()
            .map_err(|e| MaccError::Io {
                path: root.to_string_lossy().into(),
                action: "run git worktree add".into(),
                source: e,
            })?;

        if !output.status.success() {
            return Err(MaccError::Validation(format!(
                "git worktree add failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        write_worktree_metadata(
            &path,
            WorktreeMetadata {
                id: id.clone(),
                tool: spec.tool.clone(),
                scope: spec.scope.clone(),
                feature: spec.feature.clone(),
                base: spec.base.clone(),
                branch: branch.clone(),
            },
        )?;

        if let Some(scope) = &spec.scope {
            write_scope_file(&path, scope)?;
        }

        results.push(WorktreeCreateResult {
            id,
            path,
            branch,
            base: spec.base.clone(),
        });
    }

    Ok(results)
}

pub fn remove_worktree(root: &Path, path: &Path, force: bool) -> Result<()> {
    let mut cmd = Command::new("git");
    cmd.args(["worktree", "remove"]);
    if force {
        cmd.arg("--force");
    }
    let output = cmd
        .arg(path.to_string_lossy().as_ref())
        .current_dir(root)
        .output()
        .map_err(|e| MaccError::Io {
            path: root.to_string_lossy().into(),
            action: "run git worktree remove".into(),
            source: e,
        })?;

    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "git worktree remove failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

pub fn prune_worktrees(root: &Path) -> Result<()> {
    let output = Command::new("git")
        .args(["worktree", "prune"])
        .current_dir(root)
        .output()
        .map_err(|e| MaccError::Io {
            path: root.to_string_lossy().into(),
            action: "run git worktree prune".into(),
            source: e,
        })?;

    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "git worktree prune failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

fn parse_porcelain(output: &str) -> Vec<WorktreeEntry> {
    let mut entries = Vec::new();
    let mut current: Option<WorktreeEntry> = None;

    for raw in output.lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }

        if let Some(rest) = line.strip_prefix("worktree ") {
            if let Some(entry) = current.take() {
                entries.push(entry);
            }
            current = Some(WorktreeEntry {
                path: PathBuf::from(rest),
                head: None,
                branch: None,
                locked: false,
                prunable: false,
            });
            continue;
        }

        let Some(entry) = current.as_mut() else {
            continue;
        };

        if let Some(rest) = line.strip_prefix("HEAD ") {
            entry.head = Some(rest.to_string());
        } else if let Some(rest) = line.strip_prefix("branch ") {
            entry.branch = Some(rest.to_string());
        } else if line.starts_with("locked") {
            entry.locked = true;
        } else if line.starts_with("prunable") {
            entry.prunable = true;
        }
    }

    if let Some(entry) = current.take() {
        entries.push(entry);
    }

    entries
}

fn write_worktree_metadata(path: &Path, metadata: WorktreeMetadata) -> Result<()> {
    let macc_dir = path.join(".macc");
    std::fs::create_dir_all(&macc_dir).map_err(|e| MaccError::Io {
        path: macc_dir.to_string_lossy().into(),
        action: "create .macc directory".into(),
        source: e,
    })?;

    let file_path = macc_dir.join("worktree.json");
    let content = serde_json::to_string_pretty(&metadata)
        .map_err(|e| MaccError::Validation(format!("Failed to serialize worktree.json: {}", e)))?;
    std::fs::write(&file_path, content).map_err(|e| MaccError::Io {
        path: file_path.to_string_lossy().into(),
        action: "write worktree.json".into(),
        source: e,
    })?;
    Ok(())
}

pub fn read_worktree_metadata(path: &Path) -> Result<Option<WorktreeMetadata>> {
    let file_path = path.join(".macc").join("worktree.json");
    if !file_path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&file_path).map_err(|e| MaccError::Io {
        path: file_path.to_string_lossy().into(),
        action: "read worktree.json".into(),
        source: e,
    })?;
    let metadata = serde_json::from_str(&content)
        .map_err(|e| MaccError::Validation(format!("Failed to parse worktree.json: {}", e)))?;
    Ok(Some(metadata))
}

fn write_scope_file(path: &Path, scope: &str) -> Result<()> {
    let macc_dir = path.join(".macc");
    std::fs::create_dir_all(&macc_dir).map_err(|e| MaccError::Io {
        path: macc_dir.to_string_lossy().into(),
        action: "create .macc directory".into(),
        source: e,
    })?;
    let scope_path = macc_dir.join("scope.md");
    std::fs::write(&scope_path, scope).map_err(|e| MaccError::Io {
        path: scope_path.to_string_lossy().into(),
        action: "write scope.md".into(),
        source: e,
    })?;
    Ok(())
}

fn generate_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_porcelain_output() {
        let sample = "worktree /repo\nHEAD 111111\nbranch refs/heads/main\n\nworktree /repo/.worktrees/feat\nHEAD 222222\nbranch refs/heads/feat\nlocked\nprunable Worktree is locked\n";
        let entries = parse_porcelain(sample);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, PathBuf::from("/repo"));
        assert_eq!(entries[0].head.as_deref(), Some("111111"));
        assert_eq!(entries[0].branch.as_deref(), Some("refs/heads/main"));
        assert!(!entries[0].locked);

        assert_eq!(entries[1].path, PathBuf::from("/repo/.worktrees/feat"));
        assert_eq!(entries[1].head.as_deref(), Some("222222"));
        assert_eq!(entries[1].branch.as_deref(), Some("refs/heads/feat"));
        assert!(entries[1].locked);
        assert!(entries[1].prunable);
    }
}
