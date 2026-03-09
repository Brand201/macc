use crate::{MaccError, ProjectPaths, Result};
use std::path::{Path, PathBuf};

pub fn user_backup_root() -> Result<PathBuf> {
    let home = crate::find_user_home().ok_or(MaccError::HomeDirNotFound)?;
    Ok(home.join(".macc/backups"))
}

pub fn backup_root(paths: &ProjectPaths, user: bool) -> Result<PathBuf> {
    if user {
        user_backup_root()
    } else {
        Ok(paths.backups_dir.clone())
    }
}

pub fn list_backup_sets(root: &Path) -> Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut sets = Vec::new();
    for entry in std::fs::read_dir(root).map_err(|e| MaccError::Io {
        path: root.to_string_lossy().into(),
        action: "read backup root".into(),
        source: e,
    })? {
        let entry = entry.map_err(|e| MaccError::Io {
            path: root.to_string_lossy().into(),
            action: "iterate backup root".into(),
            source: e,
        })?;
        let path = entry.path();
        if path.is_dir() {
            sets.push(path);
        }
    }
    sets.sort_by(|a, b| {
        let an = a.file_name().and_then(|v| v.to_str()).unwrap_or_default();
        let bn = b.file_name().and_then(|v| v.to_str()).unwrap_or_default();
        bn.cmp(an)
    });
    Ok(sets)
}

pub fn resolve_backup_set_path(
    paths: &ProjectPaths,
    user: bool,
    id: Option<&str>,
    latest: bool,
) -> Result<PathBuf> {
    let root = backup_root(paths, user)?;
    let sets = list_backup_sets(&root)?;
    if sets.is_empty() {
        return Err(MaccError::Validation(format!(
            "No backup sets found in {}",
            root.display()
        )));
    }

    if latest {
        return Ok(sets[0].clone());
    }

    let id = id.ok_or_else(|| {
        MaccError::Validation("backup id is required unless --latest is provided".into())
    })?;
    let candidate = root.join(id);
    if !candidate.is_dir() {
        return Err(MaccError::Validation(format!(
            "Backup set not found: {}",
            candidate.display()
        )));
    }
    Ok(candidate)
}

pub fn count_files_recursive(root: &Path) -> Result<usize> {
    Ok(collect_files_recursive(root)?.len())
}

pub fn collect_files_recursive(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current).map_err(|e| MaccError::Io {
            path: current.to_string_lossy().into(),
            action: "read backup set directory".into(),
            source: e,
        })? {
            let entry = entry.map_err(|e| MaccError::Io {
                path: current.to_string_lossy().into(),
                action: "iterate backup set directory".into(),
                source: e,
            })?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.is_file() {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}
