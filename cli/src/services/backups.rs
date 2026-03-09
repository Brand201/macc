use macc_core::{MaccError, Result};

pub fn list(paths: &macc_core::ProjectPaths, user: bool) -> Result<()> {
    let root = macc_core::domain::backups::backup_root(paths, user)?;
    let sets = macc_core::domain::backups::list_backup_sets(&root)?;
    if sets.is_empty() {
        println!("No backup sets in {}", root.display());
        return Ok(());
    }
    println!("Backup sets in {}:", root.display());
    for set in sets {
        let id = set.file_name().and_then(|v| v.to_str()).unwrap_or_default();
        let files = macc_core::domain::backups::count_files_recursive(&set)?;
        println!("  - {} ({} file(s))", id, files);
    }
    Ok(())
}

pub fn open(
    paths: &macc_core::ProjectPaths,
    id: Option<&str>,
    latest: bool,
    user: bool,
    editor: &Option<String>,
) -> Result<()> {
    let set = macc_core::domain::backups::resolve_backup_set_path(paths, user, id, latest)?;
    println!("Backup set: {}", set.display());
    if let Some(cmd) = editor {
        crate::services::worktree::open_in_editor(&set, cmd)?;
    }
    Ok(())
}

pub fn restore(
    paths: &macc_core::ProjectPaths,
    user: bool,
    id: Option<&str>,
    latest: bool,
    dry_run: bool,
    yes: bool,
) -> Result<()> {
    let set = macc_core::domain::backups::resolve_backup_set_path(paths, user, id, latest)?;
    let target_root = if user {
        macc_core::find_user_home().ok_or(MaccError::HomeDirNotFound)?
    } else {
        paths.root.clone()
    };

    let files = macc_core::domain::backups::collect_files_recursive(&set)?;
    if files.is_empty() {
        println!("Backup set {} is empty.", set.display());
        return Ok(());
    }

    println!("Restore source: {}", set.display());
    println!("Restore target: {}", target_root.display());
    println!("Files to restore: {}", files.len());
    if dry_run {
        for (idx, file) in files.iter().enumerate() {
            if idx >= 20 {
                println!("  ... and {} more", files.len() - idx);
                break;
            }
            let rel = file.strip_prefix(&set).unwrap_or(file.as_path());
            println!("  - {}", rel.display());
        }
        return Ok(());
    }

    if !yes && !crate::services::project::confirm_yes_no("Proceed with restore [y/N]? ")? {
        return Err(MaccError::Validation("Restore cancelled.".into()));
    }

    let mut restored = 0usize;
    for file in files {
        let rel = file.strip_prefix(&set).map_err(|e| {
            MaccError::Validation(format!(
                "Failed to compute backup relative path for {}: {}",
                file.display(),
                e
            ))
        })?;
        let destination = target_root.join(rel);
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(|e| MaccError::Io {
                path: parent.to_string_lossy().into(),
                action: "create restore parent directory".into(),
                source: e,
            })?;
        }
        std::fs::copy(&file, &destination).map_err(|e| MaccError::Io {
            path: file.to_string_lossy().into(),
            action: format!("restore to {}", destination.display()),
            source: e,
        })?;
        restored += 1;
    }
    println!("Restored {} file(s).", restored);
    Ok(())
}
