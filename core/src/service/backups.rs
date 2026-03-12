use crate::service::interaction::InteractionHandler;
use crate::{MaccError, ProjectPaths, Result};

pub trait BackupsUi: InteractionHandler {
    fn open_in_editor(&self, path: &std::path::Path, command: &str) -> Result<()>;
}

pub fn list(paths: &ProjectPaths, user: bool, ui: &dyn BackupsUi) -> Result<()> {
    let root = crate::domain::backups::backup_root(paths, user)?;
    let sets = crate::domain::backups::list_backup_sets(&root)?;
    if sets.is_empty() {
        ui.info(&format!("No backup sets in {}", root.display()));
        return Ok(());
    }
    ui.info(&format!("Backup sets in {}:", root.display()));
    for set in sets {
        let id = set.file_name().and_then(|v| v.to_str()).unwrap_or_default();
        let files = crate::domain::backups::count_files_recursive(&set)?;
        ui.info(&format!("  - {} ({} file(s))", id, files));
    }
    Ok(())
}

pub fn open(
    paths: &ProjectPaths,
    id: Option<&str>,
    latest: bool,
    user: bool,
    editor: &Option<String>,
    ui: &dyn BackupsUi,
) -> Result<()> {
    let set = crate::domain::backups::resolve_backup_set_path(paths, user, id, latest)?;
    ui.info(&format!("Backup set: {}", set.display()));
    if let Some(cmd) = editor {
        ui.open_in_editor(&set, cmd)?;
    }
    Ok(())
}

pub fn restore(
    paths: &ProjectPaths,
    user: bool,
    id: Option<&str>,
    latest: bool,
    dry_run: bool,
    yes: bool,
    ui: &dyn BackupsUi,
) -> Result<()> {
    let set = crate::domain::backups::resolve_backup_set_path(paths, user, id, latest)?;
    let target_root = if user {
        crate::find_user_home().ok_or(MaccError::HomeDirNotFound)?
    } else {
        paths.root.clone()
    };

    let files = crate::domain::backups::collect_files_recursive(&set)?;
    if files.is_empty() {
        ui.info(&format!("Backup set {} is empty.", set.display()));
        return Ok(());
    }

    ui.info(&format!("Restore source: {}", set.display()));
    ui.info(&format!("Restore target: {}", target_root.display()));
    ui.info(&format!("Files to restore: {}", files.len()));
    if dry_run {
        for (idx, file) in files.iter().enumerate() {
            if idx >= 20 {
                ui.info(&format!("  ... and {} more", files.len() - idx));
                break;
            }
            let rel = file.strip_prefix(&set).unwrap_or(file.as_path());
            ui.info(&format!("  - {}", rel.display()));
        }
        return Ok(());
    }

    if !yes && !ui.confirm("Proceed with restore [y/N]? ")? {
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
    ui.info(&format!("Restored {} file(s).", restored));
    Ok(())
}
