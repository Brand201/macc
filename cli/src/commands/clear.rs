use crate::commands::Command;
use macc_core::Result;
use std::path::{Path, PathBuf};

pub struct ClearCommand {
    cwd: PathBuf,
}

impl ClearCommand {
    pub fn new(cwd: &Path) -> Self {
        Self { cwd: cwd.to_path_buf() }
    }
}

impl Command for ClearCommand {
    fn run(&self) -> Result<()> {
        let paths = macc_core::find_project_root(&self.cwd)?;
        println!("This will:");
        println!("  1) Remove all non-root worktrees (equivalent to: macc worktree remove --all --force)");
        println!("  2) Remove MACC-managed files/directories in this project (macc clear)");
        if !crate::services::ops::confirm_yes_no("Continue [y/N]? ")? {
            return Err(macc_core::MaccError::Validation("Clear cancelled.".into()));
        }
        let removed = crate::services::ops::remove_all_worktrees(&paths.root, false)?;
        macc_core::prune_worktrees(&paths.root)?;
        println!("Removed worktrees: {}", removed);
        let report = macc_core::clear(&paths)?;
        println!(
            "Cleared managed paths: removed={}, skipped={}",
            report.removed, report.skipped
        );
        Ok(())
    }
}
