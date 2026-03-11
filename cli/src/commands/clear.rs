use crate::commands::AppContext;
use crate::commands::Command;
use macc_core::Result;
pub struct ClearCommand {
    app: AppContext,
}

impl ClearCommand {
    pub fn new(app: AppContext) -> Self {
        Self { app }
    }
}

impl Command for ClearCommand {
    fn run(&self) -> Result<()> {
        let paths = self.app.project_paths()?;
        println!("This will:");
        println!("  1) Remove all non-root worktrees (equivalent to: macc worktree remove --all --force)");
        println!("  2) Remove MACC-managed files/directories in this project (macc clear)");
        if !crate::confirm_yes_no("Continue [y/N]? ")? {
            return Err(macc_core::MaccError::Validation("Clear cancelled.".into()));
        }
        let removed = macc_core::service::worktree::remove_all_worktrees(&paths.root, false)?;
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
