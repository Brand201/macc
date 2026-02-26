use crate::commands::Command;
use macc_core::Result;
use std::path::{Path, PathBuf};

pub struct RestoreCommand<'a> {
    cwd: PathBuf,
    latest: bool,
    user: bool,
    backup: Option<&'a str>,
    dry_run: bool,
    yes: bool,
}

impl<'a> RestoreCommand<'a> {
    pub fn new(
        cwd: &Path,
        latest: bool,
        user: bool,
        backup: Option<&'a str>,
        dry_run: bool,
        yes: bool,
    ) -> Self {
        Self { cwd: cwd.to_path_buf(), latest, user, backup, dry_run, yes }
    }
}

impl<'a> Command for RestoreCommand<'a> {
    fn run(&self) -> Result<()> {
        let paths = macc_core::find_project_root(&self.cwd)?;
        if !self.latest && self.backup.is_none() {
            return Err(macc_core::MaccError::Validation(
                "restore requires --latest or --backup <id>".into(),
            ));
        }
        crate::services::backups::restore(
            &paths,
            self.user,
            self.backup,
            self.latest,
            self.dry_run,
            self.yes,
        )
    }
}
