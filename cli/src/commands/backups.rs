use crate::commands::Command;
use crate::BackupsCommands;
use macc_core::Result;
use std::path::{Path, PathBuf};

pub struct BackupsCommand<'a> {
    cwd: PathBuf,
    command: &'a BackupsCommands,
}

impl<'a> BackupsCommand<'a> {
    pub fn new(cwd: &Path, command: &'a BackupsCommands) -> Self {
        Self { cwd: cwd.to_path_buf(), command }
    }
}

impl<'a> Command for BackupsCommand<'a> {
    fn run(&self) -> Result<()> {
        let paths = macc_core::find_project_root(&self.cwd)?;
        match self.command {
            BackupsCommands::List { user } => crate::services::ops::list_backup_sets_command(&paths, *user),
            BackupsCommands::Open {
                id,
                latest,
                user,
                editor,
            } => crate::services::ops::open_backup_set_command(&paths, id.as_deref(), *latest, *user, editor),
        }
    }
}
