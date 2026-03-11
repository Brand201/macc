use crate::commands::AppContext;
use crate::commands::Command;
use crate::BackupsCommands;
use macc_core::Result;
pub struct BackupsCommand<'a> {
    app: AppContext,
    command: &'a BackupsCommands,
}

impl<'a> BackupsCommand<'a> {
    pub fn new(app: AppContext, command: &'a BackupsCommands) -> Self {
        Self { app, command }
    }
}

impl<'a> Command for BackupsCommand<'a> {
    fn run(&self) -> Result<()> {
        let paths = self.app.project_paths()?;
        match self.command {
            BackupsCommands::List { user } => {
                crate::services::backups::list(&self.app.engine, &paths, *user)
            }
            BackupsCommands::Open {
                id,
                latest,
                user,
                editor,
            } => crate::services::backups::open(
                &self.app.engine,
                &paths,
                id.as_deref(),
                *latest,
                *user,
                editor,
            ),
        }
    }
}
