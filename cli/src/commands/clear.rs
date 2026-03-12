use crate::commands::AppContext;
use crate::commands::Command;
use crate::services::interaction::CliInteraction;
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
        let _ = self
            .app
            .engine
            .clear_project(&paths, false, &CliInteraction)?;
        Ok(())
    }
}
