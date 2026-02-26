use crate::commands::Command;
use crate::commands::AppContext;
use crate::InstallCommands;
use macc_core::Result;

pub struct InstallCommand<'a> {
    app: AppContext,
    command: &'a InstallCommands,
}

impl<'a> InstallCommand<'a> {
    pub fn new(
        app: AppContext,
        command: &'a InstallCommands,
    ) -> Self {
        Self { app, command }
    }
}

impl<'a> Command for InstallCommand<'a> {
    fn run(&self) -> Result<()> {
        let paths = self.app.project_paths()?;
        match self.command {
            InstallCommands::Skill { tool, id } => crate::services::catalog::install_skill(&paths, tool, id, &self.app.engine),
            InstallCommands::Mcp { id } => crate::services::catalog::install_mcp(&paths, id, &self.app.engine),
        }
    }
}
