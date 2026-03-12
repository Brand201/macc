use crate::commands::catalog_support::{CliCatalogInstallBackend, CliCatalogUi};
use crate::commands::AppContext;
use crate::commands::Command;
use crate::InstallCommands;
use macc_core::Result;

pub struct InstallCommand<'a> {
    app: AppContext,
    command: &'a InstallCommands,
}

impl<'a> InstallCommand<'a> {
    pub fn new(app: AppContext, command: &'a InstallCommands) -> Self {
        Self { app, command }
    }
}

impl<'a> Command for InstallCommand<'a> {
    fn run(&self) -> Result<()> {
        let paths = self.app.project_paths()?;
        let backend = CliCatalogInstallBackend { app: &self.app };
        match self.command {
            InstallCommands::Skill { tool, id } => {
                self.app
                    .engine
                    .catalog_install_skill(&paths, tool, id, &backend, &CliCatalogUi)
            }
            InstallCommands::Mcp { id } => {
                self.app
                    .engine
                    .catalog_install_mcp(&paths, id, &backend, &CliCatalogUi)
            }
        }
    }
}
