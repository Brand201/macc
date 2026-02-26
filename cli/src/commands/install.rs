use crate::commands::Command;
use crate::InstallCommands;
use macc_core::engine::Engine;
use macc_core::Result;
use std::path::{Path, PathBuf};

pub struct InstallCommand<'a, E: Engine> {
    cwd: PathBuf,
    command: &'a InstallCommands,
    engine: &'a E,
}

impl<'a, E: Engine> InstallCommand<'a, E> {
    pub fn new(cwd: &Path, command: &'a InstallCommands, engine: &'a E) -> Self {
        Self { cwd: cwd.to_path_buf(), command, engine }
    }
}

impl<'a, E: Engine> Command for InstallCommand<'a, E> {
    fn run(&self) -> Result<()> {
        let paths = macc_core::find_project_root(&self.cwd)?;
        match self.command {
            InstallCommands::Skill { tool, id } => crate::services::ops::install_skill(&paths, tool, id, self.engine),
            InstallCommands::Mcp { id } => crate::services::ops::install_mcp(&paths, id, self.engine),
        }
    }
}
