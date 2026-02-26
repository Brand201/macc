use crate::commands::Command;
use crate::ToolCommands;
use macc_core::Result;
use std::path::{Path, PathBuf};

pub struct ToolCommand<'a> {
    cwd: PathBuf,
    command: &'a ToolCommands,
}

impl<'a> ToolCommand<'a> {
    pub fn new(cwd: &Path, command: &'a ToolCommands) -> Self {
        Self { cwd: cwd.to_path_buf(), command }
    }
}

impl<'a> Command for ToolCommand<'a> {
    fn run(&self) -> Result<()> {
        let paths = crate::services::project::ensure_initialized_paths(&self.cwd)?;
        match self.command {
            ToolCommands::Install { tool_id, yes } => crate::services::ops::install_tool(&paths, tool_id, *yes),
            ToolCommands::Update {
                tool_id,
                all,
                only,
                check,
                yes,
                force,
                rollback_on_fail,
            } => crate::services::ops::update_tools(
                &paths,
                tool_id.as_deref(),
                *all,
                only.as_deref(),
                *check,
                *yes,
                *force,
                *rollback_on_fail,
            ),
            ToolCommands::Outdated { only } => crate::services::ops::show_outdated_tools(&paths, only.as_deref()),
        }
    }
}
