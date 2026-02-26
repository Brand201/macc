use crate::commands::Command;
use crate::commands::AppContext;
use crate::ToolCommands;
use macc_core::Result;
pub struct ToolCommand<'a> {
    app: AppContext,
    command: &'a ToolCommands,
}

impl<'a> ToolCommand<'a> {
    pub fn new(app: AppContext, command: &'a ToolCommands) -> Self {
        Self { app, command }
    }
}

impl<'a> Command for ToolCommand<'a> {
    fn run(&self) -> Result<()> {
        let paths = self.app.ensure_initialized_paths()?;
        match self.command {
            ToolCommands::Install { tool_id, yes } => crate::services::tooling::install_tool(&paths, tool_id, *yes),
            ToolCommands::Update {
                tool_id,
                all,
                only,
                check,
                yes,
                force,
                rollback_on_fail,
            } => crate::services::tooling::update_tools(
                &paths,
                crate::services::tooling::ToolUpdateCommandOptions {
                    tool_id: tool_id.as_deref(),
                    all: *all,
                    only: only.as_deref(),
                    check: *check,
                    assume_yes: *yes,
                    force: *force,
                    rollback_on_fail: *rollback_on_fail,
                },
            ),
            ToolCommands::Outdated { only } => crate::services::tooling::show_outdated_tools(&paths, only.as_deref()),
        }
    }
}
