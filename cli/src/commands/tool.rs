use crate::commands::AppContext;
use crate::commands::Command;
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
        let reporter = crate::services::interaction::CliInteraction;
        match self.command {
            ToolCommands::Install { tool_id, yes } => {
                self.app
                    .engine
                    .tooling_install_tool(&paths, tool_id, *yes, &reporter)?;
                Ok(())
            }
            ToolCommands::Update {
                tool_id,
                all,
                only,
                check,
                yes,
                force,
                rollback_on_fail,
            } => {
                self.app.engine.tooling_update_tools(
                    &paths,
                    macc_core::service::tooling::ToolUpdateCommandOptions {
                        tool_id: tool_id.as_deref(),
                        all: *all,
                        only: only.as_deref(),
                        check: *check,
                        assume_yes: *yes,
                        force: *force,
                        rollback_on_fail: *rollback_on_fail,
                    },
                    &reporter,
                )?;
                Ok(())
            }
            ToolCommands::Outdated { only } => {
                self.app
                    .engine
                    .tooling_show_outdated(&paths, only.as_deref(), &reporter)?;
                Ok(())
            }
        }
    }
}
