use crate::commands::AppContext;
use crate::commands::Command;
use crate::LogsCommands;
use macc_core::Result;

struct CliLogsUi;

impl macc_core::service::logs::LogsUi for CliLogsUi {
    fn print_line(&self, line: &str) {
        println!("{}", line);
    }
}
pub struct LogsCommand<'a> {
    app: AppContext,
    command: &'a LogsCommands,
}

impl<'a> LogsCommand<'a> {
    pub fn new(app: AppContext, command: &'a LogsCommands) -> Self {
        Self { app, command }
    }
}

impl<'a> Command for LogsCommand<'a> {
    fn run(&self) -> Result<()> {
        let paths = self.app.ensure_initialized_paths()?;
        match self.command {
            LogsCommands::Tail {
                component,
                worktree,
                task,
                lines,
                follow,
            } => {
                let file = self.app.engine.logs_select_file(
                    &paths,
                    component.as_str(),
                    worktree.as_deref(),
                    task.as_deref(),
                )?;
                println!("Log file: {}", file.display());
                if *follow {
                    self.app.engine.logs_tail_follow(&file, *lines)?;
                } else {
                    self.app.engine.logs_print_tail(&file, *lines, &CliLogsUi)?;
                }
                Ok(())
            }
        }
    }
}
