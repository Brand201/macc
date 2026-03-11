use crate::commands::AppContext;
use crate::commands::Command;
use crate::LogsCommands;
use macc_core::Result;
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
                if component.eq_ignore_ascii_case("all")
                    || component.eq_ignore_ascii_case("performer")
                {
                    let _ = crate::coordinator::logs::aggregate_performer_logs(&paths.root);
                }
                let file = crate::services::logs::select_log_file(
                    &self.app.engine,
                    &paths,
                    component.as_str(),
                    worktree.as_deref(),
                    task.as_deref(),
                )?;
                println!("Log file: {}", file.display());
                if *follow {
                    crate::services::logs::tail_file_follow(&self.app.engine, &file, *lines)?;
                } else {
                    crate::services::logs::print_file_tail(&self.app.engine, &file, *lines)?;
                }
                Ok(())
            }
        }
    }
}
