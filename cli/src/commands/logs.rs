use crate::commands::Command;
use crate::LogsCommands;
use macc_core::Result;
use std::path::{Path, PathBuf};

pub struct LogsCommand<'a> {
    cwd: PathBuf,
    command: &'a LogsCommands,
}

impl<'a> LogsCommand<'a> {
    pub fn new(cwd: &Path, command: &'a LogsCommands) -> Self {
        Self { cwd: cwd.to_path_buf(), command }
    }
}

impl<'a> Command for LogsCommand<'a> {
    fn run(&self) -> Result<()> {
        let paths = crate::services::project::ensure_initialized_paths(&self.cwd)?;
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
                    &paths,
                    component.as_str(),
                    worktree.as_deref(),
                    task.as_deref(),
                )?;
                println!("Log file: {}", file.display());
                if *follow {
                    crate::services::logs::tail_file_follow(&file, *lines)?;
                } else {
                    crate::services::logs::print_file_tail(&file, *lines)?;
                }
                Ok(())
            }
        }
    }
}
