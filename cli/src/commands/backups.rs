use crate::commands::AppContext;
use crate::commands::Command;
use crate::BackupsCommands;
use macc_core::service::interaction::InteractionHandler;
use macc_core::Result;

struct CliBackupsUi;

impl InteractionHandler for CliBackupsUi {
    fn info(&self, message: &str) {
        println!("{}", message);
    }

    fn warn(&self, message: &str) {
        eprintln!("{}", message);
    }

    fn error(&self, message: &str) {
        eprintln!("{}", message);
    }

    fn confirm_yes_no(&self, prompt: &str) -> Result<bool> {
        crate::confirm_yes_no(prompt)
    }
}

impl macc_core::service::backups::BackupsUi for CliBackupsUi {
    fn open_in_editor(&self, path: &std::path::Path, command: &str) -> Result<()> {
        macc_core::service::task_runner::open_in_editor(path, command)
    }
}
pub struct BackupsCommand<'a> {
    app: AppContext,
    command: &'a BackupsCommands,
}

impl<'a> BackupsCommand<'a> {
    pub fn new(app: AppContext, command: &'a BackupsCommands) -> Self {
        Self { app, command }
    }
}

impl<'a> Command for BackupsCommand<'a> {
    fn run(&self) -> Result<()> {
        let paths = self.app.project_paths()?;
        match self.command {
            BackupsCommands::List { user } => {
                self.app.engine.backups_list(&paths, *user, &CliBackupsUi)
            }
            BackupsCommands::Open {
                id,
                latest,
                user,
                editor,
            } => self.app.engine.backups_open(
                &paths,
                id.as_deref(),
                *latest,
                *user,
                editor,
                &CliBackupsUi,
            ),
        }
    }
}
