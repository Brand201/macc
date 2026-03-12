use crate::commands::AppContext;
use crate::commands::Command;
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
pub struct RestoreCommand<'a> {
    app: AppContext,
    latest: bool,
    user: bool,
    backup: Option<&'a str>,
    dry_run: bool,
    yes: bool,
}

impl<'a> RestoreCommand<'a> {
    pub fn new(
        app: AppContext,
        latest: bool,
        user: bool,
        backup: Option<&'a str>,
        dry_run: bool,
        yes: bool,
    ) -> Self {
        Self {
            app,
            latest,
            user,
            backup,
            dry_run,
            yes,
        }
    }
}

impl<'a> Command for RestoreCommand<'a> {
    fn run(&self) -> Result<()> {
        let paths = self.app.project_paths()?;
        if !self.latest && self.backup.is_none() {
            return Err(macc_core::MaccError::Validation(
                "restore requires --latest or --backup <id>".into(),
            ));
        }
        self.app.engine.backups_restore(
            &paths,
            self.user,
            self.backup,
            self.latest,
            self.dry_run,
            self.yes,
            &CliBackupsUi,
        )
    }
}
