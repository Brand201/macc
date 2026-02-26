use crate::commands::Command;
use crate::commands::AppContext;
use macc_core::Result;
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
        Self { app, latest, user, backup, dry_run, yes }
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
        crate::services::backups::restore(
            &paths,
            self.user,
            self.backup,
            self.latest,
            self.dry_run,
            self.yes,
        )
    }
}
