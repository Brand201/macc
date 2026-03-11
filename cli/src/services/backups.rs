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
        crate::commands::worktree::open_in_editor(path, command)
    }
}

pub fn list(
    engine: &crate::services::engine_provider::SharedEngine,
    paths: &macc_core::ProjectPaths,
    user: bool,
) -> Result<()> {
    engine.backups_list(paths, user, &CliBackupsUi)
}

pub fn open(
    engine: &crate::services::engine_provider::SharedEngine,
    paths: &macc_core::ProjectPaths,
    id: Option<&str>,
    latest: bool,
    user: bool,
    editor: &Option<String>,
) -> Result<()> {
    engine.backups_open(paths, id, latest, user, editor, &CliBackupsUi)
}

pub fn restore(
    engine: &crate::services::engine_provider::SharedEngine,
    paths: &macc_core::ProjectPaths,
    user: bool,
    id: Option<&str>,
    latest: bool,
    dry_run: bool,
    yes: bool,
) -> Result<()> {
    engine.backups_restore(paths, user, id, latest, dry_run, yes, &CliBackupsUi)
}
