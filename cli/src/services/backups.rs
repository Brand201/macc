use macc_core::Result;

pub fn list(paths: &macc_core::ProjectPaths, user: bool) -> Result<()> {
    crate::list_backup_sets_command(paths, user)
}

pub fn open(
    paths: &macc_core::ProjectPaths,
    id: Option<&str>,
    latest: bool,
    user: bool,
    editor: &Option<String>,
) -> Result<()> {
    crate::open_backup_set_command(paths, id, latest, user, editor)
}

pub fn restore(
    paths: &macc_core::ProjectPaths,
    user: bool,
    backup: Option<&str>,
    latest: bool,
    dry_run: bool,
    yes: bool,
) -> Result<()> {
    crate::restore_backup_set_command(paths, user, backup, latest, dry_run, yes)
}
