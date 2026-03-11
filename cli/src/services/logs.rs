use macc_core::Result;

struct CliLogsUi;

impl macc_core::service::logs::LogsUi for CliLogsUi {
    fn print_line(&self, line: &str) {
        println!("{}", line);
    }
}

pub fn select_log_file(
    engine: &crate::services::engine_provider::SharedEngine,
    paths: &macc_core::ProjectPaths,
    component: &str,
    worktree_filter: Option<&str>,
    task_filter: Option<&str>,
) -> Result<std::path::PathBuf> {
    engine.logs_select_file(paths, component, worktree_filter, task_filter)
}

pub fn print_file_tail(
    engine: &crate::services::engine_provider::SharedEngine,
    path: &std::path::Path,
    lines: usize,
) -> Result<()> {
    engine.logs_print_tail(path, lines, &CliLogsUi)
}

pub fn tail_file_follow(
    engine: &crate::services::engine_provider::SharedEngine,
    path: &std::path::Path,
    lines: usize,
) -> Result<()> {
    engine.logs_tail_follow(path, lines)
}
