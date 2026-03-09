use crate::NativeCoordinatorLogger;
use macc_core::coordinator::state_runtime as core_runtime;
use macc_core::Result;
use std::path::Path;

pub(crate) fn coordinator_pause_file_path(repo_root: &Path) -> std::path::PathBuf {
    core_runtime::coordinator_pause_file_path(repo_root)
}

pub(crate) fn write_coordinator_pause_file(
    repo_root: &Path,
    task_id: &str,
    phase: &str,
    reason: &str,
) -> Result<()> {
    core_runtime::write_coordinator_pause_file(repo_root, task_id, phase, reason)
}

pub(crate) fn set_task_paused_for_integrate(
    repo_root: &Path,
    task_id: &str,
    reason: &str,
) -> Result<()> {
    core_runtime::set_task_paused_for_integrate(repo_root, task_id, reason)
}

pub(crate) fn resume_paused_task_integrate(repo_root: &Path, task_id: &str) -> Result<()> {
    core_runtime::resume_paused_task_integrate(repo_root, task_id)
}

pub(crate) fn cleanup_dead_runtime_tasks(
    repo_root: &Path,
    reason: &str,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<usize> {
    let adapter = logger.map(|log| {
        move |line: String| {
            let _ = log.note(line);
        }
    });
    core_runtime::cleanup_dead_runtime_tasks(
        repo_root,
        reason,
        adapter.as_ref().map(|f| f as &dyn Fn(String)),
    )
}

pub(crate) fn reconcile_registry_native(repo_root: &Path) -> Result<()> {
    core_runtime::reconcile_registry_native(repo_root)
}

pub(crate) fn cleanup_registry_native(repo_root: &Path) -> Result<()> {
    core_runtime::cleanup_registry_native(repo_root)
}
