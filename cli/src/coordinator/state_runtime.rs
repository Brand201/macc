use macc_core::coordinator::state_runtime as core_runtime;
use macc_core::Result;
use std::path::Path;

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

pub(crate) fn reconcile_registry_native(repo_root: &Path) -> Result<()> {
    core_runtime::reconcile_registry_native(repo_root)
}

pub(crate) fn cleanup_registry_native(repo_root: &Path) -> Result<()> {
    core_runtime::cleanup_registry_native(repo_root)
}
