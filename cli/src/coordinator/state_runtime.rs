#[cfg(test)]
use macc_core::coordinator::state_runtime as core_runtime;
#[cfg(test)]
use macc_core::Result;
#[cfg(test)]
use std::path::Path;

#[cfg(test)]
pub(crate) fn reconcile_registry_native(repo_root: &Path) -> Result<()> {
    core_runtime::reconcile_registry_native(repo_root)
}

#[cfg(test)]
pub(crate) fn cleanup_registry_native(repo_root: &Path) -> Result<()> {
    core_runtime::cleanup_registry_native(repo_root)
}
