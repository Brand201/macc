use crate::engine::Engine;
use crate::resolve::{resolve, resolve_fetch_units, CliOverrides};
use crate::{load_canonical_config, MaccError, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub trait WorktreeFetchMaterializer {
    fn materialize_fetch_units(
        &self,
        paths: &crate::ProjectPaths,
        units: Vec<crate::resolve::FetchUnit>,
    ) -> Result<Vec<crate::resolve::MaterializedFetchUnit>>;
}

pub fn coordinator_task_registry_path(root: &Path) -> PathBuf {
    crate::domain::worktree::coordinator_task_registry_path(root)
}

pub fn canonicalize_path_fallback(path: &Path) -> PathBuf {
    crate::domain::worktree::canonicalize_path_fallback(path)
}

pub fn truncate_cell(value: &str, max: usize) -> String {
    crate::domain::worktree::truncate_cell(value, max)
}

pub fn git_worktree_is_dirty(worktree: &Path) -> Result<bool> {
    crate::domain::worktree::git_worktree_is_dirty(worktree)
}

pub fn load_worktree_session_labels(
    project_paths: Option<&crate::ProjectPaths>,
) -> Result<BTreeMap<PathBuf, String>> {
    crate::domain::worktree::load_worktree_session_labels(project_paths)
}

pub fn resolve_worktree_path(root: &Path, id: &str) -> Result<PathBuf> {
    crate::domain::worktree::resolve_worktree_path(root, id)
}

pub fn delete_branch(root: &Path, branch: Option<&str>, force: bool) -> Result<()> {
    crate::domain::worktree::delete_branch(root, branch, force)
}

pub fn remove_all_worktrees(root: &Path, remove_branches: bool) -> Result<usize> {
    crate::domain::worktree::remove_all_worktrees(root, remove_branches)
}

pub fn write_tool_json(repo_root: &Path, worktree_path: &Path, tool_id: &str) -> Result<PathBuf> {
    crate::domain::worktree::write_tool_json(repo_root, worktree_path, tool_id)
}

pub fn ensure_tool_json(repo_root: &Path, worktree_path: &Path, tool_id: &str) -> Result<PathBuf> {
    crate::domain::worktree::ensure_tool_json(repo_root, worktree_path, tool_id)
}

pub fn ensure_performer(worktree_path: &Path) -> Result<PathBuf> {
    crate::domain::worktree::ensure_performer(worktree_path)
}

pub fn resolve_worktree_task_context(
    repo_root: &Path,
    worktree_path: &Path,
    fallback_id: &str,
) -> Result<(String, PathBuf)> {
    crate::domain::worktree::resolve_worktree_task_context(repo_root, worktree_path, fallback_id)
}

pub fn apply_worktree(
    engine: &(impl Engine + ?Sized),
    fetch_materializer: &dyn WorktreeFetchMaterializer,
    repo_root: &Path,
    worktree_root: &Path,
    allow_user_scope: bool,
) -> Result<()> {
    let paths = crate::ProjectPaths::from_root(worktree_root);
    let canonical = load_canonical_config(&paths.config_path)?;
    let metadata = crate::read_worktree_metadata(worktree_root)?
        .ok_or_else(|| MaccError::Validation("Missing .macc/worktree.json".into()))?;

    let (descriptors, diagnostics) = engine.list_tools(&paths);
    crate::service::project::report_diagnostics(
        &diagnostics,
        &crate::service::tooling::NoopReporter,
    );
    let allowed_tools: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();
    let overrides = CliOverrides::from_tools_csv(metadata.tool.as_str(), &allowed_tools)?;

    let resolved = resolve(&canonical, &overrides);
    let fetch_units = resolve_fetch_units(&paths, &resolved)?;
    let materialized_units = fetch_materializer.materialize_fetch_units(&paths, fetch_units)?;

    let mut plan = engine.plan(&paths, &canonical, &materialized_units, &overrides)?;
    let _ = engine.apply(&paths, &mut plan, allow_user_scope)?;
    crate::sync_context_files_from_root(repo_root, worktree_root, &canonical)?;
    Ok(())
}
