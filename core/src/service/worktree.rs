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

#[derive(Debug, Clone, Copy, Default)]
pub struct WorktreeSetupOptions {
    pub skip_apply: bool,
    pub allow_user_scope: bool,
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

pub fn apply_all_worktrees(
    engine: &(impl Engine + ?Sized),
    fetch_materializer: &dyn WorktreeFetchMaterializer,
    repo_root: &Path,
    allow_user_scope: bool,
) -> Result<usize> {
    let entries = crate::list_worktrees(repo_root)?;
    let root = repo_root
        .canonicalize()
        .unwrap_or_else(|_| repo_root.to_path_buf());
    let mut applied = 0usize;
    for entry in entries {
        if entry.path == root {
            continue;
        }
        apply_worktree(
            engine,
            fetch_materializer,
            repo_root,
            &entry.path,
            allow_user_scope,
        )?;
        applied += 1;
    }
    Ok(applied)
}

pub fn setup_worktrees_workflow(
    engine: &(impl Engine + ?Sized),
    fetch_materializer: &dyn WorktreeFetchMaterializer,
    repo_root: &Path,
    spec: &crate::WorktreeCreateSpec,
    options: WorktreeSetupOptions,
) -> Result<Vec<crate::WorktreeCreateResult>> {
    let repo_paths = crate::ProjectPaths::from_root(repo_root);
    let canonical = load_canonical_config(&repo_paths.config_path)?;
    let yaml = canonical.to_yaml().map_err(|e| {
        MaccError::Validation(format!("Failed to serialize config for worktree: {}", e))
    })?;

    let created = crate::create_worktrees(repo_root, spec)?;
    for entry in &created {
        let worktree_paths = crate::ProjectPaths::from_root(&entry.path);
        crate::init(&worktree_paths, false)?;
        crate::atomic_write(
            &worktree_paths,
            &worktree_paths.config_path,
            yaml.as_bytes(),
        )?;

        let tool_id = crate::read_worktree_metadata(&entry.path)?
            .and_then(|m| {
                if m.tool.is_empty() {
                    None
                } else {
                    Some(m.tool)
                }
            })
            .unwrap_or_else(|| spec.tool.clone());
        write_tool_json(repo_root, &entry.path, &tool_id)?;

        if !options.skip_apply {
            apply_worktree(
                engine,
                fetch_materializer,
                repo_root,
                &entry.path,
                options.allow_user_scope,
            )?;
        }
    }

    Ok(created)
}

pub fn setup_worktree_workflow(
    engine: &(impl Engine + ?Sized),
    fetch_materializer: &dyn WorktreeFetchMaterializer,
    repo_root: &Path,
    spec: &crate::WorktreeCreateSpec,
    options: WorktreeSetupOptions,
) -> Result<Vec<crate::WorktreeCreateResult>> {
    setup_worktrees_workflow(engine, fetch_materializer, repo_root, spec, options)
}
