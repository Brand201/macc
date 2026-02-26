use macc_core::engine::Engine;
use macc_core::Result;

pub fn handle_init_command<E: Engine>(
    absolute_cwd: &std::path::Path,
    engine: &E,
    force: bool,
    wizard: bool,
) -> Result<()> {
    crate::handle_init_command(absolute_cwd, engine, force, wizard)
}

pub fn handle_plan_command<E: Engine>(
    absolute_cwd: &std::path::Path,
    engine: &E,
    tools: Option<&str>,
    json: bool,
    explain: bool,
) -> Result<()> {
    crate::handle_plan_command(absolute_cwd, engine, tools, json, explain)
}

pub fn handle_apply_command<E: Engine>(
    absolute_cwd: &std::path::Path,
    engine: &E,
    tools: Option<&str>,
    dry_run: bool,
    allow_user_scope: bool,
    json: bool,
    explain: bool,
) -> Result<()> {
    crate::handle_apply_command(
        absolute_cwd,
        engine,
        tools,
        dry_run,
        allow_user_scope,
        json,
        explain,
    )
}

pub fn run_quickstart<E: Engine>(
    absolute_cwd: &std::path::Path,
    engine: &E,
    assume_yes: bool,
    apply: bool,
    no_tui: bool,
) -> Result<()> {
    crate::run_quickstart(absolute_cwd, engine, assume_yes, apply, no_tui)
}

pub fn list_backup_sets_command(paths: &macc_core::ProjectPaths, user: bool) -> Result<()> {
    crate::list_backup_sets_command(paths, user)
}

pub fn open_backup_set_command(
    paths: &macc_core::ProjectPaths,
    id: Option<&str>,
    latest: bool,
    user: bool,
    editor: &Option<String>,
) -> Result<()> {
    crate::open_backup_set_command(paths, id, latest, user, editor)
}

pub fn restore_backup_set_command(
    paths: &macc_core::ProjectPaths,
    user: bool,
    backup: Option<&str>,
    latest: bool,
    dry_run: bool,
    yes: bool,
) -> Result<()> {
    crate::restore_backup_set_command(paths, user, backup, latest, dry_run, yes)
}

pub fn confirm_yes_no(prompt: &str) -> Result<bool> {
    crate::confirm_yes_no(prompt)
}

pub fn remove_all_worktrees(root: &std::path::Path, remove_branches: bool) -> Result<usize> {
    crate::remove_all_worktrees(root, remove_branches)
}

pub fn run_context_generation(
    paths: &macc_core::ProjectPaths,
    tool: Option<&str>,
    from_files: &[String],
    dry_run: bool,
    print_prompt: bool,
) -> Result<()> {
    crate::run_context_generation(paths, tool, from_files, dry_run, print_prompt)
}

pub fn install_tool(paths: &macc_core::ProjectPaths, tool_id: &str, assume_yes: bool) -> Result<()> {
    crate::install_tool(paths, tool_id, assume_yes)
}

pub fn update_tools(
    paths: &macc_core::ProjectPaths,
    tool_id: Option<&str>,
    all: bool,
    only: Option<&str>,
    check: bool,
    assume_yes: bool,
    force: bool,
    rollback_on_fail: bool,
) -> Result<()> {
    crate::update_tools(
        paths,
        crate::ToolUpdateCommandOptions {
            tool_id,
            all,
            only,
            check,
            assume_yes,
            force,
            rollback_on_fail,
        },
    )
}

pub fn show_outdated_tools(paths: &macc_core::ProjectPaths, only: Option<&str>) -> Result<()> {
    crate::show_outdated_tools(paths, only)
}

pub fn list_skills(catalog: &macc_core::catalog::SkillsCatalog) {
    crate::list_skills(catalog)
}

pub fn search_skills(catalog: &macc_core::catalog::SkillsCatalog, query: &str) {
    crate::search_skills(catalog, query)
}

#[allow(clippy::too_many_arguments)]
pub fn add_skill(
    paths: &macc_core::ProjectPaths,
    catalog: &mut macc_core::catalog::SkillsCatalog,
    id: String,
    name: String,
    description: String,
    tags: Option<String>,
    subpath: String,
    kind: String,
    url: String,
    reference: String,
    checksum: Option<String>,
) -> Result<()> {
    crate::add_skill(
        paths,
        catalog,
        id,
        name,
        description,
        tags,
        subpath,
        kind,
        url,
        reference,
        checksum,
    )
}

pub fn remove_skill(
    paths: &macc_core::ProjectPaths,
    catalog: &mut macc_core::catalog::SkillsCatalog,
    id: String,
) -> Result<()> {
    crate::remove_skill(paths, catalog, id)
}

pub fn list_mcp(catalog: &macc_core::catalog::McpCatalog) {
    crate::list_mcp(catalog)
}

pub fn search_mcp(catalog: &macc_core::catalog::McpCatalog, query: &str) {
    crate::search_mcp(catalog, query)
}

#[allow(clippy::too_many_arguments)]
pub fn add_mcp(
    paths: &macc_core::ProjectPaths,
    catalog: &mut macc_core::catalog::McpCatalog,
    id: String,
    name: String,
    description: String,
    tags: Option<String>,
    subpath: String,
    kind: String,
    url: String,
    reference: String,
    checksum: Option<String>,
) -> Result<()> {
    crate::add_mcp(
        paths,
        catalog,
        id,
        name,
        description,
        tags,
        subpath,
        kind,
        url,
        reference,
        checksum,
    )
}

pub fn remove_mcp(
    paths: &macc_core::ProjectPaths,
    catalog: &mut macc_core::catalog::McpCatalog,
    id: String,
) -> Result<()> {
    crate::remove_mcp(paths, catalog, id)
}

#[allow(clippy::too_many_arguments)]
pub fn import_url(
    paths: &macc_core::ProjectPaths,
    kind: &String,
    id: String,
    url: String,
    name: Option<String>,
    description: String,
    tags: Option<String>,
) -> Result<()> {
    crate::import_url(paths, kind, id, url, name, description, tags)
}

pub fn run_remote_search(
    paths: &macc_core::ProjectPaths,
    api: String,
    kind: String,
    q: String,
    add: bool,
    add_ids: Option<String>,
) -> Result<()> {
    crate::run_remote_search(paths, api, kind, q, add, add_ids)
}

pub fn install_skill<E: Engine>(
    paths: &macc_core::ProjectPaths,
    tool: &str,
    id: &str,
    engine: &E,
) -> Result<()> {
    crate::install_skill(paths, tool, id, engine)
}

pub fn install_mcp<E: Engine>(paths: &macc_core::ProjectPaths, id: &str, engine: &E) -> Result<()> {
    crate::install_mcp(paths, id, engine)
}

pub fn write_tool_json(
    repo_root: &std::path::Path,
    worktree_root: &std::path::Path,
    tool: &str,
) -> Result<std::path::PathBuf> {
    crate::services::worktree::write_tool_json(repo_root, worktree_root, tool)
}

pub fn load_worktree_session_labels(
    project_paths: Option<&macc_core::ProjectPaths>,
) -> Result<std::collections::BTreeMap<std::path::PathBuf, String>> {
    crate::services::worktree::load_worktree_session_labels(project_paths)
}

pub fn truncate_cell(value: &str, max: usize) -> String {
    crate::services::worktree::truncate_cell(value, max)
}

pub fn git_worktree_is_dirty(worktree: &std::path::Path) -> Result<bool> {
    crate::services::worktree::git_worktree_is_dirty(worktree)
}

pub fn canonicalize_path_fallback(path: &std::path::Path) -> std::path::PathBuf {
    crate::services::worktree::canonicalize_path_fallback(path)
}

pub fn resolve_worktree_path(root: &std::path::Path, id: &str) -> Result<std::path::PathBuf> {
    crate::services::worktree::resolve_worktree_path(root, id)
}

pub fn open_in_terminal(path: &std::path::Path) -> Result<()> {
    crate::services::worktree::open_in_terminal(path)
}

pub fn open_in_editor(path: &std::path::Path, command: &str) -> Result<()> {
    crate::services::worktree::open_in_editor(path, command)
}

pub fn apply_worktree<E: Engine>(
    engine: &E,
    repo_root: &std::path::Path,
    worktree_root: &std::path::Path,
    allow_user_scope: bool,
) -> Result<()> {
    crate::services::worktree::apply_worktree(engine, repo_root, worktree_root, allow_user_scope)
}

pub fn print_checks(checks: &[macc_core::doctor::ToolCheck]) {
    crate::print_checks(checks)
}

pub fn ensure_tool_json(
    repo_root: &std::path::Path,
    worktree_root: &std::path::Path,
    tool: &str,
) -> Result<std::path::PathBuf> {
    crate::services::worktree::ensure_tool_json(repo_root, worktree_root, tool)
}

pub fn resolve_worktree_task_context(
    repo_root: &std::path::Path,
    worktree_root: &std::path::Path,
    worktree_id: &str,
) -> Result<(String, std::path::PathBuf)> {
    crate::services::worktree::resolve_worktree_task_context(repo_root, worktree_root, worktree_id)
}

pub fn ensure_performer(_repo_root: &std::path::Path, worktree_root: &std::path::Path) -> Result<std::path::PathBuf> {
    crate::services::worktree::ensure_performer(worktree_root)
}

pub fn coordinator_task_registry_path(root: &std::path::Path) -> std::path::PathBuf {
    crate::services::worktree::coordinator_task_registry_path(root)
}

pub fn ensure_coordinator_run_id() -> String {
    crate::ensure_coordinator_run_id()
}

pub fn delete_branch(root: &std::path::Path, branch: Option<&str>, force: bool) -> Result<()> {
    crate::services::worktree::delete_branch(root, branch, force)
}
