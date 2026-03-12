use crate::coordinator_storage::{
    CoordinatorSnapshot, CoordinatorStorage, CoordinatorStoragePaths, JsonStorage, SqliteStorage,
};
use crate::{
    catalog::{self, Agent, Skill},
    config::CanonicalConfig,
    coordinator,
    doctor::{self, ToolCheck},
    plan::{self, ActionPlan, PlannedOp},
    resolve::{self, CliOverrides, MaterializedFetchUnit},
    tool::{ToolDescriptor, ToolDiagnostic, ToolRegistry, ToolSpecLoader},
    ApplyReport, ProjectPaths, Result, WorktreeCreateResult, WorktreeCreateSpec, WorktreeEntry,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

/// The interface for UI (CLI/TUI) to interact with MACC core logic.
pub trait Engine {
    fn list_tools(&self, paths: &ProjectPaths) -> (Vec<ToolDescriptor>, Vec<ToolDiagnostic>);
    fn doctor(&self, paths: &ProjectPaths) -> Vec<ToolCheck>;
    fn plan(
        &self,
        paths: &ProjectPaths,
        config: &CanonicalConfig,
        materialized_units: &[MaterializedFetchUnit],
        overrides: &CliOverrides,
    ) -> Result<ActionPlan>;
    fn plan_operations(&self, paths: &ProjectPaths, plan: &ActionPlan) -> Vec<PlannedOp>;
    fn apply(
        &self,
        paths: &ProjectPaths,
        plan: &mut ActionPlan,
        allow_user_scope: bool,
    ) -> Result<ApplyReport>;

    fn builtin_skills(&self) -> Vec<Skill>;
    fn builtin_agents(&self) -> Vec<Agent>;

    fn list_worktrees(&self, root: &Path) -> Result<Vec<WorktreeEntry>> {
        crate::list_worktrees(root)
    }

    fn create_worktrees(
        &self,
        root: &Path,
        spec: &WorktreeCreateSpec,
    ) -> Result<Vec<WorktreeCreateResult>> {
        crate::create_worktrees(root, spec)
    }

    fn remove_worktree(&self, root: &Path, path: &Path, force: bool) -> Result<()> {
        crate::remove_worktree(root, path, force)
    }

    fn catalog_search_remote(
        &self,
        paths: &ProjectPaths,
        provider: &dyn crate::catalog::service::CatalogRemoteSearchProvider,
        api: &str,
        kind: crate::catalog::service::CatalogSearchKind,
        query: &str,
        add: bool,
        add_ids: Option<&str>,
    ) -> Result<crate::catalog::service::RemoteSearchOutcome> {
        crate::catalog::service::execute_remote_search(
            paths, provider, api, kind, query, add, add_ids,
        )
    }

    fn install_skill(
        &self,
        paths: &ProjectPaths,
        tool: &str,
        id: &str,
        backend: &dyn crate::catalog::service::CatalogInstallBackend,
    ) -> Result<crate::catalog::service::InstallSkillOutcome> {
        crate::catalog::service::install_skill(paths, tool, id, backend)
    }

    fn install_mcp(
        &self,
        paths: &ProjectPaths,
        id: &str,
        backend: &dyn crate::catalog::service::CatalogInstallBackend,
    ) -> Result<crate::catalog::service::InstallMcpOutcome> {
        crate::catalog::service::install_mcp(paths, id, backend)
    }

    fn tooling_install_tool(
        &self,
        paths: &ProjectPaths,
        tool_id: &str,
        assume_yes: bool,
        reporter: &dyn crate::service::tooling::UserReporter,
    ) -> Result<crate::service::tooling::InstallToolOutcome> {
        crate::service::tooling::install_tool(paths, tool_id, assume_yes, reporter)
    }

    fn tooling_update_tools(
        &self,
        paths: &ProjectPaths,
        options: crate::service::tooling::ToolUpdateCommandOptions<'_>,
        reporter: &dyn crate::service::tooling::UserReporter,
    ) -> Result<crate::service::tooling::ToolUpdateSummary> {
        crate::service::tooling::update_tools(paths, options, reporter)
    }

    fn tooling_show_outdated(
        &self,
        paths: &ProjectPaths,
        only: Option<&str>,
        reporter: &dyn crate::service::tooling::UserReporter,
    ) -> Result<crate::service::tooling::OutdatedToolsReport> {
        crate::service::tooling::show_outdated_tools(paths, only, reporter)
    }

    fn context_generate(
        &self,
        paths: &ProjectPaths,
        tool_filter: Option<&str>,
        from_files: &[String],
        dry_run: bool,
        print_prompt: bool,
        reporter: &dyn crate::service::interaction::InteractionHandler,
    ) -> Result<usize> {
        crate::service::context::run_generation(
            paths,
            tool_filter,
            from_files,
            dry_run,
            print_prompt,
            reporter,
        )
    }

    fn project_ensure_initialized_paths(&self, start_dir: &Path) -> Result<ProjectPaths> {
        crate::service::project::ensure_initialized_paths(start_dir)
    }

    fn project_run_doctor(
        &self,
        paths: &ProjectPaths,
        fix: bool,
        interaction: &dyn crate::service::interaction::InteractionHandler,
    ) -> Result<()> {
        crate::service::project::run_doctor(paths, self, fix, interaction)
    }

    fn migrate_project(
        &self,
        paths: &ProjectPaths,
        canonical: crate::config::CanonicalConfig,
        allowed_tools: &[String],
        apply: bool,
        ui: &dyn crate::service::interaction::InteractionHandler,
    ) -> Result<crate::service::migrate::MigrateOutcome> {
        crate::service::migrate::migrate_project(paths, canonical, allowed_tools, apply, ui)
    }

    fn project_ensure_coordinator_run_id(&self) -> String {
        crate::service::project::ensure_coordinator_run_id()
    }

    fn worktree_apply(
        &self,
        fetch_materializer: &dyn crate::service::worktree::WorktreeFetchMaterializer,
        repo_root: &Path,
        worktree_root: &Path,
        allow_user_scope: bool,
    ) -> Result<()> {
        crate::service::worktree::apply_worktree(
            self,
            fetch_materializer,
            repo_root,
            worktree_root,
            allow_user_scope,
        )
    }

    fn worktree_apply_all(
        &self,
        fetch_materializer: &dyn crate::service::worktree::WorktreeFetchMaterializer,
        repo_root: &Path,
        allow_user_scope: bool,
    ) -> Result<usize> {
        crate::service::worktree::apply_all_worktrees(
            self,
            fetch_materializer,
            repo_root,
            allow_user_scope,
        )
    }

    fn worktree_setup_workflow(
        &self,
        fetch_materializer: &dyn crate::service::worktree::WorktreeFetchMaterializer,
        repo_root: &Path,
        spec: &crate::WorktreeCreateSpec,
        options: crate::service::worktree::WorktreeSetupOptions,
    ) -> Result<Vec<crate::WorktreeCreateResult>> {
        crate::service::worktree::setup_worktrees_workflow(
            self,
            fetch_materializer,
            repo_root,
            spec,
            options,
        )
    }

    fn setup_worktree_workflow(
        &self,
        fetch_materializer: &dyn crate::service::worktree::WorktreeFetchMaterializer,
        repo_root: &Path,
        spec: &crate::WorktreeCreateSpec,
        options: crate::service::worktree::WorktreeSetupOptions,
    ) -> Result<Vec<crate::WorktreeCreateResult>> {
        crate::service::worktree::setup_worktree_workflow(
            self,
            fetch_materializer,
            repo_root,
            spec,
            options,
        )
    }

    fn worktree_run_task(&self, paths: &ProjectPaths, id: &str) -> Result<()> {
        crate::service::task_runner::worktree_run_task(paths, id)
    }

    fn worktree_exec_task(&self, paths: &ProjectPaths, id: &str, cmd: &[String]) -> Result<()> {
        crate::service::task_runner::worktree_exec(paths, id, cmd)
    }

    fn worktree_open_in_editor(&self, path: &Path, command: &str) -> Result<()> {
        crate::service::task_runner::open_in_editor(path, command)
    }

    fn worktree_open_in_terminal(&self, path: &Path) -> Result<()> {
        crate::service::task_runner::open_in_terminal(path)
    }

    fn clear_project(
        &self,
        paths: &ProjectPaths,
        force: bool,
        ui: &dyn crate::service::interaction::InteractionHandler,
    ) -> Result<crate::service::clear::ClearExecutionReport> {
        crate::service::clear::clear_project(paths, force, ui)
    }

    fn catalog_run_remote_search(
        &self,
        paths: &ProjectPaths,
        provider: &dyn crate::catalog::service::CatalogRemoteSearchProvider,
        api: &str,
        kind: &str,
        query: &str,
        add: bool,
        add_ids: Option<&str>,
        ui: &dyn crate::service::catalog::CatalogUi,
    ) -> Result<()> {
        crate::service::catalog::run_remote_search(
            self, paths, provider, api, kind, query, add, add_ids, ui,
        )
    }

    fn catalog_list_skills(
        &self,
        catalog: &crate::catalog::SkillsCatalog,
        ui: &dyn crate::service::catalog::CatalogUi,
    ) {
        crate::service::catalog::list_skills(catalog, ui)
    }

    fn catalog_search_skills(
        &self,
        catalog: &crate::catalog::SkillsCatalog,
        query: &str,
        ui: &dyn crate::service::catalog::CatalogUi,
    ) {
        crate::service::catalog::search_skills(catalog, query, ui)
    }

    #[allow(clippy::too_many_arguments)]
    fn catalog_add_skill(
        &self,
        paths: &ProjectPaths,
        catalog: &mut crate::catalog::SkillsCatalog,
        id: String,
        name: String,
        description: String,
        tags: Option<String>,
        subpath: String,
        kind: String,
        url: String,
        reference: String,
        checksum: Option<String>,
        ui: &dyn crate::service::catalog::CatalogUi,
    ) -> Result<()> {
        crate::service::catalog::add_skill(
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
            ui,
        )
    }

    fn catalog_remove_skill(
        &self,
        paths: &ProjectPaths,
        catalog: &mut crate::catalog::SkillsCatalog,
        id: String,
        ui: &dyn crate::service::catalog::CatalogUi,
    ) -> Result<()> {
        crate::service::catalog::remove_skill(paths, catalog, id, ui)
    }

    fn catalog_list_mcp(
        &self,
        catalog: &crate::catalog::McpCatalog,
        ui: &dyn crate::service::catalog::CatalogUi,
    ) {
        crate::service::catalog::list_mcp(catalog, ui)
    }

    fn catalog_search_mcp(
        &self,
        catalog: &crate::catalog::McpCatalog,
        query: &str,
        ui: &dyn crate::service::catalog::CatalogUi,
    ) {
        crate::service::catalog::search_mcp(catalog, query, ui)
    }

    #[allow(clippy::too_many_arguments)]
    fn catalog_add_mcp(
        &self,
        paths: &ProjectPaths,
        catalog: &mut crate::catalog::McpCatalog,
        id: String,
        name: String,
        description: String,
        tags: Option<String>,
        subpath: String,
        kind: String,
        url: String,
        reference: String,
        checksum: Option<String>,
        ui: &dyn crate::service::catalog::CatalogUi,
    ) -> Result<()> {
        crate::service::catalog::add_mcp(
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
            ui,
        )
    }

    fn catalog_remove_mcp(
        &self,
        paths: &ProjectPaths,
        catalog: &mut crate::catalog::McpCatalog,
        id: String,
        ui: &dyn crate::service::catalog::CatalogUi,
    ) -> Result<()> {
        crate::service::catalog::remove_mcp(paths, catalog, id, ui)
    }

    fn catalog_install_skill(
        &self,
        paths: &ProjectPaths,
        tool: &str,
        id: &str,
        backend: &dyn crate::catalog::service::CatalogInstallBackend,
        ui: &dyn crate::service::catalog::CatalogUi,
    ) -> Result<()> {
        crate::service::catalog::install_skill(paths, tool, id, self, backend, ui)
    }

    fn catalog_install_mcp(
        &self,
        paths: &ProjectPaths,
        id: &str,
        backend: &dyn crate::catalog::service::CatalogInstallBackend,
        ui: &dyn crate::service::catalog::CatalogUi,
    ) -> Result<()> {
        crate::service::catalog::install_mcp(paths, id, self, backend, ui)
    }

    fn catalog_import_url(
        &self,
        paths: &ProjectPaths,
        kind: &str,
        id: String,
        url: String,
        name: Option<String>,
        description: String,
        tags: Option<String>,
        parser: &dyn crate::service::catalog::CatalogUrlParser,
        ui: &dyn crate::service::catalog::CatalogUi,
    ) -> Result<()> {
        crate::service::catalog::import_url(
            paths,
            kind,
            id,
            url,
            name,
            description,
            tags,
            parser,
            ui,
        )
    }

    fn backups_list(
        &self,
        paths: &ProjectPaths,
        user: bool,
        ui: &dyn crate::service::backups::BackupsUi,
    ) -> Result<()> {
        crate::service::backups::list(paths, user, ui)
    }

    fn backups_open(
        &self,
        paths: &ProjectPaths,
        id: Option<&str>,
        latest: bool,
        user: bool,
        editor: &Option<String>,
        ui: &dyn crate::service::backups::BackupsUi,
    ) -> Result<()> {
        crate::service::backups::open(paths, id, latest, user, editor, ui)
    }

    fn backups_restore(
        &self,
        paths: &ProjectPaths,
        user: bool,
        id: Option<&str>,
        latest: bool,
        dry_run: bool,
        yes: bool,
        ui: &dyn crate::service::backups::BackupsUi,
    ) -> Result<()> {
        crate::service::backups::restore(paths, user, id, latest, dry_run, yes, ui)
    }

    fn logs_select_file(
        &self,
        paths: &ProjectPaths,
        component: &str,
        worktree_filter: Option<&str>,
        task_filter: Option<&str>,
    ) -> Result<std::path::PathBuf> {
        crate::service::logs::select_log_file(paths, component, worktree_filter, task_filter)
    }

    fn logs_print_tail(
        &self,
        path: &Path,
        lines: usize,
        ui: &dyn crate::service::logs::LogsUi,
    ) -> Result<()> {
        crate::service::logs::print_file_tail(path, lines, ui)
    }

    fn logs_tail_follow(&self, path: &Path, lines: usize) -> Result<()> {
        crate::service::logs::tail_file_follow(path, lines)
    }

    fn logs_list_entries(
        &self,
        paths: &ProjectPaths,
    ) -> Result<Vec<crate::service::logs::LogFileEntry>> {
        crate::service::logs::list_log_entries(paths)
    }

    fn logs_read_file(&self, path: &Path) -> Result<String> {
        crate::service::logs::read_log_file(path)
    }

    fn get_logs(
        &self,
        paths: &ProjectPaths,
        component: &str,
        worktree_filter: Option<&str>,
    ) -> Result<String> {
        crate::service::logs::read_log_content(paths, component, worktree_filter, None)
    }

    fn env_var(&self, key: &str) -> Option<String> {
        std::env::var(key).ok()
    }

    fn current_dir(&self) -> std::path::PathBuf {
        std::env::current_dir().unwrap_or_else(|_| ".".into())
    }

    fn path_exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn analyze_last_failure(
        &self,
        paths: &ProjectPaths,
    ) -> Result<Option<crate::service::diagnostic::FailureReport>> {
        crate::service::diagnostic::analyze_last_failure(paths)
    }

    fn coordinator_start_run(
        &self,
        backend: &mut dyn coordinator::engine::ControlPlaneBackend,
        cfg: coordinator::engine::ControlPlaneLoopConfig,
    ) -> Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|e| {
                crate::MaccError::Validation(format!("build runtime for coordinator run: {}", e))
            })?;
        runtime.block_on(coordinator::engine::run_control_plane(backend, cfg))
    }

    fn coordinator_start_action_process(
        &self,
        paths: &ProjectPaths,
        action: &str,
        args: &[String],
        cfg: Option<&crate::config::CoordinatorConfig>,
    ) -> Result<crate::service::coordinator::CoordinatorProcessHandle> {
        crate::service::coordinator::coordinator_start_action_process(paths, action, args, cfg)
    }

    fn coordinator_poll_action_process(
        &self,
        handle: crate::service::coordinator::CoordinatorProcessHandle,
    ) -> Result<crate::service::coordinator::CoordinatorProcessPoll> {
        crate::service::coordinator::coordinator_poll_action_process(handle)
    }

    fn coordinator_stop_action_process(
        &self,
        handle: crate::service::coordinator::CoordinatorProcessHandle,
        graceful: bool,
    ) -> Result<crate::service::coordinator::CoordinatorStopResult> {
        crate::service::coordinator::coordinator_stop_action_process(handle, graceful)
    }

    fn coordinator_start_managed_action_process(
        &self,
        paths: &ProjectPaths,
        action: &str,
        args: &[String],
        cfg: Option<&crate::config::CoordinatorConfig>,
    ) -> Result<()> {
        crate::service::coordinator::coordinator_start_managed_action_process(
            paths, action, args, cfg,
        )
    }

    fn coordinator_poll_managed_action_process(
        &self,
        paths: &ProjectPaths,
    ) -> Result<crate::service::coordinator::CoordinatorManagedPoll> {
        crate::service::coordinator::coordinator_poll_managed_action_process(paths)
    }

    fn coordinator_poll_managed_action_state(
        &self,
        paths: &ProjectPaths,
    ) -> Result<crate::service::coordinator::CoordinatorManagedActionState> {
        crate::service::coordinator::coordinator_poll_managed_action_state(paths)
    }

    fn coordinator_stop_managed_action_process(
        &self,
        paths: &ProjectPaths,
        graceful: bool,
    ) -> Result<crate::service::coordinator::CoordinatorStopResult> {
        crate::service::coordinator::coordinator_stop_managed_action_process(paths, graceful)
    }

    fn coordinator_run_workflow(
        &self,
        paths: &ProjectPaths,
        cfg: Option<&crate::config::CoordinatorConfig>,
        options: &crate::service::coordinator_workflow::CoordinatorRunOptions,
    ) -> Result<()> {
        crate::service::coordinator_workflow::coordinator_run(paths, cfg, options)
    }

    fn get_coordinator_status(
        &self,
        paths: &ProjectPaths,
    ) -> Result<crate::service::coordinator_workflow::CoordinatorStatus> {
        crate::service::coordinator_workflow::get_coordinator_status(paths)
    }

    fn coordinator_run_cycle_workflow(
        &self,
        paths: &ProjectPaths,
        canonical: &crate::config::CanonicalConfig,
        coordinator_cfg: Option<&crate::config::CoordinatorConfig>,
        env_cfg: &crate::coordinator::types::CoordinatorEnvConfig,
        logger: Option<&dyn crate::coordinator::control_plane::CoordinatorLog>,
    ) -> Result<()> {
        crate::service::coordinator_workflow::coordinator_run_cycle(
            self,
            paths,
            canonical,
            coordinator_cfg,
            env_cfg,
            logger,
        )
    }

    fn coordinator_perform_action_workflow(
        &self,
        paths: &ProjectPaths,
        action: crate::service::coordinator_workflow::CoordinatorAction,
        request: crate::service::coordinator_workflow::CoordinatorActionRequest<'_>,
    ) -> Result<crate::service::coordinator_workflow::CoordinatorActionResult> {
        crate::service::coordinator_workflow::coordinator_perform_action(
            self, paths, action, request,
        )
    }

    fn coordinator_run(
        &self,
        paths: &ProjectPaths,
        cfg: Option<&crate::config::CoordinatorConfig>,
        options: &crate::service::coordinator_workflow::CoordinatorRunOptions,
    ) -> Result<()> {
        self.coordinator_run_workflow(paths, cfg, options)
    }

    fn coordinator_stop_workflow(&self, paths: &ProjectPaths, reason: &str) -> Result<()> {
        crate::service::coordinator_workflow::coordinator_stop(paths, reason)
    }

    fn coordinator_dispatch_workflow(
        &self,
        paths: &ProjectPaths,
        canonical: &crate::config::CanonicalConfig,
        coordinator_cfg: Option<&crate::config::CoordinatorConfig>,
        env_cfg: &crate::coordinator::types::CoordinatorEnvConfig,
        logger: Option<&dyn crate::coordinator::control_plane::CoordinatorLog>,
    ) -> Result<()> {
        crate::service::coordinator_workflow::coordinator_dispatch(
            self,
            paths,
            canonical,
            coordinator_cfg,
            env_cfg,
            logger,
        )
    }

    fn coordinator_advance_workflow(
        &self,
        paths: &ProjectPaths,
        coordinator_cfg: Option<&crate::config::CoordinatorConfig>,
        env_cfg: &crate::coordinator::types::CoordinatorEnvConfig,
        logger: Option<&dyn crate::coordinator::control_plane::CoordinatorLog>,
    ) -> Result<()> {
        crate::service::coordinator_workflow::coordinator_advance(
            self,
            paths,
            coordinator_cfg,
            env_cfg,
            logger,
        )
    }

    fn coordinator_reconcile_workflow(
        &self,
        paths: &ProjectPaths,
        logger: Option<&dyn crate::coordinator::control_plane::CoordinatorLog>,
    ) -> Result<()> {
        crate::service::coordinator_workflow::coordinator_reconcile(paths, logger)
    }

    fn coordinator_cleanup_workflow(
        &self,
        paths: &ProjectPaths,
        logger: Option<&dyn crate::coordinator::control_plane::CoordinatorLog>,
    ) -> Result<()> {
        crate::service::coordinator_workflow::coordinator_cleanup(paths, logger)
    }

    fn coordinator_sync_workflow(
        &self,
        paths: &ProjectPaths,
        coordinator_cfg: Option<&crate::config::CoordinatorConfig>,
        env_cfg: &crate::coordinator::types::CoordinatorEnvConfig,
        logger: Option<&dyn crate::coordinator::control_plane::CoordinatorLog>,
    ) -> Result<()> {
        crate::service::coordinator_workflow::coordinator_sync(
            self,
            paths,
            coordinator_cfg,
            env_cfg,
            logger,
        )
    }

    fn coordinator_unlock_workflow(
        &self,
        paths: &ProjectPaths,
        coordinator_cfg: Option<&crate::config::CoordinatorConfig>,
        env_cfg: &crate::coordinator::types::CoordinatorEnvConfig,
        args: &[String],
    ) -> Result<()> {
        crate::service::coordinator_workflow::coordinator_unlock(
            self,
            paths,
            coordinator_cfg,
            env_cfg,
            args,
        )
    }

    fn coordinator_cutover_gate_workflow(&self, paths: &ProjectPaths) -> Result<()> {
        crate::service::coordinator_workflow::coordinator_cutover_gate(paths)
    }

    #[allow(clippy::too_many_arguments)]
    fn coordinator_retry_phase_workflow(
        &self,
        paths: &ProjectPaths,
        canonical: &crate::config::CanonicalConfig,
        coordinator_cfg: Option<&crate::config::CoordinatorConfig>,
        env_cfg: &crate::coordinator::types::CoordinatorEnvConfig,
        args: &[String],
        logger: Option<&dyn crate::coordinator::control_plane::CoordinatorLog>,
    ) -> Result<()> {
        crate::service::coordinator_workflow::coordinator_retry_phase(
            self,
            paths,
            canonical,
            coordinator_cfg,
            env_cfg,
            args,
            logger,
        )
    }

    fn coordinator_stop(&self, repo_root: &Path, reason: &str) -> Result<()> {
        coordinator::state_runtime::write_coordinator_pause_file(
            repo_root,
            "global",
            "dev",
            &format!("stopped: {}", reason),
        )
    }

    fn coordinator_resume(&self, repo_root: &Path) -> Result<()> {
        if let Some(pause) = coordinator::state_runtime::read_coordinator_pause_file(repo_root)? {
            let task_id = pause
                .get("task_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let phase = pause
                .get("phase")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if !task_id.is_empty() && phase == "integrate" {
                coordinator::state_runtime::resume_paused_task_integrate(repo_root, task_id)?;
            }
        }
        let _ = coordinator::state_runtime::clear_coordinator_pause_file(repo_root)?;
        Ok(())
    }

    fn coordinator_status_snapshot(
        &self,
        project_paths: &ProjectPaths,
    ) -> Result<CoordinatorStatusSnapshot> {
        let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
        let sqlite = SqliteStorage::new(paths.clone());
        let snapshot: CoordinatorSnapshot = if sqlite.has_snapshot_data()? {
            sqlite.load_snapshot()?
        } else {
            JsonStorage::new(paths).load_snapshot()?
        };

        let tasks = snapshot
            .registry
            .get("tasks")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut counts = CoordinatorStatusSnapshot::default();
        counts.total = tasks.len();
        for task in tasks {
            match task
                .get("state")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("todo")
            {
                "todo" => counts.todo += 1,
                "blocked" => counts.blocked += 1,
                "merged" => counts.merged += 1,
                _ => counts.active += 1,
            }
        }

        if let Some(pause) =
            coordinator::state_runtime::read_coordinator_pause_file(&project_paths.root)?
        {
            counts.paused = true;
            counts.pause_reason = pause
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .map(|s| s.to_string());
            counts.pause_task_id = pause
                .get("task_id")
                .and_then(serde_json::Value::as_str)
                .map(|s| s.to_string());
            counts.pause_phase = pause
                .get("phase")
                .and_then(serde_json::Value::as_str)
                .map(|s| s.to_string());
        }

        Ok(counts)
    }

    fn get_coordinator_events(
        &self,
        project_paths: &ProjectPaths,
    ) -> Result<Vec<CoordinatorEvent>> {
        let paths = CoordinatorStoragePaths::from_project_paths(project_paths);
        let sqlite = SqliteStorage::new(paths.clone());
        let snapshot: CoordinatorSnapshot = if sqlite.has_snapshot_data()? {
            sqlite.load_snapshot()?
        } else {
            JsonStorage::new(paths).load_snapshot()?
        };

        let mut out = Vec::with_capacity(snapshot.events.len());
        for raw in snapshot.events {
            out.push(CoordinatorEvent::from_raw(raw));
        }
        Ok(out)
    }

    fn coordinator_storage_import_json_to_sqlite(&self, paths: &ProjectPaths) -> Result<()> {
        crate::coordinator_storage::coordinator_storage_import_json_to_sqlite(paths)
    }

    fn coordinator_storage_export_sqlite_to_json(&self, paths: &ProjectPaths) -> Result<()> {
        crate::coordinator_storage::coordinator_storage_export_sqlite_to_json(paths)
    }

    fn coordinator_storage_verify_parity(&self, paths: &ProjectPaths) -> Result<()> {
        crate::coordinator_storage::coordinator_storage_verify_parity(paths)
    }

    fn coordinator_aggregate_performer_logs(&self, repo_root: &Path) -> Result<usize> {
        crate::coordinator::logs::aggregate_performer_logs(repo_root)
    }

    fn coordinator_sync_registry_from_prd(&self, repo_root: &Path, prd_file: &Path) -> Result<()> {
        crate::coordinator::control_plane::sync_registry_from_prd_native(repo_root, prd_file, None)
    }

    fn coordinator_sync_registry_from_prd_with_logger(
        &self,
        repo_root: &Path,
        prd_file: &Path,
        logger: Option<&dyn crate::coordinator::control_plane::CoordinatorLog>,
    ) -> Result<()> {
        crate::coordinator::control_plane::sync_registry_from_prd_native(
            repo_root, prd_file, logger,
        )
    }

    fn coordinator_dispatch_ready_tasks_native<'a>(
        &'a self,
        repo_root: &'a Path,
        canonical: &'a crate::config::CanonicalConfig,
        coordinator: Option<&'a crate::config::CoordinatorConfig>,
        env_cfg: &'a crate::coordinator::types::CoordinatorEnvConfig,
        prd_file: &'a Path,
        state: &'a mut crate::coordinator::runtime::CoordinatorRunState,
        logger: Option<&'a dyn crate::coordinator::control_plane::CoordinatorLog>,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + 'a>> {
        Box::pin(
            crate::coordinator::control_plane::dispatch_ready_tasks_native(
                repo_root,
                canonical,
                coordinator,
                env_cfg,
                prd_file,
                state,
                logger,
            ),
        )
    }

    fn coordinator_monitor_active_jobs_native<'a>(
        &'a self,
        repo_root: &'a Path,
        env_cfg: &'a crate::coordinator::types::CoordinatorEnvConfig,
        state: &'a mut crate::coordinator::runtime::CoordinatorRunState,
        max_attempts: usize,
        phase_timeout_seconds: usize,
        logger: Option<&'a dyn crate::coordinator::control_plane::CoordinatorLog>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
        Box::pin(
            crate::coordinator::control_plane::monitor_active_jobs_native(
                repo_root,
                env_cfg,
                state,
                max_attempts,
                phase_timeout_seconds,
                logger,
            ),
        )
    }

    fn coordinator_advance_tasks_native<'a>(
        &'a self,
        repo_root: &'a Path,
        coordinator_tool_override: Option<&'a str>,
        phase_runner_max_attempts: usize,
        state: &'a mut crate::coordinator::runtime::CoordinatorRunState,
        logger: Option<&'a dyn crate::coordinator::control_plane::CoordinatorLog>,
    ) -> Pin<Box<dyn Future<Output = Result<crate::coordinator::engine::AdvanceResult>> + 'a>> {
        Box::pin(crate::coordinator::control_plane::advance_tasks_native(
            repo_root,
            coordinator_tool_override,
            phase_runner_max_attempts,
            state,
            logger,
        ))
    }

    fn coordinator_run_phase_for_task_native(
        &self,
        repo_root: &Path,
        task: &serde_json::Value,
        phase: &str,
        coordinator_tool_override: Option<&str>,
        max_attempts: usize,
        logger: Option<&dyn crate::coordinator::control_plane::CoordinatorLog>,
    ) -> Result<std::result::Result<String, String>> {
        crate::coordinator::control_plane::run_phase_for_task_native(
            repo_root,
            task,
            phase,
            coordinator_tool_override,
            max_attempts,
            logger,
        )
    }

    fn coordinator_run_review_phase_for_task_native(
        &self,
        repo_root: &Path,
        task: &serde_json::Value,
        coordinator_tool_override: Option<&str>,
        max_attempts: usize,
        logger: Option<&dyn crate::coordinator::control_plane::CoordinatorLog>,
    ) -> Result<std::result::Result<crate::coordinator::engine::ReviewVerdict, String>> {
        crate::coordinator::control_plane::run_review_phase_for_task_native(
            repo_root,
            task,
            coordinator_tool_override,
            max_attempts,
            logger,
        )
    }

    fn coordinator_state_apply_transition(
        &self,
        repo_root: &Path,
        args: &BTreeMap<String, String>,
    ) -> Result<()> {
        crate::coordinator::state::coordinator_state_apply_transition(repo_root, args)
    }

    fn coordinator_state_set_runtime(
        &self,
        repo_root: &Path,
        args: &BTreeMap<String, String>,
    ) -> Result<()> {
        crate::coordinator::state::coordinator_state_set_runtime(repo_root, args)
    }

    fn coordinator_state_task_field(
        &self,
        repo_root: &Path,
        args: &BTreeMap<String, String>,
    ) -> Result<()> {
        crate::coordinator::state::coordinator_state_task_field(repo_root, args)
    }

    fn coordinator_state_task_exists(
        &self,
        repo_root: &Path,
        args: &BTreeMap<String, String>,
    ) -> Result<()> {
        crate::coordinator::state::coordinator_state_task_exists(repo_root, args)
    }

    fn coordinator_state_counts(
        &self,
        repo_root: &Path,
        args: &BTreeMap<String, String>,
    ) -> Result<()> {
        crate::coordinator::state::coordinator_state_counts(repo_root, args)
    }

    fn coordinator_state_locks(
        &self,
        repo_root: &Path,
        args: &BTreeMap<String, String>,
    ) -> Result<()> {
        crate::coordinator::state::coordinator_state_locks(repo_root, args)
    }

    fn coordinator_state_set_merge_pending(
        &self,
        repo_root: &Path,
        args: &BTreeMap<String, String>,
    ) -> Result<()> {
        crate::coordinator::state::coordinator_state_set_merge_pending(repo_root, args)
    }

    fn coordinator_state_set_merge_processed(
        &self,
        repo_root: &Path,
        args: &BTreeMap<String, String>,
    ) -> Result<()> {
        crate::coordinator::state::coordinator_state_set_merge_processed(repo_root, args)
    }

    fn coordinator_state_increment_retries(
        &self,
        repo_root: &Path,
        args: &BTreeMap<String, String>,
    ) -> Result<()> {
        crate::coordinator::state::coordinator_state_increment_retries(repo_root, args)
    }

    fn coordinator_state_upsert_slo_warning(
        &self,
        repo_root: &Path,
        args: &BTreeMap<String, String>,
    ) -> Result<()> {
        crate::coordinator::state::coordinator_state_upsert_slo_warning(repo_root, args)
    }

    fn coordinator_state_slo_metric(
        &self,
        repo_root: &Path,
        args: &BTreeMap<String, String>,
    ) -> Result<()> {
        crate::coordinator::state::coordinator_state_slo_metric(repo_root, args)
    }

    fn coordinator_state_snapshot(
        &self,
        repo_root: &Path,
        args: &BTreeMap<String, String>,
    ) -> Result<crate::coordinator_storage::CoordinatorSnapshot> {
        crate::coordinator::state::coordinator_state_snapshot(repo_root, args)
    }

    fn coordinator_state_save_snapshot(
        &self,
        repo_root: &Path,
        args: &BTreeMap<String, String>,
        snapshot: &crate::coordinator_storage::CoordinatorSnapshot,
    ) -> Result<()> {
        crate::coordinator::state::coordinator_state_save_snapshot(repo_root, args, snapshot)
    }

    fn coordinator_state_unlock_resource(
        &self,
        repo_root: &Path,
        args: &BTreeMap<String, String>,
        resource: Option<&str>,
        clear_all: bool,
    ) -> Result<usize> {
        crate::coordinator::state::coordinator_state_unlock_resource(
            repo_root, args, resource, clear_all,
        )
    }

    fn coordinator_state_reset_runtime_to_idle(&self, task: &mut serde_json::Value) {
        crate::coordinator::state::reset_runtime_to_idle(task)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CoordinatorStatusSnapshot {
    pub total: usize,
    pub todo: usize,
    pub active: usize,
    pub blocked: usize,
    pub merged: usize,
    pub paused: bool,
    pub pause_reason: Option<String>,
    pub pause_task_id: Option<String>,
    pub pause_phase: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CoordinatorEvent {
    pub event_id: Option<String>,
    pub run_id: Option<String>,
    pub event_type: String,
    pub task_id: Option<String>,
    pub phase: Option<String>,
    pub status: Option<String>,
    pub ts: Option<String>,
    pub message: Option<String>,
    pub raw: serde_json::Value,
}

impl CoordinatorEvent {
    fn from_raw(raw: serde_json::Value) -> Self {
        Self {
            event_id: raw
                .get("event_id")
                .and_then(serde_json::Value::as_str)
                .map(|s| s.to_string()),
            run_id: raw
                .get("run_id")
                .and_then(serde_json::Value::as_str)
                .map(|s| s.to_string()),
            event_type: raw
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_string(),
            task_id: raw
                .get("task_id")
                .and_then(serde_json::Value::as_str)
                .map(|s| s.to_string()),
            phase: raw
                .get("phase")
                .and_then(serde_json::Value::as_str)
                .map(|s| s.to_string()),
            status: raw
                .get("status")
                .and_then(serde_json::Value::as_str)
                .map(|s| s.to_string()),
            ts: raw
                .get("ts")
                .and_then(serde_json::Value::as_str)
                .map(|s| s.to_string()),
            message: raw
                .get("message")
                .and_then(serde_json::Value::as_str)
                .map(|s| s.to_string()),
            raw,
        }
    }
}

/// The standard production engine.
pub struct MaccEngine {
    registry: ToolRegistry,
}

impl MaccEngine {
    /// Creates a new engine with the provided tool registry.
    pub fn new(registry: ToolRegistry) -> Self {
        Self { registry }
    }

    /// Provides access to the underlying tool registry.
    pub fn registry(&self) -> &ToolRegistry {
        &self.registry
    }
}

impl Engine for MaccEngine {
    /// Lists all available tools and their metadata, including any loading diagnostics.
    fn list_tools(&self, paths: &ProjectPaths) -> (Vec<ToolDescriptor>, Vec<ToolDiagnostic>) {
        let search_paths = ToolSpecLoader::default_search_paths(&paths.root);
        let loader = ToolSpecLoader::new(search_paths);
        let (specs, mut diagnostics) = loader.load_all_with_embedded();
        let mut descriptors: Vec<_> = specs.into_iter().map(|s| s.to_descriptor()).collect();

        // Ensure deterministic ordering by ID for UI consistency
        descriptors.sort_by(|a, b| a.id.cmp(&b.id));

        if descriptors.is_empty() {
            diagnostics.push(ToolDiagnostic {
                path: std::path::PathBuf::from("<toolspec-resolution>"),
                error: "No ToolSpecs resolved (embedded + user + project overrides).".to_string(),
                line: None,
                column: None,
            });
        }

        (descriptors, diagnostics)
    }

    /// Runs diagnostic checks for the environment and supported tools.
    fn doctor(&self, paths: &ProjectPaths) -> Vec<ToolCheck> {
        // Load specs to determine checks
        let search_paths = ToolSpecLoader::default_search_paths(&paths.root);
        let loader = ToolSpecLoader::new(search_paths);
        let (specs, _) = loader.load_all_with_embedded();

        let mut checks = doctor::checks_for_enabled_tools(&specs);
        doctor::run_checks(&mut checks);
        checks
    }

    /// Builds an effective ActionPlan based on canonical configuration and optional CLI overrides.
    fn plan(
        &self,
        paths: &ProjectPaths,
        config: &CanonicalConfig,
        materialized_units: &[MaterializedFetchUnit],
        overrides: &CliOverrides,
    ) -> Result<ActionPlan> {
        let resolved = resolve::resolve(config, overrides);
        crate::build_plan(paths, &resolved, materialized_units, &self.registry)
    }

    /// Produces a list of deterministic operations from a plan, suitable for UI preview or diff view.
    fn plan_operations(&self, paths: &ProjectPaths, plan: &ActionPlan) -> Vec<PlannedOp> {
        plan::collect_plan_operations(paths, plan)
    }

    fn apply(
        &self,
        paths: &ProjectPaths,
        plan: &mut ActionPlan,
        allow_user_scope: bool,
    ) -> Result<ApplyReport> {
        crate::apply_plan(paths, plan, allow_user_scope)
    }

    fn builtin_skills(&self) -> Vec<Skill> {
        catalog::builtin_skills()
    }

    fn builtin_agents(&self) -> Vec<Agent> {
        catalog::builtin_agents()
    }
}

/// A test-only engine that uses in-memory fixtures instead of the filesystem.
///
/// This ensures UI tests (TUI/CLI) are stable, fast, and tool-agnostic.
pub struct TestEngine {
    registry: ToolRegistry,
    specs: Vec<crate::tool::ToolSpec>,
    fixture_ids: Vec<String>,
}

impl TestEngine {
    /// Creates a new test engine with the provided registry and specs.
    pub fn new(registry: ToolRegistry, specs: Vec<crate::tool::ToolSpec>) -> Self {
        Self {
            registry,
            specs,
            fixture_ids: Vec::new(),
        }
    }

    /// Creates a default test engine with fixture tools.
    pub fn with_fixtures() -> Self {
        let fixture_ids = Self::generate_fixture_ids(2);
        Self::with_fixtures_for_ids(&fixture_ids)
    }

    /// Creates a test engine with fixture tools using the provided IDs.
    pub fn with_fixtures_for_ids(ids: &[String]) -> Self {
        use crate::tool::{
            CheckSeverity, DoctorCheckKind, DoctorCheckSpec, FieldKindSpec, FieldSpec, MockAdapter,
            ToolSpec,
        };
        use std::sync::Arc;

        assert!(
            ids.len() >= 2,
            "with_fixtures_for_ids expects at least two tool IDs"
        );

        let id_one = ids[0].clone();
        let id_two = ids[1].clone();

        let spec_one = ToolSpec {
            api_version: "v1".to_string(),
            id: id_one.clone(),
            display_name: "Fixture Tool One".to_string(),
            description: Some("First fixture tool for UI testing.".to_string()),
            capabilities: vec!["chat".to_string()],
            fields: vec![
                FieldSpec {
                    id: "enabled".to_string(),
                    label: "Enabled".to_string(),
                    kind: FieldKindSpec::Bool,
                    help: Some("Whether the tool is enabled.".to_string()),
                    pointer: Some(format!("/tools/config/{}/enabled", id_one)),
                    default: None,
                },
                FieldSpec {
                    id: "mode".to_string(),
                    label: "Mode".to_string(),
                    kind: FieldKindSpec::Enum {
                        options: vec![
                            "fast".to_string(),
                            "balanced".to_string(),
                            "precise".to_string(),
                        ],
                    },
                    help: Some("Select the operation mode.".to_string()),
                    pointer: Some(format!("/tools/config/{}/mode", id_one)),
                    default: None,
                },
                FieldSpec {
                    id: "username".to_string(),
                    label: "Username".to_string(),
                    kind: FieldKindSpec::Text,
                    help: Some("Your username for this tool.".to_string()),
                    pointer: Some(format!("/tools/config/{}/username", id_one)),
                    default: None,
                },
                FieldSpec {
                    id: "setup_mcp".to_string(),
                    label: "Setup MCP".to_string(),
                    kind: FieldKindSpec::Action(crate::tool::ActionSpec::OpenMcp {
                        target_pointer: "/selections/mcp".to_string(),
                    }),
                    help: Some("Open MCP selector.".to_string()),
                    pointer: None,
                    default: None,
                },
            ],
            doctor: Some(vec![DoctorCheckSpec {
                kind: DoctorCheckKind::Which,
                value: format!("{}-cli", id_one),
                severity: CheckSeverity::Error,
            }]),
            gitignore: Vec::new(),
            performer: None,
            install: None,
            update: None,
            version_check: None,
            defaults: None,
        };

        let spec_two = ToolSpec {
            api_version: "v1".to_string(),
            id: id_two.clone(),
            display_name: "Fixture Tool Two".to_string(),
            description: Some("Second fixture tool for UI testing.".to_string()),
            capabilities: vec!["edit".to_string()],
            fields: vec![
                FieldSpec {
                    id: "api_key".to_string(),
                    label: "API Key".to_string(),
                    kind: FieldKindSpec::Text,
                    help: Some("Sensitive API key.".to_string()),
                    pointer: Some(format!("/tools/config/{}/auth/key", id_two)),
                    default: None,
                },
                FieldSpec {
                    id: "model".to_string(),
                    label: "Model".to_string(),
                    kind: FieldKindSpec::Enum {
                        options: vec!["smart".to_string(), "small".to_string()],
                    },
                    help: None,
                    pointer: Some(format!("/tools/config/{}/settings/model_name", id_two)),
                    default: None,
                },
                FieldSpec {
                    id: "auto_apply".to_string(),
                    label: "Auto Apply".to_string(),
                    kind: FieldKindSpec::Bool,
                    help: None,
                    pointer: Some(format!("/tools/config/{}/settings/auto_apply", id_two)),
                    default: None,
                },
            ],
            doctor: Some(vec![DoctorCheckSpec {
                kind: DoctorCheckKind::PathExists,
                value: format!("~/.{}/config.json", id_two),
                severity: CheckSeverity::Warning,
            }]),
            gitignore: Vec::new(),
            performer: None,
            install: None,
            update: None,
            version_check: None,
            defaults: None,
        };

        let mut registry = ToolRegistry::new();

        let mut plan_one = ActionPlan::new();
        let output_one = format!("{}-output.txt", id_one);
        plan_one.add_action(plan::Action::WriteFile {
            path: output_one,
            content: format!("fixture content for {}\n", id_one).into_bytes(),
            scope: plan::Scope::Project,
        });

        let mut plan_two = ActionPlan::new();
        let output_two = format!("{}-output.txt", id_two);
        plan_two.add_action(plan::Action::WriteFile {
            path: output_two,
            content: format!("fixture content for {}\n", id_two).into_bytes(),
            scope: plan::Scope::Project,
        });

        registry.register(Arc::new(MockAdapter {
            id: id_one.clone(),
            plan: plan_one,
        }));
        registry.register(Arc::new(MockAdapter {
            id: id_two.clone(),
            plan: plan_two,
        }));

        Self {
            registry,
            specs: vec![spec_one, spec_two],
            fixture_ids: vec![id_one, id_two],
        }
    }

    pub fn generate_fixture_ids(count: usize) -> Vec<String> {
        let suffix = fixture_suffix();
        (0..count)
            .map(|idx| {
                let letter = (b'a' + (idx as u8)) as char;
                format!("fixture-{}-{}", letter, suffix)
            })
            .collect()
    }

    pub fn fixture_ids(&self) -> &[String] {
        &self.fixture_ids
    }
}

fn fixture_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", nanos)
}

impl Engine for TestEngine {
    /// Lists tools from the in-memory fixtures.
    fn list_tools(&self, _paths: &ProjectPaths) -> (Vec<ToolDescriptor>, Vec<ToolDiagnostic>) {
        let descriptors = self.specs.iter().map(|s| s.to_descriptor()).collect();
        (descriptors, Vec::new())
    }

    /// Runs stubbed diagnostic checks.
    fn doctor(&self, _paths: &ProjectPaths) -> Vec<ToolCheck> {
        // Since we are testing, we can simulate checks based on fixtures
        let mut checks = doctor::checks_for_enabled_tools(&self.specs);
        // Force them to be installed for tests
        for check in &mut checks {
            check.status = crate::doctor::ToolStatus::Installed;
        }
        checks
    }

    /// Produces a deterministic ActionPlan.
    fn plan(
        &self,
        paths: &ProjectPaths,
        config: &CanonicalConfig,
        materialized_units: &[MaterializedFetchUnit],
        overrides: &CliOverrides,
    ) -> Result<ActionPlan> {
        let resolved = resolve::resolve(config, overrides);
        crate::build_plan(paths, &resolved, materialized_units, &self.registry)
    }

    /// Produces a list of deterministic operations.
    fn plan_operations(&self, paths: &ProjectPaths, plan: &ActionPlan) -> Vec<PlannedOp> {
        plan::collect_plan_operations(paths, plan)
    }

    /// Applies the planned actions (using real apply, but usually with mock paths).
    fn apply(
        &self,
        paths: &ProjectPaths,
        plan: &mut ActionPlan,
        allow_user_scope: bool,
    ) -> Result<ApplyReport> {
        crate::apply_plan(paths, plan, allow_user_scope)
    }

    fn builtin_skills(&self) -> Vec<Skill> {
        vec![
            Skill {
                id: "mock-skill-one".into(),
                name: "Mock Skill One".into(),
                description: "First mock skill for testing.".into(),
            },
            Skill {
                id: "mock-skill-two".into(),
                name: "Mock Skill Two".into(),
                description: "Second mock skill for testing.".into(),
            },
        ]
    }

    fn builtin_agents(&self) -> Vec<Agent> {
        vec![
            Agent {
                id: "mock-agent-one".into(),
                name: "Mock Agent One".into(),
                description: "First mock agent for testing.".into(),
            },
            Agent {
                id: "mock-agent-two".into(),
                name: "Mock Agent Two".into(),
                description: "Second mock agent for testing.".into(),
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ToolsConfig;
    use std::fs;

    fn create_test_paths() -> (ProjectPaths, PathBuf) {
        let temp_dir = std::env::temp_dir().join(format!("macc_engine_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        (ProjectPaths::from_root(&temp_dir), temp_dir)
    }

    use std::path::PathBuf;
    fn uuid_v4_like() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{:x}", nanos)
    }

    #[test]
    fn test_engine_plan_and_apply() -> Result<()> {
        let (paths, temp_dir) = create_test_paths();
        crate::init(&paths, false)?;

        let engine = MaccEngine::new(ToolRegistry::default_registry());

        let config = CanonicalConfig {
            version: Some("v1".to_string()),
            tools: ToolsConfig {
                enabled: vec!["test".to_string()],
                ..Default::default()
            },
            ..Default::default()
        };

        // 1. Plan
        let mut plan = engine.plan(&paths, &config, &[], &CliOverrides::default())?;
        assert!(plan.actions.len() > 0);

        // 2. Plan Operations (for UI)
        let ops = engine.plan_operations(&paths, &plan);
        assert!(ops.len() > 0);
        assert!(ops.iter().any(|op| op.path == "MACC_GENERATED.txt"));

        // 3. Apply
        let report = engine.apply(&paths, &mut plan, false)?;
        assert!(temp_dir.join("MACC_GENERATED.txt").exists());
        assert_eq!(
            report.outcomes.get("MACC_GENERATED.txt").unwrap(),
            &plan::ActionStatus::Created
        );

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_engine_doctor() {
        let (paths, temp_dir) = create_test_paths();
        // Create a dummy tool spec file
        let tools_d = paths.root.join(".macc/tools.d");
        fs::create_dir_all(&tools_d).unwrap();
        let spec = r#"
api_version: v1
id: my-tool
display_name: My Tool
fields: []
"#;
        fs::write(tools_d.join("my.tool.yaml"), spec).unwrap();

        let engine = MaccEngine::new(ToolRegistry::new());
        let checks = engine.doctor(&paths);

        // Should have at least "Git" and "My Tool" (via heuristic)
        assert!(checks.iter().any(|c| c.name == "Git"));
        assert!(checks.iter().any(|c| c.name == "My Tool"));

        fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_engine_list_tools() {
        let (paths, temp_dir) = create_test_paths();
        // Create a dummy tool spec file
        let tools_d = paths.root.join(".macc/tools.d");
        fs::create_dir_all(&tools_d).unwrap();
        let spec = r#"
api_version: v1
id: my-tool
display_name: My Tool
fields: []
"#;
        fs::write(tools_d.join("my.tool.yaml"), spec).unwrap();

        let engine = MaccEngine::new(ToolRegistry::new());
        let (descriptors, diags) = engine.list_tools(&paths);

        assert!(diags.is_empty(), "Diagnostics: {:?}", diags);
        assert!(descriptors.iter().any(|d| d.id == "my-tool"));

        fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_test_engine_fixtures() -> Result<()> {
        let (paths, temp_dir) = create_test_paths();
        let fixture_ids = TestEngine::generate_fixture_ids(2);
        let tool_one = fixture_ids[0].clone();
        let tool_two = fixture_ids[1].clone();
        let engine = TestEngine::with_fixtures_for_ids(&fixture_ids);

        // 1. List tools (should use in-memory specs)
        let (descriptors, diags) = engine.list_tools(&paths);
        assert_eq!(descriptors.len(), 2);
        assert_eq!(descriptors[0].id, tool_one);
        assert_eq!(descriptors[1].id, tool_two);
        assert!(diags.is_empty());

        // 2. Doctor (should use in-memory specs)
        let checks = engine.doctor(&paths);
        // Git + Mock One + Mock Two = 3
        // Actually checks_for_enabled_tools adds Git baseline.
        // TestEngine::doctor calls generic logic.
        assert!(checks.len() >= 3);
        assert!(checks.iter().any(|c| c.tool_id == Some(tool_one.clone())));

        // 3. Plan
        let config = CanonicalConfig {
            tools: crate::config::ToolsConfig {
                enabled: vec![tool_one.clone()],
                ..Default::default()
            },
            ..Default::default()
        };
        let mut plan = engine.plan(&paths, &config, &[], &CliOverrides::default())?;
        let output_path = format!("{}-output.txt", tool_one);
        assert!(plan.actions.iter().any(|a| a.path() == output_path));

        // 4. Apply
        let report = engine.apply(&paths, &mut plan, false)?;
        assert!(temp_dir.join(format!("{}-output.txt", tool_one)).exists());
        assert_eq!(
            report
                .outcomes
                .get(&format!("{}-output.txt", tool_one))
                .unwrap(),
            &plan::ActionStatus::Created
        );

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }
}
