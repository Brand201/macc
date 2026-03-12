use crate::coordinator::legacy_helpers::{
    coordinator_select_ready_task_action, stop_coordinator_process_groups, NativeCoordinatorLogger,
};
use crate::coordinator::render::print_status_summary;
use macc_core::coordinator::engine as coordinator_engine;
use macc_core::coordinator::types::CoordinatorEnvConfig;
use macc_core::coordinator_storage::CoordinatorStorageMode;
use macc_core::service::coordinator_workflow::{CoordinatorAction, CoordinatorActionRequest};
use macc_core::{load_canonical_config, MaccError, Result};
use std::path::Path;

fn build_native_logger(
    repo_root: &Path,
    action: &str,
    env_cfg: &CoordinatorEnvConfig,
    coordinator_cfg: Option<&macc_core::config::CoordinatorConfig>,
) -> Result<NativeCoordinatorLogger> {
    NativeCoordinatorLogger::new_with_flush(
        repo_root,
        action,
        env_cfg
            .log_flush_lines
            .or_else(|| coordinator_cfg.and_then(|c| c.log_flush_lines)),
        env_cfg
            .log_flush_ms
            .or_else(|| coordinator_cfg.and_then(|c| c.log_flush_ms)),
    )
}

struct LoggerAdapter<'a>(&'a NativeCoordinatorLogger);

impl macc_core::coordinator::control_plane::CoordinatorLog for LoggerAdapter<'_> {
    fn note(&self, line: String) -> Result<()> {
        self.0.note(line)
    }
}

#[derive(Debug, Clone)]
pub struct CoordinatorCommandInput {
    pub action: String,
    pub no_tui: bool,
    pub graceful: bool,
    pub remove_worktrees: bool,
    pub remove_branches: bool,
    pub env_cfg: CoordinatorEnvConfig,
    pub extra_args: Vec<String>,
}

struct ProjectContext {
    paths: macc_core::ProjectPaths,
    canonical: macc_core::config::CanonicalConfig,
    coordinator_cfg: Option<macc_core::config::CoordinatorConfig>,
}

impl ProjectContext {
    fn load(
        absolute_cwd: &Path,
        engine: &crate::services::engine_provider::SharedEngine,
    ) -> Result<Self> {
        let paths = engine.project_ensure_initialized_paths(absolute_cwd)?;
        let canonical = load_canonical_config(&paths.config_path)?;
        let coordinator_cfg = canonical.automation.coordinator.clone();
        Ok(Self {
            paths,
            canonical,
            coordinator_cfg,
        })
    }
}

pub fn handle(
    absolute_cwd: &Path,
    engine: &crate::services::engine_provider::SharedEngine,
    input: CoordinatorCommandInput,
) -> Result<()> {
    let action: CoordinatorAction = input.action.parse()?;
    if action == CoordinatorAction::SelectReadyTask {
        coordinator_select_ready_task_action(absolute_cwd, &input.extra_args)?;
        return Ok(());
    }

    let context = ProjectContext::load(absolute_cwd, engine)?;
    let paths = &context.paths;
    let canonical = &context.canonical;
    let coordinator_cfg = context.coordinator_cfg.as_ref();

    if action == CoordinatorAction::Run && !input.no_tui {
        return macc_tui::run_tui_with_launch(macc_tui::LaunchMode::CoordinatorRun).map_err(|e| {
            MaccError::Io {
                path: "tui".into(),
                action: "run_tui coordinator live".into(),
                source: std::io::Error::other(e.to_string()),
            }
        });
    }

    let _ = macc_core::ensure_embedded_automation_scripts(paths)?;

    if let Ok(effective_storage_mode) =
        coordinator_engine::resolve_storage_mode(&input.env_cfg, coordinator_cfg)
    {
        let mode_raw = match effective_storage_mode {
            CoordinatorStorageMode::Json => "json",
            CoordinatorStorageMode::DualWrite => "dual-write",
            CoordinatorStorageMode::Sqlite => "sqlite",
        };
        std::env::set_var("COORDINATOR_STORAGE_MODE", mode_raw);
    }
    if let Some(debounce_ms) = input
        .env_cfg
        .mirror_json_debounce_ms
        .or_else(|| coordinator_cfg.and_then(|c| c.mirror_json_debounce_ms))
    {
        std::env::set_var(
            "COORDINATOR_JSON_EXPORT_DEBOUNCE_MS",
            debounce_ms.to_string(),
        );
    }
    if action.emits_runtime_events() {
        let _ = engine.project_ensure_coordinator_run_id();
    }

    if action == CoordinatorAction::Stop {
        let coordinator_path = paths.automation_coordinator_path();
        let stopped =
            stop_coordinator_process_groups(&paths.root, &coordinator_path, input.graceful)?;
        println!("Coordinator process groups signaled: {}", stopped);
    }

    let logger_action = match action {
        CoordinatorAction::ControlPlaneRun => Some("run"),
        CoordinatorAction::Dispatch => Some("dispatch"),
        CoordinatorAction::Advance => Some("advance"),
        CoordinatorAction::Sync => Some("sync"),
        CoordinatorAction::Reconcile => Some("reconcile"),
        CoordinatorAction::Cleanup => Some("cleanup"),
        _ => None,
    };
    let native_logger = if let Some(action_name) = logger_action {
        let logger =
            build_native_logger(&paths.root, action_name, &input.env_cfg, coordinator_cfg)?;
        println!("Coordinator log file: {}", logger.file.display());
        Some(logger)
    } else {
        None
    };
    let logger_adapter = native_logger.as_ref().map(LoggerAdapter);

    let response = engine.coordinator_perform_action_workflow(
        paths,
        action,
        CoordinatorActionRequest {
            canonical: Some(canonical),
            coordinator_cfg,
            env_cfg: &input.env_cfg,
            extra_args: &input.extra_args,
            logger: logger_adapter.as_ref().map(|adapter| {
                adapter as &dyn macc_core::coordinator::control_plane::CoordinatorLog
            }),
            graceful: input.graceful,
            remove_worktrees: input.remove_worktrees,
            remove_branches: input.remove_branches,
        },
    )?;

    if let Some(status) = response.status {
        print_status_summary(&paths.root, &status);
    }
    if let Some(runtime) = response.runtime_status {
        println!("{}", runtime);
    }
    if let Some(copied) = response.aggregated_performer_logs {
        println!("Aggregated {} performer log file(s).", copied);
    }
    if let Some(resumed) = response.resumed {
        if resumed {
            println!("Coordinator resume signal applied.");
        } else {
            println!("Coordinator is not paused.");
        }
    }
    if let Some(path) = response.exported_events_path {
        println!(
            "Coordinator storage export complete (sqlite -> json): {}",
            path.display()
        );
    } else if action == CoordinatorAction::StorageImport {
        println!("Coordinator storage import complete (json -> sqlite).");
    } else if action == CoordinatorAction::StorageVerify {
        println!("Coordinator storage parity OK (json == sqlite).");
    }
    if let Some(removed) = response.removed_worktrees {
        println!("Removed {} worktree(s).", removed);
        println!("Pruned git worktrees.");
    }

    Ok(())
}
