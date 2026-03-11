use crate::coordinator::helpers::now_iso_coordinator;
use crate::coordinator::state_runtime::{
    cleanup_registry_native, reconcile_registry_native, set_task_paused_for_integrate,
    write_coordinator_pause_file,
};
use crate::{
    coordinator_runtime_status_from_event_action, coordinator_select_ready_task_action,
    coordinator_storage_sync_action, load_canonical_config, remove_all_worktrees,
    stop_coordinator_process_groups, validate_coordinator_runtime_transition_action,
    validate_coordinator_transition_action, CoordinatorRunState, NativeCoordinatorLogger,
};
use macc_core::coordinator::args::parse_coordinator_extra_kv_args;
use macc_core::coordinator::engine as coordinator_engine;
use macc_core::coordinator::runtime as coordinator_runtime;
use macc_core::coordinator::types::CoordinatorEnvConfig;
use macc_core::coordinator::WorkflowState;
use macc_core::coordinator_storage::CoordinatorStorageMode;
use macc_core::{MaccError, Result};
use std::path::Path;
use std::str::FromStr;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoordinatorAction {
    Run,
    ControlPlaneRun,
    Dispatch,
    Advance,
    Resume,
    Sync,
    Status,
    Reconcile,
    Unlock,
    Cleanup,
    RetryPhase,
    CutoverGate,
    Stop,
    ValidateTransition,
    ValidateRuntimeTransition,
    RuntimeStatusFromEvent,
    StorageSync,
    StorageImport,
    StorageExport,
    EventsExport,
    StorageVerify,
    SelectReadyTask,
    AggregatePerformerLogs,
    StateApplyTransition,
    StateSetRuntime,
    StateTaskField,
    StateTaskExists,
    StateCounts,
    StateLocks,
    StateSetMergePending,
    StateSetMergeProcessed,
    StateIncrementRetries,
    StateUpsertSloWarning,
    StateSloMetric,
}

impl CoordinatorAction {
    fn emits_runtime_events(self) -> bool {
        matches!(
            self,
            Self::Run
                | Self::ControlPlaneRun
                | Self::Dispatch
                | Self::Advance
                | Self::Reconcile
                | Self::Cleanup
                | Self::Sync
                | Self::RetryPhase
        )
    }
}

impl FromStr for CoordinatorAction {
    type Err = MaccError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "run" => Ok(Self::Run),
            "control-plane-run" => Ok(Self::ControlPlaneRun),
            "dispatch" => Ok(Self::Dispatch),
            "advance" => Ok(Self::Advance),
            "resume" => Ok(Self::Resume),
            "sync" => Ok(Self::Sync),
            "status" => Ok(Self::Status),
            "reconcile" => Ok(Self::Reconcile),
            "unlock" => Ok(Self::Unlock),
            "cleanup" => Ok(Self::Cleanup),
            "retry-phase" => Ok(Self::RetryPhase),
            "cutover-gate" => Ok(Self::CutoverGate),
            "stop" => Ok(Self::Stop),
            "validate-transition" => Ok(Self::ValidateTransition),
            "validate-runtime-transition" => Ok(Self::ValidateRuntimeTransition),
            "runtime-status-from-event" => Ok(Self::RuntimeStatusFromEvent),
            "storage-sync" => Ok(Self::StorageSync),
            "storage-import" => Ok(Self::StorageImport),
            "storage-export" => Ok(Self::StorageExport),
            "events-export" => Ok(Self::EventsExport),
            "storage-verify" => Ok(Self::StorageVerify),
            "select-ready-task" => Ok(Self::SelectReadyTask),
            "aggregate-performer-logs" => Ok(Self::AggregatePerformerLogs),
            "state-apply-transition" => Ok(Self::StateApplyTransition),
            "state-set-runtime" => Ok(Self::StateSetRuntime),
            "state-task-field" => Ok(Self::StateTaskField),
            "state-task-exists" => Ok(Self::StateTaskExists),
            "state-counts" => Ok(Self::StateCounts),
            "state-locks" => Ok(Self::StateLocks),
            "state-set-merge-pending" => Ok(Self::StateSetMergePending),
            "state-set-merge-processed" => Ok(Self::StateSetMergeProcessed),
            "state-increment-retries" => Ok(Self::StateIncrementRetries),
            "state-upsert-slo-warning" => Ok(Self::StateUpsertSloWarning),
            "state-slo-metric" => Ok(Self::StateSloMetric),
            other => Err(MaccError::Validation(format!(
                "Unsupported coordinator action in native mode: {}",
                other
            ))),
        }
    }
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
    let action = CoordinatorAction::from_str(input.action.as_str())?;

    if action == CoordinatorAction::ValidateTransition {
        validate_coordinator_transition_action(&input.extra_args)?;
        return Ok(());
    }
    if action == CoordinatorAction::ValidateRuntimeTransition {
        validate_coordinator_runtime_transition_action(&input.extra_args)?;
        return Ok(());
    }
    if action == CoordinatorAction::RuntimeStatusFromEvent {
        coordinator_runtime_status_from_event_action(&input.extra_args)?;
        return Ok(());
    }
    if action == CoordinatorAction::StorageSync {
        coordinator_storage_sync_action(absolute_cwd, &input.extra_args)?;
        return Ok(());
    }
    if action == CoordinatorAction::StorageImport {
        let paths = macc_core::ProjectPaths::from_root(absolute_cwd);
        engine.coordinator_storage_import_json_to_sqlite(&paths)?;
        println!("Coordinator storage import complete (json -> sqlite).");
        return Ok(());
    }
    if matches!(
        action,
        CoordinatorAction::StorageExport | CoordinatorAction::EventsExport
    ) {
        let paths = macc_core::ProjectPaths::from_root(absolute_cwd);
        engine.coordinator_storage_export_sqlite_to_json(&paths)?;
        println!(
            "Coordinator storage export complete (sqlite -> json): {}",
            paths
                .root
                .join(".macc")
                .join("log")
                .join("coordinator")
                .join("events.jsonl")
                .display()
        );
        return Ok(());
    }
    if action == CoordinatorAction::StorageVerify {
        let paths = macc_core::ProjectPaths::from_root(absolute_cwd);
        engine.coordinator_storage_verify_parity(&paths)?;
        println!("Coordinator storage parity OK (json == sqlite).");
        return Ok(());
    }
    if action == CoordinatorAction::SelectReadyTask {
        coordinator_select_ready_task_action(absolute_cwd, &input.extra_args)?;
        return Ok(());
    }
    if action == CoordinatorAction::AggregatePerformerLogs {
        let copied = engine.coordinator_aggregate_performer_logs(absolute_cwd)?;
        println!("Aggregated {} performer log file(s).", copied);
        return Ok(());
    }
    if action == CoordinatorAction::StateApplyTransition {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        engine.coordinator_state_apply_transition(absolute_cwd, &args)?;
        return Ok(());
    }
    if action == CoordinatorAction::StateSetRuntime {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        engine.coordinator_state_set_runtime(absolute_cwd, &args)?;
        return Ok(());
    }
    if action == CoordinatorAction::StateTaskField {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        engine.coordinator_state_task_field(absolute_cwd, &args)?;
        return Ok(());
    }
    if action == CoordinatorAction::StateTaskExists {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        engine.coordinator_state_task_exists(absolute_cwd, &args)?;
        return Ok(());
    }
    if action == CoordinatorAction::StateCounts {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        engine.coordinator_state_counts(absolute_cwd, &args)?;
        return Ok(());
    }
    if action == CoordinatorAction::StateLocks {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        engine.coordinator_state_locks(absolute_cwd, &args)?;
        return Ok(());
    }
    if action == CoordinatorAction::StateSetMergePending {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        engine.coordinator_state_set_merge_pending(absolute_cwd, &args)?;
        return Ok(());
    }
    if action == CoordinatorAction::StateSetMergeProcessed {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        engine.coordinator_state_set_merge_processed(absolute_cwd, &args)?;
        return Ok(());
    }
    if action == CoordinatorAction::StateIncrementRetries {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        engine.coordinator_state_increment_retries(absolute_cwd, &args)?;
        return Ok(());
    }
    if action == CoordinatorAction::StateUpsertSloWarning {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        engine.coordinator_state_upsert_slo_warning(absolute_cwd, &args)?;
        return Ok(());
    }
    if action == CoordinatorAction::StateSloMetric {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        engine.coordinator_state_slo_metric(absolute_cwd, &args)?;
        return Ok(());
    }

    let context = ProjectContext::load(absolute_cwd, engine)?;
    let paths = context.paths;
    let canonical = context.canonical;
    let coordinator_cfg = context.coordinator_cfg;

    if action == CoordinatorAction::Run && !input.no_tui {
        return macc_tui::run_tui_with_launch(macc_tui::LaunchMode::CoordinatorRun).map_err(|e| {
            MaccError::Io {
                path: "tui".into(),
                action: "run_tui coordinator live".into(),
                source: std::io::Error::other(e.to_string()),
            }
        });
    }

    let _ = macc_core::ensure_embedded_automation_scripts(&paths)?;

    if let Ok(effective_storage_mode) =
        coordinator_engine::resolve_storage_mode(&input.env_cfg, coordinator_cfg.as_ref())
    {
        let mode_raw = match effective_storage_mode {
            CoordinatorStorageMode::Json => "json",
            CoordinatorStorageMode::DualWrite => "dual-write",
            CoordinatorStorageMode::Sqlite => "sqlite",
        };
        std::env::set_var("COORDINATOR_STORAGE_MODE", mode_raw);
    }
    if let Some(debounce_ms) = input.env_cfg.mirror_json_debounce_ms.or_else(|| {
        coordinator_cfg
            .as_ref()
            .and_then(|c| c.mirror_json_debounce_ms)
    }) {
        std::env::set_var(
            "COORDINATOR_JSON_EXPORT_DEBOUNCE_MS",
            debounce_ms.to_string(),
        );
    }
    if action.emits_runtime_events() {
        let _ = engine.project_ensure_coordinator_run_id();
    }

    if action == CoordinatorAction::ControlPlaneRun {
        let logger =
            build_native_logger(&paths.root, "run", &input.env_cfg, coordinator_cfg.as_ref())?;
        println!("Coordinator log file: {}", logger.file.display());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .map_err(|e| {
                MaccError::Validation(format!("Failed to initialize tokio runtime: {}", e))
            })?;
        runtime.block_on(coordinator_engine::run_native_control_plane(
            &paths.root,
            &canonical,
            coordinator_cfg.as_ref(),
            &input.env_cfg,
            Some(&LoggerAdapter(&logger)),
        ))?;
        return Ok(());
    }

    if action == CoordinatorAction::Stop {
        let coordinator_path = paths.automation_coordinator_path();
        let stopped =
            stop_coordinator_process_groups(&paths.root, &coordinator_path, input.graceful)?;
        println!("Coordinator process groups signaled: {}", stopped);
        let _ = engine.coordinator_stop(&paths.root, "manual stop");
        reconcile_registry_native(&paths.root)?;
        cleanup_registry_native(&paths.root)?;
        unlock_resource_locks_native(
            engine,
            &paths.root,
            &input.env_cfg,
            coordinator_cfg.as_ref(),
            None,
            true,
            "blocked",
        )?;
        if input.remove_worktrees {
            let removed = remove_all_worktrees(&paths.root, input.remove_branches)?;
            println!("Removed {} worktree(s).", removed);
            macc_core::prune_worktrees(&paths.root)?;
            println!("Pruned git worktrees.");
        }
    } else if action == CoordinatorAction::Run {
        if !input.extra_args.is_empty() {
            return Err(MaccError::Validation(
                "Action 'run' does not accept extra args after '--'.".into(),
            ));
        }
        let logger =
            build_native_logger(&paths.root, "run", &input.env_cfg, coordinator_cfg.as_ref())?;
        println!("Coordinator log file: {}", logger.file.display());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .map_err(|e| {
                MaccError::Validation(format!("Failed to initialize tokio runtime: {}", e))
            })?;
        runtime.block_on(coordinator_engine::run_native_control_plane(
            &paths.root,
            &canonical,
            coordinator_cfg.as_ref(),
            &input.env_cfg,
            Some(&LoggerAdapter(&logger)),
        ))?;
    } else if action == CoordinatorAction::Dispatch {
        if !input.extra_args.is_empty() {
            return Err(MaccError::Validation(
                "Action 'dispatch' does not accept extra args in native mode.".into(),
            ));
        }
        let prd_file = input
            .env_cfg
            .prd
            .as_ref()
            .map(std::path::PathBuf::from)
            .or_else(|| {
                coordinator_cfg
                    .as_ref()
                    .and_then(|c| c.prd_file.clone())
                    .map(std::path::PathBuf::from)
            })
            .unwrap_or_else(|| paths.root.join("prd.json"));
        engine.coordinator_sync_registry_from_prd(&paths.root, &prd_file)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .map_err(|e| {
                MaccError::Validation(format!("Failed to initialize tokio runtime: {}", e))
            })?;
        let logger = build_native_logger(
            &paths.root,
            "dispatch",
            &input.env_cfg,
            coordinator_cfg.as_ref(),
        )?;
        println!("Coordinator log file: {}", logger.file.display());
        runtime.block_on(async {
            let mut state = CoordinatorRunState::new();
            let _ = engine
                .coordinator_dispatch_ready_tasks_native(
                    &paths.root,
                    &canonical,
                    coordinator_cfg.as_ref(),
                    &input.env_cfg,
                    &prd_file,
                    &mut state,
                    Some(&LoggerAdapter(&logger)),
                )
                .await?;
            let max_attempts = input
                .env_cfg
                .phase_runner_max_attempts
                .or_else(|| {
                    coordinator_cfg
                        .as_ref()
                        .and_then(|c| c.phase_runner_max_attempts)
                })
                .unwrap_or(1)
                .max(1);
            let phase_timeout = input
                .env_cfg
                .stale_in_progress_seconds
                .or_else(|| {
                    coordinator_cfg
                        .as_ref()
                        .and_then(|c| c.stale_in_progress_seconds)
                })
                .unwrap_or(0);
            while !state.active_jobs.is_empty() {
                engine
                    .coordinator_monitor_active_jobs_native(
                        &paths.root,
                        &input.env_cfg,
                        &mut state,
                        max_attempts,
                        phase_timeout,
                        Some(&LoggerAdapter(&logger)),
                    )
                    .await?;
                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
            }
            Result::<()>::Ok(())
        })?;
    } else if action == CoordinatorAction::Advance {
        if !input.extra_args.is_empty() {
            return Err(MaccError::Validation(
                "Action 'advance' does not accept extra args in native mode.".into(),
            ));
        }
        let logger = build_native_logger(
            &paths.root,
            "advance",
            &input.env_cfg,
            coordinator_cfg.as_ref(),
        )?;
        println!("Coordinator log file: {}", logger.file.display());
        let coordinator_tool_override = input.env_cfg.coordinator_tool.clone().or_else(|| {
            coordinator_cfg
                .as_ref()
                .and_then(|c| c.coordinator_tool.clone())
        });
        let phase_runner_max_attempts = input
            .env_cfg
            .phase_runner_max_attempts
            .or_else(|| {
                coordinator_cfg
                    .as_ref()
                    .and_then(|c| c.phase_runner_max_attempts)
            })
            .unwrap_or(1)
            .max(1);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .map_err(|e| {
                MaccError::Validation(format!("Failed to initialize tokio runtime: {}", e))
            })?;
        let advance = runtime.block_on(async {
            let mut state = CoordinatorRunState::new();
            engine
                .coordinator_advance_tasks_native(
                    &paths.root,
                    coordinator_tool_override.as_deref(),
                    phase_runner_max_attempts,
                    &mut state,
                    Some(&LoggerAdapter(&logger)),
                )
                .await
        })?;
        if let Some((task_id, reason)) = advance.blocked_merge {
            set_task_paused_for_integrate(&paths.root, &task_id, &reason)?;
            write_coordinator_pause_file(&paths.root, &task_id, "integrate", &reason)?;
            return Err(MaccError::Validation(format!(
                "Coordinator paused on task {} (integrate). Resolve the merge issue, then run `macc coordinator resume`. Reason: {}",
                task_id, reason
            )));
        }
    } else if action == CoordinatorAction::Resume {
        if !input.extra_args.is_empty() {
            return Err(MaccError::Validation(
                "Action 'resume' does not accept extra args in native mode.".into(),
            ));
        }
        let before = engine.coordinator_status_snapshot(&paths)?;
        if before.paused {
            engine.coordinator_resume(&paths.root)?;
            println!("Coordinator resume signal applied.");
        } else {
            println!("Coordinator is not paused.");
        }
    } else if action == CoordinatorAction::Sync {
        if !input.extra_args.is_empty() {
            return Err(MaccError::Validation(
                "Action 'sync' does not accept extra args in native mode.".into(),
            ));
        }
        let prd_file = input
            .env_cfg
            .prd
            .as_ref()
            .map(std::path::PathBuf::from)
            .or_else(|| {
                coordinator_cfg
                    .as_ref()
                    .and_then(|c| c.prd_file.clone())
                    .map(std::path::PathBuf::from)
            })
            .unwrap_or_else(|| paths.root.join("prd.json"));
        let logger = build_native_logger(
            &paths.root,
            "sync",
            &input.env_cfg,
            coordinator_cfg.as_ref(),
        )?;
        println!("Coordinator log file: {}", logger.file.display());
        let storage_mode =
            coordinator_engine::resolve_storage_mode(&input.env_cfg, coordinator_cfg.as_ref())?;
        if storage_mode != CoordinatorStorageMode::Json {
            let storage_paths = macc_core::ProjectPaths::from_root(&paths.root);
            engine.coordinator_storage_import_json_to_sqlite(&storage_paths)?;
        }
        engine.coordinator_sync_registry_from_prd_with_logger(
            &paths.root,
            &prd_file,
            Some(&LoggerAdapter(&logger)),
        )?;
        if storage_mode != CoordinatorStorageMode::Json {
            let storage_paths = macc_core::ProjectPaths::from_root(&paths.root);
            if std::env::var("COORDINATOR_JSON_COMPAT")
                .ok()
                .map(|raw| {
                    !matches!(
                        raw.trim().to_ascii_lowercase().as_str(),
                        "0" | "false" | "no" | "off"
                    )
                })
                .unwrap_or(false)
            {
                engine.coordinator_storage_export_sqlite_to_json(&storage_paths)?;
            }
        }
    } else if action == CoordinatorAction::Reconcile {
        if !input.extra_args.is_empty() {
            return Err(MaccError::Validation(
                "Action 'reconcile' does not accept extra args in native mode.".into(),
            ));
        }
        let logger = build_native_logger(
            &paths.root,
            "reconcile",
            &input.env_cfg,
            coordinator_cfg.as_ref(),
        )?;
        println!("Coordinator log file: {}", logger.file.display());
        let _ = logger.note("- Reconcile start");
        reconcile_registry_native(&paths.root)?;
        let _ = logger.note("- Reconcile complete");
    } else if action == CoordinatorAction::Cleanup {
        if !input.extra_args.is_empty() {
            return Err(MaccError::Validation(
                "Action 'cleanup' does not accept extra args in native mode.".into(),
            ));
        }
        let logger = build_native_logger(
            &paths.root,
            "cleanup",
            &input.env_cfg,
            coordinator_cfg.as_ref(),
        )?;
        println!("Coordinator log file: {}", logger.file.display());
        let _ = logger.note("- Cleanup start");
        cleanup_registry_native(&paths.root)?;
        let _ = logger.note("- Cleanup complete");
    } else if action == CoordinatorAction::Status {
        if !input.extra_args.is_empty() {
            return Err(MaccError::Validation(
                "Action 'status' does not accept extra args in native mode.".into(),
            ));
        }
        print_status_summary_native(
            engine,
            &paths,
            &paths.root,
            &input.env_cfg,
            coordinator_cfg.as_ref(),
        )?;
    } else if action == CoordinatorAction::Unlock {
        let (task_id, resource, clear_all, unlock_state) = parse_unlock_args(&input.extra_args)?;
        if let Some(task_id) = task_id {
            unlock_task_state_native(
                engine,
                &paths.root,
                &input.env_cfg,
                coordinator_cfg.as_ref(),
                &task_id,
                &unlock_state,
            )?;
        } else {
            unlock_resource_locks_native(
                engine,
                &paths.root,
                &input.env_cfg,
                coordinator_cfg.as_ref(),
                resource,
                clear_all,
                &unlock_state,
            )?;
        }
    } else if action == CoordinatorAction::RetryPhase {
        handle_retry_phase_native(
            engine,
            &paths.root,
            &canonical,
            coordinator_cfg.as_ref(),
            &input.env_cfg,
            &input.extra_args,
        )?;
    } else if action == CoordinatorAction::CutoverGate {
        if !input.extra_args.is_empty() {
            return Err(MaccError::Validation(
                "Action 'cutover-gate' does not accept extra args in native mode.".into(),
            ));
        }
        run_cutover_gate_native(&paths.root)?;
    }

    Ok(())
}

fn parse_unlock_args(args: &[String]) -> Result<(Option<String>, Option<String>, bool, String)> {
    let mut task_id = None;
    let mut resource = None;
    let mut clear_all = false;
    let mut unlock_state = "blocked".to_string();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--task" => {
                if i + 1 >= args.len() {
                    return Err(MaccError::Validation(
                        "unlock --task requires a value".into(),
                    ));
                }
                task_id = Some(args[i + 1].clone());
                i += 2;
            }
            "--resource" => {
                if i + 1 >= args.len() {
                    return Err(MaccError::Validation(
                        "unlock --resource requires a value".into(),
                    ));
                }
                resource = Some(args[i + 1].clone());
                i += 2;
            }
            "--all" => {
                clear_all = true;
                i += 1;
            }
            "--unlock-state" => {
                if i + 1 >= args.len() {
                    return Err(MaccError::Validation(
                        "unlock --unlock-state requires a value".into(),
                    ));
                }
                unlock_state = args[i + 1].clone();
                i += 2;
            }
            other => {
                return Err(MaccError::Validation(format!(
                    "Unknown unlock arg: {}",
                    other
                )));
            }
        }
    }
    Ok((task_id, resource, clear_all, unlock_state))
}

fn unlock_task_state_native(
    engine: &crate::services::engine_provider::SharedEngine,
    repo_root: &Path,
    env_cfg: &CoordinatorEnvConfig,
    coordinator_cfg: Option<&macc_core::config::CoordinatorConfig>,
    task_id: &str,
    unlock_state: &str,
) -> Result<()> {
    let mut args = std::collections::BTreeMap::new();
    apply_storage_mode_args(&mut args, env_cfg, coordinator_cfg);
    args.insert("task-id".to_string(), task_id.to_string());
    args.insert("state".to_string(), unlock_state.to_string());
    args.insert("reason".to_string(), "manual_unlock".to_string());
    engine.coordinator_state_apply_transition(repo_root, &args)?;
    println!(
        "Unlocked task {} via transition to {}",
        task_id, unlock_state
    );
    Ok(())
}

fn unlock_resource_locks_native(
    engine: &crate::services::engine_provider::SharedEngine,
    repo_root: &Path,
    env_cfg: &CoordinatorEnvConfig,
    coordinator_cfg: Option<&macc_core::config::CoordinatorConfig>,
    resource: Option<String>,
    clear_all: bool,
    unlock_state: &str,
) -> Result<()> {
    let mut args = std::collections::BTreeMap::new();
    apply_storage_mode_args(&mut args, env_cfg, coordinator_cfg);
    if clear_all {
        let removed = engine.coordinator_state_unlock_resource(repo_root, &args, None, true)?;
        println!("Cleared all resource locks ({})", removed);
        return Ok(());
    }
    if let Some(resource) = resource {
        let removed =
            engine.coordinator_state_unlock_resource(repo_root, &args, Some(&resource), false)?;
        if removed == 0 {
            println!("Resource lock not found: {}", resource);
        } else {
            println!("Unlocked resource {}", resource);
        }
        return Ok(());
    }
    return Err(MaccError::Validation(format!(
        "unlock requires --task, --resource, or --all (unlock-state={})",
        unlock_state
    )));
}

fn apply_storage_mode_args(
    args: &mut std::collections::BTreeMap<String, String>,
    env_cfg: &CoordinatorEnvConfig,
    coordinator_cfg: Option<&macc_core::config::CoordinatorConfig>,
) {
    if let Some(value) = env_cfg
        .storage_mode
        .clone()
        .or_else(|| coordinator_cfg.and_then(|c| c.storage_mode.clone()))
    {
        args.insert("storage-mode".to_string(), value);
    }
    if let Some(value) = env_cfg
        .mirror_json_debounce_ms
        .or_else(|| coordinator_cfg.and_then(|c| c.mirror_json_debounce_ms))
    {
        args.insert("mirror-json-debounce-ms".to_string(), value.to_string());
    }
}

fn print_status_summary_native(
    engine: &crate::services::engine_provider::SharedEngine,
    paths: &macc_core::ProjectPaths,
    repo_root: &Path,
    env_cfg: &CoordinatorEnvConfig,
    coordinator_cfg: Option<&macc_core::config::CoordinatorConfig>,
) -> Result<()> {
    let _ = (env_cfg, coordinator_cfg);
    let status = engine.coordinator_status_snapshot(paths)?;
    let registry_path = repo_root
        .join(".macc")
        .join("automation")
        .join("task")
        .join("task_registry.json");
    println!("Registry: {}", registry_path.display());
    println!("Tasks: {}", status.total);
    println!("  todo: {}", status.todo);
    println!("  active: {}", status.active);
    println!("  blocked: {}", status.blocked);
    println!("  merged: {}", status.merged);
    if status.paused {
        println!("Paused: yes");
        if let Some(task) = status.pause_task_id {
            println!("  task: {}", task);
        }
        if let Some(phase) = status.pause_phase {
            println!("  phase: {}", phase);
        }
        if let Some(reason) = status.pause_reason {
            println!("  reason: {}", reason);
        }
    } else {
        println!("Paused: no");
    }
    Ok(())
}

fn handle_retry_phase_native(
    engine: &crate::services::engine_provider::SharedEngine,
    repo_root: &Path,
    canonical: &macc_core::config::CanonicalConfig,
    coordinator_cfg: Option<&macc_core::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
    args: &[String],
) -> Result<()> {
    let (task_id, phase, skip) = parse_retry_phase_args(args)?;
    let mut state_args = std::collections::BTreeMap::new();
    apply_storage_mode_args(&mut state_args, env_cfg, coordinator_cfg);
    let mut snapshot = engine.coordinator_state_snapshot(repo_root, &state_args)?;
    let tasks = snapshot
        .registry
        .get_mut("tasks")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| MaccError::Validation("Registry missing tasks array".into()))?;
    let Some(task) = tasks.iter_mut().find(|t| {
        t.get("id")
            .and_then(serde_json::Value::as_str)
            .map(|id| id == task_id)
            .unwrap_or(false)
    }) else {
        return Err(MaccError::Validation(format!(
            "Task not found in registry: {}",
            task_id
        )));
    };

    if skip {
        task["state"] = serde_json::Value::String("todo".to_string());
        engine.coordinator_state_reset_runtime_to_idle(task);
        snapshot.registry["updated_at"] = serde_json::Value::String(now_iso_coordinator());
        engine.coordinator_state_save_snapshot(repo_root, &state_args, &snapshot)?;
        println!(
            "Skipped phase '{}' for task {}; task moved back to todo.",
            phase, task_id
        );
        return Ok(());
    }

    let mut retry_args = std::collections::BTreeMap::new();
    apply_storage_mode_args(&mut retry_args, env_cfg, coordinator_cfg);
    retry_args.insert("task-id".to_string(), task_id.clone());
    engine.coordinator_state_increment_retries(repo_root, &retry_args)?;

    match phase.as_str() {
        "dev" => {
            retry_dev_phase_native(engine, repo_root, canonical, env_cfg, task_id.as_str())?;
            return Ok(());
        }
        "review" | "fix" | "integrate" => retry_tool_phase_native(
            engine,
            repo_root,
            canonical,
            coordinator_cfg,
            env_cfg,
            &mut snapshot,
            task_id.as_str(),
            &phase,
        )?,
        other => {
            return Err(MaccError::Validation(format!(
                "unsupported retry phase '{}'",
                other
            )))
        }
    }

    engine.coordinator_state_save_snapshot(repo_root, &state_args, &snapshot)?;
    Ok(())
}

fn parse_retry_phase_args(args: &[String]) -> Result<(String, String, bool)> {
    let mut task_id = None;
    let mut phase = None;
    let mut skip = false;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--retry-task" => {
                if i + 1 >= args.len() {
                    return Err(MaccError::Validation(
                        "retry-phase --retry-task requires a value".into(),
                    ));
                }
                task_id = Some(args[i + 1].clone());
                i += 2;
            }
            "--retry-phase" => {
                if i + 1 >= args.len() {
                    return Err(MaccError::Validation(
                        "retry-phase --retry-phase requires a value".into(),
                    ));
                }
                phase = Some(args[i + 1].clone());
                i += 2;
            }
            "--skip" => {
                skip = true;
                i += 1;
            }
            other => {
                return Err(MaccError::Validation(format!(
                    "Unknown retry-phase arg: {}",
                    other
                )));
            }
        }
    }
    let task_id =
        task_id.ok_or_else(|| MaccError::Validation("retry-phase requires --retry-task".into()))?;
    let phase =
        phase.ok_or_else(|| MaccError::Validation("retry-phase requires --retry-phase".into()))?;
    Ok((task_id, phase, skip))
}

fn retry_dev_phase_native(
    engine: &crate::services::engine_provider::SharedEngine,
    repo_root: &Path,
    canonical: &macc_core::config::CanonicalConfig,
    env_cfg: &CoordinatorEnvConfig,
    task_id: &str,
) -> Result<()> {
    let registry_path = repo_root
        .join(".macc")
        .join("automation")
        .join("task")
        .join("task_registry.json");
    let raw = std::fs::read_to_string(&registry_path).map_err(|e| MaccError::Io {
        path: registry_path.to_string_lossy().into(),
        action: "read coordinator registry".into(),
        source: e,
    })?;
    let registry: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
        MaccError::Validation(format!(
            "Failed to parse coordinator registry {}: {}",
            registry_path.display(),
            e
        ))
    })?;
    let task = registry
        .get("tasks")
        .and_then(serde_json::Value::as_array)
        .and_then(|tasks| {
            tasks.iter().find(|t| {
                t.get("id")
                    .and_then(serde_json::Value::as_str)
                    .map(|id| id == task_id)
                    .unwrap_or(false)
            })
        })
        .ok_or_else(|| MaccError::Validation("Task missing for retry".into()))?;
    let worktree_path = task
        .get("worktree")
        .and_then(|w| w.get("worktree_path"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| MaccError::Validation("retry-phase requires worktree".into()))?;
    let worktree = std::path::PathBuf::from(worktree_path);
    let mut state = CoordinatorRunState::new();
    let logger = NativeCoordinatorLogger::new_with_flush(
        repo_root,
        "retry-phase",
        env_cfg.log_flush_lines,
        env_cfg.log_flush_ms,
    )?;
    println!("Coordinator log file: {}", logger.file.display());
    let current_exe = std::env::current_exe().map_err(|e| {
        MaccError::Validation(format!("Failed to resolve current executable path: {}", e))
    })?;
    let pid = coordinator_runtime::spawn_performer_job(
        &current_exe,
        repo_root,
        task_id,
        &worktree,
        &state.event_tx,
        &mut state.join_set,
        env_cfg.stale_in_progress_seconds.unwrap_or(0),
    )?;
    state.active_jobs.insert(
        task_id.to_string(),
        coordinator_runtime::CoordinatorJob {
            tool: task
                .get("tool")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("codex")
                .to_string(),
            worktree_path: worktree.clone(),
            attempt: 1,
            started_at: std::time::Instant::now(),
            pid,
        },
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .map_err(|e| MaccError::Validation(format!("Failed to init tokio: {}", e)))?;
    runtime.block_on(async {
        while !state.active_jobs.is_empty() {
            engine
                .coordinator_monitor_active_jobs_native(
                    repo_root,
                    env_cfg,
                    &mut state,
                    1,
                    env_cfg.stale_in_progress_seconds.unwrap_or(0),
                    Some(&LoggerAdapter(&logger)),
                )
                .await?;
            tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        }
        Result::<()>::Ok(())
    })?;
    let _ = canonical;
    Ok(())
}

fn retry_tool_phase_native(
    engine: &crate::services::engine_provider::SharedEngine,
    repo_root: &Path,
    canonical: &macc_core::config::CanonicalConfig,
    coordinator_cfg: Option<&macc_core::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
    snapshot: &mut macc_core::coordinator_storage::CoordinatorSnapshot,
    task_id: &str,
    phase: &str,
) -> Result<()> {
    let task = snapshot
        .registry
        .get("tasks")
        .and_then(serde_json::Value::as_array)
        .and_then(|tasks| {
            tasks.iter().find(|t| {
                t.get("id")
                    .and_then(serde_json::Value::as_str)
                    .map(|id| id == task_id)
                    .unwrap_or(false)
            })
        })
        .ok_or_else(|| MaccError::Validation("Task missing for retry".into()))?
        .clone();
    let logger = build_native_logger(repo_root, "retry-phase", env_cfg, coordinator_cfg)?;
    println!("Coordinator log file: {}", logger.file.display());
    let coordinator_tool_override = env_cfg
        .coordinator_tool
        .clone()
        .or_else(|| coordinator_cfg.and_then(|c| c.coordinator_tool.clone()));
    let attempts = env_cfg
        .phase_runner_max_attempts
        .or_else(|| coordinator_cfg.and_then(|c| c.phase_runner_max_attempts))
        .unwrap_or(1)
        .max(1);
    if phase == "review" {
        let verdict = engine.coordinator_run_review_phase_for_task_native(
            repo_root,
            &task,
            coordinator_tool_override.as_deref(),
            attempts,
            Some(&LoggerAdapter(&logger)),
        )?;
        match verdict {
            Ok(v) => {
                coordinator_engine::apply_review_phase_success(
                    find_task_mut(&mut snapshot.registry, task_id)?,
                    v,
                    &now_iso_coordinator(),
                )?;
            }
            Err(reason) => {
                coordinator_engine::apply_phase_failure(
                    find_task_mut(&mut snapshot.registry, task_id)?,
                    "review",
                    &reason,
                    &now_iso_coordinator(),
                )?;
                return Err(MaccError::Validation(reason));
            }
        }
        return Ok(());
    }
    let result = engine.coordinator_run_phase_for_task_native(
        repo_root,
        &task,
        phase,
        coordinator_tool_override.as_deref(),
        attempts,
        Some(&LoggerAdapter(&logger)),
    )?;
    match result {
        Ok(_) => {
            let transition = match phase {
                "fix" => coordinator_engine::PhaseTransition {
                    mode: "fix",
                    next_state: WorkflowState::PrOpen,
                    runtime_phase: "fix",
                },
                "integrate" => coordinator_engine::PhaseTransition {
                    mode: "integrate",
                    next_state: WorkflowState::Queued,
                    runtime_phase: "integrate",
                },
                _ => return Ok(()),
            };
            coordinator_engine::apply_phase_success(
                find_task_mut(&mut snapshot.registry, task_id)?,
                transition,
                &now_iso_coordinator(),
            )?;
        }
        Err(reason) => {
            let phase_static = match phase {
                "review" => "review",
                "fix" => "fix",
                "integrate" => "integrate",
                _ => {
                    return Err(MaccError::Validation(format!(
                        "unsupported retry phase '{}'",
                        phase
                    )))
                }
            };
            coordinator_engine::apply_phase_failure(
                find_task_mut(&mut snapshot.registry, task_id)?,
                phase_static,
                &reason,
                &now_iso_coordinator(),
            )?;
            return Err(MaccError::Validation(reason));
        }
    }
    let _ = canonical;
    Ok(())
}

fn find_task_mut<'a>(
    registry: &'a mut serde_json::Value,
    task_id: &str,
) -> Result<&'a mut serde_json::Value> {
    let tasks = registry
        .get_mut("tasks")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| MaccError::Validation("Registry missing tasks array".into()))?;
    for task in tasks.iter_mut() {
        if task
            .get("id")
            .and_then(serde_json::Value::as_str)
            .map(|id| id == task_id)
            .unwrap_or(false)
        {
            return Ok(task);
        }
    }
    Err(MaccError::Validation(format!(
        "Task not found in registry: {}",
        task_id
    )))
}

fn run_cutover_gate_native(repo_root: &Path) -> Result<()> {
    let events_file = repo_root
        .join(".macc")
        .join("log")
        .join("coordinator")
        .join("events.jsonl");
    if !events_file.exists() {
        println!(
            "Cutover gate: no events file found at {}",
            events_file.display()
        );
        return Ok(());
    }
    let window_events: usize = std::env::var("CUTOVER_GATE_WINDOW_EVENTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000);
    let max_blocked_ratio: f64 = std::env::var("CUTOVER_GATE_MAX_BLOCKED_RATIO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.25);
    let max_stale_ratio: f64 = std::env::var("CUTOVER_GATE_MAX_STALE_RATIO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.25);
    let content = std::fs::read_to_string(&events_file).map_err(|e| MaccError::Io {
        path: events_file.to_string_lossy().into(),
        action: "read coordinator events".into(),
        source: e,
    })?;
    let lines: Vec<&str> = content.lines().rev().take(window_events).collect();
    let mut task_events = 0usize;
    let mut blocked_events = 0usize;
    let mut stale_events = 0usize;
    for raw in lines {
        let Ok(event) = serde_json::from_str::<serde_json::Value>(raw) else {
            continue;
        };
        let event_type = event
            .get("type")
            .or_else(|| event.get("event"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if event.get("task_id").is_some() {
            task_events += 1;
        }
        if event_type == "task_blocked" || event_type == "local_merge_failed" {
            blocked_events += 1;
        }
        if event_type == "stale_runtime_total" || event_type == "task_runtime_stale" {
            stale_events += 1;
        }
    }
    let blocked_ratio = if task_events == 0 {
        0.0
    } else {
        blocked_events as f64 / task_events as f64
    };
    let stale_ratio = if task_events == 0 {
        0.0
    } else {
        stale_events as f64 / task_events as f64
    };
    println!(
        "Cutover gate: events_window={} task_events={} blocked_ratio={:.6} stale_ratio={:.6}",
        window_events, task_events, blocked_ratio, stale_ratio
    );
    if blocked_ratio > max_blocked_ratio {
        return Err(MaccError::Validation(format!(
            "cutover gate failed: blocked ratio {} exceeds {}",
            blocked_ratio, max_blocked_ratio
        )));
    }
    if stale_ratio > max_stale_ratio {
        return Err(MaccError::Validation(format!(
            "cutover gate failed: stale ratio {} exceeds {}",
            stale_ratio, max_stale_ratio
        )));
    }
    Ok(())
}
