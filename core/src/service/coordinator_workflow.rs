use crate::config::CoordinatorConfig;
use crate::coordinator::args::{
    parse_coordinator_extra_kv_args, RuntimeStatusFromEventArgs, RuntimeTransitionArgs,
    WorkflowTransitionArgs,
};
use crate::coordinator::control_plane::CoordinatorLog;
use crate::coordinator::engine as coordinator_engine;
use crate::coordinator::runtime as coordinator_runtime;
use crate::coordinator::runtime_status_from_event;
use crate::coordinator::state_runtime;
use crate::coordinator::types::CoordinatorEnvConfig;
use crate::coordinator::{
    is_valid_runtime_transition, is_valid_workflow_transition, WorkflowState,
};
use crate::coordinator_storage::{
    CoordinatorSnapshot, CoordinatorStorage, CoordinatorStorageMode, CoordinatorStoragePaths,
    JsonStorage, SqliteStorage,
};
use crate::service::coordinator::{
    coordinator_poll_managed_action_process, coordinator_start_managed_action_process,
    coordinator_stop_managed_action_process, CoordinatorManagedPoll,
};
use crate::{MaccError, ProjectPaths, Result};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorAction {
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
    pub fn emits_runtime_events(self) -> bool {
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
                "Unknown coordinator action '{}'",
                other
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoordinatorRunOptions {
    pub extra_args: Vec<String>,
    pub env_cfg: CoordinatorEnvConfig,
}

pub struct CoordinatorActionRequest<'a> {
    pub canonical: Option<&'a crate::config::CanonicalConfig>,
    pub coordinator_cfg: Option<&'a CoordinatorConfig>,
    pub env_cfg: &'a CoordinatorEnvConfig,
    pub extra_args: &'a [String],
    pub logger: Option<&'a dyn CoordinatorLog>,
    pub graceful: bool,
    pub remove_worktrees: bool,
    pub remove_branches: bool,
}

#[derive(Debug, Clone, Default)]
pub struct CoordinatorActionResult {
    pub status: Option<CoordinatorStatus>,
    pub resumed: Option<bool>,
    pub aggregated_performer_logs: Option<usize>,
    pub runtime_status: Option<String>,
    pub exported_events_path: Option<PathBuf>,
    pub removed_worktrees: Option<usize>,
}

#[derive(Debug, Clone, Default)]
pub struct CoordinatorStatus {
    pub total: usize,
    pub todo: usize,
    pub active: usize,
    pub blocked: usize,
    pub merged: usize,
    pub paused: bool,
    pub pause_reason: Option<String>,
    pub pause_task_id: Option<String>,
    pub pause_phase: Option<String>,
    pub latest_error: Option<String>,
    pub failure_report: Option<crate::service::diagnostic::FailureReport>,
}

pub fn coordinator_run_cycle<E: crate::engine::Engine + ?Sized>(
    engine: &E,
    paths: &ProjectPaths,
    canonical: &crate::config::CanonicalConfig,
    coordinator_cfg: Option<&CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
    logger: Option<&dyn CoordinatorLog>,
) -> Result<()> {
    coordinator_sync(engine, paths, coordinator_cfg, env_cfg, logger)?;
    coordinator_dispatch(engine, paths, canonical, coordinator_cfg, env_cfg, logger)?;
    coordinator_advance(engine, paths, coordinator_cfg, env_cfg, logger)?;
    Ok(())
}

pub fn coordinator_perform_action<E: crate::engine::Engine + ?Sized>(
    engine: &E,
    paths: &ProjectPaths,
    action: CoordinatorAction,
    request: CoordinatorActionRequest<'_>,
) -> Result<CoordinatorActionResult> {
    let mut result = CoordinatorActionResult::default();
    match action {
        CoordinatorAction::Run => {
            if !request.extra_args.is_empty() {
                return Err(MaccError::Validation(
                    "Action 'run' does not accept extra args after '--'.".into(),
                ));
            }
            let options = CoordinatorRunOptions {
                extra_args: request.extra_args.to_vec(),
                env_cfg: request.env_cfg.clone(),
            };
            coordinator_run(paths, request.coordinator_cfg, &options)?;
        }
        CoordinatorAction::ControlPlaneRun => {
            let canonical = request.canonical.ok_or_else(|| {
                MaccError::Validation("control-plane-run requires canonical config".into())
            })?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .enable_io()
                .build()
                .map_err(|e| {
                    MaccError::Validation(format!("Failed to initialize tokio runtime: {}", e))
                })?;
            runtime.block_on(coordinator_engine::run_native_control_plane(
                &paths.root,
                canonical,
                request.coordinator_cfg,
                request.env_cfg,
                request.logger,
            ))?;
        }
        CoordinatorAction::Dispatch => {
            if !request.extra_args.is_empty() {
                return Err(MaccError::Validation(
                    "Action 'dispatch' does not accept extra args in native mode.".into(),
                ));
            }
            let canonical = request.canonical.ok_or_else(|| {
                MaccError::Validation("dispatch requires canonical config".into())
            })?;
            coordinator_dispatch(
                engine,
                paths,
                canonical,
                request.coordinator_cfg,
                request.env_cfg,
                request.logger,
            )?;
        }
        CoordinatorAction::Advance => {
            if !request.extra_args.is_empty() {
                return Err(MaccError::Validation(
                    "Action 'advance' does not accept extra args in native mode.".into(),
                ));
            }
            coordinator_advance(
                engine,
                paths,
                request.coordinator_cfg,
                request.env_cfg,
                request.logger,
            )?;
        }
        CoordinatorAction::Resume => {
            if !request.extra_args.is_empty() {
                return Err(MaccError::Validation(
                    "Action 'resume' does not accept extra args in native mode.".into(),
                ));
            }
            let before = engine.coordinator_status_snapshot(paths)?;
            if before.paused {
                engine.coordinator_resume(&paths.root)?;
                result.resumed = Some(true);
            } else {
                result.resumed = Some(false);
            }
        }
        CoordinatorAction::Sync => {
            if !request.extra_args.is_empty() {
                return Err(MaccError::Validation(
                    "Action 'sync' does not accept extra args in native mode.".into(),
                ));
            }
            coordinator_sync(
                engine,
                paths,
                request.coordinator_cfg,
                request.env_cfg,
                request.logger,
            )?;
        }
        CoordinatorAction::Status => {
            if !request.extra_args.is_empty() {
                return Err(MaccError::Validation(
                    "Action 'status' does not accept extra args in native mode.".into(),
                ));
            }
            result.status = Some(get_coordinator_status(paths)?);
        }
        CoordinatorAction::Reconcile => {
            if !request.extra_args.is_empty() {
                return Err(MaccError::Validation(
                    "Action 'reconcile' does not accept extra args in native mode.".into(),
                ));
            }
            coordinator_reconcile(paths, request.logger)?;
        }
        CoordinatorAction::Cleanup => {
            if !request.extra_args.is_empty() {
                return Err(MaccError::Validation(
                    "Action 'cleanup' does not accept extra args in native mode.".into(),
                ));
            }
            coordinator_cleanup(paths, request.logger)?;
        }
        CoordinatorAction::Unlock => {
            coordinator_unlock(
                engine,
                paths,
                request.coordinator_cfg,
                request.env_cfg,
                request.extra_args,
            )?;
        }
        CoordinatorAction::RetryPhase => {
            let canonical = request.canonical.ok_or_else(|| {
                MaccError::Validation("retry-phase requires canonical config".into())
            })?;
            coordinator_retry_phase(
                engine,
                paths,
                canonical,
                request.coordinator_cfg,
                request.env_cfg,
                request.extra_args,
                request.logger,
            )?;
        }
        CoordinatorAction::CutoverGate => {
            if !request.extra_args.is_empty() {
                return Err(MaccError::Validation(
                    "Action 'cutover-gate' does not accept extra args in native mode.".into(),
                ));
            }
            coordinator_cutover_gate(paths)?;
        }
        CoordinatorAction::Stop => {
            coordinator_stop(paths, "manual stop")?;
            coordinator_reconcile(paths, request.logger)?;
            coordinator_cleanup(paths, request.logger)?;
            coordinator_unlock(
                engine,
                paths,
                request.coordinator_cfg,
                request.env_cfg,
                &[
                    "--all".to_string(),
                    "--unlock-state".to_string(),
                    "blocked".to_string(),
                ],
            )?;
            if request.remove_worktrees {
                let removed = crate::service::worktree::remove_all_worktrees(
                    &paths.root,
                    request.remove_branches,
                )?;
                crate::prune_worktrees(&paths.root)?;
                result.removed_worktrees = Some(removed);
            }
            let _ = request.graceful;
        }
        CoordinatorAction::ValidateTransition => {
            let parsed = WorkflowTransitionArgs::try_from(request.extra_args)?;
            if !is_valid_workflow_transition(parsed.from, parsed.to) {
                return Err(MaccError::Validation(format!(
                    "invalid transition {} -> {}",
                    parsed.from.as_str(),
                    parsed.to.as_str()
                )));
            }
        }
        CoordinatorAction::ValidateRuntimeTransition => {
            let parsed = RuntimeTransitionArgs::try_from(request.extra_args)?;
            if !is_valid_runtime_transition(parsed.from, parsed.to) {
                return Err(MaccError::Validation(format!(
                    "invalid runtime transition {} -> {}",
                    parsed.from.as_str(),
                    parsed.to.as_str()
                )));
            }
        }
        CoordinatorAction::RuntimeStatusFromEvent => {
            let parsed = RuntimeStatusFromEventArgs::try_from(request.extra_args)?;
            result.runtime_status = Some(
                runtime_status_from_event(&parsed.event_type, &parsed.status)
                    .as_str()
                    .to_string(),
            );
        }
        CoordinatorAction::StorageImport => {
            engine.coordinator_storage_import_json_to_sqlite(paths)?;
        }
        CoordinatorAction::StorageExport | CoordinatorAction::EventsExport => {
            engine.coordinator_storage_export_sqlite_to_json(paths)?;
            result.exported_events_path = Some(
                paths
                    .root
                    .join(".macc")
                    .join("log")
                    .join("coordinator")
                    .join("events.jsonl"),
            );
        }
        CoordinatorAction::StorageVerify => {
            engine.coordinator_storage_verify_parity(paths)?;
        }
        CoordinatorAction::StorageSync => {
            let direction =
                crate::coordinator::args::StorageSyncArgs::try_from(request.extra_args)?.direction;
            match direction {
                crate::coordinator_storage::CoordinatorStorageTransfer::ImportJsonToSqlite => {
                    engine.coordinator_storage_import_json_to_sqlite(paths)?
                }
                crate::coordinator_storage::CoordinatorStorageTransfer::ExportSqliteToJson => {
                    engine.coordinator_storage_export_sqlite_to_json(paths)?
                }
                crate::coordinator_storage::CoordinatorStorageTransfer::VerifyParity => {
                    engine.coordinator_storage_verify_parity(paths)?
                }
            }
        }
        CoordinatorAction::AggregatePerformerLogs => {
            result.aggregated_performer_logs =
                Some(engine.coordinator_aggregate_performer_logs(&paths.root)?);
        }
        CoordinatorAction::StateApplyTransition => {
            let args = parse_coordinator_extra_kv_args(request.extra_args)?;
            engine.coordinator_state_apply_transition(&paths.root, &args)?;
        }
        CoordinatorAction::StateSetRuntime => {
            let args = parse_coordinator_extra_kv_args(request.extra_args)?;
            engine.coordinator_state_set_runtime(&paths.root, &args)?;
        }
        CoordinatorAction::StateTaskField => {
            let args = parse_coordinator_extra_kv_args(request.extra_args)?;
            engine.coordinator_state_task_field(&paths.root, &args)?;
        }
        CoordinatorAction::StateTaskExists => {
            let args = parse_coordinator_extra_kv_args(request.extra_args)?;
            engine.coordinator_state_task_exists(&paths.root, &args)?;
        }
        CoordinatorAction::StateCounts => {
            let args = parse_coordinator_extra_kv_args(request.extra_args)?;
            engine.coordinator_state_counts(&paths.root, &args)?;
        }
        CoordinatorAction::StateLocks => {
            let args = parse_coordinator_extra_kv_args(request.extra_args)?;
            engine.coordinator_state_locks(&paths.root, &args)?;
        }
        CoordinatorAction::StateSetMergePending => {
            let args = parse_coordinator_extra_kv_args(request.extra_args)?;
            engine.coordinator_state_set_merge_pending(&paths.root, &args)?;
        }
        CoordinatorAction::StateSetMergeProcessed => {
            let args = parse_coordinator_extra_kv_args(request.extra_args)?;
            engine.coordinator_state_set_merge_processed(&paths.root, &args)?;
        }
        CoordinatorAction::StateIncrementRetries => {
            let args = parse_coordinator_extra_kv_args(request.extra_args)?;
            engine.coordinator_state_increment_retries(&paths.root, &args)?;
        }
        CoordinatorAction::StateUpsertSloWarning => {
            let args = parse_coordinator_extra_kv_args(request.extra_args)?;
            engine.coordinator_state_upsert_slo_warning(&paths.root, &args)?;
        }
        CoordinatorAction::StateSloMetric => {
            let args = parse_coordinator_extra_kv_args(request.extra_args)?;
            engine.coordinator_state_slo_metric(&paths.root, &args)?;
        }
        CoordinatorAction::SelectReadyTask => {
            return Err(MaccError::Validation(
                "Action 'select-ready-task' is not available via workflow facade yet.".into(),
            ));
        }
    }
    Ok(result)
}

pub fn coordinator_run(
    paths: &ProjectPaths,
    cfg: Option<&CoordinatorConfig>,
    options: &CoordinatorRunOptions,
) -> Result<()> {
    let _ = options.env_cfg;
    coordinator_start_managed_action_process(paths, "run", &options.extra_args, cfg)?;

    loop {
        match coordinator_poll_managed_action_process(paths)? {
            CoordinatorManagedPoll::Idle => return Ok(()),
            CoordinatorManagedPoll::Running { .. } => {
                std::thread::sleep(Duration::from_millis(250))
            }
            CoordinatorManagedPoll::Exited {
                success,
                action,
                code,
                ..
            } => {
                if success {
                    return Ok(());
                }
                return Err(MaccError::Validation(format!(
                    "coordinator '{}' failed (status {})",
                    action,
                    code.map(|c| c.to_string())
                        .unwrap_or_else(|| "unknown".to_string())
                )));
            }
        }
    }
}

pub fn coordinator_stop(paths: &ProjectPaths, reason: &str) -> Result<()> {
    state_runtime::write_coordinator_pause_file(
        &paths.root,
        "global",
        "dev",
        &format!("stopped: {}", reason),
    )?;
    let _ = coordinator_stop_managed_action_process(paths, false)?;
    Ok(())
}

pub fn get_coordinator_status(paths: &ProjectPaths) -> Result<CoordinatorStatus> {
    let storage_paths = CoordinatorStoragePaths::from_project_paths(paths);
    let sqlite = SqliteStorage::new(storage_paths.clone());
    let snapshot: CoordinatorSnapshot = if sqlite.has_snapshot_data()? {
        sqlite.load_snapshot()?
    } else {
        JsonStorage::new(storage_paths).load_snapshot()?
    };

    let mut status = CoordinatorStatus::default();
    let tasks = snapshot
        .registry
        .get("tasks")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();

    status.total = tasks.len();
    for task in tasks {
        match task
            .get("state")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("todo")
        {
            "todo" => status.todo += 1,
            "blocked" => status.blocked += 1,
            "merged" => status.merged += 1,
            _ => status.active += 1,
        }
    }

    if let Some(pause) = state_runtime::read_coordinator_pause_file(&paths.root)? {
        status.paused = true;
        status.pause_reason = pause
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .map(|s| s.to_string());
        status.pause_task_id = pause
            .get("task_id")
            .and_then(serde_json::Value::as_str)
            .map(|s| s.to_string());
        status.pause_phase = pause
            .get("phase")
            .and_then(serde_json::Value::as_str)
            .map(|s| s.to_string());
    }

    status.failure_report = crate::service::diagnostic::analyze_last_failure(paths)?;
    if let Some(report) = &status.failure_report {
        status.latest_error = Some(report.message.clone());
    } else {
        let logs = crate::service::logs::read_log_content(paths, "coordinator", None, None)
            .unwrap_or_default();
        status.latest_error = logs
            .lines()
            .rev()
            .find(|line| line.contains("ERROR") || line.contains("failed"))
            .map(|line| line.trim().to_string());
    }

    Ok(status)
}

pub fn coordinator_dispatch<E: crate::engine::Engine + ?Sized>(
    engine: &E,
    paths: &ProjectPaths,
    canonical: &crate::config::CanonicalConfig,
    coordinator_cfg: Option<&CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
    logger: Option<&dyn CoordinatorLog>,
) -> Result<()> {
    let prd_file = env_cfg
        .prd
        .as_ref()
        .map(std::path::PathBuf::from)
        .or_else(|| {
            coordinator_cfg
                .and_then(|c| c.prd_file.clone())
                .map(std::path::PathBuf::from)
        })
        .unwrap_or_else(|| paths.root.join("prd.json"));
    engine.coordinator_sync_registry_from_prd(&paths.root, &prd_file)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .map_err(|e| MaccError::Validation(format!("Failed to initialize tokio runtime: {}", e)))?;
    runtime.block_on(async {
        let mut state = coordinator_runtime::CoordinatorRunState::new();
        let _ = engine
            .coordinator_dispatch_ready_tasks_native(
                &paths.root,
                canonical,
                coordinator_cfg,
                env_cfg,
                &prd_file,
                &mut state,
                logger,
            )
            .await?;
        let max_attempts = env_cfg
            .phase_runner_max_attempts
            .or_else(|| coordinator_cfg.and_then(|c| c.phase_runner_max_attempts))
            .unwrap_or(1)
            .max(1);
        let phase_timeout = env_cfg
            .stale_in_progress_seconds
            .or_else(|| coordinator_cfg.and_then(|c| c.stale_in_progress_seconds))
            .unwrap_or(0);
        while !state.active_jobs.is_empty() {
            engine
                .coordinator_monitor_active_jobs_native(
                    &paths.root,
                    env_cfg,
                    &mut state,
                    max_attempts,
                    phase_timeout,
                    logger,
                )
                .await?;
            tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        }
        Result::<()>::Ok(())
    })?;
    Ok(())
}

pub fn coordinator_advance<E: crate::engine::Engine + ?Sized>(
    engine: &E,
    paths: &ProjectPaths,
    coordinator_cfg: Option<&CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
    logger: Option<&dyn CoordinatorLog>,
) -> Result<()> {
    let coordinator_tool_override = env_cfg
        .coordinator_tool
        .clone()
        .or_else(|| coordinator_cfg.and_then(|c| c.coordinator_tool.clone()));
    let phase_runner_max_attempts = env_cfg
        .phase_runner_max_attempts
        .or_else(|| coordinator_cfg.and_then(|c| c.phase_runner_max_attempts))
        .unwrap_or(1)
        .max(1);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .map_err(|e| MaccError::Validation(format!("Failed to initialize tokio runtime: {}", e)))?;
    let advance = runtime.block_on(async {
        let mut state = coordinator_runtime::CoordinatorRunState::new();
        engine
            .coordinator_advance_tasks_native(
                &paths.root,
                coordinator_tool_override.as_deref(),
                phase_runner_max_attempts,
                &mut state,
                logger,
            )
            .await
    })?;
    if let Some((task_id, reason)) = advance.blocked_merge {
        state_runtime::set_task_paused_for_integrate(&paths.root, &task_id, &reason)?;
        state_runtime::write_coordinator_pause_file(&paths.root, &task_id, "integrate", &reason)?;
        return Err(MaccError::Validation(format!(
            "Coordinator paused on task {} (integrate). Resolve the merge issue, then run `macc coordinator resume`. Reason: {}",
            task_id, reason
        )));
    }
    Ok(())
}

pub fn coordinator_reconcile(
    paths: &ProjectPaths,
    logger: Option<&dyn CoordinatorLog>,
) -> Result<()> {
    if let Some(logger) = logger {
        let _ = logger.note("- Reconcile start".to_string());
    }
    state_runtime::reconcile_registry_native(&paths.root)?;
    if let Some(logger) = logger {
        let _ = logger.note("- Reconcile complete".to_string());
    }
    Ok(())
}

pub fn coordinator_cleanup(
    paths: &ProjectPaths,
    logger: Option<&dyn CoordinatorLog>,
) -> Result<()> {
    if let Some(logger) = logger {
        let _ = logger.note("- Cleanup start".to_string());
    }
    state_runtime::cleanup_registry_native(&paths.root)?;
    if let Some(logger) = logger {
        let _ = logger.note("- Cleanup complete".to_string());
    }
    Ok(())
}

pub fn coordinator_sync<E: crate::engine::Engine + ?Sized>(
    engine: &E,
    paths: &ProjectPaths,
    coordinator_cfg: Option<&CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
    logger: Option<&dyn CoordinatorLog>,
) -> Result<()> {
    let prd_file = env_cfg
        .prd
        .as_ref()
        .map(std::path::PathBuf::from)
        .or_else(|| {
            coordinator_cfg
                .and_then(|c| c.prd_file.clone())
                .map(std::path::PathBuf::from)
        })
        .unwrap_or_else(|| paths.root.join("prd.json"));
    let storage_mode = coordinator_engine::resolve_storage_mode(env_cfg, coordinator_cfg)?;
    if storage_mode != CoordinatorStorageMode::Json {
        engine.coordinator_storage_import_json_to_sqlite(paths)?;
    }
    engine.coordinator_sync_registry_from_prd_with_logger(&paths.root, &prd_file, logger)?;
    if storage_mode != CoordinatorStorageMode::Json {
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
            engine.coordinator_storage_export_sqlite_to_json(paths)?;
        }
    }
    Ok(())
}

pub fn coordinator_unlock<E: crate::engine::Engine + ?Sized>(
    engine: &E,
    paths: &ProjectPaths,
    coordinator_cfg: Option<&CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
    args: &[String],
) -> Result<()> {
    let (task_id, resource, clear_all, unlock_state) = parse_unlock_args(args)?;
    let mut state_args = BTreeMap::new();
    apply_storage_mode_args(&mut state_args, env_cfg, coordinator_cfg);
    if let Some(task_id) = task_id {
        state_args.insert("task-id".to_string(), task_id);
        state_args.insert("state".to_string(), unlock_state);
        state_args.insert("reason".to_string(), "manual_unlock".to_string());
        engine.coordinator_state_apply_transition(&paths.root, &state_args)?;
        return Ok(());
    }
    if clear_all {
        let _ = engine.coordinator_state_unlock_resource(&paths.root, &state_args, None, true)?;
        return Ok(());
    }
    if let Some(resource) = resource {
        let _ = engine.coordinator_state_unlock_resource(
            &paths.root,
            &state_args,
            Some(&resource),
            false,
        )?;
        return Ok(());
    }
    Err(MaccError::Validation(format!(
        "unlock requires --task, --resource, or --all (unlock-state={})",
        unlock_state
    )))
}

pub fn coordinator_cutover_gate(paths: &ProjectPaths) -> Result<()> {
    let events_file = paths
        .root
        .join(".macc")
        .join("log")
        .join("coordinator")
        .join("events.jsonl");
    if !events_file.exists() {
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

pub fn coordinator_retry_phase<E: crate::engine::Engine + ?Sized>(
    engine: &E,
    paths: &ProjectPaths,
    canonical: &crate::config::CanonicalConfig,
    coordinator_cfg: Option<&CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
    args: &[String],
    logger: Option<&dyn CoordinatorLog>,
) -> Result<()> {
    let (task_id, phase, skip) = parse_retry_phase_args(args)?;
    let mut state_args = BTreeMap::new();
    apply_storage_mode_args(&mut state_args, env_cfg, coordinator_cfg);
    let mut snapshot = engine.coordinator_state_snapshot(&paths.root, &state_args)?;
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
        snapshot.registry["updated_at"] =
            serde_json::Value::String(crate::coordinator::helpers::now_iso_coordinator());
        engine.coordinator_state_save_snapshot(&paths.root, &state_args, &snapshot)?;
        return Ok(());
    }

    let mut retry_args = BTreeMap::new();
    apply_storage_mode_args(&mut retry_args, env_cfg, coordinator_cfg);
    retry_args.insert("task-id".to_string(), task_id.clone());
    engine.coordinator_state_increment_retries(&paths.root, &retry_args)?;

    match phase.as_str() {
        "dev" => retry_dev_phase(engine, paths, canonical, env_cfg, task_id.as_str(), logger)?,
        "review" | "fix" | "integrate" => retry_tool_phase(
            engine,
            paths,
            canonical,
            coordinator_cfg,
            env_cfg,
            &mut snapshot,
            task_id.as_str(),
            &phase,
            logger,
        )?,
        other => {
            return Err(MaccError::Validation(format!(
                "unsupported retry phase '{}'",
                other
            )));
        }
    }

    engine.coordinator_state_save_snapshot(&paths.root, &state_args, &snapshot)?;
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

fn retry_dev_phase<E: crate::engine::Engine + ?Sized>(
    engine: &E,
    paths: &ProjectPaths,
    canonical: &crate::config::CanonicalConfig,
    env_cfg: &CoordinatorEnvConfig,
    task_id: &str,
    logger: Option<&dyn CoordinatorLog>,
) -> Result<()> {
    let registry_path = paths
        .root
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
    let mut state = coordinator_runtime::CoordinatorRunState::new();
    let current_exe = std::env::current_exe().map_err(|e| {
        MaccError::Validation(format!("Failed to resolve current executable path: {}", e))
    })?;
    let pid = coordinator_runtime::spawn_performer_job(
        &current_exe,
        &paths.root,
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
                    &paths.root,
                    env_cfg,
                    &mut state,
                    1,
                    env_cfg.stale_in_progress_seconds.unwrap_or(0),
                    logger,
                )
                .await?;
            tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        }
        Result::<()>::Ok(())
    })?;
    let _ = canonical;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn retry_tool_phase<E: crate::engine::Engine + ?Sized>(
    engine: &E,
    paths: &ProjectPaths,
    canonical: &crate::config::CanonicalConfig,
    coordinator_cfg: Option<&CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
    snapshot: &mut crate::coordinator_storage::CoordinatorSnapshot,
    task_id: &str,
    phase: &str,
    logger: Option<&dyn CoordinatorLog>,
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
            &paths.root,
            &task,
            coordinator_tool_override.as_deref(),
            attempts,
            logger,
        )?;
        match verdict {
            Ok(v) => {
                coordinator_engine::apply_review_phase_success(
                    find_task_mut(&mut snapshot.registry, task_id)?,
                    v,
                    &crate::coordinator::helpers::now_iso_coordinator(),
                )?;
            }
            Err(reason) => {
                coordinator_engine::apply_phase_failure(
                    find_task_mut(&mut snapshot.registry, task_id)?,
                    "review",
                    &reason,
                    &crate::coordinator::helpers::now_iso_coordinator(),
                )?;
                return Err(MaccError::Validation(reason));
            }
        }
        return Ok(());
    }
    let result = engine.coordinator_run_phase_for_task_native(
        &paths.root,
        &task,
        phase,
        coordinator_tool_override.as_deref(),
        attempts,
        logger,
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
                &crate::coordinator::helpers::now_iso_coordinator(),
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
                    )));
                }
            };
            coordinator_engine::apply_phase_failure(
                find_task_mut(&mut snapshot.registry, task_id)?,
                phase_static,
                &reason,
                &crate::coordinator::helpers::now_iso_coordinator(),
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

fn apply_storage_mode_args(
    args: &mut BTreeMap<String, String>,
    env_cfg: &CoordinatorEnvConfig,
    coordinator_cfg: Option<&CoordinatorConfig>,
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
                )))
            }
        }
    }
    Ok((task_id, resource, clear_all, unlock_state))
}
