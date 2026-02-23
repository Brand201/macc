use crate::{
    cleanup_registry_native, clear_coordinator_pause_file, coordinator,
    coordinator_runtime_status_from_event_action, coordinator_select_ready_task_action,
    coordinator_storage_sync_action, ensure_initialized_paths, load_canonical_config,
    parse_coordinator_extra_kv_args, read_coordinator_pause_file, reconcile_registry_native,
    remove_all_worktrees, resolve_coordinator_storage_mode, resume_paused_task_integrate,
    run_coordinator_action, run_coordinator_control_plane_rust, set_task_paused_for_integrate,
    stop_coordinator_process_groups, validate_coordinator_runtime_transition_action,
    validate_coordinator_transition_action, write_coordinator_pause_file, CoordinatorEnvConfig,
    CoordinatorRunState, NativeCoordinatorLogger,
};
use macc_core::coordinator_storage::{
    sync_coordinator_storage, CoordinatorStorageMode, CoordinatorStoragePhase,
};
use macc_core::{MaccError, Result};
use std::path::Path;

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

pub fn handle(absolute_cwd: &Path, input: CoordinatorCommandInput) -> Result<()> {
    let action_name = input.action.as_str();

    if action_name == "validate-transition" {
        validate_coordinator_transition_action(&input.extra_args)?;
        return Ok(());
    }
    if action_name == "validate-runtime-transition" {
        validate_coordinator_runtime_transition_action(&input.extra_args)?;
        return Ok(());
    }
    if action_name == "runtime-status-from-event" {
        coordinator_runtime_status_from_event_action(&input.extra_args)?;
        return Ok(());
    }
    if action_name == "storage-sync" {
        coordinator_storage_sync_action(absolute_cwd, &input.extra_args)?;
        return Ok(());
    }
    if action_name == "select-ready-task" {
        coordinator_select_ready_task_action(absolute_cwd, &input.extra_args)?;
        return Ok(());
    }
    if action_name == "aggregate-performer-logs" {
        let copied = coordinator::logs::aggregate_performer_logs(absolute_cwd)?;
        println!("Aggregated {} performer log file(s).", copied);
        return Ok(());
    }
    if action_name == "state-apply-transition" {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        coordinator::state::coordinator_state_apply_transition(absolute_cwd, &args)?;
        return Ok(());
    }
    if action_name == "state-set-runtime" {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        coordinator::state::coordinator_state_set_runtime(absolute_cwd, &args)?;
        return Ok(());
    }
    if action_name == "state-task-field" {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        coordinator::state::coordinator_state_task_field(absolute_cwd, &args)?;
        return Ok(());
    }
    if action_name == "state-task-exists" {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        coordinator::state::coordinator_state_task_exists(absolute_cwd, &args)?;
        return Ok(());
    }
    if action_name == "state-counts" {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        coordinator::state::coordinator_state_counts(absolute_cwd, &args)?;
        return Ok(());
    }
    if action_name == "state-locks" {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        coordinator::state::coordinator_state_locks(absolute_cwd, &args)?;
        return Ok(());
    }
    if action_name == "state-set-merge-pending" {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        coordinator::state::coordinator_state_set_merge_pending(absolute_cwd, &args)?;
        return Ok(());
    }
    if action_name == "state-set-merge-processed" {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        coordinator::state::coordinator_state_set_merge_processed(absolute_cwd, &args)?;
        return Ok(());
    }
    if action_name == "state-increment-retries" {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        coordinator::state::coordinator_state_increment_retries(absolute_cwd, &args)?;
        return Ok(());
    }
    if action_name == "state-upsert-slo-warning" {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        coordinator::state::coordinator_state_upsert_slo_warning(absolute_cwd, &args)?;
        return Ok(());
    }
    if action_name == "state-slo-metric" {
        let args = parse_coordinator_extra_kv_args(&input.extra_args)?;
        coordinator::state::coordinator_state_slo_metric(absolute_cwd, &args)?;
        return Ok(());
    }

    let paths = ensure_initialized_paths(absolute_cwd)?;
    let canonical = load_canonical_config(&paths.config_path)?;
    let coordinator_cfg = canonical.automation.coordinator.clone();

    if action_name == "run" && !input.no_tui {
        return macc_tui::run_tui_with_launch(macc_tui::LaunchMode::CoordinatorRun).map_err(|e| {
            MaccError::Io {
                path: "tui".into(),
                action: "run_tui coordinator live".into(),
                source: std::io::Error::other(e.to_string()),
            }
        });
    }

    let _ = macc_core::ensure_embedded_automation_scripts(&paths)?;
    let coordinator_path = paths.automation_coordinator_path();
    if !coordinator_path.exists() {
        return Err(MaccError::Validation(format!(
            "Coordinator script not found: {}",
            coordinator_path.display()
        )));
    }

    if let Ok(effective_storage_mode) =
        resolve_coordinator_storage_mode(&input.env_cfg, coordinator_cfg.as_ref())
    {
        let mode_raw = match effective_storage_mode {
            CoordinatorStorageMode::Json => "json",
            CoordinatorStorageMode::DualWrite => "dual-write",
            CoordinatorStorageMode::Sqlite => "sqlite",
        };
        std::env::set_var("COORDINATOR_STORAGE_MODE", mode_raw);
    }

    if action_name == "control-plane-run" {
        run_coordinator_control_plane_rust(
            &paths.root,
            &coordinator_path,
            &canonical,
            coordinator_cfg.as_ref(),
            &input.env_cfg,
        )?;
        return Ok(());
    }

    if action_name == "stop" {
        let stopped =
            stop_coordinator_process_groups(&paths.root, &coordinator_path, input.graceful)?;
        println!("Coordinator process groups signaled: {}", stopped);
        run_coordinator_action(
            &paths.root,
            &coordinator_path,
            "reconcile",
            &[],
            &canonical,
            coordinator_cfg.as_ref(),
            &input.env_cfg,
        )?;
        run_coordinator_action(
            &paths.root,
            &coordinator_path,
            "cleanup",
            &[],
            &canonical,
            coordinator_cfg.as_ref(),
            &input.env_cfg,
        )?;
        run_coordinator_action(
            &paths.root,
            &coordinator_path,
            "unlock",
            &["--all".to_string()],
            &canonical,
            coordinator_cfg.as_ref(),
            &input.env_cfg,
        )?;
        if input.remove_worktrees {
            let removed = remove_all_worktrees(&paths.root, input.remove_branches)?;
            println!("Removed {} worktree(s).", removed);
            macc_core::prune_worktrees(&paths.root)?;
            println!("Pruned git worktrees.");
        }
    } else if action_name == "run" {
        if !input.extra_args.is_empty() {
            return Err(MaccError::Validation(
                "Action 'run' does not accept extra args after '--'.".into(),
            ));
        }
        run_coordinator_control_plane_rust(
            &paths.root,
            &coordinator_path,
            &canonical,
            coordinator_cfg.as_ref(),
            &input.env_cfg,
        )?;
    } else if action_name == "dispatch" {
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
        coordinator::control_plane::sync_registry_from_prd_native(&paths.root, &prd_file, None)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .map_err(|e| {
                MaccError::Validation(format!("Failed to initialize tokio runtime: {}", e))
            })?;
        let logger = NativeCoordinatorLogger::new(&paths.root, "dispatch")?;
        println!("Coordinator log file: {}", logger.file.display());
        runtime.block_on(async {
            let mut state = CoordinatorRunState::new();
            let _ = coordinator::control_plane::dispatch_ready_tasks_native(
                &paths.root,
                &canonical,
                coordinator_cfg.as_ref(),
                &input.env_cfg,
                &prd_file,
                &mut state,
                Some(&logger),
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
                coordinator::control_plane::monitor_active_jobs_native(
                    &paths.root,
                    &mut state,
                    max_attempts,
                    phase_timeout,
                    Some(&logger),
                )
                .await?;
                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
            }
            Result::<()>::Ok(())
        })?;
    } else if action_name == "advance" {
        if !input.extra_args.is_empty() {
            return Err(MaccError::Validation(
                "Action 'advance' does not accept extra args in native mode.".into(),
            ));
        }
        let logger = NativeCoordinatorLogger::new(&paths.root, "advance")?;
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
            coordinator::control_plane::advance_tasks_native(
                &paths.root,
                coordinator_tool_override.as_deref(),
                phase_runner_max_attempts,
                &mut state,
                Some(&logger),
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
    } else if action_name == "resume" {
        if !input.extra_args.is_empty() {
            return Err(MaccError::Validation(
                "Action 'resume' does not accept extra args in native mode.".into(),
            ));
        }
        let pause = read_coordinator_pause_file(&paths.root)?;
        if let Some(value) = pause {
            let task_id = value
                .get("task_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if !task_id.is_empty() {
                resume_paused_task_integrate(&paths.root, task_id)?;
            }
            let _ = clear_coordinator_pause_file(&paths.root)?;
            println!("Coordinator resume signal applied.");
        } else {
            println!("Coordinator is not paused.");
        }
    } else if action_name == "sync" {
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
        let logger = NativeCoordinatorLogger::new(&paths.root, "sync")?;
        println!("Coordinator log file: {}", logger.file.display());
        let storage_mode =
            resolve_coordinator_storage_mode(&input.env_cfg, coordinator_cfg.as_ref())?;
        if storage_mode != CoordinatorStorageMode::Json {
            let storage_paths = macc_core::ProjectPaths::from_root(&paths.root);
            sync_coordinator_storage(&storage_paths, storage_mode, CoordinatorStoragePhase::Pre)?;
        }
        coordinator::control_plane::sync_registry_from_prd_native(
            &paths.root,
            &prd_file,
            Some(&logger),
        )?;
        if storage_mode != CoordinatorStorageMode::Json {
            let storage_paths = macc_core::ProjectPaths::from_root(&paths.root);
            sync_coordinator_storage(&storage_paths, storage_mode, CoordinatorStoragePhase::Post)?;
        }
    } else if action_name == "reconcile" {
        if !input.extra_args.is_empty() {
            return Err(MaccError::Validation(
                "Action 'reconcile' does not accept extra args in native mode.".into(),
            ));
        }
        let logger = NativeCoordinatorLogger::new(&paths.root, "reconcile")?;
        println!("Coordinator log file: {}", logger.file.display());
        let _ = logger.note("- Reconcile start");
        reconcile_registry_native(&paths.root)?;
        let _ = logger.note("- Reconcile complete");
    } else if action_name == "cleanup" {
        if !input.extra_args.is_empty() {
            return Err(MaccError::Validation(
                "Action 'cleanup' does not accept extra args in native mode.".into(),
            ));
        }
        let logger = NativeCoordinatorLogger::new(&paths.root, "cleanup")?;
        println!("Coordinator log file: {}", logger.file.display());
        let _ = logger.note("- Cleanup start");
        cleanup_registry_native(&paths.root)?;
        let _ = logger.note("- Cleanup complete");
    } else {
        run_coordinator_action(
            &paths.root,
            &coordinator_path,
            action_name,
            &input.extra_args,
            &canonical,
            coordinator_cfg.as_ref(),
            &input.env_cfg,
        )?;
    }

    Ok(())
}
