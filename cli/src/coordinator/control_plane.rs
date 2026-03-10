use crate::{CoordinatorRunState, NativeCoordinatorLogger};
use macc_core::coordinator::control_plane as core_cp;
use macc_core::coordinator::types::CoordinatorEnvConfig;
use macc_core::Result;
use std::path::Path;

struct LoggerAdapter<'a>(&'a NativeCoordinatorLogger);

impl core_cp::CoordinatorLog for LoggerAdapter<'_> {
    fn note(&self, line: String) -> Result<()> {
        self.0.note(line)
    }
}

pub fn sync_registry_from_prd_native(
    repo_root: &Path,
    prd_file: &Path,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<()> {
    let adapter = logger.map(LoggerAdapter);
    core_cp::sync_registry_from_prd_native(
        repo_root,
        prd_file,
        adapter.as_ref().map(|v| v as &dyn core_cp::CoordinatorLog),
    )
}

pub fn run_phase_for_task_native(
    repo_root: &Path,
    task: &serde_json::Value,
    mode: &str,
    coordinator_tool_override: Option<&str>,
    max_attempts: usize,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<std::result::Result<String, String>> {
    let adapter = logger.map(LoggerAdapter);
    core_cp::run_phase_for_task_native(
        repo_root,
        task,
        mode,
        coordinator_tool_override,
        max_attempts,
        adapter.as_ref().map(|v| v as &dyn core_cp::CoordinatorLog),
    )
}

pub fn run_review_phase_for_task_native(
    repo_root: &Path,
    task: &serde_json::Value,
    coordinator_tool_override: Option<&str>,
    max_attempts: usize,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<std::result::Result<macc_core::coordinator::engine::ReviewVerdict, String>> {
    let adapter = logger.map(LoggerAdapter);
    core_cp::run_review_phase_for_task_native(
        repo_root,
        task,
        coordinator_tool_override,
        max_attempts,
        adapter.as_ref().map(|v| v as &dyn core_cp::CoordinatorLog),
    )
}

pub async fn advance_tasks_native(
    repo_root: &Path,
    coordinator_tool_override: Option<&str>,
    phase_runner_max_attempts: usize,
    state: &mut CoordinatorRunState,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<macc_core::coordinator::engine::AdvanceResult> {
    let adapter = logger.map(LoggerAdapter);
    core_cp::advance_tasks_native(
        repo_root,
        coordinator_tool_override,
        phase_runner_max_attempts,
        state,
        adapter.as_ref().map(|v| v as &dyn core_cp::CoordinatorLog),
    )
    .await
}

pub async fn monitor_active_jobs_native(
    repo_root: &Path,
    env_cfg: &CoordinatorEnvConfig,
    state: &mut CoordinatorRunState,
    max_attempts: usize,
    phase_timeout_seconds: usize,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<()> {
    let adapter = logger.map(LoggerAdapter);
    core_cp::monitor_active_jobs_native(
        repo_root,
        env_cfg,
        state,
        max_attempts,
        phase_timeout_seconds,
        adapter.as_ref().map(|v| v as &dyn core_cp::CoordinatorLog),
    )
    .await
}

pub async fn dispatch_ready_tasks_native(
    repo_root: &Path,
    canonical: &macc_core::config::CanonicalConfig,
    coordinator: Option<&macc_core::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
    prd_file: &Path,
    state: &mut CoordinatorRunState,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<usize> {
    let adapter = logger.map(LoggerAdapter);
    core_cp::dispatch_ready_tasks_native(
        repo_root,
        canonical,
        coordinator,
        env_cfg,
        prd_file,
        state,
        adapter.as_ref().map(|v| v as &dyn core_cp::CoordinatorLog),
    )
    .await
}
