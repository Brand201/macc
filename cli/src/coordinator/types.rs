#[derive(Debug, Clone)]
pub(crate) struct CoordinatorEnvConfig {
    pub(crate) prd: Option<String>,
    pub(crate) coordinator_tool: Option<String>,
    pub(crate) reference_branch: Option<String>,
    pub(crate) tool_priority: Option<String>,
    pub(crate) max_parallel_per_tool_json: Option<String>,
    pub(crate) tool_specializations_json: Option<String>,
    pub(crate) max_dispatch: Option<usize>,
    pub(crate) max_parallel: Option<usize>,
    pub(crate) timeout_seconds: Option<usize>,
    pub(crate) phase_runner_max_attempts: Option<usize>,
    pub(crate) log_flush_lines: Option<usize>,
    pub(crate) log_flush_ms: Option<u64>,
    #[allow(dead_code)]
    pub(crate) stale_claimed_seconds: Option<usize>,
    pub(crate) stale_in_progress_seconds: Option<usize>,
    #[allow(dead_code)]
    pub(crate) stale_changes_requested_seconds: Option<usize>,
    #[allow(dead_code)]
    pub(crate) stale_action: Option<String>,
    pub(crate) storage_mode: Option<String>,
    pub(crate) error_code_retry_list: Option<String>,
    pub(crate) error_code_retry_max: Option<usize>,
}
