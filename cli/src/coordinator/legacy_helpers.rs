use macc_core::coordinator::args::parse_coordinator_extra_kv_args;
#[cfg(test)]
use macc_core::coordinator::runtime::CoordinatorRunState;
#[cfg(test)]
use macc_core::coordinator::types::CoordinatorEnvConfig;
#[cfg(test)]
use macc_core::coordinator::{
    args::{RuntimeTransitionArgs, WorkflowTransitionArgs},
    is_valid_runtime_transition, is_valid_workflow_transition,
};
use macc_core::{MaccError, Result};

#[cfg(test)]
pub(crate) const COORDINATOR_TASK_REGISTRY_REL_PATH: &str =
    macc_core::coordinator::COORDINATOR_TASK_REGISTRY_REL_PATH;

pub(crate) struct NativeCoordinatorLogger {
    pub(crate) file: std::path::PathBuf,
    state: std::sync::Mutex<NativeCoordinatorLoggerState>,
    flush_every_lines: usize,
    flush_every_interval: std::time::Duration,
}

struct NativeCoordinatorLoggerState {
    writer: std::io::BufWriter<std::fs::File>,
    pending_lines: usize,
    last_flush: std::time::Instant,
}

impl NativeCoordinatorLogger {
    pub(crate) fn new_with_flush(
        repo_root: &std::path::Path,
        action: &str,
        flush_lines_override: Option<usize>,
        flush_ms_override: Option<u64>,
    ) -> Result<Self> {
        let dir = repo_root.join(".macc").join("log").join("coordinator");
        std::fs::create_dir_all(&dir).map_err(|e| MaccError::Io {
            path: dir.to_string_lossy().into(),
            action: "create coordinator log dir".into(),
            source: e,
        })?;
        let ts = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
        let file = dir.join(format!("{}-{}.md", action, ts));
        let header = format!(
            "# Coordinator log\n\n- Command: {}\n- Repository: {}\n- Started (UTC): {}\n\n",
            action,
            repo_root.display(),
            chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        );
        std::fs::write(&file, header).map_err(|e| MaccError::Io {
            path: file.to_string_lossy().into(),
            action: "write coordinator log header".into(),
            source: e,
        })?;
        let file_handle = std::fs::OpenOptions::new()
            .append(true)
            .open(&file)
            .map_err(|e| MaccError::Io {
                path: file.to_string_lossy().into(),
                action: "open coordinator log writer".into(),
                source: e,
            })?;
        let flush_every_lines = flush_lines_override
            .or_else(|| {
                std::env::var("COORDINATOR_LOG_FLUSH_LINES")
                    .ok()
                    .and_then(|v| v.trim().parse::<usize>().ok())
            })
            .filter(|v| *v > 0)
            .unwrap_or(500);
        let flush_every_interval = std::time::Duration::from_millis(
            flush_ms_override
                .or_else(|| {
                    std::env::var("COORDINATOR_LOG_FLUSH_MS")
                        .ok()
                        .and_then(|v| v.trim().parse::<u64>().ok())
                })
                .filter(|v| *v > 0)
                .unwrap_or(60_000),
        );

        Ok(Self {
            file,
            state: std::sync::Mutex::new(NativeCoordinatorLoggerState {
                writer: std::io::BufWriter::new(file_handle),
                pending_lines: 0,
                last_flush: std::time::Instant::now(),
            }),
            flush_every_lines,
            flush_every_interval,
        })
    }

    pub(crate) fn note(&self, msg: impl AsRef<str>) -> Result<()> {
        use std::io::Write as _;
        let line = format!("{}\n", msg.as_ref());
        tracing::info!(target: "macc.coordinator.log", "{}", msg.as_ref());
        let mut state = self
            .state
            .lock()
            .map_err(|_| MaccError::Validation("Coordinator logger lock poisoned".to_string()))?;
        state
            .writer
            .write_all(line.as_bytes())
            .map_err(|e| MaccError::Io {
                path: self.file.to_string_lossy().into(),
                action: "append coordinator log".into(),
                source: e,
            })?;
        state.pending_lines += 1;
        let should_flush = state.pending_lines >= self.flush_every_lines
            || state.last_flush.elapsed() >= self.flush_every_interval;
        if should_flush {
            state.writer.flush().map_err(|e| MaccError::Io {
                path: self.file.to_string_lossy().into(),
                action: "flush coordinator log".into(),
                source: e,
            })?;
            state.pending_lines = 0;
            state.last_flush = std::time::Instant::now();
        }
        Ok(())
    }
}

impl Drop for NativeCoordinatorLogger {
    fn drop(&mut self) {
        use std::io::Write as _;
        if let Ok(mut state) = self.state.lock() {
            let _ = state.writer.flush();
        }
    }
}

#[cfg(test)]
pub(crate) fn validate_coordinator_transition_action(args: &[String]) -> Result<()> {
    let parsed = WorkflowTransitionArgs::try_from(args)?;
    let from = parsed.from;
    let to = parsed.to;
    if is_valid_workflow_transition(from, to) {
        return Ok(());
    }
    Err(MaccError::Validation(format!(
        "invalid transition {} -> {}",
        from.as_str(),
        to.as_str()
    )))
}

#[cfg(test)]
pub(crate) fn validate_coordinator_runtime_transition_action(args: &[String]) -> Result<()> {
    let parsed = RuntimeTransitionArgs::try_from(args)?;
    let from = parsed.from;
    let to = parsed.to;
    if is_valid_runtime_transition(from, to) {
        return Ok(());
    }
    Err(MaccError::Validation(format!(
        "invalid runtime transition {} -> {}",
        from.as_str(),
        to.as_str()
    )))
}

pub(crate) fn coordinator_select_ready_task_action(
    repo_root: &std::path::Path,
    extra_args: &[String],
) -> Result<()> {
    let args = parse_coordinator_extra_kv_args(extra_args)?;
    let registry_path = args
        .get("registry")
        .map(std::path::PathBuf::from)
        .map(|p| {
            if p.is_absolute() {
                p
            } else {
                repo_root.join(p)
            }
        })
        .unwrap_or_else(|| {
            repo_root
                .join(".macc")
                .join("automation")
                .join("task")
                .join("task_registry.json")
        });
    let registry_raw = std::fs::read_to_string(&registry_path).map_err(|e| MaccError::Io {
        path: registry_path.to_string_lossy().into(),
        action: "read task registry for select-ready-task".into(),
        source: e,
    })?;
    let registry: serde_json::Value = serde_json::from_str(&registry_raw).map_err(|e| {
        MaccError::Validation(format!(
            "Failed to parse task registry JSON '{}': {}",
            registry_path.display(),
            e
        ))
    })?;

    let max_parallel_raw = args
        .get("max-parallel")
        .cloned()
        .or_else(|| std::env::var("MAX_PARALLEL").ok())
        .unwrap_or_else(|| "0".to_string());
    let default_tool = args
        .get("default-tool")
        .cloned()
        .or_else(|| std::env::var("DEFAULT_TOOL").ok())
        .unwrap_or_else(|| "codex".to_string());
    let default_base_branch = args
        .get("default-base-branch")
        .cloned()
        .or_else(|| std::env::var("DEFAULT_BASE_BRANCH").ok())
        .unwrap_or_else(|| "master".to_string());

    let config = macc_core::coordinator::task_selector::TaskSelectorConfig {
        enabled_tools: parse_json_string_vec(
            args.get("enabled-tools-json")
                .map(String::as_str)
                .unwrap_or("[]"),
            "enabled-tools-json",
        )?,
        tool_priority: parse_json_string_vec(
            args.get("tool-priority-json")
                .map(String::as_str)
                .unwrap_or("[]"),
            "tool-priority-json",
        )?,
        max_parallel_per_tool: parse_json_string_usize_map(
            args.get("max-parallel-per-tool-json")
                .map(String::as_str)
                .unwrap_or("{}"),
            "max-parallel-per-tool-json",
        )?,
        tool_specializations: parse_json_string_vec_map(
            args.get("tool-specializations-json")
                .map(String::as_str)
                .unwrap_or("{}"),
            "tool-specializations-json",
        )?,
        max_parallel: max_parallel_raw
            .parse::<usize>()
            .map_err(|e| MaccError::Validation(format!("Invalid max-parallel value: {}", e)))?,
        default_tool,
        default_base_branch,
    };

    if let Some(selected) =
        macc_core::coordinator::task_selector::select_next_ready_task(&registry, &config)
    {
        println!(
            "{}\t{}\t{}\t{}",
            selected.id, selected.title, selected.tool, selected.base_branch
        );
    }
    Ok(())
}

fn parse_json_string_vec(raw: &str, field_name: &str) -> Result<Vec<String>> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| MaccError::Validation(format!("Invalid JSON for {}: {}", field_name, e)))?;
    let arr = value
        .as_array()
        .ok_or_else(|| MaccError::Validation(format!("{} must be a JSON array", field_name)))?;
    let mut out = Vec::new();
    for item in arr {
        let value = item.as_str().ok_or_else(|| {
            MaccError::Validation(format!("{} must contain string values only", field_name))
        })?;
        if !value.is_empty() {
            out.push(value.to_string());
        }
    }
    Ok(out)
}

fn parse_json_string_usize_map(
    raw: &str,
    field_name: &str,
) -> Result<std::collections::HashMap<String, usize>> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| MaccError::Validation(format!("Invalid JSON for {}: {}", field_name, e)))?;
    let obj = value
        .as_object()
        .ok_or_else(|| MaccError::Validation(format!("{} must be a JSON object", field_name)))?;
    let mut out = std::collections::HashMap::new();
    for (k, v) in obj {
        let cap = if let Some(n) = v.as_u64() {
            n as usize
        } else if let Some(s) = v.as_str() {
            s.parse::<usize>().map_err(|e| {
                MaccError::Validation(format!(
                    "Invalid numeric value '{}' for key '{}' in {}: {}",
                    s, k, field_name, e
                ))
            })?
        } else {
            return Err(MaccError::Validation(format!(
                "Invalid value type for key '{}' in {}; expected number/string",
                k, field_name
            )));
        };
        out.insert(k.clone(), cap);
    }
    Ok(out)
}

fn parse_json_string_vec_map(
    raw: &str,
    field_name: &str,
) -> Result<std::collections::HashMap<String, Vec<String>>> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| MaccError::Validation(format!("Invalid JSON for {}: {}", field_name, e)))?;
    let obj = value
        .as_object()
        .ok_or_else(|| MaccError::Validation(format!("{} must be a JSON object", field_name)))?;
    let mut out = std::collections::HashMap::new();
    for (k, v) in obj {
        let arr = v.as_array().ok_or_else(|| {
            MaccError::Validation(format!(
                "Value for key '{}' in {} must be an array of strings",
                k, field_name
            ))
        })?;
        let mut tools = Vec::new();
        for tool in arr {
            let value = tool.as_str().ok_or_else(|| {
                MaccError::Validation(format!(
                    "Value for key '{}' in {} must contain strings only",
                    k, field_name
                ))
            })?;
            if !value.is_empty() {
                tools.push(value.to_string());
            }
        }
        out.insert(k.clone(), tools);
    }
    Ok(out)
}

pub(crate) fn stop_coordinator_process_groups(
    repo_root: &std::path::Path,
    coordinator_path: &std::path::Path,
    graceful: bool,
) -> Result<usize> {
    let repo = repo_root
        .canonicalize()
        .unwrap_or_else(|_| repo_root.to_path_buf());
    let mut pids = pgrep_pids(&coordinator_path.to_string_lossy())?;
    if pids.is_empty() {
        pids = pgrep_pids("coordinator.sh")?;
    }

    let mut pgids = std::collections::BTreeSet::new();
    for pid in pids {
        if pid == std::process::id() as i32 {
            continue;
        }
        if !pid_in_repo(pid, &repo) {
            continue;
        }
        if let Some(pgid) = get_pgid(pid)? {
            pgids.insert(pgid);
        }
    }

    for pgid in &pgids {
        signal_process_group(*pgid, "-TERM")?;
    }
    if !pgids.is_empty() {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    if !graceful {
        for _ in 0..20 {
            if pgids.iter().all(|pgid| !pgid_is_alive(*pgid)) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
        for pgid in &pgids {
            if pgid_is_alive(*pgid) {
                signal_process_group(*pgid, "-KILL")?;
            }
        }
    }

    Ok(pgids.len())
}

fn pgrep_pids(pattern: &str) -> Result<Vec<i32>> {
    let output = std::process::Command::new("pgrep")
        .arg("-f")
        .arg(pattern)
        .output()
        .map_err(|e| MaccError::Io {
            path: "pgrep".into(),
            action: "find coordinator processes".into(),
            source: e,
        })?;
    if !output.status.success() {
        return Ok(Vec::new());
    }
    let text = String::from_utf8_lossy(&output.stdout);
    Ok(text
        .lines()
        .filter_map(|line| line.trim().parse::<i32>().ok())
        .collect())
}

fn pid_in_repo(pid: i32, repo_root: &std::path::Path) -> bool {
    let proc_cwd = std::path::PathBuf::from(format!("/proc/{}/cwd", pid));
    let Ok(cwd) = std::fs::read_link(proc_cwd) else {
        return false;
    };
    let cwd = cwd.canonicalize().unwrap_or(cwd);
    cwd.starts_with(repo_root)
}

fn get_pgid(pid: i32) -> Result<Option<i32>> {
    let output = std::process::Command::new("ps")
        .arg("-o")
        .arg("pgid=")
        .arg("-p")
        .arg(pid.to_string())
        .output()
        .map_err(|e| MaccError::Io {
            path: "ps".into(),
            action: "read process group".into(),
            source: e,
        })?;
    if !output.status.success() {
        return Ok(None);
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(value.parse::<i32>().ok())
}

fn signal_process_group(pgid: i32, signal: &str) -> Result<()> {
    let target = format!("-{}", pgid);
    let status = std::process::Command::new("kill")
        .arg(signal)
        .arg(target)
        .status()
        .map_err(|e| MaccError::Io {
            path: "kill".into(),
            action: format!("send {} to process group", signal),
            source: e,
        })?;
    let _ = status;
    Ok(())
}

fn pgid_is_alive(pgid: i32) -> bool {
    let target = format!("-{}", pgid);
    std::process::Command::new("kill")
        .arg("-0")
        .arg(target)
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(test)]
fn apply_coordinator_env(
    command: &mut std::process::Command,
    canonical: &macc_core::config::CanonicalConfig,
    coordinator: Option<&macc_core::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
) {
    for (key, value) in coordinator_env_pairs(canonical, coordinator, env_cfg) {
        command.env(key, value);
    }
}

#[cfg(test)]
fn coordinator_env_pairs(
    canonical: &macc_core::config::CanonicalConfig,
    coordinator: Option<&macc_core::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    out.push((
        "ENABLED_TOOLS_CSV".to_string(),
        canonical.tools.enabled.join(","),
    ));
    out.push((
        "TASK_REGISTRY_FILE".to_string(),
        COORDINATOR_TASK_REGISTRY_REL_PATH.to_string(),
    ));

    if let Some(value) = env_cfg
        .prd
        .clone()
        .or_else(|| coordinator.and_then(|c| c.prd_file.clone()))
    {
        out.push(("PRD_FILE".to_string(), value));
    }
    if let Some(value) = env_cfg
        .coordinator_tool
        .clone()
        .or_else(|| coordinator.and_then(|c| c.coordinator_tool.clone()))
    {
        out.push(("COORDINATOR_TOOL".to_string(), value));
    }
    if let Some(value) = env_cfg
        .reference_branch
        .clone()
        .or_else(|| coordinator.and_then(|c| c.reference_branch.clone()))
    {
        out.push(("DEFAULT_BASE_BRANCH".to_string(), value));
    }
    if let Some(value) = env_cfg.tool_priority.clone().or_else(|| {
        coordinator.and_then(|c| {
            if c.tool_priority.is_empty() {
                None
            } else {
                Some(c.tool_priority.join(","))
            }
        })
    }) {
        out.push(("TOOL_PRIORITY_CSV".to_string(), value));
    }
    if let Some(value) = env_cfg.max_parallel_per_tool_json.clone().or_else(|| {
        coordinator.and_then(|c| {
            if c.max_parallel_per_tool.is_empty() {
                None
            } else {
                serde_json::to_string(&c.max_parallel_per_tool).ok()
            }
        })
    }) {
        out.push(("MAX_PARALLEL_PER_TOOL_JSON".to_string(), value));
    }
    if let Some(value) = env_cfg.tool_specializations_json.clone().or_else(|| {
        coordinator.and_then(|c| {
            if c.tool_specializations.is_empty() {
                None
            } else {
                serde_json::to_string(&c.tool_specializations).ok()
            }
        })
    }) {
        out.push(("TOOL_SPECIALIZATIONS_JSON".to_string(), value));
    }
    if let Some(value) = env_cfg
        .max_dispatch
        .or_else(|| coordinator.and_then(|c| c.max_dispatch))
    {
        out.push(("MAX_DISPATCH".to_string(), value.to_string()));
    }
    if let Some(value) = env_cfg
        .max_parallel
        .or_else(|| coordinator.and_then(|c| c.max_parallel))
    {
        out.push(("MAX_PARALLEL".to_string(), value.to_string()));
    }
    if let Some(value) = env_cfg
        .timeout_seconds
        .or_else(|| coordinator.and_then(|c| c.timeout_seconds))
    {
        out.push(("TIMEOUT_SECONDS".to_string(), value.to_string()));
    }
    if let Some(value) = env_cfg
        .phase_runner_max_attempts
        .or_else(|| coordinator.and_then(|c| c.phase_runner_max_attempts))
    {
        out.push(("PHASE_RUNNER_MAX_ATTEMPTS".to_string(), value.to_string()));
    }
    if let Some(value) = env_cfg
        .stale_claimed_seconds
        .or_else(|| coordinator.and_then(|c| c.stale_claimed_seconds))
    {
        out.push(("STALE_CLAIMED_SECONDS".to_string(), value.to_string()));
    }
    if let Some(value) = env_cfg
        .stale_in_progress_seconds
        .or_else(|| coordinator.and_then(|c| c.stale_in_progress_seconds))
    {
        out.push(("STALE_IN_PROGRESS_SECONDS".to_string(), value.to_string()));
    }
    if let Some(value) = env_cfg
        .stale_changes_requested_seconds
        .or_else(|| coordinator.and_then(|c| c.stale_changes_requested_seconds))
    {
        out.push((
            "STALE_CHANGES_REQUESTED_SECONDS".to_string(),
            value.to_string(),
        ));
    }
    if let Some(value) = env_cfg
        .stale_action
        .clone()
        .or_else(|| coordinator.and_then(|c| c.stale_action.clone()))
    {
        out.push(("STALE_ACTION".to_string(), value));
    }
    if let Some(value) = env_cfg
        .storage_mode
        .clone()
        .or_else(|| coordinator.and_then(|c| c.storage_mode.clone()))
    {
        out.push(("COORDINATOR_STORAGE_MODE".to_string(), value));
    }
    if let Some(value) = env_cfg
        .mirror_json_debounce_ms
        .or_else(|| coordinator.and_then(|c| c.mirror_json_debounce_ms))
    {
        out.push((
            "COORDINATOR_JSON_EXPORT_DEBOUNCE_MS".to_string(),
            value.to_string(),
        ));
    }
    out
}

#[cfg(test)]
pub(crate) fn run_coordinator_action(
    repo_root: &std::path::Path,
    coordinator_path: &std::path::Path,
    action: &str,
    extra_args: &[String],
    canonical: &macc_core::config::CanonicalConfig,
    coordinator: Option<&macc_core::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
) -> Result<()> {
    run_coordinator_action_with_options(
        repo_root,
        coordinator_path,
        action,
        extra_args,
        canonical,
        coordinator,
        env_cfg,
        false,
    )
}

#[cfg(test)]
fn run_coordinator_action_with_options(
    repo_root: &std::path::Path,
    coordinator_path: &std::path::Path,
    action: &str,
    extra_args: &[String],
    canonical: &macc_core::config::CanonicalConfig,
    coordinator: Option<&macc_core::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
    skip_storage_sync: bool,
) -> Result<()> {
    let mut command = std::process::Command::new(coordinator_path);
    command.current_dir(repo_root);
    command.arg(action);
    command.args(extra_args);
    apply_coordinator_env(&mut command, canonical, coordinator, env_cfg);
    if skip_storage_sync {
        command.env("COORDINATOR_SKIP_STORAGE_SYNC", "1");
    }

    let status = command.status().map_err(|e| MaccError::Io {
        path: coordinator_path.to_string_lossy().into(),
        action: format!("run coordinator action '{}'", action),
        source: e,
    })?;
    if !status.success() {
        let hint = coordinator_action_hint(action);
        return Err(MaccError::Validation(format!(
            "Coordinator '{}' failed with status: {}. {}",
            action, status, hint
        )));
    }
    if let Err(err) = macc_core::coordinator::logs::aggregate_performer_logs(repo_root) {
        tracing::warn!("failed to aggregate performer logs: {}", err);
    }
    Ok(())
}

#[cfg(test)]
fn coordinator_action_hint(action: &str) -> &'static str {
    match action {
        "dispatch" => {
            "Run `macc coordinator status` and inspect logs with `macc logs tail --component coordinator`."
        }
        "advance" => {
            "Run `macc coordinator reconcile`, then `macc coordinator unlock --all` if tasks are stuck."
        }
        "reconcile" | "cleanup" => {
            "Run `macc worktree prune` and retry; if locks remain, run `macc coordinator unlock --all`."
        }
        "run" => {
            "Run `macc coordinator status`, then inspect events with `macc logs tail --component coordinator`."
        }
        "retry-phase" => {
            "Verify task/worktree consistency with `macc coordinator status` and inspect errors in `macc logs tail --component coordinator`."
        }
        "resume" => {
            "After fixing merge conflicts manually, run `macc coordinator run` to continue orchestration."
        }
        "cutover-gate" => {
            "Inspect cutover metrics in .macc/log/coordinator/events.jsonl and rerun `macc coordinator cutover-gate`."
        }
        "unlock" => {
            "Inspect lock owners in .macc/automation/task/task_registry.json then retry dispatch."
        }
        "sync" => "Check PRD/registry JSON validity and rerun `macc coordinator sync`.",
        _ => "Inspect logs with `macc logs tail --component coordinator`.",
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RegistryCounts {
    pub(crate) total: usize,
    pub(crate) todo: usize,
    pub(crate) active: usize,
    pub(crate) blocked: usize,
    pub(crate) merged: usize,
}

#[cfg(test)]
pub(crate) fn read_registry_counts(path: &std::path::Path) -> Result<RegistryCounts> {
    let content = std::fs::read_to_string(path).map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "read task registry".into(),
        source: e,
    })?;
    let root: serde_json::Value = serde_json::from_str(&content).map_err(|e| {
        MaccError::Validation(format!(
            "Failed to parse task registry JSON '{}': {}",
            path.display(),
            e
        ))
    })?;
    let tasks = root
        .get("tasks")
        .and_then(|v| v.as_array())
        .ok_or_else(|| MaccError::Validation("Task registry missing 'tasks' array".into()))?;

    let mut counts = RegistryCounts {
        total: tasks.len(),
        todo: 0,
        active: 0,
        blocked: 0,
        merged: 0,
    };

    for task in tasks {
        let state = task
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("todo")
            .to_ascii_lowercase();
        match state.as_str() {
            "todo" => counts.todo += 1,
            "claimed" | "in_progress" | "pr_open" | "changes_requested" | "queued" => {
                counts.active += 1
            }
            "blocked" => counts.blocked += 1,
            "merged" => counts.merged += 1,
            _ => {}
        }
    }
    Ok(counts)
}

#[cfg(test)]
pub(crate) fn run_coordinator_full_cycle(
    repo_root: &std::path::Path,
    canonical: &macc_core::config::CanonicalConfig,
    coordinator: Option<&macc_core::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
) -> Result<()> {
    let registry_path = repo_root.join(COORDINATOR_TASK_REGISTRY_REL_PATH);
    let prd_file = env_cfg
        .prd
        .as_ref()
        .map(std::path::PathBuf::from)
        .or_else(|| {
            coordinator
                .and_then(|c| c.prd_file.clone())
                .map(std::path::PathBuf::from)
        })
        .unwrap_or_else(|| repo_root.join("prd.json"));

    let timeout_seconds = env_cfg
        .timeout_seconds
        .or_else(|| coordinator.and_then(|c| c.timeout_seconds))
        .unwrap_or(3600) as u64;
    let max_cycles = 128usize;
    let mut no_progress_cycles = 0usize;
    let started = std::time::Instant::now();

    for cycle in 1..=max_cycles {
        macc_core::coordinator::control_plane::sync_registry_from_prd_native(
            repo_root, &prd_file, None,
        )?;

        let before = read_registry_counts(&registry_path)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .map_err(|e| MaccError::Validation(format!("Failed to init tokio runtime: {}", e)))?;
        runtime.block_on(async {
            let mut state = CoordinatorRunState::new();
            let _ = macc_core::coordinator::control_plane::dispatch_ready_tasks_native(
                repo_root,
                canonical,
                coordinator,
                env_cfg,
                &prd_file,
                &mut state,
                None,
            )
            .await?;
            let max_attempts = env_cfg
                .phase_runner_max_attempts
                .or_else(|| coordinator.and_then(|c| c.phase_runner_max_attempts))
                .unwrap_or(1)
                .max(1);
            let phase_timeout = env_cfg
                .stale_in_progress_seconds
                .or_else(|| coordinator.and_then(|c| c.stale_in_progress_seconds))
                .unwrap_or(0);
            while !state.active_jobs.is_empty() {
                macc_core::coordinator::control_plane::monitor_active_jobs_native(
                    repo_root,
                    env_cfg,
                    &mut state,
                    max_attempts,
                    phase_timeout,
                    None,
                )
                .await?;
                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
            }
            let advance = macc_core::coordinator::control_plane::advance_tasks_native(
                repo_root,
                env_cfg.coordinator_tool.as_deref(),
                max_attempts,
                &mut state,
                None,
            )
            .await?;
            if let Some((task_id, reason)) = advance.blocked_merge {
                return Err(MaccError::Validation(format!(
                    "Coordinator paused on task {} (integrate). Reason: {}",
                    task_id, reason
                )));
            }
            while !state.active_merge_jobs.is_empty() {
                let _ = macc_core::coordinator::control_plane::monitor_merge_jobs_native(
                    repo_root, &mut state, None,
                )
                .await?;
                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
            }
            Result::<()>::Ok(())
        })?;

        crate::coordinator::state_runtime::reconcile_registry_native(repo_root)?;
        crate::coordinator::state_runtime::cleanup_registry_native(repo_root)?;
        macc_core::coordinator::control_plane::sync_registry_from_prd_native(
            repo_root, &prd_file, None,
        )?;
        let after = read_registry_counts(&registry_path)?;

        println!(
            "Coordinator cycle {}: total={} todo={} active={} blocked={} merged={}",
            cycle, after.total, after.todo, after.active, after.blocked, after.merged
        );

        if after.todo == 0 && after.active == 0 {
            if after.blocked > 0 {
                return Err(MaccError::Validation(format!(
                    "Coordinator run finished with blocked tasks: {} (registry: {})",
                    after.blocked,
                    registry_path.display()
                )));
            }
            println!("Coordinator run complete.");
            return Ok(());
        }

        if after == before {
            no_progress_cycles += 1;
        } else {
            no_progress_cycles = 0;
        }

        if no_progress_cycles >= 2 {
            return Err(MaccError::Validation(format!(
                "Coordinator made no progress for {} cycles (todo={}, active={}, blocked={}). Run `macc coordinator status`, then `macc coordinator unlock --all`, and inspect logs with `macc logs tail --component coordinator`.",
                no_progress_cycles, after.todo, after.active, after.blocked
            )));
        }

        if started.elapsed() > std::time::Duration::from_secs(timeout_seconds) {
            return Err(MaccError::Validation(format!(
                "Coordinator run timed out after {} seconds. Run `macc coordinator status` and `macc logs tail --component coordinator`.",
                timeout_seconds
            )));
        }
    }

    Err(MaccError::Validation(format!(
        "Coordinator run reached max cycles ({}) without converging.",
        max_cycles
    )))
}
