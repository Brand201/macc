use crate::config::CoordinatorConfig;
use crate::{ensure_embedded_automation_scripts, MaccError, ProjectPaths, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CoordinatorProcessHandle(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorProcessPoll {
    Running,
    Exited { success: bool, code: Option<i32> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorStopResult {
    pub targets: usize,
    pub used_group: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorManagedPoll {
    Idle,
    Running {
        action: String,
        elapsed_secs: u64,
    },
    Exited {
        action: String,
        success: bool,
        code: Option<i32>,
        elapsed_secs: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorManagedActionState {
    Idle,
    Running {
        action: String,
        elapsed_secs: u64,
    },
    Succeeded {
        action: String,
        elapsed_secs: u64,
    },
    Failed {
        action: String,
        elapsed_secs: u64,
        reason: String,
        task_id: Option<String>,
        phase: Option<String>,
    },
}

struct ManagedCoordinatorProcess {
    child: Child,
}

struct ManagedCoordinatorAction {
    handle: CoordinatorProcessHandle,
    action: String,
    started_at: Instant,
}

fn process_table() -> &'static Mutex<HashMap<u64, ManagedCoordinatorProcess>> {
    static TABLE: OnceLock<Mutex<HashMap<u64, ManagedCoordinatorProcess>>> = OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn process_id_gen() -> &'static AtomicU64 {
    static ID: OnceLock<AtomicU64> = OnceLock::new();
    ID.get_or_init(|| AtomicU64::new(1))
}

fn managed_actions_by_root() -> &'static Mutex<HashMap<String, ManagedCoordinatorAction>> {
    static TABLE: OnceLock<Mutex<HashMap<String, ManagedCoordinatorAction>>> = OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn root_key(paths: &ProjectPaths) -> String {
    paths.root.to_string_lossy().into_owned()
}

pub fn coordinator_start_managed_action_process(
    paths: &ProjectPaths,
    action: &str,
    args: &[String],
    cfg: Option<&CoordinatorConfig>,
) -> Result<()> {
    let key = root_key(paths);
    {
        let mut table = managed_actions_by_root().lock().map_err(|_| {
            MaccError::Validation("coordinator managed actions table lock poisoned".into())
        })?;
        if let Some(existing) = table.get(&key) {
            match coordinator_poll_action_process(existing.handle)? {
                CoordinatorProcessPoll::Running => {
                    return Err(MaccError::Validation(format!(
                        "coordinator action '{}' is already running for this project",
                        existing.action
                    )));
                }
                CoordinatorProcessPoll::Exited { .. } => {
                    table.remove(&key);
                }
            }
        }
    }

    let handle = coordinator_start_action_process(paths, action, args, cfg)?;
    let mut table = managed_actions_by_root().lock().map_err(|_| {
        MaccError::Validation("coordinator managed actions table lock poisoned".into())
    })?;
    table.insert(
        key,
        ManagedCoordinatorAction {
            handle,
            action: action.to_string(),
            started_at: Instant::now(),
        },
    );
    Ok(())
}

pub fn coordinator_poll_managed_action_process(
    paths: &ProjectPaths,
) -> Result<CoordinatorManagedPoll> {
    let key = root_key(paths);
    let mut table = managed_actions_by_root().lock().map_err(|_| {
        MaccError::Validation("coordinator managed actions table lock poisoned".into())
    })?;
    let Some(entry) = table.get(&key) else {
        return Ok(CoordinatorManagedPoll::Idle);
    };
    let handle = entry.handle;
    let action = entry.action.clone();
    let elapsed_secs = entry.started_at.elapsed().as_secs();
    match coordinator_poll_action_process(handle)? {
        CoordinatorProcessPoll::Running => Ok(CoordinatorManagedPoll::Running {
            action,
            elapsed_secs,
        }),
        CoordinatorProcessPoll::Exited { success, code } => {
            table.remove(&key);
            Ok(CoordinatorManagedPoll::Exited {
                action,
                success,
                code,
                elapsed_secs,
            })
        }
    }
}

pub fn coordinator_poll_managed_action_state(
    paths: &ProjectPaths,
) -> Result<CoordinatorManagedActionState> {
    match coordinator_poll_managed_action_process(paths)? {
        CoordinatorManagedPoll::Idle => Ok(CoordinatorManagedActionState::Idle),
        CoordinatorManagedPoll::Running {
            action,
            elapsed_secs,
        } => Ok(CoordinatorManagedActionState::Running {
            action,
            elapsed_secs,
        }),
        CoordinatorManagedPoll::Exited {
            action,
            success,
            code,
            elapsed_secs,
        } => {
            if success {
                return Ok(CoordinatorManagedActionState::Succeeded {
                    action,
                    elapsed_secs,
                });
            }
            let failure = crate::service::diagnostic::analyze_last_failure(paths)?;
            let reason = failure
                .as_ref()
                .map(|f| f.message.clone())
                .unwrap_or_else(|| {
                    format!(
                        "Coordinator '{}' failed ({})",
                        action,
                        code.map(|v| format!("exit status: {}", v))
                            .unwrap_or_else(|| "unknown exit status".to_string())
                    )
                });
            Ok(CoordinatorManagedActionState::Failed {
                action,
                elapsed_secs,
                reason,
                task_id: failure.as_ref().and_then(|f| f.task_id.clone()),
                phase: failure.as_ref().and_then(|f| f.phase.clone()),
            })
        }
    }
}

pub fn coordinator_stop_managed_action_process(
    paths: &ProjectPaths,
    graceful: bool,
) -> Result<CoordinatorStopResult> {
    let key = root_key(paths);
    let mut table = managed_actions_by_root().lock().map_err(|_| {
        MaccError::Validation("coordinator managed actions table lock poisoned".into())
    })?;
    let Some(entry) = table.remove(&key) else {
        return Ok(CoordinatorStopResult {
            targets: 0,
            used_group: false,
        });
    };
    coordinator_stop_action_process(entry.handle, graceful)
}

pub fn coordinator_start_action_process(
    paths: &ProjectPaths,
    action: &str,
    args: &[String],
    cfg: Option<&CoordinatorConfig>,
) -> Result<CoordinatorProcessHandle> {
    let root = &paths.root;
    let mut cmd = if action == "run" {
        let current_exe = std::env::current_exe().map_err(|e| MaccError::Io {
            path: root.to_string_lossy().into(),
            action: "resolve current executable for coordinator run".into(),
            source: e,
        })?;
        let mut cmd = Command::new(current_exe);
        cmd.current_dir(root)
            .arg("--cwd")
            .arg(root)
            .arg("coordinator")
            .arg("control-plane-run")
            .arg("--no-tui")
            .args(args);
        cmd
    } else {
        ensure_embedded_automation_scripts(paths)?;
        let script = paths.automation_coordinator_path();
        if !script.exists() {
            return Err(MaccError::Validation(format!(
                "coordinator script not found: {}",
                script.display()
            )));
        }
        let mut cmd = Command::new(script);
        cmd.current_dir(root)
            .arg(action)
            .args(args)
            .env("REPO_DIR", root);
        apply_coordinator_env_overrides(&mut cmd, cfg);
        cmd
    };

    cmd.stdout(Stdio::null()).stderr(Stdio::null());
    let child = cmd.spawn().map_err(|e| MaccError::Io {
        path: root.to_string_lossy().into(),
        action: format!("spawn coordinator '{}'", action),
        source: e,
    })?;

    let id = process_id_gen().fetch_add(1, Ordering::Relaxed);
    let handle = CoordinatorProcessHandle(id);
    let mut table = process_table()
        .lock()
        .map_err(|_| MaccError::Validation("coordinator process table lock poisoned".into()))?;
    table.insert(id, ManagedCoordinatorProcess { child });
    Ok(handle)
}

pub fn coordinator_poll_action_process(
    handle: CoordinatorProcessHandle,
) -> Result<CoordinatorProcessPoll> {
    let mut table = process_table()
        .lock()
        .map_err(|_| MaccError::Validation("coordinator process table lock poisoned".into()))?;
    let Some(proc_state) = table.get_mut(&handle.0) else {
        return Ok(CoordinatorProcessPoll::Exited {
            success: false,
            code: None,
        });
    };
    let status = proc_state.child.try_wait().map_err(|e| MaccError::Io {
        path: "<process>".into(),
        action: "poll coordinator process".into(),
        source: e,
    })?;
    match status {
        Some(status) => {
            table.remove(&handle.0);
            Ok(CoordinatorProcessPoll::Exited {
                success: status.success(),
                code: status.code(),
            })
        }
        None => Ok(CoordinatorProcessPoll::Running),
    }
}

pub fn coordinator_stop_action_process(
    handle: CoordinatorProcessHandle,
    _graceful: bool,
) -> Result<CoordinatorStopResult> {
    let mut table = process_table()
        .lock()
        .map_err(|_| MaccError::Validation("coordinator process table lock poisoned".into()))?;
    let Some(mut proc_state) = table.remove(&handle.0) else {
        return Ok(CoordinatorStopResult {
            targets: 0,
            used_group: false,
        });
    };
    let coordinator_pid = proc_state.child.id() as i32;
    let (count, used_group) = stop_coordinator_process_group_or_tree(coordinator_pid)?;
    let _ = proc_state.child.kill();
    let _ = proc_state.child.wait();
    Ok(CoordinatorStopResult {
        targets: count,
        used_group,
    })
}

fn apply_coordinator_env_overrides(cmd: &mut Command, cfg: Option<&CoordinatorConfig>) {
    let Some(cfg) = cfg else {
        return;
    };
    if let Some(v) = &cfg.prd_file {
        if !v.is_empty() {
            cmd.env("PRD_FILE", v);
        }
    }
    if let Some(v) = &cfg.coordinator_tool {
        if !v.is_empty() {
            cmd.env("COORDINATOR_TOOL", v);
        }
    }
    if let Some(v) = &cfg.reference_branch {
        if !v.is_empty() {
            cmd.env("DEFAULT_BASE_BRANCH", v);
        }
    }
    if !cfg.tool_priority.is_empty() {
        cmd.env("TOOL_PRIORITY_CSV", cfg.tool_priority.join(","));
    }
    if !cfg.max_parallel_per_tool.is_empty() {
        if let Ok(json) = serde_json::to_string(&cfg.max_parallel_per_tool) {
            cmd.env("MAX_PARALLEL_PER_TOOL_JSON", json);
        }
    }
    if !cfg.tool_specializations.is_empty() {
        if let Ok(json) = serde_json::to_string(&cfg.tool_specializations) {
            cmd.env("TOOL_SPECIALIZATIONS_JSON", json);
        }
    }
    if let Some(v) = cfg.max_dispatch {
        cmd.env("MAX_DISPATCH", v.to_string());
    }
    if let Some(v) = cfg.max_parallel {
        cmd.env("MAX_PARALLEL", v.to_string());
    }
    if let Some(v) = cfg.timeout_seconds {
        cmd.env("TIMEOUT_SECONDS", v.to_string());
    }
    if let Some(v) = cfg.phase_runner_max_attempts {
        cmd.env("PHASE_RUNNER_MAX_ATTEMPTS", v.to_string());
    }
    if let Some(v) = cfg.log_flush_lines {
        cmd.env("COORDINATOR_LOG_FLUSH_LINES", v.to_string());
    }
    if let Some(v) = cfg.log_flush_ms {
        cmd.env("COORDINATOR_LOG_FLUSH_MS", v.to_string());
    }
    if let Some(v) = cfg.mirror_json_debounce_ms {
        cmd.env("COORDINATOR_JSON_EXPORT_DEBOUNCE_MS", v.to_string());
    }
    if let Some(v) = cfg.stale_claimed_seconds {
        cmd.env("STALE_CLAIMED_SECONDS", v.to_string());
    }
    if let Some(v) = cfg.stale_in_progress_seconds {
        cmd.env("STALE_IN_PROGRESS_SECONDS", v.to_string());
    }
    if let Some(v) = cfg.stale_changes_requested_seconds {
        cmd.env("STALE_CHANGES_REQUESTED_SECONDS", v.to_string());
    }
    if let Some(v) = &cfg.stale_action {
        if !v.is_empty() {
            cmd.env("STALE_ACTION", v);
        }
    }
}

fn stop_coordinator_process_group_or_tree(pid: i32) -> Result<(usize, bool)> {
    let current_pgid = pgid_for_pid(std::process::id() as i32).unwrap_or(-1);
    if let Some(target_pgid) = pgid_for_pid(pid) {
        if target_pgid > 0 && target_pgid != current_pgid {
            let _ = signal_process_group(target_pgid, "-TERM");
            std::thread::sleep(Duration::from_millis(800));
            if pgid_is_alive(target_pgid) {
                let _ = signal_process_group(target_pgid, "-KILL");
            }
            return Ok((1, true));
        }
    }

    let descendants = collect_descendant_pids(pid);
    let mut targets = descendants;
    targets.push(pid);
    targets.sort_unstable();
    targets.dedup();

    let mut signaled = 0usize;
    for target in &targets {
        if signal_pid(*target, "-TERM") {
            signaled += 1;
        }
    }

    for _ in 0..20 {
        if targets.iter().all(|target| !pid_is_alive(*target)) {
            break;
        }
        std::thread::sleep(Duration::from_millis(120));
    }

    for target in &targets {
        if pid_is_alive(*target) {
            let _ = signal_pid(*target, "-KILL");
        }
    }
    Ok((signaled, false))
}

fn collect_descendant_pids(root_pid: i32) -> Vec<i32> {
    let mut stack = vec![root_pid];
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();

    while let Some(pid) = stack.pop() {
        for child in child_pids(pid) {
            if !seen.insert(child) {
                continue;
            }
            out.push(child);
            stack.push(child);
        }
    }
    out
}

fn child_pids(pid: i32) -> Vec<i32> {
    let output = Command::new("pgrep")
        .arg("-P")
        .arg(pid.to_string())
        .output();
    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.trim().parse::<i32>().ok())
        .collect()
}

fn pgid_for_pid(pid: i32) -> Option<i32> {
    let output = Command::new("ps")
        .arg("-o")
        .arg("pgid=")
        .arg("-p")
        .arg(pid.to_string())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<i32>()
        .ok()
}

fn signal_process_group(pgid: i32, signal: &str) -> bool {
    if pgid <= 0 {
        return false;
    }
    Command::new("kill")
        .arg(signal)
        .arg(format!("-{}", pgid))
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn pgid_is_alive(pgid: i32) -> bool {
    if pgid <= 0 {
        return false;
    }
    Command::new("kill")
        .arg("-0")
        .arg(format!("-{}", pgid))
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn signal_pid(pid: i32, signal: &str) -> bool {
    if pid <= 0 {
        return false;
    }
    Command::new("kill")
        .arg(signal)
        .arg(pid.to_string())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn pid_is_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    let proc_cwd = PathBuf::from(format!("/proc/{}/cwd", pid));
    if !proc_cwd.exists() {
        return false;
    }
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}
