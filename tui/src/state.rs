use crate::screen::Screen;
use macc_adapter_shared::fetch::materialize_fetch_units;
use macc_core::catalog::{Agent, McpEntry, Skill};
use macc_core::config::{CanonicalConfig, CoordinatorConfig};
use macc_core::doctor::ToolCheck;
use macc_core::plan::{render_diff, ActionPlan, DiffView, PlannedOp, Scope};
use macc_core::resolve::{resolve, resolve_fetch_units, CliOverrides};
use macc_core::tool::{ActionKind, FieldDefault, FieldKind, ToolDescriptor, ToolField};
use macc_core::{find_project_root, Engine, ProjectPaths};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UiStatusLevel {
    Info,
    Success,
    Warning,
    Error,
}

pub struct UiStatus {
    pub level: UiStatusLevel,
    pub message: String,
    pub expires_at: Option<Instant>,
}

pub struct ApplyContext {
    pub plan: ActionPlan,
    pub operations: Vec<PlannedOp>,
    pub project_ops: usize,
    pub user_ops: usize,
    pub backup_preview: String,
}

impl ApplyContext {
    pub fn needs_user_consent(&self) -> bool {
        self.user_ops > 0
    }
}

pub struct ApplyProgress {
    pub current: usize,
    pub total: usize,
    pub path: Option<String>,
}

pub struct WorktreeStatus {
    pub current: Option<macc_core::WorktreeEntry>,
    pub total: usize,
    pub error: Option<String>,
}

pub struct LogEntry {
    pub path: PathBuf,
    pub relative: String,
}

struct QuietEnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl QuietEnvGuard {
    fn new(key: &'static str, value: &str) -> Self {
        let previous = env::var(key).ok();
        env::set_var(key, value);
        Self { key, previous }
    }
}

impl Drop for QuietEnvGuard {
    fn drop(&mut self) {
        if let Some(prev) = self.previous.take() {
            env::set_var(self.key, prev);
        } else {
            env::remove_var(self.key);
        }
    }
}

pub struct CoordinatorTaskSnapshot {
    pub total: usize,
    pub todo: usize,
    pub active: usize,
    pub blocked: usize,
    pub merged: usize,
}

struct CoordinatorProcess {
    action: String,
    child: Child,
    started_at: Instant,
}

pub struct AppState {
    pub engine: Arc<dyn Engine>,
    pub project_paths: Option<ProjectPaths>,
    pub config: Option<CanonicalConfig>,
    pub working_copy: Option<CanonicalConfig>,
    pub errors: Vec<String>,
    pub notices: Vec<String>,
    pub should_quit: bool,
    pub screen_stack: Vec<Screen>,
    pub selected_tool_index: usize,
    pub tool_field_index: usize,
    pub current_tool_id: Option<String>,
    pub tool_descriptors: Vec<ToolDescriptor>,
    pub tool_field_editing: bool,
    pub tool_field_input: String,
    pub tool_install_confirm_id: Option<String>,
    pub automation_field_index: usize,
    pub automation_field_editing: bool,
    pub automation_field_input: String,
    pub skills: Vec<Skill>,
    pub agents: Vec<Agent>,
    pub skill_selection_index: usize,
    pub agent_selection_index: usize,
    pub skill_target_path: Option<String>,
    pub agent_target_path: Option<String>,
    pub mcp_selection_index: usize,
    pub mcp_entries: Vec<McpEntry>,
    pub log_selection_index: usize,
    pub log_content_scroll: usize,
    pub log_entries: Vec<LogEntry>,
    pub log_view_content: String,
    pub preview_ops: Vec<PlannedOp>,
    pub preview_selection_index: usize,
    pub preview_error: Option<String>,
    pub preview_diff_cache: HashMap<String, DiffView>,
    pub preview_diff_scroll: HashMap<String, usize>,
    pub apply_context: Option<ApplyContext>,
    pub apply_consent_input: String,
    pub apply_user_consent_granted: bool,
    pub apply_feedback: Option<String>,
    pub apply_error: Option<String>,
    pub apply_progress: Option<ApplyProgress>,
    pub help_open: bool,
    pub tool_checks: Vec<ToolCheck>,
    pub last_screen: Option<Screen>,
    pub worktree_status: Option<WorktreeStatus>,
    pub ui_status: Option<UiStatus>,
    pub coordinator_snapshot: Option<CoordinatorTaskSnapshot>,
    pub coordinator_last_refresh: Option<Instant>,
    pub coordinator_running_action: Option<String>,
    pub coordinator_last_result: Option<String>,
    pub search_query: String,
    pub search_editing: bool,
    pub undo_stack: Vec<CanonicalConfig>,
    pub redo_stack: Vec<CanonicalConfig>,
    coordinator_process: Option<CoordinatorProcess>,
}

impl AppState {
    const AUTOMATION_FIELD_COUNT: usize = 15;

    pub fn automation_field_count(&self) -> usize {
        Self::AUTOMATION_FIELD_COUNT
    }

    pub fn new(engine: Arc<dyn Engine>) -> Self {
        let mut state = Self::with_engine(engine);
        state.load_config(None);
        state.refresh_tool_checks();
        state
    }

    pub fn with_engine(engine: Arc<dyn Engine>) -> Self {
        let mut state = Self {
            engine,
            project_paths: None,
            config: None,
            working_copy: None,
            errors: Vec::new(),
            notices: Vec::new(),
            should_quit: false,
            screen_stack: vec![Screen::Home],
            selected_tool_index: 0,
            tool_field_index: 0,
            current_tool_id: None,
            tool_descriptors: Vec::new(),
            tool_field_editing: false,
            tool_field_input: String::new(),
            tool_install_confirm_id: None,
            automation_field_index: 0,
            automation_field_editing: false,
            automation_field_input: String::new(),
            skills: Vec::new(),
            agents: Vec::new(),
            skill_selection_index: 0,
            agent_selection_index: 0,
            skill_target_path: None,
            agent_target_path: None,
            mcp_selection_index: 0,
            mcp_entries: Vec::new(),
            log_selection_index: 0,
            log_content_scroll: 0,
            log_entries: Vec::new(),
            log_view_content: String::new(),
            preview_ops: Vec::new(),
            preview_selection_index: 0,
            preview_error: None,
            preview_diff_cache: HashMap::new(),
            preview_diff_scroll: HashMap::new(),
            apply_context: None,
            apply_consent_input: String::new(),
            apply_user_consent_granted: false,
            apply_feedback: None,
            apply_error: None,
            apply_progress: None,
            help_open: false,
            tool_checks: Vec::new(),
            last_screen: None,
            worktree_status: None,
            ui_status: None,
            coordinator_snapshot: None,
            coordinator_last_refresh: None,
            coordinator_running_action: None,
            coordinator_last_result: None,
            search_query: String::new(),
            search_editing: false,
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
            coordinator_process: None,
        };

        state.refresh_tools();
        state.refresh_tool_checks();
        state.refresh_skills();
        state.refresh_mcp_entries();
        state.refresh_logs();
        state.agents = state.engine.builtin_agents();

        state
    }

    pub fn refresh_tools(&mut self) {
        let paths = self
            .project_paths
            .clone()
            .unwrap_or_else(|| ProjectPaths::from_root("."));
        let (descriptors, diagnostics) = self.engine.list_tools(&paths);
        self.tool_descriptors = descriptors;

        for diag in diagnostics {
            let location = match (diag.line, diag.column) {
                (Some(l), Some(c)) => format!(" at {}:{}", l, c),
                (Some(l), None) => format!(" at line {}", l),
                _ => "".to_string(),
            };
            self.errors.push(format!(
                "Tool Spec Error ({}{}): {}",
                diag.path.display(),
                location,
                diag.error
            ));
        }
    }

    pub fn refresh_skills(&mut self) {
        let mut skills_map: BTreeMap<String, Skill> = self
            .engine
            .builtin_skills()
            .into_iter()
            .map(|skill| (skill.id.clone(), skill))
            .collect();

        if let Some(paths) = &self.project_paths {
            match macc_core::catalog::load_skills_catalog_with_local(paths) {
                Ok(catalog) => {
                    for entry in catalog.entries {
                        skills_map.insert(
                            entry.id.clone(),
                            Skill {
                                id: entry.id,
                                name: entry.name,
                                description: entry.description,
                            },
                        );
                    }
                }
                Err(err) => {
                    self.errors
                        .push(format!("Failed to load skills catalog: {}", err));
                }
            }
        }

        let mut skills: Vec<Skill> = skills_map.into_values().collect();
        skills.sort_by(|a, b| a.id.cmp(&b.id));
        self.skills = skills;
        if self.skill_selection_index >= self.skills.len() {
            self.skill_selection_index = 0;
        }
    }

    pub fn refresh_mcp_entries(&mut self) {
        if let Some(paths) = &self.project_paths {
            match macc_core::catalog::McpCatalog::load(&paths.mcp_catalog_path()) {
                Ok(mut catalog) => {
                    catalog.entries.sort_by(|a, b| a.id.cmp(&b.id));
                    self.mcp_entries = catalog.entries;
                }
                Err(err) => {
                    self.errors
                        .push(format!("Failed to load MCP catalog: {}", err));
                    self.mcp_entries = Vec::new();
                }
            }
        } else {
            self.mcp_entries = Vec::new();
        }

        if self.mcp_selection_index >= self.mcp_entries.len() {
            self.mcp_selection_index = 0;
        }
    }

    pub fn refresh_logs(&mut self) {
        let Some(paths) = &self.project_paths else {
            self.log_entries.clear();
            self.log_view_content.clear();
            self.log_selection_index = 0;
            self.log_content_scroll = 0;
            return;
        };
        let log_root = paths.root.join(".macc/log");
        let mut entries = Vec::new();
        collect_log_files(&log_root, &log_root, &mut entries);
        entries.sort_by(|a, b| b.relative.cmp(&a.relative));
        self.log_entries = entries;
        if self.log_entries.is_empty() {
            self.log_selection_index = 0;
            self.log_content_scroll = 0;
            self.log_view_content = "No log files found in .macc/log/.".to_string();
            return;
        }
        if self.log_selection_index >= self.log_entries.len() {
            self.log_selection_index = 0;
        }
        let filtered = self.filtered_log_indices();
        if let Some(first) = filtered.first() {
            if !filtered.contains(&self.log_selection_index) {
                self.log_selection_index = *first;
            }
        }
        self.log_content_scroll = 0;
        self.load_selected_log_content();
    }

    fn load_selected_log_content(&mut self) {
        let Some(entry) = self.log_entries.get(self.log_selection_index) else {
            self.log_view_content = "No log selected.".to_string();
            return;
        };
        match std::fs::read_to_string(&entry.path) {
            Ok(content) => {
                self.log_view_content = content;
            }
            Err(err) => {
                self.log_view_content = format!(
                    "Failed to read log '{}'.\n\nCause: {}\nSuggested fix: verify file permissions and refresh logs with 'r'.",
                    entry.path.display(),
                    err
                );
            }
        }
    }

    pub fn next_log(&mut self) {
        let visible = self.filtered_log_indices();
        self.log_selection_index = next_visible_index(self.log_selection_index, &visible);
        self.log_content_scroll = 0;
        self.load_selected_log_content();
    }

    pub fn prev_log(&mut self) {
        let visible = self.filtered_log_indices();
        self.log_selection_index = prev_visible_index(self.log_selection_index, &visible);
        self.log_content_scroll = 0;
        self.load_selected_log_content();
    }

    pub fn scroll_log_content(&mut self, delta: isize) {
        let current = self.log_content_scroll as isize;
        let next = (current + delta).max(0) as usize;
        self.log_content_scroll = next;
    }

    pub fn refresh_worktree_status(&mut self) {
        let Some(paths) = &self.project_paths else {
            self.worktree_status = None;
            return;
        };

        match macc_core::list_worktrees(&paths.root) {
            Ok(entries) => {
                let current = macc_core::current_worktree(&paths.root, &entries);
                self.worktree_status = Some(WorktreeStatus {
                    current,
                    total: entries.len(),
                    error: None,
                });
            }
            Err(err) => {
                self.worktree_status = Some(WorktreeStatus {
                    current: None,
                    total: 0,
                    error: Some(err.to_string()),
                });
            }
        }
    }

    fn coordinator_registry_path(&self) -> Option<PathBuf> {
        let paths = self.project_paths.as_ref()?;
        let from_cfg = self
            .working_copy
            .as_ref()
            .and_then(|wc| wc.automation.coordinator.as_ref())
            .and_then(|c| c.task_registry_file.clone())
            .filter(|s| !s.trim().is_empty());
        let path = from_cfg
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("task_registry.json"));
        if path.is_absolute() {
            Some(path)
        } else {
            Some(paths.root.join(path))
        }
    }

    fn read_registry_snapshot(
        &self,
        path: &std::path::Path,
    ) -> Result<CoordinatorTaskSnapshot, String> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read registry '{}': {}", path.display(), e))?;
        let root: Value = serde_json::from_str(&raw)
            .map_err(|e| format!("invalid registry JSON '{}': {}", path.display(), e))?;
        let tasks = root
            .get("tasks")
            .and_then(|v| v.as_array())
            .ok_or_else(|| "registry is missing tasks array".to_string())?;
        let mut snapshot = CoordinatorTaskSnapshot {
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
                "todo" => snapshot.todo += 1,
                "claimed" | "in_progress" | "pr_open" | "changes_requested" | "queued" => {
                    snapshot.active += 1
                }
                "blocked" => snapshot.blocked += 1,
                "merged" => snapshot.merged += 1,
                _ => {}
            }
        }
        Ok(snapshot)
    }

    pub fn refresh_coordinator_snapshot(&mut self) {
        let Some(registry_path) = self.coordinator_registry_path() else {
            return;
        };
        match self.read_registry_snapshot(&registry_path) {
            Ok(snapshot) => {
                self.coordinator_snapshot = Some(snapshot);
                self.coordinator_last_refresh = Some(Instant::now());
            }
            Err(err) => {
                self.coordinator_last_result = Some(format_actionable_error(&err));
            }
        }
    }

    pub fn refresh_tool_checks(&mut self) {
        let paths = self
            .project_paths
            .clone()
            .unwrap_or_else(|| ProjectPaths::from_root("."));
        self.tool_checks = self.engine.doctor(&paths);
    }

    fn apply_coordinator_env_overrides(&self, cmd: &mut Command) {
        let Some(wc) = &self.working_copy else {
            return;
        };
        let Some(cfg) = wc.automation.coordinator.as_ref() else {
            return;
        };

        if let Some(v) = &cfg.prd_file {
            if !v.is_empty() {
                cmd.env("PRD_FILE", v);
            }
        }
        if let Some(v) = &cfg.task_registry_file {
            if !v.is_empty() {
                cmd.env("TASK_REGISTRY_FILE", v);
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

    fn coordinator_script_path(&mut self) -> Result<PathBuf, String> {
        let paths = self
            .project_paths
            .as_ref()
            .ok_or_else(|| "No project loaded.".to_string())?
            .clone();
        macc_core::ensure_embedded_automation_scripts(&paths)
            .map_err(|e| format!("failed to install automation scripts: {}", e))?;
        let script = paths.automation_coordinator_path();
        if !script.exists() {
            return Err(format!(
                "coordinator script not found: {}",
                script.display()
            ));
        }
        Ok(script)
    }

    pub fn start_coordinator_action(&mut self, action: &str) {
        if self.coordinator_process.is_some() {
            self.set_status(
                UiStatusLevel::Warning,
                "Coordinator already running.",
                Some(Duration::from_secs(3)),
            );
            return;
        }
        let Some(paths) = self.project_paths.as_ref() else {
            self.set_status(
                UiStatusLevel::Error,
                "No project loaded.",
                Some(Duration::from_secs(4)),
            );
            return;
        };
        let root = paths.root.clone();
        let script = match self.coordinator_script_path() {
            Ok(path) => path,
            Err(err) => {
                let actionable = format_actionable_error(&err);
                self.coordinator_last_result = Some(actionable.clone());
                self.set_status(
                    UiStatusLevel::Error,
                    actionable,
                    Some(Duration::from_secs(8)),
                );
                return;
            }
        };
        let mut cmd = Command::new(script);
        cmd.current_dir(&root)
            .arg(action)
            .env("REPO_DIR", &root)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        self.apply_coordinator_env_overrides(&mut cmd);
        match cmd.spawn() {
            Ok(child) => {
                self.coordinator_process = Some(CoordinatorProcess {
                    action: action.to_string(),
                    child,
                    started_at: Instant::now(),
                });
                self.coordinator_running_action = Some(action.to_string());
                self.coordinator_last_result = Some(format!("Started '{}'.", action));
                self.refresh_coordinator_snapshot();
                self.set_status(
                    UiStatusLevel::Info,
                    format!("Coordinator '{}' started.", action),
                    Some(Duration::from_secs(3)),
                );
            }
            Err(err) => {
                self.coordinator_last_result = Some(format_actionable_error(&format!(
                    "Failed to start '{}': {}",
                    action, err
                )));
                self.set_status(
                    UiStatusLevel::Error,
                    format!("Failed to start '{}'.", action),
                    Some(Duration::from_secs(8)),
                );
            }
        }
    }

    fn run_coordinator_action_blocking(
        &mut self,
        action: &str,
        args: &[&str],
    ) -> Result<(), String> {
        let Some(paths) = self.project_paths.as_ref() else {
            return Err("No project loaded.".to_string());
        };
        let root = paths.root.clone();
        let script = self.coordinator_script_path()?;
        let mut cmd = Command::new(script);
        cmd.current_dir(&root)
            .arg(action)
            .args(args)
            .env("REPO_DIR", &root)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        self.apply_coordinator_env_overrides(&mut cmd);
        let status = cmd
            .status()
            .map_err(|e| format!("failed to run '{}': {}", action, e))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("'{}' failed with status {}", action, status))
        }
    }

    pub fn stop_coordinator_action(&mut self) {
        match self.run_coordinator_action_blocking("stop", &["--graceful"]) {
            Ok(()) => {
                if let Some(mut proc_state) = self.coordinator_process.take() {
                    let _ = proc_state.child.kill();
                    let _ = proc_state.child.wait();
                }
                self.coordinator_running_action = None;
                self.coordinator_last_result = Some("Coordinator stop completed.".to_string());
                self.refresh_coordinator_snapshot();
                self.set_status(
                    UiStatusLevel::Success,
                    "Coordinator stopped.",
                    Some(Duration::from_secs(4)),
                );
            }
            Err(err) => {
                let actionable = format_actionable_error(&err);
                self.coordinator_last_result = Some(actionable.clone());
                self.set_status(
                    UiStatusLevel::Error,
                    actionable,
                    Some(Duration::from_secs(8)),
                );
            }
        }
    }

    fn ensure_working_copy(&mut self) {
        if self.working_copy.is_none() {
            self.working_copy = Some(CanonicalConfig::default());
        }
    }

    pub fn load_config(&mut self, start_dir: Option<&std::path::Path>) {
        let current_dir = if let Some(d) = start_dir {
            d.to_path_buf()
        } else {
            env::current_dir().unwrap_or_else(|_| ".".into())
        };

        match find_project_root(&current_dir) {
            Ok(paths) => {
                self.project_paths = Some(paths.clone());
                self.refresh_tools();
                self.refresh_skills();
                self.refresh_mcp_entries();
                self.refresh_logs();
                self.refresh_worktree_status();
                self.refresh_coordinator_snapshot();
                match macc_core::config::load_canonical_config(&paths.config_path) {
                    Ok(config) => {
                        self.config = Some(config.clone());
                        self.working_copy = Some(config);
                    }
                    Err(e) => {
                        self.errors.push(format!("Failed to load config: {}", e));
                    }
                }
            }
            Err(_) => {
                self.errors.push(
                    "MACC project not found. Run 'macc init' in your repository root to start."
                        .to_string(),
                );
                self.worktree_status = None;
                self.refresh_logs();
            }
        }
    }
}

impl AppState {
    pub fn current_screen(&self) -> Screen {
        *self.screen_stack.last().unwrap_or(&Screen::Home)
    }

    pub fn interaction_mode_label(&self) -> &'static str {
        let screen = self.current_screen();
        if (screen == Screen::ToolSettings && self.is_tool_field_editing())
            || (screen == Screen::Automation && self.is_automation_field_editing())
        {
            "edit"
        } else if screen == Screen::Apply {
            "confirm"
        } else {
            "browse"
        }
    }

    pub fn breadcrumbs(&self) -> String {
        if self.screen_stack.is_empty() {
            return "Home".to_string();
        }
        self.screen_stack
            .iter()
            .map(|s| s.title())
            .collect::<Vec<_>>()
            .join(" > ")
    }

    pub fn active_tool_label(&self) -> String {
        if let Some(desc) = self.tool_descriptors.get(self.selected_tool_index) {
            return desc.id.to_string();
        }
        self.working_copy
            .as_ref()
            .and_then(|wc| wc.tools.enabled.first().cloned())
            .unwrap_or_else(|| "(none)".to_string())
    }

    pub fn status_badges(&self) -> Vec<String> {
        let mut badges = Vec::new();
        badges.push(if self.project_paths.is_some() {
            "project:ok".to_string()
        } else {
            "project:none".to_string()
        });
        badges.push(format!("tool:{}", self.active_tool_label()));
        badges.push(format!("warnings:{}", self.errors.len()));
        let offline = env::var("MACC_OFFLINE")
            .ok()
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);
        badges.push(if offline {
            "offline:on".to_string()
        } else {
            "offline:off".to_string()
        });
        let cache_ok = self
            .project_paths
            .as_ref()
            .map(|p| p.cache_dir.exists())
            .unwrap_or(false);
        badges.push(if cache_ok {
            "cache:ok".to_string()
        } else {
            "cache:missing".to_string()
        });
        if !self.search_query.is_empty() {
            badges.push(format!("search:'{}'", self.search_query));
        }
        badges
    }

    pub fn set_status(
        &mut self,
        level: UiStatusLevel,
        message: impl Into<String>,
        ttl: Option<Duration>,
    ) {
        let expires_at = ttl.map(|d| Instant::now() + d);
        self.ui_status = Some(UiStatus {
            level,
            message: message.into(),
            expires_at,
        });
    }

    pub fn status_line(&self) -> Option<(UiStatusLevel, String)> {
        let status = self.ui_status.as_ref()?;
        Some((status.level, status.message.clone()))
    }

    pub fn is_coordinator_running(&self) -> bool {
        self.coordinator_process.is_some()
    }

    pub fn coordinator_elapsed_seconds(&self) -> Option<u64> {
        self.coordinator_process
            .as_ref()
            .map(|p| p.started_at.elapsed().as_secs())
    }

    pub fn tick(&mut self) {
        if let Some(status) = &self.ui_status {
            if let Some(expire) = status.expires_at {
                if Instant::now() >= expire {
                    self.ui_status = None;
                }
            }
        }

        let mut finished_message: Option<(UiStatusLevel, String)> = None;
        if let Some(proc_state) = self.coordinator_process.as_mut() {
            match proc_state.child.try_wait() {
                Ok(Some(status)) => {
                    let elapsed = proc_state.started_at.elapsed().as_secs();
                    let action = proc_state.action.clone();
                    if status.success() {
                        finished_message = Some((
                            UiStatusLevel::Success,
                            format!("Coordinator '{}' finished in {}s.", action, elapsed),
                        ));
                    } else {
                        finished_message = Some((
                            UiStatusLevel::Error,
                            format!(
                                "Coordinator '{}' failed in {}s (status {}).",
                                action, elapsed, status
                            ),
                        ));
                    }
                    self.coordinator_last_result = Some(
                        finished_message
                            .as_ref()
                            .map(|(_, msg)| msg.clone())
                            .unwrap_or_default(),
                    );
                    self.coordinator_running_action = None;
                    self.coordinator_process = None;
                    self.refresh_coordinator_snapshot();
                }
                Ok(None) => {
                    let should_refresh = self
                        .coordinator_last_refresh
                        .map(|ts| ts.elapsed() >= Duration::from_secs(1))
                        .unwrap_or(true);
                    if should_refresh {
                        self.refresh_coordinator_snapshot();
                    }
                }
                Err(err) => {
                    let action = proc_state.action.clone();
                    self.coordinator_last_result = Some(format_actionable_error(&format!(
                        "Coordinator '{}' poll error: {}",
                        action, err
                    )));
                    self.coordinator_running_action = None;
                    self.coordinator_process = None;
                    finished_message = Some((
                        UiStatusLevel::Error,
                        "Coordinator polling failed.".to_string(),
                    ));
                }
            }
        }

        if let Some((level, msg)) = finished_message {
            self.set_status(level, msg, Some(Duration::from_secs(5)));
        }
    }

    pub fn take_full_clear(&mut self) -> bool {
        let current = self.current_screen();
        let needs_clear = self.last_screen != Some(current);
        self.last_screen = Some(current);
        needs_clear
    }

    pub fn push_screen(&mut self, screen: Screen) {
        self.screen_stack.push(screen);
        self.search_editing = false;
    }

    pub fn pop_screen(&mut self) {
        if self.screen_stack.len() > 1 {
            self.screen_stack.pop();
        }
        self.search_editing = false;
    }

    pub fn goto_screen(&mut self, screen: Screen) {
        self.screen_stack.clear();
        self.screen_stack.push(screen);
        self.search_editing = false;
    }

    pub fn is_searchable_screen(&self) -> bool {
        matches!(
            self.current_screen(),
            Screen::Tools | Screen::Skills | Screen::Agents | Screen::Mcp | Screen::Logs
        )
    }

    pub fn begin_search(&mut self) {
        if self.is_searchable_screen() {
            self.search_editing = true;
        }
    }

    pub fn clear_search(&mut self) {
        self.search_query.clear();
        self.search_editing = false;
    }

    pub fn append_search_char(&mut self, ch: char) {
        if self.search_editing {
            self.search_query.push(ch);
        }
    }

    pub fn pop_search_char(&mut self) {
        if self.search_editing {
            self.search_query.pop();
        }
    }

    pub fn commit_search(&mut self) {
        self.search_editing = false;
    }

    pub fn cancel_search(&mut self) {
        self.search_editing = false;
    }

    pub fn undo_config_change(&mut self) {
        let Some(previous) = self.undo_stack.pop() else {
            self.set_status(
                UiStatusLevel::Info,
                "Undo stack is empty.",
                Some(Duration::from_secs(2)),
            );
            return;
        };
        if let Some(current) = self.working_copy.take() {
            self.redo_stack.push(current);
        }
        self.working_copy = Some(previous);
        self.set_status(
            UiStatusLevel::Success,
            "Undid last config change.",
            Some(Duration::from_secs(3)),
        );
    }

    pub fn redo_config_change(&mut self) {
        let Some(next) = self.redo_stack.pop() else {
            self.set_status(
                UiStatusLevel::Info,
                "Redo stack is empty.",
                Some(Duration::from_secs(2)),
            );
            return;
        };
        if let Some(current) = self.working_copy.take() {
            self.undo_stack.push(current);
        }
        self.working_copy = Some(next);
        self.set_status(
            UiStatusLevel::Success,
            "Redid config change.",
            Some(Duration::from_secs(3)),
        );
    }

    fn snapshot_before_config_change(&mut self) {
        let Some(cfg) = self.working_copy.as_ref() else {
            return;
        };
        self.undo_stack.push(cfg.clone());
        if self.undo_stack.len() > 128 {
            let _ = self.undo_stack.remove(0);
        }
        self.redo_stack.clear();
    }

    pub fn next_tool(&mut self) {
        let visible = self.filtered_tool_indices();
        self.selected_tool_index = next_visible_index(self.selected_tool_index, &visible);
    }

    pub fn prev_tool(&mut self) {
        let visible = self.filtered_tool_indices();
        self.selected_tool_index = prev_visible_index(self.selected_tool_index, &visible);
    }

    pub fn toggle_selected_tool(&mut self) {
        let selected_index = self
            .filtered_tool_indices()
            .into_iter()
            .find(|idx| *idx == self.selected_tool_index)
            .or_else(|| self.filtered_tool_indices().first().copied())
            .unwrap_or(self.selected_tool_index);
        let tool_id = match self.tool_descriptors.get(selected_index) {
            Some(desc) => desc.id.to_string(),
            None => return,
        };
        self.ensure_working_copy();
        self.snapshot_before_config_change();
        if let Some(ref mut wc) = self.working_copy {
            wc.tools.enabled = toggle_vec_item(wc.tools.enabled.clone(), tool_id);
        }
    }

    pub fn is_tool_install_confirmation_open(&self) -> bool {
        self.tool_install_confirm_id.is_some()
    }

    pub fn begin_tool_install_confirmation(&mut self) {
        let Some(descriptor) = self.tool_descriptors.get(self.selected_tool_index) else {
            return;
        };
        if self.project_paths.is_none() {
            self.errors
                .push("Cannot install tool: no project is loaded.".to_string());
            self.set_status(
                UiStatusLevel::Error,
                "Cannot install tool: no project is loaded.",
                Some(Duration::from_secs(5)),
            );
            return;
        }
        if descriptor.install.is_none() {
            self.errors.push(format!(
                "Tool '{}' does not define install steps.",
                descriptor.id
            ));
            self.set_status(
                UiStatusLevel::Error,
                format!("Tool '{}' has no install steps.", descriptor.id),
                Some(Duration::from_secs(5)),
            );
            return;
        }
        let status = self
            .tool_checks
            .iter()
            .find(|tc| tc.tool_id.as_deref() == Some(descriptor.id.as_str()))
            .map(|tc| tc.status.clone())
            .unwrap_or(macc_core::doctor::ToolStatus::Missing);
        if matches!(status, macc_core::doctor::ToolStatus::Installed) {
            self.notices
                .push(format!("Tool '{}' is already installed.", descriptor.id));
            self.set_status(
                UiStatusLevel::Info,
                format!("Tool '{}' is already installed.", descriptor.id),
                Some(Duration::from_secs(4)),
            );
            return;
        }
        self.tool_install_confirm_id = Some(descriptor.id.clone());
    }

    pub fn cancel_tool_install_confirmation(&mut self) {
        self.tool_install_confirm_id = None;
    }

    pub fn confirm_tool_install(&mut self) {
        let Some(tool_id) = self.tool_install_confirm_id.clone() else {
            return;
        };
        let Some(paths) = self.project_paths.clone() else {
            self.errors
                .push("Cannot install tool: no project is loaded.".to_string());
            return;
        };

        let exe = match env::current_exe() {
            Ok(path) => path,
            Err(e) => {
                self.errors
                    .push(format!("Failed to resolve executable path: {}", e));
                return;
            }
        };

        let status = std::process::Command::new(exe)
            .arg("--cwd")
            .arg(&paths.root)
            .arg("tool")
            .arg("install")
            .arg(&tool_id)
            .arg("--yes")
            .status();

        self.tool_install_confirm_id = None;

        match status {
            Ok(status) if status.success() => {
                self.notices
                    .push(format!("Tool '{}' installation completed.", tool_id));
                self.set_status(
                    UiStatusLevel::Success,
                    format!("Installed tool '{}'.", tool_id),
                    Some(Duration::from_secs(4)),
                );
                self.refresh_tool_checks();
            }
            Ok(status) => {
                self.errors.push(format!(
                    "Tool '{}' installation failed with status {}.",
                    tool_id, status
                ));
                self.set_status(
                    UiStatusLevel::Error,
                    format!("Tool '{}' install failed ({})", tool_id, status),
                    Some(Duration::from_secs(6)),
                );
                self.refresh_tool_checks();
            }
            Err(e) => {
                self.errors.push(format!(
                    "Failed to run installer for tool '{}': {}",
                    tool_id, e
                ));
                self.set_status(
                    UiStatusLevel::Error,
                    format!("Failed to run installer for '{}'.", tool_id),
                    Some(Duration::from_secs(6)),
                );
            }
        }
    }

    pub fn next_tool_field(&mut self) {
        let len = self
            .current_tool_descriptor()
            .map(|d| d.fields.len())
            .unwrap_or(1)
            .max(1);
        self.tool_field_index = next_index(self.tool_field_index, len);
    }

    pub fn prev_tool_field(&mut self) {
        let len = self
            .current_tool_descriptor()
            .map(|d| d.fields.len())
            .unwrap_or(1)
            .max(1);
        self.tool_field_index = prev_index(self.tool_field_index, len);
    }

    pub fn toggle_tool_field(&mut self) {
        let Some(field) = self.current_tool_field().cloned() else {
            return;
        };

        self.ensure_working_copy();

        match field.kind {
            FieldKind::Bool => {
                let current = self
                    .read_bool_at(&field.path)
                    .or(match &field.default {
                        Some(FieldDefault::Bool(value)) => Some(*value),
                        _ => None,
                    })
                    .unwrap_or(false);
                let _ = self.set_value_at(&field.path, Value::Bool(!current));
            }
            FieldKind::Enum(ref options) => {
                let current = self
                    .read_string_at(&field.path)
                    .or_else(|| match &field.default {
                        Some(FieldDefault::Enum(value)) => Some(value.clone()),
                        _ => None,
                    });
                let opts_refs: Vec<&str> = options.iter().map(|s| s.as_str()).collect();
                let next = cycle_value(&opts_refs, current.as_deref().unwrap_or(&options[0]));
                let _ = self.set_value_at(&field.path, Value::String(next.to_string()));
            }
            FieldKind::Text | FieldKind::Number | FieldKind::Array => {
                self.begin_tool_field_edit();
            }
            FieldKind::Action(ref action) => {
                self.handle_action(action);
            }
        }
    }

    pub fn next_automation_field(&mut self) {
        self.automation_field_index =
            next_index(self.automation_field_index, Self::AUTOMATION_FIELD_COUNT);
    }

    pub fn prev_automation_field(&mut self) {
        self.automation_field_index =
            prev_index(self.automation_field_index, Self::AUTOMATION_FIELD_COUNT);
    }

    pub fn is_automation_field_editing(&self) -> bool {
        self.automation_field_editing
    }

    pub fn automation_field_label(&self, index: usize) -> &'static str {
        match index {
            0 => "Coordinator Tool",
            1 => "Reference Branch",
            2 => "PRD File",
            3 => "Task Registry File",
            4 => "Tool Priority (CSV)",
            5 => "Max Parallel Per Tool (JSON)",
            6 => "Tool Specializations (JSON)",
            7 => "Max Dispatch",
            8 => "Max Parallel",
            9 => "Timeout Seconds",
            10 => "Phase Runner Max Attempts",
            11 => "Stale Claimed Seconds",
            12 => "Stale In Progress Seconds",
            13 => "Stale Changes Requested Seconds",
            14 => "Stale Action",
            _ => "",
        }
    }

    pub fn automation_field_help(&self, index: usize) -> &'static str {
        match index {
            0 => "Fixed tool for coordinator phase hooks (review/fix/integrate). Empty means task/default tool.",
            1 => "Default git branch used when task.base_branch is not set (default: main).",
            2 => "Path to PRD JSON used by coordinator.sh (default: prd.json).",
            3 => "Path to task registry JSON (default: task_registry.json).",
            4 => "Tool priority order as comma-separated values, e.g. tool-a,tool-b,tool-c.",
            5 => "Per-tool concurrency caps as JSON object, e.g. {\"tool-a\":3,\"tool-b\":2}.",
            6 => "Category routing as JSON object, e.g. {\"frontend\":[\"tool-b\",\"tool-c\"]}.",
            7 => "Total tasks to dispatch per run, 0 means no cap.",
            8 => "Maximum concurrent performer runs.",
            9 => "Lock wait timeout in seconds, 0 disables timeout.",
            10 => "Max attempts for phase runner fallback.",
            11 => "Auto-stale timeout for claimed tasks in seconds, 0 disables.",
            12 => "Auto-stale timeout for in_progress tasks in seconds, 0 disables.",
            13 => "Auto-stale timeout for changes_requested tasks in seconds, 0 disables.",
            14 => "Action for stale tasks: abandon, todo, blocked.",
            _ => "",
        }
    }

    pub fn automation_field_display_value(&self, index: usize) -> String {
        let coordinator = self
            .working_copy
            .as_ref()
            .and_then(|wc| wc.automation.coordinator.as_ref());
        match index {
            0 => coordinator
                .and_then(|c| c.coordinator_tool.clone())
                .unwrap_or_default(),
            1 => coordinator
                .and_then(|c| c.reference_branch.clone())
                .unwrap_or_else(|| "main".to_string()),
            2 => coordinator
                .and_then(|c| c.prd_file.clone())
                .unwrap_or_else(|| "prd.json".to_string()),
            3 => coordinator
                .and_then(|c| c.task_registry_file.clone())
                .unwrap_or_else(|| "task_registry.json".to_string()),
            4 => coordinator
                .map(|c| c.tool_priority.join(", "))
                .unwrap_or_default(),
            5 => coordinator
                .map(|c| {
                    serde_json::to_string(&c.max_parallel_per_tool)
                        .unwrap_or_else(|_| "{}".to_string())
                })
                .unwrap_or_else(|| "{}".to_string()),
            6 => coordinator
                .map(|c| {
                    serde_json::to_string(&c.tool_specializations)
                        .unwrap_or_else(|_| "{}".to_string())
                })
                .unwrap_or_else(|| "{}".to_string()),
            7 => coordinator
                .and_then(|c| c.max_dispatch)
                .unwrap_or(0)
                .to_string(),
            8 => coordinator
                .and_then(|c| c.max_parallel)
                .unwrap_or(1)
                .to_string(),
            9 => coordinator
                .and_then(|c| c.timeout_seconds)
                .unwrap_or(0)
                .to_string(),
            10 => coordinator
                .and_then(|c| c.phase_runner_max_attempts)
                .unwrap_or(1)
                .to_string(),
            11 => coordinator
                .and_then(|c| c.stale_claimed_seconds)
                .unwrap_or(0)
                .to_string(),
            12 => coordinator
                .and_then(|c| c.stale_in_progress_seconds)
                .unwrap_or(0)
                .to_string(),
            13 => coordinator
                .and_then(|c| c.stale_changes_requested_seconds)
                .unwrap_or(0)
                .to_string(),
            14 => coordinator
                .and_then(|c| c.stale_action.clone())
                .unwrap_or_else(|| "abandon".to_string()),
            _ => String::new(),
        }
    }

    pub fn begin_automation_field_edit(&mut self) {
        self.automation_field_input =
            self.automation_field_display_value(self.automation_field_index);
        self.automation_field_editing = true;
    }

    pub fn append_automation_field_char(&mut self, ch: char) {
        if self.automation_field_editing {
            self.automation_field_input.push(ch);
        }
    }

    pub fn pop_automation_field_char(&mut self) {
        if self.automation_field_editing {
            self.automation_field_input.pop();
        }
    }

    pub fn cancel_automation_field_edit(&mut self) {
        self.automation_field_editing = false;
    }

    pub fn toggle_automation_field(&mut self) {
        if self.automation_field_index == 14 {
            let current = self.automation_field_display_value(14);
            let next = match current.as_str() {
                "abandon" => "todo",
                "todo" => "blocked",
                _ => "abandon",
            };
            self.set_automation_field_string(14, next.to_string());
            return;
        }
        self.begin_automation_field_edit();
    }

    pub fn commit_automation_field_edit(&mut self) {
        if !self.automation_field_editing {
            return;
        }
        let idx = self.automation_field_index;
        let input = self.automation_field_input.trim().to_string();
        let result = match idx {
            0..=3 => {
                if input.is_empty() {
                    Err("Value cannot be empty.".to_string())
                } else {
                    self.set_automation_field_string(idx, input);
                    Ok(())
                }
            }
            4 => {
                self.set_automation_field_tool_priority(input);
                Ok(())
            }
            5 => self.set_automation_field_tool_caps(input),
            6 => self.set_automation_field_tool_specializations(input),
            7..=13 => match input.parse::<usize>() {
                Ok(value) => {
                    self.set_automation_field_usize(idx, value);
                    Ok(())
                }
                Err(_) => Err("Invalid integer value.".to_string()),
            },
            14 => {
                let value = input.to_lowercase();
                if !matches!(value.as_str(), "abandon" | "todo" | "blocked") {
                    Err("stale_action must be one of: abandon, todo, blocked.".to_string())
                } else {
                    self.set_automation_field_string(14, value);
                    Ok(())
                }
            }
            _ => Ok(()),
        };

        if let Err(err) = result {
            self.errors.push(err);
            self.set_status(
                UiStatusLevel::Error,
                "Invalid field value.",
                Some(Duration::from_secs(4)),
            );
            return;
        }
        self.automation_field_editing = false;
        self.set_status(
            UiStatusLevel::Success,
            "Automation value updated.",
            Some(Duration::from_secs(3)),
        );
    }

    fn coordinator_config_mut(&mut self) -> Option<&mut CoordinatorConfig> {
        self.ensure_working_copy();
        let wc = self.working_copy.as_mut()?;
        if wc.automation.coordinator.is_none() {
            wc.automation.coordinator = Some(CoordinatorConfig::default());
        }
        wc.automation.coordinator.as_mut()
    }

    fn set_automation_field_string(&mut self, idx: usize, value: String) {
        self.snapshot_before_config_change();
        if let Some(coordinator) = self.coordinator_config_mut() {
            match idx {
                0 => coordinator.coordinator_tool = Some(value),
                1 => coordinator.reference_branch = Some(value),
                2 => coordinator.prd_file = Some(value),
                3 => coordinator.task_registry_file = Some(value),
                14 => coordinator.stale_action = Some(value),
                _ => {}
            }
        }
    }

    fn set_automation_field_usize(&mut self, idx: usize, value: usize) {
        self.snapshot_before_config_change();
        if let Some(coordinator) = self.coordinator_config_mut() {
            match idx {
                7 => coordinator.max_dispatch = Some(value),
                8 => coordinator.max_parallel = Some(value),
                9 => coordinator.timeout_seconds = Some(value),
                10 => coordinator.phase_runner_max_attempts = Some(value),
                11 => coordinator.stale_claimed_seconds = Some(value),
                12 => coordinator.stale_in_progress_seconds = Some(value),
                13 => coordinator.stale_changes_requested_seconds = Some(value),
                _ => {}
            }
        }
    }

    fn set_automation_field_tool_priority(&mut self, value: String) {
        let parsed = parse_csv_list(&value);
        self.snapshot_before_config_change();
        if let Some(coordinator) = self.coordinator_config_mut() {
            coordinator.tool_priority = parsed;
        }
    }

    fn set_automation_field_tool_caps(&mut self, value: String) -> Result<(), String> {
        let parsed: BTreeMap<String, usize> =
            serde_json::from_str(&value).map_err(|e| format!("Invalid tool caps JSON: {}", e))?;
        self.snapshot_before_config_change();
        if let Some(coordinator) = self.coordinator_config_mut() {
            coordinator.max_parallel_per_tool = parsed;
        }
        Ok(())
    }

    fn set_automation_field_tool_specializations(&mut self, value: String) -> Result<(), String> {
        let parsed: BTreeMap<String, Vec<String>> = serde_json::from_str(&value)
            .map_err(|e| format!("Invalid tool specializations JSON: {}", e))?;
        self.snapshot_before_config_change();
        if let Some(coordinator) = self.coordinator_config_mut() {
            coordinator.tool_specializations = parsed;
        }
        Ok(())
    }

    pub fn is_tool_field_editing(&self) -> bool {
        self.tool_field_editing
    }

    pub fn begin_tool_field_edit(&mut self) {
        let Some(field) = self.current_tool_field() else {
            return;
        };
        match field.kind {
            FieldKind::Text | FieldKind::Number | FieldKind::Array => {
                self.tool_field_input = self.tool_field_display_value(field);
                self.tool_field_editing = true;
            }
            _ => {}
        }
    }

    pub fn append_tool_field_char(&mut self, ch: char) {
        if self.tool_field_editing {
            self.tool_field_input.push(ch);
        }
    }

    pub fn pop_tool_field_char(&mut self) {
        if self.tool_field_editing {
            self.tool_field_input.pop();
        }
    }

    pub fn cancel_tool_field_edit(&mut self) {
        self.tool_field_editing = false;
    }

    pub fn commit_tool_field_edit(&mut self) {
        if !self.tool_field_editing {
            return;
        }
        let Some(field) = self.current_tool_field().cloned() else {
            self.tool_field_editing = false;
            return;
        };

        self.ensure_working_copy();
        let input = self.tool_field_input.trim().to_string();

        let result = match field.kind {
            FieldKind::Text => {
                let _ = self.set_value_at(&field.path, Value::String(input));
                Ok(())
            }
            FieldKind::Number => {
                if input.is_empty() {
                    Err("Number is required.".to_string())
                } else {
                    match input.parse::<f64>() {
                        Ok(value) => match serde_json::Number::from_f64(value) {
                            Some(num) => {
                                let _ = self.set_value_at(&field.path, Value::Number(num));
                                Ok(())
                            }
                            None => Err("Number is out of range.".to_string()),
                        },
                        Err(_) => Err("Invalid number.".to_string()),
                    }
                }
            }
            FieldKind::Array => {
                let items = parse_csv_list(&input);
                let values = items.into_iter().map(Value::String).collect();
                let _ = self.set_value_at(&field.path, Value::Array(values));
                Ok(())
            }
            _ => Ok(()),
        };

        if let Err(err) = result {
            self.errors.push(err);
            self.set_status(
                UiStatusLevel::Error,
                "Invalid field value.",
                Some(Duration::from_secs(4)),
            );
            return;
        }

        self.tool_field_editing = false;
        self.set_status(
            UiStatusLevel::Success,
            "Tool field updated.",
            Some(Duration::from_secs(3)),
        );
    }

    fn handle_action(&mut self, action: &ActionKind) {
        match action {
            ActionKind::OpenSkills { target_pointer } => {
                self.skill_target_path = Some(target_pointer.to_string());
                self.skill_selection_index = 0;
                self.push_screen(Screen::Skills);
            }
            ActionKind::OpenAgents { target_pointer } => {
                self.agent_target_path = Some(target_pointer.to_string());
                self.agent_selection_index = 0;
                self.push_screen(Screen::Agents);
            }
            ActionKind::OpenMcp { .. } => {
                self.goto_screen(Screen::Mcp);
            }
            ActionKind::Custom { .. } => {
                // TODO: handle custom actions
            }
        }
    }

    pub fn current_tool_descriptor(&self) -> Option<&ToolDescriptor> {
        let id = self.current_tool_id.as_deref()?;
        self.tool_descriptors.iter().find(|d| d.id == id)
    }

    pub fn current_tool_field(&self) -> Option<&ToolField> {
        self.current_tool_descriptor()
            .and_then(|d| d.fields.get(self.tool_field_index))
    }

    pub fn tool_field_display_value(&self, field: &ToolField) -> String {
        match field.kind {
            FieldKind::Bool => {
                let current = self
                    .read_bool_at(&field.path)
                    .or(match &field.default {
                        Some(FieldDefault::Bool(value)) => Some(*value),
                        _ => None,
                    })
                    .unwrap_or(false);
                if current {
                    "on".to_string()
                } else {
                    "off".to_string()
                }
            }
            FieldKind::Enum(ref options) => self
                .read_string_at(&field.path)
                .or(match &field.default {
                    Some(FieldDefault::Enum(value)) => Some(value.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| options[0].to_string()),
            FieldKind::Text => self
                .read_string_at(&field.path)
                .or(match &field.default {
                    Some(FieldDefault::Text(value)) => Some(value.clone()),
                    _ => None,
                })
                .unwrap_or_default(),
            FieldKind::Number => self
                .read_number_at(&field.path)
                .or(match &field.default {
                    Some(FieldDefault::Number(value)) => Some(*value),
                    _ => None,
                })
                .map(format_number)
                .unwrap_or_default(),
            FieldKind::Array => self
                .read_array_at(&field.path)
                .or(match &field.default {
                    Some(FieldDefault::Array(value)) => Some(value.clone()),
                    _ => None,
                })
                .map(|items| items.join(", "))
                .unwrap_or_default(),
            FieldKind::Action(_) => "open...".to_string(),
        }
    }

    pub fn selected_skills(&self) -> Vec<String> {
        let Some(path) = self.skill_target_path.as_deref() else {
            return Vec::new();
        };
        self.read_string_list_at(path)
    }

    pub fn selected_agents(&self) -> Vec<String> {
        let Some(path) = self.agent_target_path.as_deref() else {
            return Vec::new();
        };
        self.read_string_list_at(path)
    }

    pub fn filtered_tool_indices(&self) -> Vec<usize> {
        self.tool_descriptors
            .iter()
            .enumerate()
            .filter_map(|(i, t)| {
                if matches_search(&self.search_query, &[&t.id, &t.title, &t.description]) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn filtered_skill_indices(&self) -> Vec<usize> {
        self.skills
            .iter()
            .enumerate()
            .filter_map(|(i, s)| {
                if matches_search(&self.search_query, &[&s.id, &s.name, &s.description]) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn filtered_agent_indices(&self) -> Vec<usize> {
        self.agents
            .iter()
            .enumerate()
            .filter_map(|(i, a)| {
                if matches_search(&self.search_query, &[&a.id, &a.name, &a.description]) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn filtered_mcp_indices(&self) -> Vec<usize> {
        self.mcp_entries
            .iter()
            .enumerate()
            .filter_map(|(i, m)| {
                if matches_search(&self.search_query, &[&m.id, &m.name, &m.description]) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn filtered_log_indices(&self) -> Vec<usize> {
        self.log_entries
            .iter()
            .enumerate()
            .filter_map(|(i, e)| {
                if matches_search(&self.search_query, &[&e.relative]) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }

    fn read_value_at(&self, pointer: &str) -> Option<Value> {
        if pointer.is_empty() {
            return None;
        }
        let wc = self.working_copy.as_ref()?;
        let value = serde_json::to_value(wc).ok()?;
        value.pointer(pointer).cloned()
    }

    fn read_string_at(&self, pointer: &str) -> Option<String> {
        self.read_value_at(pointer)
            .and_then(|v| v.as_str().map(|s| s.to_string()))
    }

    fn read_bool_at(&self, pointer: &str) -> Option<bool> {
        self.read_value_at(pointer).and_then(|v| v.as_bool())
    }

    fn read_number_at(&self, pointer: &str) -> Option<f64> {
        let value = self.read_value_at(pointer)?;
        if let Some(num) = value.as_f64() {
            return Some(num);
        }
        value
            .as_str()
            .and_then(|text| f64::from_str(text.trim()).ok())
    }

    fn read_array_at(&self, pointer: &str) -> Option<Vec<String>> {
        let value = self.read_value_at(pointer)?;
        if let Some(arr) = value.as_array() {
            let mut items = Vec::new();
            for entry in arr {
                if let Some(text) = entry.as_str() {
                    items.push(text.to_string());
                } else {
                    items.push(entry.to_string());
                }
            }
            return Some(items);
        }
        value.as_str().map(parse_csv_list)
    }

    fn read_string_list_at(&self, pointer: &str) -> Vec<String> {
        match self.read_value_at(pointer) {
            Some(Value::Array(values)) => values
                .into_iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            _ => Vec::new(),
        }
    }

    fn set_string_list_at(&mut self, pointer: &str, values: Vec<String>) {
        let array = values.into_iter().map(Value::String).collect();
        let _ = self.set_value_at(pointer, Value::Array(array));
    }

    fn set_value_at(&mut self, pointer: &str, new_value: Value) -> Result<(), String> {
        if pointer.is_empty() {
            return Ok(());
        }
        self.snapshot_before_config_change();
        let wc = self
            .working_copy
            .as_ref()
            .ok_or_else(|| "No working config loaded.".to_string())?;
        let mut value = serde_json::to_value(wc).map_err(|e| e.to_string())?;
        set_json_pointer(&mut value, pointer, new_value)?;
        let updated: CanonicalConfig = serde_json::from_value(value).map_err(|e| e.to_string())?;
        self.working_copy = Some(updated);
        Ok(())
    }

    pub fn next_mcp(&mut self) {
        let visible = self.filtered_mcp_indices();
        self.mcp_selection_index = next_visible_index(self.mcp_selection_index, &visible);
    }

    pub fn prev_mcp(&mut self) {
        let visible = self.filtered_mcp_indices();
        self.mcp_selection_index = prev_visible_index(self.mcp_selection_index, &visible);
    }

    pub fn toggle_mcp(&mut self) {
        let selected_index = self
            .filtered_mcp_indices()
            .into_iter()
            .find(|idx| *idx == self.mcp_selection_index)
            .or_else(|| self.filtered_mcp_indices().first().copied())
            .unwrap_or(self.mcp_selection_index);
        if self.mcp_entries.is_empty() {
            return;
        }
        let template_id = self.mcp_entries[selected_index].id.to_string();
        self.ensure_working_copy();
        self.snapshot_before_config_change();
        if let Some(ref mut wc) = self.working_copy {
            let selections = wc
                .selections
                .get_or_insert_with(macc_core::config::SelectionsConfig::default);
            selections.mcp = toggle_vec_item(selections.mcp.clone(), template_id);
        }
    }

    pub fn select_all_mcp(&mut self) {
        self.ensure_working_copy();
        self.snapshot_before_config_change();
        if let Some(ref mut wc) = self.working_copy {
            let selections = wc
                .selections
                .get_or_insert_with(macc_core::config::SelectionsConfig::default);
            selections.mcp = self.mcp_entries.iter().map(|t| t.id.to_string()).collect();
            selections.mcp.sort();
        }
    }

    pub fn select_no_mcp(&mut self) {
        self.ensure_working_copy();
        self.snapshot_before_config_change();
        if let Some(ref mut wc) = self.working_copy {
            if let Some(ref mut selections) = wc.selections {
                selections.mcp.clear();
            }
        }
    }

    pub fn next_skill(&mut self) {
        let visible = self.filtered_skill_indices();
        self.skill_selection_index = next_visible_index(self.skill_selection_index, &visible);
    }

    pub fn prev_skill(&mut self) {
        let visible = self.filtered_skill_indices();
        self.skill_selection_index = prev_visible_index(self.skill_selection_index, &visible);
    }

    pub fn toggle_skill(&mut self) {
        let Some(path) = self.skill_target_path.clone() else {
            return;
        };
        let selected_index = self
            .filtered_skill_indices()
            .into_iter()
            .find(|idx| *idx == self.skill_selection_index)
            .or_else(|| self.filtered_skill_indices().first().copied())
            .unwrap_or(self.skill_selection_index);
        self.ensure_working_copy();
        let skill_id = self.skills[selected_index].id.to_string();
        let mut skills = self.read_string_list_at(&path);
        skills = toggle_vec_item(skills, skill_id);
        self.set_string_list_at(&path, skills);
    }

    pub fn select_all_skills(&mut self) {
        let Some(path) = self.skill_target_path.clone() else {
            return;
        };
        self.ensure_working_copy();
        let mut skills: Vec<String> = self.skills.iter().map(|s| s.id.to_string()).collect();
        skills.sort();
        skills.dedup();
        self.set_string_list_at(&path, skills);
    }

    pub fn select_no_skills(&mut self) {
        let Some(path) = self.skill_target_path.clone() else {
            return;
        };
        self.ensure_working_copy();
        self.set_string_list_at(&path, Vec::new());
    }

    pub fn next_agent(&mut self) {
        let visible = self.filtered_agent_indices();
        self.agent_selection_index = next_visible_index(self.agent_selection_index, &visible);
    }

    pub fn prev_agent(&mut self) {
        let visible = self.filtered_agent_indices();
        self.agent_selection_index = prev_visible_index(self.agent_selection_index, &visible);
    }

    pub fn toggle_agent(&mut self) {
        let Some(path) = self.agent_target_path.clone() else {
            return;
        };
        let selected_index = self
            .filtered_agent_indices()
            .into_iter()
            .find(|idx| *idx == self.agent_selection_index)
            .or_else(|| self.filtered_agent_indices().first().copied())
            .unwrap_or(self.agent_selection_index);
        self.ensure_working_copy();
        let agent_id = self.agents[selected_index].id.to_string();
        let mut agents = self.read_string_list_at(&path);
        agents = toggle_vec_item(agents, agent_id);
        self.set_string_list_at(&path, agents);
    }

    pub fn select_all_agents(&mut self) {
        let Some(path) = self.agent_target_path.clone() else {
            return;
        };
        self.ensure_working_copy();
        let mut agents: Vec<String> = self.agents.iter().map(|a| a.id.to_string()).collect();
        agents.sort();
        agents.dedup();
        self.set_string_list_at(&path, agents);
    }

    pub fn select_no_agents(&mut self) {
        let Some(path) = self.agent_target_path.clone() else {
            return;
        };
        self.ensure_working_copy();
        self.set_string_list_at(&path, Vec::new());
    }

    pub fn navigate_next(&mut self) {
        match self.current_screen() {
            Screen::Tools => self.next_tool(),
            Screen::Automation => self.next_automation_field(),
            Screen::Logs => self.next_log(),
            Screen::Skills => self.next_skill(),
            Screen::Agents => self.next_agent(),
            Screen::ToolSettings => self.next_tool_field(),
            Screen::Preview => self.next_preview_op(),
            Screen::Mcp => self.next_mcp(),
            _ => {}
        }
    }

    pub fn navigate_prev(&mut self) {
        match self.current_screen() {
            Screen::Tools => self.prev_tool(),
            Screen::Automation => self.prev_automation_field(),
            Screen::Logs => self.prev_log(),
            Screen::Skills => self.prev_skill(),
            Screen::Agents => self.prev_agent(),
            Screen::ToolSettings => self.prev_tool_field(),
            Screen::Preview => self.prev_preview_op(),
            Screen::Mcp => self.prev_mcp(),
            _ => {}
        }
    }

    pub fn navigate_toggle(&mut self) {
        match self.current_screen() {
            Screen::Tools => self.toggle_selected_tool(),
            Screen::Automation => self.toggle_automation_field(),
            Screen::Skills => self.toggle_skill(),
            Screen::Agents => self.toggle_agent(),
            Screen::ToolSettings => self.toggle_tool_field(),
            Screen::Mcp => self.toggle_mcp(),
            _ => {}
        }
    }

    pub fn navigate_enter(&mut self) {
        match self.current_screen() {
            Screen::Tools => {
                let selected_index = self
                    .filtered_tool_indices()
                    .into_iter()
                    .find(|idx| *idx == self.selected_tool_index)
                    .or_else(|| self.filtered_tool_indices().first().copied())
                    .unwrap_or(self.selected_tool_index);
                let tool_id = match self.tool_descriptors.get(selected_index) {
                    Some(desc) => desc.id.clone(),
                    None => return,
                };
                let is_enabled = self
                    .working_copy
                    .as_ref()
                    .map(|c| c.tools.enabled.contains(&tool_id.to_string()))
                    .unwrap_or(false);

                if is_enabled {
                    self.current_tool_id = Some(tool_id.to_string());
                    self.tool_field_index = 0;
                    self.push_screen(Screen::ToolSettings);
                }
            }
            Screen::Automation => self.toggle_automation_field(),
            Screen::Skills => self.toggle_skill(),
            Screen::Agents => self.toggle_agent(),
            Screen::ToolSettings => self.toggle_tool_field(),
            Screen::Mcp => self.toggle_mcp(),
            Screen::Apply => self.attempt_apply(),
            _ => {}
        }
    }

    pub fn save_config(&mut self) {
        let paths = match &self.project_paths {
            Some(p) => p.clone(),
            None => {
                self.errors.push("No project loaded to save.".to_string());
                return;
            }
        };

        if self.working_copy.is_none() {
            self.errors.push("No project loaded to save.".to_string());
            return;
        }

        self.apply_tool_defaults();

        let yaml = match self
            .working_copy
            .as_ref()
            .expect("working_copy checked above")
            .to_yaml()
        {
            Ok(y) => y,
            Err(e) => {
                self.errors
                    .push(format!("Failed to serialize config: {}", e));
                return;
            }
        };

        match macc_core::write_if_changed(
            &paths,
            paths.config_path.to_string_lossy().as_ref(),
            &paths.config_path,
            yaml.as_bytes(),
            |_| Ok(()),
        ) {
            Ok(status) => {
                self.config = self.working_copy.clone();
                if status == macc_core::plan::ActionStatus::Unchanged {
                    self.notices
                        .push("Config unchanged, no save needed.".to_string());
                    self.set_status(
                        UiStatusLevel::Info,
                        "Config unchanged.",
                        Some(Duration::from_secs(3)),
                    );
                } else {
                    self.notices.push("Config saved successfully.".to_string());
                    self.set_status(
                        UiStatusLevel::Success,
                        "Config saved.",
                        Some(Duration::from_secs(3)),
                    );
                }
            }
            Err(e) => {
                self.errors.push(format!("Failed to save config: {}", e));
                self.set_status(
                    UiStatusLevel::Error,
                    format!("Save failed: {}", e),
                    Some(Duration::from_secs(6)),
                );
            }
        }
    }

    fn apply_tool_defaults(&mut self) {
        let Some(working_copy) = &self.working_copy else {
            return;
        };

        let enabled = working_copy.tools.enabled.clone();
        let mut defaults = Vec::new();
        for descriptor in &self.tool_descriptors {
            if !enabled.contains(&descriptor.id) {
                continue;
            }
            for field in &descriptor.fields {
                if field.default.is_none() {
                    continue;
                }
                if field.path.is_empty() {
                    continue;
                }
                if self.read_value_at(&field.path).is_some() {
                    continue;
                }
                if let Some(value) = field_default_json(field) {
                    defaults.push((field.path.clone(), value));
                }
            }
        }

        for (path, value) in defaults {
            let _ = self.set_value_at(&path, value);
        }

        self.apply_tool_normalizations();
    }

    fn apply_tool_normalizations(&mut self) {
        let Some(working_copy) = &self.working_copy else {
            return;
        };

        let enabled = working_copy.tools.enabled.clone();
        let mut updates = Vec::new();
        for descriptor in &self.tool_descriptors {
            if !enabled.contains(&descriptor.id) {
                continue;
            }
            for field in &descriptor.fields {
                if field.path.is_empty() {
                    continue;
                }
                match field.kind {
                    FieldKind::Number => {
                        if let Some(Value::String(text)) = self.read_value_at(&field.path) {
                            if let Ok(parsed) = text.trim().parse::<f64>() {
                                if let Some(num) = serde_json::Number::from_f64(parsed) {
                                    updates.push((field.path.clone(), Value::Number(num)));
                                }
                            }
                        }
                    }
                    FieldKind::Array => {
                        if let Some(Value::String(text)) = self.read_value_at(&field.path) {
                            let items = parse_csv_list(&text);
                            let values = items.into_iter().map(Value::String).collect();
                            updates.push((field.path.clone(), Value::Array(values)));
                        }
                    }
                    _ => {}
                }
            }
        }

        for (path, value) in updates {
            let _ = self.set_value_at(&path, value);
        }
    }

    pub fn open_preview(&mut self) {
        if self.current_screen() != Screen::Preview {
            self.push_screen(Screen::Preview);
        }
        self.refresh_preview_plan();
    }

    pub fn refresh_preview_plan(&mut self) {
        let _quiet = QuietEnvGuard::new("MACC_QUIET", "1");
        self.preview_ops.clear();
        self.preview_diff_cache.clear();
        self.preview_diff_scroll.clear();
        self.preview_error = None;
        self.preview_selection_index = 0;

        let paths = match &self.project_paths {
            Some(paths) => paths,
            None => {
                self.preview_error = Some(
                    "Preview requires a loaded MACC project. Run 'macc init' in the repo root."
                        .to_string(),
                );
                return;
            }
        };

        let canonical = match &self.working_copy {
            Some(cfg) => cfg,
            None => {
                self.preview_error =
                    Some("No canonical configuration available to plan.".to_string());
                return;
            }
        };

        let resolved = resolve(canonical, &CliOverrides::default());
        let fetch_units = match resolve_fetch_units(paths, &resolved) {
            Ok(units) => units,
            Err(e) => {
                self.preview_error = Some(format!("Failed to resolve catalog selections: {}", e));
                return;
            }
        };

        let materialized_units = match materialize_fetch_units(paths, fetch_units) {
            Ok(units) => units,
            Err(e) => {
                self.preview_error = Some(format!("Failed to materialize catalog sources: {}", e));
                return;
            }
        };

        match self.engine.plan(
            paths,
            canonical,
            &materialized_units,
            &CliOverrides::default(),
        ) {
            Ok(plan) => {
                self.preview_ops = self.engine.plan_operations(paths, &plan);
                self.set_preview_selection(0);
            }
            Err(e) => {
                self.preview_error = Some(format!("Planning failed: {}", e));
            }
        }
    }

    fn build_apply_context(&self) -> Result<ApplyContext, String> {
        let _quiet = QuietEnvGuard::new("MACC_QUIET", "1");
        let paths = self
            .project_paths
            .as_ref()
            .ok_or_else(|| "Apply requires a loaded MACC project.".to_string())?;
        let canonical = self
            .working_copy
            .as_ref()
            .ok_or_else(|| "No configuration available to build an apply plan.".to_string())?;

        let resolved = resolve(canonical, &CliOverrides::default());
        let fetch_units = resolve_fetch_units(paths, &resolved)
            .map_err(|e| format!("Failed to resolve catalog selections: {}", e))?;
        let materialized_units = materialize_fetch_units(paths, fetch_units)
            .map_err(|e| format!("Failed to materialize catalog sources: {}", e))?;

        let plan = self
            .engine
            .plan(
                paths,
                canonical,
                &materialized_units,
                &CliOverrides::default(),
            )
            .map_err(|e| format!("Failed to build apply plan: {}", e))?;

        let operations = self.engine.plan_operations(paths, &plan);
        let mut project_ops = 0;
        let mut user_ops = 0;
        for op in &operations {
            match op.scope {
                Scope::Project => project_ops += 1,
                Scope::User => user_ops += 1,
            }
        }

        let backup_preview = format!("{}/<timestamp>", paths.backups_dir.display());
        Ok(ApplyContext {
            plan,
            operations,
            project_ops,
            user_ops,
            backup_preview,
        })
    }

    pub fn open_apply_screen(&mut self) {
        self.apply_consent_input.clear();
        self.apply_user_consent_granted = false;
        self.apply_feedback = None;
        self.apply_error = None;
        self.apply_progress = None;

        match self.build_apply_context() {
            Ok(context) => self.apply_context = Some(context),
            Err(err) => {
                self.apply_context = None;
                self.apply_error = Some(err);
            }
        }

        if self.current_screen() != Screen::Apply {
            self.push_screen(Screen::Apply);
        }
    }

    pub fn append_apply_consent_char(&mut self, ch: char) {
        self.apply_consent_input.push(ch);
        self.apply_user_consent_granted = self.apply_consent_input.eq_ignore_ascii_case("YES");
    }

    pub fn pop_apply_consent_char(&mut self) {
        self.apply_consent_input.pop();
        self.apply_user_consent_granted = self.apply_consent_input.eq_ignore_ascii_case("YES");
    }

    pub fn attempt_apply(&mut self) {
        let paths = match &self.project_paths {
            Some(paths) => paths,
            None => {
                self.apply_error = Some("No project loaded for apply.".to_string());
                return;
            }
        };

        let context = match &self.apply_context {
            Some(ctx) => ctx,
            None => {
                self.apply_error =
                    Some("No apply context available. Refresh and try again.".to_string());
                return;
            }
        };

        if context.needs_user_consent() && !self.apply_user_consent_granted {
            self.apply_error =
                Some("User-level operations require typing YES before applying.".to_string());
            return;
        }

        let allow_user_scope = !context.needs_user_consent() || self.apply_user_consent_granted;
        let mut plan = context.plan.clone();

        let operations = context.operations.clone();
        self.apply_feedback = None;
        self.apply_error = None;
        self.apply_progress = Some(ApplyProgress {
            current: 0,
            total: operations.len(),
            path: None,
        });

        let result = {
            let _quiet = QuietEnvGuard::new("MACC_QUIET", "1");
            // For now, engine.apply doesn't support progress callback yet,
            // but we could add it to Engine trait if needed.
            self.engine.apply(paths, &mut plan, allow_user_scope)
        };

        match result {
            Ok(report) => {
                self.apply_feedback = Some(report.render_cli());
                self.apply_error = None;
                self.notices
                    .push("TUI apply completed successfully.".to_string());
                self.set_status(
                    UiStatusLevel::Success,
                    "Apply completed.",
                    Some(Duration::from_secs(5)),
                );
            }
            Err(err) => {
                self.apply_feedback = None;
                self.apply_error = Some(format!("Apply failed: {}", err));
                self.set_status(
                    UiStatusLevel::Error,
                    format!("Apply failed: {}", err),
                    Some(Duration::from_secs(8)),
                );
            }
        }
    }

    pub fn selected_preview_op(&self) -> Option<&PlannedOp> {
        self.preview_ops.get(self.preview_selection_index)
    }

    fn preview_diff_key(op: &PlannedOp) -> String {
        format!("{}|{:?}", op.path, op.kind)
    }

    fn preview_diff_key_for_selected(&self) -> Option<String> {
        self.selected_preview_op().map(Self::preview_diff_key)
    }

    fn ensure_selected_diff_cached(&mut self) {
        if let Some(op) = self.selected_preview_op().cloned() {
            let key = Self::preview_diff_key(&op);
            self.preview_diff_cache
                .entry(key.clone())
                .or_insert_with(|| render_diff(&op));
            self.preview_diff_scroll.entry(key).or_insert(0);
        }
    }

    fn set_preview_selection(&mut self, index: usize) {
        if self.preview_ops.is_empty() {
            self.preview_selection_index = 0;
            return;
        }
        let bounded = index.min(self.preview_ops.len() - 1);
        self.preview_selection_index = bounded;
        self.ensure_selected_diff_cached();
    }

    pub fn next_preview_op(&mut self) {
        if self.preview_ops.is_empty() {
            return;
        }
        let next = (self.preview_selection_index + 1) % self.preview_ops.len();
        self.set_preview_selection(next);
    }

    pub fn prev_preview_op(&mut self) {
        if self.preview_ops.is_empty() {
            return;
        }
        let next = if self.preview_selection_index == 0 {
            self.preview_ops.len() - 1
        } else {
            self.preview_selection_index - 1
        };
        self.set_preview_selection(next);
    }

    pub fn preview_diff_for_selected(&self) -> Option<&DiffView> {
        let key = self.preview_diff_key_for_selected()?;
        self.preview_diff_cache.get(&key)
    }

    pub fn preview_diff_scroll_position(&self) -> usize {
        self.preview_diff_key_for_selected()
            .and_then(|key| self.preview_diff_scroll.get(&key).copied())
            .unwrap_or(0)
    }

    pub fn scroll_preview_diff(&mut self, delta: isize) {
        self.ensure_selected_diff_cached();
        if let Some(key) = self.preview_diff_key_for_selected() {
            if let Some(view) = self.preview_diff_cache.get(&key) {
                let entry = self.preview_diff_scroll.entry(key.clone()).or_insert(0);
                let line_count = view.diff.lines().count();
                let next = if delta < 0 {
                    entry.saturating_sub((-delta) as usize)
                } else {
                    entry.saturating_add(delta as usize)
                };
                *entry = next.min(line_count);
            }
        }
    }

    pub fn toggle_help(&mut self) {
        self.help_open = !self.help_open;
    }

    pub fn current_tool_field_validation(&self) -> Option<String> {
        if !self.is_tool_field_editing() {
            return None;
        }
        let field = self.current_tool_field()?;
        let input = self.tool_field_input.trim();
        match field.kind {
            FieldKind::Number => {
                if input.is_empty() {
                    Some("Number is required.".to_string())
                } else if input.parse::<f64>().is_err() {
                    Some("Invalid number.".to_string())
                } else {
                    None
                }
            }
            FieldKind::Array => None,
            FieldKind::Text => None,
            _ => None,
        }
    }

    pub fn current_automation_field_validation(&self) -> Option<String> {
        if !self.is_automation_field_editing() {
            return None;
        }
        let idx = self.automation_field_index;
        let input = self.automation_field_input.trim();
        match idx {
            0..=3 => {
                if input.is_empty() {
                    Some("Value cannot be empty.".to_string())
                } else {
                    None
                }
            }
            5 => serde_json::from_str::<BTreeMap<String, usize>>(input)
                .err()
                .map(|e| format!("Invalid JSON: {}", e)),
            6 => serde_json::from_str::<BTreeMap<String, Vec<String>>>(input)
                .err()
                .map(|e| format!("Invalid JSON: {}", e)),
            7..=13 => {
                if input.parse::<usize>().is_err() {
                    Some("Invalid integer value.".to_string())
                } else {
                    None
                }
            }
            14 => {
                let value = input.to_lowercase();
                if !matches!(value.as_str(), "abandon" | "todo" | "blocked") {
                    Some("Allowed: abandon | todo | blocked".to_string())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

// --- Pure Reducer Helpers ---

fn collect_log_files(dir: &std::path::Path, root: &std::path::Path, out: &mut Vec<LogEntry>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(iter) => iter,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_log_files(&path, root, out);
            continue;
        }
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if ext != "md" && ext != "log" && ext != "txt" {
            continue;
        }
        let rel = path
            .strip_prefix(root)
            .ok()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| path.display().to_string());
        out.push(LogEntry {
            path,
            relative: rel,
        });
    }
}

fn format_actionable_error(raw: &str) -> String {
    let lower = raw.to_ascii_lowercase();
    let (cause, fix) = if lower.contains("registry is missing tasks array")
        || lower.contains("invalid registry json")
    {
        (
            "The coordinator registry is malformed.",
            "Run 'macc coordinator sync' to rebuild task_registry.json from PRD, then retry.",
        )
    } else if lower.contains("not found") || lower.contains("no such file") {
        (
            "A required file or command is missing.",
            "Check paths in Automation settings, run 'macc init' in project root, then retry.",
        )
    } else if lower.contains("permission denied") {
        (
            "MACC cannot execute a required script/binary.",
            "Ensure executable permissions (chmod +x) and that your user can access the project files.",
        )
    } else if lower.contains("failed with status") {
        (
            "A coordinator action exited with a non-zero status.",
            "Open the latest file in .macc/log/coordinator/ and resolve the first reported failure cause.",
        )
    } else {
        (
            "Coordinator action failed.",
            "Open logs in .macc/log/coordinator/ and .macc/log/performer/, then rerun the action.",
        )
    };
    format!("{}\n\nCause: {}\nSuggested fix: {}", raw, cause, fix)
}

fn next_index(current: usize, total: usize) -> usize {
    if total == 0 {
        return 0;
    }
    (current + 1) % total
}

fn prev_index(current: usize, total: usize) -> usize {
    if total == 0 {
        return 0;
    }
    if current == 0 {
        total - 1
    } else {
        current - 1
    }
}

fn next_visible_index(current: usize, visible: &[usize]) -> usize {
    if visible.is_empty() {
        return current;
    }
    if let Some(pos) = visible.iter().position(|idx| *idx == current) {
        return visible[(pos + 1) % visible.len()];
    }
    visible[0]
}

fn prev_visible_index(current: usize, visible: &[usize]) -> usize {
    if visible.is_empty() {
        return current;
    }
    if let Some(pos) = visible.iter().position(|idx| *idx == current) {
        if pos == 0 {
            return visible[visible.len() - 1];
        }
        return visible[pos - 1];
    }
    visible[0]
}

fn matches_search(query: &str, fields: &[&str]) -> bool {
    let q = query.trim().to_ascii_lowercase();
    if q.is_empty() {
        return true;
    }
    fields
        .iter()
        .any(|f| f.to_ascii_lowercase().contains(q.as_str()))
}

fn toggle_vec_item(mut vec: Vec<String>, item: String) -> Vec<String> {
    if vec.contains(&item) {
        vec.retain(|i| i != &item);
    } else {
        vec.push(item);
        vec.sort();
        vec.dedup();
    }
    vec
}

fn field_default_json(field: &ToolField) -> Option<Value> {
    match &field.default {
        Some(FieldDefault::Bool(value)) => Some(Value::Bool(*value)),
        Some(FieldDefault::Text(value)) => Some(Value::String(value.clone())),
        Some(FieldDefault::Enum(value)) => Some(Value::String(value.clone())),
        Some(FieldDefault::Number(value)) => {
            serde_json::Number::from_f64(*value).map(Value::Number)
        }
        Some(FieldDefault::Array(value)) => Some(Value::Array(
            value.iter().cloned().map(Value::String).collect(),
        )),
        None => None,
    }
}

fn cycle_value<'a>(options: &'a [&'a str], current: &str) -> &'a str {
    let current_idx = options.iter().position(|&m| m == current).unwrap_or(0);
    let next_idx = (current_idx + 1) % options.len();
    options[next_idx]
}

fn set_json_pointer(root: &mut Value, pointer: &str, new_value: Value) -> Result<(), String> {
    if pointer.is_empty() {
        return Ok(());
    }
    let tokens = pointer
        .trim_start_matches('/')
        .split('/')
        .map(decode_pointer_token)
        .collect::<Vec<_>>();

    let mut current = root;
    for (idx, token) in tokens.iter().enumerate() {
        let is_last = idx == tokens.len() - 1;
        match current {
            Value::Object(map) => {
                if is_last {
                    map.insert(token.clone(), new_value);
                    return Ok(());
                }
                current = map
                    .entry(token.clone())
                    .or_insert_with(|| Value::Object(Map::new()));
            }
            _ => {
                return Err(format!("Cannot set pointer at non-object: {}", pointer));
            }
        }
    }
    Ok(())
}

fn decode_pointer_token(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

fn parse_csv_list(value: &str) -> Vec<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    trimmed
        .split(',')
        .map(|entry| entry.trim())
        .filter(|entry| !entry.is_empty())
        .map(|entry| entry.to_string())
        .collect()
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{:.0}", value)
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use macc_core::plan::{PlannedOpKind, PlannedOpMetadata, Scope};
    use macc_core::{MaccEngine, ToolRegistry};
    use std::fs;
    use tempfile::tempdir;

    fn fixture_ids() -> Vec<String> {
        macc_core::TestEngine::generate_fixture_ids(2)
    }

    fn fixture_engine(ids: &[String]) -> Arc<macc_core::TestEngine> {
        Arc::new(macc_core::TestEngine::with_fixtures_for_ids(ids))
    }

    #[test]
    fn test_navigation_stack() {
        let engine = Arc::new(MaccEngine::new(ToolRegistry::new()));
        let mut state = AppState::with_engine(engine);
        assert_eq!(state.current_screen(), Screen::Home);

        state.push_screen(Screen::About);
        assert_eq!(state.current_screen(), Screen::About);
        assert_eq!(state.screen_stack.len(), 2);

        state.pop_screen();
        assert_eq!(state.current_screen(), Screen::Home);
        assert_eq!(state.screen_stack.len(), 1);

        // Cannot pop last screen
        state.pop_screen();
        assert_eq!(state.current_screen(), Screen::Home);
        assert_eq!(state.screen_stack.len(), 1);
    }

    #[test]
    fn test_goto_screen() {
        let engine = Arc::new(MaccEngine::new(ToolRegistry::new()));
        let mut state = AppState::with_engine(engine);
        state.push_screen(Screen::About);
        state.goto_screen(Screen::Home);
        assert_eq!(state.current_screen(), Screen::Home);
        assert_eq!(state.screen_stack.len(), 1);
    }

    #[test]
    fn test_toggle_help() {
        let engine = Arc::new(MaccEngine::new(ToolRegistry::new()));
        let mut state = AppState::with_engine(engine);
        assert!(!state.help_open);
        state.toggle_help();
        assert!(state.help_open);
        state.toggle_help();
        assert!(!state.help_open);
    }

    #[test]
    fn test_load_config_valid() {
        let dir = tempdir().unwrap();
        let macc_dir = dir.path().join(".macc");
        fs::create_dir(&macc_dir).unwrap();
        let ids = fixture_ids();
        let tool_one = ids[0].clone();
        fs::write(
            macc_dir.join("macc.yaml"),
            format!("tools:\n  enabled:\n    - {}\n", tool_one),
        )
        .unwrap();

        let engine = fixture_engine(&ids);
        let mut state = AppState::with_engine(engine);
        state.load_config(Some(dir.path()));

        assert!(state.errors.is_empty());
        assert!(state.config.is_some());
        assert_eq!(state.config.unwrap().tools.enabled, vec![tool_one]);
    }

    #[test]
    fn test_load_config_missing() {
        let dir = tempdir().unwrap();
        let engine = Arc::new(macc_core::TestEngine::with_fixtures());
        let mut state = AppState::with_engine(engine);
        state.load_config(Some(dir.path()));

        assert!(!state.errors.is_empty());
        assert!(state.errors[0].contains("MACC project not found"));
        assert!(state.config.is_none());
    }

    #[test]
    fn test_load_config_invalid_yaml() {
        let dir = tempdir().unwrap();
        let macc_dir = dir.path().join(".macc");
        fs::create_dir(&macc_dir).unwrap();
        fs::write(macc_dir.join("macc.yaml"), "tools: [invalid").unwrap();

        let engine = Arc::new(macc_core::TestEngine::with_fixtures());
        let mut state = AppState::with_engine(engine);
        state.load_config(Some(dir.path()));

        assert!(!state.errors.is_empty());
        assert!(state.errors[0].contains("Failed to load config"));
        assert!(state.config.is_none());
    }

    #[test]
    fn test_save_config() {
        let dir = tempdir().unwrap();
        let macc_dir = dir.path().join(".macc");
        fs::create_dir(&macc_dir).unwrap();
        let config_path = macc_dir.join("macc.yaml");
        let ids = fixture_ids();
        let tool_one = ids[0].clone();
        let tool_two = ids[1].clone();
        fs::write(
            &config_path,
            format!("tools:\n  enabled:\n    - {}\n", tool_one),
        )
        .unwrap();

        let engine = fixture_engine(&ids);
        let mut state = AppState::with_engine(engine);
        state.load_config(Some(dir.path()));

        // Modify working copy
        if let Some(ref mut wc) = state.working_copy {
            wc.tools.enabled.push(tool_two.clone());
        }

        state.save_config();

        assert!(state.errors.is_empty());
        assert!(state.notices[0].contains("saved successfully"));

        // Verify file content
        let saved_yaml = fs::read_to_string(&config_path).unwrap();
        assert!(saved_yaml.contains(&tool_one));
        assert!(saved_yaml.contains(&tool_two));

        // Verify idempotence
        state.notices.clear();
        state.save_config();
        assert!(state.notices[0].contains("unchanged"));
    }

    #[test]
    fn test_tool_selection_and_toggling() {
        let ids = fixture_ids();
        let tool_one = ids[0].clone();
        let tool_two = ids[1].clone();
        let engine = fixture_engine(&ids);
        let mut state = AppState::with_engine(engine);
        // Mock working copy
        state.working_copy = Some(CanonicalConfig::default());

        // Initial state
        assert_eq!(state.selected_tool_index, 0);
        assert!(state
            .working_copy
            .as_ref()
            .unwrap()
            .tools
            .enabled
            .is_empty());

        // Toggle first tool
        state.toggle_selected_tool();
        assert_eq!(
            state.working_copy.as_ref().unwrap().tools.enabled,
            vec![tool_one.clone()]
        );

        // Move to next tool
        state.next_tool();
        assert_eq!(state.selected_tool_index, 1);

        // Toggle second tool
        state.toggle_selected_tool();
        assert_eq!(
            state.working_copy.as_ref().unwrap().tools.enabled,
            vec![tool_one.clone(), tool_two.clone()]
        );

        // Toggle second tool again (disable)
        state.toggle_selected_tool();
        assert_eq!(
            state.working_copy.as_ref().unwrap().tools.enabled,
            vec![tool_one]
        );

        // Prev tool (back to first)
        state.prev_tool();
        assert_eq!(state.selected_tool_index, 0);

        // Prev tool (loops back to second)
        state.prev_tool();
        assert_eq!(state.selected_tool_index, 1);
    }

    #[test]
    fn test_preview_plan_requires_project() {
        let engine = Arc::new(MaccEngine::new(ToolRegistry::new()));
        let mut state = AppState::with_engine(engine);
        state.refresh_preview_plan();
        assert!(state.preview_ops.is_empty());
        assert!(state.preview_error.is_some());
    }

    #[test]
    fn test_preview_diff_cached_on_selection() {
        let engine = Arc::new(MaccEngine::new(ToolRegistry::new()));
        let mut state = AppState::with_engine(engine);
        let op = PlannedOp {
            path: "docs/example.txt".to_string(),
            scope: Scope::Project,
            consent_required: false,
            kind: PlannedOpKind::Write,
            metadata: PlannedOpMetadata::default(),
            before: Some(b"line\n".to_vec()),
            after: Some(b"line\nnew content\n".to_vec()),
        };

        state.preview_ops = vec![op];
        state.set_preview_selection(0);

        let diff = state.preview_diff_for_selected();
        assert!(diff.is_some());
        let diff = diff.unwrap();
        assert!(diff.diff.contains("new content"));
        assert_eq!(state.preview_diff_scroll_position(), 0);
    }

    #[test]
    fn test_tool_settings_navigation_and_cycling() {
        let ids = fixture_ids();
        let tool_two = ids[1].clone();
        let engine = fixture_engine(&ids);
        let mut state = AppState::with_engine(engine);
        state.working_copy = Some(CanonicalConfig::default());

        state.current_tool_id = Some(tool_two.clone());
        state.tool_field_index = 1; // Index 1 is 'model' in tool two

        // Cycle model (from default None to next)
        // options: [smart, small]
        // None -> uses smart -> returns small
        state.toggle_tool_field();

        let settings = state
            .working_copy
            .as_ref()
            .unwrap()
            .tools
            .config
            .get(&tool_two)
            .unwrap();
        assert_eq!(
            settings
                .get("settings")
                .unwrap()
                .get("model_name")
                .unwrap()
                .as_str()
                .unwrap(),
            "small"
        );

        // Cycle model again (loops back)
        state.toggle_tool_field();
        let settings = state
            .working_copy
            .as_ref()
            .unwrap()
            .tools
            .config
            .get(&tool_two)
            .unwrap();
        assert_eq!(
            settings
                .get("settings")
                .unwrap()
                .get("model_name")
                .unwrap()
                .as_str()
                .unwrap(),
            "smart"
        );
    }

    #[test]
    fn test_skills_selection() {
        let ids = fixture_ids();
        let tool_one = ids[0].clone();
        let engine = fixture_engine(&ids);
        let mut state = AppState::with_engine(engine);
        state.working_copy = Some(CanonicalConfig::default());
        state.skill_target_path = Some(format!("/tools/config/{}/skills", tool_one));
        state.goto_screen(Screen::Skills);

        // Initial state
        assert_eq!(state.skill_selection_index, 0);

        let empty_vec: Vec<String> = Vec::new();
        let current_skills = state
            .working_copy
            .as_ref()
            .unwrap()
            .tools
            .config
            .get(&tool_one)
            .and_then(|v| v.get("skills"))
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or(empty_vec);
        assert!(current_skills.is_empty());

        // Toggle first skill (mock-skill-one)
        state.toggle_skill();

        let current_skills: Vec<String> = serde_json::from_value(
            state
                .working_copy
                .as_ref()
                .unwrap()
                .tools
                .config
                .get(&tool_one)
                .unwrap()
                .get("skills")
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert_eq!(current_skills, vec!["mock-skill-one"]);

        // Move to next skill
        state.next_skill();
        assert_eq!(state.skill_selection_index, 1);

        // Toggle second skill (mock-skill-two)
        state.toggle_skill();
        let current_skills: Vec<String> = serde_json::from_value(
            state
                .working_copy
                .as_ref()
                .unwrap()
                .tools
                .config
                .get(&tool_one)
                .unwrap()
                .get("skills")
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert_eq!(current_skills, vec!["mock-skill-one", "mock-skill-two"]);

        // Select none
        state.select_no_skills();
        let current_skills: Vec<String> = serde_json::from_value(
            state
                .working_copy
                .as_ref()
                .unwrap()
                .tools
                .config
                .get(&tool_one)
                .unwrap()
                .get("skills")
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert!(current_skills.is_empty());

        // Select all
        state.select_all_skills();
        let current_skills: Vec<String> = serde_json::from_value(
            state
                .working_copy
                .as_ref()
                .unwrap()
                .tools
                .config
                .get(&tool_one)
                .unwrap()
                .get("skills")
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert_eq!(current_skills.len(), 2);
        assert!(current_skills.contains(&"mock-skill-one".to_string()));
        assert!(current_skills.contains(&"mock-skill-two".to_string()));
    }

    #[test]
    fn test_agents_selection() {
        let ids = fixture_ids();
        let tool_one = ids[0].clone();
        let engine = fixture_engine(&ids);
        let mut state = AppState::with_engine(engine);
        state.working_copy = Some(CanonicalConfig::default());
        state.agent_target_path = Some(format!("/tools/config/{}/agents", tool_one));
        state.goto_screen(Screen::Agents);

        // Initial state
        assert_eq!(state.agent_selection_index, 0);

        let empty_vec: Vec<String> = Vec::new();
        let current_agents = state
            .working_copy
            .as_ref()
            .unwrap()
            .tools
            .config
            .get(&tool_one)
            .and_then(|v| v.get("agents"))
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or(empty_vec);
        assert!(current_agents.is_empty());

        // Toggle first agent (mock-agent-one)
        state.toggle_agent();
        let current_agents: Vec<String> = serde_json::from_value(
            state
                .working_copy
                .as_ref()
                .unwrap()
                .tools
                .config
                .get(&tool_one)
                .unwrap()
                .get("agents")
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert_eq!(current_agents, vec!["mock-agent-one"]);

        // Move to next agent
        state.next_agent();
        assert_eq!(state.agent_selection_index, 1);

        // Toggle second agent (mock-agent-two)
        state.toggle_agent();
        let current_agents: Vec<String> = serde_json::from_value(
            state
                .working_copy
                .as_ref()
                .unwrap()
                .tools
                .config
                .get(&tool_one)
                .unwrap()
                .get("agents")
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert_eq!(current_agents, vec!["mock-agent-one", "mock-agent-two"]);

        // Select none
        state.select_no_agents();
        let current_agents: Vec<String> = serde_json::from_value(
            state
                .working_copy
                .as_ref()
                .unwrap()
                .tools
                .config
                .get(&tool_one)
                .unwrap()
                .get("agents")
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert!(current_agents.is_empty());

        // Select all
        state.select_all_agents();
        let current_agents: Vec<String> = serde_json::from_value(
            state
                .working_copy
                .as_ref()
                .unwrap()
                .tools
                .config
                .get(&tool_one)
                .unwrap()
                .get("agents")
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert_eq!(current_agents.len(), 2);
        assert!(current_agents.contains(&"mock-agent-one".to_string()));
        assert!(current_agents.contains(&"mock-agent-two".to_string()));
    }

    #[test]
    fn test_mcp_selection_toggle_and_bulk() {
        let temp = tempdir().unwrap();
        let paths = ProjectPaths::from_root(temp.path());
        std::fs::create_dir_all(&paths.macc_dir).unwrap();
        std::fs::create_dir_all(&paths.catalog_dir).unwrap();
        std::fs::write(paths.macc_dir.join("macc.yaml"), "tools:\n  enabled: []\n").unwrap();

        let mut catalog = macc_core::catalog::McpCatalog::default();
        catalog.entries.push(macc_core::catalog::McpEntry {
            id: "mcp-a".into(),
            name: "MCP A".into(),
            description: "First MCP".into(),
            tags: vec!["alpha".into()],
            selector: macc_core::catalog::Selector {
                subpath: "path/a".into(),
            },
            source: macc_core::catalog::Source {
                kind: macc_core::catalog::SourceKind::Git,
                url: "https://example.com/a.git".into(),
                reference: "main".into(),
                checksum: None,
                subpaths: vec![],
            },
        });
        catalog.entries.push(macc_core::catalog::McpEntry {
            id: "mcp-b".into(),
            name: "MCP B".into(),
            description: "Second MCP".into(),
            tags: vec!["beta".into()],
            selector: macc_core::catalog::Selector {
                subpath: "path/b".into(),
            },
            source: macc_core::catalog::Source {
                kind: macc_core::catalog::SourceKind::Git,
                url: "https://example.com/b.git".into(),
                reference: "main".into(),
                checksum: None,
                subpaths: vec![],
            },
        });
        catalog
            .save_atomically(&paths, &paths.mcp_catalog_path())
            .unwrap();

        let ids = fixture_ids();
        let engine = fixture_engine(&ids);
        let mut state = AppState::with_engine(engine);
        state.load_config(Some(temp.path()));
        state.working_copy = Some(CanonicalConfig::default());
        assert_eq!(state.mcp_entries.len(), 2);

        state.toggle_mcp();
        let selected = state
            .working_copy
            .as_ref()
            .unwrap()
            .selections
            .as_ref()
            .unwrap()
            .mcp
            .clone();
        assert_eq!(selected, vec!["mcp-a".to_string()]);

        state.select_all_mcp();
        let selected = state
            .working_copy
            .as_ref()
            .unwrap()
            .selections
            .as_ref()
            .unwrap()
            .mcp
            .clone();
        assert_eq!(selected.len(), 2);

        state.select_no_mcp();
        let selected = state
            .working_copy
            .as_ref()
            .unwrap()
            .selections
            .as_ref()
            .unwrap()
            .mcp
            .clone();
        assert!(selected.is_empty());
    }

    #[test]
    fn test_pure_helpers() {
        // next_index
        assert_eq!(next_index(0, 3), 1);
        assert_eq!(next_index(2, 3), 0);
        assert_eq!(next_index(0, 0), 0);

        // prev_index
        assert_eq!(prev_index(1, 3), 0);
        assert_eq!(prev_index(0, 3), 2);
        assert_eq!(prev_index(0, 0), 0);

        // toggle_vec_item
        let v = vec!["a".to_string(), "c".to_string()];
        let v = toggle_vec_item(v, "b".to_string());
        assert_eq!(v, vec!["a", "b", "c"]);
        let v = toggle_vec_item(v, "a".to_string());
        assert_eq!(v, vec!["b", "c"]);

        // cycle_value
        let options = &["a", "b", "c"];
        assert_eq!(cycle_value(options, "a"), "b");
        assert_eq!(cycle_value(options, "c"), "a");
        assert_eq!(cycle_value(options, "unknown"), "b"); // defaults to 0 + 1
    }

    #[test]
    fn test_unified_navigation() {
        let ids = fixture_ids();
        let tool_one = ids[0].clone();
        let engine = fixture_engine(&ids);
        let mut state = AppState::with_engine(engine);
        state.working_copy = Some(CanonicalConfig::default());

        // Home screen (no-op for these)
        state.navigate_next();
        assert_eq!(state.current_screen(), Screen::Home);

        // Tools screen
        state.goto_screen(Screen::Tools);
        state.navigate_next();
        assert_eq!(state.selected_tool_index, 1);
        state.navigate_prev();
        assert_eq!(state.selected_tool_index, 0);

        // Toggle tool
        state.navigate_toggle();
        assert_eq!(
            state.working_copy.as_ref().unwrap().tools.enabled,
            vec![tool_one.clone()]
        );

        // Enter sub-screen
        state.navigate_enter();
        assert_eq!(state.current_screen(), Screen::ToolSettings);

        // Tool settings fields
        state.navigate_next();
        // First tool has 4 fields, so it should move to 1.
        assert_eq!(state.tool_field_index, 1);
        state.navigate_prev();
        assert_eq!(state.tool_field_index, 0);

        state.navigate_toggle(); // toggle enabled
        let settings = state
            .working_copy
            .as_ref()
            .unwrap()
            .tools
            .config
            .get(&tool_one)
            .unwrap();
        assert_eq!(settings.get("enabled").unwrap().as_bool().unwrap(), true);

        // MCP screen (no catalog entries loaded in this test)
        state.goto_screen(Screen::Mcp);
        state.navigate_next();
        if state.mcp_entries.len() > 1 {
            assert_eq!(state.mcp_selection_index, 1);
        } else {
            assert_eq!(state.mcp_selection_index, 0);
        }
    }

    #[test]
    fn test_config_golden_serialization() {
        let mut config = CanonicalConfig::default();
        let ids = fixture_ids();
        let tool_one = ids[0].clone();
        let tool_two = ids[1].clone();
        config.tools.enabled = vec![tool_one.clone(), tool_two];

        config.tools.settings.insert(
            tool_one,
            serde_json::json!({
                "model": "smart",
                "language": "English",
                "permissions": "strict",
                "skills": ["create-plan", "implement"],
                "agents": ["architect"],
                "rules_enabled": false
            }),
        );

        config.selections = Some(macc_core::config::SelectionsConfig {
            mcp: vec!["local-notes".to_string()],
            ..Default::default()
        });

        let yaml = config.to_yaml().expect("Serialization failed");

        // Golden check: verify specific deterministic properties
        assert!(yaml.contains("model: smart"));
        assert!(yaml.contains("language: English"));
        assert!(yaml.contains("- create-plan"));
        assert!(yaml.contains("- implement")); // alphabetical sort check
        assert!(yaml.find("create-plan").unwrap() < yaml.find("implement").unwrap());

        // Roundtrip
        let config2 = CanonicalConfig::from_yaml(&yaml).expect("Deserialization failed");
        assert_eq!(config, config2);

        // Idempotence
        let yaml2 = config2.to_yaml().expect("Second serialization failed");
        assert_eq!(yaml, yaml2);
    }

    #[test]
    fn test_interaction_mode_labels() {
        let engine = Arc::new(MaccEngine::new(ToolRegistry::new()));
        let mut state = AppState::with_engine(engine);
        assert_eq!(state.interaction_mode_label(), "browse");

        state.push_screen(Screen::Apply);
        assert_eq!(state.interaction_mode_label(), "confirm");

        state.pop_screen();
        state.push_screen(Screen::Automation);
        state.automation_field_editing = true;
        assert_eq!(state.interaction_mode_label(), "edit");
    }

    #[test]
    fn test_inline_validation_for_automation_number_field() {
        let engine = Arc::new(MaccEngine::new(ToolRegistry::new()));
        let mut state = AppState::with_engine(engine);
        state.working_copy = Some(CanonicalConfig::default());
        state.push_screen(Screen::Automation);

        state.automation_field_index = 8; // Max Parallel
        state.automation_field_editing = true;
        state.automation_field_input = "abc".to_string();
        assert!(state.current_automation_field_validation().is_some());

        state.automation_field_input = "3".to_string();
        assert!(state.current_automation_field_validation().is_none());
    }

    #[test]
    fn test_format_actionable_error_includes_cause_and_fix() {
        let msg = format_actionable_error("invalid registry JSON");
        assert!(msg.contains("Cause:"));
        assert!(msg.contains("Suggested fix:"));
        assert!(msg.contains("registry"));
    }
}
