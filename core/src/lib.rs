pub mod automation;
pub mod catalog;
pub mod config;
pub mod doctor;
pub mod engine;
pub use config::migrate;
pub mod mcp_json;
pub mod packages;
pub mod plan;
pub mod resolve;
pub mod security;
mod structured_merge;
pub mod tool;
pub mod user_backup;
pub mod worktree;

pub use automation::{embedded_runner_path_for_ref, ensure_embedded_automation_scripts};
pub use catalog::{McpCatalog, McpEntry, Selector, SkillEntry, SkillsCatalog, Source, SourceKind};
use chrono::Local;
pub use config::load_canonical_config;
pub use engine::{Engine, MaccEngine, TestEngine};
pub use resolve::{resolve, CliOverrides, ResolvedConfig};
pub use security::Finding;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use structured_merge::StructuredToolMergePolicy;
use thiserror::Error;
pub use tool::{FieldKind, ToolAdapter, ToolDescriptor, ToolField, ToolRegistry};
pub use user_backup::{find_user_home, UserBackupEntry, UserBackupManager, UserBackupReport};
pub use worktree::{
    create_worktrees, current_worktree, list_worktrees, prune_worktrees, read_worktree_metadata,
    remove_worktree, WorktreeCreateResult, WorktreeCreateSpec, WorktreeEntry, WorktreeMetadata,
};

#[derive(Error, Debug)]
pub enum MaccError {
    #[error("Configuration error in {path}: {source}")]
    Config {
        path: String,
        #[source]
        source: serde_yaml::Error,
    },

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("ToolSpec error in {path}: {message}")]
    ToolSpec {
        path: String,
        line: Option<usize>,
        column: Option<usize>,
        message: String,
    },

    #[error("User home directory not found")]
    HomeDirNotFound,

    #[error("User scope actions are not allowed in M0: {0}")]
    UserScopeNotAllowed(String),

    #[error("IO error in {path} during {action}: {source}")]
    Io {
        path: String,
        action: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Project root not found (searched up from {start_dir})")]
    ProjectRootNotFound { start_dir: String },

    #[error("Secret(s) detected in generated output for {path}: {details}")]
    SecretDetected { path: String, details: String },
}

pub type Result<T> = std::result::Result<T, MaccError>;

#[derive(Debug, Clone, Default)]
pub struct ApplyReport {
    pub outcomes: std::collections::BTreeMap<String, plan::ActionStatus>,
    pub backup_dir: Option<PathBuf>,
    pub user_backup_report: Option<UserBackupReport>,
}

impl ApplyReport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn render_cli(&self) -> String {
        use std::fmt::Write;
        let mut output = String::new();

        writeln!(output, "\nApply Summary:").unwrap();
        writeln!(output, "{:<40} {:<10}", "PATH", "STATUS").unwrap();
        writeln!(output, "{:-<40} {:-<10}", "", "").unwrap();

        let mut created = 0;
        let mut updated = 0;
        let mut unchanged = 0;

        for (path, status) in &self.outcomes {
            writeln!(output, "{:<40} {:<10}", path, status.as_str()).unwrap();
            match status {
                plan::ActionStatus::Created => created += 1,
                plan::ActionStatus::Updated => updated += 1,
                plan::ActionStatus::Unchanged => unchanged += 1,
                _ => {}
            }
        }

        writeln!(
            output,
            "\nTotal: {} created, {} updated, {} unchanged",
            created, updated, unchanged
        )
        .unwrap();

        if let Some(backup_dir) = &self.backup_dir {
            writeln!(output, "Backups created in: {}", backup_dir.display()).unwrap();
        }
        if let Some(report) = &self.user_backup_report {
            writeln!(output, "User backups created in: {}", report.root.display()).unwrap();
        }

        output
    }
}

#[derive(Debug, Clone, Default)]
pub struct ClearReport {
    pub removed: usize,
    pub skipped: usize,
}

#[derive(Debug, Clone)]
pub struct ProjectPaths {
    pub root: PathBuf,
    pub macc_dir: PathBuf,
    pub config_path: PathBuf,
    pub backups_dir: PathBuf,
    pub tmp_dir: PathBuf,
    pub catalog_dir: PathBuf,
    pub cache_dir: PathBuf,
}

impl ProjectPaths {
    pub fn from_root<P: AsRef<Path>>(root: P) -> Self {
        let root = root.as_ref().to_path_buf();
        // Ensure root is absolute if possible, but CLI should have already done this.
        // We don't canonicalize here because the directory might not exist yet for 'init'.
        let macc_dir = root.join(".macc");
        let catalog_dir = macc_dir.join("catalog");
        ProjectPaths {
            root: root.clone(),
            config_path: macc_dir.join("macc.yaml"),
            backups_dir: macc_dir.join("backups"),
            tmp_dir: macc_dir.join("tmp"),
            catalog_dir,
            cache_dir: macc_dir.join("cache"),
            macc_dir,
        }
    }

    pub fn skills_catalog_path(&self) -> PathBuf {
        self.catalog_dir.join("skills.catalog.json")
    }

    pub fn mcp_catalog_path(&self) -> PathBuf {
        self.catalog_dir.join("mcp.catalog.json")
    }

    pub fn project_catalog_dir(&self) -> PathBuf {
        self.macc_dir.join("catalog")
    }

    pub fn project_skills_catalog_path(&self) -> PathBuf {
        self.project_catalog_dir().join("skills.catalog.json")
    }

    pub fn project_mcp_catalog_path(&self) -> PathBuf {
        self.project_catalog_dir().join("mcp.catalog.json")
    }

    pub fn source_cache_path(&self, source_key: &str) -> PathBuf {
        self.cache_dir.join(source_key)
    }

    pub fn user_cache_dir(&self) -> Option<PathBuf> {
        find_user_home().map(|home| home.join(".macc").join("cache"))
    }

    pub fn user_source_cache_path(&self, source_key: &str) -> Option<PathBuf> {
        self.user_cache_dir().map(|dir| dir.join(source_key))
    }

    pub fn automation_dir(&self) -> PathBuf {
        self.macc_dir.join("automation")
    }

    pub fn automation_runner_dir(&self) -> PathBuf {
        self.automation_dir().join("runners")
    }

    pub fn automation_performer_path(&self) -> PathBuf {
        self.automation_dir().join("performer.sh")
    }

    pub fn automation_coordinator_path(&self) -> PathBuf {
        self.automation_dir().join("coordinator.sh")
    }

    pub fn automation_runner_path(&self, tool_id: &str) -> PathBuf {
        self.automation_runner_dir()
            .join(format!("{}.performer.sh", tool_id))
    }

    pub fn managed_paths_state_path(&self) -> PathBuf {
        self.macc_dir.join("state").join("managed_paths.json")
    }
}

pub fn find_project_root<P: AsRef<Path>>(start_dir: P) -> Result<ProjectPaths> {
    let start_dir = start_dir.as_ref();
    let mut current = if start_dir.is_absolute() {
        start_dir.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| MaccError::Io {
                path: ".".into(),
                action: "get current_dir".into(),
                source: e,
            })?
            .join(start_dir)
    };

    loop {
        let macc_yaml = current.join(".macc").join("macc.yaml");
        if macc_yaml.exists() {
            // Found it, now canonicalize the root for consistency
            let root = current.canonicalize().unwrap_or(current);
            return Ok(ProjectPaths::from_root(root));
        }

        match current.parent() {
            Some(parent) => current = parent.to_path_buf(),
            None => {
                return Err(MaccError::ProjectRootNotFound {
                    start_dir: start_dir.to_string_lossy().into(),
                })
            }
        }
    }
}

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
struct ManagedPathsState {
    version: u32,
    paths: Vec<String>,
}

fn normalize_relative_path(path: &str) -> Option<String> {
    let p = Path::new(path);
    if p.is_absolute() || path.is_empty() {
        return None;
    }
    let mut normalized = PathBuf::new();
    for comp in p.components() {
        use std::path::Component;
        match comp {
            Component::Normal(seg) => normalized.push(seg),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    if normalized.as_os_str().is_empty() {
        return None;
    }
    Some(normalized.to_string_lossy().replace('\\', "/"))
}

fn relative_to_root(paths: &ProjectPaths, abs: &Path) -> Option<String> {
    abs.strip_prefix(&paths.root)
        .ok()
        .and_then(|p| normalize_relative_path(&p.to_string_lossy()))
}

fn load_managed_paths(paths: &ProjectPaths) -> Result<BTreeSet<String>> {
    let state_path = paths.managed_paths_state_path();
    if !state_path.exists() {
        return Ok(BTreeSet::new());
    }
    let raw = std::fs::read_to_string(&state_path).map_err(|e| MaccError::Io {
        path: state_path.to_string_lossy().into(),
        action: "read managed paths state".into(),
        source: e,
    })?;
    let state: ManagedPathsState = serde_json::from_str(&raw).map_err(|e| {
        MaccError::Validation(format!(
            "Failed to parse managed paths state at {}: {}",
            state_path.display(),
            e
        ))
    })?;
    Ok(state
        .paths
        .into_iter()
        .filter_map(|p| normalize_relative_path(&p))
        .collect())
}

fn save_managed_paths(paths: &ProjectPaths, entries: &BTreeSet<String>) -> Result<()> {
    let state_path = paths.managed_paths_state_path();
    if let Some(parent) = state_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| MaccError::Io {
            path: parent.to_string_lossy().into(),
            action: "create managed state directory".into(),
            source: e,
        })?;
    }
    let payload = ManagedPathsState {
        version: 1,
        paths: entries.iter().cloned().collect(),
    };
    let mut content = serde_json::to_string_pretty(&payload).map_err(|e| {
        MaccError::Validation(format!("Failed to serialize managed paths state: {}", e))
    })?;
    content.push('\n');
    atomic_write(paths, &state_path, content.as_bytes())
}

fn record_managed_path(paths: &ProjectPaths, relative_path: &str) -> Result<()> {
    let Some(normalized) = normalize_relative_path(relative_path) else {
        return Ok(());
    };
    let mut managed = load_managed_paths(paths)?;
    if managed.insert(normalized) {
        save_managed_paths(paths, &managed)?;
    }
    Ok(())
}

pub fn init(paths: &ProjectPaths, force: bool) -> Result<()> {
    println!(
        "Core: Initializing in {} (force: {})",
        paths.root.display(),
        force
    );

    if !paths.root.exists() {
        std::fs::create_dir_all(&paths.root).map_err(|e| MaccError::Io {
            path: paths.root.to_string_lossy().into(),
            action: "create project root".into(),
            source: e,
        })?;
    }

    // Create .macc and required subdirectories
    let skills_dir = paths.macc_dir.join("skills");
    let project_catalog_dir = paths.project_catalog_dir();
    let automation_dir = paths.automation_dir();
    let automation_runner_dir = paths.automation_runner_dir();
    let dirs_to_create = [
        &paths.macc_dir,
        &paths.backups_dir,
        &paths.tmp_dir,
        &paths.catalog_dir,
        &project_catalog_dir,
        &automation_dir,
        &automation_runner_dir,
        &skills_dir,
    ];
    let mut created_paths: Vec<String> = Vec::new();
    for dir in dirs_to_create {
        if !dir.exists() {
            std::fs::create_dir_all(dir).map_err(|e| MaccError::Io {
                path: dir.to_string_lossy().into(),
                action: format!("create directory {}", dir.display()),
                source: e,
            })?;
            if let Some(rel) = relative_to_root(paths, dir) {
                created_paths.push(rel);
            }
        }
    }

    if !paths.config_path.exists() || force {
        let (available_specs, default_tools) = {
            let search_paths = crate::tool::ToolSpecLoader::default_search_paths(&paths.root);
            let loader = crate::tool::ToolSpecLoader::new(search_paths);
            let (specs, _) = loader.load_all_with_embedded();
            let enabled: Vec<String> = specs.first().map(|s| s.id.clone()).into_iter().collect();
            (specs, enabled)
        };
        let mut tool_config = BTreeMap::new();
        for spec in available_specs {
            tool_config.insert(spec.id, serde_json::json!({"context": {"protect": true}}));
        }
        let default_config = config::CanonicalConfig {
            version: Some("v1".to_string()),
            tools: config::ToolsConfig {
                enabled: default_tools,
                config: tool_config,
                ..Default::default()
            },
            standards: config::StandardsConfig::default(),
            selections: None,
            automation: config::AutomationConfig::default(),
            mcp_templates: config::builtin_mcp_templates(),
        };
        let yaml = default_config.to_yaml().map_err(|e| {
            MaccError::Validation(format!("Failed to serialize default config: {}", e))
        })?;
        let status = write_if_changed(
            paths,
            paths.config_path.to_string_lossy().as_ref(),
            &paths.config_path,
            yaml.as_bytes(),
            |_| Ok(()),
        )?;
        if status == plan::ActionStatus::Created {
            if let Some(rel) = relative_to_root(paths, &paths.config_path) {
                created_paths.push(rel);
            }
        }
    }

    // Seed catalog files if missing
    if !paths.skills_catalog_path().exists() {
        let catalog = crate::catalog::SkillsCatalog::default();
        catalog.save_atomically(paths, &paths.skills_catalog_path())?;
        if let Some(rel) = relative_to_root(paths, &paths.skills_catalog_path()) {
            created_paths.push(rel);
        }
    }
    if !paths.mcp_catalog_path().exists() {
        let catalog = crate::catalog::McpCatalog::default();
        catalog.save_atomically(paths, &paths.mcp_catalog_path())?;
        if let Some(rel) = relative_to_root(paths, &paths.mcp_catalog_path()) {
            created_paths.push(rel);
        }
    }

    let created_automation = crate::automation::ensure_embedded_automation_scripts(paths)?;
    for abs in created_automation {
        if let Some(rel) = relative_to_root(paths, &abs) {
            created_paths.push(rel);
        }
    }

    let mut ignore_entries: Vec<String> = BASELINE_IGNORE_ENTRIES
        .iter()
        .map(|s| s.to_string())
        .collect();
    ignore_entries.extend(collect_tool_gitignore_entries(paths, None));
    let ignore_refs: Vec<&str> = ignore_entries.iter().map(|s| s.as_str()).collect();
    let gitignore_status = ensure_gitignore_entries(paths, &ignore_refs, None)?;
    if gitignore_status == plan::ActionStatus::Created {
        created_paths.push(".gitignore".to_string());
    }

    for rel in created_paths {
        record_managed_path(paths, &rel)?;
    }

    Ok(())
}

pub fn plan(
    paths: &ProjectPaths,
    tools: Option<&str>,
    materialized_units: &[resolve::MaterializedFetchUnit],
    registry: &ToolRegistry,
) -> Result<()> {
    let canonical = load_canonical_config(&paths.config_path)?;
    let allowed_tools = registry.list_ids();
    let overrides = if let Some(tools_csv) = tools {
        CliOverrides::from_tools_csv(tools_csv, &allowed_tools)?
    } else {
        CliOverrides::default()
    };

    let resolved = resolve(&canonical, &overrides);

    println!(
        "Core: Planning in {} with tools: {:?}",
        paths.root.display(),
        resolved.tools.enabled
    );

    let total_plan = build_plan(paths, &resolved, materialized_units, registry)?;
    preview_plan(&total_plan, paths)?;

    println!("Core: Total actions planned: {}", total_plan.actions.len());

    Ok(())
}

pub fn build_plan(
    paths: &ProjectPaths,
    resolved: &ResolvedConfig,
    materialized_units: &[resolve::MaterializedFetchUnit],
    registry: &ToolRegistry,
) -> Result<plan::ActionPlan> {
    let mut total_plan = plan::ActionPlan::new();

    // Add baseline ignore entries
    for entry in BASELINE_IGNORE_ENTRIES {
        total_plan.add_action(plan::Action::EnsureGitignore {
            pattern: entry.to_string(),
            scope: plan::Scope::Project,
        });
    }

    let tool_ignore_entries = collect_tool_gitignore_entries(paths, Some(&resolved.tools.enabled));
    for entry in tool_ignore_entries {
        total_plan.add_action(plan::Action::EnsureGitignore {
            pattern: entry,
            scope: plan::Scope::Project,
        });
    }

    // Ralph automation script
    if let Some(ralph) = &resolved.automation.ralph {
        if ralph.enabled {
            plan_ralph_script(&mut total_plan, resolved)?;
        }
    }

    let ctx = resolve::PlanningContext {
        paths,
        resolved,
        materialized_units,
    };

    for tool_id in &resolved.tools.enabled {
        if let Some(adapter) = registry.get(tool_id) {
            let tool_plan = adapter.plan(&ctx)?;
            for action in tool_plan.actions {
                total_plan.add_action(action);
            }
        }
    }

    total_plan.normalize();
    Ok(total_plan)
}

fn plan_ralph_script(plan: &mut plan::ActionPlan, resolved: &ResolvedConfig) -> Result<()> {
    let ralph = resolved.automation.ralph.as_ref().ok_or_else(|| {
        MaccError::Validation("Ralph configuration missing during planning".into())
    })?;

    let default_iterations = ralph.iterations_default;
    let stop_on_failure = if ralph.stop_on_failure {
        "true"
    } else {
        "false"
    };
    let tool = resolved
        .tools
        .enabled
        .first()
        .cloned()
        .unwrap_or_else(|| "none".to_string());

    let content = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail

# MACC Ralph Automation Script
# Generated by MACC apply - DO NOT EDIT MANUALLY

# Ralph is an iterative development loop following the sequence:
# 1. Context Loading
# 2. Task Selection (/next-task)
# 3. Implementation (/implement)
# 4. Validation (/validate)
# 5. Progress Tracking (/update-progress)
# 6. Commitment (/git-add-commit-push)

ITERATIONS=${{1:-{default_iterations}}}
TOOL="{tool}"
STOP_ON_FAILURE={stop_on_failure}

echo "Starting Ralph automation loop (Tool: $TOOL, Iterations: $ITERATIONS)"

for i in $(seq 1 "$ITERATIONS"); do
    echo "--------------------------------------------------------------------------------"
    echo " Ralph Iteration $i of $ITERATIONS"
    echo "--------------------------------------------------------------------------------"

    # NOTE: The following is a placeholder for tool-specific invocation.
    # The agent is expected to perform the steps above when invoked.

    echo "Running $TOOL agent..."

    # Example:
    # case "$TOOL" in
    #   example-tool)
    #     example-tool exec "Run one iteration of the Ralph workflow as defined in docs/ralph.md"
    #     ;;
    #   *)
    #     echo "Warning: No specific invocation for tool '$TOOL'"
    #     false
    #     ;;
    # esac

    # Placeholder logic (uncomment and customize when ready):
    # if ! [YOUR_TOOL_COMMAND_HERE]; then
    #   if [ "$STOP_ON_FAILURE" = true ]; then
    #     echo "Error: Ralph iteration $i failed. Stopping."
    #     exit 1
    #   fi
    #   echo "Warning: Ralph iteration $i failed. Continuing..."
    # fi

    echo "Iteration $i complete."
done

echo "Ralph loop finished."
"#
    );

    let path = "scripts/ralph.sh";
    plan.add_action(plan::Action::Mkdir {
        path: "scripts".into(),
        scope: plan::Scope::Project,
    });
    plan.add_action(plan::Action::WriteFile {
        path: path.into(),
        content: content.into_bytes(),
        scope: plan::Scope::Project,
    });
    plan.add_action(plan::Action::SetExecutable {
        path: path.into(),
        scope: plan::Scope::Project,
    });

    Ok(())
}

/// Produces deterministic `PlannedOp`s from the apply plan so other UIs (TUI, Ralph) can preview changes.
pub fn plan_operations(
    paths: &ProjectPaths,
    resolved: &ResolvedConfig,
    materialized_units: &[resolve::MaterializedFetchUnit],
    registry: &ToolRegistry,
) -> Result<Vec<plan::PlannedOp>> {
    let total_plan = build_plan(paths, resolved, materialized_units, registry)?;
    Ok(plan::collect_plan_operations(paths, &total_plan))
}

pub fn validate_plan(plan: &plan::ActionPlan, allow_user_scope: bool) -> Result<()> {
    for action in &plan.actions {
        if action.scope() == plan::Scope::User && !allow_user_scope {
            return Err(MaccError::UserScopeNotAllowed(format!(
                "Action for path '{}' is User scope",
                action.path()
            )));
        }

        match action {
            plan::Action::WriteFile { path, content, .. } => {
                // Scan for secrets
                let findings = security::scan_bytes(path, content);
                if findings
                    .iter()
                    .any(|f| f.severity == security::Severity::Error)
                {
                    return Err(MaccError::SecretDetected {
                        path: path.clone(),
                        details: findings
                            .iter()
                            .filter(|f| f.severity == security::Severity::Error)
                            .map(|f| format!("{} ({})", f.pattern_name, f.redacted_match))
                            .collect::<Vec<_>>()
                            .join(", "),
                    });
                }
            }
            plan::Action::MergeJson { path, patch, .. } => {
                // Scan for secrets in the patch
                let content = serde_json::to_vec(patch).unwrap_or_default();
                let findings = security::scan_bytes(path, &content);
                if findings
                    .iter()
                    .any(|f| f.severity == security::Severity::Error)
                {
                    return Err(MaccError::SecretDetected {
                        path: path.clone(),
                        details: findings
                            .iter()
                            .filter(|f| f.severity == security::Severity::Error)
                            .map(|f| format!("{} ({})", f.pattern_name, f.redacted_match))
                            .collect::<Vec<_>>()
                            .join(", "),
                    });
                }
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn preview_plan(plan: &plan::ActionPlan, paths: &ProjectPaths) -> Result<()> {
    for action in &plan.actions {
        match action {
            plan::Action::Noop { description, scope } => {
                let status = if *scope == plan::Scope::User {
                    " (REFUSED)"
                } else {
                    ""
                };
                println!("    [NOOP] {:?}{s} {}", scope, description, s = status);
            }
            plan::Action::Mkdir { path, scope } => {
                let status = if *scope == plan::Scope::User {
                    " (REFUSED)"
                } else {
                    ""
                };
                println!("    [MKDIR] {:?}{s} {}", scope, path, s = status);
            }
            plan::Action::BackupFile { path, scope } => {
                let status = if *scope == plan::Scope::User {
                    " (REFUSED)"
                } else {
                    ""
                };
                println!("    [BACKUP] {:?}{s} {}", scope, path, s = status);
            }
            plan::Action::MergeJson { path, patch, scope } => {
                let status = if *scope == plan::Scope::User {
                    " (REFUSED)"
                } else {
                    ""
                };

                // Scan for secrets in the patch
                let patch_content = serde_json::to_vec(patch).unwrap_or_default();
                let findings = security::scan_bytes(path, &patch_content);
                for finding in &findings {
                    println!(
                        "    [SECURITY {:?}] {} - {} ({})",
                        finding.severity, path, finding.pattern_name, finding.redacted_match
                    );
                }

                let full_path = paths.root.join(path);
                let existing = plan::read_existing(&full_path);

                let mut base = if existing.exists {
                    serde_json::from_slice(existing.bytes.as_deref().unwrap_or(&[]))
                        .unwrap_or(serde_json::Value::Object(serde_json::Map::new()))
                } else {
                    serde_json::Value::Object(serde_json::Map::new())
                };

                plan::deep_merge(&mut base, patch);
                let content = serde_json::to_vec_pretty(&base).unwrap_or_default();

                let diff = plan::generate_unified_diff(
                    path,
                    existing.bytes.as_deref(),
                    content.as_slice(),
                );

                if diff.is_empty() {
                    println!(
                        "    [MERGE] {:?}{s} {} (no changes)",
                        scope,
                        path,
                        s = status
                    );
                } else {
                    let indented_diff = diff
                        .lines()
                        .map(|line| format!("      {}", line))
                        .collect::<Vec<_>>()
                        .join("\n");
                    println!(
                        "    [MERGE] {:?}{s} {}\n{}",
                        scope,
                        path,
                        indented_diff,
                        s = status
                    );
                }
            }
            plan::Action::WriteFile {
                path,
                scope,
                content,
            } => {
                // Scan for secrets (for preview, we show warnings but continue unless Error)
                let findings = security::scan_bytes(path, content);
                for finding in &findings {
                    println!(
                        "    [SECURITY {:?}] {} - {} ({})",
                        finding.severity, path, finding.pattern_name, finding.redacted_match
                    );
                }

                // Placeholder validation for sensitive files
                if is_sensitive_file(path) {
                    let content_str = std::str::from_utf8(content).unwrap_or("");
                    if !security::contains_placeholder(content_str) {
                        println!(
                            "    [SECURITY WARNING] {} - Missing placeholders in sensitive file",
                            path
                        );
                    }
                }

                let full_path = paths.root.join(path);
                let existing = plan::read_existing(&full_path);

                let is_text = plan::is_text_file(path, content);

                if is_text {
                    let diff =
                        plan::generate_unified_diff(path, existing.bytes.as_deref(), content);
                    let status = if *scope == plan::Scope::User {
                        " (REFUSED)"
                    } else {
                        ""
                    };
                    if diff.is_empty() {
                        println!(
                            "    [WRITE] {:?}{s} {} (no changes)",
                            scope,
                            path,
                            s = status
                        );
                    } else {
                        // Add indentation to diff
                        let indented_diff = diff
                            .lines()
                            .map(|line| format!("      {}", line))
                            .collect::<Vec<_>>()
                            .join("\n");
                        println!(
                            "    [WRITE] {:?}{s} {}\n{}",
                            scope,
                            path,
                            indented_diff,
                            s = status
                        );
                    }
                } else {
                    let preview = format!("[BINARY {} bytes]", content.len());
                    let status = if *scope == plan::Scope::User {
                        " (REFUSED)"
                    } else {
                        ""
                    };
                    println!(
                        "    [WRITE] {:?}{s} {} ({})",
                        scope,
                        path,
                        preview,
                        s = status
                    );
                }
            }
            plan::Action::EnsureGitignore { pattern, scope } => {
                let status = if *scope == plan::Scope::User {
                    " (REFUSED)"
                } else {
                    ""
                };
                println!("    [GITIGNORE] {:?}{s} {}", scope, pattern, s = status);
            }
            plan::Action::SetExecutable { path, scope } => {
                let status = if *scope == plan::Scope::User {
                    " (REFUSED)"
                } else {
                    ""
                };
                println!("    [CHMOD +X] {:?}{s} {}", scope, path, s = status);
            }
        }
    }

    println!("\n{}", plan.render_summary(&paths.root));

    // Also call validate_plan to ensure we fail preview on errors
    validate_plan(plan, true)
}

pub fn apply(
    paths: &ProjectPaths,
    tools: Option<&str>,
    materialized_units: &[resolve::MaterializedFetchUnit],
    dry_run: bool,
    allow_user_scope: bool,
    registry: &ToolRegistry,
) -> Result<ApplyReport> {
    let canonical = load_canonical_config(&paths.config_path)?;
    let allowed_tools = registry.list_ids();
    let overrides = if let Some(tools_csv) = tools {
        CliOverrides::from_tools_csv(tools_csv, &allowed_tools)?
    } else {
        CliOverrides::default()
    };

    let resolved = resolve(&canonical, &overrides);

    if dry_run {
        println!(
            "Core: Dry-run apply (planning) in {} with tools: {:?}",
            paths.root.display(),
            resolved.tools.enabled
        );
        plan(paths, tools, materialized_units, registry)?;
        return Ok(ApplyReport::default());
    }

    println!(
        "Core: Applying in {} with tools: {:?}",
        paths.root.display(),
        resolved.tools.enabled
    );

    let mut total_plan = build_plan(paths, &resolved, materialized_units, registry)?;

    // Pre-flight check before any side effects
    validate_plan(&total_plan, allow_user_scope)?;

    apply_plan(paths, &mut total_plan, allow_user_scope)
}

pub fn apply_plan(
    paths: &ProjectPaths,
    plan: &mut plan::ActionPlan,
    allow_user_scope: bool,
) -> Result<ApplyReport> {
    plan.normalize();
    validate_plan(plan, allow_user_scope)?;

    let operations = plan::collect_plan_operations(paths, plan);
    apply_operations(paths, &operations, allow_user_scope, |_, _, _| {})
}

pub fn apply_operations<F>(
    paths: &ProjectPaths,
    operations: &[plan::PlannedOp],
    allow_user_scope: bool,
    mut on_progress: F,
) -> Result<ApplyReport>
where
    F: FnMut(&plan::PlannedOp, usize, usize),
{
    if operations.iter().any(|op| op.scope == plan::Scope::User) && !allow_user_scope {
        let first = operations
            .iter()
            .find(|op| op.scope == plan::Scope::User)
            .map(|op| op.path.clone())
            .unwrap_or_else(|| "<unknown>".to_string());
        return Err(MaccError::UserScopeNotAllowed(format!(
            "Planned operation for path '{}' is User scope",
            first
        )));
    }

    let timestamp = Local::now().format("%Y%m%d-%H%M%S").to_string();
    let total_ops = operations.len();
    let mut report = ApplyReport::new();
    let mut backup_created = false;
    let structured_merge_policy = StructuredToolMergePolicy::from_project(paths);
    let protected_context_paths = load_protected_context_paths(paths).unwrap_or_default();
    let needs_user_scope = operations.iter().any(|op| op.scope == plan::Scope::User);
    let user_backup_manager = if allow_user_scope && needs_user_scope {
        Some(UserBackupManager::try_new()?)
    } else {
        None
    };
    let mut user_backup_entries = Vec::new();
    let mut backed_user_paths = HashSet::new();

    for (idx, op) in operations.iter().enumerate() {
        on_progress(op, idx + 1, total_ops);
        let path = op.path.clone();
        let full_path = paths.root.join(&path);

        match op.kind {
            plan::PlannedOpKind::Mkdir => {
                if !full_path.exists() {
                    std::fs::create_dir_all(&full_path).map_err(|e| MaccError::Io {
                        path: full_path.to_string_lossy().into(),
                        action: "create directory".into(),
                        source: e,
                    })?;
                    report.outcomes.insert(path, plan::ActionStatus::Created);
                    if op.scope == plan::Scope::Project {
                        record_managed_path(paths, &op.path)?;
                    }
                } else {
                    report.outcomes.insert(path, plan::ActionStatus::Unchanged);
                }
            }
            plan::PlannedOpKind::Write | plan::PlannedOpKind::Merge => {
                let existing = plan::read_existing(&full_path);
                if protected_context_paths.contains(&path) && existing.exists {
                    report.outcomes.insert(path, plan::ActionStatus::Unchanged);
                    continue;
                }
                let status = if let Some(content) = &op.after {
                    let effective_content = if op.kind == plan::PlannedOpKind::Write {
                        structured_merge_policy.merge_bytes_for_path(
                            &path,
                            existing.bytes.as_deref(),
                            content,
                        )
                    } else {
                        content.clone()
                    };
                    let findings = security::scan_bytes(&path, &effective_content);
                    for finding in &findings {
                        if finding.severity == security::Severity::Warning {
                            println!(
                                "    [SECURITY WARNING] {} - {} ({})",
                                path, finding.pattern_name, finding.redacted_match
                            );
                        }
                    }

                    if is_sensitive_file(&path) {
                        let content_str = std::str::from_utf8(&effective_content).unwrap_or("");
                        if !security::contains_placeholder(content_str) {
                            println!(
                                "    [SECURITY WARNING] {} - Missing placeholders in sensitive file",
                                path
                            );
                        }
                    }

                    let status_guess =
                        plan::compute_write_status(&path, &effective_content, &existing);
                    let should_backup_project = status_guess != plan::ActionStatus::Unchanged
                        && existing.exists
                        && op.scope == plan::Scope::Project;

                    if should_backup_project && create_timestamped_backup(paths, &timestamp, &path)?
                    {
                        backup_created = true;
                    }

                    if op.scope == plan::Scope::User
                        && status_guess != plan::ActionStatus::Unchanged
                    {
                        if let Some(manager) = &user_backup_manager {
                            if backed_user_paths.insert(path.clone()) {
                                if let Some(entry) = manager.backup_file(&timestamp, &full_path)? {
                                    user_backup_entries.push(entry);
                                }
                            }
                        }
                    }

                    let status = write_if_changed_with_existing(
                        paths,
                        &path,
                        &full_path,
                        &effective_content,
                        &existing,
                        |_| Ok(()),
                    )?;

                    if op.metadata.set_executable && status != plan::ActionStatus::Noop {
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::PermissionsExt;
                            let metadata =
                                std::fs::metadata(&full_path).map_err(|e| MaccError::Io {
                                    path: full_path.to_string_lossy().into(),
                                    action: "get metadata for chmod".into(),
                                    source: e,
                                })?;
                            let mut perms = metadata.permissions();
                            perms.set_mode(perms.mode() | 0o111);
                            std::fs::set_permissions(&full_path, perms).map_err(|e| {
                                MaccError::Io {
                                    path: full_path.to_string_lossy().into(),
                                    action: "set executable permissions".into(),
                                    source: e,
                                }
                            })?;
                        }
                    }

                    status
                } else {
                    plan::ActionStatus::Noop
                };
                report.outcomes.insert(path, status);
                if status == plan::ActionStatus::Created && op.scope == plan::Scope::Project {
                    record_managed_path(paths, &op.path)?;
                }
            }
            plan::PlannedOpKind::Delete | plan::PlannedOpKind::Other => {
                report.outcomes.insert(path, plan::ActionStatus::Noop);
            }
        }
    }

    if backup_created {
        report.backup_dir = Some(paths.backups_dir.join(&timestamp));
    }

    if let Some(manager) = &user_backup_manager {
        if !user_backup_entries.is_empty() {
            report.user_backup_report = Some(manager.report(&timestamp, user_backup_entries));
        }
    }

    Ok(report)
}

fn load_protected_context_paths(paths: &ProjectPaths) -> Result<HashSet<String>> {
    let canonical = load_canonical_config(&paths.config_path)?;
    let loader = crate::tool::ToolSpecLoader::new(
        crate::tool::ToolSpecLoader::default_search_paths(&paths.root),
    );
    let (specs, _) = loader.load_all_with_embedded();
    let spec_by_id: BTreeMap<String, crate::tool::ToolSpec> = specs
        .into_iter()
        .map(|spec| (spec.id.clone(), spec))
        .collect();

    let mut protected = HashSet::new();
    for tool_id in &canonical.tools.enabled {
        let tool_cfg = canonical
            .tools
            .config
            .get(tool_id)
            .or_else(|| canonical.tools.settings.get(tool_id));
        if !context_protect_enabled(tool_cfg) {
            continue;
        }

        let mut files = tool_cfg
            .map(context_file_names_from_config)
            .unwrap_or_default();

        if files.is_empty() {
            if let Some(spec) = spec_by_id.get(tool_id) {
                files.extend(
                    spec.gitignore
                        .iter()
                        .filter(|entry| entry.to_ascii_lowercase().ends_with(".md"))
                        .cloned(),
                );
            }
        }

        if files.is_empty() {
            files.push(format!(
                "{}.md",
                tool_id.to_ascii_uppercase().replace('-', "_")
            ));
        }

        for file in files {
            if let Some(normalized) = normalize_relative_path(&file) {
                protected.insert(normalized);
            }
        }
    }
    Ok(protected)
}

fn context_protect_enabled(value: Option<&serde_json::Value>) -> bool {
    value
        .and_then(|cfg| cfg.pointer("/context/protect"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

fn context_file_names_from_config(value: &serde_json::Value) -> Vec<String> {
    let Some(file_name) = value.pointer("/context/fileName") else {
        return Vec::new();
    };
    match file_name {
        serde_json::Value::String(s) => vec![s.clone()],
        serde_json::Value::Array(items) => items
            .iter()
            .filter_map(|item| item.as_str().map(|s| s.to_string()))
            .collect(),
        _ => Vec::new(),
    }
}

pub fn clear(paths: &ProjectPaths) -> Result<ClearReport> {
    let mut managed = load_managed_paths(paths)?;

    if let Some(rel) = relative_to_root(paths, &paths.managed_paths_state_path()) {
        managed.insert(rel);
    }
    if let Some(parent) = paths.managed_paths_state_path().parent() {
        if let Some(rel) = relative_to_root(paths, parent) {
            managed.insert(rel);
        }
    }

    let mut entries: Vec<String> = managed.into_iter().collect();
    entries.sort_by(|a, b| {
        let da = a.split('/').count();
        let db = b.split('/').count();
        db.cmp(&da).then_with(|| b.cmp(a))
    });

    let mut report = ClearReport::default();
    for rel in entries {
        let full = paths.root.join(&rel);
        if !full.exists() {
            continue;
        }
        let result = if full.is_dir() {
            std::fs::remove_dir(&full)
        } else {
            std::fs::remove_file(&full)
        };
        match result {
            Ok(_) => report.removed += 1,
            Err(_) => report.skipped += 1,
        }
    }

    Ok(report)
}
pub const BASELINE_IGNORE_ENTRIES: &[&str] = &[".macc/"];

fn collect_tool_gitignore_entries(
    paths: &ProjectPaths,
    enabled_tools: Option<&[String]>,
) -> Vec<String> {
    let search_paths = crate::tool::ToolSpecLoader::default_search_paths(&paths.root);
    let loader = crate::tool::ToolSpecLoader::new(search_paths);
    let (specs, _) = loader.load_all_with_embedded();

    let enabled: Option<HashSet<&str>> =
        enabled_tools.map(|tools| tools.iter().map(|id| id.as_str()).collect());

    let mut seen = HashSet::new();
    let mut entries = Vec::new();

    for spec in specs {
        if let Some(ref enabled_set) = enabled {
            if !enabled_set.contains(spec.id.as_str()) {
                continue;
            }
        }
        for entry in spec.gitignore {
            if seen.insert(entry.clone()) {
                entries.push(entry);
            }
        }
    }

    entries
}

pub fn ensure_gitignore_entries(
    paths: &ProjectPaths,
    entries: &[&str],
    timestamp: Option<&str>,
) -> Result<plan::ActionStatus> {
    let gitignore_path = paths.root.join(".gitignore");
    let (exists, content) = if gitignore_path.exists() {
        let content = std::fs::read_to_string(&gitignore_path).map_err(|e| MaccError::Io {
            path: gitignore_path.to_string_lossy().into(),
            action: "read .gitignore".into(),
            source: e,
        })?;
        (true, content)
    } else {
        (false, String::new())
    };

    let mut lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();
    let mut changed = false;

    for entry in entries {
        if !lines.iter().any(|l| l.trim() == *entry) {
            lines.push(entry.to_string());
            changed = true;
        }
    }

    if !changed {
        return Ok(plan::ActionStatus::Unchanged);
    }

    // Backup if exists and timestamp provided
    if exists {
        if let Some(ts) = timestamp {
            create_timestamped_backup(paths, ts, ".gitignore")?;
        }
    }

    let mut new_content = lines.join("\n");
    if !new_content.is_empty() && !new_content.ends_with('\n') {
        new_content.push('\n');
    }

    let _ = write_if_changed(
        paths,
        ".gitignore",
        &gitignore_path,
        new_content.as_bytes(),
        |existing| {
            if existing.exists {
                if let Some(ts) = timestamp {
                    create_timestamped_backup(paths, ts, ".gitignore")?;
                }
            }
            Ok(())
        },
    )?;

    Ok(if exists {
        plan::ActionStatus::Updated
    } else {
        plan::ActionStatus::Created
    })
}

fn is_sensitive_file(path: &str) -> bool {
    let lower = path.to_lowercase();
    lower.contains("secret")
        || lower.contains("key")
        || lower.contains("token")
        || lower.ends_with(".env")
        || lower.ends_with(".example")
        || lower.contains("config")
        || lower.contains("settings")
}

pub fn write_if_changed<F>(
    paths: &ProjectPaths,
    logical_path: &str,
    target_path: &Path,
    content: &[u8],
    before_write: F,
) -> Result<plan::ActionStatus>
where
    F: FnMut(&plan::ExistingFile) -> Result<()>,
{
    let existing = plan::read_existing(target_path);
    write_if_changed_with_existing(
        paths,
        logical_path,
        target_path,
        content,
        &existing,
        before_write,
    )
}

pub fn write_if_changed_with_existing<F>(
    paths: &ProjectPaths,
    logical_path: &str,
    target_path: &Path,
    content: &[u8],
    existing: &plan::ExistingFile,
    mut before_write: F,
) -> Result<plan::ActionStatus>
where
    F: FnMut(&plan::ExistingFile) -> Result<()>,
{
    let status = plan::compute_write_status(logical_path, content, existing);
    if status == plan::ActionStatus::Unchanged {
        return Ok(status);
    }

    before_write(existing)?;
    atomic_write(paths, target_path, content)?;

    Ok(status)
}

pub fn atomic_write(_paths: &ProjectPaths, target_path: &Path, content: &[u8]) -> Result<()> {
    let parent = target_path.parent().unwrap_or_else(|| Path::new(""));
    if !parent.as_os_str().is_empty() && !parent.exists() {
        std::fs::create_dir_all(parent).map_err(|e| MaccError::Io {
            path: parent.to_string_lossy().into(),
            action: "create target parent directory".into(),
            source: e,
        })?;
    }

    // Generate a temporary filename in the target directory for atomic replace.
    let now = Local::now();
    let tmp_filename = format!(
        ".macc-{}-{:09}.tmp",
        now.format("%Y%m%d-%H%M%S"),
        now.timestamp_nanos_opt().unwrap_or(0) % 1_000_000_000
    );
    let tmp_path = parent.join(tmp_filename);

    // Write to the temporary file first.
    std::fs::write(&tmp_path, content).map_err(|e| MaccError::Io {
        path: tmp_path.to_string_lossy().into(),
        action: "write temporary file".into(),
        source: e,
    })?;

    // Atomically rename the temporary file to the target path.
    std::fs::rename(&tmp_path, target_path).map_err(|e| {
        // Cleanup tmp file on rename failure if it still exists
        let _ = std::fs::remove_file(&tmp_path);
        MaccError::Io {
            path: target_path.to_string_lossy().into(),
            action: format!("rename from {}", tmp_path.display()),
            source: e,
        }
    })?;

    Ok(())
}

fn create_timestamped_backup(
    paths: &ProjectPaths,
    timestamp: &str,
    relative_path: &str,
) -> Result<bool> {
    let full_source_path = paths.root.join(relative_path);
    if !full_source_path.exists() {
        return Ok(false);
    }

    let backup_path = paths.backups_dir.join(timestamp).join(relative_path);

    if let Some(parent) = backup_path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent).map_err(|e| MaccError::Io {
                path: parent.to_string_lossy().into(),
                action: "create backup directory".into(),
                source: e,
            })?;
        }
    }

    std::fs::copy(&full_source_path, &backup_path).map_err(|e| MaccError::Io {
        path: full_source_path.to_string_lossy().into(),
        action: format!("copy to backup {}", backup_path.display()),
        source: e,
    })?;

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::Duration;

    #[test]
    fn test_version() {
        assert!(!version().is_empty());
    }

    #[test]
    fn test_find_project_root() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("macc_test_{}", uuid_v4_like()));
        let project_root = temp_dir.join("my_project");
        let nested_dir = project_root.join("a/b/c/d/e");

        fs::create_dir_all(&nested_dir).map_err(|e| MaccError::Io {
            path: nested_dir.to_string_lossy().into(),
            action: "create_dir_all".into(),
            source: e,
        })?;

        let macc_dir = project_root.join(".macc");
        fs::create_dir(&macc_dir).map_err(|e| MaccError::Io {
            path: macc_dir.to_string_lossy().into(),
            action: "create_dir".into(),
            source: e,
        })?;

        fs::write(macc_dir.join("macc.yaml"), "").map_err(|e| MaccError::Io {
            path: "macc.yaml".into(),
            action: "write".into(),
            source: e,
        })?;

        // Test discovery from deep nested directory
        let paths = find_project_root(&nested_dir)?;
        assert_eq!(paths.root, project_root.canonicalize().unwrap());

        // Test discovery from a file
        let file_path = nested_dir.join("main.rs");
        fs::write(&file_path, "// test").unwrap();
        let paths = find_project_root(&file_path)?;
        assert_eq!(paths.root, project_root.canonicalize().unwrap());

        // Cleanup
        fs::remove_dir_all(&temp_dir).ok();

        Ok(())
    }

    #[test]
    fn test_catalog_paths() -> Result<()> {
        let root = PathBuf::from("/tmp/project");
        let paths = ProjectPaths::from_root(&root);
        assert!(paths.catalog_dir.ends_with("catalog"));
        assert!(paths
            .skills_catalog_path()
            .ends_with("catalog/skills.catalog.json"));
        assert!(paths
            .mcp_catalog_path()
            .ends_with("catalog/mcp.catalog.json"));

        // Test creation in init
        let temp_dir = std::env::temp_dir().join(format!("macc_catalog_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);

        init(&paths, false)?;
        assert!(paths.catalog_dir.exists());
        assert!(paths.skills_catalog_path().exists());
        assert!(paths.mcp_catalog_path().exists());

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_project_paths_anchoring() {
        let root = PathBuf::from("/tmp/project");
        let paths = ProjectPaths::from_root(&root);

        assert_eq!(paths.root, root);
        assert_eq!(paths.macc_dir, root.join(".macc"));
        assert_eq!(paths.config_path, root.join(".macc/macc.yaml"));
        assert_eq!(paths.backups_dir, root.join(".macc/backups"));
        assert_eq!(paths.tmp_dir, root.join(".macc/tmp"));
        assert_eq!(paths.cache_dir, root.join(".macc/cache"));
    }

    #[test]
    fn test_source_cache_path() {
        let root = PathBuf::from("/tmp/project");
        let paths = ProjectPaths::from_root(&root);
        let key = "abcdef123";
        assert_eq!(
            paths.source_cache_path(key),
            root.join(".macc/cache/abcdef123")
        );
    }

    #[test]
    fn test_find_project_root_not_found() {
        let temp_dir = std::env::temp_dir().join(format!("macc_test_not_found_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();

        let result = find_project_root(&temp_dir);
        assert!(matches!(result, Err(MaccError::ProjectRootNotFound { .. })));

        fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_apply_with_backups() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("macc_backup_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();

        let paths = ProjectPaths::from_root(&temp_dir);
        init(&paths, false)?;

        // 1. Create an existing file that will be overwritten by TestAdapter
        let target_file = temp_dir.join("MACC_GENERATED.txt");
        let original_content = b"Original content";
        fs::write(&target_file, original_content).map_err(|e| MaccError::Io {
            path: target_file.to_string_lossy().into(),
            action: "write initial file".into(),
            source: e,
        })?;

        // 2. Run apply with 'test' tool enabled
        apply(
            &paths,
            Some("test"),
            &[],
            false,
            false,
            &ToolRegistry::default_registry(),
        )?;

        // 3. Verify file was overwritten
        let new_content = fs::read(&target_file).unwrap();
        assert_ne!(new_content, original_content);

        // 4. Verify backup exists
        let backups = fs::read_dir(&paths.backups_dir).unwrap();
        let mut backup_found = false;
        for entry in backups {
            let entry = entry.unwrap();
            if !entry.file_type().unwrap().is_dir() {
                continue;
            }
            let backup_file = entry.path().join("MACC_GENERATED.txt");
            if backup_file.exists() {
                let backed_up_content = fs::read(backup_file).unwrap();
                assert_eq!(backed_up_content, original_content);
                backup_found = true;
                break;
            }
        }
        assert!(backup_found, "Backup of MACC_GENERATED.txt not found");

        // Cleanup
        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_apply_backup_idempotence() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("macc_backup_idem_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();

        let paths = ProjectPaths::from_root(&temp_dir);
        init(&paths, false)?;

        // 1. Create a file with same content as TestAdapter produces
        let target_file = temp_dir.join("MACC_GENERATED.txt");
        let content =
            "This is a test file generated by MACC.\nDeterministic content for verification.\n"
                .as_bytes();
        fs::write(&target_file, content).unwrap();

        // 2. Run apply
        apply(
            &paths,
            Some("test"),
            &[],
            false,
            false,
            &ToolRegistry::default_registry(),
        )?;

        // 3. Verify NO backup was created because content matched
        if paths.backups_dir.exists() {
            let backups = fs::read_dir(&paths.backups_dir).unwrap();
            for entry in backups {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    let backup_file = entry.path().join("MACC_GENERATED.txt");
                    assert!(
                        !backup_file.exists(),
                        "Backup should NOT have been created for unchanged file"
                    );
                }
            }
        }

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_apply_idempotence_normalization() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("macc_idem_norm_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();

        let paths = ProjectPaths::from_root(&temp_dir);
        init(&paths, false)?;

        // 1. Create a JSON file with specific formatting
        let target_file = temp_dir.join("test.json");
        let initial_json = br#"{"a":1,"b":2}"#;
        fs::write(&target_file, initial_json).unwrap();

        // 2. Mock an action that writes the same JSON but with different formatting
        // We use a custom registry with a mock adapter for this test
        let mut plan = plan::ActionPlan::new();
        let reordered_json = br#"{
  "b": 2,
  "a": 1
}"#;
        plan.add_action(plan::Action::WriteFile {
            path: "test.json".to_string(),
            content: reordered_json.to_vec(),
            scope: plan::Scope::Project,
        });

        // We can't easily inject a plan into apply() yet without making it public or passing it.
        // But apply() calls adapter.plan().
        // For this test, let's just use the fact that compute_write_status is used in apply.
        // And we'll verify it by checking file modification time.

        // Since we can't easily mock the tool adapter in apply() without more refactoring,
        // let's verify compute_write_status directly again here to be sure,
        // and then we'll rely on the existing apply logic which we just updated.

        let existing = plan::read_existing(&target_file);
        assert_eq!(
            plan::compute_write_status("test.json", reordered_json, &existing),
            plan::ActionStatus::Unchanged
        );

        // To really test apply, we would need to mock the adapter.
        // Let's see if we can use the "test" adapter if it produces something we can control.
        // The "test" adapter is hardcoded in ToolRegistry::default_registry().

        // Actually, let's just run a second apply with the same tool and see if it's idempotent.
        // First apply will create files.
        apply(
            &paths,
            Some("test"),
            &[],
            false,
            false,
            &ToolRegistry::default_registry(),
        )?;
        let target_generated = temp_dir.join("MACC_GENERATED.txt");
        let mtime1 = fs::metadata(&target_generated).unwrap().modified().unwrap();

        // Wait a bit to ensure mtime would change if written
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Second apply
        apply(
            &paths,
            Some("test"),
            &[],
            false,
            false,
            &ToolRegistry::default_registry(),
        )?;
        let mtime2 = fs::metadata(&target_generated).unwrap().modified().unwrap();

        assert_eq!(
            mtime1, mtime2,
            "File should NOT have been rewritten in second apply"
        );

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_create_timestamped_backup_nested() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("macc_backup_nest_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);
        init(&paths, false)?;

        let relative_path = "a/b/c/file.txt";
        let full_path = temp_dir.join(relative_path);
        fs::create_dir_all(full_path.parent().unwrap()).unwrap();
        fs::write(&full_path, "original").unwrap();

        let timestamp = "20260129-200000";
        let result = create_timestamped_backup(&paths, timestamp, relative_path)?;
        assert!(result);

        let backup_path = paths.backups_dir.join(timestamp).join(relative_path);
        assert!(backup_path.exists());
        assert_eq!(fs::read_to_string(backup_path).unwrap(), "original");

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_atomic_write() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("macc_atomic_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);

        let target_file = temp_dir.join("sub/dir/atomic.txt");
        let content = b"atomic content";

        atomic_write(&paths, &target_file, content)?;

        // Verify content
        assert_eq!(fs::read(&target_file).unwrap(), content);

        // Verify no temp files linger in the target directory
        let parent_dir = target_file.parent().unwrap();
        let entries = fs::read_dir(parent_dir).unwrap();
        for entry in entries {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            assert!(!name.starts_with(".macc-") || !name.ends_with(".tmp"));
        }

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_write_if_changed_skips_unchanged() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("macc_write_skip_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);

        let target_file = temp_dir.join("unchanged.txt");
        fs::write(&target_file, "same content").unwrap();
        let before = fs::metadata(&target_file).unwrap().modified().unwrap();

        std::thread::sleep(Duration::from_millis(1100));

        let status = write_if_changed(
            &paths,
            "unchanged.txt",
            &target_file,
            b"same content",
            |_| Ok(()),
        )?;
        assert_eq!(status, plan::ActionStatus::Unchanged);

        let after = fs::metadata(&target_file).unwrap().modified().unwrap();
        assert_eq!(before, after, "Unchanged write should not touch the file");

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_write_if_changed_rewrites_on_change() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("macc_write_change_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);

        let target_file = temp_dir.join("changed.txt");
        fs::write(&target_file, "old content").unwrap();
        let before = fs::metadata(&target_file).unwrap().modified().unwrap();

        std::thread::sleep(Duration::from_millis(1100));

        let status = write_if_changed(&paths, "changed.txt", &target_file, b"new content", |_| {
            Ok(())
        })?;
        assert_eq!(status, plan::ActionStatus::Updated);
        assert_eq!(fs::read(&target_file).unwrap(), b"new content");

        let after = fs::metadata(&target_file).unwrap().modified().unwrap();
        assert!(
            after > before,
            "Changed write should update the file timestamp"
        );

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_apply_plan_ordering() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("macc_order_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);
        init(&paths, false)?;

        let mut plan = plan::ActionPlan::new();
        // Add actions in "wrong" order
        plan.add_action(plan::Action::EnsureGitignore {
            pattern: "*.log".into(),
            scope: plan::Scope::Project,
        });
        plan.add_action(plan::Action::WriteFile {
            path: "nested/file.txt".into(),
            content: b"hello".to_vec(),
            scope: plan::Scope::Project,
        });
        plan.add_action(plan::Action::Mkdir {
            path: "nested".into(),
            scope: plan::Scope::Project,
        });
        // Duplicate Mkdir
        plan.add_action(plan::Action::Mkdir {
            path: "nested".into(),
            scope: plan::Scope::Project,
        });

        let report = apply_plan(&paths, &mut plan, false)?;

        // Mkdir (deduped) + WriteFile + EnsureGitignore = 3 actions
        assert_eq!(report.outcomes.len(), 3);
        assert_eq!(
            report.outcomes.get("nested").unwrap(),
            &plan::ActionStatus::Created
        );
        assert_eq!(
            report.outcomes.get("nested/file.txt").unwrap(),
            &plan::ActionStatus::Created
        );
        assert_eq!(
            report.outcomes.get(".gitignore").unwrap(),
            &plan::ActionStatus::Updated
        );

        assert!(temp_dir.join("nested").is_dir());
        assert_eq!(
            fs::read_to_string(temp_dir.join("nested/file.txt")).unwrap(),
            "hello"
        );
        let gitignore_content = fs::read_to_string(temp_dir.join(".gitignore")).unwrap();
        assert!(gitignore_content.contains(".macc/"));
        assert!(gitignore_content.contains("*.log"));

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_apply_respects_context_protect_flag_for_existing_file() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("macc_ctx_protect_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);
        init(&paths, false)?;

        let mut cfg = load_canonical_config(&paths.config_path)?;
        cfg.tools.enabled = vec!["test".to_string()];
        cfg.tools.config.insert(
            "test".to_string(),
            serde_json::json!({
                "context": {
                    "protect": true,
                    "fileName": "AGENTS.md"
                }
            }),
        );
        let mut yaml = cfg
            .to_yaml()
            .map_err(|e| MaccError::Validation(e.to_string()))?;
        yaml.push('\n');
        atomic_write(&paths, &paths.config_path, yaml.as_bytes())?;

        let target = temp_dir.join("AGENTS.md");
        fs::write(&target, "manual content\n").unwrap();

        let mut plan = plan::ActionPlan::new();
        plan.add_action(plan::Action::WriteFile {
            path: "AGENTS.md".into(),
            content: b"generated content\n".to_vec(),
            scope: plan::Scope::Project,
        });

        let report = apply_plan(&paths, &mut plan, false)?;
        assert_eq!(
            report.outcomes.get("AGENTS.md"),
            Some(&plan::ActionStatus::Unchanged)
        );
        assert_eq!(fs::read_to_string(&target).unwrap(), "manual content\n");

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_ensure_gitignore() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("macc_git_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);

        // 1. Missing .gitignore - should be created with baseline
        let status = ensure_gitignore_entries(&paths, BASELINE_IGNORE_ENTRIES, None)?;
        assert_eq!(status, plan::ActionStatus::Created);
        let content = fs::read_to_string(temp_dir.join(".gitignore")).unwrap();
        for entry in BASELINE_IGNORE_ENTRIES {
            assert!(content.contains(entry));
        }

        // 2. Already exists and matching - should be unchanged
        let status = ensure_gitignore_entries(&paths, BASELINE_IGNORE_ENTRIES, None)?;
        assert_eq!(status, plan::ActionStatus::Unchanged);

        // 3. Exists but missing some - should be updated
        fs::write(temp_dir.join(".gitignore"), "existing_entry\n").unwrap();
        let status = ensure_gitignore_entries(&paths, BASELINE_IGNORE_ENTRIES, None)?;
        assert_eq!(status, plan::ActionStatus::Updated);
        let content = fs::read_to_string(temp_dir.join(".gitignore")).unwrap();
        assert!(content.contains("existing_entry"));
        for entry in BASELINE_IGNORE_ENTRIES {
            assert!(content.contains(entry));
        }

        // 4. Test with backup
        fs::create_dir_all(&paths.backups_dir).unwrap();
        let timestamp = "20260129-220000";
        let status = ensure_gitignore_entries(&paths, &["new_entry"], Some(timestamp))?;
        assert_eq!(status, plan::ActionStatus::Updated);
        assert!(paths
            .backups_dir
            .join(timestamp)
            .join(".gitignore")
            .exists());

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_init_ensures_gitignore() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("macc_init_git_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);

        init(&paths, false)?;
        assert!(temp_dir.join(".gitignore").exists());
        let content = fs::read_to_string(temp_dir.join(".gitignore")).unwrap();
        for entry in BASELINE_IGNORE_ENTRIES {
            assert!(content.contains(entry));
        }

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_init_sets_context_protect_defaults() -> Result<()> {
        let temp_dir =
            std::env::temp_dir().join(format!("macc_init_ctx_defaults_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);

        init(&paths, false)?;
        let cfg = load_canonical_config(&paths.config_path)?;
        assert!(
            !cfg.tools.config.is_empty(),
            "expected tool config defaults to be populated"
        );
        let any_protected = cfg.tools.config.values().any(|tool_cfg| {
            tool_cfg
                .pointer("/context/protect")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        });
        assert!(
            any_protected,
            "expected at least one tool to have context.protect=true by default"
        );

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_apply_aborts_on_secret() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("macc_secret_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);
        init(&paths, false)?;

        let mut plan = plan::ActionPlan::new();
        plan.add_action(plan::Action::WriteFile {
            path: "leaked.txt".into(),
            content: b"My key is AKIA1234567890123456".to_vec(),
            scope: plan::Scope::Project,
        });

        let result = apply_plan(&paths, &mut plan, false);
        assert!(result.is_err());
        if let Err(MaccError::SecretDetected { path, details }) = result {
            assert_eq!(path, "leaked.txt");
            assert!(details.contains("AWS Access Key"));
            assert!(details.contains("AKIA...3456"));
            // Ensure full secret is NOT in error message
            assert!(!details.contains("123456789012"));
        } else {
            panic!("Expected SecretDetected error, got {:?}", result);
        }

        assert!(!temp_dir.join("leaked.txt").exists());

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_validate_plan_detects_secret_in_merge_json() -> Result<()> {
        let mut plan = plan::ActionPlan::new();
        plan.add_action(plan::Action::MergeJson {
            path: "config.json".into(),
            patch: serde_json::json!({
                "api_key": "AKIA1234567890123456"
            }),
            scope: plan::Scope::Project,
        });

        let result = validate_plan(&plan, false);
        assert!(result.is_err());
        if let Err(MaccError::SecretDetected { path, details }) = result {
            assert_eq!(path, "config.json");
            assert!(details.contains("AWS Access Key"));
        } else {
            panic!("Expected SecretDetected error, got {:?}", result);
        }
        Ok(())
    }

    #[test]
    fn test_is_sensitive_file() {
        assert!(is_sensitive_file(".env"));
        assert!(is_sensitive_file("config.json"));
        assert!(is_sensitive_file("settings.yaml"));
        assert!(is_sensitive_file("my_secret_token.txt"));
        assert!(is_sensitive_file("api_key.example"));
        assert!(!is_sensitive_file("README.md"));
        assert!(!is_sensitive_file("src/main.rs"));
    }

    #[test]
    fn test_apply_plan_refuses_user_scope() -> Result<()> {
        let temp_dir =
            std::env::temp_dir().join(format!("macc_user_scope_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);

        let mut plan = plan::ActionPlan::new();
        plan.add_action(plan::Action::Mkdir {
            path: "user_dir".into(),
            scope: plan::Scope::User,
        });

        let result = apply_plan(&paths, &mut plan, false);
        assert!(result.is_err());
        if let Err(MaccError::UserScopeNotAllowed(msg)) = result {
            assert!(msg.contains("user_dir"));
            assert!(msg.contains("User scope"));
        } else {
            panic!("Expected UserScopeNotAllowed error, got {:?}", result);
        }

        assert!(!temp_dir.join("user_dir").exists());

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_plan_no_backups_or_writes() -> Result<()> {
        let temp_dir =
            std::env::temp_dir().join(format!("macc_plan_safety_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);
        init(&paths, false)?;

        // 1. Create an existing file that 'test' tool would overwrite
        let target_file = temp_dir.join("MACC_GENERATED.txt");
        fs::write(&target_file, "original").unwrap();

        // 2. Clear backups created by init if any (though there shouldn't be)
        if paths.backups_dir.exists() {
            fs::remove_dir_all(&paths.backups_dir).unwrap();
            fs::create_dir_all(&paths.backups_dir).unwrap();
        }

        // 3. Run plan
        plan(&paths, Some("test"), &[], &ToolRegistry::default_registry())?;

        // 4. Verify file is UNCHANGED
        assert_eq!(fs::read_to_string(&target_file).unwrap(), "original");

        // 5. Verify NO backups created
        if paths.backups_dir.exists() {
            let backups = fs::read_dir(&paths.backups_dir).unwrap();
            for entry in backups {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    let sub_entries = fs::read_dir(entry.path()).unwrap();
                    assert_eq!(
                        sub_entries.count(),
                        0,
                        "Plan should not create any backup files"
                    );
                }
            }
        }

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_clear_removes_only_tracked_paths() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("macc_clear_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);
        init(&paths, false)?;

        let preexisting = temp_dir.join("CLAUDE.md");
        fs::write(&preexisting, "user file").unwrap();

        let generated = temp_dir.join("generated.txt");
        fs::write(&generated, "from macc").unwrap();
        record_managed_path(&paths, "generated.txt")?;

        let report = clear(&paths)?;
        assert!(report.removed >= 1);
        assert!(!generated.exists());
        assert!(preexisting.exists());

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    fn uuid_v4_like() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        format!("{:?}", since_the_epoch.as_nanos())
    }
}
