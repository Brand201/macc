use async_trait::async_trait;
use clap::{Parser, Subcommand};
use commands::Command;
use macc_core::coordinator::{
    engine as coordinator_engine, is_valid_runtime_transition, is_valid_workflow_transition,
    runtime as coordinator_runtime, runtime_status_from_event,
};
#[cfg(test)]
use macc_core::coordinator::{RuntimeStatus, WorkflowState};
use macc_core::coordinator_storage::{
    append_event_sqlite, coordinator_storage_bootstrap_sqlite_from_json,
    coordinator_storage_export_sqlite_to_json, coordinator_storage_import_json_to_sqlite,
    coordinator_storage_verify_parity, CoordinatorStorageMode, CoordinatorStorageTransfer,
};
use macc_core::engine::{Engine, MaccEngine};
use macc_core::resolve::{resolve, resolve_fetch_units, CliOverrides};
use macc_core::tool::{ToolPerformerSpec, ToolSpec, ToolSpecLoader};
use macc_core::{load_canonical_config, MaccError, Result};
use std::collections::BTreeMap;
use std::process::exit;

mod commands;
mod coordinator;
mod services;

use crate::coordinator::helpers::now_iso_coordinator;
use crate::coordinator::types::CoordinatorEnvConfig;
use crate::coordinator::args::{
    parse_coordinator_extra_kv_args, RuntimeStatusFromEventArgs, RuntimeTransitionArgs,
    StorageSyncArgs, WorkflowTransitionArgs,
};
use crate::coordinator::state_runtime::{
    cleanup_dead_runtime_tasks, coordinator_pause_file_path, resume_paused_task_integrate,
    set_task_paused_for_integrate, write_coordinator_pause_file,
};
#[cfg(test)]
use crate::coordinator::state_runtime::{cleanup_registry_native, reconcile_registry_native};

#[derive(Parser)]
#[command(name = "macc")]
#[command(about = "MACC (Multi-Agentic Coding Config)", long_about = None)]
#[command(version)]
struct Cli {
    /// Working directory
    #[arg(short, long, global = true, default_value = ".")]
    cwd: String,

    /// Enable verbose output
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize MACC in a project
    Init {
        /// Force initialization even if already initialized
        #[arg(short, long)]
        force: bool,
        /// Run interactive setup wizard (3 questions)
        #[arg(long)]
        wizard: bool,
    },
    /// Zero-friction setup: check environment, init, then open TUI or run plan+apply
    Quickstart {
        /// Auto-confirm interactive prompts
        #[arg(short = 'y', long)]
        yes: bool,
        /// Run plan first, then apply after confirmation
        #[arg(long)]
        apply: bool,
        /// Do not open TUI at the end
        #[arg(long)]
        no_tui: bool,
    },
    /// Plan changes to the project
    Plan {
        /// CSV list of tools to use
        #[arg(short, long)]
        tools: Option<String>,
        /// Output machine-readable JSON (for CI/logging)
        #[arg(long)]
        json: bool,
        /// Explain why each file operation exists
        #[arg(long)]
        explain: bool,
    },
    /// Apply configuration to the project
    Apply {
        /// CSV list of tools to use
        #[arg(short, long)]
        tools: Option<String>,

        /// Run in dry-run mode (same as plan)
        #[arg(long)]
        dry_run: bool,

        /// Allow user-scope operations (requires explicit consent)
        #[arg(long)]
        allow_user_scope: bool,
        /// Output machine-readable JSON for dry-run preview
        #[arg(long)]
        json: bool,
        /// Explain why each file operation exists in preview
        #[arg(long)]
        explain: bool,
    },
    /// Catalog management
    Catalog {
        #[command(subcommand)]
        catalog_command: CatalogCommands,
    },
    /// Install items directly from catalog
    Install {
        #[command(subcommand)]
        install_command: InstallCommands,
    },
    /// Open the interactive TUI
    Tui,
    /// Tool management
    Tool {
        #[command(subcommand)]
        tool_command: ToolCommands,
    },
    /// Ask AI tools to update their context files directly in the repo
    Context {
        /// Generate context for a single tool ID
        #[arg(long)]
        tool: Option<String>,
        /// Additional source files to include in the prompt context
        #[arg(long = "from")]
        from_files: Vec<String>,
        /// Preview only; do not run tool commands
        #[arg(long)]
        dry_run: bool,
        /// Print generated prompt(s)
        #[arg(long)]
        print_prompt: bool,
    },
    /// Run diagnostic checks for the environment and supported tools
    Doctor {
        /// Apply safe automatic fixes
        #[arg(long)]
        fix: bool,
    },
    /// Migrate legacy configuration to the new format
    Migrate {
        /// Actually write the migrated config to disk
        #[arg(short, long)]
        apply: bool,
    },
    /// Backup set utilities
    Backups {
        #[command(subcommand)]
        backups_command: BackupsCommands,
    },
    /// Restore files from backup sets
    Restore {
        /// Restore the most recent backup set
        #[arg(long)]
        latest: bool,
        /// Use user-level backup root (~/.macc/backups) instead of project backup root
        #[arg(long)]
        user: bool,
        /// Restore from an explicit backup set name (timestamp folder)
        #[arg(long)]
        backup: Option<String>,
        /// Show what would be restored without writing files
        #[arg(long)]
        dry_run: bool,
        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
    },
    /// Remove files/directories created by MACC in this project
    Clear,
    /// Worktree utilities
    Worktree {
        #[command(subcommand)]
        worktree_command: WorktreeCommands,
    },
    /// View coordinator/performer logs
    Logs {
        #[command(subcommand)]
        logs_command: LogsCommands,
    },
    /// Run the project coordinator automation script
    Coordinator {
        /// Coordinator action (run, control-plane-run, dispatch, advance, resume, sync, status, reconcile, unlock, cleanup, retry-phase, cutover-gate, stop, validate-transition, validate-runtime-transition, runtime-status-from-event, storage-import, storage-export, events-export, storage-verify, storage-sync, select-ready-task, state-apply-transition, state-set-runtime, state-task-field, state-task-exists, state-counts, state-locks, state-set-merge-pending, state-set-merge-processed, state-increment-retries, state-upsert-slo-warning, state-slo-metric)
        #[arg(default_value = "run")]
        action: String,
        /// Disable TUI live view for `macc coordinator run`
        #[arg(long)]
        no_tui: bool,
        /// Graceful stop (SIGTERM only, no SIGKILL escalation)
        #[arg(long)]
        graceful: bool,
        /// When action=stop, remove all project worktrees after coordinator shutdown
        #[arg(long)]
        remove_worktrees: bool,
        /// When action=stop and --remove-worktrees is set, also delete associated branches
        #[arg(long)]
        remove_branches: bool,
        /// Override PRD file path
        #[arg(long)]
        prd: Option<String>,
        /// Fixed tool for coordinator phase hooks (review/fix/integrate)
        #[arg(long)]
        coordinator_tool: Option<String>,
        /// Default reference/base branch when task.base_branch is not provided
        #[arg(long)]
        reference_branch: Option<String>,
        /// Tool priority order (comma-separated, e.g. tool-a,tool-b,tool-c)
        #[arg(long)]
        tool_priority: Option<String>,
        /// Per-tool concurrency cap JSON (e.g. {"tool-a":3,"tool-b":2})
        #[arg(long)]
        max_parallel_per_tool_json: Option<String>,
        /// Category->tools routing JSON (e.g. {"frontend":["tool-b","tool-c"]})
        #[arg(long)]
        tool_specializations_json: Option<String>,
        /// Override MAX_DISPATCH
        #[arg(long)]
        max_dispatch: Option<usize>,
        /// Override MAX_PARALLEL
        #[arg(long)]
        max_parallel: Option<usize>,
        /// Override TIMEOUT_SECONDS
        #[arg(long)]
        timeout_seconds: Option<usize>,
        /// Override PHASE_RUNNER_MAX_ATTEMPTS
        #[arg(long)]
        phase_runner_max_attempts: Option<usize>,
        /// Flush coordinator log buffer every N lines (default: 32)
        #[arg(long)]
        log_flush_lines: Option<usize>,
        /// Flush coordinator log buffer every N milliseconds (default: 1000)
        #[arg(long)]
        log_flush_ms: Option<u64>,
        /// Override STALE_CLAIMED_SECONDS
        #[arg(long)]
        stale_claimed_seconds: Option<usize>,
        /// Override STALE_IN_PROGRESS_SECONDS
        #[arg(long)]
        stale_in_progress_seconds: Option<usize>,
        /// Override STALE_CHANGES_REQUESTED_SECONDS
        #[arg(long)]
        stale_changes_requested_seconds: Option<usize>,
        /// Override STALE_ACTION (abandon, todo, blocked)
        #[arg(long)]
        stale_action: Option<String>,
        /// Coordinator storage mode (json, dual-write, sqlite)
        #[arg(long)]
        storage_mode: Option<String>,
        /// Extra args passed directly to coordinator.sh (use after --)
        #[arg(last = true)]
        extra_args: Vec<String>,
    },
}

#[derive(Subcommand)]
pub enum InstallCommands {
    /// Install a skill from the catalog
    Skill {
        /// Tool to install the skill for (e.g. tool-id)
        #[arg(long)]
        tool: String,
        /// Skill ID from catalog
        #[arg(long)]
        id: String,
    },
    /// Install an MCP server from the catalog
    Mcp {
        /// MCP ID from catalog
        #[arg(long)]
        id: String,
    },
}

#[derive(Subcommand)]
pub enum ToolCommands {
    /// Install a local AI tool using steps defined in ToolSpec
    Install {
        /// Tool ID from ToolSpec
        tool_id: String,
        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
    },
    /// Update a local AI tool using steps defined in ToolSpec
    Update {
        /// Tool ID from ToolSpec
        tool_id: Option<String>,
        /// Update all matching tools
        #[arg(long)]
        all: bool,
        /// Filter when used with --all: enabled or installed
        #[arg(long, value_parser = ["enabled", "installed"])]
        only: Option<String>,
        /// Show what would be updated without running commands
        #[arg(long)]
        check: bool,
        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
        /// Force update even when already up-to-date
        #[arg(long)]
        force: bool,
        /// Best-effort rollback to previous version on failure (npm tools only)
        #[arg(long)]
        rollback_on_fail: bool,
    },
    /// Show installed/outdated status for tools
    Outdated {
        /// Filter results: enabled or installed
        #[arg(long, value_parser = ["enabled", "installed"])]
        only: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum CatalogCommands {
    /// Manage skills catalog
    Skills {
        #[command(subcommand)]
        skills_command: CatalogSubCommands,
    },
    /// Manage MCP catalog (Not implemented yet)
    Mcp {
        #[command(subcommand)]
        mcp_command: CatalogSubCommands,
    },
    /// Import an entry from a URL (e.g. GitHub tree)
    ImportUrl {
        /// Kind of entry (skill or mcp)
        #[arg(long, value_parser = ["skill", "mcp"])]
        kind: String,

        /// Entry ID
        #[arg(long)]
        id: String,

        /// URL to import
        #[arg(long)]
        url: String,

        /// Name (optional, defaults to ID)
        #[arg(long)]
        name: Option<String>,

        /// Description (optional)
        #[arg(long, default_value = "")]
        description: String,

        /// Comma-separated tags (optional)
        #[arg(long)]
        tags: Option<String>,
    },
    /// Search remote registry
    SearchRemote {
        /// API URL
        #[arg(long, default_value = "https://registry.macc.dev")]
        api: String,

        /// Kind (skill or mcp)
        #[arg(long, value_parser = ["skill", "mcp"])]
        kind: String,

        /// Search query
        #[arg(long)]
        q: String,

        /// Add all found results to local catalog
        #[arg(long)]
        add: bool,

        /// Add specific IDs from results to local catalog (comma-separated)
        #[arg(long)]
        add_ids: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum CatalogSubCommands {
    /// List entries in the catalog
    List,
    /// Search for entries in the catalog
    Search {
        /// Search query (matches id, name, description, tags)
        query: String,
    },
    /// Add or update an entry in the catalog
    Add {
        /// Entry ID
        #[arg(long)]
        id: String,
        /// Entry Name
        #[arg(long)]
        name: String,
        /// Entry Description
        #[arg(long)]
        description: String,
        /// Comma-separated tags
        #[arg(long)]
        tags: Option<String>,
        /// Subpath within the source
        #[arg(long, default_value = "")]
        subpath: String,
        /// Source kind (git or http)
        #[arg(long)]
        kind: String,
        /// Source URL
        #[arg(long)]
        url: String,
        /// Source reference (e.g. branch, tag, commit)
        #[arg(long, default_value = "main")]
        reference: String,
        /// Source checksum (optional)
        #[arg(long)]
        checksum: Option<String>,
    },
    /// Remove an entry from the catalog
    Remove {
        /// Entry ID
        #[arg(long)]
        id: String,
    },
}

#[derive(Subcommand)]
pub enum WorktreeCommands {
    /// Create worktrees for parallel runs
    Create {
        /// Slug for worktree IDs (e.g. "feature")
        slug: String,
        /// Tool to apply in each worktree
        #[arg(long)]
        tool: String,
        /// Number of worktrees to create
        #[arg(long, default_value_t = 1)]
        count: usize,
        /// Base branch to create from
        #[arg(long, default_value = "main")]
        base: String,
        /// Optional scope text (written to .macc/scope.md)
        #[arg(long)]
        scope: Option<String>,
        /// Optional feature label (stored in worktree.json)
        #[arg(long)]
        feature: Option<String>,
        /// Skip applying config in the new worktrees
        #[arg(long)]
        skip_apply: bool,
        /// Allow user-scope operations during apply
        #[arg(long)]
        allow_user_scope: bool,
    },
    /// Show status for the current worktree (if any)
    Status,
    /// List git worktrees
    List,
    /// Open a worktree in an editor and/or terminal
    Open {
        /// Worktree id (folder name under .macc/worktree) or path
        id: String,
        /// Editor command (defaults to "code")
        #[arg(long)]
        editor: Option<String>,
        /// Open in a terminal (uses $TERMINAL if set)
        #[arg(long)]
        terminal: bool,
    },
    /// Apply configuration in a worktree
    Apply {
        /// Worktree id (folder name under .macc/worktree) or path
        #[arg(required_unless_present = "all")]
        id: Option<String>,
        /// Apply all worktrees (excluding the main worktree)
        #[arg(long)]
        all: bool,
        /// Allow user-scope operations
        #[arg(long)]
        allow_user_scope: bool,
    },
    /// Run doctor checks in a worktree
    Doctor {
        /// Worktree id (folder name under .macc/worktree) or path
        id: String,
    },
    /// Run performer.sh inside a worktree
    Run {
        /// Worktree id (folder name under .macc/worktree) or path
        id: String,
    },
    /// Execute a command inside a worktree
    Exec {
        /// Worktree id (folder name under .macc/worktree) or path
        id: String,
        /// Command to execute after `--`
        #[arg(last = true, required = true)]
        cmd: Vec<String>,
    },
    /// Remove a worktree by id or path
    Remove {
        /// Worktree id (folder name under .worktree) or path
        #[arg(required_unless_present = "all")]
        id: Option<String>,
        /// Force removal
        #[arg(long)]
        force: bool,
        /// Remove all worktrees (excluding the main worktree)
        #[arg(long)]
        all: bool,
        /// Also delete the git branch for the removed worktree(s)
        #[arg(long)]
        remove_branch: bool,
    },
    /// Prune git worktrees
    Prune,
}

#[derive(Subcommand)]
pub enum LogsCommands {
    /// Tail the latest matching log file
    Tail {
        /// Component filter
        #[arg(long, default_value = "all", value_parser = ["all", "coordinator", "performer"])]
        component: String,
        /// Worktree ID/path filter (performer logs)
        #[arg(long)]
        worktree: Option<String>,
        /// Task ID filter (performer logs filename contains this value)
        #[arg(long)]
        task: Option<String>,
        /// Number of lines to display
        #[arg(short = 'n', long, default_value_t = 120)]
        lines: usize,
        /// Follow log updates
        #[arg(long)]
        follow: bool,
    },
}

#[derive(Subcommand)]
pub enum BackupsCommands {
    /// List available backup sets
    List {
        /// List user-level backup sets (~/.macc/backups)
        #[arg(long)]
        user: bool,
    },
    /// Print or open a backup set path
    Open {
        /// Backup set name (timestamp folder)
        #[arg(required_unless_present = "latest")]
        id: Option<String>,
        /// Open latest backup set
        #[arg(long)]
        latest: bool,
        /// Open from user-level backup root (~/.macc/backups)
        #[arg(long)]
        user: bool,
        /// Open using a specific editor command
        #[arg(long)]
        editor: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();

    if cli.verbose {
        eprintln!("Verbose mode enabled");
    }

    // Initialize the real engine with default registry
    let engine = MaccEngine::new(macc_registry::default_registry());
    let provider = services::engine_provider::EngineProvider::new(engine);

    if let Err(e) = run_with_engine_provider(cli, provider) {
        eprintln!("Error: {}", e);
        exit(get_exit_code(&e));
    }
}

fn get_exit_code(err: &MaccError) -> i32 {
    match err {
        MaccError::Validation(_) => 1,
        MaccError::UserScopeNotAllowed(_) => 2,
        MaccError::Io { .. } => 3,
        MaccError::ProjectRootNotFound { .. } => 4,
        MaccError::Config { .. } => 5,
        MaccError::SecretDetected { .. } => 6,
        MaccError::HomeDirNotFound => 7,
        MaccError::ToolSpec { .. } => 8,
    }
}

#[cfg(test)]
fn run_with_engine<E: Engine + Send + Sync + 'static>(cli: Cli, engine: E) -> Result<()> {
    let provider = services::engine_provider::EngineProvider::new(engine);
    run_with_engine_provider(cli, provider)
}

fn run_with_engine_provider(
    cli: Cli,
    provider: services::engine_provider::EngineProvider,
) -> Result<()> {
    let cwd = std::path::PathBuf::from(&cli.cwd);
    let absolute_cwd = if cwd.is_absolute() {
        cwd
    } else {
        std::env::current_dir()
            .map_err(|e| MaccError::Io {
                path: ".".into(),
                action: "get current_dir".into(),
                source: e,
            })?
            .join(cwd)
    };

    // Try to canonicalize to resolve .. and symlinks if it exists
    let absolute_cwd = absolute_cwd.canonicalize().unwrap_or(absolute_cwd);
    let engine = provider.shared();
    let app = commands::AppContext::new(absolute_cwd.clone(), engine.clone());

    match &cli.command {
        Some(Commands::Init { force, wizard }) =>
            commands::init::InitCommand::new(app.clone(), *force, *wizard).run(),
        Some(Commands::Quickstart { yes, apply, no_tui }) => {
            commands::quickstart::QuickstartCommand::new(
                app.clone(),
                *yes,
                *apply,
                *no_tui,
            )
            .run()
        }
        Some(Commands::Plan {
            tools,
            json,
            explain,
        }) => commands::plan::PlanCommand::new(app.clone(), tools.clone(), *json, *explain).run(),
        Some(Commands::Apply {
            tools,
            dry_run,
            allow_user_scope,
            json,
            explain,
        }) => commands::apply::ApplyCommand::new(
            app.clone(),
            tools.clone(),
            *dry_run,
            *allow_user_scope,
            *json,
            *explain,
        )
        .run(),
        Some(Commands::Catalog { catalog_command }) => {
            commands::catalog::CatalogCommand::new(app.clone(), catalog_command).run()
        }
        Some(Commands::Install { install_command }) => commands::install::InstallCommand::new(
            app.clone(),
            install_command,
        )
        .run(),
        Some(Commands::Tui) => {
            let paths = services::project::ensure_initialized_paths(&absolute_cwd)?;
            std::env::set_current_dir(&paths.root).map_err(|e| MaccError::Io {
                path: paths.root.to_string_lossy().into(),
                action: "set current_dir for tui".into(),
                source: e,
            })?;
            macc_tui::run_tui().map_err(|e| MaccError::Io {
                path: "tui".into(),
                action: "run_tui".into(),
                source: std::io::Error::other(e.to_string()),
            })
        }
        Some(Commands::Tool { tool_command }) => {
            commands::tool::ToolCommand::new(app.clone(), tool_command).run()
        }
        Some(Commands::Context { tool, from_files, dry_run, print_prompt }) => {
            commands::context::ContextCommand::new(
                app.clone(),
                tool.as_deref(),
                from_files,
                *dry_run,
                *print_prompt,
            )
            .run()
        }
        Some(Commands::Doctor { fix }) => {
            commands::doctor::DoctorCommand::new(app.clone(), *fix).run()
        }
        Some(Commands::Migrate { apply }) => {
            commands::migrate::MigrateCommand::new(app.clone(), *apply).run()
        }
        Some(Commands::Backups { backups_command }) => {
            commands::backups::BackupsCommand::new(app.clone(), backups_command).run()
        }
        Some(Commands::Restore { latest, user, backup, dry_run, yes }) => {
            commands::restore::RestoreCommand::new(
                app.clone(),
                *latest,
                *user,
                backup.as_deref(),
                *dry_run,
                *yes,
            )
            .run()
        }
        Some(Commands::Clear) => commands::clear::ClearCommand::new(app.clone()).run(),
        Some(Commands::Worktree { worktree_command }) => {
            commands::worktree::WorktreeCommand::new(app.clone(), worktree_command).run()
        }
        Some(Commands::Logs { logs_command }) => {
            commands::logs::LogsCommand::new(app.clone(), logs_command).run()
        }
        Some(Commands::Coordinator {
            action,
            no_tui,
            graceful,
            remove_worktrees,
            remove_branches,
            prd,
            coordinator_tool,
            reference_branch,
            tool_priority,
            max_parallel_per_tool_json,
            tool_specializations_json,
            max_dispatch,
            max_parallel,
            timeout_seconds,
            phase_runner_max_attempts,
            log_flush_lines,
            log_flush_ms,
            stale_claimed_seconds,
            stale_in_progress_seconds,
            stale_changes_requested_seconds,
            stale_action,
            storage_mode,
            extra_args,
        }) => commands::coordinator::CoordinatorCommand::new(
            app.clone(),
            coordinator::command::CoordinatorCommandInput {
                action: action.clone(),
                no_tui: *no_tui,
                graceful: *graceful,
                remove_worktrees: *remove_worktrees,
                remove_branches: *remove_branches,
                env_cfg: CoordinatorEnvConfig {
                    prd: prd.clone(),
                    coordinator_tool: coordinator_tool.clone(),
                    reference_branch: reference_branch.clone(),
                    tool_priority: tool_priority.clone(),
                    max_parallel_per_tool_json: max_parallel_per_tool_json.clone(),
                    tool_specializations_json: tool_specializations_json.clone(),
                    max_dispatch: *max_dispatch,
                    max_parallel: *max_parallel,
                    timeout_seconds: *timeout_seconds,
                    phase_runner_max_attempts: *phase_runner_max_attempts,
                    log_flush_lines: *log_flush_lines,
                    log_flush_ms: *log_flush_ms,
                    stale_claimed_seconds: *stale_claimed_seconds,
                    stale_in_progress_seconds: *stale_in_progress_seconds,
                    stale_changes_requested_seconds: *stale_changes_requested_seconds,
                    stale_action: stale_action.clone(),
                    storage_mode: storage_mode.clone(),
                    error_code_retry_list: std::env::var("ERROR_CODE_RETRY_LIST").ok(),
                    error_code_retry_max: std::env::var("ERROR_CODE_RETRY_MAX")
                        .ok()
                        .and_then(|v| v.parse().ok()),
                },
                extra_args: extra_args.clone(),
            },
        )
        .run(),
        None => {
            let paths = services::project::ensure_initialized_paths(&absolute_cwd)?;
            std::env::set_current_dir(&paths.root).map_err(|e| MaccError::Io {
                path: paths.root.to_string_lossy().into(),
                action: "set current_dir for tui".into(),
                source: e,
            })?;
            macc_tui::run_tui().map_err(|e| MaccError::Io {
                path: "tui".into(),
                action: "run_tui".into(),
                source: std::io::Error::other(e.to_string()),
            })
        }
    }
}


pub(crate) const COORDINATOR_TASK_REGISTRY_REL_PATH: &str = ".macc/automation/task/task_registry.json";
const COORDINATOR_PAUSE_FILE_REL_PATH: &str = ".macc/automation/task/coordinator.pause.json";

pub(crate) type CoordinatorJob = coordinator_runtime::CoordinatorJob;
pub(crate) type CoordinatorMergeJob = coordinator_runtime::CoordinatorMergeJob;
pub(crate) type CoordinatorRunState = coordinator_runtime::CoordinatorRunState;

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
        let mut state = self.state.lock().map_err(|_| {
            MaccError::Validation("Coordinator logger lock poisoned".to_string())
        })?;
        state.writer.write_all(line.as_bytes()).map_err(|e| MaccError::Io {
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

fn validate_coordinator_transition_action(args: &[String]) -> Result<()> {
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

fn validate_coordinator_runtime_transition_action(args: &[String]) -> Result<()> {
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

fn coordinator_runtime_status_from_event_action(args: &[String]) -> Result<()> {
    let parsed = RuntimeStatusFromEventArgs::try_from(args)?;
    let runtime = runtime_status_from_event(&parsed.event_type, &parsed.status);
    println!("{}", runtime.as_str());
    Ok(())
}

fn coordinator_storage_sync_action(repo_root: &std::path::Path, args: &[String]) -> Result<()> {
    let direction = StorageSyncArgs::try_from(args)?.direction;
    let paths = macc_core::ProjectPaths::from_root(repo_root);
    match direction {
        CoordinatorStorageTransfer::ImportJsonToSqlite => {
            coordinator_storage_import_json_to_sqlite(&paths)
        }
        CoordinatorStorageTransfer::ExportSqliteToJson => {
            coordinator_storage_export_sqlite_to_json(&paths)
        }
        CoordinatorStorageTransfer::VerifyParity => coordinator_storage_verify_parity(&paths),
    }
}

fn coordinator_select_ready_task_action(
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
    out
}

#[cfg(test)]
fn run_coordinator_action(
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
    if let Err(err) = coordinator::logs::aggregate_performer_logs(repo_root) {
        eprintln!("warning: failed to aggregate performer logs: {}", err);
    }
    Ok(())
}

fn read_coordinator_counts(
    paths: &macc_core::ProjectPaths,
) -> Result<coordinator_engine::CoordinatorCounts> {
    let snapshot =
        coordinator::state::coordinator_state_snapshot(&paths.root, &BTreeMap::new())?;
    let tasks = snapshot
        .registry
        .get("tasks")
        .and_then(|v| v.as_array())
        .ok_or_else(|| MaccError::Validation("Registry missing .tasks array".into()))?;

    let mut counts = coordinator_engine::CoordinatorCounts {
        total: tasks.len(),
        todo: 0,
        active: 0,
        blocked: 0,
        merged: 0,
    };
    for task in tasks {
        let state = task
            .get("state")
            .and_then(|s| s.as_str())
            .unwrap_or_default();
        match state {
            "todo" => counts.todo += 1,
            "blocked" => counts.blocked += 1,
            "merged" => counts.merged += 1,
            "claimed" | "in_progress" | "pr_open" | "changes_requested" | "queued" => {
                counts.active += 1
            }
            _ => {}
        }
    }
    Ok(counts)
}

fn sync_registry_from_prd_native(
    repo_root: &std::path::Path,
    prd_file: &std::path::Path,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<()> {
    coordinator::control_plane::sync_registry_from_prd_native(repo_root, prd_file, logger)
}
pub(crate) fn append_coordinator_event_with_severity(
    repo_root: &std::path::Path,
    event_type: &str,
    task_id: &str,
    phase: &str,
    status: &str,
    message: &str,
    severity: &str,
) -> Result<()> {
    let run_id = ensure_coordinator_run_id();
    let now = now_iso_coordinator();
    let seq = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default() as u64;
    let payload = serde_json::json!({
        "schema_version":"1",
        "event_id": format!("evt-{}-{}-{}", event_type, task_id, seq),
        "run_id": run_id,
        "seq": seq,
        "ts": now,
        "source": "coordinator:native",
        "task_id": task_id,
        "type": event_type,
        "phase": phase,
        "status": status,
        "severity": severity,
        "payload": {"message": message}
    });
    let project_paths = macc_core::ProjectPaths::from_root(repo_root);
    // SQLite is source-of-truth for coordinator events.
    let _ = append_event_sqlite(&project_paths, &payload)?;
    Ok(())
}

pub(crate) fn ensure_coordinator_run_id() -> String {
    if let Ok(existing) = std::env::var("COORDINATOR_RUN_ID") {
        let trimmed = existing.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    let generated = format!(
        "run-{}-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
        std::process::id()
    );
    std::env::set_var("COORDINATOR_RUN_ID", &generated);
    generated
}

async fn advance_tasks_native(
    repo_root: &std::path::Path,
    coordinator_tool_override: Option<&str>,
    phase_runner_max_attempts: usize,
    state: &mut CoordinatorRunState,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<coordinator_engine::AdvanceResult> {
    coordinator::control_plane::advance_tasks_native(
        repo_root,
        coordinator_tool_override,
        phase_runner_max_attempts,
        state,
        logger,
    )
    .await
}
async fn monitor_active_jobs_native(
    repo_root: &std::path::Path,
    env_cfg: &CoordinatorEnvConfig,
    state: &mut CoordinatorRunState,
    max_attempts: usize,
    phase_timeout_seconds: usize,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<()> {
    coordinator::control_plane::monitor_active_jobs_native(
        repo_root,
        env_cfg,
        state,
        max_attempts,
        phase_timeout_seconds,
        logger,
    )
    .await
}
async fn monitor_merge_jobs_native(
    repo_root: &std::path::Path,
    state: &mut CoordinatorRunState,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<Option<(String, String)>> {
    coordinator::control_plane::monitor_merge_jobs_native(repo_root, state, logger).await
}
async fn dispatch_ready_tasks_native(
    repo_root: &std::path::Path,
    canonical: &macc_core::config::CanonicalConfig,
    coordinator: Option<&macc_core::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
    prd_file: &std::path::Path,
    state: &mut CoordinatorRunState,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<usize> {
    coordinator::control_plane::dispatch_ready_tasks_native(
        repo_root,
        canonical,
        coordinator,
        env_cfg,
        prd_file,
        state,
        logger,
    )
    .await
}
fn terminate_active_jobs(state: &CoordinatorRunState, logger: Option<&NativeCoordinatorLogger>) {
    for (task_id, pid) in coordinator_runtime::terminate_active_jobs(state) {
        if let Some(log) = logger {
            let _ = log.note(format!(
                "- Sent TERM to active task={} pid={}",
                task_id, pid
            ));
        }
    }
}

async fn wait_for_resume_signal(
    repo_root: &std::path::Path,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<()> {
    loop {
        if !coordinator_pause_file_path(repo_root).exists() {
            if let Some(log) = logger {
                let _ = log.note("- Resume signal received; continuing run loop");
            }
            return Ok(());
        }
        if let Some(log) = logger {
            let _ = log.note("- Waiting for resume signal (`macc coordinator resume`)");
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

struct NativeControlPlaneBackend<'a> {
    repo_root: &'a std::path::Path,
    canonical: &'a macc_core::config::CanonicalConfig,
    coordinator: Option<&'a macc_core::config::CoordinatorConfig>,
    env_cfg: &'a CoordinatorEnvConfig,
    logger: NativeCoordinatorLogger,
    prd_file: std::path::PathBuf,
    run_state: CoordinatorRunState,
    phase_runner_max_attempts: usize,
    coordinator_tool_override: Option<String>,
    phase_timeout_seconds: usize,
    storage_mode: CoordinatorStorageMode,
    storage_paths: macc_core::ProjectPaths,
    last_logged_counts: Option<coordinator_engine::CoordinatorCounts>,
}

impl<'a> NativeControlPlaneBackend<'a> {
    fn new(
        repo_root: &'a std::path::Path,
        canonical: &'a macc_core::config::CanonicalConfig,
        coordinator: Option<&'a macc_core::config::CoordinatorConfig>,
        env_cfg: &'a CoordinatorEnvConfig,
        prd_file: std::path::PathBuf,
    ) -> Result<Self> {
        let logger = NativeCoordinatorLogger::new_with_flush(
            repo_root,
            "run",
            env_cfg
                .log_flush_lines
                .or_else(|| coordinator.and_then(|c| c.log_flush_lines)),
            env_cfg
                .log_flush_ms
                .or_else(|| coordinator.and_then(|c| c.log_flush_ms)),
        )?;
        println!("Coordinator log file: {}", logger.file.display());
        let _ = logger.note("- Native Rust control-plane run started");
        let phase_runner_max_attempts = env_cfg
            .phase_runner_max_attempts
            .or_else(|| coordinator.and_then(|c| c.phase_runner_max_attempts))
            .unwrap_or(1)
            .max(1);
        let coordinator_tool_override = env_cfg
            .coordinator_tool
            .clone()
            .or_else(|| coordinator.and_then(|c| c.coordinator_tool.clone()));
        let phase_timeout_seconds = env_cfg
            .stale_in_progress_seconds
            .or_else(|| coordinator.and_then(|c| c.stale_in_progress_seconds))
            .unwrap_or(0);
        let storage_mode = resolve_coordinator_storage_mode(env_cfg, coordinator)?;
        let storage_paths = macc_core::ProjectPaths::from_root(repo_root);

        Ok(Self {
            repo_root,
            canonical,
            coordinator,
            env_cfg,
            logger,
            prd_file,
            run_state: CoordinatorRunState::new(),
            phase_runner_max_attempts,
            coordinator_tool_override,
            phase_timeout_seconds,
            storage_mode,
            storage_paths,
            last_logged_counts: None,
        })
    }
}

#[async_trait]
impl coordinator_engine::ControlPlaneBackend for NativeControlPlaneBackend<'_> {
    async fn on_cycle_start(&mut self, cycle: usize) -> Result<()> {
        let _ = cycle;
        sync_registry_from_prd_native(self.repo_root, &self.prd_file, None)?;
        let _ = coordinator_runtime::process_branch_cleanup_queue(
            self.repo_root,
            |event_type, task_id, phase, status, message, severity| {
                let _ = append_coordinator_event_with_severity(
                    self.repo_root,
                    event_type,
                    task_id,
                    phase,
                    status,
                    message,
                    severity,
                );
            },
            Some(|msg| {
                let _ = self.logger.note(msg);
            }),
        )?;
        let cycle_cleaned =
            cleanup_dead_runtime_tasks(self.repo_root, "run-cycle", Some(&self.logger))?;
        if cycle_cleaned > 0 {
            let _ = self
                .logger
                .note(format!("- Runtime cleanup fixed {} ghost task(s)", cycle_cleaned));
        }
        Ok(())
    }

    async fn monitor_active_jobs(&mut self) -> Result<()> {
        monitor_active_jobs_native(
            self.repo_root,
            self.env_cfg,
            &mut self.run_state,
            self.phase_runner_max_attempts,
            self.phase_timeout_seconds,
            Some(&self.logger),
        )
        .await
    }

    async fn monitor_merge_jobs(&mut self) -> Result<Option<(String, String)>> {
        monitor_merge_jobs_native(self.repo_root, &mut self.run_state, Some(&self.logger)).await
    }

    async fn on_blocked_merge(&mut self, task_id: &str, reason: &str) -> Result<()> {
        terminate_active_jobs(&self.run_state, Some(&self.logger));
        self.run_state.merge_join_set.abort_all();
        self.run_state.active_merge_jobs.clear();
        set_task_paused_for_integrate(self.repo_root, task_id, reason)?;
        write_coordinator_pause_file(self.repo_root, task_id, "integrate", reason)?;
        println!(
            "Coordinator paused on task {} (integrate). Resolve the merge issue, then run `macc coordinator resume`.",
            task_id
        );
        let _ = self.logger.note(format!(
            "- Run paused task={} phase=integrate reason={}",
            task_id, reason
        ));
        wait_for_resume_signal(self.repo_root, Some(&self.logger)).await?;
        resume_paused_task_integrate(self.repo_root, task_id)?;
        Ok(())
    }

    async fn advance_tasks(&mut self) -> Result<coordinator_engine::AdvanceResult> {
        advance_tasks_native(
            self.repo_root,
            self.coordinator_tool_override.as_deref(),
            self.phase_runner_max_attempts,
            &mut self.run_state,
            Some(&self.logger),
        )
        .await
    }

    async fn dispatch_ready_tasks(&mut self) -> Result<usize> {
        dispatch_ready_tasks_native(
            self.repo_root,
            self.canonical,
            self.coordinator,
            self.env_cfg,
            &self.prd_file,
            &mut self.run_state,
            Some(&self.logger),
        )
        .await
    }

    async fn on_cycle_end(
        &mut self,
        cycle: usize,
        advance: &coordinator_engine::AdvanceResult,
        dispatched: usize,
    ) -> Result<coordinator_engine::CoordinatorCounts> {
        if advance.progressed || dispatched > 0 {
            let _ = self.logger.note(format!(
                "- Cycle {} transition summary progressed={} dispatched={}",
                cycle, advance.progressed, dispatched
            ));
        }

        let _ = self.storage_mode;
        let _ = &self.storage_paths;

        let _ = coordinator::logs::aggregate_performer_logs_async(self.repo_root).await;
        let paths = macc_core::ProjectPaths::from_root(self.repo_root);
        let counts = read_coordinator_counts(&paths)?;
        let counts_changed = self.last_logged_counts != Some(counts);
        if counts_changed {
            println!(
                "Coordinator cycle {}: total={} todo={} active={} blocked={} merged={}",
                cycle, counts.total, counts.todo, counts.active, counts.blocked, counts.merged
            );
            let _ = self.logger.note(format!(
                "- Cycle {} counts total={} todo={} active={} blocked={} merged={}",
                cycle, counts.total, counts.todo, counts.active, counts.blocked, counts.merged
            ));
            self.last_logged_counts = Some(counts);
        }
        let max_dispatch_total = self
            .env_cfg
            .max_dispatch
            .or_else(|| self.coordinator.and_then(|c| c.max_dispatch))
            .unwrap_or(10);
        if max_dispatch_total > 0
            && self.run_state.dispatched_total_run >= max_dispatch_total
            && counts.active == 0
        {
            let _ = self.logger.note(format!(
                "- Run stop condition reached: dispatched_total={} max_dispatch={}",
                self.run_state.dispatched_total_run, max_dispatch_total
            ));
        }
        Ok(counts)
    }

    async fn sleep_between_cycles(&mut self) -> Result<()> {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        Ok(())
    }

    fn should_terminate_run(&self, counts: &coordinator_engine::CoordinatorCounts) -> bool {
        let max_dispatch_total = self
            .env_cfg
            .max_dispatch
            .or_else(|| self.coordinator.and_then(|c| c.max_dispatch))
            .unwrap_or(10);
        if max_dispatch_total == 0 {
            return false;
        }
        self.run_state.dispatched_total_run >= max_dispatch_total
            && counts.active == 0
            && self.run_state.active_jobs.is_empty()
            && self.run_state.active_merge_jobs.is_empty()
    }
}

fn run_coordinator_control_plane_rust(
    repo_root: &std::path::Path,
    canonical: &macc_core::config::CanonicalConfig,
    coordinator: Option<&macc_core::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .map_err(|e| MaccError::Validation(format!("Failed to initialize tokio runtime: {}", e)))?;
    runtime.block_on(run_coordinator_control_plane_rust_async(
        repo_root,
        canonical,
        coordinator,
        env_cfg,
    ))
}

async fn run_coordinator_control_plane_rust_async(
    repo_root: &std::path::Path,
    canonical: &macc_core::config::CanonicalConfig,
    coordinator: Option<&macc_core::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
) -> Result<()> {
    let run_id = ensure_coordinator_run_id();
    let _ = append_coordinator_event_with_severity(
        repo_root,
        "command_start",
        "-",
        "run",
        "started",
        &format!("Coordinator run started (run_id={})", run_id),
        "info",
    );
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
    if !prd_file.exists() {
        #[cfg(test)]
        {
            return run_coordinator_full_cycle(repo_root, canonical, coordinator, env_cfg);
        }
        #[cfg(not(test))]
        {
            return Err(MaccError::Validation(format!(
                "Coordinator PRD file not found: {}. Configure `automation.coordinator.prd_file` or pass `--prd`.",
                prd_file.display()
            )));
        }
    }

    let mut backend =
        NativeControlPlaneBackend::new(repo_root, canonical, coordinator, env_cfg, prd_file)?;
    sync_storage_with_startup_reconcile(
        &backend.storage_paths,
        backend.storage_mode,
        Some(&backend.logger),
    )?;
    let startup_cleaned =
        cleanup_dead_runtime_tasks(repo_root, "run-startup", Some(&backend.logger))?;
    if startup_cleaned > 0 {
        let _ = backend.logger.note(format!(
            "- Startup runtime cleanup fixed {} ghost task(s)",
            startup_cleaned
        ));
    }

    let timeout_seconds = env_cfg
        .timeout_seconds
        .or_else(|| coordinator.and_then(|c| c.timeout_seconds))
        .unwrap_or(0);
    let loop_cfg = coordinator_engine::ControlPlaneLoopConfig {
        timeout: if timeout_seconds > 0 {
            Some(std::time::Duration::from_secs(timeout_seconds as u64))
        } else {
            None
        },
        max_no_progress_cycles: 2,
    };
    let run_result = coordinator_engine::run_control_plane(&mut backend, loop_cfg).await;
    if run_result.is_err() {
        if let Err(err) = &run_result {
            let _ = append_coordinator_event_with_severity(
                repo_root,
                "command_end",
                "-",
                "run",
                "failed",
                &format!("Coordinator run failed: {}", err),
                "blocking",
            );
        }
        terminate_active_jobs(&backend.run_state, Some(&backend.logger));
        backend.run_state.active_jobs.clear();
        backend.run_state.join_set.abort_all();
        backend.run_state.active_merge_jobs.clear();
        backend.run_state.merge_join_set.abort_all();
        return run_result;
    }

    let _ = backend.logger.note("- Run complete");
    let _ = append_coordinator_event_with_severity(
        repo_root,
        "command_end",
        "-",
        "run",
        "done",
        "Coordinator run complete",
        "info",
    );
    println!("Coordinator run complete.");
    Ok(())
}

fn resolve_coordinator_storage_mode(
    env_cfg: &CoordinatorEnvConfig,
    coordinator: Option<&macc_core::config::CoordinatorConfig>,
) -> Result<CoordinatorStorageMode> {
    let raw = env_cfg
        .storage_mode
        .clone()
        .or_else(|| coordinator.and_then(|c| c.storage_mode.clone()))
        .unwrap_or_else(|| "sqlite".to_string());
    raw.parse::<CoordinatorStorageMode>()
        .map_err(MaccError::Validation)
}

fn sync_storage_with_startup_reconcile(
    project_paths: &macc_core::ProjectPaths,
    storage_mode: CoordinatorStorageMode,
    logger: Option<&NativeCoordinatorLogger>,
) -> Result<()> {
    if storage_mode == CoordinatorStorageMode::Json {
        return Ok(());
    }
    let imported = coordinator_storage_bootstrap_sqlite_from_json(project_paths)?;
    if imported {
        if let Some(log) = logger {
            let _ = log.note("- Storage bootstrap: imported JSON snapshot into SQLite");
        }
    }
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
        coordinator_storage_export_sqlite_to_json(project_paths)?;
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
struct RegistryCounts {
    total: usize,
    todo: usize,
    active: usize,
    blocked: usize,
    merged: usize,
}

#[cfg(test)]
fn read_registry_counts(path: &std::path::Path) -> Result<RegistryCounts> {
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
fn run_coordinator_full_cycle(
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
        coordinator::control_plane::sync_registry_from_prd_native(repo_root, &prd_file, None)?;

        let before = read_registry_counts(&registry_path)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .map_err(|e| MaccError::Validation(format!("Failed to init tokio runtime: {}", e)))?;
        runtime.block_on(async {
            let mut state = CoordinatorRunState::new();
            let _ = coordinator::control_plane::dispatch_ready_tasks_native(
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
                coordinator::control_plane::monitor_active_jobs_native(
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
            let advance = coordinator::control_plane::advance_tasks_native(
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
                let _ = coordinator::control_plane::monitor_merge_jobs_native(
                    repo_root,
                    &mut state,
                    None,
                )
                .await?;
                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
            }
            Result::<()>::Ok(())
        })?;

        reconcile_registry_native(repo_root)?;
        cleanup_registry_native(repo_root)?;
        coordinator::control_plane::sync_registry_from_prd_native(repo_root, &prd_file, None)?;
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

fn stop_coordinator_process_groups(
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
    // Group can disappear between discovery and signaling; treat that as success.
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

pub(crate) fn remove_all_worktrees(root: &std::path::Path, remove_branches: bool) -> Result<usize> {
    let entries = macc_core::list_worktrees(root)?;
    let root_canon = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let mut removed = 0usize;

    for entry in entries {
        if entry.path == root_canon {
            continue;
        }
        let branch = entry.branch.clone();
        macc_core::remove_worktree(root, &entry.path, true)?;
        if remove_branches {
            services::worktree::delete_branch(root, branch.as_deref(), true)?;
        }
        removed += 1;
    }
    Ok(removed)
}

pub(crate) fn run_context_generation(
    paths: &macc_core::ProjectPaths,
    tool_filter: Option<&str>,
    from_files: &[String],
    dry_run: bool,
    print_prompt: bool,
) -> Result<()> {
    require_apply_before_context(paths)?;

    let canonical = load_canonical_config(&paths.config_path)?;
    let loader = ToolSpecLoader::new(ToolSpecLoader::default_search_paths(&paths.root));
    let (specs, diagnostics) = loader.load_all_with_embedded();
    services::project::report_diagnostics(&diagnostics);

    let selected_tools: Vec<String> = if let Some(tool_id) = tool_filter {
        vec![tool_id.to_string()]
    } else {
        canonical.tools.enabled.clone()
    };

    if selected_tools.is_empty() {
        return Err(MaccError::Validation(
            "No tool selected. Enable tools in .macc/macc.yaml or pass --tool <id>.".into(),
        ));
    }

    let mut generated = 0usize;
    let mut missing_tools = Vec::new();
    for tool_id in selected_tools {
        let Some(spec) = specs.iter().find(|s| s.id == tool_id) else {
            missing_tools.push(tool_id.clone());
            println!("Skipping '{}': ToolSpec not found.", tool_id);
            continue;
        };
        let performer = spec.performer.as_ref().ok_or_else(|| {
            MaccError::Validation(format!(
                "Tool '{}' has no performer config; cannot generate context via AI tool.",
                tool_id
            ))
        })?;

        let target_rel = resolve_context_target_rel(&canonical, spec);
        let target_abs = paths.root.join(&target_rel);
        let prompt = build_context_prompt(paths, &canonical, spec, &target_rel, from_files)?;

        if print_prompt {
            println!(
                "\n--- Prompt for {} ({}) ---\n{}\n",
                spec.display_name, spec.id, prompt
            );
        }

        if dry_run {
            println!(
                "[dry-run] tool={} target={} prompt_chars={}",
                spec.id,
                target_rel,
                prompt.chars().count()
            );
            generated += 1;
            continue;
        }

        if let Some(parent) = target_abs.parent() {
            std::fs::create_dir_all(parent).map_err(|e| MaccError::Io {
                path: parent.to_string_lossy().into(),
                action: "create context target parent directory".into(),
                source: e,
            })?;
        }

        invoke_context_tool(paths, performer, &prompt)?;

        if !target_abs.is_file() {
            return Err(MaccError::Validation(format!(
                "Tool '{}' completed but did not produce '{}'. Ensure the agent writes that file directly.",
                spec.id, target_rel
            )));
        }

        println!(
            "Context updated in-place: {} via {}",
            target_rel, spec.display_name
        );
        generated += 1;
    }

    if generated == 0 {
        if tool_filter.is_some() && !missing_tools.is_empty() {
            return Err(MaccError::Validation(format!(
                "ToolSpec not found for tool '{}'.",
                missing_tools[0]
            )));
        }
        return Err(MaccError::Validation(
            "No context files generated. Check enabled tools and ToolSpecs.".into(),
        ));
    }

    println!("Context generation complete. Files handled: {}", generated);
    Ok(())
}

fn context_apply_marker_path(paths: &macc_core::ProjectPaths) -> std::path::PathBuf {
    paths
        .macc_dir
        .join("state")
        .join("context_ready_after_apply")
}

fn require_apply_before_context(paths: &macc_core::ProjectPaths) -> Result<()> {
    let marker = context_apply_marker_path(paths);
    if marker.exists() {
        return Ok(());
    }
    Err(MaccError::Validation(
        "macc context is locked until at least one successful 'macc apply' has completed in this project.".into(),
    ))
}

fn mark_apply_completed(paths: &macc_core::ProjectPaths) -> Result<()> {
    let marker = context_apply_marker_path(paths);
    if let Some(parent) = marker.parent() {
        std::fs::create_dir_all(parent).map_err(|e| MaccError::Io {
            path: parent.to_string_lossy().into(),
            action: "create apply marker directory".into(),
            source: e,
        })?;
    }
    std::fs::write(&marker, b"applied\n").map_err(|e| MaccError::Io {
        path: marker.to_string_lossy().into(),
        action: "write apply marker".into(),
        source: e,
    })?;
    Ok(())
}

fn resolve_context_target_rel(
    canonical: &macc_core::config::CanonicalConfig,
    spec: &ToolSpec,
) -> String {
    if let Some(rel) = context_target_from_tool_settings(canonical, &spec.id) {
        return rel;
    }

    if let Some(md) = spec.gitignore.iter().find_map(|entry| {
        let path = std::path::Path::new(entry);
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if ext.eq_ignore_ascii_case("md") {
            Some(entry.clone())
        } else {
            None
        }
    }) {
        return md;
    }

    format!("{}.md", spec.id.to_ascii_uppercase().replace('-', "_"))
}

fn context_target_from_tool_settings(
    canonical: &macc_core::config::CanonicalConfig,
    tool_id: &str,
) -> Option<String> {
    let config_map_entry = canonical.tools.config.get(tool_id);
    let legacy_entry = canonical.tools.settings.get(tool_id);
    for entry in [config_map_entry, legacy_entry].into_iter().flatten() {
        if let Some(target) = extract_context_file_name_from_json(entry) {
            return Some(target);
        }
    }
    None
}

fn extract_context_file_name_from_json(value: &serde_json::Value) -> Option<String> {
    let context = value.get("context")?;
    let file_name = context.get("fileName")?;
    match file_name {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Array(items) => items.first()?.as_str().map(|s| s.to_string()),
        _ => None,
    }
}

fn build_context_prompt(
    paths: &macc_core::ProjectPaths,
    canonical: &macc_core::config::CanonicalConfig,
    spec: &ToolSpec,
    target_rel: &str,
    from_files: &[String],
) -> Result<String> {
    let mut sources: Vec<String> = Vec::new();
    for item in from_files {
        if !sources.contains(item) {
            sources.push(item.clone());
        }
    }

    let mut snippets = Vec::new();
    for rel in sources {
        let abs = paths.root.join(&rel);
        if !abs.exists() || !abs.is_file() {
            continue;
        }
        let content = std::fs::read_to_string(&abs).map_err(|e| MaccError::Io {
            path: abs.to_string_lossy().into(),
            action: "read context source file".into(),
            source: e,
        })?;
        let excerpt = truncate_text_for_prompt(&content, 6000);
        snippets.push((rel, excerpt));
    }

    let mut prompt = String::new();
    prompt.push_str(
        "You are a technical audit agent and developer assistant embedded in this repository.\n",
    );
    prompt.push_str(&format!(
        "Your mission is to update `{}` as operational context and a working guide for {} AI agent (or developer) contributing to this project.\n\n",
        target_rel,
        spec.id
    ));
    prompt.push_str(&format!("Tool ID: {}\n", spec.id));
    prompt.push_str(&format!("Tool Name: {}\n", spec.display_name));
    prompt.push_str(&format!("Target file: {}\n", target_rel));
    prompt.push_str(&format!(
        "Enabled tools: {}\n\n",
        canonical.tools.enabled.join(", ")
    ));
    prompt.push_str("Strict constraints\n");
    prompt.push_str("- Rely only on the repository's actual contents (README, docs, folder structure, config, CI, scripts).\n");
    prompt.push_str("- Do not invent anything.\n");
    prompt.push_str("- If information is missing, write: `Unknown (to verify)` + indicate where to find it (files/commands).\n");
    prompt.push_str("- For important statements (setup, commands, CI, tests, env vars, rules), indicate source as: `seen in <path/file>`.\n");
    prompt.push_str("- Priority: security + compliance + quality + maintainability.\n");
    prompt.push_str("- Style: clear, actionable, concise Markdown with checklists.\n\n");

    prompt.push_str("Required method (perform before writing)\n");
    prompt.push_str(
        "1. Scan the folder structure: identify modules, entry points, key directories.\n",
    );
    prompt.push_str("2. Detect the stack: languages, frameworks, dependency management, tooling (lint/format/build).\n");
    prompt.push_str("3. Map workflows: local execution, tests, CI, release.\n");
    prompt.push_str("4. Security audit: secrets, auth, permissions, dependencies, sensitive data, attack surfaces.\n");
    prompt.push_str("5. Compliance audit: licenses, personal data, logs, retention, traceability, requirements (if present).\n");
    prompt.push_str(
        "6. Tests & quality audit: test types, coverage, flakiness, mocks, fixtures, strategy.\n",
    );
    prompt.push_str("7. Synthesis: produce a context file that is immediately usable.\n\n");

    prompt.push_str("Mandatory skill-routing section (must be present in the generated context file)\n");
    prompt.push_str("Add a dedicated `# Project Mandates` section and keep it deterministic.\n");
    prompt.push_str("Separate clearly:\n");
    prompt.push_str("- Global mandates (always valid)\n");
    prompt.push_str("- Mode-specific mandates (active only for the current phase)\n");
    prompt.push_str("Use repository-relative paths only (no absolute paths):\n");
    prompt.push_str("- planning -> `skills/macc-prd-planner/SKILL.md`\n");
    prompt.push_str("- execution -> `skills/macc-performer/SKILL.md`\n");
    prompt.push_str("- review -> `skills/macc-code-reviewer/SKILL.md`\n");
    prompt.push_str("Fallback rule (explicit and mandatory): if a required skill file is absent or inaccessible, stop and report the error.\n");
    prompt.push_str("Add a `## Path validation` subsection with a short checklist that verifies these files exist.\n");
    prompt.push_str("Add `## Architecture Source of Truth` with repository-relative references:\n");
    prompt.push_str("- `skills/macc-performer/docs/ERRORS.md`\n");
    prompt.push_str("- `skills/macc-performer/docs/adr/0000-template.md`\n\n");

    prompt.push_str("Deliverable: write the target file with this exact outline\n");
    prompt.push_str("0. Project Mandates (global + mode-specific + path validation)\n");
    prompt.push_str("1. TL;DR (max 10 lines)\n");
    prompt.push_str("2. Project identity card\n");
    prompt.push_str("3. Stack & tooling (with sources)\n");
    prompt.push_str("4. Architecture & components\n");
    prompt.push_str("5. Reproducible local setup\n");
    prompt.push_str("6. Essential commands (copy/paste)\n");
    prompt.push_str("7. Developer standards (Do / Don't)\n");
    prompt.push_str("8. Test & quality strategy\n");
    prompt.push_str("9. Productivity playbooks (typical tasks)\n");
    prompt.push_str("10. Security (priority)\n");
    prompt.push_str("11. Compliance & governance\n");
    prompt.push_str("12. \"Where to find what\" (agent FAQ)\n");
    prompt.push_str("13. Unknowns & documentation debt\n\n");

    prompt.push_str("Output rules\n");
    prompt.push_str(&format!(
        "- Edit `{}` directly in the repository.\n",
        target_rel
    ));
    prompt.push_str("- Do not return the full file content in output.\n");
    prompt.push_str("- At the end, print a short status line indicating the file was updated.\n");
    prompt.push_str("- Every command must be copyable, exact, and sourced when possible.\n");
    prompt.push_str(
        "- Add Markdown checklists (`- [ ]`) for PR / security / release (if applicable).\n",
    );
    prompt.push_str("- Clearly mark what is observed vs inferred.\n\n");

    if snippets.is_empty() {
        prompt.push_str("Sources:\n- none provided\n");
    } else {
        prompt.push_str("Sources:\n");
        for (rel, excerpt) in snippets {
            prompt.push_str(&format!("\n--- BEGIN SOURCE: {} ---\n", rel));
            prompt.push_str(&excerpt);
            prompt.push_str(&format!("\n--- END SOURCE: {} ---\n", rel));
        }
    }
    Ok(prompt)
}

fn truncate_text_for_prompt(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    input.chars().take(max_chars).collect::<String>()
}

fn invoke_context_tool(
    paths: &macc_core::ProjectPaths,
    performer: &ToolPerformerSpec,
    prompt: &str,
) -> Result<()> {
    if !command_exists(&performer.command) {
        return Err(MaccError::Validation(format!(
            "Tool command '{}' not found in PATH. Run 'macc doctor' and install/login the tool first.",
            performer.command
        )));
    }

    let mut cmd = std::process::Command::new(&performer.command);
    cmd.current_dir(&paths.root);
    cmd.args(&performer.args);

    let prompt_mode = performer
        .prompt
        .as_ref()
        .map(|p| p.mode.as_str())
        .unwrap_or("stdin");

    match prompt_mode {
        "arg" => {
            let arg = performer
                .prompt
                .as_ref()
                .and_then(|p| p.arg.as_ref())
                .ok_or_else(|| {
                    MaccError::Validation(format!(
                        "Tool '{}' prompt mode is 'arg' but no prompt arg is configured.",
                        performer.command
                    ))
                })?;
            cmd.arg(arg);
            cmd.arg(prompt);
            let output = cmd.output().map_err(|e| MaccError::Io {
                path: performer.command.clone(),
                action: "run tool context generation command".into(),
                source: e,
            })?;
            validate_context_tool_exit(&performer.command, output)
        }
        "stdin" => {
            use std::io::Write;
            let mut child = cmd
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .map_err(|e| MaccError::Io {
                    path: performer.command.clone(),
                    action: "spawn tool context generation command".into(),
                    source: e,
                })?;
            if let Some(mut stdin) = child.stdin.take() {
                stdin
                    .write_all(prompt.as_bytes())
                    .map_err(|e| MaccError::Io {
                        path: performer.command.clone(),
                        action: "write prompt to tool stdin".into(),
                        source: e,
                    })?;
            }
            let output = child.wait_with_output().map_err(|e| MaccError::Io {
                path: performer.command.clone(),
                action: "wait for tool context generation command".into(),
                source: e,
            })?;
            validate_context_tool_exit(&performer.command, output)
        }
        other => Err(MaccError::Validation(format!(
            "Unsupported prompt mode '{}' for tool '{}'.",
            other, performer.command
        ))),
    }
}

fn validate_context_tool_exit(command: &str, output: std::process::Output) -> Result<()> {
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if !output.status.success() {
        let reason = if !stderr.trim().is_empty() {
            stderr.trim().to_string()
        } else if !stdout.trim().is_empty() {
            stdout.trim().to_string()
        } else {
            format!("exit status {}", output.status)
        };
        return Err(MaccError::Validation(format!(
            "Context generation command '{}' failed: {}",
            command, reason
        )));
    }

    Ok(())
}

fn command_exists(cmd: &str) -> bool {
    std::process::Command::new("bash")
        .arg("-lc")
        .arg(format!("command -v {} >/dev/null 2>&1", shell_escape(cmd)))
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn shell_escape(input: &str) -> String {
    if input
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '/')
    {
        input.to_string()
    } else {
        format!("'{}'", input.replace('\'', "'\"'\"'"))
    }
}

pub(crate) fn confirm_yes_no(prompt: &str) -> Result<bool> {
    use std::io::{self, Write};

    print!("{}", prompt);
    io::stdout().flush().map_err(|e| MaccError::Io {
        path: "stdout".into(),
        action: "flush prompt".into(),
        source: e,
    })?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| MaccError::Io {
            path: "stdin".into(),
            action: "read confirmation".into(),
            source: e,
        })?;
    let value = input.trim().to_ascii_lowercase();
    Ok(value == "y" || value == "yes")
}

pub(crate) fn print_checks(checks: &[macc_core::doctor::ToolCheck]) {
    println!("{:<20} {:<10} {:<30}", "CHECK", "STATUS", "TARGET");
    println!("{:-<20} {:-<10} {:-<30}", "", "", "");

    for check in checks {
        let status_str = match &check.status {
            macc_core::doctor::ToolStatus::Installed => "OK".to_string(),
            macc_core::doctor::ToolStatus::Missing => "MISSING".to_string(),
            macc_core::doctor::ToolStatus::Error(e) => format!("ERROR: {}", e),
        };
        println!(
            "{:<20} {:<10} {:<30}",
            check.name, status_str, check.check_target
        );
    }
}

#[derive(Debug, serde::Serialize)]
struct PlanPreviewSummary {
    total_actions: usize,
    files_write: usize,
    files_merge: usize,
    consent_required: usize,
    backup_required: usize,
    backup_path: String,
}

#[derive(Debug, serde::Serialize)]
struct PlanPreviewOp {
    path: String,
    kind: String,
    scope: String,
    consent_required: bool,
    backup_required: bool,
    set_executable: bool,
    explain: String,
    diff_kind: String,
    diff: Option<String>,
    diff_truncated: bool,
}

#[derive(Debug, serde::Serialize)]
struct PlanPreviewOutput {
    summary: PlanPreviewSummary,
    operations: Vec<PlanPreviewOp>,
}

fn render_plan_preview(
    paths: &macc_core::ProjectPaths,
    plan: &macc_core::plan::ActionPlan,
    ops: &[macc_core::plan::PlannedOp],
    json_output: bool,
    explain: bool,
) -> Result<()> {
    // Keep core validation behavior from legacy preview.
    macc_core::validate_plan(plan, true)?;
    let summary = build_plan_preview_summary(paths, plan, ops);

    if json_output {
        let payload = PlanPreviewOutput {
            summary,
            operations: build_plan_preview_ops(ops, true),
        };
        let rendered = serde_json::to_string_pretty(&payload).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize plan JSON output: {}", e))
        })?;
        println!("{}", rendered);
        return Ok(());
    }

    print_plan_preview_summary(&summary);
    print_plan_preview_ops(ops, explain);
    println!("Core: Total actions planned: {}", plan.actions.len());
    Ok(())
}

fn build_plan_preview_summary(
    paths: &macc_core::ProjectPaths,
    plan: &macc_core::plan::ActionPlan,
    ops: &[macc_core::plan::PlannedOp],
) -> PlanPreviewSummary {
    let files_write = ops
        .iter()
        .filter(|op| op.kind == macc_core::plan::PlannedOpKind::Write)
        .count();
    let files_merge = ops
        .iter()
        .filter(|op| op.kind == macc_core::plan::PlannedOpKind::Merge)
        .count();
    let consent_required = ops.iter().filter(|op| op.consent_required).count();
    let backup_required = ops.iter().filter(|op| op.metadata.backup_required).count();
    PlanPreviewSummary {
        total_actions: plan.actions.len(),
        files_write,
        files_merge,
        consent_required,
        backup_required,
        backup_path: paths.backups_dir.display().to_string(),
    }
}

fn print_plan_preview_summary(summary: &PlanPreviewSummary) {
    println!("Plan summary:");
    println!(
        "  - files write: {} | merges: {} | user-level changes: {}{}",
        summary.files_write,
        summary.files_merge,
        summary.consent_required,
        if summary.consent_required > 0 {
            " (consent required)"
        } else {
            ""
        }
    );
    println!(
        "  - backup-required ops: {} | backup path: {}",
        summary.backup_required, summary.backup_path
    );
}

fn build_plan_preview_ops(
    ops: &[macc_core::plan::PlannedOp],
    include_diff: bool,
) -> Vec<PlanPreviewOp> {
    ops.iter()
        .map(|op| {
            let mut diff_kind = "unsupported".to_string();
            let mut diff = None;
            let mut truncated = false;
            if include_diff {
                let view = macc_core::plan::render_diff(op);
                diff_kind = match view.kind {
                    macc_core::plan::DiffViewKind::Text => "text".to_string(),
                    macc_core::plan::DiffViewKind::Json => "json".to_string(),
                    macc_core::plan::DiffViewKind::Unsupported => "unsupported".to_string(),
                };
                truncated = view.truncated;
                if !view.diff.is_empty() {
                    diff = Some(view.diff);
                }
            }
            PlanPreviewOp {
                path: op.path.clone(),
                kind: format!("{:?}", op.kind).to_ascii_lowercase(),
                scope: match op.scope {
                    macc_core::plan::Scope::Project => "project".into(),
                    macc_core::plan::Scope::User => "user".into(),
                },
                consent_required: op.consent_required,
                backup_required: op.metadata.backup_required,
                set_executable: op.metadata.set_executable,
                explain: explain_operation(op),
                diff_kind,
                diff,
                diff_truncated: truncated,
            }
        })
        .collect()
}

fn print_plan_preview_ops(ops: &[macc_core::plan::PlannedOp], explain: bool) {
    for op in ops {
        let scope = match op.scope {
            macc_core::plan::Scope::Project => "project",
            macc_core::plan::Scope::User => "user",
        };
        println!(
            "\n[{}] {} ({})",
            format!("{:?}", op.kind).to_ascii_uppercase(),
            op.path,
            scope
        );
        if explain {
            println!("  why: {}", explain_operation(op));
        }
        let diff_view = macc_core::plan::render_diff(op);
        if !diff_view.diff.is_empty() {
            let indented = diff_view
                .diff
                .lines()
                .map(|line| format!("    {}", line))
                .collect::<Vec<_>>()
                .join("\n");
            println!("{}", indented);
            if diff_view.truncated {
                println!("  warning: diff truncated for readability.");
            }
        } else {
            println!("  (no textual diff available)");
        }
    }
}

fn explain_operation(op: &macc_core::plan::PlannedOp) -> String {
    match op.kind {
        macc_core::plan::PlannedOpKind::Write => {
            if op.path == ".gitignore" {
                "ensures required ignore patterns are present".into()
            } else {
                "writes generated configuration/content".into()
            }
        }
        macc_core::plan::PlannedOpKind::Merge => {
            "merges generated JSON fragment into existing file".into()
        }
        macc_core::plan::PlannedOpKind::Mkdir => "creates required directory structure".into(),
        macc_core::plan::PlannedOpKind::Delete => "deletes stale managed artifact".into(),
        macc_core::plan::PlannedOpKind::Other => "normalization/supplementary operation".into(),
    }
}

fn print_pre_apply_summary(
    paths: &macc_core::ProjectPaths,
    plan: &macc_core::plan::ActionPlan,
    ops: &[macc_core::plan::PlannedOp],
) {
    let summary = build_plan_preview_summary(paths, plan, ops);
    println!("Pre-apply summary:");
    println!(
        "  - {} writes, {} merges, {} user-level changes{}",
        summary.files_write,
        summary.files_merge,
        summary.consent_required,
        if summary.consent_required > 0 {
            " (consent required)"
        } else {
            ""
        }
    );
    println!("  - backups may be created under {}", summary.backup_path);
}

fn print_pre_apply_explanations(ops: &[macc_core::plan::PlannedOp]) {
    println!("Pre-apply explain:");
    for op in ops {
        println!("  - {}: {}", op.path, explain_operation(op));
    }
}

fn confirm_user_scope_apply(
    paths: &macc_core::ProjectPaths,
    ops: &[macc_core::plan::PlannedOp],
) -> Result<()> {
    let user_ops: Vec<&macc_core::plan::PlannedOp> = ops
        .iter()
        .filter(|op| op.scope == macc_core::plan::Scope::User)
        .collect();
    if user_ops.is_empty() {
        return Ok(());
    }

    println!("\nUser-level merge confirmation required");
    println!(
        "  - {} user-scoped file(s) will be touched.",
        user_ops.len()
    );
    let preview_limit = 12usize;
    for op in user_ops.iter().take(preview_limit) {
        println!("    - {}", op.path);
    }
    if user_ops.len() > preview_limit {
        println!("    ... and {} more", user_ops.len() - preview_limit);
    }

    let user_backup_root = user_backup_root()?;
    println!(
        "  - Backups will be written under: {}",
        user_backup_root.display()
    );
    println!("  - To inspect backups: macc backups list --user");
    println!("  - To restore latest user backup set: macc restore --latest --user");
    println!(
        "  - Project backups (if any) are under: {}",
        paths.backups_dir.display()
    );

    if !confirm_yes_no("Proceed with user-level changes [y/N]? ")? {
        return Err(MaccError::Validation(
            "Apply cancelled by user at user-level merge confirmation.".into(),
        ));
    }

    Ok(())
}

fn user_backup_root() -> Result<std::path::PathBuf> {
    let home = macc_core::find_user_home().ok_or(MaccError::HomeDirNotFound)?;
    Ok(home.join(".macc/backups"))
}

fn backup_root(paths: &macc_core::ProjectPaths, user: bool) -> Result<std::path::PathBuf> {
    if user {
        user_backup_root()
    } else {
        Ok(paths.backups_dir.clone())
    }
}

fn list_backup_sets(root: &std::path::Path) -> Result<Vec<std::path::PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut sets = Vec::new();
    for entry in std::fs::read_dir(root).map_err(|e| MaccError::Io {
        path: root.to_string_lossy().into(),
        action: "read backup root".into(),
        source: e,
    })? {
        let entry = entry.map_err(|e| MaccError::Io {
            path: root.to_string_lossy().into(),
            action: "iterate backup root".into(),
            source: e,
        })?;
        let path = entry.path();
        if path.is_dir() {
            sets.push(path);
        }
    }
    sets.sort_by(|a, b| {
        let an = a.file_name().and_then(|v| v.to_str()).unwrap_or_default();
        let bn = b.file_name().and_then(|v| v.to_str()).unwrap_or_default();
        bn.cmp(an)
    });
    Ok(sets)
}

fn resolve_backup_set_path(
    paths: &macc_core::ProjectPaths,
    user: bool,
    id: Option<&str>,
    latest: bool,
) -> Result<std::path::PathBuf> {
    let root = backup_root(paths, user)?;
    let sets = list_backup_sets(&root)?;
    if sets.is_empty() {
        return Err(MaccError::Validation(format!(
            "No backup sets found in {}",
            root.display()
        )));
    }

    if latest {
        return Ok(sets[0].clone());
    }

    let id = id.ok_or_else(|| {
        MaccError::Validation("backup id is required unless --latest is provided".into())
    })?;
    let candidate = root.join(id);
    if !candidate.is_dir() {
        return Err(MaccError::Validation(format!(
            "Backup set not found: {}",
            candidate.display()
        )));
    }
    Ok(candidate)
}

pub(crate) fn list_backup_sets_command(paths: &macc_core::ProjectPaths, user: bool) -> Result<()> {
    let root = backup_root(paths, user)?;
    let sets = list_backup_sets(&root)?;
    if sets.is_empty() {
        println!("No backup sets in {}", root.display());
        return Ok(());
    }
    println!("Backup sets in {}:", root.display());
    for set in sets {
        let id = set.file_name().and_then(|v| v.to_str()).unwrap_or_default();
        let files = count_files_recursive(&set)?;
        println!("  - {} ({} file(s))", id, files);
    }
    Ok(())
}

pub(crate) fn open_backup_set_command(
    paths: &macc_core::ProjectPaths,
    id: Option<&str>,
    latest: bool,
    user: bool,
    editor: &Option<String>,
) -> Result<()> {
    let set = resolve_backup_set_path(paths, user, id, latest)?;
    println!("Backup set: {}", set.display());
    if let Some(cmd) = editor {
        services::worktree::open_in_editor(&set, cmd)?;
    }
    Ok(())
}

pub(crate) fn restore_backup_set_command(
    paths: &macc_core::ProjectPaths,
    user: bool,
    id: Option<&str>,
    latest: bool,
    dry_run: bool,
    yes: bool,
) -> Result<()> {
    let set = resolve_backup_set_path(paths, user, id, latest)?;
    let target_root = if user {
        macc_core::find_user_home().ok_or(MaccError::HomeDirNotFound)?
    } else {
        paths.root.clone()
    };

    let files = collect_files_recursive(&set)?;
    if files.is_empty() {
        println!("Backup set {} is empty.", set.display());
        return Ok(());
    }

    println!("Restore source: {}", set.display());
    println!("Restore target: {}", target_root.display());
    println!("Files to restore: {}", files.len());
    if dry_run {
        for (idx, file) in files.iter().enumerate() {
            if idx >= 20 {
                println!("  ... and {} more", files.len() - idx);
                break;
            }
            let rel = file.strip_prefix(&set).unwrap_or(file.as_path());
            println!("  - {}", rel.display());
        }
        return Ok(());
    }

    if !yes && !confirm_yes_no("Proceed with restore [y/N]? ")? {
        return Err(MaccError::Validation("Restore cancelled.".into()));
    }

    let mut restored = 0usize;
    for file in files {
        let rel = file.strip_prefix(&set).map_err(|e| {
            MaccError::Validation(format!(
                "Failed to compute backup relative path for {}: {}",
                file.display(),
                e
            ))
        })?;
        let destination = target_root.join(rel);
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(|e| MaccError::Io {
                path: parent.to_string_lossy().into(),
                action: "create restore parent directory".into(),
                source: e,
            })?;
        }
        std::fs::copy(&file, &destination).map_err(|e| MaccError::Io {
            path: file.to_string_lossy().into(),
            action: format!("restore to {}", destination.display()),
            source: e,
        })?;
        restored += 1;
    }
    println!("Restored {} file(s).", restored);
    Ok(())
}

fn count_files_recursive(root: &std::path::Path) -> Result<usize> {
    Ok(collect_files_recursive(root)?.len())
}

fn collect_files_recursive(root: &std::path::Path) -> Result<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current).map_err(|e| MaccError::Io {
            path: current.to_string_lossy().into(),
            action: "read backup set directory".into(),
            source: e,
        })? {
            let entry = entry.map_err(|e| MaccError::Io {
                path: current.to_string_lossy().into(),
                action: "iterate backup set directory".into(),
                source: e,
            })?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.is_file() {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn apply_worktree(
    engine: &dyn Engine,
    repo_root: &std::path::Path,
    worktree_root: &std::path::Path,
    allow_user_scope: bool,
) -> Result<()> {
    let paths = macc_core::ProjectPaths::from_root(worktree_root);
    let canonical = load_canonical_config(&paths.config_path)?;
    let metadata = macc_core::read_worktree_metadata(worktree_root)?
        .ok_or_else(|| MaccError::Validation("Missing .macc/worktree.json".into()))?;

    let (descriptors, diagnostics) = engine.list_tools(&paths);
    services::project::report_diagnostics(&diagnostics);
    let allowed_tools: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();
    let overrides = CliOverrides::from_tools_csv(metadata.tool.as_str(), &allowed_tools)?;

    let resolved = resolve(&canonical, &overrides);
    let fetch_units = resolve_fetch_units(&paths, &resolved)?;
    let materialized_units =
        macc_adapter_shared::fetch::materialize_fetch_units(&paths, fetch_units)?;

    let mut plan = engine.plan(&paths, &canonical, &materialized_units, &overrides)?;
    let _ = engine.apply(&paths, &mut plan, allow_user_scope)?;
    macc_core::sync_context_files_from_root(repo_root, worktree_root, &canonical)?;
    Ok(())
}

// ... existing catalog functions (run_remote_search, list_skills, etc) ...

#[cfg(test)]
mod tests {
    use super::*;
    use macc_core::MaccError;
    use macc_core::TestEngine;
    use std::fs;
    use std::io;
    use std::net::TcpListener;

    fn bind_loopback() -> Option<(TcpListener, u16)> {
        match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => {
                let port = listener.local_addr().ok()?.port();
                Some((listener, port))
            }
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("Skipping test: cannot bind loopback socket ({})", e);
                None
            }
            Err(e) => panic!("Failed to bind loopback socket: {}", e),
        }
    }

    fn fixture_ids() -> Vec<String> {
        TestEngine::generate_fixture_ids(2)
    }

    fn fixture_engine(ids: &[String]) -> TestEngine {
        TestEngine::with_fixtures_for_ids(ids)
    }

    fn write_executable_script(path: &std::path::Path, content: &str) {
        std::fs::write(path, content).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(path).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(path, perms).unwrap();
        }
    }

    #[test]
    fn test_exit_code_mapping() {
        assert_eq!(get_exit_code(&MaccError::Validation("test".into())), 1);
        assert_eq!(
            get_exit_code(&MaccError::UserScopeNotAllowed("test".into())),
            2
        );
        assert_eq!(
            get_exit_code(&MaccError::Io {
                path: "test".into(),
                action: "test".into(),
                source: io::Error::new(io::ErrorKind::Other, "test")
            }),
            3
        );
        assert_eq!(
            get_exit_code(&MaccError::ProjectRootNotFound {
                start_dir: "test".into()
            }),
            4
        );
        let yaml_err = serde_yaml::from_str::<serde_yaml::Value>("[").unwrap_err();
        assert_eq!(
            get_exit_code(&MaccError::Config {
                path: "test.yaml".into(),
                source: yaml_err
            }),
            5
        );
        assert_eq!(
            get_exit_code(&MaccError::SecretDetected {
                path: "test.txt".into(),
                details: "test".into()
            }),
            6
        );
    }

    #[test]
    fn test_parse_coordinator_validate_transition_args() {
        let args = vec![
            "--from".to_string(),
            "todo".to_string(),
            "--to".to_string(),
            "claimed".to_string(),
        ];
        let parsed = WorkflowTransitionArgs::try_from(args.as_slice()).unwrap();
        let from = parsed.from;
        let to = parsed.to;
        assert_eq!(from, WorkflowState::Todo);
        assert_eq!(to, WorkflowState::Claimed);
    }

    #[test]
    fn test_validate_coordinator_transition_action_rejects_invalid() {
        let args = vec![
            "--from".to_string(),
            "todo".to_string(),
            "--to".to_string(),
            "merged".to_string(),
        ];
        let err = validate_coordinator_transition_action(&args).unwrap_err();
        assert!(err.to_string().contains("invalid transition"));
    }

    #[test]
    fn test_parse_coordinator_validate_runtime_transition_args() {
        let args = vec![
            "--from".to_string(),
            "running".to_string(),
            "--to".to_string(),
            "phase_done".to_string(),
        ];
        let parsed = RuntimeTransitionArgs::try_from(args.as_slice()).unwrap();
        let from = parsed.from;
        let to = parsed.to;
        assert_eq!(from, RuntimeStatus::Running);
        assert_eq!(to, RuntimeStatus::PhaseDone);
    }

    #[test]
    fn test_validate_coordinator_runtime_transition_action_rejects_invalid() {
        let args = vec![
            "--from".to_string(),
            "idle".to_string(),
            "--to".to_string(),
            "phase_done".to_string(),
        ];
        let err = validate_coordinator_runtime_transition_action(&args).unwrap_err();
        assert!(err.to_string().contains("invalid runtime transition"));
    }

    #[test]
    fn test_parse_coordinator_runtime_status_from_event_args() {
        let args = vec![
            "--type".to_string(),
            "heartbeat".to_string(),
            "--status".to_string(),
            "running".to_string(),
        ];
        let parsed = RuntimeStatusFromEventArgs::try_from(args.as_slice()).unwrap();
        let event_type = parsed.event_type;
        let status = parsed.status;
        assert_eq!(event_type, "heartbeat");
        assert_eq!(status, "running");
    }

    #[test]
    fn test_parse_coordinator_storage_sync_args() {
        let args = vec!["--direction".to_string(), "import".to_string()];
        let direction = StorageSyncArgs::try_from(args.as_slice()).unwrap().direction;
        assert_eq!(direction, CoordinatorStorageTransfer::ImportJsonToSqlite);
    }

    #[test]
    fn test_read_coordinator_counts() {
        let root = std::env::temp_dir().join(format!("macc_counts_test_{}", uuid_v4_like()));
        let registry = root
            .join(".macc")
            .join("automation")
            .join("task")
            .join("task_registry.json");
        std::fs::create_dir_all(registry.parent().unwrap()).unwrap();
        std::fs::write(
            &registry,
            r#"{
  "tasks": [
    {"id":"A","state":"todo"},
    {"id":"B","state":"in_progress"},
    {"id":"C","state":"blocked"},
    {"id":"D","state":"merged"},
    {"id":"E","state":"queued"}
  ]
}"#,
        )
        .unwrap();
        let paths = macc_core::ProjectPaths::from_root(&root);
        let counts = read_coordinator_counts(&paths).unwrap();
        assert_eq!(counts.total, 5);
        assert_eq!(counts.todo, 1);
        assert_eq!(counts.active, 2);
        assert_eq!(counts.blocked, 1);
        assert_eq!(counts.merged, 1);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_cwd_support() -> macc_core::Result<()> {
        let temp_base = std::env::temp_dir().join(format!("macc_cli_test_{}", uuid_v4_like()));
        let project_dir = temp_base.join("nested/project");
        // Do not create project_dir, let 'init' handle it (or create its parent)
        std::fs::create_dir_all(&temp_base).unwrap();

        // Mock Cli for 'init'
        let cli = Cli {
            cwd: project_dir.to_string_lossy().into(),
            verbose: true,
            command: Some(Commands::Init {
                force: false,
                wizard: false,
            }),
        };

        run_with_engine(cli, TestEngine::with_fixtures())?;

        // Verify files created
        assert!(project_dir.exists());
        assert!(project_dir.join(".macc/macc.yaml").exists());

        // Cleanup
        std::fs::remove_dir_all(&temp_base).ok();

        Ok(())
    }

    #[test]
    fn test_init_idempotence_and_force() -> macc_core::Result<()> {
        let temp_base = std::env::temp_dir().join(format!("macc_init_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();

        // 1. Initial init
        let cli = Cli {
            cwd: temp_base.to_string_lossy().into(),
            verbose: false,
            command: Some(Commands::Init {
                force: false,
                wizard: false,
            }),
        };
        run_with_engine(cli, TestEngine::with_fixtures())?;

        assert!(temp_base.join(".macc/macc.yaml").exists());
        assert!(temp_base.join(".macc/backups").is_dir());
        assert!(temp_base.join(".macc/tmp").is_dir());

        // Modify config to check if it's preserved
        let config_path = temp_base.join(".macc/macc.yaml");
        let original_content = "modified: true";
        std::fs::write(&config_path, original_content).unwrap();

        // 2. Second init without force (idempotence)
        let cli_idempotent = Cli {
            cwd: temp_base.to_string_lossy().into(),
            verbose: false,
            command: Some(Commands::Init {
                force: false,
                wizard: false,
            }),
        };
        run_with_engine(cli_idempotent, TestEngine::with_fixtures())?;

        let content = std::fs::read_to_string(&config_path).unwrap();
        assert_eq!(
            content, original_content,
            "Config should not be overwritten without --force"
        );

        // 3. Third init with force
        let cli_force = Cli {
            cwd: temp_base.to_string_lossy().into(),
            verbose: false,
            command: Some(Commands::Init {
                force: true,
                wizard: false,
            }),
        };
        run_with_engine(cli_force, TestEngine::with_fixtures())?;

        let content = std::fs::read_to_string(&config_path).unwrap();
        assert_ne!(
            content, original_content,
            "Config should be overwritten with --force"
        );
        assert!(
            content.contains("version: v1"),
            "Should contain default config"
        );

        // Cleanup
        std::fs::remove_dir_all(&temp_base).ok();

        Ok(())
    }

    #[test]
    fn test_plan_with_tools_override() -> macc_core::Result<()> {
        let temp_base = std::env::temp_dir().join(format!("macc_tools_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let ids = fixture_ids();
        let tool_one = ids[0].clone();
        let tool_two = ids[1].clone();

        // 1. Init
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Init {
                    force: false,
                    wizard: false,
                }),
            },
            fixture_engine(&ids),
        )?;

        // 2. Plan with valid tool override (using fixtures)
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Plan {
                    tools: Some(format!("{},{}", tool_one, tool_two)),
                    json: false,
                    explain: false,
                }),
            },
            fixture_engine(&ids),
        )?;

        // 3. Plan with unknown tool (should NOT error, just skip/warn)
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Plan {
                    tools: Some(format!("{},unknown", tool_one)),
                    json: false,
                    explain: false,
                }),
            },
            fixture_engine(&ids),
        )?;

        // Cleanup
        std::fs::remove_dir_all(&temp_base).ok();
        Ok(())
    }

    #[test]
    fn test_coordinator_run_full_cycle_converges() -> macc_core::Result<()> {
        let root = std::env::temp_dir().join(format!("macc_cli_coord_run_{}", uuid_v4_like()));
        std::fs::create_dir_all(&root).unwrap();
        let registry = root.join(COORDINATOR_TASK_REGISTRY_REL_PATH);
        std::fs::create_dir_all(registry.parent().expect("registry parent")).unwrap();
        fs::write(
            &registry,
            r#"{
  "schema_version": 1,
  "tasks": [
    {
      "id": "TASK-1",
      "state": "todo",
      "dependencies": [],
      "exclusive_resources": []
    }
  ],
  "resource_locks": {},
  "state_mapping": {}
}"#,
        )
        .unwrap();
        let prd_path = root.join("prd.json");
        fs::write(
            &prd_path,
            r#"{
  "lot": "Test",
  "tasks": [
    {
      "id": "TASK-1",
      "title": "Test task",
      "dependencies": [],
      "exclusive_resources": []
    }
  ]
}"#,
        )
        .unwrap();

        let canonical = macc_core::config::CanonicalConfig::default();
        let coordinator_cfg = macc_core::config::CoordinatorConfig {
            timeout_seconds: Some(10),
            ..Default::default()
        };
        let env_cfg = CoordinatorEnvConfig {
            prd: None,
            coordinator_tool: None,
            reference_branch: None,
            tool_priority: None,
            max_parallel_per_tool_json: None,
            tool_specializations_json: None,
            max_dispatch: None,
            max_parallel: None,
            timeout_seconds: Some(10),
            phase_runner_max_attempts: None,
            stale_claimed_seconds: None,
            stale_in_progress_seconds: None,
            stale_changes_requested_seconds: None,
            stale_action: None,
            storage_mode: None,
            error_code_retry_list: None,
            error_code_retry_max: None,
        };

        run_coordinator_full_cycle(&root, &canonical, Some(&coordinator_cfg), &env_cfg)?;

        let final_state: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&registry).unwrap()).unwrap();
        assert_eq!(
            final_state["tasks"][0]["state"].as_str(),
            Some("merged"),
            "coordinator run should converge to merged"
        );
        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn test_coordinator_run_detects_no_progress() -> macc_core::Result<()> {
        let root = std::env::temp_dir().join(format!("macc_cli_coord_stall_{}", uuid_v4_like()));
        std::fs::create_dir_all(&root).unwrap();
        let registry = root.join(COORDINATOR_TASK_REGISTRY_REL_PATH);
        std::fs::create_dir_all(registry.parent().expect("registry parent")).unwrap();
        fs::write(
            &registry,
            r#"{
  "schema_version": 1,
  "tasks": [
    {
      "id": "TASK-STALL",
      "state": "todo",
      "dependencies": [],
      "exclusive_resources": []
    }
  ],
  "resource_locks": {},
  "state_mapping": {}
}"#,
        )
        .unwrap();
        let prd_path = root.join("prd.json");
        fs::write(
            &prd_path,
            r#"{
  "lot": "Test",
  "tasks": [
    {
      "id": "TASK-STALL",
      "title": "Stall task",
      "dependencies": [],
      "exclusive_resources": []
    }
  ]
}"#,
        )
        .unwrap();

        let canonical = macc_core::config::CanonicalConfig::default();
        let coordinator_cfg = macc_core::config::CoordinatorConfig {
            timeout_seconds: Some(10),
            ..Default::default()
        };
        let env_cfg = CoordinatorEnvConfig {
            prd: None,
            coordinator_tool: None,
            reference_branch: None,
            tool_priority: None,
            max_parallel_per_tool_json: None,
            tool_specializations_json: None,
            max_dispatch: None,
            max_parallel: None,
            timeout_seconds: Some(10),
            phase_runner_max_attempts: None,
            stale_claimed_seconds: None,
            stale_in_progress_seconds: None,
            stale_changes_requested_seconds: None,
            stale_action: None,
            storage_mode: None,
            error_code_retry_list: None,
            error_code_retry_max: None,
        };

        let err = run_coordinator_full_cycle(&root, &canonical, Some(&coordinator_cfg), &env_cfg)
        .expect_err("stalling coordinator should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("no progress"),
            "expected no-progress error, got: {}",
            msg
        );
        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn test_coordinator_control_plane_same_input_same_final_state() -> macc_core::Result<()> {
        fn run_once(
            root: &std::path::Path,
            script: &std::path::Path,
        ) -> macc_core::Result<serde_json::Value> {
            let registry = root.join(COORDINATOR_TASK_REGISTRY_REL_PATH);
            std::fs::create_dir_all(registry.parent().expect("registry parent")).unwrap();
            fs::write(
                &registry,
                r#"{
  "schema_version": 1,
  "tasks": [
    {"id":"T1","state":"todo","dependencies":[],"exclusive_resources":[]},
    {"id":"T2","state":"todo","dependencies":[],"exclusive_resources":[]}
  ],
  "resource_locks": {},
  "state_mapping": {}
}"#,
            )
            .unwrap();

            let canonical = macc_core::config::CanonicalConfig::default();
            let coordinator_cfg = macc_core::config::CoordinatorConfig {
                timeout_seconds: Some(10),
                ..Default::default()
            };
            let env_cfg = CoordinatorEnvConfig {
                prd: None,
                coordinator_tool: None,
                reference_branch: None,
                tool_priority: None,
                max_parallel_per_tool_json: None,
                tool_specializations_json: None,
                max_dispatch: None,
                max_parallel: None,
                timeout_seconds: Some(10),
                phase_runner_max_attempts: None,
                stale_claimed_seconds: None,
                stale_in_progress_seconds: None,
                stale_changes_requested_seconds: None,
                stale_action: None,
                storage_mode: None,
                error_code_retry_list: None,
                error_code_retry_max: None,
            };

            run_coordinator_control_plane_rust(root, &canonical, Some(&coordinator_cfg), &env_cfg)?;

            let final_state: serde_json::Value =
                serde_json::from_str(&fs::read_to_string(&registry).unwrap()).unwrap();
            Ok(final_state)
        }

        let root =
            std::env::temp_dir().join(format!("macc_cli_cp_deterministic_{}", uuid_v4_like()));
        std::fs::create_dir_all(&root).unwrap();
        let script = root.join("fake-cp-deterministic.sh");
        write_executable_script(
            &script,
            r#"#!/usr/bin/env bash
set -euo pipefail
action="${1:-dispatch}"
case "$action" in
  dispatch)
    tmp="$(mktemp)"
    jq '
      .tasks |= map(
        if .state == "todo" then .state = "in_progress" else . end
      )
    ' "$TASK_REGISTRY_FILE" >"$tmp"
    mv "$tmp" "$TASK_REGISTRY_FILE"
    ;;
  advance)
    tmp="$(mktemp)"
    jq '
      .tasks |= map(
        if .state == "in_progress" then .state = "merged" else . end
      )
    ' "$TASK_REGISTRY_FILE" >"$tmp"
    mv "$tmp" "$TASK_REGISTRY_FILE"
    ;;
  sync|reconcile|cleanup) ;;
  *) ;;
esac
"#,
        );

        let first = run_once(&root, &script)?;
        let second = run_once(&root, &script)?;
        assert_eq!(first, second, "same inputs must yield same final state");

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn test_coordinator_parallel_dispatch_behavior() -> macc_core::Result<()> {
        let root =
            std::env::temp_dir().join(format!("macc_cli_parallel_dispatch_{}", uuid_v4_like()));
        std::fs::create_dir_all(&root).unwrap();
        let registry = root.join(COORDINATOR_TASK_REGISTRY_REL_PATH);
        std::fs::create_dir_all(registry.parent().expect("registry parent")).unwrap();
        fs::write(
            &registry,
            r#"{
  "schema_version": 1,
  "tasks": [
    {"id":"T1","state":"todo","dependencies":[],"exclusive_resources":[]},
    {"id":"T2","state":"todo","dependencies":[],"exclusive_resources":[]},
    {"id":"T3","state":"todo","dependencies":[],"exclusive_resources":[]}
  ],
  "resource_locks": {},
  "state_mapping": {}
}"#,
        )
        .unwrap();

        let script = root.join("fake-parallel-dispatch.sh");
        write_executable_script(
            &script,
            r#"#!/usr/bin/env bash
set -euo pipefail
action="${1:-dispatch}"
if [[ "$action" == "dispatch" ]]; then
  tmp="$(mktemp)"
  jq '
    .tasks |= (
      reduce .[] as $task ({count: 0, out: []};
        if ($task.state == "todo" and .count < 2) then
          {count: (.count + 1), out: (.out + [($task + {state: "in_progress"})])}
        else
          {count: .count, out: (.out + [$task])}
        end
      ) | .out
    )
  ' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
fi
"#,
        );

        let canonical = macc_core::config::CanonicalConfig::default();
        let env_cfg = CoordinatorEnvConfig {
            prd: None,
            coordinator_tool: None,
            reference_branch: None,
            tool_priority: None,
            max_parallel_per_tool_json: None,
            tool_specializations_json: None,
            max_dispatch: None,
            max_parallel: None,
            timeout_seconds: None,
            phase_runner_max_attempts: None,
            stale_claimed_seconds: None,
            stale_in_progress_seconds: None,
            stale_changes_requested_seconds: None,
            stale_action: None,
            storage_mode: None,
            error_code_retry_list: None,
            error_code_retry_max: None,
        };

        run_coordinator_action(&root, &script, "dispatch", &[], &canonical, None, &env_cfg)?;

        let value: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&registry).unwrap()).unwrap();
        let active = value["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t["state"].as_str() == Some("in_progress"))
            .count();
        let todo = value["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t["state"].as_str() == Some("todo"))
            .count();
        assert_eq!(active, 2, "dispatch should activate two tasks in parallel");
        assert_eq!(todo, 1, "one task should remain todo");

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn test_coordinator_retry_phase_behavior() -> macc_core::Result<()> {
        let root = std::env::temp_dir().join(format!("macc_cli_retry_phase_{}", uuid_v4_like()));
        std::fs::create_dir_all(&root).unwrap();
        let registry = root.join(COORDINATOR_TASK_REGISTRY_REL_PATH);
        std::fs::create_dir_all(registry.parent().expect("registry parent")).unwrap();
        fs::write(
            &registry,
            r#"{
  "schema_version": 1,
  "tasks": [
    {"id":"TASK-R","state":"blocked","dependencies":[],"exclusive_resources":[]}
  ],
  "resource_locks": {},
  "state_mapping": {}
}"#,
        )
        .unwrap();

        let script = root.join("fake-retry-phase.sh");
        write_executable_script(
            &script,
            r#"#!/usr/bin/env bash
set -euo pipefail
action="${1:-dispatch}"
if [[ "$action" == "retry-phase" ]]; then
  shift
  task=""
  phase=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --retry-task) task="$2"; shift 2 ;;
      --retry-phase) phase="$2"; shift 2 ;;
      *) shift ;;
    esac
  done
  [[ "$task" == "TASK-R" ]] || exit 2
  [[ "$phase" == "integrate" ]] || exit 3
  tmp="$(mktemp)"
  jq '.tasks |= map(if .id=="TASK-R" then .state="queued" else . end)' "$TASK_REGISTRY_FILE" >"$tmp"
  mv "$tmp" "$TASK_REGISTRY_FILE"
fi
"#,
        );

        let canonical = macc_core::config::CanonicalConfig::default();
        let env_cfg = CoordinatorEnvConfig {
            prd: None,
            coordinator_tool: None,
            reference_branch: None,
            tool_priority: None,
            max_parallel_per_tool_json: None,
            tool_specializations_json: None,
            max_dispatch: None,
            max_parallel: None,
            timeout_seconds: None,
            phase_runner_max_attempts: None,
            stale_claimed_seconds: None,
            stale_in_progress_seconds: None,
            stale_changes_requested_seconds: None,
            stale_action: None,
            storage_mode: None,
            error_code_retry_list: None,
            error_code_retry_max: None,
        };

        run_coordinator_action(
            &root,
            &script,
            "retry-phase",
            &[
                "--retry-task".to_string(),
                "TASK-R".to_string(),
                "--retry-phase".to_string(),
                "integrate".to_string(),
            ],
            &canonical,
            None,
            &env_cfg,
        )?;

        let value: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&registry).unwrap()).unwrap();
        assert_eq!(
            value["tasks"][0]["state"].as_str(),
            Some("queued"),
            "retry-phase integrate should update blocked task to queued in this test harness"
        );

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn test_coordinator_stop_removes_worktrees_and_branches() -> macc_core::Result<()> {
        let root = std::env::temp_dir().join(format!("macc_cli_coord_stop_{}", uuid_v4_like()));
        std::fs::create_dir_all(&root).unwrap();
        let ids = fixture_ids();
        std::fs::write(root.join("README.md"), "seed\n").unwrap();
        let _ = std::process::Command::new("git")
            .arg("-C")
            .arg(&root)
            .arg("init")
            .status()
            .unwrap();
        let _ = std::process::Command::new("git")
            .arg("-C")
            .arg(&root)
            .arg("config")
            .arg("user.email")
            .arg("macc-tests@example.com")
            .status()
            .unwrap();
        let _ = std::process::Command::new("git")
            .arg("-C")
            .arg(&root)
            .arg("config")
            .arg("user.name")
            .arg("macc-tests")
            .status()
            .unwrap();
        let _ = std::process::Command::new("git")
            .arg("-C")
            .arg(&root)
            .arg("add")
            .arg("README.md")
            .status()
            .unwrap();
        let _ = std::process::Command::new("git")
            .arg("-C")
            .arg(&root)
            .arg("commit")
            .arg("-m")
            .arg("seed")
            .status()
            .unwrap();

        run_with_engine(
            Cli {
                cwd: root.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Init {
                    force: false,
                    wizard: false,
                }),
            },
            fixture_engine(&ids),
        )?;

        // Prepare required coordinator inputs for reconcile/cleanup during stop.
        std::fs::write(
            root.join("prd.json"),
            r#"{
  "lot":"Test",
  "version":"1.0",
  "generated_at":"2026-01-01",
  "timezone":"UTC",
  "priority_mapping":{},
  "tasks":[]
}"#,
        )
        .unwrap();
        let stop_registry = root.join(COORDINATOR_TASK_REGISTRY_REL_PATH);
        std::fs::create_dir_all(stop_registry.parent().expect("registry parent")).unwrap();
        std::fs::write(
            stop_registry,
            r#"{
  "schema_version":1,
  "tasks":[],
  "resource_locks":{},
  "state_mapping":{}
}"#,
        )
        .unwrap();

        let wt_path = root.join(".macc/worktree/stop-test");
        std::fs::create_dir_all(root.join(".macc/worktree")).unwrap();
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(&root)
            .arg("worktree")
            .arg("add")
            .arg("-b")
            .arg("ai/stop-test")
            .arg(&wt_path)
            .arg("HEAD")
            .status()
            .unwrap();
        assert!(status.success(), "failed creating test worktree");

        run_with_engine(
            Cli {
                cwd: root.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Coordinator {
                    action: "stop".to_string(),
                    no_tui: true,
                    graceful: true,
                    remove_worktrees: true,
                    remove_branches: true,
                    prd: None,
                    coordinator_tool: None,
                    reference_branch: None,
                    tool_priority: None,
                    max_parallel_per_tool_json: None,
                    tool_specializations_json: None,
                    max_dispatch: None,
                    max_parallel: None,
                    timeout_seconds: None,
                    phase_runner_max_attempts: None,
                    stale_claimed_seconds: None,
                    stale_in_progress_seconds: None,
                    stale_changes_requested_seconds: None,
                    stale_action: None,
                    storage_mode: None,
                    extra_args: Vec::new(),
                }),
            },
            fixture_engine(&ids),
        )?;

        assert!(
            !wt_path.exists(),
            "worktree should be removed by coordinator stop"
        );

        let branch_check = std::process::Command::new("git")
            .arg("-C")
            .arg(&root)
            .arg("rev-parse")
            .arg("--verify")
            .arg("ai/stop-test")
            .status()
            .unwrap();
        assert!(
            !branch_check.success(),
            "branch should be deleted by coordinator stop --remove-branches"
        );

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn test_apply_with_test_adapter() -> macc_core::Result<()> {
        let temp_base = std::env::temp_dir().join(format!("macc_apply_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let ids = fixture_ids();
        let tool_one = ids[0].clone();

        // 1. Init
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Init {
                    force: false,
                    wizard: false,
                }),
            },
            fixture_engine(&ids),
        )?;

        // 2. Apply with first tool
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Apply {
                    tools: Some(tool_one.clone()),
                    dry_run: false,
                    allow_user_scope: false,
                    json: false,
                    explain: false,
                }),
            },
            fixture_engine(&ids),
        )?;

        // 3. Verify files created
        let generated_txt = temp_base.join(format!("{}-output.txt", tool_one));

        assert!(generated_txt.exists(), "expected output.txt should exist");

        let txt_content = std::fs::read_to_string(generated_txt).unwrap();
        assert!(txt_content.contains("fixture content for"));

        // Cleanup
        std::fs::remove_dir_all(&temp_base).ok();
        Ok(())
    }

    #[test]
    fn test_context_requires_prior_apply() -> macc_core::Result<()> {
        let temp_base =
            std::env::temp_dir().join(format!("macc_context_gate_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();

        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Init {
                    force: false,
                    wizard: false,
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        let err = run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Context {
                    tool: None,
                    from_files: Vec::new(),
                    dry_run: true,
                    print_prompt: false,
                }),
            },
            TestEngine::with_fixtures(),
        )
        .expect_err("context should require at least one successful apply");

        let msg = err.to_string();
        assert!(
            msg.contains("at least one successful 'macc apply'"),
            "unexpected error message: {}",
            msg
        );

        std::fs::remove_dir_all(&temp_base).ok();
        Ok(())
    }

    #[test]
    fn test_catalog_skills_workflow() -> macc_core::Result<()> {
        let temp_base =
            std::env::temp_dir().join(format!("macc_catalog_cli_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();

        // 1. Init
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Init {
                    force: false,
                    wizard: false,
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        // 2. Add skill
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::Skills {
                        skills_command: CatalogSubCommands::Add {
                            id: "test-skill".into(),
                            name: "Test Skill".into(),
                            description: "A test skill".into(),
                            tags: Some("tag1,tag2".into()),
                            subpath: "path".into(),
                            kind: "git".into(),
                            url: "https://github.com/test/test.git".into(),
                            reference: "main".into(),
                            checksum: None,
                        },
                    },
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        let catalog_path = macc_core::ProjectPaths::from_root(&temp_base).skills_catalog_path();
        assert!(catalog_path.exists());

        // 3. List skills (mostly for coverage and ensuring no crash)
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::Skills {
                        skills_command: CatalogSubCommands::List,
                    },
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        // 4. Search skill
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::Skills {
                        skills_command: CatalogSubCommands::Search {
                            query: "test".into(),
                        },
                    },
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        // 5. Remove skill
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::Skills {
                        skills_command: CatalogSubCommands::Remove {
                            id: "test-skill".into(),
                        },
                    },
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        let catalog = SkillsCatalog::load(&catalog_path)?;
        assert_eq!(catalog.entries.len(), 0);

        // Cleanup
        fs::remove_dir_all(&temp_base).ok();
        Ok(())
    }

    #[test]
    fn test_catalog_mcp_workflow() -> macc_core::Result<()> {
        let temp_base = std::env::temp_dir().join(format!("macc_mcp_cli_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();

        // 1. Init
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Init {
                    force: false,
                    wizard: false,
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        // 2. Add MCP
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::Mcp {
                        mcp_command: CatalogSubCommands::Add {
                            id: "test-mcp".into(),
                            name: "Test MCP".into(),
                            description: "A test MCP".into(),
                            tags: Some("mcp".into()),
                            subpath: "".into(),
                            kind: "http".into(),
                            url: "https://example.com/mcp.zip".into(),
                            reference: "".into(),
                            checksum: Some("sha256:123".into()),
                        },
                    },
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        let catalog_path = macc_core::ProjectPaths::from_root(&temp_base).mcp_catalog_path();
        assert!(catalog_path.exists());

        // 3. List MCP
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::Mcp {
                        mcp_command: CatalogSubCommands::List,
                    },
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        // 4. Search MCP
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::Mcp {
                        mcp_command: CatalogSubCommands::Search {
                            query: "mcp".into(),
                        },
                    },
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        // 5. Remove MCP
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::Mcp {
                        mcp_command: CatalogSubCommands::Remove {
                            id: "test-mcp".into(),
                        },
                    },
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        let catalog = McpCatalog::load(&catalog_path)?;
        assert_eq!(catalog.entries.len(), 0);

        // Cleanup
        std::fs::remove_dir_all(&temp_base).ok();
        Ok(())
    }

    #[test]
    fn test_install_skill_cli() -> macc_core::Result<()> {
        let temp_base =
            std::env::temp_dir().join(format!("macc_install_skill_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let ids = fixture_ids();
        let tool_one = ids[0].clone();

        // 1. Init
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Init {
                    force: false,
                    wizard: false,
                }),
            },
            fixture_engine(&ids),
        )?;

        // 2. Add skill to catalog
        let skill_source_dir = temp_base.join("remote_skill");
        std::fs::create_dir_all(&skill_source_dir).unwrap();
        let manifest = format!(
            r#"{{
  "type": "skill",
  "id": "remote-skill",
  "version": "0.1.0",
  "targets": {{
    "{tool_one}": [
      {{ "src": "SKILL.md", "dest": ".{tool_one}/skills/remote-skill/SKILL.md" }}
    ]
  }}
}}
"#
        );
        std::fs::write(skill_source_dir.join("macc.package.json"), manifest).unwrap();
        std::fs::write(skill_source_dir.join("SKILL.md"), "remote content").unwrap();

        std::process::Command::new("git")
            .args(["init", "-b", "main"])
            .current_dir(&skill_source_dir)
            .status()
            .unwrap();
        std::process::Command::new("git")
            .args(["config", "user.email", "test@example.com"])
            .current_dir(&skill_source_dir)
            .status()
            .unwrap();
        std::process::Command::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(&skill_source_dir)
            .status()
            .unwrap();
        std::process::Command::new("git")
            .args(["add", "."])
            .current_dir(&skill_source_dir)
            .status()
            .unwrap();
        std::process::Command::new("git")
            .args(["commit", "-m", "initial"])
            .current_dir(&skill_source_dir)
            .status()
            .unwrap();

        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::Skills {
                        skills_command: CatalogSubCommands::Add {
                            id: "remote-skill".into(),
                            name: "Remote Skill".into(),
                            description: "desc".into(),
                            tags: None,
                            subpath: "".into(),
                            kind: "git".into(),
                            url: skill_source_dir.to_string_lossy().into(),
                            reference: "main".into(),
                            checksum: None,
                        },
                    },
                }),
            },
            fixture_engine(&ids),
        )?;

        // 3. Install skill
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Install {
                    install_command: InstallCommands::Skill {
                        tool: tool_one.clone(),
                        id: "remote-skill".into(),
                    },
                }),
            },
            fixture_engine(&ids),
        )?;

        // 4. Verify installation
        let installed_file = temp_base.join(format!(".{}/skills/remote-skill/SKILL.md", tool_one));
        assert!(installed_file.exists());
        assert_eq!(
            std::fs::read_to_string(installed_file).unwrap(),
            "remote content"
        );

        // Cleanup
        std::fs::remove_dir_all(&temp_base).ok();
        Ok(())
    }

    #[test]
    fn test_install_mcp_cli() -> macc_core::Result<()> {
        let temp_base =
            std::env::temp_dir().join(format!("macc_install_mcp_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();

        // 1. Init
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Init {
                    force: false,
                    wizard: false,
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        // 2. Prepare MCP source (git repo)
        let mcp_source_dir = temp_base.join("remote_mcp");
        std::fs::create_dir_all(&mcp_source_dir).unwrap();
        let manifest = serde_json::json!({
            "type": "mcp",
            "id": "remote-mcp",
            "version": "1.0.0",
            "mcp": {
                "server": {
                    "command": "node",
                    "args": ["index.js"]
                }
            },
            "merge_target": "mcpServers.remote-mcp"
        });
        std::fs::write(
            mcp_source_dir.join("macc.package.json"),
            serde_json::to_string(&manifest).unwrap(),
        )
        .unwrap();

        std::process::Command::new("git")
            .args(["init", "-b", "main"])
            .current_dir(&mcp_source_dir)
            .status()
            .unwrap();
        std::process::Command::new("git")
            .args(["config", "user.email", "test@example.com"])
            .current_dir(&mcp_source_dir)
            .status()
            .unwrap();
        std::process::Command::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(&mcp_source_dir)
            .status()
            .unwrap();
        std::process::Command::new("git")
            .args(["add", "."])
            .current_dir(&mcp_source_dir)
            .status()
            .unwrap();
        std::process::Command::new("git")
            .args(["commit", "-m", "initial"])
            .current_dir(&mcp_source_dir)
            .status()
            .unwrap();

        // 3. Add to catalog
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::Mcp {
                        mcp_command: CatalogSubCommands::Add {
                            id: "remote-mcp".into(),
                            name: "Remote MCP".into(),
                            description: "desc".into(),
                            tags: None,
                            subpath: "".into(),
                            kind: "git".into(),
                            url: mcp_source_dir.to_string_lossy().into(),
                            reference: "main".into(),
                            checksum: None,
                        },
                    },
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        // 4. Install MCP
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Install {
                    install_command: InstallCommands::Mcp {
                        id: "remote-mcp".into(),
                    },
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        // 5. Verify .mcp.json update
        let mcp_json = temp_base.join(".mcp.json");
        assert!(mcp_json.exists());
        let content = std::fs::read_to_string(mcp_json).unwrap();
        let json: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(
            json["mcpServers"]["remote-mcp"]["command"],
            serde_json::Value::String("node".into())
        );

        // Cleanup
        std::fs::remove_dir_all(&temp_base).ok();
        Ok(())
    }

    #[test]
    fn test_catalog_import_url() -> macc_core::Result<()> {
        let temp_base =
            std::env::temp_dir().join(format!("macc_catalog_import_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();

        // 1. Init
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Init {
                    force: false,
                    wizard: false,
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        // 2. Import Skill from GitHub tree URL
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::ImportUrl {
                        kind: "skill".into(),
                        id: "imported-skill".into(),
                        url: "https://github.com/org/repo/tree/v1.0/path/to/skill".into(),
                        name: Some("Imported Skill".into()),
                        description: "Imported from URL".into(),
                        tags: Some("import".into()),
                    },
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        // Verify Skill Catalog
        let skills_catalog_path =
            macc_core::ProjectPaths::from_root(&temp_base).skills_catalog_path();
        let skills_catalog = SkillsCatalog::load(&skills_catalog_path)?;
        assert_eq!(skills_catalog.entries.len(), 1);
        let entry = &skills_catalog.entries[0];
        assert_eq!(entry.id, "imported-skill");
        assert_eq!(entry.name, "Imported Skill");
        assert_eq!(entry.selector.subpath, "path/to/skill");
        assert_eq!(entry.source.url, "https://github.com/org/repo.git");
        assert_eq!(entry.source.reference, "v1.0");

        // 3. Import MCP from GitHub root URL (implicit main/empty subpath)
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::ImportUrl {
                        kind: "mcp".into(),
                        id: "imported-mcp".into(),
                        url: "https://github.com/org/mcp-repo".into(),
                        name: None,
                        description: "Imported MCP".into(),
                        tags: None,
                    },
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        // Verify MCP Catalog
        let mcp_catalog_path = macc_core::ProjectPaths::from_root(&temp_base).mcp_catalog_path();
        let mcp_catalog = McpCatalog::load(&mcp_catalog_path)?;
        assert_eq!(mcp_catalog.entries.len(), 1);
        let entry = &mcp_catalog.entries[0];
        assert_eq!(entry.id, "imported-mcp");
        assert_eq!(entry.name, "imported-mcp"); // Default to ID
        assert_eq!(entry.selector.subpath, "");
        assert_eq!(entry.source.url, "https://github.com/org/mcp-repo.git");
        assert_eq!(entry.source.reference, "");

        // Cleanup
        std::fs::remove_dir_all(&temp_base).ok();
        Ok(())
    }

    #[test]
    fn test_search_remote_cli() -> macc_core::Result<()> {
        use std::io::{BufRead, BufReader, Write};
        use std::thread;

        // Mock server
        let (listener, port) = match bind_loopback() {
            Some(v) => v,
            None => return Ok(()),
        };
        let server_url = format!("http://127.0.0.1:{}", port);

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(&mut stream);
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            // Consume headers
            while line.trim() != "" {
                line.clear();
                reader.read_line(&mut line).unwrap();
            }

            // Return mock response
            let response_body = r#"{
                "items": [
                    {
                        "id": "remote-skill-1",
                        "name": "Remote Skill 1",
                        "description": "Desc",
                        "tags": ["remote"],
                        "selector": {"subpath": ""},
                        "source": {
                            "kind": "git",
                            "url": "https://example.com/repo.git",
                            "ref": "main",
                            "checksum": null
                        }
                    }
                ]
            }"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        let temp_base =
            std::env::temp_dir().join(format!("macc_search_remote_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();

        // 1. Init
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Init {
                    force: false,
                    wizard: false,
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        // 2. Search remote and add
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::SearchRemote {
                        api: server_url,
                        kind: "skill".into(),
                        q: "test".into(),
                        add: true,
                        add_ids: None,
                    },
                }),
            },
            TestEngine::with_fixtures(),
        )?;

        // 3. Verify it was added to catalog
        let catalog_path = macc_core::ProjectPaths::from_root(&temp_base).skills_catalog_path();
        let catalog = SkillsCatalog::load(&catalog_path)?;
        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.entries[0].id, "remote-skill-1");

        std::fs::remove_dir_all(&temp_base).ok();
        Ok(())
    }

    #[test]
    fn test_install_skill_multi_zip_cli() -> macc_core::Result<()> {
        use std::io::{BufRead, BufReader, Write};
        use std::thread;

        let ids = fixture_ids();
        let tool_one = ids[0].clone();

        // 1. Prepare a zip file containing two skills
        let archive_bytes = {
            let mut buf = Vec::new();
            {
                let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
                let options = zip::write::SimpleFileOptions::default();

                let manifest_a = format!(
                    r#"{{
  "type": "skill",
  "id": "skill-a",
  "version": "0.1.0",
  "targets": {{
    "{tool_one}": [
      {{ "src": "SKILL.md", "dest": ".{tool_one}/skills/skill-a/SKILL.md" }}
    ]
  }}
}}
"#
                );
                zip.start_file("skills/a/macc.package.json", options)
                    .unwrap();
                zip.write_all(manifest_a.as_bytes()).unwrap();
                zip.start_file("skills/a/SKILL.md", options).unwrap();
                zip.write_all(b"content a").unwrap();

                let manifest_b = format!(
                    r#"{{
  "type": "skill",
  "id": "skill-b",
  "version": "0.1.0",
  "targets": {{
    "{tool_one}": [
      {{ "src": "SKILL.md", "dest": ".{tool_one}/skills/skill-b/SKILL.md" }}
    ]
  }}
}}
"#
                );
                zip.start_file("skills/b/macc.package.json", options)
                    .unwrap();
                zip.write_all(manifest_b.as_bytes()).unwrap();
                zip.start_file("skills/b/SKILL.md", options).unwrap();
                zip.write_all(b"content b").unwrap();

                zip.finish().unwrap();
            }
            buf
        };

        // 2. Mock server to serve this zip
        let (listener, port) = match bind_loopback() {
            Some(v) => v,
            None => return Ok(()),
        };
        let server_url = format!("http://127.0.0.1:{}/skills.zip", port);

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(&mut stream);
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            // Consume headers
            while line.trim() != "" {
                line.clear();
                reader.read_line(&mut line).unwrap();
            }

            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/zip\r\nContent-Length: {}\r\n\r\n",
                archive_bytes.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
            stream.write_all(&archive_bytes).unwrap();
        });

        let temp_base =
            std::env::temp_dir().join(format!("macc_install_multi_zip_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();

        // 3. Init
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Init {
                    force: false,
                    wizard: false,
                }),
            },
            fixture_engine(&ids),
        )?;

        // 4. Add skill 'a' to catalog pointing to the zip with subpath 'skills/a'
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::Skills {
                        skills_command: CatalogSubCommands::Add {
                            id: "skill-a".into(),
                            name: "Skill A".into(),
                            description: "desc a".into(),
                            tags: None,
                            subpath: "skills/a".into(),
                            kind: "http".into(),
                            url: server_url,
                            reference: "".into(),
                            checksum: None,
                        },
                    },
                }),
            },
            fixture_engine(&ids),
        )?;

        // 5. Install skill 'a'
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Install {
                    install_command: InstallCommands::Skill {
                        tool: tool_one.clone(),
                        id: "skill-a".into(),
                    },
                }),
            },
            fixture_engine(&ids),
        )?;

        // 6. Verify skill 'a' exists and 'b' does not
        let skill_a_file = temp_base.join(format!(".{}/skills/skill-a/SKILL.md", tool_one));
        assert!(skill_a_file.exists(), "Skill A should be installed");
        assert_eq!(std::fs::read_to_string(skill_a_file).unwrap(), "content a");

        let skill_b_dir = temp_base.join(format!(".{}/skills/skill-b", tool_one));
        assert!(!skill_b_dir.exists(), "Skill B should NOT be installed");

        // Also ensure that the parent 'skills/a' subpath didn't leak into the destination path
        assert!(!temp_base
            .join(format!(".{}/skills/skill-a/skills", tool_one))
            .exists());

        std::fs::remove_dir_all(&temp_base).ok();
        Ok(())
    }

    #[test]
    fn test_install_skill_multi_git_cli() -> macc_core::Result<()> {
        use std::process::Command;

        let temp_base =
            std::env::temp_dir().join(format!("macc_install_multi_git_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let ids = fixture_ids();
        let tool_one = ids[0].clone();

        let repo_path = temp_base.join("remote_repo");
        std::fs::create_dir_all(&repo_path).unwrap();

        // 1. Initialize a local git repo
        let run_git = |args: &[&str], dir: &std::path::Path| {
            let output = Command::new("git")
                .args(args)
                .current_dir(dir)
                .output()
                .expect("Failed to execute git command");
            if !output.status.success() {
                panic!(
                    "git command failed: {:?} -> {}",
                    args,
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        };

        run_git(&["init"], &repo_path);
        // Set user info for commits
        run_git(&["config", "user.email", "test@example.com"], &repo_path);
        run_git(&["config", "user.name", "Test User"], &repo_path);
        run_git(&["config", "commit.gpgsign", "false"], &repo_path);
        run_git(&["checkout", "-b", "main"], &repo_path);

        let skill_a_dir = repo_path.join("skills/a");
        std::fs::create_dir_all(&skill_a_dir).unwrap();
        let manifest_a = format!(
            r#"{{
  "type": "skill",
  "id": "skill-a",
  "version": "0.1.0",
  "targets": {{
    "{tool_one}": [
      {{ "src": "SKILL.md", "dest": ".{tool_one}/skills/skill-a/SKILL.md" }}
    ]
  }}
}}
"#
        );
        std::fs::write(skill_a_dir.join("macc.package.json"), manifest_a).unwrap();
        std::fs::write(skill_a_dir.join("SKILL.md"), "content a").unwrap();

        let skill_b_dir = repo_path.join("skills/b");
        std::fs::create_dir_all(&skill_b_dir).unwrap();
        let manifest_b = format!(
            r#"{{
  "type": "skill",
  "id": "skill-b",
  "version": "0.1.0",
  "targets": {{
    "{tool_one}": [
      {{ "src": "SKILL.md", "dest": ".{tool_one}/skills/skill-b/SKILL.md" }}
    ]
  }}
}}
"#
        );
        std::fs::write(skill_b_dir.join("macc.package.json"), manifest_b).unwrap();
        std::fs::write(skill_b_dir.join("SKILL.md"), "content b").unwrap();

        run_git(&["add", "."], &repo_path);
        run_git(&["commit", "-m", "initial commit"], &repo_path);

        let repo_url = format!("file://{}", repo_path.to_string_lossy());

        let project_path = temp_base.join("project");
        std::fs::create_dir_all(&project_path).unwrap();

        // 2. Init MACC project
        run_with_engine(
            Cli {
                cwd: project_path.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Init {
                    force: false,
                    wizard: false,
                }),
            },
            fixture_engine(&ids),
        )?;

        // 3. Add skills 'a' and 'b' to catalog pointing to the same git repo
        run_with_engine(
            Cli {
                cwd: project_path.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::Skills {
                        skills_command: CatalogSubCommands::Add {
                            id: "skill-a".into(),
                            name: "Skill A".into(),
                            description: "desc a".into(),
                            tags: None,
                            subpath: "skills/a".into(),
                            kind: "git".into(),
                            url: repo_url.clone(),
                            reference: "main".into(),
                            checksum: None,
                        },
                    },
                }),
            },
            fixture_engine(&ids),
        )?;

        run_with_engine(
            Cli {
                cwd: project_path.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::Skills {
                        skills_command: CatalogSubCommands::Add {
                            id: "skill-b".into(),
                            name: "Skill B".into(),
                            description: "desc b".into(),
                            tags: None,
                            subpath: "skills/b".into(),
                            kind: "git".into(),
                            url: repo_url,
                            reference: "main".into(),
                            checksum: None,
                        },
                    },
                }),
            },
            fixture_engine(&ids),
        )?;

        // 4. Install skill 'a'
        run_with_engine(
            Cli {
                cwd: project_path.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Install {
                    install_command: InstallCommands::Skill {
                        tool: tool_one.clone(),
                        id: "skill-a".into(),
                    },
                }),
            },
            fixture_engine(&ids),
        )?;

        // 5. Verify skill 'a' exists and 'b' does not in the project
        let skill_a_file = project_path.join(format!(".{}/skills/skill-a/SKILL.md", tool_one));
        assert!(skill_a_file.exists(), "Skill A should be installed");
        assert_eq!(std::fs::read_to_string(skill_a_file).unwrap(), "content a");

        let skill_b_dir = project_path.join(format!(".{}/skills/skill-b", tool_one));
        assert!(!skill_b_dir.exists(), "Skill B should NOT be installed");

        // 6. Verify sparse checkout in cache (project cache or shared user cache)
        let mut found_cache = false;
        let mut found_sparse_match = false;
        let mut cache_roots = vec![project_path.join(".macc/cache")];
        if let Some(home) = std::env::var_os("HOME") {
            cache_roots.push(std::path::PathBuf::from(home).join(".macc/cache"));
        }
        for cache_dir in cache_roots {
            if let Ok(entries) = std::fs::read_dir(cache_dir) {
                for entry in entries.flatten() {
                    let repo_dir = entry.path().join("repo");
                    if repo_dir.exists() {
                        found_cache = true;
                        // Look for the cache entry matching this test's sparse checkout.
                        if repo_dir.join("skills/a").exists() {
                            assert!(
                                !repo_dir.join("skills/b").exists(),
                                "skills/b should NOT be materialized in sparse checkout"
                            );
                            found_sparse_match = true;
                        }
                    }
                }
            }
        }
        assert!(found_cache, "Cache entry for git repo should exist");
        assert!(
            found_sparse_match,
            "Expected at least one sparse cache entry with skills/a"
        );

        std::fs::remove_dir_all(&temp_base).ok();
        Ok(())
    }

    #[test]
    #[cfg(unix)]
    fn test_install_skill_rejects_symlink_cli() -> macc_core::Result<()> {
        use std::io::{BufRead, BufReader, Write};
        use std::thread;

        let ids = fixture_ids();
        let tool_one = ids[0].clone();

        // 1. Prepare a zip file containing a symlink
        let archive_bytes = {
            let mut buf = Vec::new();
            {
                let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
                let options = zip::write::SimpleFileOptions::default();

                let manifest = format!(
                    r#"{{
  "type": "skill",
  "id": "symlink-skill",
  "version": "0.1.0",
  "targets": {{
    "{tool_one}": [
      {{ "src": "SKILL.md", "dest": ".{tool_one}/skills/symlink-skill/SKILL.md" }}
    ]
  }}
}}
"#
                );
                zip.start_file("macc.package.json", options).unwrap();
                zip.write_all(manifest.as_bytes()).unwrap();
                zip.start_file("SKILL.md", options).unwrap();
                zip.write_all(b"real content").unwrap();

                zip.add_symlink("link.txt", "SKILL.md", options).unwrap();

                zip.finish().unwrap();
            }
            buf
        };

        // 2. Mock server
        let (listener, port) = match bind_loopback() {
            Some(v) => v,
            None => return Ok(()),
        };
        let server_url = format!("http://127.0.0.1:{}/malicious.zip", port);

        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut reader = BufReader::new(&mut stream);
                let mut line = String::new();
                let _ = reader.read_line(&mut line);
                while line.trim() != "" {
                    line.clear();
                    let _ = reader.read_line(&mut line);
                }

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/zip\r\nContent-Length: {}\r\n\r\n",
                    archive_bytes.len()
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.write_all(&archive_bytes);
            }
        });

        let temp_base =
            std::env::temp_dir().join(format!("macc_install_symlink_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();

        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Init {
                    force: false,
                    wizard: false,
                }),
            },
            fixture_engine(&ids),
        )?;

        // Add to catalog
        run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Catalog {
                    catalog_command: CatalogCommands::Skills {
                        skills_command: CatalogSubCommands::Add {
                            id: "malicious".into(),
                            name: "Malicious".into(),
                            description: "desc".into(),
                            tags: None,
                            subpath: "".into(),
                            kind: "http".into(),
                            url: server_url,
                            reference: "".into(),
                            checksum: None,
                        },
                    },
                }),
            },
            fixture_engine(&ids),
        )?;

        // Try install
        let result = run_with_engine(
            Cli {
                cwd: temp_base.to_string_lossy().into(),
                verbose: false,
                command: Some(Commands::Install {
                    install_command: InstallCommands::Skill {
                        tool: tool_one,
                        id: "malicious".into(),
                    },
                }),
            },
            fixture_engine(&ids),
        );

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Symlinks are not supported"),
            "Error message should mention symlinks: {}",
            err_msg
        );

        std::fs::remove_dir_all(&temp_base).ok();
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

    #[test]
    fn test_run_version_command_generic() {
        let cmd = macc_core::tool::ToolInstallCommand {
            command: "bash".to_string(),
            args: vec!["-lc".to_string(), "echo v1.2.3".to_string()],
        };
        assert_eq!(run_version_command(&cmd), Some("1.2.3".to_string()));
    }

    #[test]
    fn test_extract_version_token() {
        assert_eq!(
            extract_version_token("tool version v0.101.0"),
            Some("0.101.0".to_string())
        );
        assert_eq!(
            extract_version_token("my-cli 1.2.3-beta"),
            Some("1.2.3-beta".to_string())
        );
        assert_eq!(extract_version_token("no version here"), None);
    }
}
