use clap::{Parser, Subcommand};
use commands::Command;
#[cfg(test)]
use macc_core::coordinator::{RuntimeStatus, WorkflowState};
#[cfg(test)]
use macc_core::coordinator_storage::CoordinatorStorageTransfer;
use macc_core::engine::MaccEngine;
#[cfg(test)]
use macc_core::Engine;
use macc_core::{MaccError, Result};
use std::process::exit;
use tracing::{debug, error, info};

mod commands;
mod coordinator;
mod services;
#[cfg(test)]
mod test_support;

#[cfg(test)]
use macc_core::coordinator::args::{
    RuntimeStatusFromEventArgs, RuntimeTransitionArgs, StorageSyncArgs, WorkflowTransitionArgs,
};
use macc_core::coordinator::types::CoordinatorEnvConfig;

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
        /// Debounce SQLite -> JSON compatibility export in milliseconds (0 disables debounce)
        #[arg(long)]
        mirror_json_debounce_ms: Option<u64>,
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
    init_tracing(cli.verbose);

    if cli.verbose {
        info!("Verbose mode enabled");
    }

    // Initialize the real engine with default registry
    let engine = MaccEngine::new(macc_registry::default_registry());
    let provider = services::engine_provider::EngineProvider::new(engine);

    if let Err(e) = run_with_engine_provider(cli, provider) {
        error!(error = %e, "Command failed");
        eprintln!("Error: {}", e);
        exit(get_exit_code(&e));
    }
}

fn init_tracing(verbose: bool) {
    let fallback = if verbose { "debug" } else { "info" };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new(fallback))
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
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
    debug!(cwd = %cli.cwd, "Starting CLI command routing");
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
        Some(Commands::Init { force, wizard }) => {
            commands::init::InitCommand::new(app.clone(), *force, *wizard).run()
        }
        Some(Commands::Quickstart { yes, apply, no_tui }) => {
            commands::quickstart::QuickstartCommand::new(app.clone(), *yes, *apply, *no_tui).run()
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
        Some(Commands::Install { install_command }) => {
            commands::install::InstallCommand::new(app.clone(), install_command).run()
        }
        Some(Commands::Tui) => {
            let paths = engine.project_ensure_initialized_paths(&absolute_cwd)?;
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
        Some(Commands::Context {
            tool,
            from_files,
            dry_run,
            print_prompt,
        }) => commands::context::ContextCommand::new(
            app.clone(),
            tool.as_deref(),
            from_files,
            *dry_run,
            *print_prompt,
        )
        .run(),
        Some(Commands::Doctor { fix }) => {
            commands::doctor::DoctorCommand::new(app.clone(), *fix).run()
        }
        Some(Commands::Migrate { apply }) => {
            commands::migrate::MigrateCommand::new(app.clone(), *apply).run()
        }
        Some(Commands::Backups { backups_command }) => {
            commands::backups::BackupsCommand::new(app.clone(), backups_command).run()
        }
        Some(Commands::Restore {
            latest,
            user,
            backup,
            dry_run,
            yes,
        }) => commands::restore::RestoreCommand::new(
            app.clone(),
            *latest,
            *user,
            backup.as_deref(),
            *dry_run,
            *yes,
        )
        .run(),
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
            mirror_json_debounce_ms,
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
                    mirror_json_debounce_ms: *mirror_json_debounce_ms,
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
            let paths = engine.project_ensure_initialized_paths(&absolute_cwd)?;
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

    let user_backup_root = macc_core::domain::backups::user_backup_root()?;
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

// ... existing catalog functions (run_remote_search, list_skills, etc) ...

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::legacy_helpers::{
        read_registry_counts, run_coordinator_action, run_coordinator_full_cycle,
        validate_coordinator_runtime_transition_action, validate_coordinator_transition_action,
        COORDINATOR_TASK_REGISTRY_REL_PATH,
    };
    use crate::test_support::run_git_ok;
    use macc_core::service::tooling::{extract_version_token, run_version_command};
    use macc_core::TestEngine;
    use macc_core::{MaccError, McpCatalog, SkillsCatalog};
    use std::fs;
    use std::io;
    use std::net::TcpListener;
    use std::path::{Path, PathBuf};

    fn bind_loopback() -> Option<(TcpListener, u16)> {
        match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => {
                let port = listener.local_addr().ok()?.port();
                Some((listener, port))
            }
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                tracing::warn!("Skipping test: cannot bind loopback socket ({})", e);
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

    fn collect_rs_files(root: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(root) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_rs_files(&path, out);
            } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
                out.push(path);
            }
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
    fn test_no_direct_git_process_invocations_in_cli_tui() {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let cli_src = manifest_dir.join("src");
        let tui_src = manifest_dir.join("../tui/src");
        let patterns = [
            "Command::new(\"git\")",
            "std::process::Command::new(\"git\")",
        ];

        let mut files = Vec::new();
        collect_rs_files(&cli_src, &mut files);
        collect_rs_files(&tui_src, &mut files);

        let mut violations = Vec::new();
        for file in files {
            let Ok(content) = std::fs::read_to_string(&file) else {
                continue;
            };
            if patterns.iter().any(|p| content.contains(p)) {
                violations.push(file.display().to_string());
            }
        }

        assert!(
            violations.is_empty(),
            "Direct git process invocation detected; use macc_core::git facade instead: {:?}",
            violations
        );
    }

    #[test]
    fn test_tui_has_no_direct_process_management() {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let tui_src = manifest_dir.join("../tui/src");
        let patterns = [
            "std::process::Child",
            "std::process::Command",
            "tokio::process::Child",
            "tokio::process::Command",
        ];

        let mut files = Vec::new();
        collect_rs_files(&tui_src, &mut files);

        let mut violations = Vec::new();
        for file in files {
            let Ok(content) = std::fs::read_to_string(&file) else {
                continue;
            };
            if patterns.iter().any(|p| content.contains(p)) {
                violations.push(file.display().to_string());
            }
        }

        assert!(
            violations.is_empty(),
            "Direct process management in TUI detected; route via Engine coordinator/task-runner APIs instead: {:?}",
            violations
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
        let direction = StorageSyncArgs::try_from(args.as_slice())
            .unwrap()
            .direction;
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
        let counts = read_registry_counts(&registry).unwrap();
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
            log_flush_lines: None,
            log_flush_ms: None,
            mirror_json_debounce_ms: None,
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
            log_flush_lines: None,
            log_flush_ms: None,
            mirror_json_debounce_ms: None,
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
            _script: &std::path::Path,
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
                log_flush_lines: None,
                log_flush_ms: None,
                mirror_json_debounce_ms: None,
                stale_claimed_seconds: None,
                stale_in_progress_seconds: None,
                stale_changes_requested_seconds: None,
                stale_action: None,
                storage_mode: None,
                error_code_retry_list: None,
                error_code_retry_max: None,
            };

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .enable_io()
                .build()
                .map_err(|e| {
                    MaccError::Validation(format!("Failed to initialize tokio runtime: {}", e))
                })?;
            runtime.block_on(macc_core::coordinator::engine::run_native_control_plane(
                root,
                &canonical,
                Some(&coordinator_cfg),
                &env_cfg,
                None,
            ))?;

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
            log_flush_lines: None,
            log_flush_ms: None,
            mirror_json_debounce_ms: None,
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
            log_flush_lines: None,
            log_flush_ms: None,
            mirror_json_debounce_ms: None,
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
        run_git_ok(&root, &["init"]);
        run_git_ok(&root, &["config", "user.email", "macc-tests@example.com"]);
        run_git_ok(&root, &["config", "user.name", "macc-tests"]);
        run_git_ok(&root, &["add", "README.md"]);
        run_git_ok(&root, &["commit", "-m", "seed"]);

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
        run_git_ok(
            &root,
            &[
                "worktree",
                "add",
                "-b",
                "ai/stop-test",
                wt_path.to_string_lossy().as_ref(),
                "HEAD",
            ],
        );

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
                    log_flush_lines: None,
                    log_flush_ms: None,
                    mirror_json_debounce_ms: None,
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

        assert!(
            !macc_core::git::rev_parse_verify(&root, "ai/stop-test").unwrap_or(false),
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

        run_git_ok(&skill_source_dir, &["init", "-b", "main"]);
        run_git_ok(
            &skill_source_dir,
            &["config", "user.email", "test@example.com"],
        );
        run_git_ok(&skill_source_dir, &["config", "user.name", "Test"]);
        run_git_ok(&skill_source_dir, &["add", "."]);
        run_git_ok(&skill_source_dir, &["commit", "-m", "initial"]);

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

        run_git_ok(&mcp_source_dir, &["init", "-b", "main"]);
        run_git_ok(
            &mcp_source_dir,
            &["config", "user.email", "test@example.com"],
        );
        run_git_ok(&mcp_source_dir, &["config", "user.name", "Test"]);
        run_git_ok(&mcp_source_dir, &["add", "."]);
        run_git_ok(&mcp_source_dir, &["commit", "-m", "initial"]);

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
        let temp_base =
            std::env::temp_dir().join(format!("macc_install_multi_git_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let ids = fixture_ids();
        let tool_one = ids[0].clone();

        let repo_path = temp_base.join("remote_repo");
        std::fs::create_dir_all(&repo_path).unwrap();

        // 1. Initialize a local git repo
        let run_git = |args: &[&str], dir: &std::path::Path| run_git_ok(dir, args);

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
