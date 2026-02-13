use clap::{Parser, Subcommand};
use macc_adapter_shared::catalog::{remote_search, SearchKind as RemoteSearchKind};
use macc_core::catalog::{
    load_effective_mcp_catalog, load_effective_skills_catalog, McpCatalog, McpEntry, Selector,
    SkillEntry, SkillsCatalog, Source, SourceKind,
};
use macc_core::engine::{Engine, MaccEngine};
use macc_core::plan::builders::{plan_mcp_install, plan_skill_install};
use macc_core::plan::ActionPlan;
use macc_core::resolve::{
    resolve, resolve_fetch_units, CliOverrides, FetchUnit, Selection, SelectionKind,
};
use macc_core::{load_canonical_config, MaccError, Result};
use std::collections::BTreeMap;
use std::process::exit;

#[derive(Parser)]
#[command(name = "macc")]
#[command(about = "MACC (Multi-Assistant Code Config)", long_about = None)]
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
        /// Coordinator action (run, dispatch, advance, sync, status, reconcile, unlock, cleanup, stop)
        #[arg(default_value = "run")]
        action: String,
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
        /// Override task registry file path
        #[arg(long)]
        registry: Option<String>,
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

    if let Err(e) = run_with_engine(cli, engine) {
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

fn run_with_engine<E: Engine>(cli: Cli, engine: E) -> Result<()> {
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

    match &cli.command {
        Some(Commands::Init { force, wizard }) => {
            let paths = macc_core::find_project_root(&absolute_cwd)
                .unwrap_or_else(|_| macc_core::ProjectPaths::from_root(&absolute_cwd));
            macc_core::init(&paths, *force)?;
            if *wizard {
                run_init_wizard(&paths, &engine)?;
            }
            let checks = engine.doctor(&paths);
            print_checks(&checks);
            Ok(())
        }
        Some(Commands::Quickstart { yes, apply, no_tui }) => {
            run_quickstart(&absolute_cwd, &engine, *yes, *apply, *no_tui)
        }
        Some(Commands::Plan {
            tools,
            json,
            explain,
        }) => {
            let paths = macc_core::find_project_root(&absolute_cwd)?;
            let canonical = load_canonical_config(&paths.config_path)?;

            let (descriptors, diagnostics) = engine.list_tools(&paths);
            report_diagnostics(&diagnostics);
            let allowed_tools: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();

            let migration =
                macc_core::migrate::migrate_with_known_tools(canonical.clone(), &allowed_tools);
            if !migration.warnings.is_empty() {
                eprintln!("Warning: Legacy configuration detected. Run 'macc migrate' to update your config.");
            }

            let overrides = if let Some(tools_csv) = tools {
                CliOverrides::from_tools_csv(tools_csv, &allowed_tools)?
            } else {
                CliOverrides::default()
            };

            let resolved = resolve(&canonical, &overrides);

            let enabled_titles: Vec<String> = resolved
                .tools
                .enabled
                .iter()
                .map(|id| {
                    descriptors
                        .iter()
                        .find(|d| &d.id == id)
                        .map(|d| d.title.clone())
                        .unwrap_or_else(|| id.clone())
                })
                .collect();

            if !*json {
                println!(
                    "Core: Planning in {} with tools: {:?}",
                    paths.root.display(),
                    enabled_titles
                );
            }

            let fetch_units = resolve_fetch_units(&paths, &resolved)?;
            let materialized_units =
                macc_adapter_shared::fetch::materialize_fetch_units(&paths, fetch_units)?;

            let plan = engine.plan(&paths, &canonical, &materialized_units, &overrides)?;
            let ops = engine.plan_operations(&paths, &plan);
            render_plan_preview(&paths, &plan, &ops, *json, *explain)?;
            Ok(())
        }
        Some(Commands::Apply {
            tools,
            dry_run,
            allow_user_scope,
            json,
            explain,
        }) => {
            let paths = macc_core::find_project_root(&absolute_cwd)?;
            let canonical = load_canonical_config(&paths.config_path)?;

            let (descriptors, diagnostics) = engine.list_tools(&paths);
            report_diagnostics(&diagnostics);
            let allowed_tools: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();

            let migration =
                macc_core::migrate::migrate_with_known_tools(canonical.clone(), &allowed_tools);
            if !migration.warnings.is_empty() {
                eprintln!("Warning: Legacy configuration detected. Run 'macc migrate' to update your config.");
            }

            let overrides = if let Some(tools_csv) = tools {
                CliOverrides::from_tools_csv(tools_csv, &allowed_tools)?
            } else {
                CliOverrides::default()
            };
            let resolved = resolve(&canonical, &overrides);

            let enabled_titles: Vec<String> = resolved
                .tools
                .enabled
                .iter()
                .map(|id| {
                    descriptors
                        .iter()
                        .find(|d| &d.id == id)
                        .map(|d| d.title.clone())
                        .unwrap_or_else(|| id.clone())
                })
                .collect();

            let fetch_units = resolve_fetch_units(&paths, &resolved)?;
            let materialized_units =
                macc_adapter_shared::fetch::materialize_fetch_units(&paths, fetch_units)?;

            if *dry_run {
                if !*json {
                    println!(
                        "Core: Dry-run apply (planning) in {} with tools: {:?}",
                        paths.root.display(),
                        enabled_titles
                    );
                }
                let plan = engine.plan(&paths, &canonical, &materialized_units, &overrides)?;
                let ops = engine.plan_operations(&paths, &plan);
                render_plan_preview(&paths, &plan, &ops, *json, *explain)?;
                return Ok(());
            }

            println!(
                "Core: Applying in {} with tools: {:?}",
                paths.root.display(),
                enabled_titles
            );
            let mut plan = engine.plan(&paths, &canonical, &materialized_units, &overrides)?;
            let ops = engine.plan_operations(&paths, &plan);
            if !*json {
                print_pre_apply_summary(&paths, &plan, &ops);
                if *explain {
                    print_pre_apply_explanations(&ops);
                }
            }
            if *allow_user_scope {
                confirm_user_scope_apply(&paths, &ops)?;
            }

            // Use engine to apply
            let report = engine.apply(&paths, &mut plan, *allow_user_scope)?;

            println!("{}", report.render_cli());
            Ok(())
        }
        Some(Commands::Catalog { catalog_command }) => {
            let paths = macc_core::find_project_root(&absolute_cwd)?;
            match catalog_command {
                CatalogCommands::Skills { skills_command } => match skills_command {
                    CatalogSubCommands::List => {
                        let catalog = load_effective_skills_catalog(&paths)?;
                        list_skills(&catalog);
                        Ok(())
                    }
                    CatalogSubCommands::Search { query } => {
                        let catalog = load_effective_skills_catalog(&paths)?;
                        search_skills(&catalog, query);
                        Ok(())
                    }
                    CatalogSubCommands::Add {
                        id,
                        name,
                        description,
                        tags,
                        subpath,
                        kind,
                        url,
                        reference,
                        checksum,
                    } => {
                        let mut catalog = SkillsCatalog::load(&paths.skills_catalog_path())?;
                        add_skill(
                            &paths,
                            &mut catalog,
                            id.clone(),
                            name.clone(),
                            description.clone(),
                            tags.clone(),
                            subpath.clone(),
                            kind.clone(),
                            url.clone(),
                            reference.clone(),
                            checksum.clone(),
                        )
                    }
                    CatalogSubCommands::Remove { id } => {
                        let mut catalog = SkillsCatalog::load(&paths.skills_catalog_path())?;
                        remove_skill(&paths, &mut catalog, id.clone())
                    }
                },
                CatalogCommands::Mcp { mcp_command } => match mcp_command {
                    CatalogSubCommands::List => {
                        let catalog = load_effective_mcp_catalog(&paths)?;
                        list_mcp(&catalog);
                        Ok(())
                    }
                    CatalogSubCommands::Search { query } => {
                        let catalog = load_effective_mcp_catalog(&paths)?;
                        search_mcp(&catalog, query);
                        Ok(())
                    }
                    CatalogSubCommands::Add {
                        id,
                        name,
                        description,
                        tags,
                        subpath,
                        kind,
                        url,
                        reference,
                        checksum,
                    } => {
                        let mut catalog = McpCatalog::load(&paths.mcp_catalog_path())?;
                        add_mcp(
                            &paths,
                            &mut catalog,
                            id.clone(),
                            name.clone(),
                            description.clone(),
                            tags.clone(),
                            subpath.clone(),
                            kind.clone(),
                            url.clone(),
                            reference.clone(),
                            checksum.clone(),
                        )
                    }
                    CatalogSubCommands::Remove { id } => {
                        let mut catalog = McpCatalog::load(&paths.mcp_catalog_path())?;
                        remove_mcp(&paths, &mut catalog, id.clone())
                    }
                },
                CatalogCommands::ImportUrl {
                    kind,
                    id,
                    url,
                    name,
                    description,
                    tags,
                } => import_url(
                    &paths,
                    kind,
                    id.clone(),
                    url.clone(),
                    name.clone(),
                    description.clone(),
                    tags.clone(),
                ),
                CatalogCommands::SearchRemote {
                    api,
                    kind,
                    q,
                    add,
                    add_ids,
                } => run_remote_search(
                    &paths,
                    api.clone(),
                    kind.clone(),
                    q.clone(),
                    *add,
                    add_ids.clone(),
                ),
            }
        }
        Some(Commands::Install { install_command }) => {
            let paths = macc_core::find_project_root(&absolute_cwd)?;
            match install_command {
                InstallCommands::Skill { tool, id } => install_skill(&paths, tool, id, &engine),
                InstallCommands::Mcp { id } => install_mcp(&paths, id, &engine),
            }
        }
        Some(Commands::Tui) => {
            let paths = ensure_initialized_paths(&absolute_cwd)?;
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
            let paths = ensure_initialized_paths(&absolute_cwd)?;
            match tool_command {
                ToolCommands::Install { tool_id, yes } => install_tool(&paths, tool_id, *yes),
            }
        }
        Some(Commands::Doctor { fix }) => {
            let paths = macc_core::find_project_root(&absolute_cwd)?;
            run_doctor(&paths, &engine, *fix)
        }
        Some(Commands::Migrate { apply }) => {
            let paths = macc_core::find_project_root(&absolute_cwd)?;
            let canonical = load_canonical_config(&paths.config_path)?;

            let (descriptors, diagnostics) = engine.list_tools(&paths);
            report_diagnostics(&diagnostics);
            let allowed_tools: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();

            let result = macc_core::migrate::migrate_with_known_tools(canonical, &allowed_tools);

            if result.warnings.is_empty() {
                println!("No legacy configuration found. Your config is up to date.");
                return Ok(());
            }

            println!("Legacy configuration detected:");
            for warning in &result.warnings {
                println!("  - {}", warning);
            }

            if *apply {
                let yaml = result.config.to_yaml().map_err(|e| {
                    MaccError::Validation(format!("Failed to serialize migrated config: {}", e))
                })?;

                macc_core::atomic_write(&paths, &paths.config_path, yaml.as_bytes())?;
                println!(
                    "\nMigrated configuration written to {}",
                    paths.config_path.display()
                );
            } else {
                println!("\nDry-run: use --apply to write the migrated configuration to disk.");
                println!("Preview of migrated config:");
                println!("---");
                println!("{}", result.config.to_yaml().unwrap());
                println!("---");
            }

            Ok(())
        }
        Some(Commands::Backups { backups_command }) => {
            let paths = macc_core::find_project_root(&absolute_cwd)?;
            match backups_command {
                BackupsCommands::List { user } => list_backup_sets_command(&paths, *user),
                BackupsCommands::Open {
                    id,
                    latest,
                    user,
                    editor,
                } => open_backup_set_command(&paths, id.as_deref(), *latest, *user, editor),
            }
        }
        Some(Commands::Restore {
            latest,
            user,
            backup,
            dry_run,
            yes,
        }) => {
            let paths = macc_core::find_project_root(&absolute_cwd)?;
            if !*latest && backup.is_none() {
                return Err(MaccError::Validation(
                    "restore requires --latest or --backup <id>".into(),
                ));
            }
            restore_backup_set_command(&paths, *user, backup.as_deref(), *latest, *dry_run, *yes)
        }
        Some(Commands::Clear) => {
            let paths = macc_core::find_project_root(&absolute_cwd)?;
            println!("This will:");
            println!("  1) Remove all non-root worktrees (equivalent to: macc worktree remove --all --force)");
            println!("  2) Remove MACC-managed files/directories in this project (macc clear)");
            if !confirm_yes_no("Continue [y/N]? ")? {
                return Err(MaccError::Validation("Clear cancelled.".into()));
            }
            let removed = remove_all_worktrees(&paths.root, false)?;
            macc_core::prune_worktrees(&paths.root)?;
            println!("Removed worktrees: {}", removed);
            let report = macc_core::clear(&paths)?;
            println!(
                "Cleared managed paths: removed={}, skipped={}",
                report.removed, report.skipped
            );
            Ok(())
        }
        Some(Commands::Worktree { worktree_command }) => match worktree_command {
            WorktreeCommands::Create {
                slug,
                tool,
                count,
                base,
                scope,
                feature,
                skip_apply,
                allow_user_scope,
            } => {
                let paths = macc_core::find_project_root(&absolute_cwd)?;
                let canonical = load_canonical_config(&paths.config_path)?;

                let spec = macc_core::WorktreeCreateSpec {
                    slug: slug.clone(),
                    tool: tool.clone(),
                    count: *count,
                    base: base.clone(),
                    dir: std::path::PathBuf::from(".macc/worktree"),
                    scope: scope.clone(),
                    feature: feature.clone(),
                };
                let created = macc_core::create_worktrees(&paths.root, &spec)?;

                let (descriptors, diagnostics) = engine.list_tools(&paths);
                report_diagnostics(&diagnostics);
                let allowed_tools: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();
                let overrides = CliOverrides::from_tools_csv(tool.as_str(), &allowed_tools)?;

                let yaml = canonical.to_yaml().map_err(|e| {
                    MaccError::Validation(format!("Failed to serialize config for worktree: {}", e))
                })?;

                for entry in &created {
                    let worktree_paths = macc_core::ProjectPaths::from_root(&entry.path);
                    macc_core::init(&worktree_paths, false)?;
                    macc_core::atomic_write(
                        &worktree_paths,
                        &worktree_paths.config_path,
                        yaml.as_bytes(),
                    )?;
                    write_tool_json(&paths.root, &entry.path, tool)?;

                    if !*skip_apply {
                        let resolved = resolve(&canonical, &overrides);
                        let fetch_units = resolve_fetch_units(&worktree_paths, &resolved)?;
                        let materialized_units =
                            macc_adapter_shared::fetch::materialize_fetch_units(
                                &worktree_paths,
                                fetch_units,
                            )?;
                        let mut plan = engine.plan(
                            &worktree_paths,
                            &canonical,
                            &materialized_units,
                            &overrides,
                        )?;
                        let _ = engine.apply(&worktree_paths, &mut plan, *allow_user_scope)?;
                    }
                }

                println!("Created {} worktree(s):", created.len());
                for entry in created {
                    println!(
                        "  {}  branch={} base={} path={}",
                        entry.id,
                        entry.branch,
                        entry.base,
                        entry.path.display()
                    );
                }
                if *skip_apply {
                    println!("Note: config apply skipped (--skip-apply).");
                }
                Ok(())
            }
            WorktreeCommands::Status => {
                let entries = macc_core::list_worktrees(&absolute_cwd)?;
                let current = macc_core::current_worktree(&absolute_cwd, &entries);
                println!("Worktree status:");
                if let Some(entry) = current {
                    println!("  Path: {}", entry.path.display());
                    if let Some(branch) = entry.branch {
                        println!("  Branch: {}", branch);
                    }
                    if let Some(head) = entry.head {
                        println!("  HEAD: {}", head);
                    }
                    println!("  Locked: {}", if entry.locked { "yes" } else { "no" });
                    println!("  Prunable: {}", if entry.prunable { "yes" } else { "no" });
                } else {
                    println!("  Not a git worktree (or git worktree list unavailable).");
                }
                println!("  Total worktrees: {}", entries.len());
                Ok(())
            }
            WorktreeCommands::List => {
                let entries = macc_core::list_worktrees(&absolute_cwd)?;
                if entries.is_empty() {
                    println!("No git worktrees found.");
                    return Ok(());
                }
                let project_paths = macc_core::find_project_root(&absolute_cwd)
                    .map(|root| macc_core::ProjectPaths::from_root(&root.root))
                    .ok();
                let session_map = load_worktree_session_map(project_paths.as_ref())?;

                println!(
                    "{:<54} {:<12} {:<24} {:<8} {:<10} {:<16} {:<8} {:<8}",
                    "WORKTREE", "TOOL", "BRANCH", "SCOPE", "STATE", "SESSION", "LOCKED", "PRUNE"
                );
                println!(
                    "{:-<54} {:-<12} {:-<24} {:-<8} {:-<10} {:-<16} {:-<8} {:-<8}",
                    "", "", "", "", "", "", "", ""
                );
                for entry in entries {
                    let metadata = macc_core::read_worktree_metadata(&entry.path)
                        .ok()
                        .flatten();
                    let tool = metadata
                        .as_ref()
                        .map(|m| m.tool.as_str())
                        .unwrap_or("n/a")
                        .to_string();
                    let branch = metadata
                        .as_ref()
                        .map(|m| m.branch.as_str())
                        .or(entry.branch.as_deref())
                        .unwrap_or("-")
                        .to_string();
                    let scope = metadata
                        .as_ref()
                        .and_then(|m| m.scope.as_ref())
                        .map(|s| truncate_cell(s, 8))
                        .unwrap_or_else(|| "-".into());
                    let git_state = if git_worktree_is_dirty(&entry.path).unwrap_or(false) {
                        "dirty"
                    } else {
                        "clean"
                    };
                    let session = session_map
                        .get(&canonicalize_path_fallback(&entry.path))
                        .map(format_worktree_session_status)
                        .unwrap_or_else(|| "-".into());
                    println!(
                        "{:<54} {:<12} {:<24} {:<8} {:<10} {:<16} {:<8} {:<8}",
                        truncate_cell(&entry.path.display().to_string(), 54),
                        truncate_cell(&tool, 12),
                        truncate_cell(&branch, 24),
                        scope,
                        git_state,
                        truncate_cell(&session, 16),
                        if entry.locked { "yes" } else { "no" },
                        if entry.prunable { "yes" } else { "no" }
                    );
                }
                Ok(())
            }
            WorktreeCommands::Open {
                id,
                editor,
                terminal,
            } => {
                let paths = macc_core::find_project_root(&absolute_cwd)?;
                let worktree_path = resolve_worktree_path(&paths.root, id)?;
                if !worktree_path.exists() {
                    return Err(MaccError::Validation(format!(
                        "Worktree path does not exist: {}",
                        worktree_path.display()
                    )));
                }

                if *terminal {
                    open_in_terminal(&worktree_path)?;
                }
                if let Some(cmd) = editor {
                    open_in_editor(&worktree_path, cmd)?;
                } else {
                    open_in_editor(&worktree_path, "code")?;
                }

                println!("Opened worktree: {}", worktree_path.display());
                Ok(())
            }
            WorktreeCommands::Apply {
                id,
                all,
                allow_user_scope,
            } => {
                let paths = macc_core::find_project_root(&absolute_cwd)?;
                if *all {
                    let entries = macc_core::list_worktrees(&paths.root)?;
                    let root = paths.root.canonicalize().unwrap_or(paths.root.clone());
                    let mut applied = 0;
                    for entry in entries {
                        if entry.path == root {
                            continue;
                        }
                        apply_worktree(&engine, &entry.path, *allow_user_scope)?;
                        applied += 1;
                    }
                    println!("Applied {} worktree(s).", applied);
                    return Ok(());
                }

                let id = id.as_ref().ok_or_else(|| {
                    MaccError::Validation("worktree apply requires <ID> or --all".into())
                })?;
                let worktree_path = resolve_worktree_path(&paths.root, id)?;
                apply_worktree(&engine, &worktree_path, *allow_user_scope)?;
                println!("Applied worktree: {}", worktree_path.display());
                Ok(())
            }
            WorktreeCommands::Doctor { id } => {
                let paths = macc_core::find_project_root(&absolute_cwd)?;
                let worktree_path = resolve_worktree_path(&paths.root, id)?;
                if !worktree_path.exists() {
                    return Err(MaccError::Validation(format!(
                        "Worktree path does not exist: {}",
                        worktree_path.display()
                    )));
                }
                let worktree_paths = macc_core::ProjectPaths::from_root(&worktree_path);
                let checks = engine.doctor(&worktree_paths);
                print_checks(&checks);
                Ok(())
            }
            WorktreeCommands::Run { id } => {
                let paths = macc_core::find_project_root(&absolute_cwd)?;
                let worktree_path = resolve_worktree_path(&paths.root, id)?;
                if !worktree_path.exists() {
                    return Err(MaccError::Validation(format!(
                        "Worktree path does not exist: {}",
                        worktree_path.display()
                    )));
                }

                let metadata = macc_core::read_worktree_metadata(&worktree_path)?
                    .ok_or_else(|| MaccError::Validation("Missing .macc/worktree.json".into()))?;
                ensure_tool_json(&paths.root, &worktree_path, &metadata.tool)?;
                let (task_id, prd_path) =
                    resolve_worktree_task_context(&paths.root, &worktree_path, &metadata.id)?;
                let performer_path = ensure_performer(&paths.root, &worktree_path)?;
                let registry_path = paths.root.join("task_registry.json");

                let status = std::process::Command::new(&performer_path)
                    .current_dir(&worktree_path)
                    .arg("--repo")
                    .arg(&paths.root)
                    .arg("--worktree")
                    .arg(&worktree_path)
                    .arg("--task-id")
                    .arg(&task_id)
                    .arg("--tool")
                    .arg(&metadata.tool)
                    .arg("--registry")
                    .arg(&registry_path)
                    .arg("--prd")
                    .arg(&prd_path)
                    .status()
                    .map_err(|e| MaccError::Io {
                        path: performer_path.to_string_lossy().into(),
                        action: "run worktree performer".into(),
                        source: e,
                    })?;
                if !status.success() {
                    return Err(MaccError::Validation(format!(
                        "Performer failed with status: {}. Inspect logs with `macc logs tail --component performer --worktree {}` and if the task is stuck run `macc coordinator unlock --task {}`.",
                        status, metadata.id, task_id
                    )));
                }
                Ok(())
            }
            WorktreeCommands::Exec { id, cmd } => {
                let paths = macc_core::find_project_root(&absolute_cwd)?;
                let worktree_path = resolve_worktree_path(&paths.root, id)?;
                if !worktree_path.exists() {
                    return Err(MaccError::Validation(format!(
                        "Worktree path does not exist: {}",
                        worktree_path.display()
                    )));
                }
                if cmd.is_empty() {
                    return Err(MaccError::Validation(
                        "worktree exec requires a command after --".into(),
                    ));
                }

                let mut command = std::process::Command::new(&cmd[0]);
                if cmd.len() > 1 {
                    command.args(&cmd[1..]);
                }
                let status =
                    command
                        .current_dir(&worktree_path)
                        .status()
                        .map_err(|e| MaccError::Io {
                            path: worktree_path.to_string_lossy().into(),
                            action: "run worktree exec".into(),
                            source: e,
                        })?;
                if !status.success() {
                    return Err(MaccError::Validation(format!(
                        "Command failed with status: {}",
                        status
                    )));
                }
                Ok(())
            }
            WorktreeCommands::Remove {
                id,
                force,
                all,
                remove_branch,
            } => {
                let paths = macc_core::find_project_root(&absolute_cwd)?;
                if *all {
                    let entries = macc_core::list_worktrees(&paths.root)?;
                    let root = paths.root.canonicalize().unwrap_or(paths.root.clone());
                    let mut removed = 0;
                    for entry in entries {
                        if entry.path == root {
                            continue;
                        }
                        let branch = entry.branch.clone();
                        macc_core::remove_worktree(&paths.root, &entry.path, *force)?;
                        if *remove_branch {
                            delete_branch(&paths.root, branch.as_deref(), *force)?;
                        }
                        println!("Removed worktree: {}", entry.path.display());
                        removed += 1;
                    }
                    println!("Removed {} worktree(s).", removed);
                    return Ok(());
                }

                let id = id.as_ref().ok_or_else(|| {
                    MaccError::Validation("worktree remove requires <ID> or --all".into())
                })?;
                let entries = macc_core::list_worktrees(&paths.root)?;
                let candidate = std::path::Path::new(id);
                let worktree_path =
                    if candidate.is_absolute() || id.contains(std::path::MAIN_SEPARATOR) {
                        std::path::PathBuf::from(id)
                    } else {
                        paths.root.join(".macc/worktree").join(id)
                    };

                let branch = entries
                    .iter()
                    .find(|entry| entry.path == worktree_path)
                    .and_then(|entry| entry.branch.clone());
                macc_core::remove_worktree(&paths.root, &worktree_path, *force)?;
                if *remove_branch {
                    delete_branch(&paths.root, branch.as_deref(), *force)?;
                }
                println!("Removed worktree: {}", worktree_path.display());
                Ok(())
            }
            WorktreeCommands::Prune => {
                let paths = macc_core::find_project_root(&absolute_cwd)?;
                macc_core::prune_worktrees(&paths.root)?;
                println!("Pruned git worktrees.");
                Ok(())
            }
        },
        Some(Commands::Logs { logs_command }) => {
            let paths = ensure_initialized_paths(&absolute_cwd)?;
            match logs_command {
                LogsCommands::Tail {
                    component,
                    worktree,
                    task,
                    lines,
                    follow,
                } => {
                    let file = select_log_file(
                        &paths,
                        component.as_str(),
                        worktree.as_deref(),
                        task.as_deref(),
                    )?;
                    println!("Log file: {}", file.display());
                    if *follow {
                        tail_file_follow(&file, *lines)?;
                    } else {
                        print_file_tail(&file, *lines)?;
                    }
                    Ok(())
                }
            }
        }
        Some(Commands::Coordinator {
            action,
            graceful,
            remove_worktrees,
            remove_branches,
            prd,
            registry,
            coordinator_tool,
            reference_branch,
            tool_priority,
            max_parallel_per_tool_json,
            tool_specializations_json,
            max_dispatch,
            max_parallel,
            timeout_seconds,
            phase_runner_max_attempts,
            stale_claimed_seconds,
            stale_in_progress_seconds,
            stale_changes_requested_seconds,
            stale_action,
            extra_args,
        }) => {
            let paths = ensure_initialized_paths(&absolute_cwd)?;
            let canonical = load_canonical_config(&paths.config_path)?;
            let coordinator = canonical.automation.coordinator.clone();

            let _ = macc_core::ensure_embedded_automation_scripts(&paths)?;
            let coordinator_path = paths.automation_coordinator_path();
            if !coordinator_path.exists() {
                return Err(MaccError::Validation(format!(
                    "Coordinator script not found: {}",
                    coordinator_path.display()
                )));
            }

            let env_cfg = CoordinatorEnvConfig {
                prd: prd.clone(),
                registry: registry.clone(),
                coordinator_tool: coordinator_tool.clone(),
                reference_branch: reference_branch.clone(),
                tool_priority: tool_priority.clone(),
                max_parallel_per_tool_json: max_parallel_per_tool_json.clone(),
                tool_specializations_json: tool_specializations_json.clone(),
                max_dispatch: *max_dispatch,
                max_parallel: *max_parallel,
                timeout_seconds: *timeout_seconds,
                phase_runner_max_attempts: *phase_runner_max_attempts,
                stale_claimed_seconds: *stale_claimed_seconds,
                stale_in_progress_seconds: *stale_in_progress_seconds,
                stale_changes_requested_seconds: *stale_changes_requested_seconds,
                stale_action: stale_action.clone(),
            };

            if action == "stop" {
                let stopped =
                    stop_coordinator_process_groups(&paths.root, &coordinator_path, *graceful)?;
                println!("Coordinator process groups signaled: {}", stopped);

                run_coordinator_action(
                    &paths.root,
                    &coordinator_path,
                    "reconcile",
                    &[],
                    &canonical,
                    coordinator.as_ref(),
                    &env_cfg,
                )?;
                run_coordinator_action(
                    &paths.root,
                    &coordinator_path,
                    "cleanup",
                    &[],
                    &canonical,
                    coordinator.as_ref(),
                    &env_cfg,
                )?;
                run_coordinator_action(
                    &paths.root,
                    &coordinator_path,
                    "unlock",
                    &["--all".to_string()],
                    &canonical,
                    coordinator.as_ref(),
                    &env_cfg,
                )?;

                if *remove_worktrees {
                    let removed = remove_all_worktrees(&paths.root, *remove_branches)?;
                    println!("Removed {} worktree(s).", removed);
                    macc_core::prune_worktrees(&paths.root)?;
                    println!("Pruned git worktrees.");
                }
            } else if action == "run" {
                if !extra_args.is_empty() {
                    return Err(MaccError::Validation(
                        "Action 'run' does not accept extra args after '--'.".into(),
                    ));
                }
                run_coordinator_full_cycle(
                    &paths.root,
                    &coordinator_path,
                    &canonical,
                    coordinator.as_ref(),
                    &env_cfg,
                )?;
            } else {
                run_coordinator_action(
                    &paths.root,
                    &coordinator_path,
                    action,
                    extra_args,
                    &canonical,
                    coordinator.as_ref(),
                    &env_cfg,
                )?;
            }
            Ok(())
        }
        None => {
            let paths = ensure_initialized_paths(&absolute_cwd)?;
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

fn ensure_initialized_paths(start_dir: &std::path::Path) -> Result<macc_core::ProjectPaths> {
    let paths = macc_core::find_project_root(start_dir)
        .unwrap_or_else(|_| macc_core::ProjectPaths::from_root(start_dir));
    macc_core::init(&paths, false)?;
    Ok(paths)
}

fn run_quickstart<E: Engine>(
    absolute_cwd: &std::path::Path,
    engine: &E,
    assume_yes: bool,
    apply: bool,
    no_tui: bool,
) -> Result<()> {
    let paths = macc_core::find_project_root(absolute_cwd)
        .unwrap_or_else(|_| macc_core::ProjectPaths::from_root(absolute_cwd));

    let mut missing = Vec::new();
    for cmd in ["git", "curl", "jq"] {
        if !is_command_available(cmd) {
            missing.push(cmd);
        }
    }
    if !missing.is_empty() {
        return Err(MaccError::Validation(format!(
            "Missing required commands: {}",
            missing.join(", ")
        )));
    }

    if !paths.root.join(".git").exists() {
        println!("No .git directory found in {}.", paths.root.display());
        if !assume_yes && !confirm_yes_no("Continue anyway [y/N]? ")? {
            return Err(MaccError::Validation("Quickstart cancelled.".into()));
        }
    }

    if !paths.macc_dir.exists() && !assume_yes {
        println!(".macc/ was not found in this project.");
        if !confirm_yes_no("Run 'macc init' now [y/N]? ")? {
            return Err(MaccError::Validation(
                "Quickstart requires initialization. Cancelled.".into(),
            ));
        }
    }

    // init seeds config, catalogs, automation scripts, and gitignore entries.
    macc_core::init(&paths, false)?;
    println!(
        "Quickstart: initialized project at {}",
        paths.root.display()
    );

    if apply {
        run_plan_then_optional_apply(engine, &paths, assume_yes)?;
        return Ok(());
    }

    if no_tui {
        println!("Quickstart complete.");
        println!("Next: run 'macc plan' then 'macc apply'.");
        return Ok(());
    }

    println!("Quickstart complete. Opening TUI...");
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

fn run_plan_then_optional_apply<E: Engine>(
    engine: &E,
    paths: &macc_core::ProjectPaths,
    assume_yes: bool,
) -> Result<()> {
    let canonical = load_canonical_config(&paths.config_path)?;
    let (_descriptors, diagnostics) = engine.list_tools(paths);
    report_diagnostics(&diagnostics);
    let overrides = CliOverrides::default();
    let resolved = resolve(&canonical, &overrides);
    let fetch_units = resolve_fetch_units(paths, &resolved)?;
    let materialized_units =
        macc_adapter_shared::fetch::materialize_fetch_units(paths, fetch_units)?;

    let plan = engine.plan(paths, &canonical, &materialized_units, &overrides)?;
    macc_core::preview_plan(&plan, paths)?;
    println!("Core: Total actions planned: {}", plan.actions.len());

    if !assume_yes && !confirm_yes_no("Apply this plan now [y/N]? ")? {
        println!("Plan generated only. Run 'macc apply' when ready.");
        return Ok(());
    }

    // Re-resolve from disk before apply.
    let canonical = load_canonical_config(&paths.config_path)?;
    let overrides = CliOverrides::default();
    let resolved = resolve(&canonical, &overrides);
    let fetch_units = resolve_fetch_units(paths, &resolved)?;
    let materialized_units =
        macc_adapter_shared::fetch::materialize_fetch_units(paths, fetch_units)?;
    let mut apply_plan = engine.plan(paths, &canonical, &materialized_units, &overrides)?;
    let report = engine.apply(paths, &mut apply_plan, false)?;
    println!("{}", report.render_cli());
    Ok(())
}

fn run_init_wizard<E: Engine>(paths: &macc_core::ProjectPaths, engine: &E) -> Result<()> {
    println!("Init wizard (3 questions)");
    let mut config = load_canonical_config(&paths.config_path)?;
    let (descriptors, diagnostics) = engine.list_tools(paths);
    report_diagnostics(&diagnostics);
    let tool_ids: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();

    if !tool_ids.is_empty() {
        println!("Available tools: {}", tool_ids.join(", "));
    }
    let tools_answer = prompt_line("Q1/3 - Enabled tools (CSV, empty keeps current): ")?;
    if !tools_answer.is_empty() {
        let selected = parse_csv(&tools_answer);
        if selected.is_empty() {
            return Err(MaccError::Validation(
                "Wizard: at least one tool is required when tools are provided.".into(),
            ));
        }
        let unknown: Vec<String> = selected
            .iter()
            .filter(|id| !tool_ids.iter().any(|known| known == *id))
            .cloned()
            .collect();
        if !unknown.is_empty() {
            return Err(MaccError::Validation(format!(
                "Wizard: unknown tools: {}",
                unknown.join(", ")
            )));
        }
        config.tools.enabled = selected;
    }

    println!("Standards presets: minimal | strict | none");
    let preset = prompt_line("Q2/3 - Standards preset [minimal]: ")?;
    apply_standards_preset(
        &mut config,
        if preset.is_empty() {
            "minimal"
        } else {
            &preset
        },
    )?;

    let mcp_answer = prompt_line("Q3/3 - Enable default MCP templates in selections? [y/N]: ")?;
    let enable_mcp = matches!(mcp_answer.trim().to_ascii_lowercase().as_str(), "y" | "yes");
    if enable_mcp {
        let ids: Vec<String> = config.mcp_templates.iter().map(|t| t.id.clone()).collect();
        let mut selections = config.selections.unwrap_or_default();
        selections.mcp = ids;
        config.selections = Some(selections);
    } else if let Some(selections) = config.selections.as_mut() {
        selections.mcp.clear();
    }

    let yaml = config
        .to_yaml()
        .map_err(|e| MaccError::Validation(format!("Failed to serialize wizard config: {}", e)))?;
    macc_core::atomic_write(paths, &paths.config_path, yaml.as_bytes())?;
    println!("Wizard saved: {}", paths.config_path.display());
    Ok(())
}

fn apply_standards_preset(
    config: &mut macc_core::config::CanonicalConfig,
    preset: &str,
) -> Result<()> {
    config.standards.path = None;
    config.standards.inline.clear();

    match preset.trim().to_ascii_lowercase().as_str() {
        "minimal" => {
            config
                .standards
                .inline
                .insert("language".into(), "English".into());
            config
                .standards
                .inline
                .insert("package_manager".into(), "pnpm".into());
        }
        "strict" => {
            config
                .standards
                .inline
                .insert("language".into(), "English".into());
            config
                .standards
                .inline
                .insert("package_manager".into(), "pnpm".into());
            config
                .standards
                .inline
                .insert("typescript".into(), "strict".into());
            config
                .standards
                .inline
                .insert("imports".into(), "absolute:@/".into());
        }
        "none" => {}
        other => {
            return Err(MaccError::Validation(format!(
                "Wizard: unknown standards preset '{}'. Use minimal|strict|none.",
                other
            )));
        }
    }
    Ok(())
}

fn parse_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

fn prompt_line(prompt: &str) -> Result<String> {
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
            action: "read input".into(),
            source: e,
        })?;
    Ok(input.trim().to_string())
}

fn is_command_available(cmd: &str) -> bool {
    std::process::Command::new("sh")
        .arg("-lc")
        .arg(format!("command -v {} >/dev/null 2>&1", cmd))
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn unix_timestamp_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn delete_branch(root: &std::path::Path, branch: Option<&str>, force: bool) -> Result<()> {
    let Some(branch) = branch else {
        return Ok(());
    };
    let branch = branch.strip_prefix("refs/heads/").unwrap_or(branch);
    if branch.is_empty() {
        return Ok(());
    }

    let mut cmd = std::process::Command::new("git");
    cmd.arg("branch");
    if force {
        cmd.arg("-D");
    } else {
        cmd.arg("-d");
    }
    let output = cmd
        .arg(branch)
        .current_dir(root)
        .output()
        .map_err(|e| MaccError::Io {
            path: root.to_string_lossy().into(),
            action: "run git branch delete".into(),
            source: e,
        })?;

    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "git branch delete failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

fn resolve_worktree_path(root: &std::path::Path, id: &str) -> Result<std::path::PathBuf> {
    let candidate = std::path::Path::new(id);
    Ok(
        if candidate.is_absolute() || id.contains(std::path::MAIN_SEPARATOR) {
            std::path::PathBuf::from(id)
        } else {
            root.join(".macc/worktree").join(id)
        },
    )
}

fn write_tool_json(
    repo_root: &std::path::Path,
    worktree_path: &std::path::Path,
    tool_id: &str,
) -> Result<std::path::PathBuf> {
    let search_paths = macc_core::tool::ToolSpecLoader::default_search_paths(repo_root);
    let loader = macc_core::tool::ToolSpecLoader::new(search_paths);
    let (specs, diagnostics) = loader.load_all_with_embedded();
    report_diagnostics(&diagnostics);

    let spec = specs
        .into_iter()
        .find(|spec| spec.id == tool_id)
        .ok_or_else(|| MaccError::Validation(format!("Tool spec not found: {}", tool_id)))?;
    let mut runtime = spec.to_runtime_config().ok_or_else(|| {
        MaccError::Validation(format!("Tool spec missing performer section: {}", tool_id))
    })?;

    let worktree_paths = macc_core::ProjectPaths::from_root(worktree_path);
    let _ = macc_core::ensure_embedded_automation_scripts(&worktree_paths)?;
    if let Some(runner_path) =
        macc_core::embedded_runner_path_for_ref(&worktree_paths, &runtime.performer.runner)?
    {
        runtime.performer.runner = runner_path.to_string_lossy().into_owned();
    }

    let macc_dir = worktree_path.join(".macc");
    std::fs::create_dir_all(&macc_dir).map_err(|e| MaccError::Io {
        path: macc_dir.to_string_lossy().into(),
        action: "create .macc directory".into(),
        source: e,
    })?;

    let tool_json_path = macc_dir.join("tool.json");
    let content = serde_json::to_string_pretty(&runtime)
        .map_err(|e| MaccError::Validation(format!("Failed to serialize tool.json: {}", e)))?;
    std::fs::write(&tool_json_path, content).map_err(|e| MaccError::Io {
        path: tool_json_path.to_string_lossy().into(),
        action: "write tool.json".into(),
        source: e,
    })?;
    Ok(tool_json_path)
}

fn ensure_tool_json(
    repo_root: &std::path::Path,
    worktree_path: &std::path::Path,
    tool_id: &str,
) -> Result<std::path::PathBuf> {
    let tool_json_path = worktree_path.join(".macc").join("tool.json");
    if tool_json_path.exists() {
        return Ok(tool_json_path);
    }
    write_tool_json(repo_root, worktree_path, tool_id)
}

fn ensure_performer(
    _repo_root: &std::path::Path,
    worktree_path: &std::path::Path,
) -> Result<std::path::PathBuf> {
    let target = worktree_path.join("performer.sh");
    if target.exists() {
        return Ok(target);
    }

    let worktree_paths = macc_core::ProjectPaths::from_root(worktree_path);
    let _ = macc_core::ensure_embedded_automation_scripts(&worktree_paths)?;
    let source = worktree_paths.automation_performer_path();

    std::fs::copy(&source, &target).map_err(|e| MaccError::Io {
        path: target.to_string_lossy().into(),
        action: "copy performer.sh".into(),
        source: e,
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&target)
            .map_err(|e| MaccError::Io {
                path: target.to_string_lossy().into(),
                action: "read performer permissions".into(),
                source: e,
            })?
            .permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&target, perms).map_err(|e| MaccError::Io {
            path: target.to_string_lossy().into(),
            action: "set performer permissions".into(),
            source: e,
        })?;
    }

    Ok(target)
}

fn resolve_worktree_task_context(
    repo_root: &std::path::Path,
    worktree_path: &std::path::Path,
    fallback_id: &str,
) -> Result<(String, std::path::PathBuf)> {
    let prd_path = worktree_path.join("worktree.prd.json");
    if prd_path.exists() {
        let content = std::fs::read_to_string(&prd_path).map_err(|e| MaccError::Io {
            path: prd_path.to_string_lossy().into(),
            action: "read worktree.prd.json".into(),
            source: e,
        })?;
        let json: serde_json::Value = serde_json::from_str(&content).map_err(|e| {
            MaccError::Validation(format!("Failed to parse worktree.prd.json: {}", e))
        })?;
        let task_id = json
            .get("tasks")
            .and_then(|tasks| tasks.get(0))
            .and_then(|task| task.get("id"))
            .and_then(|id| match id {
                serde_json::Value::String(s) => Some(s.clone()),
                serde_json::Value::Number(n) => Some(n.to_string()),
                _ => None,
            })
            .ok_or_else(|| {
                MaccError::Validation("worktree.prd.json is missing tasks[0].id".into())
            })?;
        return Ok((task_id, prd_path));
    }

    let fallback_prd = repo_root.join("prd.json");
    if !fallback_prd.exists() {
        return Err(MaccError::Validation(
            "Missing worktree.prd.json and prd.json".into(),
        ));
    }
    Ok((fallback_id.to_string(), fallback_prd))
}

struct CoordinatorEnvConfig {
    prd: Option<String>,
    registry: Option<String>,
    coordinator_tool: Option<String>,
    reference_branch: Option<String>,
    tool_priority: Option<String>,
    max_parallel_per_tool_json: Option<String>,
    tool_specializations_json: Option<String>,
    max_dispatch: Option<usize>,
    max_parallel: Option<usize>,
    timeout_seconds: Option<usize>,
    phase_runner_max_attempts: Option<usize>,
    stale_claimed_seconds: Option<usize>,
    stale_in_progress_seconds: Option<usize>,
    stale_changes_requested_seconds: Option<usize>,
    stale_action: Option<String>,
}

fn apply_coordinator_env(
    command: &mut std::process::Command,
    canonical: &macc_core::config::CanonicalConfig,
    coordinator: Option<&macc_core::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
) {
    command.env("ENABLED_TOOLS_CSV", canonical.tools.enabled.join(","));

    if let Some(value) = env_cfg
        .prd
        .clone()
        .or_else(|| coordinator.and_then(|c| c.prd_file.clone()))
    {
        command.env("PRD_FILE", value);
    }
    if let Some(value) = env_cfg
        .registry
        .clone()
        .or_else(|| coordinator.and_then(|c| c.task_registry_file.clone()))
    {
        command.env("TASK_REGISTRY_FILE", value);
    }
    if let Some(value) = env_cfg
        .coordinator_tool
        .clone()
        .or_else(|| coordinator.and_then(|c| c.coordinator_tool.clone()))
    {
        command.env("COORDINATOR_TOOL", value);
    }
    if let Some(value) = env_cfg
        .reference_branch
        .clone()
        .or_else(|| coordinator.and_then(|c| c.reference_branch.clone()))
    {
        command.env("DEFAULT_BASE_BRANCH", value);
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
        command.env("TOOL_PRIORITY_CSV", value);
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
        command.env("MAX_PARALLEL_PER_TOOL_JSON", value);
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
        command.env("TOOL_SPECIALIZATIONS_JSON", value);
    }
    if let Some(value) = env_cfg
        .max_dispatch
        .or_else(|| coordinator.and_then(|c| c.max_dispatch))
    {
        command.env("MAX_DISPATCH", value.to_string());
    }
    if let Some(value) = env_cfg
        .max_parallel
        .or_else(|| coordinator.and_then(|c| c.max_parallel))
    {
        command.env("MAX_PARALLEL", value.to_string());
    }
    if let Some(value) = env_cfg
        .timeout_seconds
        .or_else(|| coordinator.and_then(|c| c.timeout_seconds))
    {
        command.env("TIMEOUT_SECONDS", value.to_string());
    }
    if let Some(value) = env_cfg
        .phase_runner_max_attempts
        .or_else(|| coordinator.and_then(|c| c.phase_runner_max_attempts))
    {
        command.env("PHASE_RUNNER_MAX_ATTEMPTS", value.to_string());
    }
    if let Some(value) = env_cfg
        .stale_claimed_seconds
        .or_else(|| coordinator.and_then(|c| c.stale_claimed_seconds))
    {
        command.env("STALE_CLAIMED_SECONDS", value.to_string());
    }
    if let Some(value) = env_cfg
        .stale_in_progress_seconds
        .or_else(|| coordinator.and_then(|c| c.stale_in_progress_seconds))
    {
        command.env("STALE_IN_PROGRESS_SECONDS", value.to_string());
    }
    if let Some(value) = env_cfg
        .stale_changes_requested_seconds
        .or_else(|| coordinator.and_then(|c| c.stale_changes_requested_seconds))
    {
        command.env("STALE_CHANGES_REQUESTED_SECONDS", value.to_string());
    }
    if let Some(value) = env_cfg
        .stale_action
        .clone()
        .or_else(|| coordinator.and_then(|c| c.stale_action.clone()))
    {
        command.env("STALE_ACTION", value);
    }
}

fn run_coordinator_action(
    repo_root: &std::path::Path,
    coordinator_path: &std::path::Path,
    action: &str,
    extra_args: &[String],
    canonical: &macc_core::config::CanonicalConfig,
    coordinator: Option<&macc_core::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
) -> Result<()> {
    let mut command = std::process::Command::new(coordinator_path);
    command.current_dir(repo_root);
    command.arg(action);
    command.args(extra_args);
    apply_coordinator_env(&mut command, canonical, coordinator, env_cfg);

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
    Ok(())
}

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
        "unlock" => "Inspect lock owners in task_registry.json then retry dispatch.",
        "sync" => "Check PRD/registry JSON validity and rerun `macc coordinator sync`.",
        _ => "Inspect logs with `macc logs tail --component coordinator`.",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RegistryCounts {
    total: usize,
    todo: usize,
    active: usize,
    blocked: usize,
    merged: usize,
}

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

fn run_coordinator_full_cycle(
    repo_root: &std::path::Path,
    coordinator_path: &std::path::Path,
    canonical: &macc_core::config::CanonicalConfig,
    coordinator: Option<&macc_core::config::CoordinatorConfig>,
    env_cfg: &CoordinatorEnvConfig,
) -> Result<()> {
    let registry_path = env_cfg
        .registry
        .clone()
        .or_else(|| coordinator.and_then(|c| c.task_registry_file.clone()))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| repo_root.join("task_registry.json"));

    let timeout_seconds = env_cfg
        .timeout_seconds
        .or_else(|| coordinator.and_then(|c| c.timeout_seconds))
        .unwrap_or(3600) as u64;
    let max_cycles = 128usize;
    let mut no_progress_cycles = 0usize;
    let started = std::time::Instant::now();

    for cycle in 1..=max_cycles {
        run_coordinator_action(
            repo_root,
            coordinator_path,
            "sync",
            &[],
            canonical,
            coordinator,
            env_cfg,
        )?;

        let before = read_registry_counts(&registry_path)?;
        run_coordinator_action(
            repo_root,
            coordinator_path,
            "dispatch",
            &[],
            canonical,
            coordinator,
            env_cfg,
        )?;
        run_coordinator_action(
            repo_root,
            coordinator_path,
            "advance",
            &[],
            canonical,
            coordinator,
            env_cfg,
        )?;
        run_coordinator_action(
            repo_root,
            coordinator_path,
            "reconcile",
            &[],
            canonical,
            coordinator,
            env_cfg,
        )?;
        run_coordinator_action(
            repo_root,
            coordinator_path,
            "cleanup",
            &[],
            canonical,
            coordinator,
            env_cfg,
        )?;
        run_coordinator_action(
            repo_root,
            coordinator_path,
            "sync",
            &[],
            canonical,
            coordinator,
            env_cfg,
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

fn remove_all_worktrees(root: &std::path::Path, remove_branches: bool) -> Result<usize> {
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
            delete_branch(root, branch.as_deref(), true)?;
        }
        removed += 1;
    }
    Ok(removed)
}

fn install_tool(paths: &macc_core::ProjectPaths, tool_id: &str, assume_yes: bool) -> Result<()> {
    let search_paths = macc_core::tool::ToolSpecLoader::default_search_paths(&paths.root);
    let loader = macc_core::tool::ToolSpecLoader::new(search_paths);
    let (specs, diagnostics) = loader.load_all_with_embedded();
    report_diagnostics(&diagnostics);

    let spec = specs
        .into_iter()
        .find(|s| s.id == tool_id)
        .ok_or_else(|| MaccError::Validation(format!("Unknown tool: {}", tool_id)))?;

    let install = spec.install.clone().ok_or_else(|| {
        MaccError::Validation(format!(
            "Tool '{}' does not define installation steps in ToolSpec.",
            tool_id
        ))
    })?;

    if install.commands.is_empty() {
        return Err(MaccError::Validation(format!(
            "Tool '{}' install commands are empty.",
            tool_id
        )));
    }

    let confirm_message = install.confirm_message.unwrap_or_else(|| {
        "You must already have an account or API key for this tool. Continue installation?"
            .to_string()
    });
    if !assume_yes {
        println!("{}", confirm_message);
        if !confirm_yes_no("Proceed [y/N]? ")? {
            return Err(MaccError::Validation("Installation cancelled.".into()));
        }
    }

    println!("Installing tool '{}'.", tool_id);
    for command in &install.commands {
        run_install_command(&paths.root, command, false)?;
    }

    let initial_checks = run_tool_health_checks(&spec);
    print_checks(&initial_checks);
    if !checks_all_installed(&initial_checks) {
        return Err(MaccError::Validation(format!(
            "Install completed but doctor checks are still failing for '{}'.",
            tool_id
        )));
    }

    if let Some(post_install) = &install.post_install {
        println!("Running post-install setup for '{}'.", tool_id);
        run_install_command(&paths.root, post_install, true)?;
    }

    let final_checks = run_tool_health_checks(&spec);
    print_checks(&final_checks);
    if !checks_all_installed(&final_checks) {
        return Err(MaccError::Validation(format!(
            "Post-install validation failed for '{}'.",
            tool_id
        )));
    }

    println!("Tool '{}' is installed and healthy.", tool_id);
    Ok(())
}

fn confirm_yes_no(prompt: &str) -> Result<bool> {
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

fn run_install_command(
    cwd: &std::path::Path,
    command: &macc_core::tool::ToolInstallCommand,
    interactive: bool,
) -> Result<()> {
    let mut cmd = std::process::Command::new(&command.command);
    cmd.args(&command.args).current_dir(cwd);
    if interactive {
        cmd.stdin(std::process::Stdio::inherit())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit());
    }
    let status = cmd.status().map_err(|e| MaccError::Io {
        path: command.command.clone(),
        action: "run tool install command".into(),
        source: e,
    })?;
    if !status.success() {
        return Err(MaccError::Validation(format!(
            "Command failed: {} {} (status: {})",
            command.command,
            command.args.join(" "),
            status
        )));
    }
    Ok(())
}

fn run_tool_health_checks(spec: &macc_core::tool::ToolSpec) -> Vec<macc_core::doctor::ToolCheck> {
    let mut checks = Vec::new();
    if let Some(doctor_specs) = &spec.doctor {
        for check_spec in doctor_specs {
            checks.push(macc_core::doctor::ToolCheck {
                name: spec.display_name.clone(),
                tool_id: Some(spec.id.clone()),
                check_target: check_spec.value.clone(),
                kind: check_spec.kind.clone(),
                status: macc_core::doctor::ToolStatus::Missing,
                severity: check_spec.severity.clone(),
            });
        }
    } else {
        checks.push(macc_core::doctor::ToolCheck {
            name: spec.display_name.clone(),
            tool_id: Some(spec.id.clone()),
            check_target: spec.id.clone(),
            kind: macc_core::tool::DoctorCheckKind::Which,
            status: macc_core::doctor::ToolStatus::Missing,
            severity: macc_core::tool::CheckSeverity::Warning,
        });
    }

    macc_core::doctor::run_checks(&mut checks);
    checks
}

fn checks_all_installed(checks: &[macc_core::doctor::ToolCheck]) -> bool {
    checks
        .iter()
        .all(|check| matches!(check.status, macc_core::doctor::ToolStatus::Installed))
}

fn print_checks(checks: &[macc_core::doctor::ToolCheck]) {
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

#[derive(Debug, Clone)]
struct WorktreeSessionStatus {
    tool: String,
    session_id: String,
    stale: bool,
}

fn canonicalize_path_fallback(path: &std::path::Path) -> std::path::PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn truncate_cell(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    if max <= 1 {
        return ".".to_string();
    }
    let keep = max.saturating_sub(3);
    let trimmed = value.chars().take(keep).collect::<String>();
    format!("{}...", trimmed)
}

fn git_worktree_is_dirty(worktree: &std::path::Path) -> Result<bool> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(["status", "--porcelain"])
        .output()
        .map_err(|e| MaccError::Io {
            path: worktree.to_string_lossy().into(),
            action: "read git worktree status".into(),
            source: e,
        })?;
    if !output.status.success() {
        return Ok(false);
    }
    Ok(!String::from_utf8_lossy(&output.stdout).trim().is_empty())
}

fn load_worktree_session_map(
    project_paths: Option<&macc_core::ProjectPaths>,
) -> Result<BTreeMap<std::path::PathBuf, WorktreeSessionStatus>> {
    let mut map = BTreeMap::new();
    let Some(paths) = project_paths else {
        return Ok(map);
    };

    let sessions_path = paths.macc_dir.join("state/tool-sessions.json");
    if !sessions_path.exists() {
        return Ok(map);
    }

    let now = unix_timestamp_secs() as i64;
    let content = std::fs::read_to_string(&sessions_path).map_err(|e| MaccError::Io {
        path: sessions_path.to_string_lossy().into(),
        action: "read tool sessions state".into(),
        source: e,
    })?;
    let root: serde_json::Value = serde_json::from_str(&content).map_err(|e| {
        MaccError::Validation(format!(
            "Failed to parse sessions file '{}': {}",
            sessions_path.display(),
            e
        ))
    })?;

    let tools = root
        .get("tools")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    for (tool_id, tool_value) in tools {
        let leases = tool_value
            .get("leases")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        for (session_id, lease) in leases {
            let status = lease
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            if status != "active" {
                continue;
            }
            let owner = lease
                .get("owner_worktree")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            if owner.is_empty() {
                continue;
            }
            let heartbeat = lease
                .get("heartbeat_epoch")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let stale = heartbeat <= 0 || (now - heartbeat) > 1800;
            let owner_path = canonicalize_path_fallback(std::path::Path::new(owner));
            map.insert(
                owner_path,
                WorktreeSessionStatus {
                    tool: tool_id.clone(),
                    session_id,
                    stale,
                },
            );
        }
    }

    Ok(map)
}

fn format_worktree_session_status(status: &WorktreeSessionStatus) -> String {
    if status.stale {
        format!("stale:{}:{}", status.tool, status.session_id)
    } else {
        format!("occupied:{}:{}", status.tool, status.session_id)
    }
}

fn select_log_file(
    paths: &macc_core::ProjectPaths,
    component: &str,
    worktree_filter: Option<&str>,
    task_filter: Option<&str>,
) -> Result<std::path::PathBuf> {
    let normalized = component.to_ascii_lowercase();
    let mut files = Vec::new();

    if normalized == "all" || normalized == "coordinator" {
        files.extend(collect_log_files(
            &paths.macc_dir.join("log/coordinator"),
            None,
        )?);
    }
    if normalized == "all" || normalized == "performer" {
        files.extend(collect_log_files(
            &paths.macc_dir.join("log/performer"),
            task_filter,
        )?);
        files.extend(collect_performer_worktree_logs(
            &paths.root,
            worktree_filter,
            task_filter,
        )?);
    }

    if files.is_empty() {
        return Err(MaccError::Validation(
            "No logs found. Run `macc coordinator run` or `macc worktree run <id>` first.".into(),
        ));
    }

    files.sort_by(|a, b| {
        let am = std::fs::metadata(a)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let bm = std::fs::metadata(b)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        bm.cmp(&am)
    });
    Ok(files[0].clone())
}

fn collect_log_files(
    dir: &std::path::Path,
    task_filter: Option<&str>,
) -> Result<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();
    if !dir.exists() {
        return Ok(files);
    }
    for entry in std::fs::read_dir(dir).map_err(|e| MaccError::Io {
        path: dir.to_string_lossy().into(),
        action: "read log directory".into(),
        source: e,
    })? {
        let path = entry
            .map_err(|e| MaccError::Io {
                path: dir.to_string_lossy().into(),
                action: "iterate log directory".into(),
                source: e,
            })?
            .path();
        if !path.is_file() {
            continue;
        }
        if let Some(filter) = task_filter {
            let name = path
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default();
            if !name.contains(filter) {
                continue;
            }
        }
        files.push(path);
    }
    Ok(files)
}

fn collect_performer_worktree_logs(
    root: &std::path::Path,
    worktree_filter: Option<&str>,
    task_filter: Option<&str>,
) -> Result<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();
    let base = root.join(".macc/worktree");
    if !base.exists() {
        return Ok(files);
    }
    for entry in std::fs::read_dir(&base).map_err(|e| MaccError::Io {
        path: base.to_string_lossy().into(),
        action: "read worktree log base".into(),
        source: e,
    })? {
        let wt = entry
            .map_err(|e| MaccError::Io {
                path: base.to_string_lossy().into(),
                action: "iterate worktree log base".into(),
                source: e,
            })?
            .path();
        if !wt.is_dir() {
            continue;
        }
        if let Some(filter) = worktree_filter {
            let needle = filter.to_ascii_lowercase();
            let text = wt.display().to_string().to_ascii_lowercase();
            if !text.contains(&needle) {
                continue;
            }
        }
        let log_dir = wt.join(".macc/log/performer");
        files.extend(collect_log_files(&log_dir, task_filter)?);
    }
    Ok(files)
}

fn print_file_tail(path: &std::path::Path, lines: usize) -> Result<()> {
    let content = std::fs::read_to_string(path).map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "read log file".into(),
        source: e,
    })?;
    let all = content.lines().collect::<Vec<_>>();
    let start = all.len().saturating_sub(lines);
    for line in &all[start..] {
        println!("{}", line);
    }
    Ok(())
}

fn tail_file_follow(path: &std::path::Path, lines: usize) -> Result<()> {
    let status = std::process::Command::new("tail")
        .arg("-n")
        .arg(lines.to_string())
        .arg("-F")
        .arg(path)
        .status()
        .map_err(|e| MaccError::Io {
            path: "tail".into(),
            action: "follow log file".into(),
            source: e,
        })?;
    if !status.success() {
        return Err(MaccError::Validation(format!(
            "tail failed with status: {}",
            status
        )));
    }
    Ok(())
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

fn list_backup_sets_command(paths: &macc_core::ProjectPaths, user: bool) -> Result<()> {
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

fn open_backup_set_command(
    paths: &macc_core::ProjectPaths,
    id: Option<&str>,
    latest: bool,
    user: bool,
    editor: &Option<String>,
) -> Result<()> {
    let set = resolve_backup_set_path(paths, user, id, latest)?;
    println!("Backup set: {}", set.display());
    if let Some(cmd) = editor {
        open_in_editor(&set, cmd)?;
    }
    Ok(())
}

fn restore_backup_set_command(
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DoctorLevel {
    Info,
    Warning,
    Error,
}

struct DoctorIssue {
    level: DoctorLevel,
    check: String,
    status: String,
    detail: String,
    suggestion: Option<String>,
    fixed: bool,
}

fn run_doctor<E: Engine>(paths: &macc_core::ProjectPaths, engine: &E, fix: bool) -> Result<()> {
    let mut issues = Vec::<DoctorIssue>::new();
    let checks = engine.doctor(paths);
    let search_paths = macc_core::tool::ToolSpecLoader::default_search_paths(&paths.root);
    let loader = macc_core::tool::ToolSpecLoader::new(search_paths);
    let (specs, diagnostics) = loader.load_all_with_embedded();
    report_diagnostics(&diagnostics);

    collect_tool_binary_issues(&checks, &specs, &mut issues);
    collect_path_permission_issues(paths, fix, &mut issues)?;
    collect_worktree_and_session_issues(paths, fix, &mut issues)?;
    collect_cache_issues(paths, fix, &mut issues)?;
    collect_gitignore_cache_issue(paths, fix, &mut issues)?;

    print_doctor_issues(&issues);

    let errors = issues
        .iter()
        .filter(|i| matches!(i.level, DoctorLevel::Error))
        .count();
    let warnings = issues
        .iter()
        .filter(|i| matches!(i.level, DoctorLevel::Warning))
        .count();
    let fixed = issues.iter().filter(|i| i.fixed).count();

    println!(
        "\nDoctor summary: errors={}, warnings={}, fixed={}",
        errors, warnings, fixed
    );

    if errors > 0 {
        return Err(MaccError::Validation(
            "Doctor found blocking issues. See actionable suggestions above.".into(),
        ));
    }
    Ok(())
}

fn collect_tool_binary_issues(
    checks: &[macc_core::doctor::ToolCheck],
    specs: &[macc_core::tool::ToolSpec],
    issues: &mut Vec<DoctorIssue>,
) {
    for check in checks {
        let level = match (&check.status, &check.severity) {
            (macc_core::doctor::ToolStatus::Installed, _) => DoctorLevel::Info,
            (macc_core::doctor::ToolStatus::Missing, macc_core::tool::CheckSeverity::Error) => {
                DoctorLevel::Error
            }
            (macc_core::doctor::ToolStatus::Missing, macc_core::tool::CheckSeverity::Warning) => {
                DoctorLevel::Warning
            }
            (macc_core::doctor::ToolStatus::Error(_), _) => DoctorLevel::Error,
        };

        let status = match &check.status {
            macc_core::doctor::ToolStatus::Installed => "OK".to_string(),
            macc_core::doctor::ToolStatus::Missing => "MISSING".to_string(),
            macc_core::doctor::ToolStatus::Error(err) => format!("ERROR ({})", err),
        };

        let suggestion = if matches!(check.status, macc_core::doctor::ToolStatus::Missing) {
            check
                .tool_id
                .as_ref()
                .map(|tool_id| format!("Run: macc tool install {}", tool_id))
                .or_else(|| {
                    if check.check_target == "git" {
                        Some("Install git with your package manager (e.g. apt install git).".into())
                    } else {
                        None
                    }
                })
                .or_else(|| find_tool_install_hint(check.tool_id.as_deref(), specs))
        } else {
            None
        };

        issues.push(DoctorIssue {
            level,
            check: format!("tool:{}", check.name),
            status,
            detail: format!("target={}", check.check_target),
            suggestion,
            fixed: false,
        });
    }
}

fn find_tool_install_hint(
    tool_id: Option<&str>,
    specs: &[macc_core::tool::ToolSpec],
) -> Option<String> {
    let id = tool_id?;
    let spec = specs.iter().find(|s| s.id == id)?;
    let install = spec.install.as_ref()?;
    let first = install.commands.first()?;
    Some(format!(
        "Suggested install command: {} {}",
        first.command,
        first.args.join(" ")
    ))
}

fn collect_path_permission_issues(
    paths: &macc_core::ProjectPaths,
    fix: bool,
    issues: &mut Vec<DoctorIssue>,
) -> Result<()> {
    let expected_dirs = vec![
        paths.macc_dir.clone(),
        paths.cache_dir.clone(),
        paths.macc_dir.join("state"),
        paths.macc_dir.join("log"),
        paths.macc_dir.join("log/coordinator"),
        paths.macc_dir.join("log/performer"),
        paths.automation_dir(),
    ];

    for dir in expected_dirs {
        let mut fixed = false;
        if !dir.exists() {
            let mut suggestion = Some("Create missing directory.".to_string());
            let mut level = DoctorLevel::Warning;
            let mut status = "MISSING".to_string();
            if fix {
                std::fs::create_dir_all(&dir).map_err(|e| MaccError::Io {
                    path: dir.to_string_lossy().into(),
                    action: "create missing doctor directory".into(),
                    source: e,
                })?;
                fixed = true;
                level = DoctorLevel::Info;
                status = "FIXED".to_string();
                suggestion = None;
            }
            issues.push(DoctorIssue {
                level,
                check: "path".into(),
                status,
                detail: format!("missing directory {}", dir.display()),
                suggestion,
                fixed,
            });
            continue;
        }

        if !dir.is_dir() {
            issues.push(DoctorIssue {
                level: DoctorLevel::Error,
                check: "path".into(),
                status: "INVALID".into(),
                detail: format!("expected directory but found file: {}", dir.display()),
                suggestion: Some("Replace this path with a directory.".into()),
                fixed: false,
            });
            continue;
        }

        match test_dir_permissions(&dir) {
            Ok(()) => issues.push(DoctorIssue {
                level: DoctorLevel::Info,
                check: "path".into(),
                status: "OK".into(),
                detail: format!("read/write {}", dir.display()),
                suggestion: None,
                fixed: false,
            }),
            Err(reason) => issues.push(DoctorIssue {
                level: DoctorLevel::Error,
                check: "path".into(),
                status: "PERMISSION".into(),
                detail: format!("{} ({})", dir.display(), reason),
                suggestion: Some(format!(
                    "Fix permissions, e.g. chmod/chown so current user can read/write {}",
                    dir.display()
                )),
                fixed: false,
            }),
        }
    }

    Ok(())
}

fn test_dir_permissions(dir: &std::path::Path) -> std::result::Result<(), String> {
    std::fs::read_dir(dir).map_err(|e| format!("cannot read dir: {}", e))?;
    let probe = dir.join(format!(".doctor-write-{}.tmp", std::process::id()));
    std::fs::write(&probe, b"ok").map_err(|e| format!("cannot write dir: {}", e))?;
    std::fs::remove_file(&probe).map_err(|e| format!("cannot cleanup probe file: {}", e))?;
    Ok(())
}

fn collect_worktree_and_session_issues(
    paths: &macc_core::ProjectPaths,
    fix: bool,
    issues: &mut Vec<DoctorIssue>,
) -> Result<()> {
    let entries = macc_core::list_worktrees(&paths.root)?;
    let root_canon = paths
        .root
        .canonicalize()
        .unwrap_or_else(|_| paths.root.clone());
    let active = entries.iter().filter(|e| e.path != root_canon).count();
    issues.push(DoctorIssue {
        level: DoctorLevel::Info,
        check: "worktree".into(),
        status: "OK".into(),
        detail: format!("worktrees total={}, active={}", entries.len(), active),
        suggestion: None,
        fixed: false,
    });

    let sessions_path = paths.macc_dir.join("state/tool-sessions.json");
    if !sessions_path.exists() {
        let mut fixed_now = false;
        if fix {
            write_default_sessions_file(&sessions_path)?;
            fixed_now = true;
        }
        issues.push(DoctorIssue {
            level: if fixed_now {
                DoctorLevel::Info
            } else {
                DoctorLevel::Warning
            },
            check: "sessions".into(),
            status: if fixed_now {
                "FIXED".into()
            } else {
                "MISSING".into()
            },
            detail: format!("missing {}", sessions_path.display()),
            suggestion: if fixed_now {
                None
            } else {
                Some("Create .macc/state/tool-sessions.json (or run with --fix).".into())
            },
            fixed: fixed_now,
        });
        return Ok(());
    }

    let content = std::fs::read_to_string(&sessions_path).map_err(|e| MaccError::Io {
        path: sessions_path.to_string_lossy().into(),
        action: "read tool sessions".into(),
        source: e,
    })?;
    match serde_json::from_str::<serde_json::Value>(&content) {
        Ok(value) => {
            let tools = value
                .get("tools")
                .and_then(|v| v.as_object())
                .map(|v| v.len())
                .unwrap_or(0);
            let active_leases = value
                .get("tools")
                .and_then(|t| t.as_object())
                .map(|all| {
                    all.values()
                        .filter_map(|tool| tool.get("leases").and_then(|v| v.as_object()))
                        .flat_map(|leases| leases.values())
                        .filter(|lease| {
                            lease.get("status").and_then(|s| s.as_str()) == Some("active")
                        })
                        .count()
                })
                .unwrap_or(0);
            issues.push(DoctorIssue {
                level: DoctorLevel::Info,
                check: "sessions".into(),
                status: "OK".into(),
                detail: format!(
                    "session state valid (tools={}, active_leases={})",
                    tools, active_leases
                ),
                suggestion: None,
                fixed: false,
            });
        }
        Err(err) => {
            let mut fixed_now = false;
            let mut detail = format!("invalid JSON in {} ({})", sessions_path.display(), err);
            if fix {
                let backup =
                    sessions_path.with_extension(format!("corrupt-{}.json", unix_timestamp_secs()));
                std::fs::rename(&sessions_path, &backup).map_err(|e| MaccError::Io {
                    path: sessions_path.to_string_lossy().into(),
                    action: format!("backup corrupt sessions to {}", backup.display()),
                    source: e,
                })?;
                write_default_sessions_file(&sessions_path)?;
                fixed_now = true;
                detail = format!(
                    "replaced corrupt sessions file; backup kept at {}",
                    backup.display()
                );
            }
            issues.push(DoctorIssue {
                level: if fixed_now {
                    DoctorLevel::Warning
                } else {
                    DoctorLevel::Error
                },
                check: "sessions".into(),
                status: if fixed_now {
                    "FIXED".into()
                } else {
                    "CORRUPT".into()
                },
                detail,
                suggestion: if fixed_now {
                    None
                } else {
                    Some("Run `macc doctor --fix` to backup and recreate sessions state.".into())
                },
                fixed: fixed_now,
            });
        }
    }
    Ok(())
}

fn write_default_sessions_file(path: &std::path::Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| MaccError::Io {
            path: parent.to_string_lossy().into(),
            action: "create sessions state parent directory".into(),
            source: e,
        })?;
    }
    let data = serde_json::json!({
        "tools": {}
    });
    let mut content = serde_json::to_string_pretty(&data)
        .map_err(|e| MaccError::Validation(format!("serialize default sessions JSON: {}", e)))?;
    content.push('\n');
    std::fs::write(path, content).map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "write default sessions state".into(),
        source: e,
    })?;
    Ok(())
}

fn collect_cache_issues(
    paths: &macc_core::ProjectPaths,
    fix: bool,
    issues: &mut Vec<DoctorIssue>,
) -> Result<()> {
    let cache_dir = &paths.cache_dir;
    if !cache_dir.exists() {
        let mut fixed_now = false;
        if fix {
            std::fs::create_dir_all(cache_dir).map_err(|e| MaccError::Io {
                path: cache_dir.to_string_lossy().into(),
                action: "create cache directory".into(),
                source: e,
            })?;
            fixed_now = true;
        }
        issues.push(DoctorIssue {
            level: if fixed_now {
                DoctorLevel::Info
            } else {
                DoctorLevel::Warning
            },
            check: "cache".into(),
            status: if fixed_now { "FIXED" } else { "MISSING" }.into(),
            detail: format!("cache directory {}", cache_dir.display()),
            suggestion: if fixed_now {
                None
            } else {
                Some("Create .macc/cache (or run with --fix).".into())
            },
            fixed: fixed_now,
        });
        return Ok(());
    }

    let mut entries = 0usize;
    let mut broken = 0usize;
    for entry in std::fs::read_dir(cache_dir).map_err(|e| MaccError::Io {
        path: cache_dir.to_string_lossy().into(),
        action: "read cache directory".into(),
        source: e,
    })? {
        let entry = match entry {
            Ok(v) => v,
            Err(_) => {
                broken += 1;
                continue;
            }
        };
        entries += 1;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("tmp") {
            broken += 1;
            if fix {
                let _ = std::fs::remove_file(&path);
            }
            continue;
        }
        if let Ok(meta) = std::fs::symlink_metadata(&path) {
            if meta.file_type().is_symlink() && std::fs::metadata(&path).is_err() {
                broken += 1;
                if fix {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
    }
    issues.push(DoctorIssue {
        level: if broken == 0 {
            DoctorLevel::Info
        } else {
            DoctorLevel::Warning
        },
        check: "cache".into(),
        status: if broken == 0 {
            "OK".into()
        } else if fix {
            "FIXED/PARTIAL".into()
        } else {
            "WARN".into()
        },
        detail: format!(
            "cache entries={}, suspicious_or_corrupt={}",
            entries, broken
        ),
        suggestion: if broken == 0 {
            None
        } else {
            Some("Run `macc doctor --fix` or remove corrupted cache entries manually.".into())
        },
        fixed: fix && broken > 0,
    });
    Ok(())
}

fn collect_gitignore_cache_issue(
    paths: &macc_core::ProjectPaths,
    fix: bool,
    issues: &mut Vec<DoctorIssue>,
) -> Result<()> {
    let gitignore = paths.root.join(".gitignore");
    let required = ".macc/cache/";
    let mut content = String::new();
    if gitignore.exists() {
        content = std::fs::read_to_string(&gitignore).map_err(|e| MaccError::Io {
            path: gitignore.to_string_lossy().into(),
            action: "read .gitignore".into(),
            source: e,
        })?;
    }
    let present = content.lines().any(|line| line.trim() == required);
    if present {
        issues.push(DoctorIssue {
            level: DoctorLevel::Info,
            check: "gitignore".into(),
            status: "OK".into(),
            detail: format!("contains '{}'", required),
            suggestion: None,
            fixed: false,
        });
        return Ok(());
    }

    let mut fixed_now = false;
    if fix {
        if !content.ends_with('\n') && !content.is_empty() {
            content.push('\n');
        }
        content.push_str(required);
        content.push('\n');
        std::fs::write(&gitignore, content).map_err(|e| MaccError::Io {
            path: gitignore.to_string_lossy().into(),
            action: "update .gitignore with cache entry".into(),
            source: e,
        })?;
        fixed_now = true;
    }

    issues.push(DoctorIssue {
        level: if fixed_now {
            DoctorLevel::Info
        } else {
            DoctorLevel::Warning
        },
        check: "gitignore".into(),
        status: if fixed_now { "FIXED" } else { "MISSING" }.into(),
        detail: format!("missing '{}' in {}", required, gitignore.display()),
        suggestion: if fixed_now {
            None
        } else {
            Some("Add '.macc/cache/' to .gitignore (or run with --fix).".into())
        },
        fixed: fixed_now,
    });
    Ok(())
}

fn print_doctor_issues(issues: &[DoctorIssue]) {
    println!(
        "{:<10} {:<18} {:<14} {:<60}",
        "LEVEL", "CHECK", "STATUS", "DETAIL"
    );
    println!("{:-<10} {:-<18} {:-<14} {:-<60}", "", "", "", "");

    for issue in issues {
        let level = match issue.level {
            DoctorLevel::Info => "INFO",
            DoctorLevel::Warning => "WARN",
            DoctorLevel::Error => "ERROR",
        };
        println!(
            "{:<10} {:<18} {:<14} {:<60}",
            level, issue.check, issue.status, issue.detail
        );
        if let Some(s) = &issue.suggestion {
            println!("{:<10} {:<18} {:<14} -> {}", "", "suggestion", "", s);
        }
    }
}

fn apply_worktree<E: Engine>(
    engine: &E,
    worktree_root: &std::path::Path,
    allow_user_scope: bool,
) -> Result<()> {
    let paths = macc_core::ProjectPaths::from_root(worktree_root);
    let canonical = load_canonical_config(&paths.config_path)?;
    let metadata = macc_core::read_worktree_metadata(worktree_root)?
        .ok_or_else(|| MaccError::Validation("Missing .macc/worktree.json".into()))?;

    let (descriptors, diagnostics) = engine.list_tools(&paths);
    report_diagnostics(&diagnostics);
    let allowed_tools: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();
    let overrides = CliOverrides::from_tools_csv(metadata.tool.as_str(), &allowed_tools)?;

    let resolved = resolve(&canonical, &overrides);
    let fetch_units = resolve_fetch_units(&paths, &resolved)?;
    let materialized_units =
        macc_adapter_shared::fetch::materialize_fetch_units(&paths, fetch_units)?;

    let mut plan = engine.plan(&paths, &canonical, &materialized_units, &overrides)?;
    let _ = engine.apply(&paths, &mut plan, allow_user_scope)?;
    Ok(())
}

fn open_in_editor(path: &std::path::Path, command: &str) -> Result<()> {
    let mut parts = command.split_whitespace();
    let Some(bin) = parts.next() else {
        return Ok(());
    };
    let mut cmd = std::process::Command::new(bin);
    for arg in parts {
        cmd.arg(arg);
    }
    let status = cmd.arg(path).status().map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "launch editor".into(),
        source: e,
    })?;
    if !status.success() {
        return Err(MaccError::Validation(format!(
            "Editor command failed with status: {}",
            status
        )));
    }
    Ok(())
}

fn open_in_terminal(path: &std::path::Path) -> Result<()> {
    if let Ok(term) = std::env::var("TERMINAL") {
        launch_terminal(&term, path)?;
        return Ok(());
    }

    let candidates = [
        ("x-terminal-emulator", &["-e", "bash", "-lc"]),
        ("gnome-terminal", &["--", "bash", "-lc"]),
        ("konsole", &["-e", "bash", "-lc"]),
        ("xterm", &["-e", "bash", "-lc"]),
    ];
    for (bin, prefix) in candidates {
        if launch_terminal_with_prefix(bin, prefix, path).is_ok() {
            return Ok(());
        }
    }

    Err(MaccError::Validation(
        "No terminal launcher found (set $TERMINAL)".into(),
    ))
}

fn launch_terminal(command: &str, path: &std::path::Path) -> Result<()> {
    let mut parts = command.split_whitespace();
    let Some(bin) = parts.next() else {
        return Ok(());
    };
    let mut cmd = std::process::Command::new(bin);
    for arg in parts {
        cmd.arg(arg);
    }
    cmd.arg("--");
    cmd.arg("bash");
    cmd.arg("-lc");
    cmd.arg(format!("cd {}; exec $SHELL", path.display()));
    cmd.spawn().map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "launch terminal".into(),
        source: e,
    })?;
    Ok(())
}

fn launch_terminal_with_prefix(bin: &str, prefix: &[&str], path: &std::path::Path) -> Result<()> {
    let mut cmd = std::process::Command::new(bin);
    for arg in prefix {
        cmd.arg(arg);
    }
    cmd.arg(format!("cd {}; exec $SHELL", path.display()));
    cmd.spawn().map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "launch terminal".into(),
        source: e,
    })?;
    Ok(())
}

// ... existing catalog functions (run_remote_search, list_skills, etc) ...

fn run_remote_search(
    paths: &macc_core::ProjectPaths,
    api: String,
    kind: String,
    q: String,
    add: bool,
    add_ids: Option<String>,
) -> Result<()> {
    let search_kind = match kind.as_str() {
        "skill" => RemoteSearchKind::Skill,
        "mcp" => RemoteSearchKind::Mcp,
        _ => {
            return Err(MaccError::Validation(format!(
                "Invalid kind: {}. Must be 'skill' or 'mcp'.",
                kind
            )))
        }
    };

    println!("Searching {} for '{}' in {}...", kind, q, api);

    let whitelist: Option<Vec<String>> = add_ids
        .as_ref()
        .map(|s| s.split(',').map(|i| i.trim().to_string()).collect());
    let should_save = add || whitelist.is_some();

    match search_kind {
        RemoteSearchKind::Skill => {
            let results: Vec<SkillEntry> = remote_search(&api, search_kind, &q)?;
            if results.is_empty() {
                println!("No skills found.");
                return Ok(());
            }

            println!("{:<20} {:<30} {:<10} {:<20}", "ID", "NAME", "KIND", "TAGS");
            println!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", "");

            let mut catalog = if should_save {
                Some(SkillsCatalog::load(&paths.skills_catalog_path())?)
            } else {
                None
            };

            for entry in &results {
                let tags = entry.tags.join(", ");
                let kind_str = match entry.source.kind {
                    SourceKind::Git => "git",
                    SourceKind::Http => "http",
                    SourceKind::Local => "local",
                };
                println!(
                    "{:<20} {:<30} {:<10} {:<20}",
                    entry.id, entry.name, kind_str, tags
                );

                if let Some(cat) = &mut catalog {
                    let should_add = if add {
                        true
                    } else if let Some(wl) = &whitelist {
                        wl.contains(&entry.id)
                    } else {
                        false
                    };

                    if should_add {
                        cat.upsert_skill_entry(entry.clone());
                        println!("  [+] Queued import for '{}'", entry.id);
                    }
                }
            }

            if let Some(cat) = catalog {
                cat.save_atomically(paths, &paths.skills_catalog_path())?;
                println!("Saved changes to skills catalog.");
            }
        }
        RemoteSearchKind::Mcp => {
            let results: Vec<McpEntry> = remote_search(&api, search_kind, &q)?;
            if results.is_empty() {
                println!("No MCP servers found.");
                return Ok(());
            }

            println!("{:<20} {:<30} {:<10} {:<20}", "ID", "NAME", "KIND", "TAGS");
            println!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", "");

            let mut catalog = if should_save {
                Some(McpCatalog::load(&paths.mcp_catalog_path())?)
            } else {
                None
            };

            for entry in &results {
                let tags = entry.tags.join(", ");
                let kind_str = match entry.source.kind {
                    SourceKind::Git => "git",
                    SourceKind::Http => "http",
                    SourceKind::Local => "local",
                };
                println!(
                    "{:<20} {:<30} {:<10} {:<20}",
                    entry.id, entry.name, kind_str, tags
                );

                if let Some(cat) = &mut catalog {
                    let should_add = if add {
                        true
                    } else if let Some(wl) = &whitelist {
                        wl.contains(&entry.id)
                    } else {
                        false
                    };

                    if should_add {
                        cat.upsert_mcp_entry(entry.clone());
                        println!("  [+] Queued import for '{}'", entry.id);
                    }
                }
            }

            if let Some(cat) = catalog {
                cat.save_atomically(paths, &paths.mcp_catalog_path())?;
                println!("Saved changes to MCP catalog.");
            }
        }
    }

    Ok(())
}

fn report_diagnostics(diagnostics: &[macc_core::tool::ToolDiagnostic]) {
    for diag in diagnostics {
        let location = match (diag.line, diag.column) {
            (Some(l), Some(c)) => format!(" at {}:{}", l, c),
            (Some(l), None) => format!(" at line {}", l),
            _ => "".to_string(),
        };
        eprintln!(
            "Error loading tool spec {}{}: {}",
            diag.path.display(),
            location,
            diag.error
        );
    }
}

fn list_skills(catalog: &SkillsCatalog) {
    if catalog.entries.is_empty() {
        println!("No skills found in catalog.");
        return;
    }

    println!("{:<20} {:<30} {:<10} {:<20}", "ID", "NAME", "KIND", "TAGS");
    println!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", "");
    for entry in &catalog.entries {
        let tags = entry.tags.join(", ");
        let kind = match entry.source.kind {
            SourceKind::Git => "git",
            SourceKind::Http => "http",
            SourceKind::Local => "local",
        };
        println!(
            "{:<20} {:<30} {:<10} {:<20}",
            entry.id, entry.name, kind, tags
        );
    }
}

fn search_skills(catalog: &SkillsCatalog, query: &str) {
    let query = query.to_lowercase();
    let filtered: Vec<_> = catalog
        .entries
        .iter()
        .filter(|e| {
            e.id.to_lowercase().contains(&query)
                || e.name.to_lowercase().contains(&query)
                || e.description.to_lowercase().contains(&query)
                || e.tags.iter().any(|t| t.to_lowercase().contains(&query))
        })
        .collect();

    if filtered.is_empty() {
        println!("No skills matching '{}' found.", query);
        return;
    }

    println!("{:<20} {:<30} {:<10} {:<20}", "ID", "NAME", "KIND", "TAGS");
    println!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", "");
    for entry in filtered {
        let tags = entry.tags.join(", ");
        let kind = match entry.source.kind {
            SourceKind::Git => "git",
            SourceKind::Http => "http",
            SourceKind::Local => "local",
        };
        println!(
            "{:<20} {:<30} {:<10} {:<20}",
            entry.id, entry.name, kind, tags
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn add_skill(
    paths: &macc_core::ProjectPaths,
    catalog: &mut SkillsCatalog,
    id: String,
    name: String,
    description: String,
    tags: Option<String>,
    subpath: String,
    kind: String,
    url: String,
    reference: String,
    checksum: Option<String>,
) -> Result<()> {
    let tags = tags
        .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();
    let source_kind = match kind.to_lowercase().as_str() {
        "git" => SourceKind::Git,
        "http" => SourceKind::Http,
        "local" => SourceKind::Local,
        _ => {
            return Err(MaccError::Validation(format!(
                "Invalid source kind: {}. Must be 'git', 'http', or 'local'.",
                kind
            )))
        }
    };

    let entry = SkillEntry {
        id: id.clone(),
        name,
        description,
        tags,
        selector: Selector { subpath },
        source: Source {
            kind: source_kind,
            url,
            reference,
            checksum,
            subpaths: vec![],
        },
    };

    catalog.upsert_skill_entry(entry);
    catalog.save_atomically(paths, &paths.skills_catalog_path())?;
    println!("Skill '{}' upserted successfully.", id);
    Ok(())
}

fn remove_skill(
    paths: &macc_core::ProjectPaths,
    catalog: &mut SkillsCatalog,
    id: String,
) -> Result<()> {
    if catalog.delete_skill_entry(&id) {
        catalog.save_atomically(paths, &paths.skills_catalog_path())?;
        println!("Skill '{}' removed successfully.", id);
    } else {
        println!("Skill '{}' not found in catalog.", id);
    }
    Ok(())
}

fn list_mcp(catalog: &McpCatalog) {
    if catalog.entries.is_empty() {
        println!("No MCP servers found in catalog.");
        return;
    }

    println!("{:<20} {:<30} {:<10} {:<20}", "ID", "NAME", "KIND", "TAGS");
    println!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", "");
    for entry in &catalog.entries {
        let tags = entry.tags.join(", ");
        let kind = match entry.source.kind {
            SourceKind::Git => "git",
            SourceKind::Http => "http",
            SourceKind::Local => "local",
        };
        println!(
            "{:<20} {:<30} {:<10} {:<20}",
            entry.id, entry.name, kind, tags
        );
    }
}

fn search_mcp(catalog: &McpCatalog, query: &str) {
    let query = query.to_lowercase();
    let filtered: Vec<_> = catalog
        .entries
        .iter()
        .filter(|e| {
            e.id.to_lowercase().contains(&query)
                || e.name.to_lowercase().contains(&query)
                || e.description.to_lowercase().contains(&query)
                || e.tags.iter().any(|t| t.to_lowercase().contains(&query))
        })
        .collect();

    if filtered.is_empty() {
        println!("No MCP servers matching '{}' found.", query);
        return;
    }

    println!("{:<20} {:<30} {:<10} {:<20}", "ID", "NAME", "KIND", "TAGS");
    println!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", "");
    for entry in filtered {
        let tags = entry.tags.join(", ");
        let kind = match entry.source.kind {
            SourceKind::Git => "git",
            SourceKind::Http => "http",
            SourceKind::Local => "local",
        };
        println!(
            "{:<20} {:<30} {:<10} {:<20}",
            entry.id, entry.name, kind, tags
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn add_mcp(
    paths: &macc_core::ProjectPaths,
    catalog: &mut McpCatalog,
    id: String,
    name: String,
    description: String,
    tags: Option<String>,
    subpath: String,
    kind: String,
    url: String,
    reference: String,
    checksum: Option<String>,
) -> Result<()> {
    let tags = tags
        .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();
    let source_kind = match kind.to_lowercase().as_str() {
        "git" => SourceKind::Git,
        "http" => SourceKind::Http,
        "local" => SourceKind::Local,
        _ => {
            return Err(MaccError::Validation(format!(
                "Invalid source kind: {}. Must be 'git', 'http', or 'local'.",
                kind
            )))
        }
    };

    let entry = McpEntry {
        id: id.clone(),
        name,
        description,
        tags,
        selector: Selector { subpath },
        source: Source {
            kind: source_kind,
            url,
            reference,
            checksum,
            subpaths: vec![],
        },
    };

    catalog.upsert_mcp_entry(entry);
    catalog.save_atomically(paths, &paths.mcp_catalog_path())?;
    println!("MCP server '{}' upserted successfully.", id);
    Ok(())
}

fn remove_mcp(paths: &macc_core::ProjectPaths, catalog: &mut McpCatalog, id: String) -> Result<()> {
    if catalog.delete_mcp_entry(&id) {
        catalog.save_atomically(paths, &paths.mcp_catalog_path())?;
        println!("MCP server '{}' removed successfully.", id);
    } else {
        println!("MCP server '{}' not found in catalog.", id);
    }
    Ok(())
}

fn install_skill<E: Engine>(
    paths: &macc_core::ProjectPaths,
    tool: &str,
    id: &str,
    engine: &E,
) -> Result<()> {
    // 1. Find entry
    let catalog = load_effective_skills_catalog(paths)?;
    let entry =
        catalog.entries.iter().find(|e| e.id == id).ok_or_else(|| {
            MaccError::Validation(format!("Skill '{}' not found in catalog.", id))
        })?;

    let (descriptors, diagnostics) = engine.list_tools(paths);
    report_diagnostics(&diagnostics);
    let tool_title = descriptors
        .iter()
        .find(|d| d.id == tool)
        .map(|d| d.title.as_str())
        .unwrap_or(tool);

    println!("Installing skill '{}' for {}...", id, tool_title);

    // 2. Materialize
    let mut source = entry.source.clone();
    if !entry.selector.subpath.is_empty() && entry.selector.subpath != "." {
        source.subpaths = vec![entry.selector.subpath.clone()];
    }

    let fetch_unit = FetchUnit {
        source,
        selections: vec![Selection {
            id: entry.id.clone(),
            subpath: entry.selector.subpath.clone(),
            kind: SelectionKind::Skill,
        }],
    };
    let materialized = macc_adapter_shared::fetch::materialize_fetch_unit(paths, fetch_unit)?;

    // 3. Plan
    let mut plan = ActionPlan::new();
    plan_skill_install(
        &mut plan,
        tool,
        id,
        &materialized.source_root_path,
        &entry.selector.subpath,
    )
    .map_err(MaccError::Validation)?;

    // 4. Apply
    let report = engine.apply(paths, &mut plan, false)?;
    println!("{}", report.render_cli());

    Ok(())
}

fn install_mcp<E: Engine>(paths: &macc_core::ProjectPaths, id: &str, engine: &E) -> Result<()> {
    // 1. Load catalog and find MCP
    let catalog = load_effective_mcp_catalog(paths)?;
    let entry = catalog.entries.iter().find(|e| e.id == id).ok_or_else(|| {
        MaccError::Validation(format!("MCP server '{}' not found in catalog.", id))
    })?;

    println!("Installing MCP server '{}'...", id);

    // 2. Materialize
    let mut source = entry.source.clone();
    if !entry.selector.subpath.is_empty() && entry.selector.subpath != "." {
        source.subpaths = vec![entry.selector.subpath.clone()];
    }

    let fetch_unit = FetchUnit {
        source,
        selections: vec![Selection {
            id: entry.id.clone(),
            subpath: entry.selector.subpath.clone(),
            kind: SelectionKind::Mcp,
        }],
    };
    let materialized = macc_adapter_shared::fetch::materialize_fetch_unit(paths, fetch_unit)?;

    // 3. Plan
    let mut plan = ActionPlan::new();
    plan_mcp_install(
        &mut plan,
        id,
        &materialized.source_root_path,
        &entry.selector.subpath,
    )
    .map_err(MaccError::Validation)?;

    // 4. Apply
    let report = engine.apply(paths, &mut plan, false)?;
    println!("{}", report.render_cli());

    Ok(())
}

fn import_url(
    paths: &macc_core::ProjectPaths,
    kind: &str,
    id: String,
    url: String,
    name: Option<String>,
    description: String,
    tags: Option<String>,
) -> Result<()> {
    // 1. Normalize URL (GitHub tree/repo preferred, HTTP fallback).
    let (source_kind, clone_or_url, reference, subpath) =
        if let Some(normalized) = macc_adapter_shared::url_parsing::normalize_git_input(&url) {
            (
                SourceKind::Git,
                normalized.clone_url,
                normalized.reference,
                normalized.subpath,
            )
        } else if macc_adapter_shared::url_parsing::validate_http_url(&url) {
            (
                SourceKind::Http,
                url.trim().to_string(),
                String::new(),
                String::new(),
            )
        } else {
            return Err(MaccError::Validation(format!(
                "Invalid or unsupported URL: {}",
                url
            )));
        };

    // 2. Prepare common fields
    let name = name.unwrap_or_else(|| id.clone());
    let tags_vec = tags
        .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();

    let selector = Selector {
        subpath: subpath.clone(),
    };
    let source = Source {
        kind: source_kind.clone(),
        url: clone_or_url.clone(),
        reference: reference.clone(),
        checksum: None,
        subpaths: vec![],
    };

    print_import_understanding(kind, &id, &source, &selector);
    print_trust_hints(&source);
    validate_import_source_now(paths, kind, &id, &source, &selector);

    // 3. Upsert
    match kind {
        "skill" => {
            let mut catalog = SkillsCatalog::load(&paths.skills_catalog_path())?;
            let entry = SkillEntry {
                id: id.clone(),
                name,
                description,
                tags: tags_vec,
                selector,
                source,
            };
            catalog.upsert_skill_entry(entry);
            catalog.save_atomically(paths, &paths.skills_catalog_path())?;
            println!("Skill '{}' imported successfully.", id);
        }
        "mcp" => {
            let mut catalog = McpCatalog::load(&paths.mcp_catalog_path())?;
            let entry = McpEntry {
                id: id.clone(),
                name,
                description,
                tags: tags_vec,
                selector,
                source,
            };
            catalog.upsert_mcp_entry(entry);
            catalog.save_atomically(paths, &paths.mcp_catalog_path())?;
            println!("MCP server '{}' imported successfully.", id);
        }
        _ => unreachable!("clap should prevent this"),
    }
    Ok(())
}

fn print_import_understanding(kind: &str, id: &str, source: &Source, selector: &Selector) {
    println!("Import URL: here's what I understood:");
    println!("  - catalog kind: {}", kind);
    println!("  - entry id: {}", id);
    println!(
        "  - source kind: {}",
        match source.kind {
            SourceKind::Git => "git",
            SourceKind::Http => "http",
            SourceKind::Local => "local",
        }
    );
    println!("  - source url: {}", source.url);
    println!(
        "  - source ref: {}",
        if source.reference.is_empty() {
            "(default branch/head)"
        } else {
            source.reference.as_str()
        }
    );
    println!(
        "  - subpath: {}",
        if selector.subpath.is_empty() {
            "(root)"
        } else {
            selector.subpath.as_str()
        }
    );
}

fn print_trust_hints(source: &Source) {
    let pinned_ref_hint = if source.kind == SourceKind::Git {
        if is_commit_sha(&source.reference) {
            "ref appears pinned to commit SHA (strong reproducibility)"
        } else if source.reference.is_empty()
            || source.reference.eq_ignore_ascii_case("main")
            || source.reference.eq_ignore_ascii_case("master")
        {
            "ref is moving/default branch (lower reproducibility)"
        } else {
            "ref looks like tag/branch; pin to commit SHA for stronger reproducibility"
        }
    } else {
        "non-git source; reproducibility depends on URL immutability + checksum"
    };

    let checksum_hint = if let Some(checksum) = &source.checksum {
        if macc_adapter_shared::url_parsing::validate_checksum(checksum) {
            "checksum format looks valid"
        } else {
            "checksum format looks invalid (expected sha256:<64-hex>)"
        }
    } else if source.kind == SourceKind::Http {
        "checksum missing for HTTP source (recommended to add)"
    } else {
        "checksum not set"
    };

    println!("Trust hints (informational, not a security guarantee):");
    println!("  - {}", pinned_ref_hint);
    println!("  - {}", checksum_hint);
}

fn validate_import_source_now(
    paths: &macc_core::ProjectPaths,
    kind: &str,
    id: &str,
    source: &Source,
    selector: &Selector,
) {
    use macc_core::resolve::{FetchUnit, Selection, SelectionKind};
    let selection_kind = if kind == "skill" {
        SelectionKind::Skill
    } else {
        SelectionKind::Mcp
    };
    let mut source_for_fetch = source.clone();
    if !selector.subpath.is_empty() && selector.subpath != "." {
        source_for_fetch.subpaths = vec![selector.subpath.clone()];
    }

    let unit = FetchUnit {
        source: source_for_fetch,
        selections: vec![Selection {
            id: id.to_string(),
            subpath: selector.subpath.clone(),
            kind: selection_kind,
        }],
    };

    match macc_adapter_shared::fetch::materialize_fetch_unit(paths, unit) {
        Ok(materialized) => {
            let effective = if selector.subpath.is_empty() || selector.subpath == "." {
                materialized.source_root_path
            } else {
                materialized.source_root_path.join(&selector.subpath)
            };
            println!("Validation: OK");
            println!("  - materialized source root: {}", effective.display());
            if effective.join("macc.package.json").exists() {
                println!("  - manifest: macc.package.json found");
            } else {
                println!("  - warning: macc.package.json not found");
            }
        }
        Err(err) => {
            println!("Validation: WARNING");
            println!("  - could not fully validate source now: {}", err);
            println!("  - import will continue, but installation may fail later if subpath/manifest is invalid.");
        }
    }
}

fn is_commit_sha(reference: &str) -> bool {
    if reference.len() != 40 {
        return false;
    }
    reference.chars().all(|c| c.is_ascii_hexdigit())
}

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
        let registry = root.join("task_registry.json");
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

        let script = root.join("fake-coordinator.sh");
        write_executable_script(
            &script,
            r#"#!/usr/bin/env bash
set -euo pipefail
action="${1:-dispatch}"
case "$action" in
  dispatch)
    cat >"$TASK_REGISTRY_FILE" <<'JSON'
{
  "schema_version": 1,
  "tasks": [
    {
      "id": "TASK-1",
      "state": "merged",
      "dependencies": [],
      "exclusive_resources": [],
      "worktree": null
    }
  ],
  "resource_locks": {},
  "state_mapping": {}
}
JSON
    ;;
  sync|reconcile|cleanup) ;;
  *) ;;
esac
"#,
        );

        let canonical = macc_core::config::CanonicalConfig::default();
        let coordinator_cfg = macc_core::config::CoordinatorConfig {
            task_registry_file: Some(registry.to_string_lossy().to_string()),
            timeout_seconds: Some(10),
            ..Default::default()
        };
        let env_cfg = CoordinatorEnvConfig {
            prd: None,
            registry: None,
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
        };

        run_coordinator_full_cycle(&root, &script, &canonical, Some(&coordinator_cfg), &env_cfg)?;

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
        let registry = root.join("task_registry.json");
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

        let script = root.join("fake-stall-coordinator.sh");
        write_executable_script(
            &script,
            r#"#!/usr/bin/env bash
set -euo pipefail
exit 0
"#,
        );

        let canonical = macc_core::config::CanonicalConfig::default();
        let coordinator_cfg = macc_core::config::CoordinatorConfig {
            task_registry_file: Some(registry.to_string_lossy().to_string()),
            timeout_seconds: Some(10),
            ..Default::default()
        };
        let env_cfg = CoordinatorEnvConfig {
            prd: None,
            registry: None,
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
        };

        let err = run_coordinator_full_cycle(
            &root,
            &script,
            &canonical,
            Some(&coordinator_cfg),
            &env_cfg,
        )
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
        std::fs::write(
            root.join("task_registry.json"),
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
                    graceful: true,
                    remove_worktrees: true,
                    remove_branches: true,
                    prd: None,
                    registry: None,
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

        // 6. Verify sparse checkout in cache
        let cache_dir = project_path.join(".macc/cache");
        let mut found_cache = false;
        if let Ok(entries) = std::fs::read_dir(cache_dir) {
            for entry in entries.flatten() {
                let repo_dir = entry.path().join("repo");
                if repo_dir.exists() {
                    found_cache = true;
                    // In sparse checkout (cone mode), skills/a should exist, but skills/b should NOT be materialized
                    assert!(repo_dir.join("skills/a").exists());
                    assert!(
                        !repo_dir.join("skills/b").exists(),
                        "skills/b should NOT be materialized in sparse checkout"
                    );
                }
            }
        }
        assert!(found_cache, "Cache entry for git repo should exist");

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
}
