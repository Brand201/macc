use crate::commands::Command;
use crate::WorktreeCommands;
use macc_core::engine::Engine;
use macc_core::Result;
use std::path::{Path, PathBuf};

pub struct WorktreeCommand<'a, E: Engine> {
    cwd: PathBuf,
    command: &'a WorktreeCommands,
    engine: &'a E,
}

impl<'a, E: Engine> WorktreeCommand<'a, E> {
    pub fn new(cwd: &Path, command: &'a WorktreeCommands, engine: &'a E) -> Self {
        Self { cwd: cwd.to_path_buf(), command, engine }
    }
}

impl<'a, E: Engine> Command for WorktreeCommand<'a, E> {
    fn run(&self) -> Result<()> {
        match self.command {
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
                let paths = macc_core::find_project_root(&self.cwd)?;
                let canonical = macc_core::load_canonical_config(&paths.config_path)?;

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

                let (descriptors, diagnostics) = self.engine.list_tools(&paths);
                crate::services::project::report_diagnostics(&diagnostics);
                let allowed_tools: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();
                let overrides = macc_core::resolve::CliOverrides::from_tools_csv(
                    tool.as_str(),
                    &allowed_tools,
                )?;

                let yaml = canonical.to_yaml().map_err(|e| {
                    macc_core::MaccError::Validation(format!(
                        "Failed to serialize config for worktree: {}",
                        e
                    ))
                })?;

                for entry in &created {
                    let worktree_paths = macc_core::ProjectPaths::from_root(&entry.path);
                    macc_core::init(&worktree_paths, false)?;
                    macc_core::atomic_write(
                        &worktree_paths,
                        &worktree_paths.config_path,
                        yaml.as_bytes(),
                    )?;
                    crate::services::worktree::write_tool_json(&paths.root, &entry.path, tool)?;

                    if !*skip_apply {
                        let resolved = macc_core::resolve::resolve(&canonical, &overrides);
                        let fetch_units =
                            macc_core::resolve::resolve_fetch_units(&worktree_paths, &resolved)?;
                        let materialized_units =
                            macc_adapter_shared::fetch::materialize_fetch_units(
                                &worktree_paths,
                                fetch_units,
                            )?;
                        let mut plan = self.engine.plan(
                            &worktree_paths,
                            &canonical,
                            &materialized_units,
                            &overrides,
                        )?;
                        let _ = self
                            .engine
                            .apply(&worktree_paths, &mut plan, *allow_user_scope)?;
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
                let entries = macc_core::list_worktrees(&self.cwd)?;
                let current = macc_core::current_worktree(&self.cwd, &entries);
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
                let entries = macc_core::list_worktrees(&self.cwd)?;
                if entries.is_empty() {
                    println!("No git worktrees found.");
                    return Ok(());
                }
                let project_paths = macc_core::find_project_root(&self.cwd)
                    .map(|root| macc_core::ProjectPaths::from_root(&root.root))
                    .ok();
                let session_map = crate::services::worktree::load_worktree_session_labels(project_paths.as_ref())?;

                println!(
                    "{:<54} {:<12} {:<24} {:<8} {:<10} {:<16} {:<8} {:<8}",
                    "WORKTREE", "TOOL", "BRANCH", "SCOPE", "STATE", "SESSION", "LOCKED", "PRUNE"
                );
                println!(
                    "{:-<54} {:-<12} {:-<24} {:-<8} {:-<10} {:-<16} {:-<8} {:-<8}",
                    "", "", "", "", "", "", "", ""
                );
                for entry in entries {
                    let metadata = macc_core::read_worktree_metadata(&entry.path).ok().flatten();
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
                        .map(|s| crate::services::worktree::truncate_cell(s, 8))
                        .unwrap_or_else(|| "-".into());
                    let git_state = if crate::services::worktree::git_worktree_is_dirty(&entry.path).unwrap_or(false) {
                        "dirty"
                    } else {
                        "clean"
                    };
                    let session = session_map
                        .get(&crate::services::worktree::canonicalize_path_fallback(&entry.path))
                        .cloned()
                        .unwrap_or_else(|| "-".into());
                    println!(
                        "{:<54} {:<12} {:<24} {:<8} {:<10} {:<16} {:<8} {:<8}",
                        crate::services::worktree::truncate_cell(&entry.path.display().to_string(), 54),
                        crate::services::worktree::truncate_cell(&tool, 12),
                        crate::services::worktree::truncate_cell(&branch, 24),
                        scope,
                        git_state,
                        crate::services::worktree::truncate_cell(&session, 16),
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
                let paths = macc_core::find_project_root(&self.cwd)?;
                let worktree_path = crate::services::worktree::resolve_worktree_path(&paths.root, id)?;
                if !worktree_path.exists() {
                    return Err(macc_core::MaccError::Validation(format!(
                        "Worktree path does not exist: {}",
                        worktree_path.display()
                    )));
                }

                if *terminal {
                    crate::services::worktree::open_in_terminal(&worktree_path)?;
                }
                if let Some(cmd) = editor {
                    crate::services::worktree::open_in_editor(&worktree_path, cmd)?;
                } else {
                    crate::services::worktree::open_in_editor(&worktree_path, "code")?;
                }

                println!("Opened worktree: {}", worktree_path.display());
                Ok(())
            }
            WorktreeCommands::Apply {
                id,
                all,
                allow_user_scope,
            } => {
                let paths = macc_core::find_project_root(&self.cwd)?;
                if *all {
                    let entries = macc_core::list_worktrees(&paths.root)?;
                    let root = paths.root.canonicalize().unwrap_or(paths.root.clone());
                    let mut applied = 0;
                    for entry in entries {
                        if entry.path == root {
                            continue;
                        }
                        crate::services::worktree::apply_worktree(self.engine, &paths.root, &entry.path, *allow_user_scope)?;
                        applied += 1;
                    }
                    println!("Applied {} worktree(s).", applied);
                    return Ok(());
                }

                let id = id.as_ref().ok_or_else(|| {
                    macc_core::MaccError::Validation(
                        "worktree apply requires <ID> or --all".into(),
                    )
                })?;
                let worktree_path = crate::services::worktree::resolve_worktree_path(&paths.root, id)?;
                crate::services::worktree::apply_worktree(self.engine, &paths.root, &worktree_path, *allow_user_scope)?;
                println!("Applied worktree: {}", worktree_path.display());
                Ok(())
            }
            WorktreeCommands::Doctor { id } => {
                let paths = macc_core::find_project_root(&self.cwd)?;
                let worktree_path = crate::services::worktree::resolve_worktree_path(&paths.root, id)?;
                if !worktree_path.exists() {
                    return Err(macc_core::MaccError::Validation(format!(
                        "Worktree path does not exist: {}",
                        worktree_path.display()
                    )));
                }
                let worktree_paths = macc_core::ProjectPaths::from_root(&worktree_path);
                let checks = self.engine.doctor(&worktree_paths);
                crate::services::tooling::print_checks(&checks);
                Ok(())
            }
            WorktreeCommands::Run { id } => {
                let paths = macc_core::find_project_root(&self.cwd)?;
                let worktree_path = crate::services::worktree::resolve_worktree_path(&paths.root, id)?;
                if !worktree_path.exists() {
                    return Err(macc_core::MaccError::Validation(format!(
                        "Worktree path does not exist: {}",
                        worktree_path.display()
                    )));
                }

                let metadata = macc_core::read_worktree_metadata(&worktree_path)?
                    .ok_or_else(|| macc_core::MaccError::Validation("Missing .macc/worktree.json".into()))?;
                crate::services::worktree::ensure_tool_json(&paths.root, &worktree_path, &metadata.tool)?;
                let (task_id, prd_path) =
                    crate::services::worktree::resolve_worktree_task_context(&paths.root, &worktree_path, &metadata.id)?;
                let performer_path = crate::services::worktree::ensure_performer(&worktree_path)?;
                let registry_path = crate::services::worktree::coordinator_task_registry_path(&paths.root);
                let events_file = paths
                    .root
                    .join(".macc")
                    .join("log")
                    .join("coordinator")
                    .join("events.jsonl");
                if let Some(parent) = events_file.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| macc_core::MaccError::Io {
                        path: parent.to_string_lossy().into(),
                        action: "create coordinator log dir for events".into(),
                        source: e,
                    })?;
                }

                let status = std::process::Command::new(&performer_path)
                    .current_dir(&worktree_path)
                    .env("COORD_EVENTS_FILE", events_file.to_string_lossy().to_string())
                    .env("COORDINATOR_RUN_ID", crate::services::project::ensure_coordinator_run_id())
                    .env(
                        "MACC_EVENT_SOURCE",
                        format!(
                            "worktree-run:{}:{}",
                            task_id,
                            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
                        ),
                    )
                    .env("MACC_EVENT_TASK_ID", &task_id)
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
                    .map_err(|e| macc_core::MaccError::Io {
                        path: performer_path.to_string_lossy().into(),
                        action: "run worktree performer".into(),
                        source: e,
                    })?;
                if !status.success() {
                    return Err(macc_core::MaccError::Validation(format!(
                        "Performer failed with status: {}. Inspect logs with `macc logs tail --component performer --worktree {}` and if the task is stuck run `macc coordinator unlock --task {}`.",
                        status, metadata.id, task_id
                    )));
                }
                Ok(())
            }
            WorktreeCommands::Exec { id, cmd } => {
                let paths = macc_core::find_project_root(&self.cwd)?;
                let worktree_path = crate::services::worktree::resolve_worktree_path(&paths.root, id)?;
                if !worktree_path.exists() {
                    return Err(macc_core::MaccError::Validation(format!(
                        "Worktree path does not exist: {}",
                        worktree_path.display()
                    )));
                }
                if cmd.is_empty() {
                    return Err(macc_core::MaccError::Validation(
                        "worktree exec requires a command after --".into(),
                    ));
                }

                let mut command = std::process::Command::new(&cmd[0]);
                if cmd.len() > 1 {
                    command.args(&cmd[1..]);
                }
                let status = command
                    .current_dir(&worktree_path)
                    .status()
                    .map_err(|e| macc_core::MaccError::Io {
                        path: worktree_path.to_string_lossy().into(),
                        action: "run worktree exec".into(),
                        source: e,
                    })?;
                if !status.success() {
                    return Err(macc_core::MaccError::Validation(format!(
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
                let paths = macc_core::find_project_root(&self.cwd)?;
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
                            crate::services::worktree::delete_branch(&paths.root, branch.as_deref(), *force)?;
                        }
                        println!("Removed worktree: {}", entry.path.display());
                        removed += 1;
                    }
                    println!("Removed {} worktree(s).", removed);
                    return Ok(());
                }

                let id = id.as_ref().ok_or_else(|| {
                    macc_core::MaccError::Validation(
                        "worktree remove requires <ID> or --all".into(),
                    )
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
                    crate::services::worktree::delete_branch(&paths.root, branch.as_deref(), *force)?;
                }
                println!("Removed worktree: {}", worktree_path.display());
                Ok(())
            }
            WorktreeCommands::Prune => {
                let paths = macc_core::find_project_root(&self.cwd)?;
                macc_core::prune_worktrees(&paths.root)?;
                println!("Pruned git worktrees.");
                Ok(())
            }
        }
    }
}
