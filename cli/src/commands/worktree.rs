use crate::commands::AppContext;
use crate::commands::Command;
use crate::WorktreeCommands;
use macc_core::Result;

struct CliFetchMaterializer;

impl macc_core::service::worktree::WorktreeFetchMaterializer for CliFetchMaterializer {
    fn materialize_fetch_units(
        &self,
        paths: &macc_core::ProjectPaths,
        units: Vec<macc_core::resolve::FetchUnit>,
    ) -> Result<Vec<macc_core::resolve::MaterializedFetchUnit>> {
        macc_adapter_shared::fetch::materialize_fetch_units(paths, units)
    }
}

pub struct WorktreeCommand<'a> {
    app: AppContext,
    command: &'a WorktreeCommands,
}

impl<'a> WorktreeCommand<'a> {
    pub fn new(app: AppContext, command: &'a WorktreeCommands) -> Self {
        Self { app, command }
    }
}

impl<'a> Command for WorktreeCommand<'a> {
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
                let paths = self.app.project_paths()?;

                let spec = macc_core::WorktreeCreateSpec {
                    slug: slug.clone(),
                    tool: tool.clone(),
                    count: *count,
                    base: base.clone(),
                    dir: std::path::PathBuf::from(".macc/worktree"),
                    scope: scope.clone(),
                    feature: feature.clone(),
                };
                let created = self.app.engine.setup_worktree_workflow(
                    &CliFetchMaterializer,
                    &paths.root,
                    &spec,
                    macc_core::service::worktree::WorktreeSetupOptions {
                        skip_apply: *skip_apply,
                        allow_user_scope: *allow_user_scope,
                    },
                )?;

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
                let entries = macc_core::list_worktrees(&self.app.cwd)?;
                let current = macc_core::current_worktree(&self.app.cwd, &entries);
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
                let entries = macc_core::list_worktrees(&self.app.cwd)?;
                if entries.is_empty() {
                    println!("No git worktrees found.");
                    return Ok(());
                }
                let project_paths = macc_core::find_project_root(&self.app.cwd)
                    .map(|root| macc_core::ProjectPaths::from_root(&root.root))
                    .ok();
                let session_map = macc_core::service::worktree::load_worktree_session_labels(
                    project_paths.as_ref(),
                )?;

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
                        .map(|s| macc_core::service::worktree::truncate_cell(s, 8))
                        .unwrap_or_else(|| "-".into());
                    let git_state =
                        if macc_core::service::worktree::git_worktree_is_dirty(&entry.path)
                            .unwrap_or(false)
                        {
                            "dirty"
                        } else {
                            "clean"
                        };
                    let session = session_map
                        .get(&macc_core::service::worktree::canonicalize_path_fallback(
                            &entry.path,
                        ))
                        .cloned()
                        .unwrap_or_else(|| "-".into());
                    println!(
                        "{:<54} {:<12} {:<24} {:<8} {:<10} {:<16} {:<8} {:<8}",
                        macc_core::service::worktree::truncate_cell(
                            &entry.path.display().to_string(),
                            54
                        ),
                        macc_core::service::worktree::truncate_cell(&tool, 12),
                        macc_core::service::worktree::truncate_cell(&branch, 24),
                        scope,
                        git_state,
                        macc_core::service::worktree::truncate_cell(&session, 16),
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
                let paths = self.app.project_paths()?;
                let worktree_path =
                    macc_core::service::worktree::resolve_worktree_path(&paths.root, id)?;
                if !worktree_path.exists() {
                    return Err(macc_core::MaccError::Validation(format!(
                        "Worktree path does not exist: {}",
                        worktree_path.display()
                    )));
                }

                if *terminal {
                    self.app.engine.worktree_open_in_terminal(&worktree_path)?;
                }
                if let Some(cmd) = editor {
                    self.app
                        .engine
                        .worktree_open_in_editor(&worktree_path, cmd)?;
                } else {
                    self.app
                        .engine
                        .worktree_open_in_editor(&worktree_path, "code")?;
                }

                println!("Opened worktree: {}", worktree_path.display());
                Ok(())
            }
            WorktreeCommands::Apply {
                id,
                all,
                allow_user_scope,
            } => {
                let paths = self.app.project_paths()?;
                if *all {
                    let applied = self.app.engine.worktree_apply_all(
                        &CliFetchMaterializer,
                        &paths.root,
                        *allow_user_scope,
                    )?;
                    println!("Applied {} worktree(s).", applied);
                    return Ok(());
                }

                let id = id.as_ref().ok_or_else(|| {
                    macc_core::MaccError::Validation("worktree apply requires <ID> or --all".into())
                })?;
                let worktree_path =
                    macc_core::service::worktree::resolve_worktree_path(&paths.root, id)?;
                self.app.engine.worktree_apply(
                    &CliFetchMaterializer,
                    &paths.root,
                    &worktree_path,
                    *allow_user_scope,
                )?;
                println!("Applied worktree: {}", worktree_path.display());
                Ok(())
            }
            WorktreeCommands::Doctor { id } => {
                let paths = self.app.project_paths()?;
                let worktree_path =
                    macc_core::service::worktree::resolve_worktree_path(&paths.root, id)?;
                if !worktree_path.exists() {
                    return Err(macc_core::MaccError::Validation(format!(
                        "Worktree path does not exist: {}",
                        worktree_path.display()
                    )));
                }
                let worktree_paths = macc_core::ProjectPaths::from_root(&worktree_path);
                let checks = self.app.engine.doctor(&worktree_paths);
                crate::print_checks(&checks);
                Ok(())
            }
            WorktreeCommands::Run { id } => {
                let paths = self.app.project_paths()?;
                self.app.engine.worktree_run_task(&paths, id)
            }
            WorktreeCommands::Exec { id, cmd } => {
                let paths = self.app.project_paths()?;
                self.app.engine.worktree_exec_task(&paths, id, cmd)
            }
            WorktreeCommands::Remove {
                id,
                force,
                all,
                remove_branch,
            } => {
                let paths = self.app.project_paths()?;
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
                            macc_core::service::worktree::delete_branch(
                                &paths.root,
                                branch.as_deref(),
                                *force,
                            )?;
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
                    macc_core::service::worktree::delete_branch(
                        &paths.root,
                        branch.as_deref(),
                        *force,
                    )?;
                }
                println!("Removed worktree: {}", worktree_path.display());
                Ok(())
            }
            WorktreeCommands::Prune => {
                let paths = self.app.project_paths()?;
                macc_core::prune_worktrees(&paths.root)?;
                println!("Pruned git worktrees.");
                Ok(())
            }
        }
    }
}
