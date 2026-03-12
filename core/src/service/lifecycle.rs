use crate::config::CanonicalConfig;
use crate::engine::Engine;
use crate::resolve::{
    resolve, resolve_fetch_units, CliOverrides, FetchUnit, MaterializedFetchUnit,
};
use crate::service::interaction::InteractionHandler;
use crate::{load_canonical_config, MaccError, ProjectPaths, Result, ToolDescriptor};
use std::path::Path;

pub trait LifecycleFetchMaterializer {
    fn materialize_fetch_units(
        &self,
        paths: &ProjectPaths,
        units: Vec<FetchUnit>,
    ) -> Result<Vec<MaterializedFetchUnit>>;
}

pub trait LifecycleUi: InteractionHandler {
    fn print_checks(&self, checks: &[crate::doctor::ToolCheck]);
    fn render_plan_preview(
        &self,
        paths: &ProjectPaths,
        plan: &crate::plan::ActionPlan,
        ops: &[crate::plan::PlannedOp],
        json: bool,
        explain: bool,
    ) -> Result<()>;
    fn print_pre_apply_summary(
        &self,
        paths: &ProjectPaths,
        plan: &crate::plan::ActionPlan,
        ops: &[crate::plan::PlannedOp],
    );
    fn print_pre_apply_explanations(&self, ops: &[crate::plan::PlannedOp]);
    fn confirm_user_scope_apply(
        &self,
        paths: &ProjectPaths,
        ops: &[crate::plan::PlannedOp],
    ) -> Result<()>;
    fn mark_apply_completed(&self, paths: &ProjectPaths) -> Result<()>;
    fn run_tui(&self) -> Result<()>;
    fn set_current_dir(&self, path: &Path) -> Result<()>;
    fn prompt_line(&self, prompt: &str) -> Result<String>;
    fn is_command_available(&self, command: &str) -> bool;
}

pub fn init(
    cwd: &Path,
    engine: &dyn Engine,
    force: bool,
    wizard: bool,
    ui: &dyn LifecycleUi,
) -> Result<()> {
    let paths = crate::find_project_root(cwd).unwrap_or_else(|_| ProjectPaths::from_root(cwd));
    crate::init(&paths, force)?;
    if wizard {
        run_init_wizard(&paths, engine, ui)?;
    }
    let checks = engine.doctor(&paths);
    ui.print_checks(&checks);
    Ok(())
}

pub fn plan(
    cwd: &Path,
    engine: &dyn Engine,
    tools: Option<&str>,
    json: bool,
    explain: bool,
    ui: &dyn LifecycleUi,
    fetch_materializer: &dyn LifecycleFetchMaterializer,
) -> Result<()> {
    let project_ctx = load_project_context(cwd, engine)?;
    let paths = project_ctx.paths.clone();
    let canonical = project_ctx.canonical.clone();
    let descriptors = project_ctx.descriptors.clone();
    crate::service::project::report_diagnostics(&project_ctx.diagnostics, ui);
    let allowed_tools = project_ctx.allowed_tools;

    let migration = crate::migrate::migrate_with_known_tools(canonical.clone(), &allowed_tools);
    if !migration.warnings.is_empty() {
        ui.warn(
            "Warning: Legacy configuration detected. Run 'macc migrate' to update your config.",
        );
    }

    let overrides = if let Some(tools_csv) = tools {
        CliOverrides::from_tools_csv(tools_csv, &allowed_tools)?
    } else {
        CliOverrides::default()
    };
    let resolved = resolve(&canonical, &overrides);
    let enabled_titles = enabled_titles(&descriptors, &resolved.tools.enabled);
    if !json {
        ui.info(&format!(
            "Core: Planning in {} with tools: {:?}",
            paths.root.display(),
            enabled_titles
        ));
    }

    let fetch_units = resolve_fetch_units(&paths, &resolved)?;
    let materialized_units = fetch_materializer.materialize_fetch_units(&paths, fetch_units)?;
    let plan = engine.plan(&paths, &canonical, &materialized_units, &overrides)?;
    let ops = engine.plan_operations(&paths, &plan);
    ui.render_plan_preview(&paths, &plan, &ops, json, explain)
}

pub fn apply(
    cwd: &Path,
    engine: &dyn Engine,
    tools: Option<&str>,
    dry_run: bool,
    allow_user_scope: bool,
    json: bool,
    explain: bool,
    ui: &dyn LifecycleUi,
    fetch_materializer: &dyn LifecycleFetchMaterializer,
) -> Result<()> {
    let project_ctx = load_project_context(cwd, engine)?;
    let paths = project_ctx.paths.clone();
    let canonical = project_ctx.canonical.clone();
    let descriptors = project_ctx.descriptors.clone();
    crate::service::project::report_diagnostics(&project_ctx.diagnostics, ui);
    let allowed_tools = project_ctx.allowed_tools;

    let migration = crate::migrate::migrate_with_known_tools(canonical.clone(), &allowed_tools);
    if !migration.warnings.is_empty() {
        ui.warn(
            "Warning: Legacy configuration detected. Run 'macc migrate' to update your config.",
        );
    }

    let overrides = if let Some(tools_csv) = tools {
        CliOverrides::from_tools_csv(tools_csv, &allowed_tools)?
    } else {
        CliOverrides::default()
    };
    let resolved = resolve(&canonical, &overrides);
    let enabled_titles = enabled_titles(&descriptors, &resolved.tools.enabled);
    let fetch_units = resolve_fetch_units(&paths, &resolved)?;
    let materialized_units = fetch_materializer.materialize_fetch_units(&paths, fetch_units)?;

    if dry_run {
        if !json {
            ui.info(&format!(
                "Core: Dry-run apply (planning) in {} with tools: {:?}",
                paths.root.display(),
                enabled_titles
            ));
        }
        let plan = engine.plan(&paths, &canonical, &materialized_units, &overrides)?;
        let ops = engine.plan_operations(&paths, &plan);
        return ui.render_plan_preview(&paths, &plan, &ops, json, explain);
    }

    ui.info(&format!(
        "Core: Applying in {} with tools: {:?}",
        paths.root.display(),
        enabled_titles
    ));
    let mut plan = engine.plan(&paths, &canonical, &materialized_units, &overrides)?;
    let ops = engine.plan_operations(&paths, &plan);
    if !json {
        ui.print_pre_apply_summary(&paths, &plan, &ops);
        if explain {
            ui.print_pre_apply_explanations(&ops);
        }
    }
    if allow_user_scope {
        ui.confirm_user_scope_apply(&paths, &ops)?;
    }
    let report = engine.apply(&paths, &mut plan, allow_user_scope)?;
    ui.info(&report.render_cli());
    ui.mark_apply_completed(&paths)?;
    Ok(())
}

pub fn quickstart(
    cwd: &Path,
    engine: &dyn Engine,
    assume_yes: bool,
    apply: bool,
    no_tui: bool,
    ui: &dyn LifecycleUi,
    fetch_materializer: &dyn LifecycleFetchMaterializer,
) -> Result<()> {
    let paths = crate::find_project_root(cwd).unwrap_or_else(|_| ProjectPaths::from_root(cwd));

    let mut missing = Vec::new();
    for cmd in ["git", "curl", "jq"] {
        if !ui.is_command_available(cmd) {
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
        ui.info(&format!(
            "No .git directory found in {}.",
            paths.root.display()
        ));
        if !assume_yes && !ui.confirm("Continue anyway [y/N]? ")? {
            return Err(MaccError::Validation("Quickstart cancelled.".into()));
        }
    }

    if !paths.macc_dir.exists() && !assume_yes {
        ui.info(".macc/ was not found in this project.");
        if !ui.confirm("Run 'macc init' now [y/N]? ")? {
            return Err(MaccError::Validation(
                "Quickstart requires initialization. Cancelled.".into(),
            ));
        }
    }

    crate::init(&paths, false)?;
    ui.info(&format!(
        "Quickstart: initialized project at {}",
        paths.root.display()
    ));

    if apply {
        run_plan_then_optional_apply(engine, &paths, assume_yes, ui, fetch_materializer)?;
        return Ok(());
    }

    if no_tui {
        ui.info("Quickstart complete.");
        ui.info("Next: run 'macc plan' then 'macc apply'.");
        return Ok(());
    }

    ui.info("Quickstart complete. Opening TUI...");
    ui.set_current_dir(&paths.root)?;
    ui.run_tui()
}

fn run_plan_then_optional_apply(
    engine: &dyn Engine,
    paths: &ProjectPaths,
    assume_yes: bool,
    ui: &dyn LifecycleUi,
    fetch_materializer: &dyn LifecycleFetchMaterializer,
) -> Result<()> {
    let canonical = load_canonical_config(&paths.config_path)?;
    let (_descriptors, diagnostics) = engine.list_tools(paths);
    crate::service::project::report_diagnostics(&diagnostics, ui);
    let overrides = CliOverrides::default();
    let resolved = resolve(&canonical, &overrides);
    let fetch_units = resolve_fetch_units(paths, &resolved)?;
    let materialized_units = fetch_materializer.materialize_fetch_units(paths, fetch_units)?;
    let plan = engine.plan(paths, &canonical, &materialized_units, &overrides)?;
    crate::preview_plan(&plan, paths)?;
    ui.info(&format!(
        "Core: Total actions planned: {}",
        plan.actions.len()
    ));

    if !assume_yes && !ui.confirm("Apply this plan now [y/N]? ")? {
        ui.info("Plan generated only. Run 'macc apply' when ready.");
        return Ok(());
    }

    let overrides = CliOverrides::default();
    let resolved = resolve(&canonical, &overrides);
    let fetch_units = resolve_fetch_units(paths, &resolved)?;
    let materialized_units = fetch_materializer.materialize_fetch_units(paths, fetch_units)?;
    let mut apply_plan = engine.plan(paths, &canonical, &materialized_units, &overrides)?;
    let report = engine.apply(paths, &mut apply_plan, false)?;
    ui.info(&report.render_cli());
    ui.mark_apply_completed(paths)?;
    Ok(())
}

fn run_init_wizard(paths: &ProjectPaths, engine: &dyn Engine, ui: &dyn LifecycleUi) -> Result<()> {
    ui.info("Init wizard (3 questions)");
    let mut config = load_canonical_config(&paths.config_path)?;
    let (descriptors, diagnostics) = engine.list_tools(paths);
    crate::service::project::report_diagnostics(&diagnostics, ui);
    let tool_ids: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();
    if !tool_ids.is_empty() {
        ui.info(&format!("Available tools: {}", tool_ids.join(", ")));
    }

    let tools_answer = ui.prompt_line("Q1/3 - Enabled tools (CSV, empty keeps current): ")?;
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

    ui.info("Standards presets: minimal | strict | none");
    let preset = ui.prompt_line("Q2/3 - Standards preset [minimal]: ")?;
    apply_standards_preset(
        &mut config,
        if preset.is_empty() {
            "minimal"
        } else {
            &preset
        },
    )?;

    let mcp_answer =
        ui.prompt_line("Q3/3 - Enable default MCP templates in selections? [y/N]: ")?;
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
    crate::atomic_write(paths, &paths.config_path, yaml.as_bytes())?;
    ui.info(&format!("Wizard saved: {}", paths.config_path.display()));
    Ok(())
}

fn apply_standards_preset(config: &mut CanonicalConfig, preset: &str) -> Result<()> {
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

struct LoadedProjectContext {
    paths: ProjectPaths,
    canonical: CanonicalConfig,
    descriptors: Vec<ToolDescriptor>,
    diagnostics: Vec<crate::tool::ToolDiagnostic>,
    allowed_tools: Vec<String>,
}

fn load_project_context(cwd: &Path, engine: &dyn Engine) -> Result<LoadedProjectContext> {
    let paths = crate::find_project_root(cwd)?;
    let canonical = load_canonical_config(&paths.config_path)?;
    let (descriptors, diagnostics) = engine.list_tools(&paths);
    let allowed_tools: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();
    Ok(LoadedProjectContext {
        paths,
        canonical,
        descriptors,
        diagnostics,
        allowed_tools,
    })
}

fn enabled_titles(descriptors: &[ToolDescriptor], enabled_ids: &[String]) -> Vec<String> {
    enabled_ids
        .iter()
        .map(|id| {
            descriptors
                .iter()
                .find(|d| &d.id == id)
                .map(|d| d.title.clone())
                .unwrap_or_else(|| id.clone())
        })
        .collect()
}
