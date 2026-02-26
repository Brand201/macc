use macc_core::resolve::{resolve, resolve_fetch_units, CliOverrides};
use macc_core::{load_canonical_config, MaccError, Result};
use std::io::{self, Write};
use crate::commands::AppContext;
use crate::services::engine_provider::SharedEngine;

pub fn init(
    app: &AppContext,
    force: bool,
    wizard: bool,
) -> Result<()> {
    let paths = macc_core::find_project_root(&app.cwd)
        .unwrap_or_else(|_| macc_core::ProjectPaths::from_root(&app.cwd));
    macc_core::init(&paths, force)?;
    if wizard {
        run_init_wizard(&paths, &app.engine)?;
    }
    let checks = app.engine.doctor(&paths);
    crate::services::tooling::print_checks(&checks);
    Ok(())
}

pub fn plan(
    app: &AppContext,
    tools: Option<&str>,
    json: bool,
    explain: bool,
) -> Result<()> {
    let project_ctx = crate::commands::ProjectContext::load(app)?;
    let paths = project_ctx.paths.clone();
    let canonical = project_ctx.canonical.clone();
    let descriptors = project_ctx.descriptors.clone();
    let (_, diagnostics) = app.engine.list_tools(&paths);
    crate::services::project::report_diagnostics(&diagnostics);
    let allowed_tools = project_ctx.allowed_tools.clone();

    let migration = macc_core::migrate::migrate_with_known_tools(canonical.clone(), &allowed_tools);
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

    if !json {
        println!(
            "Core: Planning in {} with tools: {:?}",
            paths.root.display(),
            enabled_titles
        );
    }

    let fetch_units = resolve_fetch_units(&paths, &resolved)?;
    let materialized_units = macc_adapter_shared::fetch::materialize_fetch_units(&paths, fetch_units)?;

    let plan = app.engine.plan(&paths, &canonical, &materialized_units, &overrides)?;
    let ops = app.engine.plan_operations(&paths, &plan);
    crate::render_plan_preview(&paths, &plan, &ops, json, explain)?;
    Ok(())
}

pub fn apply(
    app: &AppContext,
    tools: Option<&str>,
    dry_run: bool,
    allow_user_scope: bool,
    json: bool,
    explain: bool,
) -> Result<()> {
    let project_ctx = crate::commands::ProjectContext::load(app)?;
    let paths = project_ctx.paths.clone();
    let canonical = project_ctx.canonical.clone();
    let descriptors = project_ctx.descriptors.clone();
    let (_, diagnostics) = app.engine.list_tools(&paths);
    crate::services::project::report_diagnostics(&diagnostics);
    let allowed_tools = project_ctx.allowed_tools.clone();

    let migration = macc_core::migrate::migrate_with_known_tools(canonical.clone(), &allowed_tools);
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
    let materialized_units = macc_adapter_shared::fetch::materialize_fetch_units(&paths, fetch_units)?;

    if dry_run {
        if !json {
            println!(
                "Core: Dry-run apply (planning) in {} with tools: {:?}",
                paths.root.display(),
                enabled_titles
            );
        }
        let plan = app.engine.plan(&paths, &canonical, &materialized_units, &overrides)?;
        let ops = app.engine.plan_operations(&paths, &plan);
        crate::render_plan_preview(&paths, &plan, &ops, json, explain)?;
        return Ok(());
    }

    println!(
        "Core: Applying in {} with tools: {:?}",
        paths.root.display(),
        enabled_titles
    );
    let mut plan = app.engine.plan(&paths, &canonical, &materialized_units, &overrides)?;
    let ops = app.engine.plan_operations(&paths, &plan);
    if !json {
        crate::print_pre_apply_summary(&paths, &plan, &ops);
        if explain {
            crate::print_pre_apply_explanations(&ops);
        }
    }
    if allow_user_scope {
        crate::confirm_user_scope_apply(&paths, &ops)?;
    }

    let report = app.engine.apply(&paths, &mut plan, allow_user_scope)?;
    println!("{}", report.render_cli());
    crate::mark_apply_completed(&paths)?;
    Ok(())
}

pub fn quickstart(
    app: &AppContext,
    assume_yes: bool,
    apply: bool,
    no_tui: bool,
) -> Result<()> {
    let paths = macc_core::find_project_root(&app.cwd)
        .unwrap_or_else(|_| macc_core::ProjectPaths::from_root(&app.cwd));

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
        if !assume_yes && !crate::confirm_yes_no("Continue anyway [y/N]? ")? {
            return Err(MaccError::Validation("Quickstart cancelled.".into()));
        }
    }

    if !paths.macc_dir.exists() && !assume_yes {
        println!(".macc/ was not found in this project.");
        if !crate::confirm_yes_no("Run 'macc init' now [y/N]? ")? {
            return Err(MaccError::Validation(
                "Quickstart requires initialization. Cancelled.".into(),
            ));
        }
    }

    macc_core::init(&paths, false)?;
    println!(
        "Quickstart: initialized project at {}",
        paths.root.display()
    );

    if apply {
        run_plan_then_optional_apply(&app.engine, &paths, assume_yes)?;
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

fn run_plan_then_optional_apply(
    engine: &SharedEngine,
    paths: &macc_core::ProjectPaths,
    assume_yes: bool,
) -> Result<()> {
    let canonical = load_canonical_config(&paths.config_path)?;
    let (_descriptors, diagnostics) = engine.list_tools(paths);
    crate::services::project::report_diagnostics(&diagnostics);
    let overrides = CliOverrides::default();
    let resolved = resolve(&canonical, &overrides);
    let fetch_units = resolve_fetch_units(paths, &resolved)?;
    let materialized_units =
        macc_adapter_shared::fetch::materialize_fetch_units(paths, fetch_units)?;

    let plan = engine.plan(paths, &canonical, &materialized_units, &overrides)?;
    macc_core::preview_plan(&plan, paths)?;
    println!("Core: Total actions planned: {}", plan.actions.len());

    if !assume_yes && !crate::confirm_yes_no("Apply this plan now [y/N]? ")? {
        println!("Plan generated only. Run 'macc apply' when ready.");
        return Ok(());
    }

    let canonical = load_canonical_config(&paths.config_path)?;
    let overrides = CliOverrides::default();
    let resolved = resolve(&canonical, &overrides);
    let fetch_units = resolve_fetch_units(paths, &resolved)?;
    let materialized_units =
        macc_adapter_shared::fetch::materialize_fetch_units(paths, fetch_units)?;
    let mut apply_plan = engine.plan(paths, &canonical, &materialized_units, &overrides)?;
    let report = engine.apply(paths, &mut apply_plan, false)?;
    println!("{}", report.render_cli());
    crate::mark_apply_completed(paths)?;
    Ok(())
}

fn run_init_wizard(
    paths: &macc_core::ProjectPaths,
    engine: &SharedEngine,
) -> Result<()> {
    println!("Init wizard (3 questions)");
    let mut config = load_canonical_config(&paths.config_path)?;
    let (descriptors, diagnostics) = engine.list_tools(paths);
    crate::services::project::report_diagnostics(&diagnostics);
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
