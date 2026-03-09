use macc_core::config::CanonicalConfig;
use macc_core::tool::ToolSpec;
use macc_core::{load_canonical_config, MaccError, Result};

#[derive(Debug, Clone, Copy)]
pub struct ToolUpdateCommandOptions<'a> {
    pub tool_id: Option<&'a str>,
    pub all: bool,
    pub only: Option<&'a str>,
    pub check: bool,
    pub assume_yes: bool,
    pub force: bool,
    pub rollback_on_fail: bool,
}

#[derive(Debug, Clone)]
struct ToolUpdateStatus {
    id: String,
    installed: bool,
    current_version: Option<String>,
    latest_version: Option<String>,
    source: String,
}

impl ToolUpdateStatus {
    fn is_outdated(&self) -> bool {
        match (&self.current_version, &self.latest_version) {
            (Some(current), Some(latest)) => current != latest,
            _ => false,
        }
    }
}

pub fn install_tool(paths: &macc_core::ProjectPaths, tool_id: &str, assume_yes: bool) -> Result<()> {
    let specs = load_toolspecs_with_diagnostics(paths)?;
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

pub fn update_tools(paths: &macc_core::ProjectPaths, opts: ToolUpdateCommandOptions<'_>) -> Result<()> {
    let specs = load_toolspecs_with_diagnostics(paths)?;
    let canonical = load_canonical_config(&paths.config_path)?;
    let selected = select_tools_for_update(&specs, &canonical, opts.tool_id, opts.all, opts.only)?;
    if selected.is_empty() {
        return Err(MaccError::Validation(
            "No matching tools found for update.".into(),
        ));
    }

    let mut updated = 0usize;
    let mut already_latest = 0usize;
    let mut skipped = 0usize;
    let mut failed: Vec<String> = Vec::new();

    for spec in selected {
        let status = get_tool_update_status(&spec);
        let latest_display = status
            .latest_version
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let current_display = status
            .current_version
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        if !status.installed && !opts.force {
            println!(
                "Skipping '{}': not currently installed (run `macc tool install {}`).",
                spec.id, spec.id
            );
            skipped += 1;
            continue;
        }
        if !opts.force && status.latest_version.is_some() && !status.is_outdated() {
            println!(
                "Skipping '{}': already latest (current={}, latest={}).",
                spec.id, current_display, latest_display
            );
            already_latest += 1;
            continue;
        }
        if opts.check {
            println!(
                "[check] tool={} installed={} current={} latest={} source={}",
                spec.id, status.installed, current_display, latest_display, status.source
            );
            continue;
        }

        match update_single_tool(paths, &spec, opts.assume_yes, opts.rollback_on_fail) {
            Ok(()) => {
                println!("Updated '{}'.", spec.id);
                updated += 1;
            }
            Err(err) => {
                eprintln!("Failed to update '{}': {}", spec.id, err);
                failed.push(spec.id.clone());
            }
        }
    }

    if opts.check {
        return Ok(());
    }

    println!(
        "Update summary: updated={} already_latest={} skipped={} failed={}",
        updated,
        already_latest,
        skipped,
        failed.len()
    );
    if failed.is_empty() {
        Ok(())
    } else {
        Err(MaccError::Validation(format!(
            "Tool update failed for: {}",
            failed.join(", ")
        )))
    }
}

pub fn show_outdated_tools(paths: &macc_core::ProjectPaths, only: Option<&str>) -> Result<()> {
    let specs = load_toolspecs_with_diagnostics(paths)?;
    let canonical = load_canonical_config(&paths.config_path)?;
    let selected = select_tools_for_update(&specs, &canonical, None, true, only)?;

    println!(
        "{:<14} {:<10} {:<16} {:<16} {:<14}",
        "TOOL", "INSTALLED", "CURRENT", "LATEST", "STATE"
    );
    println!(
        "{:-<14} {:-<10} {:-<16} {:-<16} {:-<14}",
        "", "", "", "", ""
    );
    let mut outdated_count = 0usize;
    for spec in selected {
        let status = get_tool_update_status(&spec);
        let state = if !status.installed {
            "not_installed"
        } else if status.is_outdated() {
            outdated_count += 1;
            "outdated"
        } else if status.latest_version.is_some() {
            "up_to_date"
        } else {
            "unknown"
        };
        println!(
            "{:<14} {:<10} {:<16} {:<16} {:<14}",
            status.id,
            if status.installed { "yes" } else { "no" },
            status.current_version.unwrap_or_else(|| "-".to_string()),
            status.latest_version.unwrap_or_else(|| "-".to_string()),
            state
        );
    }
    println!();
    println!("Outdated tools: {}", outdated_count);
    Ok(())
}

pub fn print_checks(checks: &[macc_core::doctor::ToolCheck]) {
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

fn run_tool_health_checks(spec: &ToolSpec) -> Vec<macc_core::doctor::ToolCheck> {
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

fn load_toolspecs_with_diagnostics(paths: &macc_core::ProjectPaths) -> Result<Vec<ToolSpec>> {
    let search_paths = macc_core::tool::ToolSpecLoader::default_search_paths(&paths.root);
    let loader = macc_core::tool::ToolSpecLoader::new(search_paths);
    let (specs, diagnostics) = loader.load_all_with_embedded();
    crate::services::project::report_diagnostics(&diagnostics);
    Ok(specs)
}

fn select_tools_for_update(
    specs: &[ToolSpec],
    canonical: &CanonicalConfig,
    tool_id: Option<&str>,
    all: bool,
    only: Option<&str>,
) -> Result<Vec<ToolSpec>> {
    if !all && tool_id.is_none() {
        return Err(MaccError::Validation(
            "Use `macc tool update <tool_id>` or `macc tool update --all`.".into(),
        ));
    }
    if all && tool_id.is_some() {
        return Err(MaccError::Validation(
            "Use either <tool_id> or --all, not both.".into(),
        ));
    }

    let mut selected: Vec<ToolSpec> = if let Some(id) = tool_id {
        let spec = specs
            .iter()
            .find(|s| s.id == id)
            .ok_or_else(|| MaccError::Validation(format!("Unknown tool: {}", id)))?;
        vec![spec.clone()]
    } else {
        specs.to_vec()
    };
    selected.retain(|spec| spec.install.is_some());
    if let Some(filter) = only {
        match filter {
            "enabled" => selected.retain(|spec| canonical.tools.enabled.iter().any(|id| id == &spec.id)),
            "installed" => selected.retain(|spec| get_tool_update_status(spec).installed),
            _ => {}
        }
    }
    Ok(selected)
}

fn get_tool_update_status(spec: &ToolSpec) -> ToolUpdateStatus {
    let checks = run_tool_health_checks(spec);
    let installed = checks_all_installed(&checks);
    let (current_version, latest_version, source) = if let Some(vs) = &spec.version_check {
        let current = run_version_command(&vs.current);
        let latest = vs.latest.as_ref().and_then(run_version_command);
        (
            current,
            latest,
            format!(
                "{}{}",
                vs.current.command,
                if vs.latest.is_some() { " (+latest)" } else { "" }
            ),
        )
    } else {
        (None, None, "unknown".to_string())
    };
    ToolUpdateStatus {
        id: spec.id.clone(),
        installed,
        current_version,
        latest_version,
        source,
    }
}

pub(crate) fn run_version_command(cmd_spec: &macc_core::tool::ToolInstallCommand) -> Option<String> {
    let output = std::process::Command::new(&cmd_spec.command)
        .args(&cmd_spec.args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stdout.is_empty() && stdout.chars().all(|c| !c.is_whitespace()) {
        return Some(stdout.trim_start_matches('v').to_string());
    }
    let text = format!(
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    extract_version_token(&text)
}

pub(crate) fn extract_version_token(text: &str) -> Option<String> {
    for raw in text.split_whitespace() {
        let token =
            raw.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '.' && c != '-');
        let normalized = token.trim_start_matches('v');
        if normalized
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
            && normalized.chars().any(|c| c.is_ascii_digit())
            && normalized.contains('.')
        {
            return Some(normalized.to_string());
        }
    }
    None
}

fn update_single_tool(
    paths: &macc_core::ProjectPaths,
    spec: &ToolSpec,
    assume_yes: bool,
    rollback_on_fail: bool,
) -> Result<()> {
    let update_spec = spec
        .update
        .clone()
        .or_else(|| spec.install.clone())
        .ok_or_else(|| {
            MaccError::Validation(format!(
                "Tool '{}' does not define update/install steps in ToolSpec.",
                spec.id
            ))
        })?;
    if update_spec.commands.is_empty() {
        return Err(MaccError::Validation(format!(
            "Tool '{}' update commands are empty.",
            spec.id
        )));
    }
    if !assume_yes {
        println!(
            "{}",
            update_spec.confirm_message.unwrap_or_else(|| {
                format!(
                    "This will run update commands for '{}'. Continue?",
                    spec.display_name
                )
            })
        );
        if !confirm_yes_no("Proceed [y/N]? ")? {
            return Err(MaccError::Validation("Update cancelled.".into()));
        }
    }

    let update_result: Result<()> = (|| {
        for command in &update_spec.commands {
            run_install_command(&paths.root, command, false)?;
        }
        if let Some(post_install) = &update_spec.post_install {
            run_install_command(&paths.root, post_install, true)?;
        }
        let final_checks = run_tool_health_checks(spec);
        print_checks(&final_checks);
        if !checks_all_installed(&final_checks) {
            return Err(MaccError::Validation(format!(
                "Post-update validation failed for '{}'.",
                spec.id
            )));
        }
        Ok(())
    })();

    if update_result.is_ok() || !rollback_on_fail {
        return update_result;
    }
    eprintln!(
        "Rollback requested for '{}' but no generic rollback contract is defined. Configure tool-specific rollback in ToolSpec before enabling this in production.",
        spec.id
    );
    update_result
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
    io::stdin().read_line(&mut input).map_err(|e| MaccError::Io {
        path: "stdin".into(),
        action: "read confirmation".into(),
        source: e,
    })?;
    let value = input.trim().to_ascii_lowercase();
    Ok(value == "y" || value == "yes")
}
