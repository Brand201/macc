use crate::config::CanonicalConfig;
use crate::doctor::{run_checks, ToolCheck, ToolStatus};
use crate::service::interaction::InteractionHandler;
use crate::tool::ToolDiagnostic;
use crate::tool::{DoctorCheckKind, ToolInstallCommand, ToolSpec, ToolSpecLoader};
use crate::{load_canonical_config, MaccError, ProjectPaths, Result};

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
pub struct ToolUpdateStatus {
    pub id: String,
    pub installed: bool,
    pub current_version: Option<String>,
    pub latest_version: Option<String>,
    pub source: String,
}

impl ToolUpdateStatus {
    pub fn is_outdated(&self) -> bool {
        match (&self.current_version, &self.latest_version) {
            (Some(current), Some(latest)) => current != latest,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InstallToolOutcome {
    pub tool_id: String,
    pub check_count: usize,
}

#[derive(Debug, Clone, Default)]
pub struct ToolUpdateSummary {
    pub checked: bool,
    pub updated: usize,
    pub already_latest: usize,
    pub skipped: usize,
    pub failed_tools: Vec<String>,
    pub statuses: Vec<ToolUpdateStatus>,
}

#[derive(Debug, Clone, Default)]
pub struct OutdatedToolsReport {
    pub statuses: Vec<ToolUpdateStatus>,
    pub outdated_count: usize,
}

pub struct NoopReporter;

impl InteractionHandler for NoopReporter {}

pub trait UserReporter: InteractionHandler {
    fn report_diagnostics(&self, diagnostics: &[ToolDiagnostic]) {
        for diagnostic in diagnostics {
            if let Some(line) = diagnostic.line {
                self.warn(&format!(
                    "Warning: ToolSpec {}:{}:{}",
                    diagnostic.path.display(),
                    line,
                    diagnostic.column.unwrap_or(0),
                ));
            } else {
                self.warn(&format!(
                    "Warning: ToolSpec {}: {}",
                    diagnostic.path.display(),
                    diagnostic.error
                ));
            }
        }
    }
}

impl<T: InteractionHandler + ?Sized> UserReporter for T {}

pub fn install_tool(
    paths: &ProjectPaths,
    tool_id: &str,
    assume_yes: bool,
    reporter: &dyn UserReporter,
) -> Result<InstallToolOutcome> {
    let specs = load_toolspecs_with_diagnostics(paths, reporter)?;
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
        reporter.info(&confirm_message);
        if !reporter.confirm("Proceed [y/N]? ")? {
            return Err(MaccError::Validation("Installation cancelled.".into()));
        }
    }

    reporter.info(&format!("Installing tool '{}'.", tool_id));
    for command in &install.commands {
        run_install_command(&paths.root, command, false)?;
    }

    let initial_checks = run_tool_health_checks(&spec);
    reporter.info(&format_checks_table(&initial_checks));
    if !checks_all_installed(&initial_checks) {
        return Err(MaccError::Validation(format!(
            "Install completed but doctor checks are still failing for '{}'.",
            tool_id
        )));
    }

    if let Some(post_install) = &install.post_install {
        reporter.info(&format!("Running post-install setup for '{}'.", tool_id));
        run_install_command(&paths.root, post_install, true)?;
    }

    let final_checks = run_tool_health_checks(&spec);
    reporter.info(&format_checks_table(&final_checks));
    if !checks_all_installed(&final_checks) {
        return Err(MaccError::Validation(format!(
            "Post-install validation failed for '{}'.",
            tool_id
        )));
    }

    reporter.info(&format!("Tool '{}' is installed and healthy.", tool_id));
    Ok(InstallToolOutcome {
        tool_id: tool_id.to_string(),
        check_count: final_checks.len(),
    })
}

pub fn update_tools(
    paths: &ProjectPaths,
    opts: ToolUpdateCommandOptions<'_>,
    reporter: &dyn UserReporter,
) -> Result<ToolUpdateSummary> {
    let specs = load_toolspecs_with_diagnostics(paths, reporter)?;
    let canonical = load_canonical_config(&paths.config_path)?;
    let selected = select_tools_for_update(&specs, &canonical, opts.tool_id, opts.all, opts.only)?;
    if selected.is_empty() {
        return Err(MaccError::Validation(
            "No matching tools found for update.".into(),
        ));
    }

    let mut summary = ToolUpdateSummary {
        checked: opts.check,
        ..ToolUpdateSummary::default()
    };

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
            reporter.info(&format!(
                "Skipping '{}': not currently installed (run `macc tool install {}`).",
                spec.id, spec.id
            ));
            summary.skipped += 1;
            summary.statuses.push(status);
            continue;
        }
        if !opts.force && status.latest_version.is_some() && !status.is_outdated() {
            reporter.info(&format!(
                "Skipping '{}': already latest (current={}, latest={}).",
                spec.id, current_display, latest_display
            ));
            summary.already_latest += 1;
            summary.statuses.push(status);
            continue;
        }
        if opts.check {
            reporter.info(&format!(
                "[check] tool={} installed={} current={} latest={} source={}",
                spec.id, status.installed, current_display, latest_display, status.source
            ));
            summary.statuses.push(status);
            continue;
        }

        match update_single_tool(
            paths,
            &spec,
            opts.assume_yes,
            opts.rollback_on_fail,
            reporter,
        ) {
            Ok(()) => {
                reporter.info(&format!("Updated '{}'.", spec.id));
                summary.updated += 1;
            }
            Err(err) => {
                reporter.error(&format!("Failed to update '{}': {}", spec.id, err));
                summary.failed_tools.push(spec.id.clone());
            }
        }
        summary.statuses.push(status);
    }

    if opts.check {
        return Ok(summary);
    }

    reporter.info(&format!(
        "Update summary: updated={} already_latest={} skipped={} failed={}",
        summary.updated,
        summary.already_latest,
        summary.skipped,
        summary.failed_tools.len()
    ));

    if summary.failed_tools.is_empty() {
        Ok(summary)
    } else {
        Err(MaccError::Validation(format!(
            "Tool update failed for: {}",
            summary.failed_tools.join(", ")
        )))
    }
}

pub fn show_outdated_tools(
    paths: &ProjectPaths,
    only: Option<&str>,
    reporter: &dyn UserReporter,
) -> Result<OutdatedToolsReport> {
    let specs = load_toolspecs_with_diagnostics(paths, reporter)?;
    let canonical = load_canonical_config(&paths.config_path)?;
    let selected = select_tools_for_update(&specs, &canonical, None, true, only)?;

    let mut report = OutdatedToolsReport::default();
    for spec in selected {
        let status = get_tool_update_status(&spec);
        if status.installed && status.is_outdated() {
            report.outdated_count += 1;
        }
        report.statuses.push(status);
    }

    reporter.info(&format_outdated_table(&report));
    Ok(report)
}

pub fn format_checks_table(checks: &[ToolCheck]) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "{:<20} {:<10} {:<30}\n",
        "CHECK", "STATUS", "TARGET"
    ));
    output.push_str(&format!("{:-<20} {:-<10} {:-<30}\n", "", "", ""));
    for check in checks {
        let status_str = match &check.status {
            ToolStatus::Installed => "OK".to_string(),
            ToolStatus::Missing => "MISSING".to_string(),
            ToolStatus::Error(e) => format!("ERROR: {}", e),
        };
        output.push_str(&format!(
            "{:<20} {:<10} {:<30}\n",
            check.name, status_str, check.check_target
        ));
    }
    output
}

pub fn format_outdated_table(report: &OutdatedToolsReport) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "{:<14} {:<10} {:<16} {:<16} {:<14}\n",
        "TOOL", "INSTALLED", "CURRENT", "LATEST", "STATE"
    ));
    output.push_str(&format!(
        "{:-<14} {:-<10} {:-<16} {:-<16} {:-<14}\n",
        "", "", "", "", ""
    ));
    for status in &report.statuses {
        let state = if !status.installed {
            "not_installed"
        } else if status.is_outdated() {
            "outdated"
        } else if status.latest_version.is_some() {
            "up_to_date"
        } else {
            "unknown"
        };
        output.push_str(&format!(
            "{:<14} {:<10} {:<16} {:<16} {:<14}\n",
            status.id,
            if status.installed { "yes" } else { "no" },
            status
                .current_version
                .clone()
                .unwrap_or_else(|| "-".to_string()),
            status
                .latest_version
                .clone()
                .unwrap_or_else(|| "-".to_string()),
            state
        ));
    }
    output.push('\n');
    output.push_str(&format!("Outdated tools: {}", report.outdated_count));
    output
}

fn run_install_command(
    cwd: &std::path::Path,
    command: &ToolInstallCommand,
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

fn run_tool_health_checks(spec: &ToolSpec) -> Vec<ToolCheck> {
    let mut checks = Vec::new();
    if let Some(doctor_specs) = &spec.doctor {
        for check_spec in doctor_specs {
            checks.push(ToolCheck {
                name: spec.display_name.clone(),
                tool_id: Some(spec.id.clone()),
                check_target: check_spec.value.clone(),
                kind: check_spec.kind.clone(),
                status: ToolStatus::Missing,
                severity: check_spec.severity.clone(),
            });
        }
    } else {
        checks.push(ToolCheck {
            name: spec.display_name.clone(),
            tool_id: Some(spec.id.clone()),
            check_target: spec.id.clone(),
            kind: DoctorCheckKind::Which,
            status: ToolStatus::Missing,
            severity: crate::tool::CheckSeverity::Warning,
        });
    }
    run_checks(&mut checks);
    checks
}

fn checks_all_installed(checks: &[ToolCheck]) -> bool {
    checks
        .iter()
        .all(|check| matches!(check.status, ToolStatus::Installed))
}

fn load_toolspecs_with_diagnostics(
    paths: &ProjectPaths,
    reporter: &dyn UserReporter,
) -> Result<Vec<ToolSpec>> {
    let search_paths = ToolSpecLoader::default_search_paths(&paths.root);
    let loader = ToolSpecLoader::new(search_paths);
    let (specs, diagnostics) = loader.load_all_with_embedded();
    reporter.report_diagnostics(&diagnostics);
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
            "enabled" => {
                selected.retain(|spec| canonical.tools.enabled.iter().any(|id| id == &spec.id))
            }
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
                if vs.latest.is_some() {
                    " (+latest)"
                } else {
                    ""
                }
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

pub fn run_version_command(cmd_spec: &ToolInstallCommand) -> Option<String> {
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

pub fn extract_version_token(text: &str) -> Option<String> {
    for raw in text.split_whitespace() {
        let token = raw.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '.' && c != '-');
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
    paths: &ProjectPaths,
    spec: &ToolSpec,
    assume_yes: bool,
    rollback_on_fail: bool,
    reporter: &dyn UserReporter,
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
        reporter.info(&update_spec.confirm_message.unwrap_or_else(|| {
            format!(
                "This will run update commands for '{}'. Continue?",
                spec.display_name
            )
        }));
        if !reporter.confirm("Proceed [y/N]? ")? {
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
        reporter.info(&format_checks_table(&final_checks));
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
    reporter.warn(&format!(
        "Rollback requested for '{}' but no generic rollback contract is defined. Configure tool-specific rollback in ToolSpec before enabling this in production.",
        spec.id
    ));
    update_result
}
