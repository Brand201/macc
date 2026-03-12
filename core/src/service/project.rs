use crate::doctor::ToolStatus;
use crate::service::interaction::InteractionHandler;
use crate::tool::spec::CheckSeverity;
use crate::tool::ToolDiagnostic;
use crate::{MaccError, ProjectPaths, Result};

pub fn ensure_initialized_paths(start_dir: &std::path::Path) -> Result<ProjectPaths> {
    let paths =
        crate::find_project_root(start_dir).unwrap_or_else(|_| ProjectPaths::from_root(start_dir));
    crate::init(&paths, false)?;
    Ok(paths)
}

pub fn run_doctor(
    paths: &ProjectPaths,
    engine: &(impl crate::engine::Engine + ?Sized),
    fix: bool,
    interaction: &dyn InteractionHandler,
) -> Result<()> {
    let checks = engine.doctor(paths);
    interaction.info(&crate::service::tooling::format_checks_table(&checks));

    let failed: Vec<_> = checks
        .iter()
        .filter(|c| !matches!(c.status, ToolStatus::Installed))
        .collect();
    if failed.is_empty() {
        interaction.info("All checks passed.");
        return Ok(());
    }

    interaction.info(&format!("\n{} check(s) failed.", failed.len()));
    if !fix {
        interaction.info("Run with --fix to apply safe automatic fixes.");
        return Err(MaccError::Validation("Doctor checks failed.".into()));
    }

    let any_applied = false;
    for check in failed {
        interaction.info(&format!(
            "No automatic fix registered for doctor check '{}' (target='{}').",
            check.name, check.check_target
        ));
    }

    if any_applied {
        interaction.info("\nRe-running checks...\n");
        let checks = engine.doctor(paths);
        interaction.info(&crate::service::tooling::format_checks_table(&checks));
        if checks
            .iter()
            .all(|c| matches!(c.status, crate::doctor::ToolStatus::Installed))
        {
            interaction.info("All checks passed after fixes.");
            return Ok(());
        }
    }

    let blocking = checks.iter().any(|check| {
        !matches!(check.status, ToolStatus::Installed)
            && matches!(check.severity, CheckSeverity::Error)
    });

    if blocking {
        return Err(MaccError::Validation("Doctor checks failed.".into()));
    }

    Ok(())
}

pub fn report_diagnostics(diagnostics: &[ToolDiagnostic], interaction: &dyn InteractionHandler) {
    if diagnostics.is_empty() {
        return;
    }
    for d in diagnostics {
        match (d.line, d.column) {
            (Some(line), Some(column)) => {
                interaction.warn(&format!(
                    "Warning: ToolSpec {}:{}:{}: {}",
                    d.path.display(),
                    line,
                    column,
                    d.error
                ));
            }
            _ => {
                interaction.warn(&format!(
                    "Warning: ToolSpec {}: {}",
                    d.path.display(),
                    d.error
                ));
            }
        }
    }
}

pub fn ensure_coordinator_run_id() -> String {
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
