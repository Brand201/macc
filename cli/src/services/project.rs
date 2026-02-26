use macc_core::engine::Engine;
use macc_core::tool::spec::CheckSeverity;
use macc_core::{MaccError, Result};

pub fn ensure_initialized_paths(start_dir: &std::path::Path) -> Result<macc_core::ProjectPaths> {
    let paths = macc_core::find_project_root(start_dir)
        .unwrap_or_else(|_| macc_core::ProjectPaths::from_root(start_dir));
    macc_core::init(&paths, false)?;
    Ok(paths)
}

pub fn run_doctor<E: Engine>(
    paths: &macc_core::ProjectPaths,
    engine: &E,
    fix: bool,
) -> Result<()> {
    let checks = engine.doctor(paths);
    crate::print_checks(&checks);

    let failed: Vec<_> = checks
        .iter()
        .filter(|c| !matches!(c.status, macc_core::doctor::ToolStatus::Installed))
        .collect();
    if failed.is_empty() {
        println!("All checks passed.");
        return Ok(());
    }

    println!("\n{} check(s) failed.", failed.len());
    if !fix {
        println!("Run with --fix to apply safe automatic fixes.");
        return Err(MaccError::Validation("Doctor checks failed.".into()));
    }

    let any_applied = false;
    for check in failed {
        if fix {
            println!(
                "No automatic fix registered for doctor check '{}' (target='{}').",
                check.name, check.check_target
            );
        }
    }

    if any_applied {
        println!("\nRe-running checks...\n");
        let checks = engine.doctor(paths);
        crate::print_checks(&checks);
        if checks
            .iter()
            .all(|c| matches!(c.status, macc_core::doctor::ToolStatus::Installed))
        {
            println!("All checks passed after fixes.");
            return Ok(());
        }
    }

    let blocking = checks.iter().any(|check| {
        !matches!(check.status, macc_core::doctor::ToolStatus::Installed)
            && matches!(check.severity, CheckSeverity::Error)
    });

    if blocking {
        return Err(MaccError::Validation("Doctor checks failed.".into()));
    }

    Ok(())
}

pub fn report_diagnostics(diagnostics: &[macc_core::tool::ToolDiagnostic]) {
    if diagnostics.is_empty() {
        return;
    }
    for d in diagnostics {
        match (d.line, d.column) {
            (Some(line), Some(column)) => {
                eprintln!(
                    "Warning: ToolSpec {}:{}:{}: {}",
                    d.path.display(), line, column, d.error
                );
            }
            _ => {
                eprintln!("Warning: ToolSpec {}: {}", d.path.display(), d.error);
            }
        }
    }
}
