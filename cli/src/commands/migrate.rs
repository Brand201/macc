use crate::commands::AppContext;
use crate::commands::Command;
use crate::services::interaction::CliInteraction;
use macc_core::Result;

pub struct MigrateCommand {
    app: AppContext,
    apply: bool,
}

impl MigrateCommand {
    pub fn new(app: AppContext, apply: bool) -> Self {
        Self { app, apply }
    }
}

impl Command for MigrateCommand {
    fn run(&self) -> Result<()> {
        let paths = self.app.project_paths()?;
        let canonical = self.app.canonical_config()?;

        let (descriptors, diagnostics) = self.app.engine.list_tools(&paths);
        macc_core::service::project::report_diagnostics(&diagnostics, &CliInteraction);
        let allowed_tools: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();
        let result = self.app.engine.migrate_project(
            &paths,
            canonical,
            &allowed_tools,
            self.apply,
            &CliInteraction,
        )?;

        if result.warnings.is_empty() {
            println!("No legacy configuration found. Your config is up to date.");
            return Ok(());
        }

        println!("Legacy configuration detected:");
        for warning in &result.warnings {
            println!("  - {}", warning);
        }

        if result.wrote_config {
            println!(
                "\nMigrated configuration written to {}",
                paths.config_path.display()
            );
        } else {
            println!(
                "\nMigration not written. Use --apply to force write, or confirm when prompted."
            );
            println!("Preview of migrated config:");
            println!("---");
            if let Some(preview) = result.preview_yaml {
                println!("{}", preview);
            }
            println!("---");
        }

        Ok(())
    }
}
