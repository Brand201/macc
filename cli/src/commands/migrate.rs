use crate::commands::Command;
use crate::commands::AppContext;
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
        crate::services::project::report_diagnostics(&diagnostics);
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

        if self.apply {
            let yaml = result.config.to_yaml().map_err(|e| {
                macc_core::MaccError::Validation(format!(
                    "Failed to serialize migrated config: {}",
                    e
                ))
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
}
