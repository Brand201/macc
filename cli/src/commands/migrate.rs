use crate::commands::Command;
use macc_core::engine::Engine;
use macc_core::Result;
use std::path::{Path, PathBuf};

pub struct MigrateCommand<'a, E: Engine> {
    cwd: PathBuf,
    engine: &'a E,
    apply: bool,
}

impl<'a, E: Engine> MigrateCommand<'a, E> {
    pub fn new(cwd: &Path, engine: &'a E, apply: bool) -> Self {
        Self { cwd: cwd.to_path_buf(), engine, apply }
    }
}

impl<'a, E: Engine> Command for MigrateCommand<'a, E> {
    fn run(&self) -> Result<()> {
        let paths = macc_core::find_project_root(&self.cwd)?;
        let canonical = macc_core::load_canonical_config(&paths.config_path)?;

        let (descriptors, diagnostics) = self.engine.list_tools(&paths);
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
