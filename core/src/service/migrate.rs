use crate::config::CanonicalConfig;
use crate::service::interaction::InteractionHandler;
use crate::{MaccError, ProjectPaths, Result};

#[derive(Debug, Clone, Default)]
pub struct MigrateOutcome {
    pub warnings: Vec<String>,
    pub wrote_config: bool,
    pub preview_yaml: Option<String>,
}

pub fn migrate_project(
    paths: &ProjectPaths,
    canonical: CanonicalConfig,
    allowed_tools: &[String],
    apply: bool,
    ui: &dyn InteractionHandler,
) -> Result<MigrateOutcome> {
    let result = crate::migrate::migrate_with_known_tools(canonical, allowed_tools);
    if result.warnings.is_empty() {
        return Ok(MigrateOutcome::default());
    }

    let mut wrote_config = false;
    let mut preview_yaml = None;

    if apply || ui.confirm("Write migrated configuration to disk now [y/N]? ")? {
        let yaml = result.config.to_yaml().map_err(|e| {
            MaccError::Validation(format!("Failed to serialize migrated config: {}", e))
        })?;
        crate::atomic_write(paths, &paths.config_path, yaml.as_bytes())?;
        wrote_config = true;
    } else {
        preview_yaml = Some(result.config.to_yaml().map_err(|e| {
            MaccError::Validation(format!(
                "Failed to serialize migrated config preview: {}",
                e
            ))
        })?);
    }

    Ok(MigrateOutcome {
        warnings: result.warnings,
        wrote_config,
        preview_yaml,
    })
}
