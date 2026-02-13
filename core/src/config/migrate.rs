use crate::config::CanonicalConfig;

#[derive(Debug, Clone)]
pub struct MigrationResult {
    pub config: CanonicalConfig,
    pub warnings: Vec<String>,
}

/// Migrates legacy per-tool configuration fields from `tools.settings` to `tools.config`.
pub fn migrate(config: CanonicalConfig) -> MigrationResult {
    migrate_with_known_tools(config, &[])
}

/// Migrates legacy per-tool configuration fields, using a known-tool list to detect
/// configs that should move even if the tool is currently disabled.
pub fn migrate_with_known_tools(
    mut config: CanonicalConfig,
    known_tools: &[String],
) -> MigrationResult {
    let mut warnings = Vec::new();

    // Identify legacy fields in tools.settings that should be in tools.config.
    // We migrate any key in settings that matches an enabled tool OR
    // is a known tool provided by the caller.
    let known: std::collections::HashSet<String> = known_tools.iter().cloned().collect();

    let mut keys_to_migrate = Vec::new();
    for key in config.tools.settings.keys() {
        if config.tools.enabled.contains(key) || known.contains(key) {
            keys_to_migrate.push(key.clone());
        }
    }

    for key in keys_to_migrate {
        if let Some(val) = config.tools.settings.remove(&key) {
            if !config.tools.config.contains_key(&key) {
                config.tools.config.insert(key.clone(), val);
                warnings.push(format!(
                    "Migrated legacy tool configuration for '{}' to 'tools.config.{}'",
                    key, key
                ));
            } else {
                // Collision: both top-level and in config map.
                // Keep the one in config map, but warn.
                warnings.push(format!(
                    "Found legacy configuration for '{}' at top-level of 'tools', but 'tools.config.{}' already exists. Keeping 'tools.config.{}' and removing legacy field.",
                    key, key, key
                ));
            }
        }
    }

    MigrationResult { config, warnings }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ToolsConfig;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn tool_ids() -> (String, String) {
        let suffix = uuid_v4_like();
        (format!("tool-a-{}", suffix), format!("tool-b-{}", suffix))
    }

    fn uuid_v4_like() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{:x}", nanos)
    }

    #[test]
    fn test_migrate_enabled_tool_settings() {
        let (tool_a, _) = tool_ids();
        let mut settings = BTreeMap::new();
        settings.insert(tool_a.clone(), json!({"agents": ["architect"]}));

        let config = CanonicalConfig {
            tools: ToolsConfig {
                enabled: vec![tool_a.clone()],
                config: BTreeMap::new(),
                settings,
            },
            ..Default::default()
        };

        let result = migrate(config);

        assert_eq!(result.warnings.len(), 1);
        assert!(result.warnings[0].contains("Migrated"));
        assert!(result.config.tools.config.contains_key(&tool_a));
        assert!(!result.config.tools.settings.contains_key(&tool_a));

        let tool_cfg = result.config.tools.config.get(&tool_a).unwrap();
        assert_eq!(tool_cfg["agents"], json!(["architect"]));
    }

    #[test]
    fn test_migrate_unknown_but_enabled_tool() {
        let mut settings = BTreeMap::new();
        settings.insert("custom_tool".to_string(), json!({"key": "val"}));

        let config = CanonicalConfig {
            tools: ToolsConfig {
                enabled: vec!["custom_tool".to_string()],
                config: BTreeMap::new(),
                settings,
            },
            ..Default::default()
        };

        let result = migrate(config);

        assert_eq!(result.warnings.len(), 1);
        assert!(result.config.tools.config.contains_key("custom_tool"));
    }

    #[test]
    fn test_migrate_known_but_disabled_tool() {
        let (_, tool_b) = tool_ids();
        let mut settings = BTreeMap::new();
        settings.insert(tool_b.clone(), json!({"user_mcp_merge": true}));

        let config = CanonicalConfig {
            tools: ToolsConfig {
                enabled: vec![],
                config: BTreeMap::new(),
                settings,
            },
            ..Default::default()
        };

        let result = migrate_with_known_tools(config, &vec![tool_b.clone()]);

        assert_eq!(result.warnings.len(), 1);
        assert!(result.config.tools.config.contains_key(&tool_b));
    }

    #[test]
    fn test_migration_collision_prefers_config_map() {
        let (tool_a, _) = tool_ids();
        let mut settings = BTreeMap::new();
        settings.insert(tool_a.clone(), json!({"agents": ["old"]}));

        let mut config_map = BTreeMap::new();
        config_map.insert(tool_a.clone(), json!({"agents": ["new"]}));

        let config = CanonicalConfig {
            tools: ToolsConfig {
                enabled: vec![tool_a.clone()],
                config: config_map,
                settings,
            },
            ..Default::default()
        };

        let result = migrate(config);

        assert_eq!(result.warnings.len(), 1);
        assert!(result.warnings[0].contains("already exists"));

        let tool_cfg = result.config.tools.config.get(&tool_a).unwrap();
        assert_eq!(tool_cfg["agents"], json!(["new"]));
    }

    #[test]
    fn test_non_tool_settings_are_not_migrated() {
        let (tool_a, _) = tool_ids();
        let mut settings = BTreeMap::new();
        settings.insert("random_key".to_string(), json!(true));

        let config = CanonicalConfig {
            tools: ToolsConfig {
                enabled: vec![tool_a],
                config: BTreeMap::new(),
                settings,
            },
            ..Default::default()
        };

        let result = migrate(config);

        assert_eq!(result.warnings.len(), 0);
        assert!(result.config.tools.settings.contains_key("random_key"));
    }
}
