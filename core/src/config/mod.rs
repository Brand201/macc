pub mod migrate;

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct CanonicalConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub tools: ToolsConfig,
    #[serde(default)]
    pub standards: StandardsConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selections: Option<SelectionsConfig>,
    #[serde(default)]
    pub automation: AutomationConfig,
    #[serde(default = "default_mcp_templates")]
    pub mcp_templates: Vec<McpTemplateDefinition>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Default)]
pub struct ToolsConfig {
    pub enabled: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub config: BTreeMap<String, serde_json::Value>,
    #[serde(flatten)]
    pub settings: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Default)]
pub struct StandardsConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(flatten, skip_serializing_if = "BTreeMap::is_empty")]
    pub inline: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct SelectionsConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skills: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agents: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mcp: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct AutomationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ralph: Option<RalphConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coordinator: Option<CoordinatorConfig>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct RalphConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_ralph_iterations")]
    pub iterations_default: usize,
    #[serde(default = "default_ralph_branch")]
    pub branch_name: String,
    #[serde(default = "default_true")]
    pub stop_on_failure: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct CoordinatorConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coordinator_tool: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference_branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prd_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_registry_file: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_priority: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub max_parallel_per_tool: BTreeMap<String, usize>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tool_specializations: BTreeMap<String, Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_dispatch: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_parallel: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase_runner_max_attempts: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_claimed_seconds: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_in_progress_seconds: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_changes_requested_seconds: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_action: Option<String>,
}

fn default_true() -> bool {
    true
}

fn default_ralph_iterations() -> usize {
    5
}

fn default_ralph_branch() -> String {
    "ralph".to_string()
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct McpTemplateDefinition {
    pub id: String,
    pub title: String,
    pub description: String,
    pub command: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env_placeholders: Vec<McpEnvPlaceholder>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_notes: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct McpEnvPlaceholder {
    pub name: String,
    pub placeholder: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

pub fn load_canonical_config<P: AsRef<Path>>(path: P) -> crate::Result<CanonicalConfig> {
    let path = path.as_ref();
    let content = std::fs::read_to_string(path).map_err(|e| crate::MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "read config".into(),
        source: e,
    })?;

    let config = CanonicalConfig::from_yaml(&content).map_err(|e| crate::MaccError::Config {
        path: path.to_string_lossy().into(),
        source: e,
    })?;

    config.validate()?;
    Ok(config)
}

impl CanonicalConfig {
    pub fn from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    pub fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }

    pub fn validate(&self) -> crate::Result<()> {
        let mut seen_ids = HashSet::new();

        for template in &self.mcp_templates {
            let normalized_id = template.id.trim();
            if normalized_id.is_empty() {
                return Err(crate::MaccError::Validation(
                    "MCP template ID cannot be empty".into(),
                ));
            }

            if !seen_ids.insert(normalized_id.to_string()) {
                return Err(crate::MaccError::Validation(format!(
                    "Duplicate MCP template ID: {}",
                    normalized_id
                )));
            }

            if template.command.trim().is_empty() {
                return Err(crate::MaccError::Validation(format!(
                    "MCP template '{}' must include a command",
                    template.id
                )));
            }

            for placeholder in &template.env_placeholders {
                if placeholder.name.trim().is_empty() {
                    return Err(crate::MaccError::Validation(format!(
                        "MCP template '{}' contains an env placeholder without a name",
                        template.id
                    )));
                }

                if placeholder.placeholder.trim().is_empty() {
                    return Err(crate::MaccError::Validation(format!(
                        "MCP template '{}' contains an env placeholder '{}' without a placeholder value",
                        template.id, placeholder.name
                    )));
                }
            }
        }

        Ok(())
    }
}

impl Default for CanonicalConfig {
    fn default() -> Self {
        Self {
            version: None,
            tools: ToolsConfig::default(),
            standards: StandardsConfig::default(),
            selections: None,
            automation: AutomationConfig::default(),
            mcp_templates: default_mcp_templates(),
        }
    }
}

fn default_mcp_templates() -> Vec<McpTemplateDefinition> {
    vec![
        McpTemplateDefinition {
            id: "brave-search".to_string(),
            title: "Brave Search".to_string(),
            description: "Search the web via the Brave Search API (placeholder only).".to_string(),
            command: "node".to_string(),
            args: vec!["scripts/brave-search-mcp.js".to_string()],
            env_placeholders: vec![McpEnvPlaceholder {
                name: "BRAVE_API_KEY".to_string(),
                placeholder: "${BRAVE_API_KEY}".to_string(),
                description: Some(
                    "Brave Search API key placeholder; set this locally before running."
                        .to_string(),
                ),
            }],
            auth_notes: Some(
                "Provide ${BRAVE_API_KEY} via your environment; MACC only writes the placeholder."
                    .to_string(),
            ),
        },
        McpTemplateDefinition {
            id: "github-issues".to_string(),
            title: "GitHub Issues".to_string(),
            description: "Manage GitHub issues for the current repository (placeholder auth)."
                .to_string(),
            command: "python".to_string(),
            args: vec!["scripts/github-issues-mcp.py".to_string()],
            env_placeholders: vec![McpEnvPlaceholder {
                name: "GITHUB_TOKEN".to_string(),
                placeholder: "${GITHUB_TOKEN}".to_string(),
                description: Some(
                    "Personal access token with repo scope; MACC keeps only the placeholder."
                        .to_string(),
                ),
            }],
            auth_notes: Some(
                "Set ${GITHUB_TOKEN} locally and keep the real token out of version control."
                    .to_string(),
            ),
        },
        McpTemplateDefinition {
            id: "local-notes".to_string(),
            title: "Local Notes".to_string(),
            description:
                "Expose project notes stored in the repository without additional authentication."
                    .to_string(),
            command: "bash".to_string(),
            args: vec![
                "scripts/local-notes.sh".to_string(),
                "--dir".to_string(),
                "./notes".to_string(),
            ],
            env_placeholders: vec![],
            auth_notes: Some(
                "No secrets required; reads from the checked-in notes directory.".to_string(),
            ),
        },
    ]
}

pub fn builtin_mcp_templates() -> Vec<McpTemplateDefinition> {
    default_mcp_templates()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tool_ids() -> (String, String) {
        let suffix = uuid_v4_like();
        (format!("tool-a-{}", suffix), format!("tool-b-{}", suffix))
    }

    #[test]
    fn test_minimal_roundtrip() {
        let (tool_one, tool_two) = tool_ids();
        let yaml = format!("tools:\n  enabled:\n  - {}\n  - {}\n", tool_one, tool_two);
        let config = CanonicalConfig::from_yaml(&yaml).expect("Should parse minimal yaml");
        assert_eq!(config.tools.enabled, vec![tool_one, tool_two]);

        let reserialized = config.to_yaml().expect("Should serialize back to yaml");
        let config2 =
            CanonicalConfig::from_yaml(&reserialized).expect("Should parse reserialized yaml");
        assert_eq!(config, config2);
    }

    #[test]
    fn test_full_roundtrip() {
        let (tool_one, _) = tool_ids();
        let yaml = format!(
            "version: v1\ntools:\n  enabled:\n  - {}\nstandards:\n  path: config/standards.md\nselections:\n  skills:\n  - implement\n  agents:\n  - architect\n",
            tool_one
        );
        let config = CanonicalConfig::from_yaml(&yaml).expect("Should parse full yaml");
        assert_eq!(config.version, Some("v1".to_string()));
        assert_eq!(
            config.standards.path,
            Some("config/standards.md".to_string())
        );
        assert_eq!(
            config.selections.as_ref().unwrap().skills,
            vec!["implement"]
        );

        let reserialized = config.to_yaml().expect("Should serialize back to yaml");
        let config2 =
            CanonicalConfig::from_yaml(&reserialized).expect("Should parse reserialized yaml");
        assert_eq!(config, config2);
    }

    #[test]
    fn test_tool_config_agents_roundtrip() {
        let (tool_one, _) = tool_ids();
        let yaml = format!(
            "tools:\n  enabled:\n  - {}\n  {}:\n    agents:\n    - architect\n    - reviewer\n",
            tool_one, tool_one
        );
        let config = CanonicalConfig::from_yaml(&yaml).expect("Should parse tool agents");
        let tool_val = config
            .tools
            .settings
            .get(&tool_one)
            .expect("tool config present");
        let agents = tool_val
            .get("agents")
            .expect("agents present")
            .as_array()
            .expect("is array");
        assert_eq!(agents.len(), 2);
        assert_eq!(agents[0].as_str().unwrap(), "architect");
        assert_eq!(agents[1].as_str().unwrap(), "reviewer");

        let reserialized = config.to_yaml().expect("Should serialize back to yaml");
        let config2 =
            CanonicalConfig::from_yaml(&reserialized).expect("Should parse reserialized yaml");
        assert_eq!(config, config2);
    }

    #[test]
    fn test_tool_user_mcp_merge_roundtrip() {
        let (_, tool_two) = tool_ids();
        let yaml = format!(
            "tools:\n  enabled:\n  - {}\n  {}:\n    user_mcp_merge: true\n",
            tool_two, tool_two
        );
        let config = CanonicalConfig::from_yaml(&yaml).expect("Should parse tool config");
        let tool_val = config
            .tools
            .settings
            .get(&tool_two)
            .expect("tool config present");
        assert_eq!(
            tool_val
                .get("user_mcp_merge")
                .expect("field present")
                .as_bool()
                .unwrap(),
            true
        );

        let reserialized = config.to_yaml().expect("Should serialize back to yaml");
        let config2 =
            CanonicalConfig::from_yaml(&reserialized).expect("Should parse reserialized yaml");
        assert_eq!(config, config2);
    }

    #[test]
    fn test_tools_config_map_roundtrip() {
        let (tool_one, _) = tool_ids();
        let yaml = format!(
            "tools:\n  enabled:\n  - {}\n  config:\n    {}:\n      agents:\n      - architect\n",
            tool_one, tool_one
        );
        let config = CanonicalConfig::from_yaml(&yaml).expect("Should parse tools.config map");
        let tool_config = config
            .tools
            .config
            .get(&tool_one)
            .expect("tool config present in map");
        let agents = tool_config
            .get("agents")
            .expect("agents present")
            .as_array()
            .expect("is array");
        assert_eq!(agents[0].as_str().unwrap(), "architect");

        let reserialized = config.to_yaml().expect("Should serialize back to yaml");
        assert!(reserialized.contains("config:"));
        assert!(reserialized.contains(&format!("{}:", tool_one)));

        let config2 =
            CanonicalConfig::from_yaml(&reserialized).expect("Should parse reserialized yaml");
        assert_eq!(config, config2);
    }

    #[test]
    fn test_ralph_automation_roundtrip() {
        let yaml = r#"tools:
  enabled: []
automation:
  ralph:
    enabled: true
    iterations_default: 10
    branch_name: custom-ralph
    stop_on_failure: false
"#;
        let config = CanonicalConfig::from_yaml(yaml).expect("Should parse ralph config");
        let ralph = config
            .automation
            .ralph
            .as_ref()
            .expect("ralph config present");
        assert!(ralph.enabled);
        assert_eq!(ralph.iterations_default, 10);
        assert_eq!(ralph.branch_name, "custom-ralph");
        assert!(!ralph.stop_on_failure);

        let reserialized = config.to_yaml().expect("Should serialize back to yaml");
        let config2 =
            CanonicalConfig::from_yaml(&reserialized).expect("Should parse reserialized yaml");
        assert_eq!(config, config2);
    }

    #[test]
    fn test_coordinator_automation_roundtrip() {
        let yaml = r#"tools:
  enabled: []
automation:
  coordinator:
    coordinator_tool: tool-alpha
    reference_branch: develop
    prd_file: prd.json
    task_registry_file: task_registry.json
    tool_priority:
      - tool-alpha
      - tool-beta
    max_parallel_per_tool:
      tool-alpha: 3
      tool-beta: 2
    tool_specializations:
      frontend:
        - tool-beta
        - tool-gamma
    max_dispatch: 5
    max_parallel: 2
    timeout_seconds: 30
    phase_runner_max_attempts: 2
    stale_claimed_seconds: 600
    stale_in_progress_seconds: 1200
    stale_changes_requested_seconds: 1800
    stale_action: blocked
"#;
        let config = CanonicalConfig::from_yaml(yaml).expect("Should parse coordinator config");
        let coordinator = config
            .automation
            .coordinator
            .as_ref()
            .expect("coordinator config present");
        assert_eq!(coordinator.coordinator_tool.as_deref(), Some("tool-alpha"));
        assert_eq!(coordinator.reference_branch.as_deref(), Some("develop"));
        assert_eq!(coordinator.prd_file.as_deref(), Some("prd.json"));
        assert_eq!(
            coordinator.task_registry_file.as_deref(),
            Some("task_registry.json")
        );
        assert_eq!(coordinator.tool_priority, vec!["tool-alpha", "tool-beta"]);
        assert_eq!(
            coordinator.max_parallel_per_tool.get("tool-alpha"),
            Some(&3)
        );
        assert_eq!(
            coordinator.tool_specializations.get("frontend"),
            Some(&vec!["tool-beta".to_string(), "tool-gamma".to_string()])
        );
        assert_eq!(coordinator.max_dispatch, Some(5));
        assert_eq!(coordinator.max_parallel, Some(2));
        assert_eq!(coordinator.timeout_seconds, Some(30));
        assert_eq!(coordinator.phase_runner_max_attempts, Some(2));
        assert_eq!(coordinator.stale_claimed_seconds, Some(600));
        assert_eq!(coordinator.stale_in_progress_seconds, Some(1200));
        assert_eq!(coordinator.stale_changes_requested_seconds, Some(1800));
        assert_eq!(coordinator.stale_action.as_deref(), Some("blocked"));

        let reserialized = config.to_yaml().expect("Should serialize back to yaml");
        let config2 =
            CanonicalConfig::from_yaml(&reserialized).expect("Should parse reserialized yaml");
        assert_eq!(config, config2);
    }

    #[test]
    fn test_inline_standards() {
        let yaml = r#"tools:
  enabled: []
standards:
  language: English
  package_manager: pnpm
"#;
        let config = CanonicalConfig::from_yaml(yaml).expect("Should parse inline standards");
        assert_eq!(config.standards.inline.get("language").unwrap(), "English");
        assert_eq!(
            config.standards.inline.get("package_manager").unwrap(),
            "pnpm"
        );

        let reserialized = config.to_yaml().expect("Should serialize back to yaml");
        assert!(reserialized.contains("language: English"));
        assert!(reserialized.contains("package_manager: pnpm"));
    }

    #[test]
    fn test_deterministic_standards_serialization() {
        let mut inline = BTreeMap::new();
        inline.insert("z".to_string(), "last".to_string());
        inline.insert("a".to_string(), "first".to_string());
        inline.insert("m".to_string(), "middle".to_string());

        let config = CanonicalConfig {
            version: None,
            tools: ToolsConfig {
                enabled: vec!["test".to_string()],
                ..Default::default()
            },
            standards: StandardsConfig { path: None, inline },
            selections: None,
            automation: AutomationConfig::default(),
            mcp_templates: Vec::new(),
        };

        let yaml1 = config.to_yaml().expect("Should serialize");
        let yaml2 = config.to_yaml().expect("Should serialize");

        assert_eq!(yaml1, yaml2);

        // Check that keys are in alphabetical order in YAML
        let a_pos = yaml1.find("a: first").unwrap();
        let m_pos = yaml1.find("m: middle").unwrap();
        let z_pos = yaml1.find("z: last").unwrap();

        assert!(a_pos < m_pos);
        assert!(m_pos < z_pos);
    }

    #[test]
    fn test_deny_unknown_fields() {
        let yaml = r#"tools:
  enabled: []
unknown_field: true
"#;
        let err = CanonicalConfig::from_yaml(yaml).unwrap_err();
        assert!(err.to_string().contains("unknown field `unknown_field`"));
    }

    #[test]
    fn test_invalid_yaml_syntax() {
        let yaml = r#"tools:
  enabled: [
"#;
        let err = CanonicalConfig::from_yaml(yaml).unwrap_err();
        assert!(err
            .to_string()
            .contains("did not find expected node content"));
    }

    #[test]
    fn test_missing_required_field() {
        let yaml = r#"version: v1
"#;
        let err = CanonicalConfig::from_yaml(yaml).unwrap_err();
        assert!(err.to_string().contains("missing field `tools`"));
    }

    #[test]
    fn test_default_mcp_templates_ids_stable() {
        let ids: Vec<_> = default_mcp_templates()
            .iter()
            .map(|template| template.id.clone())
            .collect();

        assert_eq!(
            ids,
            vec![
                "brave-search".to_string(),
                "github-issues".to_string(),
                "local-notes".to_string()
            ]
        );
    }

    #[test]
    fn test_duplicate_mcp_template_ids_rejected() {
        let mut config = CanonicalConfig::default();
        let duplicate_id = config.mcp_templates[0].id.clone();
        config.mcp_templates.push(McpTemplateDefinition {
            id: duplicate_id.clone(),
            title: "Duplicate Entry".to_string(),
            description: "Another template using the same ID for testing.".to_string(),
            command: "echo".to_string(),
            args: vec!["test".to_string()],
            env_placeholders: vec![],
            auth_notes: None,
        });

        let err = config.validate().unwrap_err();
        assert!(err
            .to_string()
            .contains(&format!("Duplicate MCP template ID: {}", duplicate_id)));
    }

    #[test]
    fn test_load_config_errors() {
        use std::fs;

        let temp_dir = std::env::temp_dir().join(format!("macc_config_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();

        // 1. Invalid YAML syntax
        let path = temp_dir.join("invalid_syntax.yaml");
        fs::write(&path, "tools:\n  enabled: [").unwrap();
        let err = load_canonical_config(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Configuration error in"));
        assert!(msg.contains("invalid_syntax.yaml"));
        assert!(msg.contains("did not find expected node content"));

        // 2. Unknown field
        let path = temp_dir.join("unknown_field.yaml");
        fs::write(&path, "tools:\n  enabled: []\nextra: true").unwrap();
        let err = load_canonical_config(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown_field.yaml"));
        assert!(msg.contains("unknown field `extra`"));

        // 3. Missing required field
        let path = temp_dir.join("missing_field.yaml");
        fs::write(&path, "version: v1").unwrap();
        let err = load_canonical_config(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("missing_field.yaml"));
        assert!(msg.contains("missing field `tools`"));

        // 4. Missing sub-field
        let path = temp_dir.join("missing_subfield.yaml");
        fs::write(&path, "tools: {}").unwrap();
        let err = load_canonical_config(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("missing_subfield.yaml"));
        assert!(msg.contains("missing field `enabled`"));

        fs::remove_dir_all(&temp_dir).ok();
    }

    fn uuid_v4_like() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        format!("{:?}", since_the_epoch.as_nanos())
    }
}
