use macc_core::resolve::ResolvedConfig;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct ClaudeConfig {
    pub language: String,
    pub model: String,
    pub permissions: String,
    pub standards_inline: BTreeMap<String, String>,
    pub standards_path: Option<String>,
    pub skills: Vec<String>,
    pub agents: Vec<String>,
    pub has_mcp: bool,
    pub rules_enabled: bool,
    pub tool_config: serde_json::Value,
}

#[derive(Debug, Deserialize, Default)]
struct ClaudeConfigSource {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    permissions: Option<String>,
    #[serde(default)]
    skills: Vec<String>,
    #[serde(default)]
    agents: Vec<String>,
    #[serde(default)]
    rules_enabled: bool,
}

impl ClaudeConfig {
    pub fn from_resolved(resolved: &ResolvedConfig) -> Self {
        let tool_config = resolved
            .tools
            .config
            .get("claude")
            .or_else(|| resolved.tools.specific.get("claude"))
            .cloned()
            .unwrap_or_else(|| serde_json::Value::Object(Default::default()));

        let source: ClaudeConfigSource =
            serde_json::from_value(tool_config.clone()).unwrap_or_default();

        let language = source
            .language
            .or_else(|| resolved.standards.inline.get("language").cloned())
            .unwrap_or_else(|| "English".to_string());

        let model = source.model.unwrap_or_else(|| "sonnet".to_string());

        let permissions = source.permissions.unwrap_or_else(|| "safe".to_string());

        let mut skills_set = BTreeSet::new();
        // Global selections
        for skill in &resolved.selections.skills {
            skills_set.insert(skill.clone());
        }
        // Tool-specific selections
        for skill in &source.skills {
            skills_set.insert(skill.clone());
        }

        let mut agents_set = BTreeSet::new();
        for agent in &resolved.selections.agents {
            agents_set.insert(agent.clone());
        }
        for agent in &source.agents {
            agents_set.insert(agent.clone());
        }
        let rules_enabled = source.rules_enabled;

        let has_mcp = !resolved.selections.mcp.is_empty();

        Self {
            language,
            model,
            permissions,
            standards_inline: resolved.standards.inline.clone(),
            standards_path: resolved.standards.path.clone(),
            skills: skills_set.into_iter().collect(),
            agents: agents_set.into_iter().collect(),
            has_mcp,
            rules_enabled,
            tool_config,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use macc_core::resolve::{
        ResolvedConfig, ResolvedSelectionsConfig, ResolvedStandardsConfig, ResolvedToolsConfig,
    };
    use serde_json::json;

    fn base_resolved() -> ResolvedConfig {
        ResolvedConfig {
            version: "v1".to_string(),
            tools: ResolvedToolsConfig {
                enabled: vec!["claude".to_string()],
                ..Default::default()
            },
            standards: ResolvedStandardsConfig {
                path: None,
                inline: Default::default(),
            },
            selections: ResolvedSelectionsConfig {
                skills: vec![],
                agents: vec![],
                mcp: vec![],
            },
            mcp_templates: Vec::new(),
            automation: macc_core::config::AutomationConfig::default(),
        }
    }

    #[test]
    fn merges_global_and_tool_specific_agents_with_stable_ordering() {
        let mut resolved = base_resolved();
        resolved.selections.agents = vec!["reviewer".to_string()];

        let claude_json = json!({
            "agents": ["architect", "prompt-engineer"]
        });
        resolved
            .tools
            .specific
            .insert("claude".to_string(), claude_json);

        let cfg = ClaudeConfig::from_resolved(&resolved);

        assert_eq!(
            cfg.agents,
            vec![
                "architect".to_string(),
                "prompt-engineer".to_string(),
                "reviewer".to_string()
            ]
        );
    }

    #[test]
    fn no_agents_when_not_selected() {
        let resolved = base_resolved();
        let cfg = ClaudeConfig::from_resolved(&resolved);
        assert!(cfg.agents.is_empty());
    }

    #[test]
    fn rules_enabled_defaults_to_false() {
        let resolved = base_resolved();
        let cfg = ClaudeConfig::from_resolved(&resolved);
        assert!(!cfg.rules_enabled);
    }
}
