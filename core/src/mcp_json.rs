use crate::config::McpTemplateDefinition;
use crate::resolve::ResolvedConfig;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

pub fn render_project_mcp_json(resolved: &ResolvedConfig) -> Option<String> {
    let selection_ids: BTreeSet<String> = resolved.selections.mcp.iter().cloned().collect();
    if selection_ids.is_empty() {
        return None;
    }

    let mut servers = BTreeMap::new();
    for template in &resolved.mcp_templates {
        if selection_ids.contains(&template.id) {
            servers.insert(template.id.clone(), template_to_value(template));
        }
    }

    if servers.is_empty() {
        return None;
    }

    Some(render_mcp_json(&servers))
}

pub fn template_to_value(template: &McpTemplateDefinition) -> Value {
    let mut env_placeholders: Vec<&crate::config::McpEnvPlaceholder> =
        template.env_placeholders.iter().collect();
    env_placeholders.sort_by(|a, b| a.name.cmp(&b.name));

    let mut env = Map::new();
    for placeholder in env_placeholders {
        env.insert(
            placeholder.name.clone(),
            Value::String(placeholder.placeholder.clone()),
        );
    }

    let args = Value::Array(
        template
            .args
            .iter()
            .map(|arg| Value::String(arg.clone()))
            .collect(),
    );

    let mut entry = Map::new();
    entry.insert(
        "command".to_string(),
        Value::String(template.command.clone()),
    );
    entry.insert("args".to_string(), args);
    entry.insert("env".to_string(), Value::Object(env));

    Value::Object(entry)
}

pub fn render_mcp_json(servers: &BTreeMap<String, Value>) -> String {
    let mut doc_servers = Map::new();
    for (id, value) in servers {
        doc_servers.insert(id.clone(), value.clone());
    }

    let mut doc = Map::new();
    doc.insert("mcpServers".to_string(), Value::Object(doc_servers));

    render_json_pretty(&Value::Object(doc))
}

fn render_json_pretty(value: &Value) -> String {
    let mut rendered = serde_json::to_string_pretty(value).unwrap_or_else(|_| "{}".to_string());
    if !rendered.ends_with('\n') {
        rendered.push('\n');
    }
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{McpEnvPlaceholder, McpTemplateDefinition};
    use crate::resolve::{
        ResolvedConfig, ResolvedSelectionsConfig, ResolvedStandardsConfig, ResolvedToolsConfig,
    };

    #[test]
    fn render_project_mcp_json_outputs_expected_golden() {
        let templates = vec![
            McpTemplateDefinition {
                id: "brave-search".into(),
                title: "Brave Search".into(),
                description: "Brave Search MCP".into(),
                command: "node".into(),
                args: vec!["scripts/brave-search-mcp.js".into()],
                env_placeholders: vec![McpEnvPlaceholder {
                    name: "BRAVE_API_KEY".into(),
                    placeholder: "${BRAVE_API_KEY}".into(),
                    description: Some(
                        "Provide the Brave Search key via env placeholder before running.".into(),
                    ),
                }],
                auth_notes: Some(
                    "Set BRAVE_API_KEY locally; MACC keeps only the placeholder.".into(),
                ),
            },
            McpTemplateDefinition {
                id: "local-notes".into(),
                title: "Local Notes".into(),
                description: "Local notes reader".into(),
                command: "bash".into(),
                args: vec![
                    "scripts/local-notes.sh".into(),
                    "--dir".into(),
                    "./notes".into(),
                ],
                env_placeholders: Vec::new(),
                auth_notes: Some("Reads the repo notes directory with no auth.".into()),
            },
        ];

        let resolved = ResolvedConfig {
            version: "v1".into(),
            tools: ResolvedToolsConfig::default(),
            standards: ResolvedStandardsConfig {
                path: None,
                inline: Default::default(),
            },
            selections: ResolvedSelectionsConfig {
                skills: Vec::new(),
                agents: Vec::new(),
                mcp: vec!["brave-search".into(), "local-notes".into()],
            },
            mcp_templates: templates.clone(),
            automation: crate::config::AutomationConfig::default(),
        };

        let output = render_project_mcp_json(&resolved).unwrap();
        let expected = r#"{
  "mcpServers": {
    "brave-search": {
      "args": [
        "scripts/brave-search-mcp.js"
      ],
      "command": "node",
      "env": {
        "BRAVE_API_KEY": "${BRAVE_API_KEY}"
      }
    },
    "local-notes": {
      "args": [
        "scripts/local-notes.sh",
        "--dir",
        "./notes"
      ],
      "command": "bash",
      "env": {}
    }
  }
}
"#;
        assert_eq!(output, expected);
    }
}
