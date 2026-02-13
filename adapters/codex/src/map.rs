use macc_core::resolve::ResolvedConfig;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct CodexConfig {
    pub standards_inline: BTreeMap<String, String>,
    pub standards_path: Option<String>,
    pub skills: Vec<String>,
    pub tool_config: CodexToolConfig,
}

#[derive(Debug, Clone, Default)]
pub struct CodexToolConfig {
    pub model: Option<String>,
    pub model_reasoning_effort: Option<String>,
    pub approval_policy: Option<String>,
    pub sandbox_mode: Option<String>,
    pub features_undo: Option<bool>,
    pub features_shell_snapshot: Option<bool>,
    pub profile_deep_review_model: Option<String>,
    pub profile_deep_review_model_reasoning_effort: Option<String>,
    pub profile_deep_review_approval_policy: Option<String>,
    pub rules_enabled: Option<bool>,
    pub raw: serde_json::Value,
}

impl CodexConfig {
    pub fn from_resolved(resolved: &ResolvedConfig) -> Self {
        let tool_config = extract_tool_config(resolved);
        let mut skills_set = BTreeSet::new();
        for skill in &resolved.selections.skills {
            skills_set.insert(skill.clone());
        }
        let tool_skills = resolved
            .tools
            .config
            .get("codex")
            .or_else(|| resolved.tools.specific.get("codex"))
            .and_then(|value| get_string_list(value, &["skills"]))
            .unwrap_or_default();
        for skill in tool_skills {
            skills_set.insert(skill);
        }

        Self {
            standards_inline: resolved.standards.inline.clone(),
            standards_path: resolved.standards.path.clone(),
            skills: skills_set.into_iter().collect(),
            tool_config,
        }
    }
}

fn extract_tool_config(resolved: &ResolvedConfig) -> CodexToolConfig {
    let tool_config = resolved
        .tools
        .config
        .get("codex")
        .or_else(|| resolved.tools.specific.get("codex"));

    let Some(tool_config) = tool_config else {
        return CodexToolConfig {
            raw: serde_json::Value::Object(Default::default()),
            ..CodexToolConfig::default()
        };
    };

    CodexToolConfig {
        raw: tool_config.clone(),
        model: get_string(tool_config, &["model"]),
        model_reasoning_effort: get_string(tool_config, &["model_reasoning_effort"]),
        approval_policy: get_string(tool_config, &["approval_policy"]),
        sandbox_mode: get_string(tool_config, &["sandbox_mode"]),
        features_undo: get_bool(tool_config, &["features", "undo"]),
        features_shell_snapshot: get_bool(tool_config, &["features", "shell_snapshot"]),
        profile_deep_review_model: get_string(tool_config, &["profiles", "deep-review", "model"]),
        profile_deep_review_model_reasoning_effort: get_string(
            tool_config,
            &["profiles", "deep-review", "model_reasoning_effort"],
        ),
        profile_deep_review_approval_policy: get_string(
            tool_config,
            &["profiles", "deep-review", "approval_policy"],
        ),
        rules_enabled: get_bool(tool_config, &["rules_enabled"]),
    }
}

fn get_string(root: &Value, path: &[&str]) -> Option<String> {
    let mut current = root;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str().map(|s| s.to_string())
}

fn get_bool(root: &Value, path: &[&str]) -> Option<bool> {
    let mut current = root;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_bool()
}

fn get_string_list(root: &Value, path: &[&str]) -> Option<Vec<String>> {
    let mut current = root;
    for key in path {
        current = current.get(*key)?;
    }
    match current {
        Value::Array(values) => {
            let mut out = Vec::new();
            for val in values {
                if let Some(s) = val.as_str() {
                    out.push(s.to_string());
                }
            }
            Some(out)
        }
        Value::String(s) => {
            let list = s
                .split(',')
                .map(|entry| entry.trim().to_string())
                .filter(|entry| !entry.is_empty())
                .collect::<Vec<_>>();
            Some(list)
        }
        _ => None,
    }
}
