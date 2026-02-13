use crate::map::GeminiConfig;
use macc_adapter_shared::render::format::render_json_pretty;
use serde_json::{json, Map as JsonMap, Value as JsonValue};

pub fn render_settings_json(config: &GeminiConfig) -> String {
    let has_skills = !config.skills.is_empty() || !config.agents.is_empty();

    let mut settings = json!(
        {
            "hasSeenIdeIntegrationNudge": true,
            "ide": {
                "enabled": true
            },
            "security": {
                "disableYoloMode": false,
            },
            "general": {
                "previewFeatures": true
            },
            "model": {
                "name": "auto-gemini-3"
            },
            "privacy": {
                "usageStatisticsEnabled": true
            },
            "tools": {
                "approvalMode": "default",
            },
            "experimental": {
                "skills": has_skills,
                "plan": false
            }
        }
    );

    let raw = sanitize_raw_config(&config.tool_config);
    merge_json(&mut settings, raw);

    set_if_present(
        &mut settings,
        &config.tool_config,
        "/general/vimMode",
        "/general/vimMode",
        false,
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/general/preferredEditor",
        "/general/preferredEditor",
        false,
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/general/sessionRetention/enabled",
        "/general/sessionRetention/enabled",
        false,
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/general/sessionRetention/maxAge",
        "/general/sessionRetention/maxAge",
        false,
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/general/sessionRetention/maxCount",
        "/general/sessionRetention/maxCount",
        false,
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/ui/theme",
        "/ui/theme",
        false,
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/ui/hideBanner",
        "/ui/hideBanner",
        false,
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/ui/hideTips",
        "/ui/hideTips",
        false,
    );
    set_array_if_present(
        &mut settings,
        &config.tool_config,
        "/ui/customWittyPhrases",
        "/ui/customWittyPhrases",
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/tools/sandbox",
        "/tools/sandbox",
        false,
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/tools/discoveryCommand",
        "/tools/discoveryCommand",
        false,
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/tools/callCommand",
        "/tools/callCommand",
        false,
    );
    set_array_if_present(
        &mut settings,
        &config.tool_config,
        "/tools/exclude",
        "/tools/exclude",
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/telemetry/enabled",
        "/telemetry/enabled",
        false,
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/telemetry/target",
        "/telemetry/target",
        false,
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/telemetry/otlpEndpoint",
        "/telemetry/otlpEndpoint",
        false,
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/telemetry/logPrompts",
        "/telemetry/logPrompts",
        false,
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/privacy/usageStatisticsEnabled",
        "/privacy/usageStatisticsEnabled",
        false,
    );
    let model_name = config
        .tool_config
        .pointer("/model/name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            config
                .tool_config
                .pointer("/model")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        });
    if let Some(name) = model_name {
        let _ = set_json_pointer(&mut settings, "/model/name", JsonValue::String(name));
    }
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/model/maxSessionTurns",
        "/model/maxSessionTurns",
        false,
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/model/summarizeToolOutput",
        "/model/summarizeToolOutput",
        true,
    );
    set_array_if_present(
        &mut settings,
        &config.tool_config,
        "/context/fileName",
        "/context/fileName",
    );
    set_array_if_present(
        &mut settings,
        &config.tool_config,
        "/context/includeDirectories",
        "/context/includeDirectories",
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/context/loadFromIncludeDirectories",
        "/context/loadFromIncludeDirectories",
        false,
    );
    set_if_present(
        &mut settings,
        &config.tool_config,
        "/context/fileFiltering/respectGitIgnore",
        "/context/fileFiltering/respectGitIgnore",
        false,
    );
    set_array_if_present(
        &mut settings,
        &config.tool_config,
        "/advanced/excludedEnvVars",
        "/advanced/excludedEnvVars",
    );

    let mut mcp_servers = match read_json_value(&config.tool_config, "/mcpServers") {
        Some(JsonValue::Object(map)) => map,
        _ => JsonMap::new(),
    };
    for (key, value) in &config.mcp_servers {
        mcp_servers.insert(key.clone(), value.clone());
    }
    let _ = set_json_pointer(&mut settings, "/mcpServers", JsonValue::Object(mcp_servers));

    render_json_pretty(&settings)
}

fn set_if_present(
    settings: &mut JsonValue,
    config: &JsonValue,
    source_pointer: &str,
    target_pointer: &str,
    parse_json: bool,
) {
    let value = if parse_json {
        read_json_value(config, source_pointer)
    } else {
        config.pointer(source_pointer).cloned()
    };
    if let Some(value) = value {
        let _ = set_json_pointer(settings, target_pointer, value);
    }
}

fn set_array_if_present(
    settings: &mut JsonValue,
    config: &JsonValue,
    source_pointer: &str,
    target_pointer: &str,
) {
    let value = read_array_value(config, source_pointer);
    if let Some(value) = value {
        let _ = set_json_pointer(settings, target_pointer, value);
    }
}

fn read_json_value(value: &JsonValue, pointer: &str) -> Option<JsonValue> {
    let raw = value.pointer(pointer)?;
    if let Some(text) = raw.as_str() {
        let trimmed = text.trim();
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            if let Ok(parsed) = serde_json::from_str::<JsonValue>(trimmed) {
                return Some(parsed);
            }
        }
        return Some(JsonValue::String(text.to_string()));
    }
    Some(raw.clone())
}

fn read_array_value(value: &JsonValue, pointer: &str) -> Option<JsonValue> {
    let raw = value.pointer(pointer)?;
    if let Some(arr) = raw.as_array() {
        return Some(JsonValue::Array(arr.clone()));
    }
    if let Some(text) = raw.as_str() {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Some(JsonValue::Array(Vec::new()));
        }
        if trimmed.starts_with('[') {
            if let Ok(parsed) = serde_json::from_str::<JsonValue>(trimmed) {
                if parsed.is_array() {
                    return Some(parsed);
                }
            }
        }
        let items = parse_csv_list(trimmed)
            .into_iter()
            .map(JsonValue::String)
            .collect();
        return Some(JsonValue::Array(items));
    }
    None
}

fn set_json_pointer(root: &mut JsonValue, pointer: &str, new_value: JsonValue) -> Result<(), ()> {
    if pointer.is_empty() {
        return Ok(());
    }
    let tokens = pointer
        .trim_start_matches('/')
        .split('/')
        .map(decode_pointer_token)
        .collect::<Vec<_>>();

    let mut current = root;
    for (idx, token) in tokens.iter().enumerate() {
        let is_last = idx == tokens.len() - 1;
        match current {
            JsonValue::Object(map) => {
                if is_last {
                    map.insert(token.clone(), new_value);
                    return Ok(());
                }
                current = map
                    .entry(token.clone())
                    .or_insert_with(|| JsonValue::Object(JsonMap::new()));
            }
            _ => return Err(()),
        }
    }
    Ok(())
}

fn decode_pointer_token(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

fn parse_csv_list(value: &str) -> Vec<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    trimmed
        .split(',')
        .map(|entry| entry.trim())
        .filter(|entry| !entry.is_empty())
        .map(|entry| entry.to_string())
        .collect()
}

fn sanitize_raw_config(raw: &JsonValue) -> JsonValue {
    let mut value = raw.clone();
    let JsonValue::Object(map) = &mut value else {
        return value;
    };

    map.remove("skills");
    map.remove("agents");
    map.remove("user_mcp_merge");
    map.remove("mcpServers");

    if let Some(JsonValue::String(model)) = map.get("model") {
        let name = model.clone();
        let mut obj = JsonMap::new();
        obj.insert("name".to_string(), JsonValue::String(name));
        map.insert("model".to_string(), JsonValue::Object(obj));
    }

    value
}

fn merge_json(base: &mut JsonValue, overlay: JsonValue) {
    match (base, overlay) {
        (JsonValue::Object(base_map), JsonValue::Object(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(&key) {
                    Some(existing) => merge_json(existing, value),
                    None => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
        (base_slot, overlay_value) => {
            *base_slot = overlay_value;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn test_render_settings_json_with_mcp() {
        let mut mcp_servers = BTreeMap::new();
        mcp_servers.insert(
            "test-server".to_string(),
            json!({
                "command": "node",
                "args": ["test.js"],
                "env": {
                    "API_KEY": "${API_KEY}"
                }
            }),
        );

        let config = GeminiConfig {
            standards_inline: BTreeMap::new(),
            standards_path: None,
            skills: vec![],
            agents: vec![],
            user_mcp_merge: false,
            mcp_servers,
            tool_config: JsonValue::Object(JsonMap::new()),
        };

        let output = render_settings_json(&config);
        assert!(output.contains("\"mcpServers\": {"));
        assert!(output.contains("\"test-server\": {"));
        assert!(output.contains("\"command\": \"node\""));
        assert!(output.contains("\"API_KEY\": \"${API_KEY}\""));
    }
}
