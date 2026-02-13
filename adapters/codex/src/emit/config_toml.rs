use crate::map::CodexToolConfig;
use macc_adapter_shared::render::format::render_toml;
use serde_json::Value as JsonValue;
use toml::Value;

pub fn render_config_toml(config: &CodexToolConfig) -> String {
    let mut root = toml::map::Map::new();
    let model = config
        .model
        .clone()
        .unwrap_or_else(|| "gpt-5.2-codex".to_string());
    let approval_policy = config
        .approval_policy
        .clone()
        .unwrap_or_else(|| "never".to_string());
    let sandbox_mode = config
        .sandbox_mode
        .clone()
        .unwrap_or_else(|| "workspace-write".to_string());
    let model_reasoning_effort = config
        .model_reasoning_effort
        .clone()
        .unwrap_or_else(|| "medium".to_string());

    root.insert("model".to_string(), Value::String(model));
    root.insert(
        "approval_policy".to_string(),
        Value::String(approval_policy),
    );
    root.insert("sandbox_mode".to_string(), Value::String(sandbox_mode));
    root.insert(
        "model_reasoning_effort".to_string(),
        Value::String(model_reasoning_effort),
    );

    let mut features = toml::map::Map::new();
    features.insert(
        "undo".to_string(),
        Value::Boolean(config.features_undo.unwrap_or(true)),
    );
    features.insert(
        "shell_snapshot".to_string(),
        Value::Boolean(config.features_shell_snapshot.unwrap_or(false)),
    );
    root.insert("features".to_string(), Value::Table(features));

    let mut profile = toml::map::Map::new();
    let profile_model = config
        .profile_deep_review_model
        .clone()
        .unwrap_or_else(|| "gpt-5.2-codex".to_string());
    let profile_model_reasoning = config
        .profile_deep_review_model_reasoning_effort
        .clone()
        .unwrap_or_else(|| "medium".to_string());
    let profile_approval_policy = config
        .profile_deep_review_approval_policy
        .clone()
        .unwrap_or_else(|| "never".to_string());

    profile.insert("model".to_string(), Value::String(profile_model));
    profile.insert(
        "model_reasoning_effort".to_string(),
        Value::String(profile_model_reasoning),
    );
    profile.insert(
        "approval_policy".to_string(),
        Value::String(profile_approval_policy),
    );

    let mut profiles = toml::map::Map::new();
    profiles.insert("deep-review".to_string(), Value::Table(profile));
    root.insert("profiles".to_string(), Value::Table(profiles));

    let mut merged = Value::Table(root);
    let raw = sanitize_raw_config(&config.raw);
    if let Some(raw_toml) = json_to_toml(&raw) {
        merge_toml(&mut merged, raw_toml);
    }

    render_toml(&merged)
}

fn sanitize_raw_config(raw: &JsonValue) -> JsonValue {
    let mut value = raw.clone();
    let JsonValue::Object(map) = &mut value else {
        return value;
    };

    map.remove("skills");
    map.remove("agents");
    map.remove("rules_enabled");

    if let Some(JsonValue::Object(features)) = map.get_mut("features") {
        features.remove("web_search_request");
    }

    value
}

fn json_to_toml(value: &JsonValue) -> Option<Value> {
    match value {
        JsonValue::Null => None,
        JsonValue::Bool(v) => Some(Value::Boolean(*v)),
        JsonValue::Number(v) => {
            if let Some(i) = v.as_i64() {
                Some(Value::Integer(i))
            } else {
                v.as_f64().map(Value::Float)
            }
        }
        JsonValue::String(v) => Some(Value::String(v.clone())),
        JsonValue::Array(items) => {
            let mut out = Vec::new();
            for item in items {
                if let Some(val) = json_to_toml(item) {
                    out.push(val);
                }
            }
            Some(Value::Array(out))
        }
        JsonValue::Object(obj) => {
            let mut table = toml::map::Map::new();
            for (key, val) in obj {
                if let Some(converted) = json_to_toml(val) {
                    table.insert(key.clone(), converted);
                }
            }
            Some(Value::Table(table))
        }
    }
}

fn merge_toml(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Table(base_map), Value::Table(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(&key) {
                    Some(existing) => merge_toml(existing, value),
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
