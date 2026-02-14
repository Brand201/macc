use crate::map::CodexToolConfig;
use macc_adapter_shared::render::format::render_toml;
use serde_json::Value as JsonValue;
use toml::Value;

pub fn render_config_toml(config: &CodexToolConfig) -> String {
    let mut merged = Value::Table(toml::map::Map::new());
    let raw = sanitize_raw_config(&config.raw);
    if let Some(raw_toml) = json_to_toml(&raw) {
        merge_toml(&mut merged, raw_toml);
    }

    set_toml_string(&mut merged, "model", config.model.as_deref());
    set_toml_string(
        &mut merged,
        "approval_policy",
        config.approval_policy.as_deref(),
    );
    set_toml_string(&mut merged, "sandbox_mode", config.sandbox_mode.as_deref());
    set_toml_string(
        &mut merged,
        "model_reasoning_effort",
        config.model_reasoning_effort.as_deref(),
    );
    set_toml_bool(&mut merged, &["features", "undo"], config.features_undo);
    set_toml_bool(
        &mut merged,
        &["features", "shell_snapshot"],
        config.features_shell_snapshot,
    );
    set_toml_string(
        &mut merged,
        "profiles.deep-review.model",
        config.profile_deep_review_model.as_deref(),
    );
    set_toml_string(
        &mut merged,
        "profiles.deep-review.model_reasoning_effort",
        config.profile_deep_review_model_reasoning_effort.as_deref(),
    );
    set_toml_string(
        &mut merged,
        "profiles.deep-review.approval_policy",
        config.profile_deep_review_approval_policy.as_deref(),
    );

    render_toml(&merged)
}

fn set_toml_string(root: &mut Value, dotted_path: &str, value: Option<&str>) {
    let Some(value) = value else {
        return;
    };
    set_toml_value(
        root,
        &dotted_path.split('.').collect::<Vec<_>>(),
        Value::String(value.to_string()),
    );
}

fn set_toml_bool(root: &mut Value, path: &[&str], value: Option<bool>) {
    let Some(value) = value else {
        return;
    };
    set_toml_value(root, path, Value::Boolean(value));
}

fn set_toml_value(root: &mut Value, path: &[&str], value: Value) {
    if path.is_empty() {
        *root = value;
        return;
    }
    let mut current = root;
    for key in &path[..path.len() - 1] {
        if !matches!(current, Value::Table(_)) {
            *current = Value::Table(toml::map::Map::new());
        }
        let Value::Table(table) = current else {
            return;
        };
        current = table
            .entry((*key).to_string())
            .or_insert_with(|| Value::Table(toml::map::Map::new()));
    }
    if !matches!(current, Value::Table(_)) {
        *current = Value::Table(toml::map::Map::new());
    }
    if let Value::Table(table) = current {
        table.insert(path[path.len() - 1].to_string(), value);
    }
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
