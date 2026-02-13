use crate::map::ClaudeConfig;
use macc_adapter_shared::render::format::render_json_pretty;
use serde_json::{json, Value as JsonValue};

pub fn render_settings_json(config: &ClaudeConfig) -> String {
    let (deny, allow) = match config.permissions.as_str() {
        "strict" => (
            vec!["Read(./.env*)", "Read(./secrets/**)", "Bash(*)"],
            vec!["Bash(git status:*)", "Bash(git diff:*)"],
        ),
        "dev" => (
            vec!["Read(./.env*)", "Read(./secrets/**)"],
            vec!["Bash(*)", "Read(*)", "Write(*)"],
        ),
        _ => (
            // "safe" is default
            vec!["Read(./.env*)", "Read(./secrets/**)"],
            vec![
                "Bash(pnpm:*)",
                "Bash(git status:*)",
                "Bash(git diff:*)",
                "Bash(git log:*)",
            ],
        ),
    };

    let mut settings = json!({
        "language": config.language,
        "permissions": {
            "deny": deny,
            "allow": allow
        }
    });

    let raw = sanitize_raw_config(&config.tool_config);
    merge_json(&mut settings, raw);

    render_json_pretty(&settings)
}

fn sanitize_raw_config(raw: &JsonValue) -> JsonValue {
    let mut value = raw.clone();
    let JsonValue::Object(map) = &mut value else {
        return value;
    };

    map.remove("permissions");
    map.remove("skills");
    map.remove("agents");
    map.remove("rules_enabled");
    map.remove("user_mcp_merge");

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
