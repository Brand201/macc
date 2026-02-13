pub fn ensure_trailing_newline(mut content: String) -> String {
    if !content.ends_with('\n') {
        content.push('\n');
    }
    content
}

pub fn render_json_pretty(value: &serde_json::Value) -> String {
    let rendered = serde_json::to_string_pretty(value).unwrap_or_else(|_| "{}".into());
    ensure_trailing_newline(rendered)
}

pub fn render_toml(value: &toml::Value) -> String {
    let rendered = toml::to_string(value).unwrap_or_default();
    ensure_trailing_newline(rendered)
}
