use crate::tool::ToolSpecLoader;
use crate::ProjectPaths;
use serde_json::Value as JsonValue;
use serde_yaml::Value as YamlValue;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Default)]
pub struct StructuredToolMergePolicy {
    managed_prefixes: Vec<String>,
}

impl StructuredToolMergePolicy {
    pub fn from_project(paths: &ProjectPaths) -> Self {
        let search_paths = ToolSpecLoader::default_search_paths(&paths.root);
        let loader = ToolSpecLoader::new(search_paths);
        let (specs, _) = loader.load_all_with_embedded();

        let mut prefixes = BTreeSet::new();
        for spec in specs {
            for entry in spec.gitignore {
                if let Some(prefix) = normalize_managed_prefix(&entry) {
                    prefixes.insert(prefix);
                }
            }
        }

        Self {
            managed_prefixes: prefixes.into_iter().collect(),
        }
    }

    pub fn should_merge_path(&self, path: &str) -> bool {
        if !is_structured_extension(path) {
            return false;
        }
        let normalized = normalize_path(path);
        self.managed_prefixes
            .iter()
            .any(|prefix| normalized.starts_with(prefix))
    }

    pub fn merge_bytes_for_path(
        &self,
        path: &str,
        existing: Option<&[u8]>,
        desired: &[u8],
    ) -> Vec<u8> {
        if !self.should_merge_path(path) {
            return desired.to_vec();
        }
        let Some(existing_bytes) = existing else {
            return desired.to_vec();
        };
        if existing_bytes.is_empty() {
            return desired.to_vec();
        }

        match extension(path) {
            Some("json") => {
                merge_json_bytes(existing_bytes, desired).unwrap_or_else(|| desired.to_vec())
            }
            Some("toml") => {
                merge_toml_bytes(existing_bytes, desired).unwrap_or_else(|| desired.to_vec())
            }
            Some("yaml") | Some("yml") => {
                merge_yaml_bytes(existing_bytes, desired).unwrap_or_else(|| desired.to_vec())
            }
            _ => desired.to_vec(),
        }
    }
}

fn normalize_managed_prefix(entry: &str) -> Option<String> {
    let normalized = normalize_path(entry);
    if normalized.ends_with('/') {
        Some(normalized)
    } else {
        None
    }
}

fn normalize_path(path: &str) -> String {
    let mut value = path.replace('\\', "/");
    while value.starts_with("./") {
        value = value.trim_start_matches("./").to_string();
    }
    value
}

fn extension(path: &str) -> Option<&str> {
    path.rsplit_once('.').map(|(_, ext)| ext)
}

fn is_structured_extension(path: &str) -> bool {
    matches!(
        extension(path).map(|ext| ext.to_ascii_lowercase()),
        Some(ext) if ext == "json" || ext == "toml" || ext == "yaml" || ext == "yml"
    )
}

fn merge_json_bytes(existing: &[u8], desired: &[u8]) -> Option<Vec<u8>> {
    let mut base: JsonValue = serde_json::from_slice(existing).ok()?;
    let overlay: JsonValue = serde_json::from_slice(desired).ok()?;
    deep_merge_json(&mut base, &overlay);
    let mut out = serde_json::to_vec_pretty(&base).ok()?;
    if !out.ends_with(b"\n") {
        out.push(b'\n');
    }
    Some(out)
}

fn deep_merge_json(base: &mut JsonValue, overlay: &JsonValue) {
    match (base, overlay) {
        (JsonValue::Object(base_map), JsonValue::Object(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(key) {
                    Some(existing) => deep_merge_json(existing, value),
                    None => {
                        base_map.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (slot, value) => {
            *slot = value.clone();
        }
    }
}

fn merge_toml_bytes(existing: &[u8], desired: &[u8]) -> Option<Vec<u8>> {
    let existing_text = std::str::from_utf8(existing).ok()?;
    let desired_text = std::str::from_utf8(desired).ok()?;
    let mut base: toml::Value = toml::from_str(existing_text).ok()?;
    let overlay: toml::Value = toml::from_str(desired_text).ok()?;
    deep_merge_toml(&mut base, overlay);
    let mut rendered = toml::to_string_pretty(&base).ok()?;
    if !rendered.ends_with('\n') {
        rendered.push('\n');
    }
    Some(rendered.into_bytes())
}

fn deep_merge_toml(base: &mut toml::Value, overlay: toml::Value) {
    match (base, overlay) {
        (toml::Value::Table(base_map), toml::Value::Table(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(&key) {
                    Some(existing) => deep_merge_toml(existing, value),
                    None => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
        (slot, value) => {
            *slot = value;
        }
    }
}

fn merge_yaml_bytes(existing: &[u8], desired: &[u8]) -> Option<Vec<u8>> {
    let mut base: YamlValue = serde_yaml::from_slice(existing).ok()?;
    let overlay: YamlValue = serde_yaml::from_slice(desired).ok()?;
    deep_merge_yaml(&mut base, &overlay);
    let mut out = serde_yaml::to_string(&base).ok()?.into_bytes();
    if !out.ends_with(b"\n") {
        out.push(b'\n');
    }
    Some(out)
}

fn deep_merge_yaml(base: &mut YamlValue, overlay: &YamlValue) {
    match (base, overlay) {
        (YamlValue::Mapping(base_map), YamlValue::Mapping(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(key) {
                    Some(existing) => deep_merge_yaml(existing, value),
                    None => {
                        base_map.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (slot, value) => {
            *slot = value.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StructuredToolMergePolicy;

    #[test]
    fn merges_json_and_preserves_user_keys() {
        let policy = StructuredToolMergePolicy {
            managed_prefixes: vec![".toolx/".to_string()],
        };
        let existing = br#"{
  "tools": {
    "approvalMode": "yolo",
    "userExtra": true
  },
  "manualOnly": "keep"
}"#;
        let desired = br#"{
  "tools": {
    "approvalMode": "default"
  }
}"#;

        let merged = policy.merge_bytes_for_path(".toolx/settings.json", Some(existing), desired);
        let value: serde_json::Value = serde_json::from_slice(&merged).unwrap();
        assert_eq!(
            value
                .pointer("/tools/approvalMode")
                .and_then(|v| v.as_str()),
            Some("default")
        );
        assert_eq!(
            value.pointer("/tools/userExtra").and_then(|v| v.as_bool()),
            Some(true)
        );
        assert_eq!(
            value.pointer("/manualOnly").and_then(|v| v.as_str()),
            Some("keep")
        );
    }

    #[test]
    fn ignores_non_tool_paths() {
        let policy = StructuredToolMergePolicy {
            managed_prefixes: vec![".toolx/".to_string()],
        };
        let desired = br#"{"a":1}"#;
        let merged = policy.merge_bytes_for_path("README.md", Some(br#"{"a":2}"#), desired);
        assert_eq!(merged, desired);
    }
}
