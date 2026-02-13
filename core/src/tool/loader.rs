use crate::tool::ToolSpec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolDiagnostic {
    pub path: PathBuf,
    pub error: String,
    pub line: Option<usize>,
    pub column: Option<usize>,
}

pub struct ToolSpecLoader {
    search_paths: Vec<PathBuf>,
}

impl ToolSpecLoader {
    pub fn new(search_paths: Vec<PathBuf>) -> Self {
        Self { search_paths }
    }

    /// Returns override search paths in order of INCREASING precedence:
    /// 1. User: ~/.config/macc/tools.d
    /// 2. Project: <project_root>/.macc/tools.d
    ///
    /// Built-in specs are embedded in the binary and loaded separately.
    pub fn default_search_paths(project_root: &Path) -> Vec<PathBuf> {
        let mut paths = Vec::new();

        // 1. User
        if let Some(home) = crate::find_user_home() {
            paths.push(home.join(".config/macc/tools.d"));
        }

        // 2. Project
        paths.push(project_root.join(".macc/tools.d"));

        paths
    }

    pub fn load_all_with_embedded(&self) -> (Vec<ToolSpec>, Vec<ToolDiagnostic>) {
        let (embedded_specs, mut diagnostics) = embedded_tool_specs();
        let mut specs: BTreeMap<String, ToolSpec> = embedded_specs
            .into_iter()
            .map(|s| (s.id.clone(), s))
            .collect();

        let (file_specs, file_diags) = self.load_all();
        diagnostics.extend(file_diags);
        for spec in file_specs {
            specs.insert(spec.id.clone(), spec);
        }

        (specs.into_values().collect(), diagnostics)
    }

    pub fn load_all(&self) -> (Vec<ToolSpec>, Vec<ToolDiagnostic>) {
        let mut specs = BTreeMap::new();
        let mut diagnostics = Vec::new();

        for dir in &self.search_paths {
            if !dir.is_dir() {
                continue;
            }

            let entries = match fs::read_dir(dir) {
                Ok(entries) => entries,
                Err(e) => {
                    diagnostics.push(ToolDiagnostic {
                        path: dir.clone(),
                        error: format!("Failed to read directory: {}", e),
                        line: None,
                        column: None,
                    });
                    continue;
                }
            };

            // To ensure determinism within a directory, we sort entries
            let mut entries: Vec<_> = entries.filter_map(|e| e.ok()).collect();
            entries.sort_by_key(|e| e.file_name());

            for entry in entries {
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }

                let is_tool_spec = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .map(|s| s.ends_with(".tool.yaml") || s.ends_with(".tool.json"))
                    .unwrap_or(false);

                if !is_tool_spec {
                    continue;
                }

                let content = match fs::read_to_string(&path) {
                    Ok(content) => content,
                    Err(e) => {
                        diagnostics.push(ToolDiagnostic {
                            path: path.clone(),
                            error: format!("Failed to read file: {}", e),
                            line: None,
                            column: None,
                        });
                        continue;
                    }
                };

                let spec_result = if path.to_string_lossy().ends_with(".json") {
                    ToolSpec::from_json(&content)
                } else {
                    ToolSpec::from_yaml(&content)
                };

                match spec_result {
                    Ok(spec) => {
                        specs.insert(spec.id.clone(), spec);
                    }
                    Err(e) => {
                        let mut diag = ToolDiagnostic {
                            path: path.clone(),
                            error: e.to_string(),
                            line: None,
                            column: None,
                        };

                        if let crate::MaccError::ToolSpec {
                            line,
                            column,
                            message,
                            ..
                        } = e
                        {
                            diag.line = line;
                            diag.column = column;
                            diag.error = message;
                        }

                        diagnostics.push(diag);
                    }
                }
            }
        }

        let sorted_specs = specs.into_values().collect();
        (sorted_specs, diagnostics)
    }
}

fn embedded_tool_specs() -> (Vec<ToolSpec>, Vec<ToolDiagnostic>) {
    let mut specs = Vec::new();
    let mut diags = Vec::new();
    let embedded = [
        (
            "embedded:claude.tool.yaml",
            include_str!("../../../registry/tools.d/claude.tool.yaml"),
        ),
        (
            "embedded:codex.tool.yaml",
            include_str!("../../../registry/tools.d/codex.tool.yaml"),
        ),
        (
            "embedded:gemini.tool.yaml",
            include_str!("../../../registry/tools.d/gemini.tool.yaml"),
        ),
    ];

    for (name, content) in embedded {
        match ToolSpec::from_yaml(content) {
            Ok(spec) => specs.push(spec),
            Err(e) => diags.push(ToolDiagnostic {
                path: PathBuf::from(name),
                error: e.to_string(),
                line: None,
                column: None,
            }),
        }
    }

    (specs, diags)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn create_temp_dir(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("macc_loader_test_{}_{}", name, uuid_v4_like()));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn uuid_v4_like() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{:x}", nanos)
    }

    fn ordered_tool_ids() -> (String, String, String) {
        let suffix = uuid_v4_like();
        (
            format!("a-{}", suffix),
            format!("b-{}", suffix),
            format!("c-{}", suffix),
        )
    }

    #[test]
    fn test_loader_precedence() {
        let dir1 = create_temp_dir("p1");
        let dir2 = create_temp_dir("p2");

        let tool1_v1 = r#"
api_version: v1
id: tool1
display_name: Tool 1 V1
fields: []
"#;
        let tool1_v2 = r#"
api_version: v1
id: tool1
display_name: Tool 1 V2
fields: []
"#;
        let tool2 = r#"
api_version: v1
id: tool2
display_name: Tool 2
fields: []
"#;

        fs::write(dir1.join("tool1.tool.yaml"), tool1_v1).unwrap();
        fs::write(dir1.join("tool2.tool.yaml"), tool2).unwrap();
        fs::write(dir2.join("tool1.tool.yaml"), tool1_v2).unwrap();

        let loader = ToolSpecLoader::new(vec![dir1.to_path_buf(), dir2.to_path_buf()]);
        let (specs, diags) = loader.load_all();

        assert!(diags.is_empty(), "Diagnostics should be empty: {:?}", diags);
        assert_eq!(specs.len(), 2);

        let t1 = specs.iter().find(|s| s.id == "tool1").unwrap();
        let t2 = specs.iter().find(|s| s.id == "tool2").unwrap();

        assert_eq!(t1.display_name, "Tool 1 V2"); // Overridden by dir2
        assert_eq!(t2.display_name, "Tool 2"); // From dir1

        fs::remove_dir_all(&dir1).ok();
        fs::remove_dir_all(&dir2).ok();
    }

    #[test]
    fn test_loader_json_and_yaml() {
        let dir = create_temp_dir("json_yaml");

        let yaml_tool = r#"
api_version: v1
id: yaml-tool
display_name: YAML Tool
fields: []
"#;
        let json_tool = r#"{
  "api_version": "v1",
  "id": "json-tool",
  "display_name": "JSON Tool",
  "fields": []
}"#;

        fs::write(dir.join("a.tool.yaml"), yaml_tool).unwrap();
        fs::write(dir.join("b.tool.json"), json_tool).unwrap();

        let loader = ToolSpecLoader::new(vec![dir.to_path_buf()]);
        let (specs, diags) = loader.load_all();

        assert!(diags.is_empty(), "Diagnostics should be empty: {:?}", diags);
        assert_eq!(specs.len(), 2);
        assert!(specs.iter().any(|s| s.id == "yaml-tool"));
        assert!(specs.iter().any(|s| s.id == "json-tool"));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_loader_append_and_determinism() {
        let dir1 = create_temp_dir("a1");
        let dir2 = create_temp_dir("a2");

        let (tool_a_id, tool_b_id, tool_c_id) = ordered_tool_ids();
        let tool_c = format!(
            r#"
api_version: v1
id: {tool_c_id}
display_name: Tool C
fields: []
"#
        );
        let tool_a = format!(
            r#"
api_version: v1
id: {tool_a_id}
display_name: Tool A
fields: []
"#
        );
        let tool_b = format!(
            r#"
api_version: v1
id: {tool_b_id}
display_name: Tool B
fields: []
"#
        );

        // Dir 1 has C and A
        fs::write(dir1.join("c.tool.yaml"), tool_c).unwrap();
        fs::write(dir1.join("a.tool.yaml"), tool_a).unwrap();

        // Dir 2 has B
        fs::write(dir2.join("b.tool.yaml"), tool_b).unwrap();

        let loader = ToolSpecLoader::new(vec![dir1.to_path_buf(), dir2.to_path_buf()]);
        let (specs, diags) = loader.load_all();

        assert!(diags.is_empty());
        assert_eq!(specs.len(), 3);

        // Should be sorted by ID: a-, b-, c-
        assert_eq!(specs[0].id, tool_a_id);
        assert_eq!(specs[1].id, tool_b_id);
        assert_eq!(specs[2].id, tool_c_id);

        fs::remove_dir_all(&dir1).ok();
        fs::remove_dir_all(&dir2).ok();
    }

    #[test]
    fn test_loader_diagnostics() {
        let dir = create_temp_dir("diag");

        let invalid_tool = "api_version: v1\nid: test\ndisplay_name: Test\nfields:\n  - id: f1\n    label: L\n    kind:\n      type: enum\n      options: []"; // Empty enum options is a validation error
        fs::write(dir.join("invalid.tool.yaml"), invalid_tool).unwrap();

        let loader = ToolSpecLoader::new(vec![dir.to_path_buf()]);
        let (specs, diags) = loader.load_all();

        assert_eq!(specs.len(), 0);
        assert_eq!(diags.len(), 1);
        assert!(diags[0]
            .path
            .to_string_lossy()
            .contains("invalid.tool.yaml"));
        assert!(
            diags[0].error.contains("must have at least one option"),
            "Error was: {}",
            diags[0].error
        );

        // Syntax error test
        let syntax_error = "api_version: v1\ninvalid: [";
        fs::write(dir.join("syntax.tool.yaml"), syntax_error).unwrap();

        let (specs, diags) = loader.load_all();
        assert_eq!(specs.len(), 0);
        assert_eq!(diags.len(), 2);

        let syntax_diag = diags
            .iter()
            .find(|d| d.path.to_string_lossy().contains("syntax.tool.yaml"))
            .unwrap();
        assert!(syntax_diag.line.is_some());
        assert!(syntax_diag.column.is_some());

        fs::remove_dir_all(&dir).ok();
    }
}
