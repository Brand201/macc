use crate::MaccError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ActionSpec {
    OpenMcp { target_pointer: String },
    OpenSkills { target_pointer: String },
    OpenAgents { target_pointer: String },
    Custom { target: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FieldKindSpec {
    Bool,
    Enum { options: Vec<String> },
    Text,
    Number,
    Array,
    Action(ActionSpec),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolPerformerCommand {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolPerformerPrompt {
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arg: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolPerformerSessionSpec {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub init_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extract_regex: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume: Option<ToolPerformerCommand>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discover: Option<ToolPerformerCommand>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id_strategy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolPerformerSpec {
    pub runner: String,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<ToolPerformerCommand>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<ToolPerformerPrompt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<ToolPerformerSessionSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolInstallCommand {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolInstallSpec {
    #[serde(default)]
    pub commands: Vec<ToolInstallCommand>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub post_install: Option<ToolInstallCommand>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirm_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolRuntimeConfig {
    pub api_version: String,
    pub id: String,
    pub display_name: String,
    pub performer: ToolPerformerSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defaults: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldSpec {
    pub id: String,
    pub label: String,
    pub kind: FieldKindSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "pointer",
        alias = "json_pointer"
    )]
    pub pointer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DoctorCheckKind {
    Which,
    PathExists,
    Custom,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoctorCheckSpec {
    pub kind: DoctorCheckKind,
    pub value: String,
    pub severity: CheckSeverity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolSpec {
    pub api_version: String,
    pub id: String,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    pub fields: Vec<FieldSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doctor: Option<Vec<DoctorCheckSpec>>,
    #[serde(default)]
    pub gitignore: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub performer: Option<ToolPerformerSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub install: Option<ToolInstallSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defaults: Option<serde_json::Value>,
}

impl ToolSpec {
    pub fn to_descriptor(&self) -> crate::tool::ToolDescriptor {
        use crate::tool::FieldDefault;
        use crate::tool::{FieldKind, ToolDescriptor, ToolField};

        ToolDescriptor {
            id: self.id.clone(),
            title: self.display_name.clone(),
            description: self
                .description
                .clone()
                .unwrap_or_else(|| format!("Settings for {}.", self.display_name)),
            install: self
                .install
                .as_ref()
                .map(|spec| crate::tool::ToolInstallDescriptor {
                    confirm_message: spec.confirm_message.clone().unwrap_or_else(|| {
                        "This installer may require an existing account or API key. Continue?"
                            .to_string()
                    }),
                }),
            fields: self
                .fields
                .iter()
                .map(|f| ToolField {
                    id: f.id.clone(),
                    label: f.label.clone(),
                    help: f.help.clone().unwrap_or_default(),
                    path: f.pointer.clone().unwrap_or_default(),
                    kind: match &f.kind {
                        FieldKindSpec::Bool => FieldKind::Bool,
                        FieldKindSpec::Enum { options } => FieldKind::Enum(options.clone()),
                        FieldKindSpec::Text => FieldKind::Text,
                        FieldKindSpec::Number => FieldKind::Number,
                        FieldKindSpec::Array => FieldKind::Array,
                        FieldKindSpec::Action(spec) => FieldKind::Action(match spec {
                            ActionSpec::OpenMcp { target_pointer } => {
                                crate::tool::ActionKind::OpenMcp {
                                    target_pointer: target_pointer.clone(),
                                }
                            }
                            ActionSpec::OpenSkills { target_pointer } => {
                                crate::tool::ActionKind::OpenSkills {
                                    target_pointer: target_pointer.clone(),
                                }
                            }
                            ActionSpec::OpenAgents { target_pointer } => {
                                crate::tool::ActionKind::OpenAgents {
                                    target_pointer: target_pointer.clone(),
                                }
                            }
                            ActionSpec::Custom { target } => crate::tool::ActionKind::Custom {
                                target: target.clone(),
                            },
                        }),
                    },
                    default: match (&f.kind, &f.default) {
                        (FieldKindSpec::Bool, Some(value)) => {
                            value.as_bool().map(FieldDefault::Bool)
                        }
                        (FieldKindSpec::Text, Some(value)) => {
                            value.as_str().map(|s| FieldDefault::Text(s.to_string()))
                        }
                        (FieldKindSpec::Enum { .. }, Some(value)) => {
                            value.as_str().map(|s| FieldDefault::Enum(s.to_string()))
                        }
                        (FieldKindSpec::Number, Some(value)) => {
                            parse_number_default(value).map(FieldDefault::Number)
                        }
                        (FieldKindSpec::Array, Some(value)) => {
                            parse_array_default(value).map(FieldDefault::Array)
                        }
                        _ => None,
                    },
                })
                .collect(),
        }
    }

    pub fn to_runtime_config(&self) -> Option<ToolRuntimeConfig> {
        self.performer.as_ref().map(|performer| ToolRuntimeConfig {
            api_version: self.api_version.clone(),
            id: self.id.clone(),
            display_name: self.display_name.clone(),
            performer: performer.clone(),
            defaults: self.defaults.clone(),
        })
    }

    pub fn validate(&self) -> crate::Result<()> {
        if self.api_version != "v1" {
            return Err(MaccError::Validation(format!(
                "Unsupported api_version: {}. Supported: v1",
                self.api_version
            )));
        }

        if !is_kebab_case(&self.id) {
            return Err(MaccError::Validation(format!(
                "Tool ID must be kebab-case: {}",
                self.id
            )));
        }

        for field in &self.fields {
            if let FieldKindSpec::Enum { options } = &field.kind {
                if options.is_empty() {
                    return Err(MaccError::Validation(format!(
                        "Enum field '{}' must have at least one option",
                        field.id
                    )));
                }
            }

            if let Some(ptr) = &field.pointer {
                if !ptr.starts_with('/') {
                    return Err(MaccError::Validation(format!(
                        "Pointer for field '{}' must start with '/': {}",
                        field.id, ptr
                    )));
                }

                if !self.is_pointer_allowed(ptr) {
                    return Err(MaccError::Validation(format!(
                        "Pointer for field '{}' uses unauthorized root: {}. Allowed roots: /tools/enabled, /tools/config/{}/, /selections/skills, /selections/agents, /selections/mcp, /standards/path, /standards/inline/",
                        field.id, ptr, self.id
                    )));
                }
            }

            if let Some(default_value) = &field.default {
                let pointer = field.pointer.as_deref().unwrap_or("");
                if pointer.is_empty() {
                    return Err(MaccError::Validation(format!(
                        "Default value for field '{}' requires a pointer",
                        field.id
                    )));
                }

                match &field.kind {
                    FieldKindSpec::Bool => {
                        if default_value.as_bool().is_none() {
                            return Err(MaccError::Validation(format!(
                                "Default value for field '{}' must be boolean",
                                field.id
                            )));
                        }
                    }
                    FieldKindSpec::Text => {
                        if default_value.as_str().is_none() {
                            return Err(MaccError::Validation(format!(
                                "Default value for field '{}' must be a string",
                                field.id
                            )));
                        }
                    }
                    FieldKindSpec::Enum { options } => {
                        let Some(default_str) = default_value.as_str() else {
                            return Err(MaccError::Validation(format!(
                                "Default value for field '{}' must be a string",
                                field.id
                            )));
                        };
                        if !options.iter().any(|opt| opt == default_str) {
                            return Err(MaccError::Validation(format!(
                                "Default value '{}' for field '{}' must be one of: {}",
                                default_str,
                                field.id,
                                options.join(", ")
                            )));
                        }
                    }
                    FieldKindSpec::Number => {
                        if parse_number_default(default_value).is_none() {
                            return Err(MaccError::Validation(format!(
                                "Default value for field '{}' must be a number",
                                field.id
                            )));
                        }
                    }
                    FieldKindSpec::Array => {
                        if parse_array_default(default_value).is_none() {
                            return Err(MaccError::Validation(format!(
                                "Default value for field '{}' must be an array or comma-separated string",
                                field.id
                            )));
                        }
                    }
                    FieldKindSpec::Action(_) => {
                        return Err(MaccError::Validation(format!(
                            "Default value is not allowed for action field '{}'",
                            field.id
                        )));
                    }
                }
            }

            if let FieldKindSpec::Action(action) = &field.kind {
                let ptr = match action {
                    ActionSpec::OpenMcp { target_pointer } => target_pointer,
                    ActionSpec::OpenSkills { target_pointer } => target_pointer,
                    ActionSpec::OpenAgents { target_pointer } => target_pointer,
                    ActionSpec::Custom { .. } => "",
                };

                if !ptr.is_empty() {
                    if !ptr.starts_with('/') {
                        return Err(MaccError::Validation(format!(
                            "Action target pointer for field '{}' must start with '/': {}",
                            field.id, ptr
                        )));
                    }

                    if !self.is_pointer_allowed(ptr) {
                        return Err(MaccError::Validation(format!(
                            "Action target pointer for field '{}' uses unauthorized root: {}. Allowed roots: /tools/enabled, /tools/config/{}/, /selections/skills, /selections/agents, /selections/mcp, /standards/path, /standards/inline/",
                            field.id, ptr, self.id
                        )));
                    }
                }
            }
        }

        if let Some(performer) = &self.performer {
            if performer.command.trim().is_empty() {
                return Err(MaccError::Validation(format!(
                    "Performer command must be set for tool '{}'",
                    self.id
                )));
            }
            if performer.runner.trim().is_empty() {
                return Err(MaccError::Validation(format!(
                    "Performer runner must be set for tool '{}'",
                    self.id
                )));
            }
            if let Some(retry) = &performer.retry {
                if retry.command.trim().is_empty() {
                    return Err(MaccError::Validation(format!(
                        "Performer retry command must be set for tool '{}'",
                        self.id
                    )));
                }
            }
            if let Some(prompt) = &performer.prompt {
                let mode = prompt.mode.as_str();
                if mode != "stdin" && mode != "arg" {
                    return Err(MaccError::Validation(format!(
                        "Performer prompt mode must be 'stdin' or 'arg' for tool '{}'",
                        self.id
                    )));
                }
                if mode == "arg" && prompt.arg.as_deref().unwrap_or("").is_empty() {
                    return Err(MaccError::Validation(format!(
                        "Performer prompt arg must be set for tool '{}'",
                        self.id
                    )));
                }
            }
            if let Some(session) = &performer.session {
                if let Some(scope) = &session.scope {
                    if scope != "project" && scope != "worktree" {
                        return Err(MaccError::Validation(format!(
                            "Performer session scope must be 'project' or 'worktree' for tool '{}'",
                            self.id
                        )));
                    }
                }
                if let Some(resume) = &session.resume {
                    if resume.command.trim().is_empty() {
                        return Err(MaccError::Validation(format!(
                            "Performer session resume command must be set for tool '{}'",
                            self.id
                        )));
                    }
                }
                if let Some(discover) = &session.discover {
                    if discover.command.trim().is_empty() {
                        return Err(MaccError::Validation(format!(
                            "Performer session discover command must be set for tool '{}'",
                            self.id
                        )));
                    }
                }
                if let Some(id_strategy) = &session.id_strategy {
                    if id_strategy != "generated" && id_strategy != "discovered" {
                        return Err(MaccError::Validation(format!(
                            "Performer session id_strategy must be 'generated' or 'discovered' for tool '{}'",
                            self.id
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    fn is_pointer_allowed(&self, ptr: &str) -> bool {
        if ptr == "/tools/enabled" {
            return true;
        }
        if ptr == "/selections/skills" || ptr == "/selections/agents" || ptr == "/selections/mcp" {
            return true;
        }
        if ptr == "/standards/path" {
            return true;
        }
        if ptr.starts_with("/standards/inline/") {
            return true;
        }

        let config_prefix = format!("/tools/config/{}/", self.id);
        if ptr.starts_with(&config_prefix) {
            return true;
        }

        // Exact match for the tool config root is also allowed
        if ptr == format!("/tools/config/{}", self.id) {
            return true;
        }

        false
    }

    pub fn from_yaml(s: &str) -> crate::Result<Self> {
        let spec: Self = serde_yaml::from_str(s).map_err(|e| {
            let (line, column) = e.location().map(|l| (l.line(), l.column())).unzip();
            crate::MaccError::ToolSpec {
                path: "ToolSpec(yaml)".to_string(),
                line,
                column,
                message: e.to_string(),
            }
        })?;
        spec.validate().map_err(|e| crate::MaccError::ToolSpec {
            path: "ToolSpec(yaml)".to_string(),
            line: None,
            column: None,
            message: e.to_string(),
        })?;
        Ok(spec)
    }

    pub fn from_json(s: &str) -> crate::Result<Self> {
        let spec: Self = serde_json::from_str(s).map_err(|e| crate::MaccError::ToolSpec {
            path: "ToolSpec(json)".to_string(),
            line: Some(e.line()),
            column: Some(e.column()),
            message: e.to_string(),
        })?;
        spec.validate().map_err(|e| crate::MaccError::ToolSpec {
            path: "ToolSpec(json)".to_string(),
            line: None,
            column: None,
            message: e.to_string(),
        })?;
        Ok(spec)
    }
}

fn is_kebab_case(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    s.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !s.starts_with('-')
        && !s.ends_with('-')
        && !s.contains("--")
}

fn parse_number_default(value: &serde_json::Value) -> Option<f64> {
    if let Some(num) = value.as_f64() {
        return Some(num);
    }
    if let Some(text) = value.as_str() {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return None;
        }
        if let Ok(parsed) = trimmed.parse::<f64>() {
            return Some(parsed);
        }
    }
    None
}

fn parse_array_default(value: &serde_json::Value) -> Option<Vec<String>> {
    if let Some(arr) = value.as_array() {
        let mut items = Vec::new();
        for entry in arr {
            let Some(text) = entry.as_str() else {
                return None;
            };
            items.push(text.to_string());
        }
        return Some(items);
    }

    if let Some(text) = value.as_str() {
        return Some(parse_csv_list(text));
    }

    None
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

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_kebab_case() {
        assert!(is_kebab_case("my-tool"));
        assert!(is_kebab_case("tool123"));
        assert!(is_kebab_case("abc-123-def"));
        assert!(!is_kebab_case("MyTool"));
        assert!(!is_kebab_case("my_tool"));
        assert!(!is_kebab_case("-tool"));
        assert!(!is_kebab_case("tool-"));
        assert!(!is_kebab_case("tool--name"));
        assert!(!is_kebab_case(""));
    }

    #[test]
    fn test_validation() {
        let mut spec = ToolSpec {
            api_version: "v1".to_string(),
            id: "test-tool".to_string(),
            display_name: "Test Tool".to_string(),
            description: None,
            capabilities: vec![],
            fields: vec![FieldSpec {
                id: "enabled".to_string(),
                label: "Enabled".to_string(),
                kind: FieldKindSpec::Bool,
                help: None,
                pointer: Some("/tools/config/test-tool/enabled".to_string()),
                default: None,
            }],
            doctor: None,
            gitignore: Vec::new(),
            performer: None,
            install: None,
            defaults: None,
        };

        assert!(spec.validate().is_ok());

        // Test unauthorized root
        spec.fields[0].pointer = Some("/unauthorized".to_string());
        assert!(spec.validate().is_err());

        spec.fields[0].pointer = Some("/tools/config/test-tool/enabled".to_string());

        spec.id = "Invalid_ID".to_string();
        assert!(spec.validate().is_err());

        spec.id = "test-tool".to_string();
        spec.api_version = "v2".to_string();
        assert!(spec.validate().is_err());
    }

    #[test]
    fn test_enum_validation() {
        let spec = ToolSpec {
            api_version: "v1".to_string(),
            id: "test-tool".to_string(),
            display_name: "Test Tool".to_string(),
            description: None,
            capabilities: vec![],
            fields: vec![FieldSpec {
                id: "choice".to_string(),
                label: "Choice".to_string(),
                kind: FieldKindSpec::Enum { options: vec![] },
                help: None,
                pointer: None,
                default: None,
            }],
            doctor: None,
            gitignore: Vec::new(),
            performer: None,
            install: None,
            defaults: None,
        };

        assert!(spec.validate().is_err());
    }

    #[test]
    fn test_pointer_validation() {
        let spec = ToolSpec {
            api_version: "v1".to_string(),
            id: "test-tool".to_string(),
            display_name: "Test Tool".to_string(),
            description: None,
            capabilities: vec![],
            fields: vec![FieldSpec {
                id: "p".to_string(),
                label: "P".to_string(),
                kind: FieldKindSpec::Text,
                help: None,
                pointer: Some("invalid".to_string()),
                default: None,
            }],
            doctor: None,
            gitignore: Vec::new(),
            performer: None,
            install: None,
            defaults: None,
        };

        assert!(spec.validate().is_err());
    }

    #[test]
    fn test_to_descriptor_conversion() {
        let spec = ToolSpec {
            api_version: "v1".to_string(),
            id: "full-tool".to_string(),
            display_name: "Full Tool".to_string(),
            description: Some("A tool with all field types.".to_string()),
            capabilities: vec!["chat".to_string()],
            fields: vec![
                FieldSpec {
                    id: "b".to_string(),
                    label: "Bool".to_string(),
                    kind: FieldKindSpec::Bool,
                    help: Some("Help B".to_string()),
                    pointer: Some("/tools/config/full-tool/b".to_string()),
                    default: None,
                },
                FieldSpec {
                    id: "e".to_string(),
                    label: "Enum".to_string(),
                    kind: FieldKindSpec::Enum {
                        options: vec!["o1".to_string(), "o2".to_string()],
                    },
                    help: None,
                    pointer: Some("/tools/config/full-tool/e".to_string()),
                    default: None,
                },
                FieldSpec {
                    id: "t".to_string(),
                    label: "Text".to_string(),
                    kind: FieldKindSpec::Text,
                    help: None,
                    pointer: Some("/tools/config/full-tool/t".to_string()),
                    default: None,
                },
                FieldSpec {
                    id: "a".to_string(),
                    label: "Action".to_string(),
                    kind: FieldKindSpec::Action(ActionSpec::OpenMcp {
                        target_pointer: "/selections/mcp".to_string(),
                    }),
                    help: None,
                    pointer: None,
                    default: None,
                },
            ],
            doctor: None,
            gitignore: Vec::new(),
            performer: None,
            install: None,
            defaults: None,
        };

        let desc = spec.to_descriptor();

        assert_eq!(desc.id, "full-tool");
        assert_eq!(desc.title, "Full Tool");
        assert_eq!(desc.description, "A tool with all field types.");
        assert_eq!(desc.fields.len(), 4);

        // Bool field
        assert_eq!(desc.fields[0].id, "b");
        assert_eq!(desc.fields[0].label, "Bool");
        assert_eq!(desc.fields[0].help, "Help B");
        assert_eq!(desc.fields[0].path, "/tools/config/full-tool/b");
        assert!(matches!(desc.fields[0].kind, crate::tool::FieldKind::Bool));

        // Enum field
        assert_eq!(desc.fields[1].id, "e");
        assert_eq!(desc.fields[1].path, "/tools/config/full-tool/e");
        assert!(matches!(
            &desc.fields[1].kind,
            crate::tool::FieldKind::Enum(opts) if opts == &vec!["o1".to_string(), "o2".to_string()]
        ));

        // Text field
        assert_eq!(desc.fields[2].id, "t");
        assert_eq!(desc.fields[2].path, "/tools/config/full-tool/t");
        assert!(matches!(desc.fields[2].kind, crate::tool::FieldKind::Text));

        // Action field
        assert_eq!(desc.fields[3].id, "a");
        assert_eq!(desc.fields[3].path, "");
        assert!(matches!(
            &desc.fields[3].kind,
            crate::tool::FieldKind::Action(crate::tool::ActionKind::OpenMcp { ref target_pointer }) if target_pointer == "/selections/mcp"
        ));
    }

    #[test]
    fn test_to_descriptor_defaults() {
        let spec = ToolSpec {
            api_version: "v1".to_string(),
            id: "minimal".to_string(),
            display_name: "Minimal".to_string(),
            description: None,
            capabilities: vec![],
            fields: vec![],
            doctor: None,
            gitignore: Vec::new(),
            performer: None,
            install: None,
            defaults: None,
        };

        let desc = spec.to_descriptor();
        assert_eq!(desc.description, "Settings for Minimal.");
        assert!(desc.fields.is_empty());
    }

    #[test]
    fn test_yaml_parsing() {
        let yaml = r#"
api_version: v1
id: sample-tool
display_name: Sample Tool
fields:
  - id: model
    label: Model
    kind:
      type: enum
      options: [sonnet, opus, haiku]
    pointer: /tools/config/sample-tool/model
  - id: rules_enabled
    label: Rules
    kind:
      type: bool
    pointer: /tools/config/sample-tool/rules_enabled
  - id: configure_mcp
    label: MCP
    kind:
      type: action
      action: open_mcp
      target_pointer: /selections/mcp
"#;
        let spec = ToolSpec::from_yaml(yaml).unwrap();
        assert_eq!(spec.id, "sample-tool");
        assert_eq!(spec.fields.len(), 3);
        assert!(matches!(spec.fields[0].kind, FieldKindSpec::Enum { .. }));
        assert!(matches!(spec.fields[1].kind, FieldKindSpec::Bool));
        assert!(matches!(
            spec.fields[2].kind,
            FieldKindSpec::Action(ActionSpec::OpenMcp { .. })
        ));
    }

    #[test]
    fn test_json_parsing() {
        let json = r#"{
  "api_version": "v1",
  "id": "sample-tool",
  "display_name": "Sample Tool",
  "fields": [
    {
      "id": "model",
      "label": "Model",
      "kind": { "type": "enum", "options": ["sonnet"] },
      "pointer": "/tools/config/sample-tool/model"
    },
    {
      "id": "rules",
      "label": "Rules",
      "kind": { "type": "bool" },
      "pointer": "/tools/config/sample-tool/rules"
    }
  ]
}"#;
        let spec = ToolSpec::from_json(json).unwrap();
        assert_eq!(spec.id, "sample-tool");
        assert_eq!(spec.fields.len(), 2);
    }

    #[test]
    fn test_invalid_action_parsing() {
        let yaml = r#"
api_version: v1
id: test
display_name: Test
fields:
  - id: act
    label: Act
    kind:
      type: action
      action: unknown_action
      target_pointer: /selections/mcp
"#;
        let result = ToolSpec::from_yaml(yaml);
        assert!(result.is_err());
    }
}
