#[derive(Debug, Clone, PartialEq)]
pub struct ToolDescriptor {
    pub id: String,
    pub title: String,
    pub description: String,
    pub fields: Vec<ToolField>,
    pub install: Option<ToolInstallDescriptor>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolInstallDescriptor {
    pub confirm_message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolField {
    pub id: String,
    pub label: String,
    pub help: String,
    pub path: String,
    pub kind: FieldKind,
    pub default: Option<FieldDefault>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldDefault {
    Bool(bool),
    Text(String),
    Enum(String),
    Number(f64),
    Array(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionKind {
    OpenMcp { target_pointer: String },
    OpenSkills { target_pointer: String },
    OpenAgents { target_pointer: String },
    Custom { target: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldKind {
    Bool,
    Enum(Vec<String>),
    Text,
    Number,
    Array,
    Action(ActionKind),
}
