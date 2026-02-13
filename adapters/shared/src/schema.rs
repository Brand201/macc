#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolSchema {
    pub version: String,
}

impl ToolSchema {
    pub fn new(version: impl Into<String>) -> Self {
        Self {
            version: version.into(),
        }
    }
}
