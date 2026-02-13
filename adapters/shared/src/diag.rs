#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagLevel {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diag {
    pub level: DiagLevel,
    pub message: String,
    pub hints: Vec<String>,
}

impl Diag {
    pub fn info(message: impl Into<String>) -> Self {
        Self {
            level: DiagLevel::Info,
            message: message.into(),
            hints: Vec::new(),
        }
    }

    pub fn warning(message: impl Into<String>) -> Self {
        Self {
            level: DiagLevel::Warning,
            message: message.into(),
            hints: Vec::new(),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            level: DiagLevel::Error,
            message: message.into(),
            hints: Vec::new(),
        }
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hints.push(hint.into());
        self
    }
}
