use std::collections::BTreeSet;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolCapabilities {
    pub features: BTreeSet<String>,
}

impl ToolCapabilities {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_feature(mut self, feature: impl Into<String>) -> Self {
        self.features.insert(feature.into());
        self
    }

    pub fn supports(&self, feature: &str) -> bool {
        self.features.contains(feature)
    }
}
