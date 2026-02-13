pub mod descriptor;
pub mod loader;
pub mod registry;
pub mod spec;

pub use descriptor::{
    ActionKind, FieldDefault, FieldKind, ToolDescriptor, ToolField, ToolInstallDescriptor,
};
pub use loader::{ToolDiagnostic, ToolSpecLoader};
pub use registry::{AdapterRegistration, MockAdapter, TestAdapter, ToolAdapter, ToolRegistry};
pub use spec::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolve::PlanningContext;
    use crate::resolve::ResolvedConfig;
    use crate::ProjectPaths;

    #[test]
    fn test_registry_basics() {
        let registry = ToolRegistry::from_inventory();
        assert!(registry.get("test").is_some());
        assert!(registry.get("unknown").is_none());
        assert_eq!(registry.list_ids(), vec!["test"]);
    }

    #[test]
    fn test_adapter_planning() {
        let adapter = TestAdapter;
        let paths = ProjectPaths::from_root(".");
        let resolved = ResolvedConfig {
            version: "v1".to_string(),
            tools: crate::resolve::ResolvedToolsConfig {
                enabled: vec!["test".to_string()],
                ..Default::default()
            },
            standards: crate::resolve::ResolvedStandardsConfig {
                path: None,
                inline: Default::default(),
            },
            selections: crate::resolve::ResolvedSelectionsConfig {
                skills: vec![],
                agents: vec![],
                mcp: vec![],
            },
            mcp_templates: Vec::new(),
            automation: crate::config::AutomationConfig::default(),
        };

        let ctx = PlanningContext {
            paths: &paths,
            resolved: &resolved,
            materialized_units: &[],
        };

        let plan = adapter.plan(&ctx).unwrap();
        // MACC_GENERATED.txt, .macc/test-output.json, .env.example
        assert_eq!(plan.actions.len(), 3);

        match &plan.actions[0] {
            crate::plan::Action::WriteFile { path, .. } => {
                assert_eq!(path, "MACC_GENERATED.txt");
            }
            _ => panic!("Expected WriteFile action"),
        }

        match &plan.actions[1] {
            crate::plan::Action::WriteFile { path, .. } => {
                assert_eq!(path, ".macc/test-output.json");
            }
            _ => panic!("Expected WriteFile action"),
        }

        match &plan.actions[2] {
            crate::plan::Action::WriteFile { path, .. } => {
                assert_eq!(path, ".env.example");
            }
            _ => panic!("Expected WriteFile action"),
        }
    }
}
