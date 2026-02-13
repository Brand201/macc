use crate::{
    catalog::{self, Agent, Skill},
    config::CanonicalConfig,
    doctor::{self, ToolCheck},
    plan::{self, ActionPlan, PlannedOp},
    resolve::{self, CliOverrides, MaterializedFetchUnit},
    tool::{ToolDescriptor, ToolDiagnostic, ToolRegistry, ToolSpecLoader},
    ApplyReport, ProjectPaths, Result,
};

/// The interface for UI (CLI/TUI) to interact with MACC core logic.
pub trait Engine {
    fn list_tools(&self, paths: &ProjectPaths) -> (Vec<ToolDescriptor>, Vec<ToolDiagnostic>);
    fn doctor(&self, paths: &ProjectPaths) -> Vec<ToolCheck>;
    fn plan(
        &self,
        paths: &ProjectPaths,
        config: &CanonicalConfig,
        materialized_units: &[MaterializedFetchUnit],
        overrides: &CliOverrides,
    ) -> Result<ActionPlan>;
    fn plan_operations(&self, paths: &ProjectPaths, plan: &ActionPlan) -> Vec<PlannedOp>;
    fn apply(
        &self,
        paths: &ProjectPaths,
        plan: &mut ActionPlan,
        allow_user_scope: bool,
    ) -> Result<ApplyReport>;

    fn builtin_skills(&self) -> Vec<Skill>;
    fn builtin_agents(&self) -> Vec<Agent>;
}

/// The standard production engine.
pub struct MaccEngine {
    registry: ToolRegistry,
}

impl MaccEngine {
    /// Creates a new engine with the provided tool registry.
    pub fn new(registry: ToolRegistry) -> Self {
        Self { registry }
    }

    /// Provides access to the underlying tool registry.
    pub fn registry(&self) -> &ToolRegistry {
        &self.registry
    }
}

impl Engine for MaccEngine {
    /// Lists all available tools and their metadata, including any loading diagnostics.
    fn list_tools(&self, paths: &ProjectPaths) -> (Vec<ToolDescriptor>, Vec<ToolDiagnostic>) {
        let search_paths = ToolSpecLoader::default_search_paths(&paths.root);
        let loader = ToolSpecLoader::new(search_paths);
        let (specs, mut diagnostics) = loader.load_all_with_embedded();
        let mut descriptors: Vec<_> = specs.into_iter().map(|s| s.to_descriptor()).collect();

        // Ensure deterministic ordering by ID for UI consistency
        descriptors.sort_by(|a, b| a.id.cmp(&b.id));

        if descriptors.is_empty() {
            diagnostics.push(ToolDiagnostic {
                path: std::path::PathBuf::from("<toolspec-resolution>"),
                error: "No ToolSpecs resolved (embedded + user + project overrides).".to_string(),
                line: None,
                column: None,
            });
        }

        (descriptors, diagnostics)
    }

    /// Runs diagnostic checks for the environment and supported tools.
    fn doctor(&self, paths: &ProjectPaths) -> Vec<ToolCheck> {
        // Load specs to determine checks
        let search_paths = ToolSpecLoader::default_search_paths(&paths.root);
        let loader = ToolSpecLoader::new(search_paths);
        let (specs, _) = loader.load_all_with_embedded();

        let mut checks = doctor::checks_for_enabled_tools(&specs);
        doctor::run_checks(&mut checks);
        checks
    }

    /// Builds an effective ActionPlan based on canonical configuration and optional CLI overrides.
    fn plan(
        &self,
        paths: &ProjectPaths,
        config: &CanonicalConfig,
        materialized_units: &[MaterializedFetchUnit],
        overrides: &CliOverrides,
    ) -> Result<ActionPlan> {
        let resolved = resolve::resolve(config, overrides);
        crate::build_plan(paths, &resolved, materialized_units, &self.registry)
    }

    /// Produces a list of deterministic operations from a plan, suitable for UI preview or diff view.
    fn plan_operations(&self, paths: &ProjectPaths, plan: &ActionPlan) -> Vec<PlannedOp> {
        plan::collect_plan_operations(paths, plan)
    }

    fn apply(
        &self,
        paths: &ProjectPaths,
        plan: &mut ActionPlan,
        allow_user_scope: bool,
    ) -> Result<ApplyReport> {
        crate::apply_plan(paths, plan, allow_user_scope)
    }

    fn builtin_skills(&self) -> Vec<Skill> {
        catalog::builtin_skills()
    }

    fn builtin_agents(&self) -> Vec<Agent> {
        catalog::builtin_agents()
    }
}

/// A test-only engine that uses in-memory fixtures instead of the filesystem.
///
/// This ensures UI tests (TUI/CLI) are stable, fast, and tool-agnostic.
pub struct TestEngine {
    registry: ToolRegistry,
    specs: Vec<crate::tool::ToolSpec>,
    fixture_ids: Vec<String>,
}

impl TestEngine {
    /// Creates a new test engine with the provided registry and specs.
    pub fn new(registry: ToolRegistry, specs: Vec<crate::tool::ToolSpec>) -> Self {
        Self {
            registry,
            specs,
            fixture_ids: Vec::new(),
        }
    }

    /// Creates a default test engine with fixture tools.
    pub fn with_fixtures() -> Self {
        let fixture_ids = Self::generate_fixture_ids(2);
        Self::with_fixtures_for_ids(&fixture_ids)
    }

    /// Creates a test engine with fixture tools using the provided IDs.
    pub fn with_fixtures_for_ids(ids: &[String]) -> Self {
        use crate::tool::{
            CheckSeverity, DoctorCheckKind, DoctorCheckSpec, FieldKindSpec, FieldSpec, MockAdapter,
            ToolSpec,
        };
        use std::sync::Arc;

        assert!(
            ids.len() >= 2,
            "with_fixtures_for_ids expects at least two tool IDs"
        );

        let id_one = ids[0].clone();
        let id_two = ids[1].clone();

        let spec_one = ToolSpec {
            api_version: "v1".to_string(),
            id: id_one.clone(),
            display_name: "Fixture Tool One".to_string(),
            description: Some("First fixture tool for UI testing.".to_string()),
            capabilities: vec!["chat".to_string()],
            fields: vec![
                FieldSpec {
                    id: "enabled".to_string(),
                    label: "Enabled".to_string(),
                    kind: FieldKindSpec::Bool,
                    help: Some("Whether the tool is enabled.".to_string()),
                    pointer: Some(format!("/tools/config/{}/enabled", id_one)),
                    default: None,
                },
                FieldSpec {
                    id: "mode".to_string(),
                    label: "Mode".to_string(),
                    kind: FieldKindSpec::Enum {
                        options: vec![
                            "fast".to_string(),
                            "balanced".to_string(),
                            "precise".to_string(),
                        ],
                    },
                    help: Some("Select the operation mode.".to_string()),
                    pointer: Some(format!("/tools/config/{}/mode", id_one)),
                    default: None,
                },
                FieldSpec {
                    id: "username".to_string(),
                    label: "Username".to_string(),
                    kind: FieldKindSpec::Text,
                    help: Some("Your username for this tool.".to_string()),
                    pointer: Some(format!("/tools/config/{}/username", id_one)),
                    default: None,
                },
                FieldSpec {
                    id: "setup_mcp".to_string(),
                    label: "Setup MCP".to_string(),
                    kind: FieldKindSpec::Action(crate::tool::ActionSpec::OpenMcp {
                        target_pointer: "/selections/mcp".to_string(),
                    }),
                    help: Some("Open MCP selector.".to_string()),
                    pointer: None,
                    default: None,
                },
            ],
            doctor: Some(vec![DoctorCheckSpec {
                kind: DoctorCheckKind::Which,
                value: format!("{}-cli", id_one),
                severity: CheckSeverity::Error,
            }]),
            gitignore: Vec::new(),
            performer: None,
            install: None,
            defaults: None,
        };

        let spec_two = ToolSpec {
            api_version: "v1".to_string(),
            id: id_two.clone(),
            display_name: "Fixture Tool Two".to_string(),
            description: Some("Second fixture tool for UI testing.".to_string()),
            capabilities: vec!["edit".to_string()],
            fields: vec![
                FieldSpec {
                    id: "api_key".to_string(),
                    label: "API Key".to_string(),
                    kind: FieldKindSpec::Text,
                    help: Some("Sensitive API key.".to_string()),
                    pointer: Some(format!("/tools/config/{}/auth/key", id_two)),
                    default: None,
                },
                FieldSpec {
                    id: "model".to_string(),
                    label: "Model".to_string(),
                    kind: FieldKindSpec::Enum {
                        options: vec!["smart".to_string(), "small".to_string()],
                    },
                    help: None,
                    pointer: Some(format!("/tools/config/{}/settings/model_name", id_two)),
                    default: None,
                },
                FieldSpec {
                    id: "auto_apply".to_string(),
                    label: "Auto Apply".to_string(),
                    kind: FieldKindSpec::Bool,
                    help: None,
                    pointer: Some(format!("/tools/config/{}/settings/auto_apply", id_two)),
                    default: None,
                },
            ],
            doctor: Some(vec![DoctorCheckSpec {
                kind: DoctorCheckKind::PathExists,
                value: format!("~/.{}/config.json", id_two),
                severity: CheckSeverity::Warning,
            }]),
            gitignore: Vec::new(),
            performer: None,
            install: None,
            defaults: None,
        };

        let mut registry = ToolRegistry::new();

        let mut plan_one = ActionPlan::new();
        let output_one = format!("{}-output.txt", id_one);
        plan_one.add_action(plan::Action::WriteFile {
            path: output_one,
            content: format!("fixture content for {}\n", id_one).into_bytes(),
            scope: plan::Scope::Project,
        });

        let mut plan_two = ActionPlan::new();
        let output_two = format!("{}-output.txt", id_two);
        plan_two.add_action(plan::Action::WriteFile {
            path: output_two,
            content: format!("fixture content for {}\n", id_two).into_bytes(),
            scope: plan::Scope::Project,
        });

        registry.register(Arc::new(MockAdapter {
            id: id_one.clone(),
            plan: plan_one,
        }));
        registry.register(Arc::new(MockAdapter {
            id: id_two.clone(),
            plan: plan_two,
        }));

        Self {
            registry,
            specs: vec![spec_one, spec_two],
            fixture_ids: vec![id_one, id_two],
        }
    }

    pub fn generate_fixture_ids(count: usize) -> Vec<String> {
        let suffix = fixture_suffix();
        (0..count)
            .map(|idx| {
                let letter = (b'a' + (idx as u8)) as char;
                format!("fixture-{}-{}", letter, suffix)
            })
            .collect()
    }

    pub fn fixture_ids(&self) -> &[String] {
        &self.fixture_ids
    }
}

fn fixture_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", nanos)
}

impl Engine for TestEngine {
    /// Lists tools from the in-memory fixtures.
    fn list_tools(&self, _paths: &ProjectPaths) -> (Vec<ToolDescriptor>, Vec<ToolDiagnostic>) {
        let descriptors = self.specs.iter().map(|s| s.to_descriptor()).collect();
        (descriptors, Vec::new())
    }

    /// Runs stubbed diagnostic checks.
    fn doctor(&self, _paths: &ProjectPaths) -> Vec<ToolCheck> {
        // Since we are testing, we can simulate checks based on fixtures
        let mut checks = doctor::checks_for_enabled_tools(&self.specs);
        // Force them to be installed for tests
        for check in &mut checks {
            check.status = crate::doctor::ToolStatus::Installed;
        }
        checks
    }

    /// Produces a deterministic ActionPlan.
    fn plan(
        &self,
        paths: &ProjectPaths,
        config: &CanonicalConfig,
        materialized_units: &[MaterializedFetchUnit],
        overrides: &CliOverrides,
    ) -> Result<ActionPlan> {
        let resolved = resolve::resolve(config, overrides);
        crate::build_plan(paths, &resolved, materialized_units, &self.registry)
    }

    /// Produces a list of deterministic operations.
    fn plan_operations(&self, paths: &ProjectPaths, plan: &ActionPlan) -> Vec<PlannedOp> {
        plan::collect_plan_operations(paths, plan)
    }

    /// Applies the planned actions (using real apply, but usually with mock paths).
    fn apply(
        &self,
        paths: &ProjectPaths,
        plan: &mut ActionPlan,
        allow_user_scope: bool,
    ) -> Result<ApplyReport> {
        crate::apply_plan(paths, plan, allow_user_scope)
    }

    fn builtin_skills(&self) -> Vec<Skill> {
        vec![
            Skill {
                id: "mock-skill-one".into(),
                name: "Mock Skill One".into(),
                description: "First mock skill for testing.".into(),
            },
            Skill {
                id: "mock-skill-two".into(),
                name: "Mock Skill Two".into(),
                description: "Second mock skill for testing.".into(),
            },
        ]
    }

    fn builtin_agents(&self) -> Vec<Agent> {
        vec![
            Agent {
                id: "mock-agent-one".into(),
                name: "Mock Agent One".into(),
                description: "First mock agent for testing.".into(),
            },
            Agent {
                id: "mock-agent-two".into(),
                name: "Mock Agent Two".into(),
                description: "Second mock agent for testing.".into(),
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ToolsConfig;
    use std::fs;

    fn create_test_paths() -> (ProjectPaths, PathBuf) {
        let temp_dir = std::env::temp_dir().join(format!("macc_engine_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        (ProjectPaths::from_root(&temp_dir), temp_dir)
    }

    use std::path::PathBuf;
    fn uuid_v4_like() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{:x}", nanos)
    }

    #[test]
    fn test_engine_plan_and_apply() -> Result<()> {
        let (paths, temp_dir) = create_test_paths();
        crate::init(&paths, false)?;

        let engine = MaccEngine::new(ToolRegistry::default_registry());

        let config = CanonicalConfig {
            version: Some("v1".to_string()),
            tools: ToolsConfig {
                enabled: vec!["test".to_string()],
                ..Default::default()
            },
            ..Default::default()
        };

        // 1. Plan
        let mut plan = engine.plan(&paths, &config, &[], &CliOverrides::default())?;
        assert!(plan.actions.len() > 0);

        // 2. Plan Operations (for UI)
        let ops = engine.plan_operations(&paths, &plan);
        assert!(ops.len() > 0);
        assert!(ops.iter().any(|op| op.path == "MACC_GENERATED.txt"));

        // 3. Apply
        let report = engine.apply(&paths, &mut plan, false)?;
        assert!(temp_dir.join("MACC_GENERATED.txt").exists());
        assert_eq!(
            report.outcomes.get("MACC_GENERATED.txt").unwrap(),
            &plan::ActionStatus::Created
        );

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_engine_doctor() {
        let (paths, temp_dir) = create_test_paths();
        // Create a dummy tool spec file
        let tools_d = paths.root.join("registry/tools.d");
        fs::create_dir_all(&tools_d).unwrap();
        let spec = r#"
api_version: v1
id: my-tool
display_name: My Tool
fields: []
"#;
        fs::write(tools_d.join("my.tool.yaml"), spec).unwrap();

        let engine = MaccEngine::new(ToolRegistry::new());
        let checks = engine.doctor(&paths);

        // Should have at least "Git" and "My Tool" (via heuristic)
        assert!(checks.iter().any(|c| c.name == "Git"));
        assert!(checks.iter().any(|c| c.name == "My Tool"));

        fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_engine_list_tools() {
        let (paths, temp_dir) = create_test_paths();
        // Create a dummy tool spec file
        let tools_d = paths.root.join("registry/tools.d");
        fs::create_dir_all(&tools_d).unwrap();
        let spec = r#"
api_version: v1
id: my-tool
display_name: My Tool
fields: []
"#;
        fs::write(tools_d.join("my.tool.yaml"), spec).unwrap();

        let engine = MaccEngine::new(ToolRegistry::new());
        let (descriptors, diags) = engine.list_tools(&paths);

        assert!(diags.is_empty(), "Diagnostics: {:?}", diags);
        assert!(descriptors.iter().any(|d| d.id == "my-tool"));

        fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_test_engine_fixtures() -> Result<()> {
        let (paths, temp_dir) = create_test_paths();
        let fixture_ids = TestEngine::generate_fixture_ids(2);
        let tool_one = fixture_ids[0].clone();
        let tool_two = fixture_ids[1].clone();
        let engine = TestEngine::with_fixtures_for_ids(&fixture_ids);

        // 1. List tools (should use in-memory specs)
        let (descriptors, diags) = engine.list_tools(&paths);
        assert_eq!(descriptors.len(), 2);
        assert_eq!(descriptors[0].id, tool_one);
        assert_eq!(descriptors[1].id, tool_two);
        assert!(diags.is_empty());

        // 2. Doctor (should use in-memory specs)
        let checks = engine.doctor(&paths);
        // Git + Mock One + Mock Two = 3
        // Actually checks_for_enabled_tools adds Git baseline.
        // TestEngine::doctor calls generic logic.
        assert!(checks.len() >= 3);
        assert!(checks.iter().any(|c| c.tool_id == Some(tool_one.clone())));

        // 3. Plan
        let config = CanonicalConfig {
            tools: crate::config::ToolsConfig {
                enabled: vec![tool_one.clone()],
                ..Default::default()
            },
            ..Default::default()
        };
        let mut plan = engine.plan(&paths, &config, &[], &CliOverrides::default())?;
        let output_path = format!("{}-output.txt", tool_one);
        assert!(plan.actions.iter().any(|a| a.path() == output_path));

        // 4. Apply
        let report = engine.apply(&paths, &mut plan, false)?;
        assert!(temp_dir.join(format!("{}-output.txt", tool_one)).exists());
        assert_eq!(
            report
                .outcomes
                .get(&format!("{}-output.txt", tool_one))
                .unwrap(),
            &plan::ActionStatus::Created
        );

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }
}
