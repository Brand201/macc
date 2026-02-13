use macc_core::resolve::{
    PlanningContext, ResolvedConfig, ResolvedSelectionsConfig, ResolvedStandardsConfig,
    ResolvedToolsConfig,
};
use macc_core::tool::ToolSpecLoader;
use macc_core::{ProjectPaths, ToolAdapter};
use std::collections::BTreeSet;
use std::sync::Arc;

#[test]
fn test_adapter_contract_conformance() {
    let registry = macc_registry::default_registry();
    let ids = registry.list_ids();

    assert!(!ids.is_empty(), "Registry should not be empty");

    for id in ids {
        let adapter = registry.get(&id).expect("Adapter must exist");
        check_adapter_contract(adapter);
    }
}

fn check_adapter_contract(adapter: Arc<dyn ToolAdapter>) {
    let id = adapter.id();
    let paths = ProjectPaths::from_root(".");

    // Create a minimal resolved config that enables this tool
    let resolved = ResolvedConfig {
        version: "v1".to_string(),
        tools: ResolvedToolsConfig {
            enabled: vec![id.clone()],
            ..Default::default()
        },
        standards: ResolvedStandardsConfig {
            path: None,
            inline: Default::default(),
        },
        selections: ResolvedSelectionsConfig {
            skills: vec![],
            agents: vec![],
            mcp: vec![],
        },
        mcp_templates: Vec::new(),
        automation: Default::default(),
    };

    let ctx = PlanningContext {
        paths: &paths,
        resolved: &resolved,
        materialized_units: &[],
    };

    // 1 & 2. Non-panicking and Deterministic output
    let plan1 = adapter
        .plan(&ctx)
        .expect(&format!("Adapter {} failed to plan", id));
    let plan2 = adapter
        .plan(&ctx)
        .expect(&format!("Adapter {} failed to plan second time", id));
    assert_eq!(
        plan1, plan2,
        "Adapter {} is not deterministic (sequential calls produced different plans)",
        id
    );

    // 3. Path normalization and safety
    for action in &plan1.actions {
        let path = action.path();

        // No absolute paths for project scope
        if action.scope() == macc_core::plan::Scope::Project {
            assert!(
                !path.starts_with('/'),
                "Adapter {} produced absolute path in Project scope: {}",
                id,
                path
            );
            assert!(
                !path.contains(".."),
                "Adapter {} produced path with traversal in Project scope: {}",
                id,
                path
            );
            // Windows-style absolute paths
            if path.len() > 2 && path.chars().nth(1) == Some(':') {
                panic!("Adapter {} produced Windows absolute path: {}", id, path);
            }
        }

        // Empty paths are suspicious
        if !matches!(action, macc_core::plan::Action::Noop { .. }) {
            assert!(
                !path.is_empty(),
                "Adapter {} produced an empty path for action {:?}",
                id,
                action
            );
        }
    }

    // 4. Forbidden operations by default
    // We don't expect any Noop actions in production adapters by default.
    for action in &plan1.actions {
        if let macc_core::plan::Action::Noop { description, .. } = action {
            if id != "test" {
                // test adapter is allowed to have noops
                panic!("Adapter {} produced a Noop action: {}", id, description);
            }
        }
    }

    // 5. Normalization check: ensure plan is at least deterministic in its own ordering.
    // (We already checked plan1 == plan2)
}

#[test]
fn test_adapter_with_skills_and_agents() {
    let registry = macc_registry::default_registry();
    let ids = registry.list_ids();

    for id in ids {
        let adapter = registry.get(&id).expect("Adapter must exist");

        let paths = ProjectPaths::from_root(".");
        let resolved = ResolvedConfig {
            version: "v1".to_string(),
            tools: ResolvedToolsConfig {
                enabled: vec![id.clone()],
                ..Default::default()
            },
            standards: ResolvedStandardsConfig {
                path: None,
                inline: Default::default(),
            },
            selections: ResolvedSelectionsConfig {
                skills: vec!["test-skill".to_string()],
                agents: vec!["test-agent".to_string()],
                mcp: vec![],
            },
            mcp_templates: Vec::new(),
            automation: Default::default(),
        };

        let ctx = PlanningContext {
            paths: &paths,
            resolved: &resolved,
            materialized_units: &[],
        };

        let plan = adapter
            .plan(&ctx)
            .expect(&format!("Adapter {} failed to plan with selections", id));

        // Assertions for well-formedness
        assert!(
            !plan.actions.is_empty(),
            "Adapter {} should produce actions when selections are present",
            id
        );

        for action in &plan.actions {
            assert!(!action.path().is_empty());
            if action.scope() == macc_core::plan::Scope::Project {
                assert!(!action.path().contains(".."));
            }
        }
    }
}

#[test]
fn contract_embedded_toolspecs_match_registered_adapters() {
    let registry = macc_registry::default_registry();
    let adapter_ids: BTreeSet<String> = registry
        .list_ids()
        .into_iter()
        .filter(|id| !is_internal_adapter(id))
        .collect();

    let loader = ToolSpecLoader::new(Vec::new());
    let (specs, diags) = loader.load_all_with_embedded();
    assert!(
        diags.is_empty(),
        "Embedded ToolSpec diagnostics should be empty: {diags:?}"
    );

    let spec_ids: BTreeSet<String> = specs
        .into_iter()
        .filter(|spec| spec.performer.is_some())
        .map(|spec| spec.id)
        .collect();

    assert_eq!(
        spec_ids, adapter_ids,
        "Embedded ToolSpec performer IDs must match registered adapter IDs."
    );
}

fn is_internal_adapter(id: &str) -> bool {
    matches!(id, "test")
}
