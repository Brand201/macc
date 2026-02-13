use macc_core::{
    plan::{collect_plan_operations, Action, ActionPlan, Scope},
    plan_operations,
    resolve::{
        ResolvedConfig, ResolvedSelectionsConfig, ResolvedStandardsConfig, ResolvedToolsConfig,
    },
    ProjectPaths, Result, ToolRegistry,
};
use serde_json::json;
use std::fs;
use std::path::PathBuf;

#[test]
fn plan_operations_returns_expected_write_ops() -> Result<()> {
    let temp_dir = temp_dir("plan_operations");
    let paths = ProjectPaths::from_root(&temp_dir);

    let resolved = ResolvedConfig {
        version: "v1".to_string(),
        tools: ResolvedToolsConfig {
            enabled: vec!["test".to_string()],
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
        automation: macc_core::config::AutomationConfig::default(),
    };
    let ops = plan_operations(&paths, &resolved, &[], &ToolRegistry::default_registry())?;

    assert_eq!(
        ops.len(),
        4,
        "expected env, gitignore, test-output, and MACC file"
    );
    assert_eq!(ops[0].path, ".env.example");
    assert_eq!(ops[1].path, ".gitignore");
    assert_eq!(ops[2].path, ".macc/test-output.json");
    assert_eq!(ops[3].path, "MACC_GENERATED.txt");

    let gitignore_op = &ops[1];
    let gitignore_after = gitignore_op
        .after
        .as_ref()
        .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
        .unwrap_or_default();
    assert!(gitignore_after.contains(".macc/"));
    assert_eq!(gitignore_op.kind, macc_core::plan::PlannedOpKind::Write);

    let macs = &ops[3];
    assert_eq!(macs.kind, macc_core::plan::PlannedOpKind::Write);
    assert!(macs
        .after
        .as_ref()
        .map_or(false, |bytes| bytes.starts_with(b"This is a test")));

    fs::remove_dir_all(&temp_dir).ok();

    Ok(())
}

fn temp_dir(name: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!("macc_plan_ops_{}_{}", name, uuid()));
    if dir.exists() {
        fs::remove_dir_all(&dir).ok();
    }
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn uuid() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time error");
    format!("{}", since_epoch.as_nanos())
}

#[test]
fn plan_operations_flags_user_consent() -> Result<()> {
    let temp_dir = temp_dir("plan_consents");
    let paths = ProjectPaths::from_root(&temp_dir);

    let mut plan = ActionPlan::new();
    let tool_id = format!("tool-{}", uuid());
    plan.add_action(Action::MergeJson {
        path: "user/settings.json".to_string(),
        patch: json!({ "tool": tool_id }),
        scope: Scope::User,
    });

    let ops = collect_plan_operations(&paths, &plan);
    assert_eq!(ops.len(), 1);
    let op = &ops[0];
    assert_eq!(op.scope, Scope::User);
    assert!(op.consent_required);
    assert!(op.metadata.consent_required);

    fs::remove_dir_all(&temp_dir).ok();
    Ok(())
}

#[test]
fn ralph_script_is_generated_when_enabled() -> Result<()> {
    let temp_dir = temp_dir("ralph_gen");
    let paths = ProjectPaths::from_root(&temp_dir);

    let mut config = macc_core::config::CanonicalConfig::default();
    config.automation.ralph = Some(macc_core::config::RalphConfig {
        enabled: true,
        iterations_default: 42,
        branch_name: "ralph-branch".to_string(),
        stop_on_failure: true,
    });

    let resolved = macc_core::resolve(&config, &macc_core::resolve::CliOverrides::default());
    let ops = plan_operations(&paths, &resolved, &[], &ToolRegistry::default_registry())?;

    let ralph_op = ops
        .iter()
        .find(|op| op.path == "scripts/ralph.sh")
        .expect("ralph script op not found");
    assert_eq!(ralph_op.kind, macc_core::plan::PlannedOpKind::Write);
    assert!(ralph_op.metadata.set_executable);

    let content = String::from_utf8(ralph_op.after.as_ref().unwrap().clone()).unwrap();
    assert!(content.contains("ITERATIONS=${1:-42}"));
    assert!(content.contains("STOP_ON_FAILURE=true"));

    fs::remove_dir_all(&temp_dir).ok();
    Ok(())
}
