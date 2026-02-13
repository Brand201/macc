use macc_core::{apply, init, plan, plan::ActionStatus, ProjectPaths, Result, ToolRegistry};
use std::fs;

#[test]
fn test_plan_apply_lifecycle() -> Result<()> {
    let temp_dir =
        std::env::temp_dir().join(format!("macc_integration_plan_apply_{}", uuid_v4_like()));
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).unwrap();
    }
    fs::create_dir_all(&temp_dir).unwrap();

    let paths = ProjectPaths::from_root(&temp_dir);

    // 1. Create temp project and run init.
    init(&paths, false)?;

    // Baseline: .macc/ and .gitignore created.
    assert!(paths.config_path.exists());
    assert!(temp_dir.join(".gitignore").exists());

    // 2. Run plan and assert no backups directory created beyond baseline.
    // We expect 0 backups at this point (init doesn't create backups unless .gitignore exists and changed).
    let backups_count = if paths.backups_dir.exists() {
        fs::read_dir(&paths.backups_dir).unwrap().count()
    } else {
        0
    };

    plan(&paths, Some("test"), &[], &ToolRegistry::default_registry())?;

    let backups_count_after_plan = if paths.backups_dir.exists() {
        fs::read_dir(&paths.backups_dir).unwrap().count()
    } else {
        0
    };
    assert_eq!(
        backups_count, backups_count_after_plan,
        "Plan should not create any backups"
    );

    // Assert no generated files exist yet
    assert!(!temp_dir.join("MACC_GENERATED.txt").exists());

    // 3. Run apply and assert generated files exist.
    let report = apply(
        &paths,
        Some("test"),
        &[],
        false,
        false,
        &ToolRegistry::default_registry(),
    )?;

    assert!(temp_dir.join("MACC_GENERATED.txt").exists());
    assert!(temp_dir.join(".macc/test-output.json").exists());
    assert!(temp_dir.join(".env.example").exists());

    // Check report outcomes
    assert_eq!(
        report.outcomes.get("MACC_GENERATED.txt"),
        Some(&ActionStatus::Created)
    );
    assert_eq!(
        report.outcomes.get(".macc/test-output.json"),
        Some(&ActionStatus::Created)
    );
    assert_eq!(
        report.outcomes.get(".env.example"),
        Some(&ActionStatus::Created)
    );

    // 4. Modify a generated file, run apply again, and assert backup created.
    let target_file = temp_dir.join("MACC_GENERATED.txt");
    fs::write(&target_file, "Modified content").unwrap();

    // Wait to ensure timestamp for backup directory is likely different or at least we can find it.
    std::thread::sleep(std::time::Duration::from_millis(1000)); // 1s to be sure about timestamp dir

    let report2 = apply(
        &paths,
        Some("test"),
        &[],
        false,
        false,
        &ToolRegistry::default_registry(),
    )?;

    assert_eq!(
        report2.outcomes.get("MACC_GENERATED.txt"),
        Some(&ActionStatus::Updated)
    );
    assert!(
        report2.backup_dir.is_some(),
        "Backup should have been created for updated file"
    );

    let backup_dir = report2.backup_dir.as_ref().unwrap();
    assert!(backup_dir.join("MACC_GENERATED.txt").exists());
    assert_eq!(
        fs::read_to_string(backup_dir.join("MACC_GENERATED.txt")).unwrap(),
        "Modified content"
    );

    // 5. Run apply third time and assert no changes (unchanged statuses).
    let report3 = apply(
        &paths,
        Some("test"),
        &[],
        false,
        false,
        &ToolRegistry::default_registry(),
    )?;

    assert_eq!(
        report3.outcomes.get("MACC_GENERATED.txt"),
        Some(&ActionStatus::Unchanged)
    );
    assert_eq!(
        report3.outcomes.get(".macc/test-output.json"),
        Some(&ActionStatus::Unchanged)
    );
    assert!(
        report3.backup_dir.is_none(),
        "No backup should be created when nothing changed"
    );

    // Cleanup
    fs::remove_dir_all(&temp_dir).ok();

    Ok(())
}

#[test]
fn test_apply_with_mcp_selection() -> Result<()> {
    let temp_dir =
        std::env::temp_dir().join(format!("macc_integration_mcp_selection_{}", uuid_v4_like()));
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).unwrap();
    }
    fs::create_dir_all(&temp_dir).unwrap();

    let paths = ProjectPaths::from_root(&temp_dir);

    // 1. Run init to setup baseline
    init(&paths, false)?;

    // 2. Modify macc.yaml to select an MCP server
    let config_content = r#"
version: v1
tools:
  enabled: [test]
selections:
  mcp: [brave-search]
mcp_templates:
  - id: brave-search
    title: Brave Search
    description: Brave Search MCP server
    command: node
    args: [brave.js]
    env_placeholders:
      - name: API_KEY
        placeholder: ${BRAVE_API_KEY}
"#;
    fs::write(&paths.config_path, config_content).unwrap();

    // 3. Run apply
    let report = apply(
        &paths,
        None, // Use enabled tools from config
        &[],
        false,
        false,
        &ToolRegistry::default_registry(),
    )?;

    // 4. Assert .mcp.json exists and has expected content
    let mcp_json_path = temp_dir.join(".mcp.json");
    assert!(mcp_json_path.exists());

    let content = fs::read_to_string(&mcp_json_path).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();

    assert!(json["mcpServers"]["brave-search"].is_object());
    assert_eq!(json["mcpServers"]["brave-search"]["command"], "node");
    assert_eq!(
        json["mcpServers"]["brave-search"]["env"]["API_KEY"],
        "${BRAVE_API_KEY}"
    );

    // Check report outcomes
    assert_eq!(
        report.outcomes.get(".mcp.json"),
        Some(&ActionStatus::Created)
    );

    // 5. Run apply again and verify idempotence
    let report2 = apply(
        &paths,
        None,
        &[],
        false,
        false,
        &ToolRegistry::default_registry(),
    )?;

    assert_eq!(
        report2.outcomes.get(".mcp.json"),
        Some(&ActionStatus::Unchanged)
    );

    // Cleanup
    fs::remove_dir_all(&temp_dir).ok();

    Ok(())
}

fn uuid_v4_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    format!("{:?}", since_the_epoch.as_nanos())
}
