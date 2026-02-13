use macc_core::{init, ProjectPaths, Result, BASELINE_IGNORE_ENTRIES};
use std::fs;

#[test]
fn test_init_creates_canonical_structure_and_is_idempotent() -> Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("macc_integration_init_{}", uuid_v4_like()));
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).unwrap();
    }
    fs::create_dir_all(&temp_dir).unwrap();

    let paths = ProjectPaths::from_root(&temp_dir);

    // 1. First run: should create everything
    init(&paths, false)?;

    // Assert directories exist
    assert!(paths.macc_dir.is_dir(), ".macc directory should exist");
    assert!(paths.backups_dir.is_dir(), ".macc/backups should exist");
    assert!(paths.tmp_dir.is_dir(), ".macc/tmp should exist");

    // Assert config exists
    assert!(paths.config_path.is_file(), ".macc/macc.yaml should exist");
    let config_content = fs::read_to_string(&paths.config_path).unwrap();
    assert!(config_content.contains("version: v1"));
    assert!(config_content.contains("enabled:"));

    // Assert .gitignore exists and contains baseline entries
    let gitignore_path = temp_dir.join(".gitignore");
    assert!(gitignore_path.is_file(), ".gitignore should exist");
    let gitignore_content = fs::read_to_string(&gitignore_path).unwrap();
    for entry in BASELINE_IGNORE_ENTRIES {
        assert!(
            gitignore_content.contains(entry),
            "Missing gitignore entry: {}",
            entry
        );
    }

    // Capture modification times
    let config_mtime = fs::metadata(&paths.config_path)
        .unwrap()
        .modified()
        .unwrap();
    let gitignore_mtime = fs::metadata(&gitignore_path).unwrap().modified().unwrap();

    // 2. Second run: should be idempotent (no changes)
    // Wait a tiny bit to ensure mtime would change if written
    std::thread::sleep(std::time::Duration::from_millis(10));

    init(&paths, false)?;

    let config_mtime_2 = fs::metadata(&paths.config_path)
        .unwrap()
        .modified()
        .unwrap();
    let gitignore_mtime_2 = fs::metadata(&gitignore_path).unwrap().modified().unwrap();

    assert_eq!(
        config_mtime, config_mtime_2,
        "Config file should not be rewritten"
    );
    assert_eq!(
        gitignore_mtime, gitignore_mtime_2,
        ".gitignore should not be rewritten"
    );

    // 3. Third run with force: should overwrite config
    // Modify config first
    fs::write(&paths.config_path, "version: v0\nmodified: true").unwrap();
    let modified_mtime = fs::metadata(&paths.config_path)
        .unwrap()
        .modified()
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(10));

    init(&paths, true)?;

    let config_mtime_3 = fs::metadata(&paths.config_path)
        .unwrap()
        .modified()
        .unwrap();
    assert_ne!(
        modified_mtime, config_mtime_3,
        "Config file should be rewritten with force: true"
    );

    let final_content = fs::read_to_string(&paths.config_path).unwrap();
    assert!(
        final_content.contains("version: v1"),
        "Config should be reset to default"
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
