use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub type ToolFiles = Vec<(String, Vec<u8>)>;

pub const SKILL_MARKERS: &[&str] = &["SKILL.md", "skill.md", "README.md"];

pub fn validate_skill_folder(path: &Path, require_manifest: bool) -> Result<(), String> {
    if !path.exists() {
        return Err(format!("Skill folder does not exist: {}", path.display()));
    }
    if !path.is_dir() {
        return Err(format!("Skill path is not a directory: {}", path.display()));
    }

    if require_manifest {
        let manifest_path = path.join("macc.package.json");
        if !manifest_path.is_file() {
            if has_skill_marker(path) {
                return Ok(());
            }
            return Err(format!(
                "Skill folder '{}' is missing 'macc.package.json'",
                path.display()
            ));
        }

        let manifest: PackageManifest = load_manifest(&manifest_path)?;
        if manifest.r#type != "skill" {
            return Err(format!(
                "Skill manifest at '{}' has invalid type '{}' (expected 'skill')",
                manifest_path.display(),
                manifest.r#type
            ));
        }
        return Ok(());
    }

    if has_skill_marker(path) {
        return Ok(());
    }

    Err(format!(
        "Skill folder '{}' does not contain any marker files ({})",
        path.display(),
        SKILL_MARKERS.join(", ")
    ))
}

fn has_skill_marker(path: &Path) -> bool {
    SKILL_MARKERS
        .iter()
        .any(|marker| path.join(marker).is_file())
}

#[derive(Debug, Clone, Deserialize)]
pub struct PackageManifest {
    pub r#type: String,
    pub id: String,
    pub version: String,
    pub targets: BTreeMap<String, Vec<PackageTarget>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PackageTarget {
    pub src: String,
    pub dest: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpManifest {
    pub r#type: String,
    pub id: String,
    pub version: String,
    pub mcp: McpDetails,
    pub merge_target: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpDetails {
    pub server: serde_json::Value,
}

pub fn validate_mcp_folder(path: &Path, expected_id: &str) -> Result<McpManifest, String> {
    if !path.exists() {
        return Err(format!("MCP folder does not exist: {}", path.display()));
    }
    if !path.is_dir() {
        return Err(format!("MCP path is not a directory: {}", path.display()));
    }

    let manifest_path = path.join("macc.package.json");
    if !manifest_path.is_file() {
        return Err(format!(
            "MCP folder '{}' is missing 'macc.package.json'",
            path.display()
        ));
    }

    let manifest: McpManifest = load_mcp_manifest(&manifest_path)?;

    if manifest.r#type != "mcp" {
        return Err(format!(
            "MCP manifest at '{}' has invalid type '{}' (expected 'mcp')",
            manifest_path.display(),
            manifest.r#type
        ));
    }

    if manifest.id != expected_id {
        return Err(format!(
            "MCP manifest at '{}' id mismatch: found '{}', expected '{}'",
            manifest_path.display(),
            manifest.id,
            expected_id
        ));
    }

    if manifest.merge_target.is_empty() {
        return Err(format!(
            "MCP manifest at '{}' is missing 'merge_target'",
            manifest_path.display()
        ));
    }

    Ok(manifest)
}

pub fn load_mcp_manifest(path: &Path) -> Result<McpManifest, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read manifest {}: {}", path.display(), e))?;
    serde_json::from_str(&content)
        .map_err(|e| format!("Failed to parse MCP manifest {}: {}", path.display(), e))
}

pub fn load_manifest(path: &Path) -> Result<PackageManifest, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read manifest {}: {}", path.display(), e))?;
    serde_json::from_str(&content)
        .map_err(|e| format!("Failed to parse manifest {}: {}", path.display(), e))
}

pub fn collect_tool_files(
    package_root: &Path,
    manifest: &PackageManifest,
    tool: &str,
) -> Result<ToolFiles, String> {
    let mut outputs = Vec::new();
    let targets = match manifest.targets.get(tool) {
        Some(targets) => targets,
        None => return Ok(outputs),
    };

    for target in targets {
        let mut entries = expand_target(package_root, target)?;
        outputs.append(&mut entries);
    }

    Ok(outputs)
}

pub fn load_cached_tool_files(
    macc_dir: &Path,
    package_id: &str,
    tool: &str,
) -> Result<Option<ToolFiles>, String> {
    let package_root = macc_dir.join("cache").join(package_id);
    let manifest_path = package_root.join("macc.package.json");
    if !manifest_path.exists() {
        return Ok(None);
    }

    let manifest = load_manifest(&manifest_path)?;
    let files = collect_tool_files(&package_root, &manifest, tool)?;
    if files.is_empty() {
        Ok(None)
    } else {
        Ok(Some(files))
    }
}

fn expand_target(package_root: &Path, target: &PackageTarget) -> Result<ToolFiles, String> {
    let src = target.src.trim_end_matches("/*");
    let src_path = package_root.join(src);
    let dest_base = PathBuf::from(target.dest.trim_end_matches("/*"));

    if src_path.is_file() {
        let dest = resolve_dest_for_file(&dest_base, &src_path, &target.dest);
        let bytes = std::fs::read(&src_path)
            .map_err(|e| format!("Failed to read {}: {}", src_path.display(), e))?;
        return Ok(vec![(normalize_path(&dest), bytes)]);
    }

    if !src_path.is_dir() {
        return Err(format!("Source path not found: {}", src_path.display()));
    }

    let mut outputs = Vec::new();
    collect_dir_files(&src_path, &dest_base, &src_path, &mut outputs)?;
    Ok(outputs)
}

fn collect_dir_files(
    root: &Path,
    dest_base: &Path,
    current: &Path,
    outputs: &mut ToolFiles,
) -> Result<(), String> {
    let entries = std::fs::read_dir(current)
        .map_err(|e| format!("Failed to read {}: {}", current.display(), e))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("Read dir entry failed: {}", e))?;
        let path = entry.path();
        if path.is_dir() {
            collect_dir_files(root, dest_base, &path, outputs)?;
            continue;
        }
        if !path.is_file() {
            continue;
        }
        let rel = path
            .strip_prefix(root)
            .map_err(|e| format!("Path error: {}", e))?;
        let dest = dest_base.join(rel);
        let bytes = std::fs::read(&path)
            .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
        outputs.push((normalize_path(&dest), bytes));
    }
    Ok(())
}

fn resolve_dest_for_file(dest_base: &Path, src_path: &Path, dest_raw: &str) -> PathBuf {
    if dest_raw.ends_with('/') || dest_raw.ends_with("/*") {
        if let Some(name) = src_path.file_name() {
            return dest_base.join(name);
        }
    }
    dest_base.to_path_buf()
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_dir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("macc_test_{}", name));
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn tool_id() -> String {
        format!("tool-{}", uuid_v4_like())
    }

    fn uuid_v4_like() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{:x}", nanos)
    }

    #[test]
    fn test_validate_skill_folder_ok() {
        let path = temp_dir("validate_ok");

        // No markers yet
        assert!(validate_skill_folder(&path, false).is_err());

        // Add README.md
        fs::write(path.join("README.md"), "hello").unwrap();
        assert!(validate_skill_folder(&path, false).is_ok());

        // Remove README.md and add SKILL.md
        fs::remove_file(path.join("README.md")).unwrap();
        fs::write(path.join("SKILL.md"), "hello").unwrap();
        assert!(validate_skill_folder(&path, false).is_ok());

        fs::remove_dir_all(&path).ok();
    }

    #[test]
    fn test_validate_skill_folder_fails() {
        let path = temp_dir("validate_fails");

        // Empty dir
        let err = validate_skill_folder(&path, false).unwrap_err();
        assert!(err.contains("does not contain any marker files"));

        // File instead of dir
        let file_path = path.join("not_a_dir");
        fs::write(&file_path, "not a dir").unwrap();
        let err = validate_skill_folder(&file_path, false).unwrap_err();
        assert!(err.contains("is not a directory"));

        // Non-existent
        let non_existent = path.join("nope");
        let err = validate_skill_folder(&non_existent, false).unwrap_err();
        assert!(err.contains("does not exist"));

        fs::remove_dir_all(&path).ok();
    }

    #[test]
    fn test_validate_skill_folder_requires_manifest() {
        let path = temp_dir("validate_manifest_required");
        std::fs::write(path.join("SKILL.md"), "skill content").unwrap();

        assert!(validate_skill_folder(&path, true).is_ok());

        let tool_id = tool_id();
        let manifest = format!(
            r#"{{
  "type": "skill",
  "id": "test",
  "version": "0.1.0",
  "targets": {{
    "{tool_id}": [
      {{ "src": "SKILL.md", "dest": ".{tool_id}/skills/test/SKILL.md" }}
    ]
  }}
}}
"#
        );
        std::fs::write(path.join("macc.package.json"), manifest).unwrap();
        assert!(validate_skill_folder(&path, true).is_ok());

        std::fs::remove_file(path.join("SKILL.md")).unwrap();
        std::fs::remove_file(path.join("macc.package.json")).unwrap();
        let err = validate_skill_folder(&path, true).unwrap_err();
        assert!(err.contains("missing 'macc.package.json'"));

        std::fs::remove_dir_all(&path).ok();
    }

    #[test]
    fn test_validate_mcp_folder_ok() {
        let path = temp_dir("mcp_ok");
        let manifest = r#"{ 
            "type": "mcp",
            "id": "test-mcp",
            "version": "1.0.0",
            "mcp": {
                "server": {
                    "command": "node",
                    "args": ["index.js"]
                }
            },
            "merge_target": "mcpServers.test-mcp"
        }"#;
        fs::write(path.join("macc.package.json"), manifest).unwrap();

        let res = validate_mcp_folder(&path, "test-mcp").unwrap();
        assert_eq!(res.id, "test-mcp");
        assert_eq!(res.merge_target, "mcpServers.test-mcp");

        fs::remove_dir_all(&path).ok();
    }

    #[test]
    fn test_validate_mcp_folder_fails() {
        let path = temp_dir("mcp_fails");

        // Missing manifest
        let err = validate_mcp_folder(&path, "any").unwrap_err();
        assert!(err.contains("missing 'macc.package.json'"));

        // Invalid JSON
        fs::write(path.join("macc.package.json"), "{ invalid").unwrap();
        let err = validate_mcp_folder(&path, "any").unwrap_err();
        assert!(err.contains("Failed to parse MCP manifest"));

        // Wrong type
        let manifest_wrong_type = r#"{ 
            "type": "skill",
            "id": "test-mcp",
            "version": "1.0.0",
            "mcp": { "server": {} },
            "merge_target": "target"
        }"#;
        fs::write(path.join("macc.package.json"), manifest_wrong_type).unwrap();
        let err = validate_mcp_folder(&path, "test-mcp").unwrap_err();
        assert!(err.contains("invalid type 'skill'"));

        // ID mismatch
        let manifest_id_mismatch = r#"{ 
            "type": "mcp",
            "id": "wrong-id",
            "version": "1.0.0",
            "mcp": { "server": {} },
            "merge_target": "target"
        }"#;
        fs::write(path.join("macc.package.json"), manifest_id_mismatch).unwrap();
        let err = validate_mcp_folder(&path, "test-mcp").unwrap_err();
        assert!(err.contains("id mismatch"));

        fs::remove_dir_all(&path).ok();
    }
}
