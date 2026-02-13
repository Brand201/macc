use crate::plan::{Action, ActionPlan, Scope};
use std::path::{Path, PathBuf};

pub fn write_file(plan: &mut ActionPlan, path: impl Into<String>, content: Vec<u8>) {
    plan.add_action(Action::WriteFile {
        path: path.into(),
        content,
        scope: Scope::Project,
    });
}

pub fn write_text(plan: &mut ActionPlan, path: impl Into<String>, content: &str) {
    write_file(plan, path, content.as_bytes().to_vec());
}

pub fn expand_directory_to_plan(
    plan: &mut ActionPlan,
    src_dir: &Path,
    dest_root: &Path,
) -> Result<(), String> {
    if !src_dir.is_dir() {
        return Err(format!("Source is not a directory: {}", src_dir.display()));
    }

    let mut files = Vec::new();
    collect_files_recursive(src_dir, Path::new(""), &mut files)?;

    // Sort paths for determinism
    files.sort_by(|a, b| a.0.cmp(&b.0));

    for (rel_path, abs_path) in files {
        let content = std::fs::read(&abs_path)
            .map_err(|e| format!("Failed to read file {}: {}", abs_path.display(), e))?;

        let dest_path = dest_root.join(rel_path);
        let dest_path_str = dest_path.to_string_lossy().replace('\\', "/");

        write_file(plan, &dest_path_str, content);
    }

    Ok(())
}

fn collect_files_recursive(
    base: &Path,
    rel: &Path,
    files: &mut Vec<(PathBuf, PathBuf)>,
) -> Result<(), String> {
    let current = base.join(rel);
    let entries = std::fs::read_dir(&current)
        .map_err(|e| format!("Failed to read directory {}: {}", current.display(), e))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
        let file_type = entry
            .file_type()
            .map_err(|e| format!("Failed to get file type: {}", e))?;

        let path = entry.path();
        let name = entry.file_name();
        let new_rel = rel.join(name);

        if file_type.is_dir() {
            collect_files_recursive(base, &new_rel, files)?;
        } else if file_type.is_symlink() {
            return Err(format!("Symlinks are not supported: {}", path.display()));
        } else if file_type.is_file() {
            files.push((new_rel, path));
        } else {
            return Err(format!("Unsupported file type at: {}", path.display()));
        }
    }
    Ok(())
}

pub fn plan_skill_install(
    plan: &mut ActionPlan,
    tool: &str,
    skill_id: &str,
    materialized_root: &Path,
    subpath: &str,
) -> Result<(), String> {
    let skill_path = if subpath.is_empty() || subpath == "." {
        materialized_root.to_path_buf()
    } else {
        materialized_root.join(subpath)
    };

    // 1. Validate with heuristics
    crate::packages::validate_skill_folder(&skill_path, true)?;

    // 2. Destination root for this skill (tool-agnostic convention)
    let config_dir = format!(".{}", tool);
    let dest_skill_root = PathBuf::from(config_dir).join("skills").join(skill_id);

    // 3. Expand directory -> WriteFile actions
    expand_directory_to_plan(plan, &skill_path, &dest_skill_root)?;

    Ok(())
}

pub fn plan_mcp_install(
    plan: &mut ActionPlan,
    mcp_id: &str,
    materialized_root: &Path,
    subpath: &str,
) -> Result<crate::packages::McpManifest, String> {
    let mcp_path = if subpath.is_empty() || subpath == "." {
        materialized_root.to_path_buf()
    } else {
        materialized_root.join(subpath)
    };

    // 1. Load and validate manifest
    let manifest = crate::packages::validate_mcp_folder(&mcp_path, mcp_id)?;

    // 2. Build JSON fragment for merge
    let patch = build_patch_from_merge_target(&manifest.merge_target, manifest.mcp.server.clone())?;

    // 3. Plan merge into .mcp.json
    plan.add_action(Action::MergeJson {
        path: ".mcp.json".into(),
        patch,
        scope: Scope::Project,
    });

    Ok(manifest)
}

fn build_patch_from_merge_target(
    merge_target: &str,
    value: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let parts: Vec<&str> = merge_target.split('.').map(|p| p.trim()).collect();
    if parts.is_empty() || parts.iter().any(|p| p.is_empty()) {
        return Err(format!("Invalid merge_target: '{}'", merge_target));
    }

    let mut current = value;
    for key in parts.into_iter().rev() {
        let mut map = serde_json::Map::new();
        map.insert(key.to_string(), current);
        current = serde_json::Value::Object(map);
    }

    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_dir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "macc_install_test_{}_{}",
            name,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn uuid_v4_like() -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{:x}", nanos)
    }

    #[test]
    fn test_expand_directory_to_plan() {
        let src = temp_dir("src");
        fs::write(src.join("a.txt"), "a").unwrap();
        fs::create_dir(src.join("sub")).unwrap();
        fs::write(src.join("sub/b.txt"), "b").unwrap();

        let mut plan = ActionPlan::new();
        expand_directory_to_plan(&mut plan, &src, Path::new("dest")).unwrap();
        plan.normalize();

        assert_eq!(plan.actions.len(), 2);
        // Sorted actions in plan: a.txt then sub/b.txt (because of normalize() in build())
        match &plan.actions[0] {
            Action::WriteFile { path, content, .. } => {
                assert_eq!(path, "dest/a.txt");
                assert_eq!(content, b"a");
            }
            _ => panic!("Expected WriteFile"),
        }
        match &plan.actions[1] {
            Action::WriteFile { path, content, .. } => {
                assert_eq!(path, "dest/sub/b.txt");
                assert_eq!(content, b"b");
            }
            _ => panic!("Expected WriteFile"),
        }

        fs::remove_dir_all(&src).ok();
    }

    #[test]
    fn test_expand_directory_rejects_symlinks() {
        #[cfg(unix)]
        {
            let src = temp_dir("src_symlink");
            fs::write(src.join("real.txt"), "real").unwrap();
            std::os::unix::fs::symlink(src.join("real.txt"), src.join("link.txt")).unwrap();

            let mut plan = ActionPlan::new();
            let res = expand_directory_to_plan(&mut plan, &src, Path::new("dest"));
            assert!(res.is_err());
            assert!(res.unwrap_err().contains("Symlinks are not supported"));

            fs::remove_dir_all(&src).ok();
        }
    }

    #[test]
    fn test_plan_skill_install() {
        let root = temp_dir("skill_install");
        let skill_dir = root.join("my-skill");
        fs::create_dir(&skill_dir).unwrap();
        let tool_id = format!("tool-{}", uuid_v4_like());
        let manifest = format!(
            r#"{{
  "type": "skill",
  "id": "my-skill",
  "version": "0.1.0",
  "targets": {{
    "{tool_id}": [
      {{ "src": "SKILL.md", "dest": ".{tool_id}/skills/my-skill/SKILL.md" }}
    ]
  }}
}}
"#
        );
        fs::write(skill_dir.join("macc.package.json"), manifest).unwrap();
        fs::write(skill_dir.join("SKILL.md"), "skill content").unwrap();

        let mut plan = ActionPlan::new();
        plan_skill_install(&mut plan, &tool_id, "my-skill", &root, "my-skill").unwrap();
        plan.normalize();

        assert_eq!(plan.actions.len(), 2);
        let mut paths = plan
            .actions
            .iter()
            .filter_map(|action| match action {
                Action::WriteFile { path, .. } => Some(path.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        paths.sort();
        assert_eq!(
            paths,
            vec![
                format!(".{}/skills/my-skill/SKILL.md", tool_id),
                format!(".{}/skills/my-skill/macc.package.json", tool_id)
            ]
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn test_plan_mcp_install() {
        let root = temp_dir("mcp_install");
        let mcp_dir = root.join("my-mcp");
        fs::create_dir(&mcp_dir).unwrap();
        let manifest = serde_json::json!({
            "type": "mcp",
            "id": "my-mcp",
            "version": "1.0.0",
            "mcp": {
                "server": {
                    "command": "node",
                    "args": ["index.js"]
                }
            },
            "merge_target": "mcpServers.my-mcp"
        });
        fs::write(
            mcp_dir.join("macc.package.json"),
            serde_json::to_string(&manifest).unwrap(),
        )
        .unwrap();

        let mut plan = ActionPlan::new();
        let manifest = plan_mcp_install(&mut plan, "my-mcp", &root, "my-mcp").unwrap();
        assert_eq!(manifest.id, "my-mcp");
        plan.normalize();

        assert_eq!(plan.actions.len(), 1);
        match &plan.actions[0] {
            Action::MergeJson { path, patch, .. } => {
                assert_eq!(path, ".mcp.json");
                assert_eq!(
                    patch["mcpServers"]["my-mcp"]["command"],
                    serde_json::Value::String("node".into())
                );
            }
            _ => panic!("Expected MergeJson"),
        }

        fs::remove_dir_all(&root).ok();
    }
}
