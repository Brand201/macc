use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use macc_core::{find_user_home, plan::Action, plan::ActionPlan, plan::Scope, MaccError, Result};
use serde_json::{Map, Value};

pub fn plan_user_mcp_merge(plan: &mut ActionPlan, servers: &BTreeMap<String, Value>) -> Result<()> {
    if servers.is_empty() {
        return Ok(());
    }

    let home = find_user_home().ok_or(MaccError::HomeDirNotFound)?;
    let user_config = home.join(".claude.json");
    let user_path = user_config.to_string_lossy().into_owned();

    let existing = read_user_claude_json(&user_config, &user_path)?;

    let existing_servers = match existing.get("mcpServers") {
        Some(Value::Object(map)) => map.clone(),
        Some(_) => {
            return Err(MaccError::Validation(format!(
                "Expected 'mcpServers' to be an object in {}",
                user_path
            )))
        }
        None => Map::new(),
    };

    let existing_server_names = existing_servers.keys().cloned().collect::<BTreeSet<_>>();
    let missing = servers
        .iter()
        .filter(|(id, _)| !existing_server_names.contains(*id))
        .map(|(id, server)| (id.clone(), server.clone()))
        .collect::<BTreeMap<_, _>>();

    if missing.is_empty() {
        return Ok(());
    }

    let mut patch_servers = Map::new();
    for (id, server) in missing {
        patch_servers.insert(id, server);
    }

    let mut patch = Map::new();
    patch.insert("mcpServers".to_string(), Value::Object(patch_servers));

    plan.add_action(Action::BackupFile {
        path: user_path.clone(),
        scope: Scope::User,
    });
    plan.add_action(Action::MergeJson {
        path: user_path,
        patch: Value::Object(patch),
        scope: Scope::User,
    });

    Ok(())
}

fn read_user_claude_json(path: &Path, display_path: &str) -> Result<Value> {
    if !path.exists() {
        return Ok(Value::Object(Map::new()));
    }

    let content = fs::read_to_string(path).map_err(|e| MaccError::Io {
        path: display_path.to_string(),
        action: "read user Claude config".into(),
        source: e,
    })?;

    if content.trim().is_empty() {
        return Ok(Value::Object(Map::new()));
    }

    serde_json::from_str(&content)
        .map_err(|e| MaccError::Validation(format!("Invalid JSON in {}: {}", display_path, e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use macc_core::plan::Action;
    use serde_json::json;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Mutex;

    static HOME_LOCK: Mutex<()> = Mutex::new(());

    fn with_temp_home<F: FnOnce(PathBuf) -> R, R>(test: F) -> R {
        let temp_dir = std::env::temp_dir().join(format!(
            "claude_user_merge_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&temp_dir).unwrap();
        let guard = HOME_LOCK.lock().unwrap();
        let prev_home = env::var_os("HOME");
        env::set_var("HOME", &temp_dir);
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| test(temp_dir.clone())));
        if let Some(prev) = prev_home {
            env::set_var("HOME", prev);
        } else {
            env::remove_var("HOME");
        }
        fs::remove_dir_all(&temp_dir).unwrap();
        drop(guard);
        match result {
            Ok(value) => value,
            Err(err) => std::panic::resume_unwind(err),
        }
    }

    #[test]
    fn adds_missing_servers_and_preserves_existing() {
        with_temp_home(|home| {
            let user_file = home.join(".claude.json");
            fs::write(
                &user_file,
                serde_json::json!({
                    "mcpServers": {
                        "existing": { "command": "node" }
                    },
                    "other": 1
                })
                .to_string(),
            )
            .unwrap();

            let mut plan = ActionPlan::new();
            let mut servers = BTreeMap::new();
            servers.insert("new-server".to_string(), json!({"command": "python"}));
            servers.insert("existing".to_string(), json!({"command": "node"}));

            plan_user_mcp_merge(&mut plan, &servers).expect("merge should succeed");

            assert_eq!(plan.actions.len(), 2);
            assert!(
                matches!( plan.actions.get(0), Some(Action::BackupFile { path, .. }) if path.ends_with(".claude.json"))
            );
            if let Some(Action::MergeJson { patch, .. }) = plan.actions.get(1) {
                let patch_obj = patch.as_object().expect("patch is object");
                let mcp = patch_obj.get("mcpServers").unwrap().as_object().unwrap();
                assert!(mcp.contains_key("new-server"));
                assert!(!mcp.contains_key("existing"));
            } else {
                panic!("expected MergeJson");
            }
        })
    }

    #[test]
    fn skips_when_no_missing_servers() {
        with_temp_home(|home| {
            let user_file = home.join(".claude.json");
            fs::write(
                &user_file,
                serde_json::json!({
                    "mcpServers": {
                        "existing": { "command": "node" }
                    }
                })
                .to_string(),
            )
            .unwrap();

            let mut plan = ActionPlan::new();
            let mut servers = BTreeMap::new();
            servers.insert("existing".to_string(), json!({"command": "node"}));

            plan_user_mcp_merge(&mut plan, &servers).unwrap();
            assert!(plan.actions.is_empty());
        })
    }

    #[test]
    fn errors_on_invalid_json() {
        with_temp_home(|home| {
            let user_file = home.join(".claude.json");
            fs::write(&user_file, "{ invalid json ").unwrap();

            let mut plan = ActionPlan::new();
            let mut servers = BTreeMap::new();
            servers.insert("new".to_string(), json!({"command": "python"}));

            let result = plan_user_mcp_merge(&mut plan, &servers);
            assert!(result.is_err());
        })
    }
}
