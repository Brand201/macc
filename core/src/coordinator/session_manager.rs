use crate::{MaccError, Result};
use serde_json::{json, Map, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

const TOOL_SESSIONS_REL_PATH: &str = ".macc/state/tool-sessions.json";

#[derive(Debug, Clone, Default)]
pub struct SessionSealOutcome {
    pub sealed: bool,
    pub session_id: Option<String>,
}

pub fn seal_worktree_scoped_session(
    repo_root: &Path,
    tool_id: &str,
    worktree_path: &Path,
    task_id: &str,
    now_iso: &str,
) -> Result<SessionSealOutcome> {
    if tool_id.trim().is_empty() {
        return Ok(SessionSealOutcome::default());
    }

    let scope = read_session_scope(worktree_path).unwrap_or_else(|| "worktree".to_string());
    if scope != "worktree" {
        return Ok(SessionSealOutcome::default());
    }

    let sessions_path = repo_root.join(TOOL_SESSIONS_REL_PATH);
    if !sessions_path.exists() {
        return Ok(SessionSealOutcome::default());
    }
    let lock_dir = sessions_path.with_extension("json.lock");
    acquire_lock(&lock_dir)?;

    let result = (|| {
        let raw = fs::read_to_string(&sessions_path).map_err(|e| MaccError::Io {
            path: sessions_path.to_string_lossy().into(),
            action: "read tool sessions state".into(),
            source: e,
        })?;
        let mut root: Value = serde_json::from_str(&raw).map_err(|e| {
            MaccError::Validation(format!(
                "Failed to parse sessions file '{}': {}",
                sessions_path.display(),
                e
            ))
        })?;

        let tools = root
            .get_mut("tools")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                MaccError::Validation("sessions file missing root .tools object".into())
            })?;
        let Some(tool) = tools.get_mut(tool_id).and_then(Value::as_object_mut) else {
            return Ok(SessionSealOutcome::default());
        };
        let Some(sessions_obj) = tool.get_mut("sessions").and_then(Value::as_object_mut) else {
            return Ok(SessionSealOutcome::default());
        };

        let key_candidates = worktree_key_candidates(worktree_path);
        let mut selected_key: Option<String> = None;
        let mut session_id: Option<String> = None;
        for key in key_candidates {
            let Some(candidate) = sessions_obj.get(&key) else {
                continue;
            };
            let sid = candidate
                .get("session_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if sid.is_empty() {
                continue;
            }
            selected_key = Some(key);
            session_id = Some(sid.to_string());
            break;
        }

        let (Some(selected_key), Some(session_id)) = (selected_key, session_id) else {
            return Ok(SessionSealOutcome::default());
        };

        sessions_obj.remove(&selected_key);

        let archived = tool
            .entry("archived")
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .ok_or_else(|| MaccError::Validation("sessions .archived must be an object".into()))?;
        let archive_key = format!("{}-{}", task_id, now_iso.replace(':', ""));
        archived.insert(
            archive_key,
            json!({
                "task_id": task_id,
                "session_id": session_id,
                "sealed_at": now_iso,
                "scope": "worktree",
                "session_key": selected_key,
                "worktree_path": worktree_path.to_string_lossy().to_string(),
                "reason": "task_commit_completed",
            }),
        );

        if let Some(leases_obj) = tool.get_mut("leases").and_then(Value::as_object_mut) {
            if let Some(lease) = leases_obj
                .get_mut(&session_id)
                .and_then(Value::as_object_mut)
            {
                lease.insert("status".to_string(), Value::String("sealed".to_string()));
                lease.insert("updated_at".to_string(), Value::String(now_iso.to_string()));
            }
        }

        persist_sessions_file(&sessions_path, &root)?;

        Ok(SessionSealOutcome {
            sealed: true,
            session_id: Some(session_id),
        })
    })();

    release_lock(&lock_dir);
    result
}

fn read_session_scope(worktree_path: &Path) -> Option<String> {
    let tool_json_path = worktree_path.join(".macc/tool.json");
    let raw = fs::read_to_string(tool_json_path).ok()?;
    let parsed: Value = serde_json::from_str(&raw).ok()?;
    Some(
        parsed
            .get("performer")
            .and_then(|v| v.get("session"))
            .and_then(|v| v.get("scope"))
            .and_then(Value::as_str)
            .unwrap_or("worktree")
            .to_string(),
    )
}

fn persist_sessions_file(path: &Path, value: &Value) -> Result<()> {
    let mut body = serde_json::to_string_pretty(value).map_err(|e| {
        MaccError::Validation(format!(
            "serialize sessions file '{}': {}",
            path.display(),
            e
        ))
    })?;
    body.push('\n');
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, body).map_err(|e| MaccError::Io {
        path: tmp.to_string_lossy().into(),
        action: "write sessions temp".into(),
        source: e,
    })?;
    fs::rename(&tmp, path).map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "persist sessions state".into(),
        source: e,
    })?;
    Ok(())
}

fn worktree_key_candidates(worktree_path: &Path) -> Vec<String> {
    let mut keys = Vec::new();
    keys.push(worktree_path.to_string_lossy().to_string());
    if let Ok(canon) = fs::canonicalize(worktree_path) {
        let canon_s = canon.to_string_lossy().to_string();
        if !keys.iter().any(|k| k == &canon_s) {
            keys.push(canon_s);
        }
    }
    keys
}

fn acquire_lock(lock_dir: &PathBuf) -> Result<()> {
    for _ in 0..80 {
        match fs::create_dir(lock_dir) {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(err) => {
                return Err(MaccError::Io {
                    path: lock_dir.to_string_lossy().into(),
                    action: "acquire tool session lock".into(),
                    source: err,
                });
            }
        }
    }
    Err(MaccError::Validation(format!(
        "Timed out acquiring tool session lock '{}'",
        lock_dir.display()
    )))
}

fn release_lock(lock_dir: &PathBuf) {
    let _ = fs::remove_dir(lock_dir);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(prefix: &str) -> PathBuf {
        let id = format!(
            "{}_{}_{}",
            prefix,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        std::env::temp_dir().join(id)
    }

    #[test]
    fn seal_session_archives_only_matching_tool() {
        let root = temp_dir("macc_session_manager");
        let worktree = root.join(".macc/worktree/worker-01");
        fs::create_dir_all(worktree.join(".macc")).expect("create worktree");
        fs::create_dir_all(root.join(".macc/state")).expect("create state dir");
        fs::write(
            worktree.join(".macc/tool.json"),
            r#"{"id":"codex","performer":{"session":{"scope":"worktree"}}}"#,
        )
        .expect("write tool json");

        let sessions_path = root.join(TOOL_SESSIONS_REL_PATH);
        let sessions = json!({
            "tools": {
                "codex": {
                    "sessions": {
                        worktree.to_string_lossy().to_string(): {
                            "session_id": "codex-sid",
                            "updated_at": "2026-02-01T00:00:00Z"
                        }
                    },
                    "leases": {
                        "codex-sid": { "status": "active", "owner_worktree": worktree.to_string_lossy().to_string() }
                    }
                },
                "gemini": {
                    "sessions": {
                        worktree.to_string_lossy().to_string(): {
                            "session_id": "gemini-sid",
                            "updated_at": "2026-02-01T00:00:00Z"
                        }
                    },
                    "leases": {
                        "gemini-sid": { "status": "active", "owner_worktree": worktree.to_string_lossy().to_string() }
                    }
                }
            }
        });
        persist_sessions_file(&sessions_path, &sessions).expect("persist sessions seed");

        let outcome = seal_worktree_scoped_session(
            &root,
            "codex",
            &worktree,
            "TASK-1",
            "2026-02-23T00:00:00Z",
        )
        .expect("seal session");
        assert!(outcome.sealed);
        assert_eq!(outcome.session_id.as_deref(), Some("codex-sid"));

        let updated: Value = serde_json::from_str(&fs::read_to_string(&sessions_path).unwrap())
            .expect("updated sessions json");
        assert!(
            updated["tools"]["codex"]["sessions"]
                .as_object()
                .map(|o| o.is_empty())
                .unwrap_or(false),
            "codex session should be removed from active sessions"
        );
        assert_eq!(
            updated["tools"]["gemini"]["sessions"][worktree.to_string_lossy().to_string()]
                ["session_id"]
                .as_str(),
            Some("gemini-sid"),
            "gemini session must stay untouched"
        );

        let _ = fs::remove_dir_all(&root);
    }
}
