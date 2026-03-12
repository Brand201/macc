use crate::coordinator::model::TaskRegistry;
use crate::coordinator::runtime as coordinator_runtime;
use crate::coordinator_storage::append_event_sqlite;
use crate::{MaccError, Result};
use std::collections::HashSet;
use std::path::Path;

pub fn now_iso_coordinator() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

pub fn set_registry_updated_at(registry: &mut serde_json::Value) {
    if let Ok(mut typed) = TaskRegistry::from_value(registry) {
        typed.set_updated_at(now_iso_coordinator());
        if let Ok(next) = typed.to_value() {
            *registry = next;
            return;
        }
    }
    registry["updated_at"] = serde_json::Value::String(now_iso_coordinator());
}

pub fn recompute_resource_locks_from_tasks(registry: &mut serde_json::Value) {
    if let Ok(mut typed) = TaskRegistry::from_value(registry) {
        typed.recompute_resource_locks(&now_iso_coordinator());
        if let Ok(next) = typed.to_value() {
            *registry = next;
            return;
        }
    }

    let mut locks = serde_json::Map::new();
    if let Some(tasks) = registry.get("tasks").and_then(serde_json::Value::as_array) {
        for task in tasks {
            let state = task
                .get("state")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("todo");
            if !matches!(
                state,
                "claimed" | "in_progress" | "pr_open" | "changes_requested" | "queued"
            ) {
                continue;
            }
            let task_id = task
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if task_id.is_empty() {
                continue;
            }
            let worktree_path = task
                .get("worktree")
                .and_then(|w| w.get("worktree_path"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            for res in task
                .get("exclusive_resources")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default()
            {
                let res_name = res.as_str().unwrap_or_default();
                if res_name.is_empty() {
                    continue;
                }
                locks.insert(
                    res_name.to_string(),
                    serde_json::json!({
                        "task_id": task_id,
                        "worktree_path": worktree_path,
                        "locked_at": now_iso_coordinator(),
                    }),
                );
            }
        }
    }
    registry["resource_locks"] = serde_json::Value::Object(locks);
}

fn sanitize_slug(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if ch == '-' || ch == '_' || ch == ' ' {
            out.push('-');
        }
    }
    while out.contains("--") {
        out = out.replace("--", "-");
    }
    out.trim_matches('-').to_string()
}

fn is_worktree_clean(worktree_path: &Path) -> Result<bool> {
    Ok(!crate::git::is_dirty(worktree_path)?)
}

fn active_task_worktree_paths(registry: &serde_json::Value) -> HashSet<String> {
    if let Ok(typed) = TaskRegistry::from_value(registry) {
        return typed.active_task_worktree_paths();
    }
    let mut out = HashSet::new();
    if let Some(tasks) = registry.get("tasks").and_then(serde_json::Value::as_array) {
        for task in tasks {
            let state = task
                .get("state")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("todo");
            if !matches!(
                state,
                "claimed" | "in_progress" | "pr_open" | "changes_requested" | "queued"
            ) {
                continue;
            }
            if let Some(path) = task
                .get("worktree")
                .and_then(|w| w.get("worktree_path"))
                .and_then(serde_json::Value::as_str)
            {
                if !path.is_empty() {
                    out.insert(path.to_string());
                }
            }
        }
    }
    out
}

fn can_reuse_worktree_slot(registry: &serde_json::Value, worktree_path: &Path) -> bool {
    if let Ok(typed) = TaskRegistry::from_value(registry) {
        return typed.can_reuse_worktree_slot(&worktree_path.to_string_lossy());
    }
    let key = worktree_path.to_string_lossy().to_string();
    let mut seen = false;
    let mut all_merged = true;
    if let Some(tasks) = registry.get("tasks").and_then(serde_json::Value::as_array) {
        for task in tasks {
            let path = task
                .get("worktree")
                .and_then(|w| w.get("worktree_path"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if path != key {
                continue;
            }
            seen = true;
            let state = task
                .get("state")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("todo");
            if state != "merged" {
                all_merged = false;
            }
        }
    }
    seen && all_merged
}

fn has_in_progress_or_queued_on_worktree(
    registry: &serde_json::Value,
    worktree_path: &Path,
) -> bool {
    if let Ok(typed) = TaskRegistry::from_value(registry) {
        return typed.has_in_progress_or_queued_on_worktree(&worktree_path.to_string_lossy());
    }
    let key = worktree_path.to_string_lossy().to_string();
    registry
        .get("tasks")
        .and_then(serde_json::Value::as_array)
        .map(|tasks| {
            tasks.iter().any(|task| {
                let state = task
                    .get("state")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("todo");
                if !matches!(state, "in_progress" | "queued") {
                    return false;
                }
                let path = task
                    .get("worktree")
                    .and_then(|w| w.get("worktree_path"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                path == key
            })
        })
        .unwrap_or(false)
}

fn write_worktree_metadata_file(
    worktree_path: &Path,
    metadata: &crate::WorktreeMetadata,
) -> Result<()> {
    let macc_dir = worktree_path.join(".macc");
    std::fs::create_dir_all(&macc_dir).map_err(|e| MaccError::Io {
        path: macc_dir.to_string_lossy().into(),
        action: "create worktree .macc dir".into(),
        source: e,
    })?;
    let path = macc_dir.join("worktree.json");
    let data = serde_json::to_string_pretty(metadata).map_err(|e| {
        MaccError::Validation(format!("Failed to serialize worktree metadata: {}", e))
    })?;
    std::fs::write(&path, data).map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "write worktree metadata".into(),
        source: e,
    })
}

pub fn build_non_task_worker_slug(worker_count: usize) -> String {
    format!("worker-{:02}", worker_count + 1)
}

fn build_reuse_branch_name(tool: &str, worktree_path: &Path) -> String {
    let slot = sanitize_slug(
        worktree_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("worker"),
    );
    let ts = chrono::Utc::now().format("%Y%m%d%H%M%S");
    format!(
        "ai/{}/{}-{}",
        tool,
        if slot.is_empty() { "worker" } else { &slot },
        ts
    )
}

fn git_current_branch_name(worktree_path: &Path) -> Option<String> {
    crate::git::current_branch(worktree_path).ok()
}

fn prepare_reused_worktree_base(worktree_path: &Path, base_branch: &str) -> Result<(bool, bool)> {
    if !crate::git::reset_hard(worktree_path, "HEAD")? {
        return Ok((false, false));
    }
    if !crate::git::clean_fd(worktree_path)? {
        return Ok((false, false));
    }
    if !crate::git::checkout(worktree_path, base_branch, false)?
        && !crate::git::checkout_reset_branch(worktree_path, base_branch, false)?
    {
        return Ok((false, false));
    }
    if !crate::git::fetch(worktree_path, "origin")? {
        return Ok((false, false));
    }
    if !crate::git::reset_hard(worktree_path, base_branch)? {
        return Ok((false, false));
    }
    if !crate::git::reset_hard(worktree_path, "HEAD")? {
        return Ok((false, false));
    }
    if !crate::git::clean_fd(worktree_path)? {
        return Ok((false, false));
    }
    Ok((true, false))
}

fn is_branch_merged_into_base(worktree_path: &Path, branch: &str, base_branch: &str) -> bool {
    if branch.is_empty() || branch == base_branch {
        return true;
    }
    let exists = crate::git::rev_parse_verify(worktree_path, branch).unwrap_or(false);
    if !exists {
        return true;
    }
    crate::git::merge_base_is_ancestor(worktree_path, branch, base_branch).unwrap_or(false)
}

pub fn find_reusable_worktree_native(
    repo_root: &Path,
    registry: &serde_json::Value,
    tool: &str,
    base_branch: &str,
) -> Result<(
    Option<(std::path::PathBuf, String, String, bool, bool)>,
    Option<(String, String)>,
)> {
    let active_paths = active_task_worktree_paths(registry);
    let pool_root = repo_root.join(".macc").join("worktree");
    let entries = crate::list_worktrees(repo_root)?;
    let mut last_prepare_error: Option<(String, String)> = None;
    for entry in entries {
        if !entry.path.starts_with(&pool_root) {
            continue;
        }
        let key = entry.path.to_string_lossy().to_string();
        if active_paths.contains(&key) {
            continue;
        }
        if !can_reuse_worktree_slot(registry, &entry.path) {
            continue;
        }
        let dirty_before = !is_worktree_clean(&entry.path)?;
        if dirty_before && has_in_progress_or_queued_on_worktree(registry, &entry.path) {
            last_prepare_error = Some((
                "dirty_inflight_guard".to_string(),
                format!(
                    "worktree {} is dirty and still assigned to an in_progress/queued task",
                    entry.path.display()
                ),
            ));
            continue;
        }
        let merge_head = crate::git::rev_parse_verify(&entry.path, "MERGE_HEAD").unwrap_or(false);
        if merge_head {
            last_prepare_error = Some((
                "merge_head_present".to_string(),
                format!(
                    "worktree {} has unresolved MERGE_HEAD",
                    entry.path.display()
                ),
            ));
            continue;
        }
        let base_ok = crate::git::rev_parse_verify(&entry.path, base_branch).unwrap_or(false);
        if !base_ok {
            last_prepare_error = Some((
                "base_branch_missing".to_string(),
                format!(
                    "worktree {} cannot resolve base branch {}",
                    entry.path.display(),
                    base_branch
                ),
            ));
            continue;
        }

        let previous_branch = git_current_branch_name(&entry.path).unwrap_or_default();
        if !is_branch_merged_into_base(&entry.path, &previous_branch, base_branch) {
            last_prepare_error = Some((
                "previous_branch_not_merged".to_string(),
                format!(
                    "worktree {} branch {} is not merged into {}",
                    entry.path.display(),
                    previous_branch,
                    base_branch
                ),
            ));
            continue;
        }

        let (prepared, skipped_reset) = prepare_reused_worktree_base(&entry.path, base_branch)?;
        if !prepared {
            last_prepare_error = Some((
                "sanitize_failed".to_string(),
                format!(
                    "sanitize failed for worktree {} on base {}",
                    entry.path.display(),
                    base_branch
                ),
            ));
            continue;
        }
        if !is_worktree_clean(&entry.path)? {
            last_prepare_error = Some((
                "sanitize_dirty_after".to_string(),
                format!("sanitize left worktree {} dirty", entry.path.display()),
            ));
            continue;
        }

        let mut branch = build_reuse_branch_name(tool, &entry.path);
        let mut i = 0usize;
        loop {
            let exists = crate::git::rev_parse_verify(repo_root, &branch).unwrap_or(false);
            if !exists {
                break;
            }
            i += 1;
            branch = format!("{}-{}", build_reuse_branch_name(tool, &entry.path), i);
        }
        if !crate::git::checkout_new_branch_from_base(&entry.path, &branch, base_branch)? {
            last_prepare_error = Some((
                "checkout_new_branch_failed".to_string(),
                format!(
                    "failed to create branch {} in reused worktree {}",
                    branch,
                    entry.path.display()
                ),
            ));
            continue;
        }
        if !previous_branch.is_empty()
            && previous_branch != base_branch
            && previous_branch != branch
        {
            coordinator_runtime::report_branch_cleanup_outcome(
                repo_root,
                None,
                "dispatch",
                &previous_branch,
                base_branch,
                "reused_worktree_switch",
                coordinator_runtime::cleanup_merged_local_branch(
                    repo_root,
                    &previous_branch,
                    base_branch,
                ),
                |event_type, task_id, phase, status, message, severity| {
                    let _ = append_coordinator_event_with_severity(
                        repo_root, event_type, task_id, phase, status, message, severity,
                    );
                },
                |msg| tracing::warn!("{}", msg),
            );
        }
        let last_commit = crate::git::head_commit(&entry.path).unwrap_or_default();

        let existing =
            crate::read_worktree_metadata(&entry.path)?.unwrap_or(crate::WorktreeMetadata {
                id: entry
                    .path
                    .file_name()
                    .and_then(|v| v.to_str())
                    .unwrap_or("worker")
                    .to_string(),
                tool: tool.to_string(),
                scope: None,
                feature: None,
                base: base_branch.to_string(),
                branch: branch.clone(),
            });
        let updated = crate::WorktreeMetadata {
            id: existing.id,
            tool: tool.to_string(),
            scope: existing.scope,
            feature: existing.feature,
            base: base_branch.to_string(),
            branch: branch.clone(),
        };
        write_worktree_metadata_file(&entry.path, &updated)?;
        return Ok((
            Some((entry.path, branch, last_commit, skipped_reset, dirty_before)),
            None,
        ));
    }
    Ok((None, last_prepare_error))
}

pub fn count_pool_worktrees(repo_root: &Path) -> Result<usize> {
    let pool_root = repo_root.join(".macc").join("worktree");
    let entries = crate::list_worktrees(repo_root)?;
    Ok(entries
        .into_iter()
        .filter(|e| e.path.starts_with(&pool_root))
        .count())
}

pub fn append_coordinator_event(
    repo_root: &Path,
    event_type: &str,
    task_id: &str,
    phase: &str,
    status: &str,
    message: &str,
) -> Result<()> {
    let severity = if status.eq_ignore_ascii_case("failed") || status.eq_ignore_ascii_case("error")
    {
        "blocking"
    } else {
        "info"
    };
    append_coordinator_event_with_severity(
        repo_root, event_type, task_id, phase, status, message, severity,
    )
}

pub fn append_coordinator_event_with_severity(
    repo_root: &Path,
    event_type: &str,
    task_id: &str,
    phase: &str,
    status: &str,
    message: &str,
    severity: &str,
) -> Result<()> {
    let run_id = ensure_coordinator_run_id();
    let now = now_iso_coordinator();
    let seq = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default() as u64;
    let payload = serde_json::json!({
        "schema_version":"1",
        "event_id": format!("evt-{}-{}-{}", event_type, task_id, seq),
        "run_id": run_id,
        "seq": seq,
        "ts": now,
        "source": "coordinator:native",
        "task_id": task_id,
        "type": event_type,
        "phase": phase,
        "status": status,
        "severity": severity,
        "payload": {"message": message}
    });
    let project_paths = crate::ProjectPaths::from_root(repo_root);
    let _ = append_event_sqlite(&project_paths, &payload)?;
    Ok(())
}

pub fn ensure_coordinator_run_id() -> String {
    if let Ok(existing) = std::env::var("COORDINATOR_RUN_ID") {
        let trimmed = existing.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    let generated = format!(
        "run-{}-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
        std::process::id()
    );
    std::env::set_var("COORDINATOR_RUN_ID", &generated);
    generated
}

pub fn write_worktree_prd_for_task(
    prd_file: &Path,
    task_id: &str,
    worktree_path: &Path,
) -> Result<()> {
    let prd_raw = std::fs::read_to_string(prd_file).map_err(|e| MaccError::Io {
        path: prd_file.to_string_lossy().into(),
        action: "read prd for worktree".into(),
        source: e,
    })?;
    let prd: serde_json::Value = serde_json::from_str(&prd_raw).map_err(|e| {
        MaccError::Validation(format!(
            "Failed to parse PRD {} for worktree: {}",
            prd_file.display(),
            e
        ))
    })?;
    let selected = prd
        .get("tasks")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .find(|t| {
            t.get("id")
                .and_then(serde_json::Value::as_str)
                .map(|id| id == task_id)
                .unwrap_or(false)
        })
        .ok_or_else(|| {
            MaccError::Validation(format!(
                "Task '{}' not found in PRD {}",
                task_id,
                prd_file.display()
            ))
        })?;
    let payload = serde_json::json!({
        "lot": prd.get("lot").cloned().unwrap_or(serde_json::Value::Null),
        "version": prd.get("version").cloned().unwrap_or(serde_json::Value::Null),
        "generated_at": prd.get("generated_at").cloned().unwrap_or(serde_json::Value::Null),
        "timezone": prd.get("timezone").cloned().unwrap_or(serde_json::Value::String("UTC".to_string())),
        "priority_mapping": prd.get("priority_mapping").cloned().unwrap_or_else(|| serde_json::json!({})),
        "assumptions": prd.get("assumptions").cloned().unwrap_or_else(|| serde_json::json!([])),
        "tasks": [selected],
    });
    let out_path = worktree_path.join("worktree.prd.json");
    std::fs::write(
        &out_path,
        serde_json::to_string_pretty(&payload).map_err(|e| {
            MaccError::Validation(format!(
                "Failed to serialize worktree.prd.json payload: {}",
                e
            ))
        })?,
    )
    .map_err(|e| MaccError::Io {
        path: out_path.to_string_lossy().into(),
        action: "write worktree.prd.json".into(),
        source: e,
    })
}
