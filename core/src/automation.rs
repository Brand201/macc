use crate::{MaccError, ProjectPaths, Result};
use std::path::{Component, Path, PathBuf};

const EMBEDDED_PERFORMER_SH: &str = include_str!("../../automat/performer.sh");
const EMBEDDED_COORDINATOR_SH: &str = include_str!("../../automat/coordinator.sh");
const EMBEDDED_COORDINATOR_LEGACY_SH: &str = include_str!("../../automat/coordinator_legacy.sh");
const EMBEDDED_COORDINATOR_RUNTIME_SH: &str =
    include_str!("../../automat/legacy_coordinator/runtime.sh");
const EMBEDDED_COORDINATOR_STATE_SH: &str =
    include_str!("../../automat/legacy_coordinator/state.sh");
const EMBEDDED_COORDINATOR_EVENTS_SH: &str =
    include_str!("../../automat/legacy_coordinator/events.sh");
const EMBEDDED_COORDINATOR_JOBS_SH: &str = include_str!("../../automat/legacy_coordinator/jobs.sh");
const EMBEDDED_COORDINATOR_VCS_SH: &str = include_str!("../../automat/legacy_coordinator/vcs.sh");
const EMBEDDED_MERGE_WORKER_SH: &str = include_str!("../../automat/merge_worker.sh");
const EMBEDDED_MERGE_FIX_HOOK_SH: &str = include_str!("../../automat/hooks/ai-merge-fix.sh");
include!(concat!(env!("OUT_DIR"), "/embedded_automation_runners.rs"));

pub fn ensure_embedded_automation_scripts(paths: &ProjectPaths) -> Result<Vec<PathBuf>> {
    let mut created = Vec::new();
    std::fs::create_dir_all(paths.automation_dir()).map_err(|e| MaccError::Io {
        path: paths.automation_dir().to_string_lossy().into(),
        action: "create automation directory".into(),
        source: e,
    })?;
    std::fs::create_dir_all(paths.automation_runner_dir()).map_err(|e| MaccError::Io {
        path: paths.automation_runner_dir().to_string_lossy().into(),
        action: "create automation runners directory".into(),
        source: e,
    })?;
    if let Some(parent) = paths.automation_merge_fix_hook_path().parent() {
        std::fs::create_dir_all(parent).map_err(|e| MaccError::Io {
            path: parent.to_string_lossy().into(),
            action: "create automation hooks directory".into(),
            source: e,
        })?;
    }
    let coordinator_module_dir = paths.automation_dir().join("legacy_coordinator");
    std::fs::create_dir_all(&coordinator_module_dir).map_err(|e| MaccError::Io {
        path: coordinator_module_dir.to_string_lossy().into(),
        action: "create automation coordinator module directory".into(),
        source: e,
    })?;

    if write_executable_if_changed(&paths.automation_performer_path(), EMBEDDED_PERFORMER_SH)? {
        created.push(paths.automation_performer_path());
    }
    if write_executable_if_changed(
        &paths.automation_coordinator_path(),
        EMBEDDED_COORDINATOR_SH,
    )? {
        created.push(paths.automation_coordinator_path());
    }
    if write_executable_if_changed(
        &paths.automation_coordinator_legacy_path(),
        EMBEDDED_COORDINATOR_LEGACY_SH,
    )? {
        created.push(paths.automation_coordinator_legacy_path());
    }
    let coordinator_runtime_path = coordinator_module_dir.join("runtime.sh");
    if write_executable_if_changed(&coordinator_runtime_path, EMBEDDED_COORDINATOR_RUNTIME_SH)? {
        created.push(coordinator_runtime_path);
    }
    let coordinator_state_path = coordinator_module_dir.join("state.sh");
    if write_executable_if_changed(&coordinator_state_path, EMBEDDED_COORDINATOR_STATE_SH)? {
        created.push(coordinator_state_path);
    }
    let coordinator_events_path = coordinator_module_dir.join("events.sh");
    if write_executable_if_changed(&coordinator_events_path, EMBEDDED_COORDINATOR_EVENTS_SH)? {
        created.push(coordinator_events_path);
    }
    let coordinator_jobs_path = coordinator_module_dir.join("jobs.sh");
    if write_executable_if_changed(&coordinator_jobs_path, EMBEDDED_COORDINATOR_JOBS_SH)? {
        created.push(coordinator_jobs_path);
    }
    let coordinator_vcs_path = coordinator_module_dir.join("vcs.sh");
    if write_executable_if_changed(&coordinator_vcs_path, EMBEDDED_COORDINATOR_VCS_SH)? {
        created.push(coordinator_vcs_path);
    }
    if write_executable_if_changed(
        &paths.automation_merge_worker_path(),
        EMBEDDED_MERGE_WORKER_SH,
    )? {
        created.push(paths.automation_merge_worker_path());
    }
    if write_executable_if_changed(
        &paths.automation_merge_fix_hook_path(),
        EMBEDDED_MERGE_FIX_HOOK_SH,
    )? {
        created.push(paths.automation_merge_fix_hook_path());
    }
    for (runner_ref, content) in EMBEDDED_RUNNERS {
        let local_path = local_runner_path(paths, runner_ref)?;
        if write_executable_if_changed(&local_path, content)? {
            created.push(local_path);
        }
    }

    Ok(created)
}

pub fn embedded_runner_path_for_ref(
    paths: &ProjectPaths,
    runner_ref: &str,
) -> Result<Option<PathBuf>> {
    let Some(content) = embedded_runner_content(runner_ref) else {
        return Ok(None);
    };

    let local_path = local_runner_path(paths, runner_ref)?;
    let _ = write_executable_if_changed(&local_path, content)?;
    Ok(Some(local_path))
}

fn embedded_runner_content(runner_ref: &str) -> Option<&'static str> {
    EMBEDDED_RUNNERS.iter().find_map(|(path, content)| {
        if *path == runner_ref {
            Some(*content)
        } else {
            None
        }
    })
}

fn local_runner_path(paths: &ProjectPaths, runner_ref: &str) -> Result<PathBuf> {
    let relative = sanitized_relative_runner_path(runner_ref)?;
    let path = paths.automation_dir().join("embedded").join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| MaccError::Io {
            path: parent.to_string_lossy().into(),
            action: "create embedded runner directory".into(),
            source: e,
        })?;
    }
    Ok(path)
}

fn sanitized_relative_runner_path(runner_ref: &str) -> Result<PathBuf> {
    let path = Path::new(runner_ref);
    if path.is_absolute() {
        return Err(MaccError::Validation(format!(
            "Runner path must be relative: {}",
            runner_ref
        )));
    }

    let mut cleaned = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::Normal(seg) => cleaned.push(seg),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(MaccError::Validation(format!(
                    "Runner path contains unsupported component: {}",
                    runner_ref
                )))
            }
        }
    }

    if cleaned.as_os_str().is_empty() {
        return Err(MaccError::Validation(format!(
            "Runner path is empty: {}",
            runner_ref
        )));
    }
    Ok(cleaned)
}

fn write_executable_if_changed(path: &Path, content: &str) -> Result<bool> {
    let existed = path.exists();
    let needs_write = match std::fs::read_to_string(path) {
        Ok(existing) => existing != content,
        Err(_) => true,
    };

    if needs_write {
        std::fs::write(path, content).map_err(|e| MaccError::Io {
            path: path.to_string_lossy().into(),
            action: "write automation script".into(),
            source: e,
        })?;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(path)
            .map_err(|e| MaccError::Io {
                path: path.to_string_lossy().into(),
                action: "read automation script permissions".into(),
                source: e,
            })?
            .permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(path, perms).map_err(|e| MaccError::Io {
            path: path.to_string_lossy().into(),
            action: "set automation script permissions".into(),
            source: e,
        })?;
    }

    Ok(!existed)
}
