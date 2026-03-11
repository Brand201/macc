use crate::{MaccError, Result};
use serde_json::json;
use std::path::{Path, PathBuf};
use tokio::fs;

pub fn aggregate_performer_logs(repo_root: &Path) -> Result<usize> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| MaccError::Validation(format!("create tokio runtime: {}", e)))?;
    runtime.block_on(aggregate_performer_logs_async(repo_root))
}

pub async fn aggregate_performer_logs_async(repo_root: &Path) -> Result<usize> {
    let worktrees_root = repo_root.join(".macc").join("worktree");
    let target_root = repo_root.join(".macc").join("log").join("performer");
    fs::create_dir_all(&target_root)
        .await
        .map_err(|e| MaccError::Io {
            path: target_root.to_string_lossy().into(),
            action: "create performer log aggregation directory".into(),
            source: e,
        })?;

    let mut entries = Vec::new();
    if !worktrees_root.exists() {
        write_index(&target_root.join("index.json"), &entries).await?;
        return Ok(0);
    }

    let mut copied = 0usize;
    let mut worktree_entries = fs::read_dir(&worktrees_root)
        .await
        .map_err(|e| MaccError::Io {
            path: worktrees_root.to_string_lossy().into(),
            action: "read worktree root for performer logs".into(),
            source: e,
        })?;
    while let Some(wt) = worktree_entries
        .next_entry()
        .await
        .map_err(|e| MaccError::Io {
            path: worktrees_root.to_string_lossy().into(),
            action: "iterate worktree root for performer logs".into(),
            source: e,
        })?
    {
        let wt_path = wt.path();
        if !wt
            .file_type()
            .await
            .map_err(|e| MaccError::Io {
                path: wt_path.to_string_lossy().into(),
                action: "read worktree entry type".into(),
                source: e,
            })?
            .is_dir()
        {
            continue;
        }
        let wt_name = wt_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("worktree");
        let performer_dir = wt_path.join(".macc").join("log").join("performer");
        if !performer_dir.is_dir() {
            continue;
        }

        let mut log_entries = fs::read_dir(&performer_dir)
            .await
            .map_err(|e| MaccError::Io {
                path: performer_dir.to_string_lossy().into(),
                action: "read performer worktree log dir".into(),
                source: e,
            })?;
        while let Some(file) = log_entries.next_entry().await.map_err(|e| MaccError::Io {
            path: performer_dir.to_string_lossy().into(),
            action: "iterate performer worktree log dir".into(),
            source: e,
        })? {
            let source = file.path();
            if !is_markdown_file(&source) {
                continue;
            }

            let source_name = source
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("task.md");
            let target_name = format!("{}--{}", sanitize_name(wt_name), sanitize_name(source_name));
            let target = target_root.join(target_name);

            copy_if_changed(&source, &target).await?;
            copied += 1;

            let metadata = fs::metadata(&source).await.map_err(|e| MaccError::Io {
                path: source.to_string_lossy().into(),
                action: "read performer source metadata".into(),
                source: e,
            })?;
            let mtime = metadata
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            entries.push(json!({
                "worktree": wt_name,
                "source": source.to_string_lossy().to_string(),
                "target": target.to_string_lossy().to_string(),
                "bytes": metadata.len(),
                "mtime_epoch": mtime,
            }));
        }
    }

    entries.sort_by(|a, b| {
        let ka = a
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let kb = b
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        ka.cmp(&kb)
    });

    write_index(&target_root.join("index.json"), &entries).await?;
    Ok(copied)
}

async fn write_index(path: &Path, entries: &[serde_json::Value]) -> Result<()> {
    let generated_at_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let index = json!({
        "schema_version": 1,
        "generated_at_epoch": generated_at_epoch,
        "entries": entries,
    });
    let body = serde_json::to_vec_pretty(&index).map_err(|e| {
        MaccError::Validation(format!(
            "serialize performer log index '{}': {}",
            path.display(),
            e
        ))
    })?;
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, body).await.map_err(|e| MaccError::Io {
        path: tmp.to_string_lossy().into(),
        action: "write performer log index temp file".into(),
        source: e,
    })?;
    fs::rename(&tmp, path).await.map_err(|e| MaccError::Io {
        path: path.to_string_lossy().into(),
        action: "persist performer log index".into(),
        source: e,
    })?;
    Ok(())
}

async fn copy_if_changed(src: &PathBuf, dst: &PathBuf) -> Result<()> {
    let src_bytes = fs::read(src).await.map_err(|e| MaccError::Io {
        path: src.to_string_lossy().into(),
        action: "read performer source log".into(),
        source: e,
    })?;
    if let Ok(existing) = fs::read(dst).await {
        if existing == src_bytes {
            return Ok(());
        }
    }
    fs::write(dst, src_bytes).await.map_err(|e| MaccError::Io {
        path: dst.to_string_lossy().into(),
        action: "write aggregated performer log".into(),
        source: e,
    })?;
    Ok(())
}

fn is_markdown_file(path: &Path) -> bool {
    path.extension()
        .and_then(|s| s.to_str())
        .map(|s| s.eq_ignore_ascii_case("md"))
        .unwrap_or(false)
}

fn sanitize_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            out.push(ch);
        } else {
            out.push('-');
        }
    }
    if out.is_empty() {
        "log".to_string()
    } else {
        out
    }
}
