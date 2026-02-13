use crate::catalog::{Source, SourceKind};
use macc_core::resolve::{FetchUnit, MaterializedFetchUnit, SelectionKind};
use macc_core::{write_if_changed, MaccError, ProjectPaths, Result as MaccResult};
use reqwest::blocking::Client;
use sha2::{Digest, Sha256};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

fn cache_candidates(paths: &ProjectPaths, key: &str) -> Vec<PathBuf> {
    let mut candidates = vec![paths.source_cache_path(key)];
    if let Some(user) = paths.user_source_cache_path(key) {
        candidates.push(user);
    }
    candidates
}

fn existing_cache_root(paths: &ProjectPaths, key: &str) -> Option<PathBuf> {
    cache_candidates(paths, key)
        .into_iter()
        .find(|root| root.exists())
}

fn choose_writable_cache_root(paths: &ProjectPaths, key: &str) -> MaccResult<PathBuf> {
    if let Some(user_root) = paths.user_source_cache_path(key) {
        if std::fs::create_dir_all(&user_root).is_ok() {
            return Ok(user_root);
        }
    }

    let project_root = paths.source_cache_path(key);
    std::fs::create_dir_all(&project_root).map_err(|e| MaccError::Io {
        path: project_root.to_string_lossy().into(),
        action: "create project cache directory".into(),
        source: e,
    })?;
    Ok(project_root)
}

fn cache_root_from_archive_path(archive_path: &Path) -> MaccResult<PathBuf> {
    let raw_dir = archive_path.parent().ok_or_else(|| {
        MaccError::Validation(format!(
            "Invalid archive path layout (missing parent): {}",
            archive_path.display()
        ))
    })?;
    let cache_root = raw_dir.parent().ok_or_else(|| {
        MaccError::Validation(format!(
            "Invalid archive path layout (missing cache root): {}",
            archive_path.display()
        ))
    })?;
    Ok(cache_root.to_path_buf())
}

pub fn download_source_raw(paths: &ProjectPaths, source: &Source) -> MaccResult<PathBuf> {
    if source.kind != SourceKind::Http {
        return Err(MaccError::Validation(format!(
            "download_source_raw only supports HTTP sources, got {:?}",
            source.kind
        )));
    }

    let key = source.cache_key();
    if let Some(root) = existing_cache_root(paths, &key) {
        let target = root.join("raw").join("archive.zip");
        if target.exists() {
            if let Some(expected_checksum) = &source.checksum {
                let actual_bytes = std::fs::read(&target).map_err(|e| MaccError::Io {
                    path: target.to_string_lossy().into(),
                    action: "read cached archive".into(),
                    source: e,
                })?;
                let actual_checksum = format!("sha256:{:x}", Sha256::digest(&actual_bytes));
                if actual_checksum.to_lowercase() == expected_checksum.to_lowercase() {
                    return Ok(target);
                }
                log_info(&format!(
                    "Cached archive checksum mismatch for {}. Re-downloading...",
                    source.url
                ));
                let _ = std::fs::remove_file(&target);
            } else {
                return Ok(target);
            }
        }
    }

    let cache_root = choose_writable_cache_root(paths, &key)?;
    let raw_dir = cache_root.join("raw");
    let target_path = raw_dir.join("archive.zip");

    if !raw_dir.exists() {
        std::fs::create_dir_all(&raw_dir).map_err(|e| MaccError::Io {
            path: raw_dir.to_string_lossy().into(),
            action: "create raw cache directory".into(),
            source: e,
        })?;
    }

    log_info(&format!("Fetching {}...", source.url));

    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| MaccError::Validation(format!("Failed to create HTTP client: {}", e)))?;

    let response = client
        .get(&source.url)
        .send()
        .map_err(|e| MaccError::Validation(format!("Failed to fetch {}: {}", source.url, e)))?;

    if !response.status().is_success() {
        return Err(MaccError::Validation(format!(
            "Failed to fetch {}: status {}",
            source.url,
            response.status()
        )));
    }

    let bytes = response.bytes().map_err(|e| {
        MaccError::Validation(format!(
            "Failed to read response bytes from {}: {}",
            source.url, e
        ))
    })?;

    // Verify checksum of downloaded bytes
    if let Some(expected_checksum) = &source.checksum {
        let actual_checksum = format!("sha256:{:x}", Sha256::digest(&bytes));
        if actual_checksum.to_lowercase() != expected_checksum.to_lowercase() {
            return Err(MaccError::Validation(format!(
                "Checksum mismatch for {}. Expected {}, got {}",
                source.url, expected_checksum, actual_checksum
            )));
        }
    }

    let _ = write_if_changed(
        paths,
        target_path.to_string_lossy().as_ref(),
        &target_path,
        &bytes,
        |_| Ok(()),
    )?;

    Ok(target_path)
}

/// Materializes a source (Git or Http) into the cache.
pub fn materialize_source(paths: &ProjectPaths, source: &Source) -> MaccResult<PathBuf> {
    match source.kind {
        SourceKind::Git => git_fetch(paths, source),
        SourceKind::Http => download_and_unpack(paths, source),
        SourceKind::Local => resolve_local_source(paths, source),
    }
}

fn resolve_local_source(paths: &ProjectPaths, source: &Source) -> MaccResult<PathBuf> {
    let path = PathBuf::from(&source.url);
    let resolved = if path.is_absolute() {
        path
    } else {
        paths.root.join(path)
    };

    if !resolved.exists() {
        return Err(MaccError::Validation(format!(
            "Local source path not found: {}",
            resolved.display()
        )));
    }

    Ok(resolved)
}

/// Pipeline step to materialize a FetchUnit.
pub fn materialize_fetch_unit(
    paths: &ProjectPaths,
    unit: FetchUnit,
) -> MaccResult<MaterializedFetchUnit> {
    let root = materialize_source(paths, &unit.source)?;

    // Validate that each selection's subpath exists under returned root
    for selection in &unit.selections {
        let p = if selection.subpath.is_empty() || selection.subpath == "." {
            root.clone()
        } else {
            root.join(&selection.subpath)
        };

        if !p.exists() {
            return Err(MaccError::Validation(format!(
                "Selection '{}' (subpath: '{}') not found in materialized source at {}",
                selection.id,
                selection.subpath,
                root.display()
            )));
        }

        // Skill package validation (manifest required for remote sources)
        if selection.kind == SelectionKind::Skill {
            macc_core::packages::validate_skill_folder(&p, true).map_err(MaccError::Validation)?;
        }

        // MCP package validation (macc.package.json required)
        if selection.kind == SelectionKind::Mcp {
            macc_core::packages::validate_mcp_folder(&p, &selection.id)
                .map_err(MaccError::Validation)?;
        }
    }

    Ok(MaterializedFetchUnit {
        source_root_path: root,
        selections: unit.selections,
    })
}

/// Pipeline step to materialize multiple FetchUnits.
pub fn materialize_fetch_units(
    paths: &ProjectPaths,
    units: Vec<FetchUnit>,
) -> MaccResult<Vec<MaterializedFetchUnit>> {
    let mut materialized = Vec::new();
    for unit in units {
        materialized.push(materialize_fetch_unit(paths, unit)?);
    }
    Ok(materialized)
}

/// Fetches a Git source into the cache.
pub fn git_fetch(paths: &ProjectPaths, source: &Source) -> MaccResult<PathBuf> {
    if source.kind != SourceKind::Git {
        return Err(MaccError::Validation(format!(
            "git_fetch only supports Git sources, got {:?}",
            source.kind
        )));
    }

    let key = source.cache_key();
    let cache_root = if let Some(existing) = existing_cache_root(paths, &key) {
        existing
    } else {
        choose_writable_cache_root(paths, &key)?
    };
    let repo_dir = cache_root.join("repo");

    if !repo_dir.exists() {
        log_info(&format!(
            "Cloning {} into {}...",
            source.url,
            repo_dir.display()
        ));
        let mut args = vec!["clone", "--no-checkout"];
        // If we have subpaths, we can optimize the clone
        if !source.subpaths.is_empty() {
            args.push("--filter=blob:none");
        }
        args.push(&source.url);
        args.push("repo");

        let output = Command::new("git")
            .args(args)
            .current_dir(&cache_root)
            .output()
            .map_err(|e| MaccError::Io {
                path: cache_root.to_string_lossy().into(),
                action: "run git clone".into(),
                source: e,
            })?;

        if !output.status.success() {
            return Err(MaccError::Validation(format!(
                "git clone failed for {}: {}",
                source.url,
                String::from_utf8_lossy(&output.stderr)
            )));
        }
    } else {
        log_info(&format!(
            "Fetching {} in {}...",
            source.url,
            repo_dir.display()
        ));
        let output = Command::new("git")
            .args(["fetch", "--all", "--tags"])
            .current_dir(&repo_dir)
            .output()
            .map_err(|e| MaccError::Io {
                path: repo_dir.to_string_lossy().into(),
                action: "run git fetch".into(),
                source: e,
            })?;

        if !output.status.success() {
            return Err(MaccError::Validation(format!(
                "git fetch failed for {}: {}",
                source.url,
                String::from_utf8_lossy(&output.stderr)
            )));
        }
    }

    // Handle sparse checkout if subpaths are provided
    if !source.subpaths.is_empty() {
        enable_sparse_checkout(&repo_dir)?;
        set_sparse_paths(&repo_dir, &source.subpaths)?;
    }

    if !source.reference.is_empty() {
        log_info(&format!("Resolving ref {}...", source.reference));
        let sha = resolve_ref_to_sha(&repo_dir, &source.reference)?;
        log_info(&format!("Checking out {}...", sha));
        checkout_ref(&repo_dir, &sha)?;
    } else if !source.subpaths.is_empty() {
        // If we have subpaths but no specific ref, we still need to checkout the default branch
        log_info("Checking out default branch...");
        checkout_ref(&repo_dir, "HEAD")?;
    }

    // Validate subpaths exist
    if !source.subpaths.is_empty() {
        for subpath in &source.subpaths {
            let p = repo_dir.join(subpath);
            if !p.exists() {
                return Err(MaccError::Validation(format!(
                    "Subpath '{}' not found in repository after checkout",
                    subpath
                )));
            }
        }
    }

    Ok(repo_dir)
}

fn log_info(message: &str) {
    if std::env::var("MACC_QUIET").is_ok() {
        return;
    }
    println!("{}", message);
}

fn enable_sparse_checkout(repo_dir: &Path) -> MaccResult<()> {
    let output = Command::new("git")
        .args(["sparse-checkout", "init", "--cone"])
        .current_dir(repo_dir)
        .output()
        .map_err(|e| MaccError::Io {
            path: repo_dir.to_string_lossy().into(),
            action: "init sparse-checkout".into(),
            source: e,
        })?;

    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "git sparse-checkout init failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

fn set_sparse_paths(repo_dir: &Path, subpaths: &[String]) -> MaccResult<()> {
    let mut args = vec!["sparse-checkout", "set"];
    for p in subpaths {
        args.push(p);
    }

    let output = Command::new("git")
        .args(args)
        .current_dir(repo_dir)
        .output()
        .map_err(|e| MaccError::Io {
            path: repo_dir.to_string_lossy().into(),
            action: "set sparse paths".into(),
            source: e,
        })?;

    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "git sparse-checkout set failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

fn resolve_ref_to_sha(repo_dir: &Path, reference: &str) -> MaccResult<String> {
    let candidates = [
        format!("refs/remotes/origin/{}", reference),
        format!("refs/tags/{}", reference),
        reference.to_string(),
    ];

    for candidate in &candidates {
        let output = Command::new("git")
            .args(["rev-parse", &format!("{}^{{commit}}", candidate)])
            .current_dir(repo_dir)
            .output()
            .map_err(|e| MaccError::Io {
                path: repo_dir.to_string_lossy().into(),
                action: "resolve git ref".into(),
                source: e,
            })?;

        if output.status.success() {
            let sha = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !sha.is_empty() {
                return Ok(sha);
            }
        }
    }

    Err(MaccError::Validation(format!(
        "git rev-parse {} failed",
        reference
    )))
}

fn checkout_ref(repo_dir: &Path, reference: &str) -> MaccResult<()> {
    let output = Command::new("git")
        .args(["checkout", "--force", reference])
        .current_dir(repo_dir)
        .output()
        .map_err(|e| MaccError::Io {
            path: repo_dir.to_string_lossy().into(),
            action: "run git checkout".into(),
            source: e,
        })?;

    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "git checkout {} failed: {}",
            reference,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

#[allow(dead_code)]
fn disable_sparse_checkout(repo_dir: &Path) -> MaccResult<()> {
    let output = Command::new("git")
        .args(["sparse-checkout", "disable"])
        .current_dir(repo_dir)
        .output()
        .map_err(|e| MaccError::Io {
            path: repo_dir.to_string_lossy().into(),
            action: "disable sparse-checkout".into(),
            source: e,
        })?;

    if !output.status.success() {
        return Err(MaccError::Validation(format!(
            "git sparse-checkout disable failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

/// Downloads and unpacks a source into the cache.
pub fn download_and_unpack(paths: &ProjectPaths, source: &Source) -> MaccResult<PathBuf> {
    let archive_path = download_source_raw(paths, source)?;
    let cache_root = cache_root_from_archive_path(&archive_path)?;
    let unpack_dir = cache_root.join("unpacked");

    if unpack_dir.exists() {
        // For now, if it exists, we assume it's valid.
        return Ok(unpack_dir);
    }

    let tmp_unpack = cache_root.join(format!(
        "unpacked-{}",
        chrono::Local::now().format("%Y%m%d-%H%M%S")
    ));
    if tmp_unpack.exists() {
        std::fs::remove_dir_all(&tmp_unpack).ok();
    }

    if let Err(err) = unpack_archive(&archive_path, &tmp_unpack) {
        let _ = std::fs::remove_dir_all(&tmp_unpack);
        return Err(err);
    }

    // Atomic promote of unpacked directory
    if let Some(parent) = unpack_dir.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent).map_err(|e| MaccError::Io {
                path: parent.to_string_lossy().into(),
                action: "create unpack parent directory".into(),
                source: e,
            })?;
        }
    }
    std::fs::rename(&tmp_unpack, &unpack_dir).map_err(|e| MaccError::Io {
        path: unpack_dir.to_string_lossy().into(),
        action: "finalize unpack directory".into(),
        source: e,
    })?;

    Ok(unpack_dir)
}

/// Safely unpacks a ZIP archive into the target directory with Zip Slip protection.
pub fn unpack_archive(archive_path: &Path, target_dir: &Path) -> MaccResult<()> {
    let file = std::fs::File::open(archive_path).map_err(|e| MaccError::Io {
        path: archive_path.to_string_lossy().into(),
        action: "open archive for unpacking".into(),
        source: e,
    })?;

    let mut archive = zip::ZipArchive::new(file).map_err(|e| {
        MaccError::Validation(format!(
            "Failed to read zip archive {}: {}",
            archive_path.display(),
            e
        ))
    })?;

    if !target_dir.exists() {
        std::fs::create_dir_all(target_dir).map_err(|e| MaccError::Io {
            path: target_dir.to_string_lossy().into(),
            action: "create target unpack directory".into(),
            source: e,
        })?;
    }

    // Canonicalize target_dir to ensure starts_with check is robust against different path representations
    let target_dir_canonical = target_dir.canonicalize().map_err(|e| MaccError::Io {
        path: target_dir.to_string_lossy().into(),
        action: "canonicalize target unpack directory".into(),
        source: e,
    })?;

    for i in 0..archive.len() {
        let mut file = archive
            .by_index(i)
            .map_err(|e| MaccError::Validation(format!("Failed to read zip entry {}: {}", i, e)))?;

        // enclosed_name() prevents basic Zip Slip by rejecting absolute paths and ".." that escape root
        let ename = file.enclosed_name().ok_or_else(|| {
            MaccError::Validation(format!(
                "Invalid or malicious entry name in zip: {}",
                file.name()
            ))
        })?;

        let outpath = target_dir_canonical.join(ename);

        // Reject symlinks for security
        #[cfg(unix)]
        if let Some(mode) = file.unix_mode() {
            if (mode & 0o170000) == 0o120000 {
                return Err(MaccError::Validation(format!(
                    "Symlinks are not supported in ZIP archives: {}",
                    file.name()
                )));
            }
        }

        // Extra guard: Ensure the destination is indeed inside the target directory.
        if !outpath.starts_with(&target_dir_canonical) {
            return Err(MaccError::Validation(format!(
                "Zip Slip detected: entry {} attempts to write outside target directory",
                file.name()
            )));
        }

        if file.is_dir() {
            std::fs::create_dir_all(&outpath).map_err(|e| MaccError::Io {
                path: outpath.to_string_lossy().into(),
                action: "create directory from zip".into(),
                source: e,
            })?;
        } else {
            if let Some(p) = outpath.parent() {
                if !p.exists() {
                    std::fs::create_dir_all(p).map_err(|e| MaccError::Io {
                        path: p.to_string_lossy().into(),
                        action: "create parent directory from zip".into(),
                        source: e,
                    })?;
                }
            }

            let mut outfile = std::fs::File::create(&outpath).map_err(|e| MaccError::Io {
                path: outpath.to_string_lossy().into(),
                action: "create file from zip".into(),
                source: e,
            })?;

            io::copy(&mut file, &mut outfile).map_err(|e| MaccError::Io {
                path: outpath.to_string_lossy().into(),
                action: "extract file from zip".into(),
                source: e,
            })?;
        }

        // Apply unix permissions if present
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Some(mode) = file.unix_mode() {
                if (mode & 0o7000) != 0 {
                    return Err(MaccError::Validation(format!(
                        "Refusing to apply dangerous permission bits for {}",
                        file.name()
                    )));
                }
                let safe_mode = mode & 0o777;
                let _ =
                    std::fs::set_permissions(&outpath, std::fs::Permissions::from_mode(safe_mode));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SourceKind;
    use macc_core::ProjectPaths;

    #[test]
    fn test_download_checksum_logic() {
        let temp_base = std::env::temp_dir().join(format!("macc_fetch_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let paths = ProjectPaths::from_root(&temp_base);

        let bytes = b"test archive content";
        let actual_hash = format!("sha256:{:x}", Sha256::digest(bytes));

        let source = Source {
            kind: SourceKind::Http,
            url: "http://example.com/test.zip".into(),
            reference: "".into(),
            checksum: Some(actual_hash.clone()),
            subpaths: vec![],
        };

        let key = source.cache_key();
        let target_path = paths.source_cache_path(&key).join("raw/archive.zip");
        std::fs::create_dir_all(target_path.parent().unwrap()).unwrap();

        // 1. Test "already exists and matches"
        std::fs::write(&target_path, bytes).unwrap();
        let result = download_source_raw(&paths, &source).unwrap();
        assert_eq!(result, target_path);

        std::fs::remove_dir_all(&temp_base).ok();
    }

    fn uuid_v4_like() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        format!("{:?}", since_the_epoch.as_nanos())
    }

    #[test]
    fn test_download_and_unpack_integration() {
        let temp_base =
            std::env::temp_dir().join(format!("macc_download_unpack_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let paths = ProjectPaths::from_root(&temp_base);

        // 1. Prepare a zip file and its checksum
        let archive_bytes = {
            let mut buf = Vec::new();
            {
                let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
                let options = zip::write::SimpleFileOptions::default();
                zip.start_file("test.txt", options).unwrap();
                use std::io::Write;
                zip.write_all(b"content").unwrap();
                zip.finish().unwrap();
            }
            buf
        };
        let checksum = format!("sha256:{:x}", Sha256::digest(&archive_bytes));

        let source = Source {
            kind: SourceKind::Http,
            url: "http://example.com/test.zip".into(),
            reference: "".into(),
            checksum: Some(checksum),
            subpaths: vec![],
        };

        let key = source.cache_key();
        let cache_root = paths.source_cache_path(&key);
        let target_path = cache_root.join("raw/archive.zip");
        std::fs::create_dir_all(target_path.parent().unwrap()).unwrap();
        std::fs::write(&target_path, &archive_bytes).unwrap();

        // 2. Call download_and_unpack (it should use the cached file)
        let unpack_dir = download_and_unpack(&paths, &source).expect("Should download and unpack");

        // 3. Verify
        assert!(unpack_dir.exists());
        assert!(unpack_dir.ends_with("unpacked"));
        assert_eq!(
            std::fs::read_to_string(unpack_dir.join("test.txt")).unwrap(),
            "content"
        );

        std::fs::remove_dir_all(&temp_base).ok();
    }

    #[test]
    fn test_git_fetch_integration() {
        let temp_base =
            std::env::temp_dir().join(format!("macc_git_fetch_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let paths = ProjectPaths::from_root(&temp_base);

        // 1. Create a local git repo to act as remote
        let remote_dir = temp_base.join("remote_repo");
        std::fs::create_dir_all(&remote_dir).unwrap();
        Command::new("git")
            .args(["init", "-b", "master"])
            .current_dir(&remote_dir)
            .status()
            .unwrap();
        Command::new("git")
            .args(["config", "user.email", "you@example.com"])
            .current_dir(&remote_dir)
            .status()
            .unwrap();
        Command::new("git")
            .args(["config", "user.name", "Your Name"])
            .current_dir(&remote_dir)
            .status()
            .unwrap();
        std::fs::write(remote_dir.join("README.md"), "hello").unwrap();
        Command::new("git")
            .args(["add", "."])
            .current_dir(&remote_dir)
            .status()
            .unwrap();
        Command::new("git")
            .args(["commit", "-m", "initial"])
            .current_dir(&remote_dir)
            .status()
            .unwrap();

        let source = Source {
            kind: SourceKind::Git,
            url: remote_dir.to_string_lossy().into(),
            reference: "master".into(), // Or "main" depending on git version, but init usually defaults to master or we can force it.
            checksum: None,
            subpaths: vec![],
        };

        // 2. Fetch it
        let repo_dir = git_fetch(&paths, &source).expect("Should fetch git repo");

        // 3. Verify
        assert!(repo_dir.exists());
        assert!(repo_dir.join(".git").exists());
        assert_eq!(
            std::fs::read_to_string(repo_dir.join("README.md")).unwrap(),
            "hello"
        );

        // 4. Test update
        std::fs::write(remote_dir.join("README.md"), "updated").unwrap();
        Command::new("git")
            .args(["add", "."])
            .current_dir(&remote_dir)
            .status()
            .unwrap();
        Command::new("git")
            .args(["commit", "-m", "update"])
            .current_dir(&remote_dir)
            .status()
            .unwrap();

        let repo_dir_v2 = git_fetch(&paths, &source).expect("Should fetch git repo update");
        assert_eq!(repo_dir, repo_dir_v2);

        // Since we are on master, and git fetch doesn't automatically merge,
        // and our git_fetch only does 'fetch' + 'checkout'.
        // If we checkout 'master' again, it might not move if it's already on master.
        // Actually 'git checkout <branch>' if already on branch doesn't pull new changes from remote.
        // But we are tracking a "local remote".

        // To be sure, let's checkout the SHA
        let sha_output = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&remote_dir)
            .output()
            .unwrap();
        let sha = String::from_utf8_lossy(&sha_output.stdout)
            .trim()
            .to_string();

        let source_sha = Source {
            kind: SourceKind::Git,
            url: remote_dir.to_string_lossy().into(),
            reference: sha.clone(),
            checksum: None,
            subpaths: vec![],
        };

        let repo_dir_sha = git_fetch(&paths, &source_sha).expect("Should fetch git SHA");
        assert_eq!(
            std::fs::read_to_string(repo_dir_sha.join("README.md")).unwrap(),
            "updated"
        );

        std::fs::remove_dir_all(&temp_base).ok();
    }

    #[test]
    fn test_git_fetch_sparse_checkout() {
        let temp_base =
            std::env::temp_dir().join(format!("macc_git_sparse_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let paths = ProjectPaths::from_root(&temp_base);

        // 1. Create a local git repo with multiple folders
        let remote_dir = temp_base.join("remote_repo");
        std::fs::create_dir_all(&remote_dir.join("folder1")).unwrap();
        std::fs::create_dir_all(&remote_dir.join("folder2")).unwrap();

        Command::new("git")
            .args(["init", "-b", "master"])
            .current_dir(&remote_dir)
            .status()
            .unwrap();
        Command::new("git")
            .args(["config", "user.email", "you@example.com"])
            .current_dir(&remote_dir)
            .status()
            .unwrap();
        Command::new("git")
            .args(["config", "user.name", "Your Name"])
            .current_dir(&remote_dir)
            .status()
            .unwrap();

        std::fs::write(remote_dir.join("folder1/file1.txt"), "content1").unwrap();
        std::fs::write(remote_dir.join("folder2/file2.txt"), "content2").unwrap();
        std::fs::write(remote_dir.join("root.txt"), "root").unwrap();

        Command::new("git")
            .args(["add", "."])
            .current_dir(&remote_dir)
            .status()
            .unwrap();
        Command::new("git")
            .args(["commit", "-m", "initial"])
            .current_dir(&remote_dir)
            .status()
            .unwrap();

        let source = Source {
            kind: SourceKind::Git,
            url: remote_dir.to_string_lossy().into(),
            reference: "master".into(),
            checksum: None,
            subpaths: vec!["folder1".into()],
        };

        // 2. Fetch it
        let repo_dir = git_fetch(&paths, &source).expect("Should fetch git repo sparsely");

        // 3. Verify
        assert!(repo_dir.join("folder1").exists());
        assert!(repo_dir.join("folder1/file1.txt").exists());
        assert!(repo_dir.join("root.txt").exists());
        // folder2 should NOT exist in the working tree
        assert!(!repo_dir.join("folder2").exists());

        // 4. Test multiple subpaths
        let source_multi = Source {
            kind: SourceKind::Git,
            url: remote_dir.to_string_lossy().into(),
            reference: "master".into(),
            checksum: None,
            subpaths: vec!["folder1".into(), "folder2".into()],
        };
        let repo_dir_multi =
            git_fetch(&paths, &source_multi).expect("Should fetch git repo with multi subpaths");
        assert!(repo_dir_multi.join("folder1").exists());
        assert!(repo_dir_multi.join("folder2").exists());
        assert!(repo_dir_multi.join("root.txt").exists());

        std::fs::remove_dir_all(&temp_base).ok();
    }

    #[test]
    fn test_materialize_fetch_unit_http() {
        use macc_core::resolve::{Selection, SelectionKind};
        let temp_base =
            std::env::temp_dir().join(format!("macc_materialize_unit_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let paths = ProjectPaths::from_root(&temp_base);

        // Prepare zip
        let archive_bytes = {
            let mut buf = Vec::new();
            {
                let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
                let options = zip::write::SimpleFileOptions::default();
                zip.start_file("skills/s1/macc.package.json", options)
                    .unwrap();
                use std::io::Write;
                let manifest = r#"{
  "type": "skill",
  "id": "s1",
  "version": "0.1.0",
  "targets": {
    "claude": [
      { "src": "SKILL.md", "dest": ".claude/skills/s1/SKILL.md" }
    ]
  }
}
"#;
                zip.write_all(manifest.as_bytes()).unwrap();
                zip.start_file("skills/s1/SKILL.md", options).unwrap();
                zip.finish().unwrap();
            }
            buf
        };

        let source = Source {
            kind: SourceKind::Http,
            url: "http://example.com/test.zip".into(),
            reference: "".into(),
            checksum: None,
            subpaths: vec!["skills/s1".into()],
        };

        let key = source.cache_key();
        let target_path = paths.source_cache_path(&key).join("raw/archive.zip");
        std::fs::create_dir_all(target_path.parent().unwrap()).unwrap();
        std::fs::write(&target_path, &archive_bytes).unwrap();

        let unit = FetchUnit {
            source: source.clone(),
            selections: vec![Selection {
                id: "s1".into(),
                subpath: "skills/s1".into(),
                kind: SelectionKind::Skill,
            }],
        };

        let result = materialize_fetch_unit(&paths, unit).expect("Should materialize unit");
        assert!(result.source_root_path.ends_with("unpacked"));
        assert_eq!(result.selections.len(), 1);
        assert_eq!(result.selections[0].id, "s1");

        // Test failure when subpath missing
        let unit_fail = FetchUnit {
            source,
            selections: vec![Selection {
                id: "s2".into(),
                subpath: "missing".into(),
                kind: SelectionKind::Skill,
            }],
        };
        let result_fail = materialize_fetch_unit(&paths, unit_fail);
        assert!(result_fail.is_err());
        assert!(result_fail
            .unwrap_err()
            .to_string()
            .contains("Selection 's2' (subpath: 'missing') not found"));

        std::fs::remove_dir_all(&temp_base).ok();
    }

    #[test]
    fn test_materialize_fetch_unit_skill_no_markers() {
        use macc_core::resolve::{Selection, SelectionKind};
        let temp_base =
            std::env::temp_dir().join(format!("macc_materialize_no_markers_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let paths = ProjectPaths::from_root(&temp_base);

        // Prepare zip with a folder but no manifest
        let archive_bytes = {
            let mut buf = Vec::new();
            {
                let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
                let options = zip::write::SimpleFileOptions::default();
                zip.start_file("skills/s1/something.txt", options).unwrap();
                zip.finish().unwrap();
            }
            buf
        };

        let source = Source {
            kind: SourceKind::Http,
            url: "http://example.com/test.zip".into(),
            reference: "".into(),
            checksum: None,
            subpaths: vec!["skills/s1".into()],
        };

        let key = source.cache_key();
        let target_path = paths.source_cache_path(&key).join("raw/archive.zip");
        std::fs::create_dir_all(target_path.parent().unwrap()).unwrap();
        std::fs::write(&target_path, &archive_bytes).unwrap();

        let unit = FetchUnit {
            source,
            selections: vec![Selection {
                id: "s1".into(),
                subpath: "skills/s1".into(),
                kind: SelectionKind::Skill,
            }],
        };

        let result = materialize_fetch_unit(&paths, unit);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("missing 'macc.package.json'"));

        std::fs::remove_dir_all(&temp_base).ok();
    }

    #[test]
    fn test_unpack_safe() {
        let temp_base = std::env::temp_dir().join(format!("macc_unpack_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let archive_path = temp_base.join("test.zip");
        let unpack_dir = temp_base.join("unpacked");

        // Create a valid zip
        let file = std::fs::File::create(&archive_path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);

        zip.start_file("hello.txt", options).unwrap();
        use std::io::Write;
        zip.write_all(b"hello world").unwrap();
        zip.add_directory("subdir/", options).unwrap();
        zip.start_file("subdir/nested.txt", options).unwrap();
        zip.write_all(b"nested content").unwrap();
        zip.finish().unwrap();

        // Unpack it
        unpack_archive(&archive_path, &unpack_dir).expect("Should unpack safely");

        // Verify
        assert_eq!(
            std::fs::read_to_string(unpack_dir.join("hello.txt")).unwrap(),
            "hello world"
        );
        assert_eq!(
            std::fs::read_to_string(unpack_dir.join("subdir/nested.txt")).unwrap(),
            "nested content"
        );

        std::fs::remove_dir_all(&temp_base).ok();
    }

    #[test]
    fn test_unpack_zip_slip_defense() {
        let temp_base = std::env::temp_dir().join(format!("macc_zip_slip_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let archive_path = temp_base.join("malicious.zip");
        let unpack_dir = temp_base.join("unpacked");
        std::fs::create_dir_all(&unpack_dir).unwrap();

        let file = std::fs::File::create(&archive_path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();

        let malicious_name = "../../evil.txt";
        let res = zip.start_file(malicious_name, options);

        if res.is_ok() {
            use std::io::Write;
            zip.write_all(b"evil").unwrap();
            zip.finish().unwrap();

            // Try to unpack
            let result = unpack_archive(&archive_path, &unpack_dir);
            assert!(result.is_err(), "Should have rejected Zip Slip entry");
        }

        std::fs::remove_dir_all(&temp_base).ok();
    }

    #[test]
    #[cfg(unix)]
    fn test_unpack_rejects_symlinks() {
        let temp_base =
            std::env::temp_dir().join(format!("macc_zip_symlink_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let archive_path = temp_base.join("symlink.zip");
        let unpack_dir = temp_base.join("unpacked");
        std::fs::create_dir_all(&unpack_dir).unwrap();

        let file = std::fs::File::create(&archive_path).unwrap();
        let mut zip = zip::ZipWriter::new(file);

        let options = zip::write::SimpleFileOptions::default();

        // zip.add_symlink("link.txt", "real.txt", options).unwrap();
        // If add_symlink doesn't exist, we might need another way.
        // Let's try to use unix_permissions again but with the full mode if it allows it.
        // Or maybe it's external_attributes.

        // Let's try to use a more manual way if add_symlink is not there.
        // I'll try add_symlink first.
        zip.add_symlink("link.txt", "real.txt", options).unwrap();

        zip.finish().unwrap();

        // Try to unpack
        let result = unpack_archive(&archive_path, &unpack_dir);
        assert!(result.is_err(), "Should have rejected symlink entry");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Symlinks are not supported"));

        std::fs::remove_dir_all(&temp_base).ok();
    }
}
