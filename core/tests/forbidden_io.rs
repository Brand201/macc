use std::fs;
use std::path::{Path, PathBuf};

fn collect_rs_files(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

#[test]
fn test_core_forbidden_io_patterns() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let core_src = manifest_dir.join("src");

    // Transitional allow-list for modules that still expose shell-facing output paths.
    // Keep this list short and remove entries as migration to UI-agnostic core completes.
    let allowed_rel_paths = ["src/lib.rs", "src/coordinator/state.rs"];

    let forbidden_patterns = [
        "println!(",
        "eprintln!(",
        "std::io::stdin",
        "io::stdin()",
    ];

    let mut files = Vec::new();
    collect_rs_files(&core_src, &mut files);

    let mut violations = Vec::new();
    for file in files {
        let rel = file
            .strip_prefix(&manifest_dir)
            .unwrap_or(file.as_path())
            .to_string_lossy()
            .to_string();
        if allowed_rel_paths.iter().any(|allowed| rel == *allowed) {
            continue;
        }
        let Ok(content) = fs::read_to_string(&file) else {
            continue;
        };
        for pattern in forbidden_patterns {
            if content.contains(pattern) {
                violations.push(format!("{} contains forbidden pattern `{}`", rel, pattern));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "Forbidden IO usage detected in core (move to InteractionHandler/log traits):\n{}",
        violations.join("\n")
    );
}

