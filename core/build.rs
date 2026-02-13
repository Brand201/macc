use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=../adapters");

    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR missing"));
    let repo_root = manifest_dir
        .parent()
        .expect("core crate must be inside repository root");
    let adapters_dir = repo_root.join("adapters");

    let mut runner_paths = collect_runner_paths(&adapters_dir)
        .into_iter()
        .filter_map(|p| p.strip_prefix(repo_root).ok().map(path_to_forward_slashes))
        .collect::<Vec<_>>();
    runner_paths.sort();
    runner_paths.dedup();

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR missing"));
    let dest_path = out_dir.join("embedded_automation_runners.rs");

    let mut generated = String::from("pub const EMBEDDED_RUNNERS: &[(&str, &str)] = &[\n");
    for rel in runner_paths {
        let rel_lit = format!("{:?}", rel);
        generated.push_str("    (");
        generated.push_str(&rel_lit);
        generated.push_str(", include_str!(concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/../");
        generated.push_str(&rel);
        generated.push_str("\"))),\n");
    }
    generated.push_str("];\n");

    fs::write(dest_path, generated).expect("failed to write embedded_automation_runners.rs");
}

fn collect_runner_paths(adapters_dir: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let Ok(entries) = fs::read_dir(adapters_dir) else {
        return paths;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Ok(sub_entries) = fs::read_dir(&path) else {
            continue;
        };
        for sub in sub_entries.flatten() {
            let sub_path = sub.path();
            let Some(name) = sub_path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if name.ends_with(".performer.sh") {
                paths.push(sub_path);
            }
        }
    }

    paths
}

fn path_to_forward_slashes(path: &Path) -> String {
    path.components()
        .map(|c| c.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join("/")
}
