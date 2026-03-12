#[cfg(test)]
pub(crate) fn run_git_ok(dir: &std::path::Path, args: &[&str]) {
    let output = macc_core::git::run_git_output_mapped(dir, args, "run test git command")
        .unwrap_or_else(|e| panic!("git command failed {:?}: {}", args, e));
    if !output.status.success() {
        panic!(
            "git command failed: {:?} -> {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
