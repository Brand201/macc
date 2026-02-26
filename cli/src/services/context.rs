use macc_core::Result;

pub fn run_generation(
    paths: &macc_core::ProjectPaths,
    tool: Option<&str>,
    from_files: &[String],
    dry_run: bool,
    print_prompt: bool,
) -> Result<()> {
    crate::run_context_generation(paths, tool, from_files, dry_run, print_prompt)
}
