use crate::commands::Command;
use macc_core::Result;
use std::path::{Path, PathBuf};

pub struct ContextCommand<'a> {
    cwd: PathBuf,
    tool: Option<&'a str>,
    from_files: &'a [String],
    dry_run: bool,
    print_prompt: bool,
}

impl<'a> ContextCommand<'a> {
    pub fn new(
        cwd: &Path,
        tool: Option<&'a str>,
        from_files: &'a [String],
        dry_run: bool,
        print_prompt: bool,
    ) -> Self {
        Self { cwd: cwd.to_path_buf(), tool, from_files, dry_run, print_prompt }
    }
}

impl<'a> Command for ContextCommand<'a> {
    fn run(&self) -> Result<()> {
        let paths = crate::services::project::ensure_initialized_paths(&self.cwd)?;
        crate::services::ops::run_context_generation(
            &paths,
            self.tool,
            self.from_files,
            self.dry_run,
            self.print_prompt,
        )
    }
}
