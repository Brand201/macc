use crate::commands::Command;
use crate::commands::AppContext;
use macc_core::Result;

pub struct ContextCommand<'a> {
    app: AppContext,
    tool: Option<&'a str>,
    from_files: &'a [String],
    dry_run: bool,
    print_prompt: bool,
}

impl<'a> ContextCommand<'a> {
    pub fn new(
        app: AppContext,
        tool: Option<&'a str>,
        from_files: &'a [String],
        dry_run: bool,
        print_prompt: bool,
    ) -> Self {
        Self { app, tool, from_files, dry_run, print_prompt }
    }
}

impl<'a> Command for ContextCommand<'a> {
    fn run(&self) -> Result<()> {
        let paths = self.app.ensure_initialized_paths()?;
        crate::services::context::run_generation(
            &paths,
            self.tool,
            self.from_files,
            self.dry_run,
            self.print_prompt,
        )
    }
}
