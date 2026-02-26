use crate::commands::Command;
use macc_core::engine::Engine;
use macc_core::Result;
use std::path::PathBuf;

pub struct ApplyCommand<'a, E: Engine> {
    cwd: PathBuf,
    engine: &'a E,
    tools: Option<String>,
    dry_run: bool,
    allow_user_scope: bool,
    json: bool,
    explain: bool,
}

impl<'a, E: Engine> ApplyCommand<'a, E> {
    pub fn new(
        cwd: PathBuf,
        engine: &'a E,
        tools: Option<String>,
        dry_run: bool,
        allow_user_scope: bool,
        json: bool,
        explain: bool,
    ) -> Self {
        Self {
            cwd,
            engine,
            tools,
            dry_run,
            allow_user_scope,
            json,
            explain,
        }
    }
}

impl<'a, E: Engine> Command for ApplyCommand<'a, E> {
    fn run(&self) -> Result<()> {
        crate::services::lifecycle::apply(
            &self.cwd,
            self.engine,
            self.tools.as_deref(),
            self.dry_run,
            self.allow_user_scope,
            self.json,
            self.explain,
        )
    }
}
