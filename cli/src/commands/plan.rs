use crate::commands::Command;
use macc_core::engine::Engine;
use macc_core::Result;
use std::path::PathBuf;

pub struct PlanCommand<'a, E: Engine> {
    cwd: PathBuf,
    engine: &'a E,
    tools: Option<String>,
    json: bool,
    explain: bool,
}

impl<'a, E: Engine> PlanCommand<'a, E> {
    pub fn new(
        cwd: PathBuf,
        engine: &'a E,
        tools: Option<String>,
        json: bool,
        explain: bool,
    ) -> Self {
        Self {
            cwd,
            engine,
            tools,
            json,
            explain,
        }
    }
}

impl<'a, E: Engine> Command for PlanCommand<'a, E> {
    fn run(&self) -> Result<()> {
        crate::services::lifecycle::plan(
            &self.cwd,
            self.engine,
            self.tools.as_deref(),
            self.json,
            self.explain,
        )
    }
}
