use crate::commands::Command;
use macc_core::engine::Engine;
use macc_core::Result;
use std::path::PathBuf;

pub struct QuickstartCommand<'a, E: Engine> {
    cwd: PathBuf,
    engine: &'a E,
    yes: bool,
    apply: bool,
    no_tui: bool,
}

impl<'a, E: Engine> QuickstartCommand<'a, E> {
    pub fn new(cwd: PathBuf, engine: &'a E, yes: bool, apply: bool, no_tui: bool) -> Self {
        Self {
            cwd,
            engine,
            yes,
            apply,
            no_tui,
        }
    }
}

impl<'a, E: Engine> Command for QuickstartCommand<'a, E> {
    fn run(&self) -> Result<()> {
        crate::services::lifecycle::quickstart(
            &self.cwd,
            self.engine,
            self.yes,
            self.apply,
            self.no_tui,
        )
    }
}
