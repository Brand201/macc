use crate::commands::Command;
use macc_core::engine::Engine;
use macc_core::Result;
use std::path::PathBuf;

pub struct InitCommand<'a, E: Engine> {
    cwd: PathBuf,
    engine: &'a E,
    force: bool,
    wizard: bool,
}

impl<'a, E: Engine> InitCommand<'a, E> {
    pub fn new(cwd: PathBuf, engine: &'a E, force: bool, wizard: bool) -> Self {
        Self {
            cwd,
            engine,
            force,
            wizard,
        }
    }
}

impl<'a, E: Engine> Command for InitCommand<'a, E> {
    fn run(&self) -> Result<()> {
        crate::services::ops::handle_init_command(&self.cwd, self.engine, self.force, self.wizard)
    }
}
