use crate::commands::Command;
use macc_core::engine::Engine;
use macc_core::Result;
use std::path::{Path, PathBuf};

pub struct DoctorCommand<'a, E: Engine> {
    cwd: PathBuf,
    engine: &'a E,
    fix: bool,
}

impl<'a, E: Engine> DoctorCommand<'a, E> {
    pub fn new(cwd: &Path, engine: &'a E, fix: bool) -> Self {
        Self { cwd: cwd.to_path_buf(), engine, fix }
    }
}

impl<'a, E: Engine> Command for DoctorCommand<'a, E> {
    fn run(&self) -> Result<()> {
        let paths = macc_core::find_project_root(&self.cwd)?;
        crate::services::project::run_doctor(&paths, self.engine, self.fix)
    }
}
