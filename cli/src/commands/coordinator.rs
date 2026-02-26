use crate::commands::Command;
use crate::coordinator::command::{handle, CoordinatorCommandInput};
use macc_core::Result;
use std::path::{Path, PathBuf};

pub struct CoordinatorCommand {
    cwd: PathBuf,
    input: CoordinatorCommandInput,
}

impl CoordinatorCommand {
    pub fn new(cwd: &Path, input: CoordinatorCommandInput) -> Self {
        Self {
            cwd: cwd.to_path_buf(),
            input,
        }
    }
}

impl Command for CoordinatorCommand {
    fn run(&self) -> Result<()> {
        handle(&self.cwd, self.input.clone())
    }
}
