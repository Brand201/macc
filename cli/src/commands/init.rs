use crate::commands::AppContext;
use crate::commands::Command;
use macc_core::Result;

pub struct InitCommand {
    app: AppContext,
    force: bool,
    wizard: bool,
}

impl InitCommand {
    pub fn new(app: AppContext, force: bool, wizard: bool) -> Self {
        Self { app, force, wizard }
    }
}

impl Command for InitCommand {
    fn run(&self) -> Result<()> {
        crate::commands::lifecycle_support::init(&self.app, self.force, self.wizard)
    }
}
