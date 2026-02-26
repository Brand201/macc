use crate::commands::Command;
use crate::commands::AppContext;
use crate::coordinator::command::{handle, CoordinatorCommandInput};
use macc_core::Result;

pub struct CoordinatorCommand {
    app: AppContext,
    input: CoordinatorCommandInput,
}

impl CoordinatorCommand {
    pub fn new(app: AppContext, input: CoordinatorCommandInput) -> Self {
        Self { app, input }
    }
}

impl Command for CoordinatorCommand {
    fn run(&self) -> Result<()> {
        handle(&self.app.cwd, self.input.clone())
    }
}
