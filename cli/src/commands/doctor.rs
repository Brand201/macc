use crate::commands::AppContext;
use crate::commands::Command;
use macc_core::Result;

pub struct DoctorCommand {
    app: AppContext,
    fix: bool,
}

impl DoctorCommand {
    pub fn new(app: AppContext, fix: bool) -> Self {
        Self { app, fix }
    }
}

impl Command for DoctorCommand {
    fn run(&self) -> Result<()> {
        let paths = self.app.project_paths()?;
        self.app.engine.project_run_doctor(
            &paths,
            self.fix,
            &crate::services::interaction::CliInteraction,
        )
    }
}
