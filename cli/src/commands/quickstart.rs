use crate::commands::AppContext;
use crate::commands::Command;
use macc_core::Result;

pub struct QuickstartCommand {
    app: AppContext,
    yes: bool,
    apply: bool,
    no_tui: bool,
}

impl QuickstartCommand {
    pub fn new(app: AppContext, yes: bool, apply: bool, no_tui: bool) -> Self {
        Self {
            app,
            yes,
            apply,
            no_tui,
        }
    }
}

impl Command for QuickstartCommand {
    fn run(&self) -> Result<()> {
        crate::commands::lifecycle_support::quickstart(&self.app, self.yes, self.apply, self.no_tui)
    }
}
