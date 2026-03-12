use crate::commands::AppContext;
use crate::commands::Command;
use macc_core::Result;

pub struct PlanCommand {
    app: AppContext,
    tools: Option<String>,
    json: bool,
    explain: bool,
}

impl PlanCommand {
    pub fn new(app: AppContext, tools: Option<String>, json: bool, explain: bool) -> Self {
        Self {
            app,
            tools,
            json,
            explain,
        }
    }
}

impl Command for PlanCommand {
    fn run(&self) -> Result<()> {
        crate::commands::lifecycle_support::plan(
            &self.app,
            self.tools.as_deref(),
            self.json,
            self.explain,
        )
    }
}
