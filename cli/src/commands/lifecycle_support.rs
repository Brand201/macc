use crate::commands::AppContext;
use macc_core::service::interaction::InteractionHandler;
use macc_core::{MaccError, ProjectPaths, Result};

pub(crate) struct CliFetchMaterializer;

impl macc_core::service::lifecycle::LifecycleFetchMaterializer for CliFetchMaterializer {
    fn materialize_fetch_units(
        &self,
        paths: &ProjectPaths,
        units: Vec<macc_core::resolve::FetchUnit>,
    ) -> Result<Vec<macc_core::resolve::MaterializedFetchUnit>> {
        macc_adapter_shared::fetch::materialize_fetch_units(paths, units)
    }
}

pub(crate) struct CliLifecycleUi;

impl InteractionHandler for CliLifecycleUi {
    fn info(&self, message: &str) {
        println!("{}", message);
    }

    fn warn(&self, message: &str) {
        eprintln!("{}", message);
    }

    fn error(&self, message: &str) {
        eprintln!("{}", message);
    }

    fn confirm_yes_no(&self, prompt: &str) -> Result<bool> {
        crate::confirm_yes_no(prompt)
    }
}

impl macc_core::service::lifecycle::LifecycleUi for CliLifecycleUi {
    fn print_checks(&self, checks: &[macc_core::doctor::ToolCheck]) {
        crate::print_checks(checks);
    }

    fn render_plan_preview(
        &self,
        paths: &ProjectPaths,
        plan: &macc_core::plan::ActionPlan,
        ops: &[macc_core::plan::PlannedOp],
        json: bool,
        explain: bool,
    ) -> Result<()> {
        crate::render_plan_preview(paths, plan, ops, json, explain)
    }

    fn print_pre_apply_summary(
        &self,
        paths: &ProjectPaths,
        plan: &macc_core::plan::ActionPlan,
        ops: &[macc_core::plan::PlannedOp],
    ) {
        crate::print_pre_apply_summary(paths, plan, ops);
    }

    fn print_pre_apply_explanations(&self, ops: &[macc_core::plan::PlannedOp]) {
        crate::print_pre_apply_explanations(ops);
    }

    fn confirm_user_scope_apply(
        &self,
        paths: &ProjectPaths,
        ops: &[macc_core::plan::PlannedOp],
    ) -> Result<()> {
        crate::confirm_user_scope_apply(paths, ops)
    }

    fn mark_apply_completed(&self, paths: &ProjectPaths) -> Result<()> {
        macc_core::service::context::mark_apply_completed(paths)
    }

    fn run_tui(&self) -> Result<()> {
        macc_tui::run_tui().map_err(|e| MaccError::Io {
            path: "tui".into(),
            action: "run_tui".into(),
            source: std::io::Error::other(e.to_string()),
        })
    }

    fn set_current_dir(&self, path: &std::path::Path) -> Result<()> {
        std::env::set_current_dir(path).map_err(|e| MaccError::Io {
            path: path.to_string_lossy().into(),
            action: "set current_dir for tui".into(),
            source: e,
        })
    }

    fn prompt_line(&self, prompt: &str) -> Result<String> {
        use std::io::{self, Write};
        print!("{}", prompt);
        io::stdout().flush().map_err(|e| MaccError::Io {
            path: "stdout".into(),
            action: "flush prompt".into(),
            source: e,
        })?;
        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .map_err(|e| MaccError::Io {
                path: "stdin".into(),
                action: "read input".into(),
                source: e,
            })?;
        Ok(input.trim().to_string())
    }

    fn is_command_available(&self, command: &str) -> bool {
        std::process::Command::new("sh")
            .arg("-lc")
            .arg(format!("command -v {} >/dev/null 2>&1", command))
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
}

pub(crate) fn init(app: &AppContext, force: bool, wizard: bool) -> Result<()> {
    macc_core::service::lifecycle::init(
        &app.cwd,
        app.engine.as_ref(),
        force,
        wizard,
        &CliLifecycleUi,
    )
}

pub(crate) fn plan(app: &AppContext, tools: Option<&str>, json: bool, explain: bool) -> Result<()> {
    macc_core::service::lifecycle::plan(
        &app.cwd,
        app.engine.as_ref(),
        tools,
        json,
        explain,
        &CliLifecycleUi,
        &CliFetchMaterializer,
    )
}

pub(crate) fn apply(
    app: &AppContext,
    tools: Option<&str>,
    dry_run: bool,
    allow_user_scope: bool,
    json: bool,
    explain: bool,
) -> Result<()> {
    macc_core::service::lifecycle::apply(
        &app.cwd,
        app.engine.as_ref(),
        tools,
        dry_run,
        allow_user_scope,
        json,
        explain,
        &CliLifecycleUi,
        &CliFetchMaterializer,
    )
}

pub(crate) fn quickstart(
    app: &AppContext,
    assume_yes: bool,
    apply: bool,
    no_tui: bool,
) -> Result<()> {
    macc_core::service::lifecycle::quickstart(
        &app.cwd,
        app.engine.as_ref(),
        assume_yes,
        apply,
        no_tui,
        &CliLifecycleUi,
        &CliFetchMaterializer,
    )
}
