use crate::emit::{agents_md, config_toml, rules};
use crate::map::CodexConfig;
use macc_core::plan::builders as plan_builders;
use macc_core::plan::ActionPlan;
use macc_core::resolve::{PlanningContext, SelectionKind};
use macc_core::ToolAdapter;
use std::collections::BTreeSet;

pub struct CodexAdapter;

impl ToolAdapter for CodexAdapter {
    fn id(&self) -> String {
        "codex".to_string()
    }

    fn plan(&self, ctx: &PlanningContext) -> macc_core::Result<ActionPlan> {
        let config = CodexConfig::from_resolved(ctx.resolved);
        let mut plan = ActionPlan::new();

        if config.tool_config.rules_enabled.unwrap_or(false) {
            plan_builders::write_text(
                &mut plan,
                "AGENTS.md",
                &agents_md::render_agents_md(&config),
            );
        }
        plan_builders::write_text(
            &mut plan,
            ".codex/config.toml",
            &config_toml::render_config_toml(&config.tool_config),
        );

        let installed_skills = install_remote_skills(&mut plan, ctx)?;
        add_skills(&mut plan, &config, &installed_skills);
        if config.tool_config.rules_enabled.unwrap_or(false) {
            plan_builders::write_text(
                &mut plan,
                ".codex/rules/default.rules",
                &rules::render_default_rules(),
            );
        }

        Ok(plan)
    }
}

fn install_remote_skills(
    plan: &mut ActionPlan,
    ctx: &PlanningContext,
) -> macc_core::Result<BTreeSet<String>> {
    let mut installed = BTreeSet::new();
    for unit in ctx.materialized_units {
        for selection in &unit.selections {
            if selection.kind == SelectionKind::Skill {
                plan_builders::plan_skill_install(
                    plan,
                    "codex",
                    &selection.id,
                    &unit.source_root_path,
                    &selection.subpath,
                )
                .map_err(macc_core::MaccError::Validation)?;
                installed.insert(selection.id.clone());
            }
        }
    }
    Ok(installed)
}

fn add_skills(plan: &mut ActionPlan, config: &CodexConfig, installed_skills: &BTreeSet<String>) {
    for skill in &config.skills {
        if installed_skills.contains(skill) {
            continue;
        }
        let content = render_skill_md(skill);
        plan_builders::write_text(plan, format!(".codex/skills/{}/SKILL.md", skill), &content);
    }
}

fn render_skill_md(name: &str) -> String {
    let (description, body) = match name {
        "implement" => (
            "End-to-end implementation workflow: read context, plan, implement, validate, review.",
            "Run:\n1) Read relevant docs and files (AGENTS.md, existing code).\n2) Propose a short implementation plan.\n3) Implement small, safe changes.\n4) Validate via `validate` skill.\n5) Provide a short review summary and suggested commit message.\n",
        ),
        "validate" => (
            "Run the project validation workflow when the user asks to validate, run tests, or verify changes.",
            "Run:\n1) `pnpm lint`\n2) `pnpm build`\n3) `pnpm test:e2e`\n\nIf any step fails:\n- report the first failing command and its output summary\n- propose the smallest fix and rerun only the failing step\n\nNever skip validation steps unless the user explicitly requests it.\n",
        ),
        _ => (
            "MACC skill workflow.",
            "Execute this workflow following MACC standards.\n",
        ),
    };

    let mut md = String::new();
    md.push_str("---\n");
    md.push_str(&format!("name: {}\n", name));
    md.push_str(&format!("description: {}\n", description));
    md.push_str("---\n\n");
    md.push_str(body);
    if !md.ends_with('\n') {
        md.push('\n');
    }
    md
}
