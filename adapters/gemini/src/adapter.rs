use crate::emit::{gemini_md, geminiignore, settings_json, styleguide_md};
use crate::map::GeminiConfig;
use crate::user_mcp_merge::plan_user_mcp_merge;
use macc_adapter_shared::render::format::render_toml;
use macc_core::plan::builders as plan_builders;
use macc_core::plan::ActionPlan;
use macc_core::resolve::{PlanningContext, SelectionKind};
use macc_core::ToolAdapter;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};
use toml::Value as TomlValue;

pub struct GeminiAdapter;

impl ToolAdapter for GeminiAdapter {
    fn id(&self) -> String {
        "gemini".to_string()
    }

    fn plan(&self, ctx: &PlanningContext) -> macc_core::Result<ActionPlan> {
        let mut config = GeminiConfig::from_resolved(ctx.resolved);
        let mut plan = ActionPlan::new();

        let mcp_servers = collect_mcp_servers(ctx)?;
        config.mcp_servers.extend(mcp_servers.clone());

        plan_builders::write_text(
            &mut plan,
            "GEMINI.md",
            &gemini_md::render_gemini_md(&config),
        );
        plan_builders::write_text(
            &mut plan,
            ".gemini/settings.json",
            &settings_json::render_settings_json(&config),
        );
        plan_builders::write_text(
            &mut plan,
            ".gemini/styleguide.md",
            &styleguide_md::render_styleguide_md(&config),
        );
        plan_builders::write_text(
            &mut plan,
            ".geminiignore",
            &geminiignore::render_geminiignore(),
        );

        let installed_skills = install_remote_skills(&mut plan, ctx)?;
        add_commands(&mut plan, &config, &installed_skills);
        add_skills(&mut plan, &config, &installed_skills);

        if config.user_mcp_merge {
            plan_user_mcp_merge(&mut plan, &mcp_servers)?;
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
                    "gemini",
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

fn add_commands(plan: &mut ActionPlan, config: &GeminiConfig, cached: &BTreeSet<String>) {
    for name in &config.skills {
        if cached.contains(name) {
            continue;
        }
        let (desc, prompt) = command_prompt(name);
        let toml_value = command_toml(&desc, &prompt);
        let content = render_toml(&toml_value);
        plan_builders::write_text(plan, format!(".gemini/commands/{}.toml", name), &content);
    }
}

fn command_toml(description: &str, prompt: &str) -> TomlValue {
    let mut table = toml::map::Map::new();
    table.insert(
        "description".to_string(),
        TomlValue::String(description.to_string()),
    );
    table.insert("prompt".to_string(), TomlValue::String(prompt.to_string()));
    TomlValue::Table(table)
}

fn command_prompt(name: &str) -> (String, String) {
    match name {
        "validate" => (
            "Run the standard validation pipeline (lint -> build -> e2e tests).".to_string(),
            "You are executing the project's validation workflow.\n\n1) Run:\n- pnpm lint\n- pnpm build\n- pnpm test:e2e\n\n2) Summarize failures clearly and propose fixes.\n3) If all succeeds, report success and next steps."
                .to_string(),
        ),
        "implement" => (
            "End-to-end implementation workflow: read context, plan, implement, validate, review."
                .to_string(),
            "Follow this workflow:\n1) Read relevant docs and files (GEMINI.md, memory-bank, existing code).\n2) Propose a short implementation plan.\n3) Implement small, safe changes.\n4) Validate via /validate (or equivalent commands).\n5) Provide a short review summary and suggested commit message."
                .to_string(),
        ),
        _ => (
            format!("MACC Skill: {}", name),
            "Execute this workflow following MACC standards:\n1) Clarify inputs.\n2) Plan briefly.\n3) Execute safely.\n4) Summarize outcomes and next steps."
                .to_string(),
        ),
    }
}

fn add_skills(plan: &mut ActionPlan, config: &GeminiConfig, cached: &BTreeSet<String>) {
    for skill in &config.skills {
        if cached.contains(skill) {
            continue;
        }
        let content = render_skill_md(skill);
        plan_builders::write_text(plan, format!(".gemini/skills/{}/SKILL.md", skill), &content);
    }

    for agent in &config.agents {
        let content = render_skill_md(agent);
        plan_builders::write_text(plan, format!(".gemini/skills/{}/SKILL.md", agent), &content);
    }
}

fn collect_mcp_servers(ctx: &PlanningContext) -> macc_core::Result<BTreeMap<String, JsonValue>> {
    let mut mcp_servers = BTreeMap::new();

    for unit in ctx.materialized_units {
        for selection in &unit.selections {
            if selection.kind != SelectionKind::Mcp {
                continue;
            }

            let mcp_path = if selection.subpath.is_empty() || selection.subpath == "." {
                unit.source_root_path.clone()
            } else {
                unit.source_root_path.join(&selection.subpath)
            };

            let manifest = macc_core::packages::validate_mcp_folder(&mcp_path, &selection.id)
                .map_err(macc_core::MaccError::Validation)?;

            mcp_servers
                .entry(selection.id.clone())
                .or_insert_with(|| manifest.mcp.server.clone());
        }
    }

    Ok(mcp_servers)
}

fn render_skill_md(name: &str) -> String {
    let (goal, steps, done) = match name {
        "validate" => (
            "Run the project validation pipeline and report results.",
            "1) Run `pnpm lint`.\n2) Run `pnpm build`.\n3) Run `pnpm test:e2e`.\n4) Summarize failures and propose fixes.",
            "All validation steps pass or remaining failures are clearly explained.",
        ),
        "implement" => (
            "Deliver a change end-to-end with planning, implementation, and validation.",
            "1) Read relevant context (GEMINI.md, styleguide, code).\n2) Propose a short plan.\n3) Implement small, safe changes.\n4) Validate using `/validate`.\n5) Summarize changes and suggest a commit message.",
            "Implementation is complete, validated, and summarized with next steps.",
        ),
        _ => (
            "Execute the workflow for this skill following MACC standards.",
            "1) Clarify inputs and scope.\n2) Plan briefly.\n3) Execute safely.\n4) Summarize outcomes and next steps.",
            "The workflow is completed with a clear summary.",
        ),
    };

    let mut md = String::new();
    md.push_str(&format!("# /{}\n\n", name));
    md.push_str("## Goal\n");
    md.push_str(goal);
    md.push_str("\n\n## Steps\n");
    md.push_str(steps);
    md.push_str("\n\n## Done when\n");
    md.push_str(done);
    md.push('\n');
    md
}
