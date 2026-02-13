use crate::emit::{claude_md, rules, settings_json};
use crate::map::ClaudeConfig;
use crate::user_mcp_merge::plan_user_mcp_merge;
use macc_core::mcp_json;
use macc_core::plan::builders as plan_builders;
use macc_core::plan::ActionPlan;
use macc_core::resolve::{PlanningContext, SelectionKind};
use macc_core::ToolAdapter;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub struct ClaudeAdapter;

struct InstalledRemoteContent {
    skills: BTreeSet<String>,
    mcp_servers: BTreeMap<String, Value>,
}

impl ToolAdapter for ClaudeAdapter {
    fn id(&self) -> String {
        "claude".to_string()
    }

    fn plan(&self, ctx: &PlanningContext) -> macc_core::Result<ActionPlan> {
        let config = ClaudeConfig::from_resolved(ctx.resolved);
        let mut plan = ActionPlan::new();

        plan_builders::write_text(
            &mut plan,
            "CLAUDE.md",
            &claude_md::render_claude_md(&config),
        );
        plan_builders::write_text(
            &mut plan,
            ".claude/settings.json",
            &settings_json::render_settings_json(&config),
        );

        let installed_content = install_remote_content(&mut plan, ctx)?;

        add_skills(&mut plan, &config, &installed_content.skills);
        if !config.agents.is_empty() {
            add_agents(&mut plan, &config);
        }
        if config.rules_enabled {
            add_rules(&mut plan, &config);
        }

        plan_user_mcp_merge(&mut plan, &installed_content.mcp_servers)?;

        let mut all_mcp_servers = installed_content.mcp_servers.clone();
        let selection_ids: BTreeSet<String> = ctx.resolved.selections.mcp.iter().cloned().collect();
        for template in &ctx.resolved.mcp_templates {
            if selection_ids.contains(&template.id) {
                all_mcp_servers.insert(template.id.clone(), mcp_json::template_to_value(template));
            }
        }

        if !all_mcp_servers.is_empty() {
            let content = mcp_json::render_mcp_json(&all_mcp_servers);
            plan_builders::write_text(&mut plan, ".mcp.json", &content);
        }

        Ok(plan)
    }
}

fn install_remote_content(
    plan: &mut ActionPlan,
    ctx: &PlanningContext,
) -> macc_core::Result<InstalledRemoteContent> {
    let mut installed_skills = BTreeSet::new();
    let mut mcp_servers = BTreeMap::new();
    for unit in ctx.materialized_units {
        for selection in &unit.selections {
            match selection.kind {
                SelectionKind::Skill => {
                    plan_builders::plan_skill_install(
                        plan,
                        "claude",
                        &selection.id,
                        &unit.source_root_path,
                        &selection.subpath,
                    )
                    .map_err(macc_core::MaccError::Validation)?;
                    installed_skills.insert(selection.id.clone());
                }
                SelectionKind::Mcp => {
                    let manifest = plan_builders::plan_mcp_install(
                        plan,
                        &selection.id,
                        &unit.source_root_path,
                        &selection.subpath,
                    )
                    .map_err(macc_core::MaccError::Validation)?;
                    mcp_servers
                        .entry(selection.id.clone())
                        .or_insert_with(|| manifest.mcp.server.clone());
                }
            }
        }
    }
    Ok(InstalledRemoteContent {
        skills: installed_skills,
        mcp_servers,
    })
}

fn add_skills(plan: &mut ActionPlan, config: &ClaudeConfig, installed_skills: &BTreeSet<String>) {
    for skill in &config.skills {
        if installed_skills.contains(skill) {
            continue;
        }
        let content = render_skill_md(skill);
        plan_builders::write_text(plan, format!(".claude/skills/{}/SKILL.md", skill), &content);
    }
}

fn add_agents(plan: &mut ActionPlan, config: &ClaudeConfig) {
    for agent in &config.agents {
        let content = render_agent_md(agent);
        plan_builders::write_text(plan, format!(".claude/agents/{}.md", agent), &content);
    }
}

fn add_rules(plan: &mut ActionPlan, config: &ClaudeConfig) {
    for rule in rules::render_rules(config) {
        plan_builders::write_text(plan, rule.path, &rule.content);
    }
}

fn render_skill_md(name: &str) -> String {
    let (goal, steps, done) = match name {
        "create-plan" => (
            "Produce a structured implementation plan from a user request.",
            "1) Analyze the request and existing codebase.\n2) Identify required changes and affected files.\n3) Break down the task into small, reviewable steps.\n4) Format as a structured plan with clear objectives for each step.",
            "A coherent, actionable implementation plan is presented.",
        ),
        "implement" => (
            "End-to-end implementation workflow: read context, plan, implement, validate, review.",
            "1) Read relevant docs and files (CLAUDE.md, existing code).\n2) Propose a short implementation plan.\n3) Implement small, safe changes.\n4) Validate via tests and linting.\n5) Provide a short review summary and suggested commit message.",
            "Changes are implemented, validated, and reviewed.",
        ),
        "security-check" => (
            "Perform basic security checks for common issues and unsafe operations.",
            "1) Scan for hardcoded secrets (API keys, tokens).\n2) Identify unsafe file operations or path traversals.\n3) Check for outdated or vulnerable dependencies.\n4) Summarize findings with severity levels and mitigation steps.",
            "Security scan is complete and all findings are reported or resolved.",
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

fn render_agent_md(name: &str) -> String {
    match name {
        "architect" => {
            let content = r###"---
name: architect
description: Tech decisions, system design, planning.
model: opus
---

You are a software architect.
- Analyze requirements and propose technical solutions.
- Ensure system design aligns with architectural patterns.
- Create implementation plans.
"###;
            content.to_string()
        }
        "reviewer" => {
            let content = r###"---
name: reviewer
description: Reviews code changes for correctness, security, and maintainability.
model: inherit
---

You are a meticulous code reviewer.
- Identify correctness issues, edge cases, and risky changes.
- Flag security pitfalls (secrets, injection, auth).
- Prefer small, actionable suggestions.
- Follow project standards from CLAUDE.md and rules.
"###;
            content.to_string()
        }
        _ => format!(
            "---\nname: {0}\ndescription: MACC agent {0}.\nmodel: inherit\n---\n\nYou are the {0} agent. Follow project standards and provide concise, actionable guidance.\n",
            name
        ),
    }
}
