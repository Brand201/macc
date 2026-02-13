use crate::map::CodexConfig;
use macc_adapter_shared::render::format::ensure_trailing_newline;

pub fn render_agents_md(config: &CodexConfig) -> String {
    let mut md = String::from("# Project Instructions (MACC)\n\n");

    md.push_str("## Standards\n");
    if config.standards_inline.is_empty() {
        md.push_str("- No inline standards configured.\n");
    } else {
        for (key, value) in &config.standards_inline {
            md.push_str(&format!("- {}: {}\n", key, value));
        }
    }
    if let Some(path) = &config.standards_path {
        md.push_str(&format!("\nSee additional standards in: {}\n", path));
    }

    md.push_str("\n## Required Workflows\n");
    md.push_str("- Always run tests before committing.\n");
    md.push_str("- Use English for code, docs, and commit messages.\n");

    md.push_str("\n## Codex Skills\n");
    md.push_str("- Use `validate` to run the standard validation pipeline.\n");
    md.push_str("- Use `implement` for full implementation workflow.\n");

    md.push_str("\n## Workflow Chain (BMAD-lite)\n");
    md.push_str("- /brainstorm -> /prd -> /tech-stack -> /implementation-plan -> /implement\n");

    ensure_trailing_newline(md)
}
