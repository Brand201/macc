use crate::map::ClaudeConfig;
use macc_adapter_shared::render::format::ensure_trailing_newline;

pub fn render_claude_md(config: &ClaudeConfig) -> String {
    let mut md = String::from("# Project Instructions (MACC)\n\n");

    md.push_str(&format!("- **Primary Model**: {}\n", config.model));
    md.push_str(&format!("- **Language**: {}\n\n", config.language));

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

    md.push_str("\n## Common commands\n");
    md.push_str("- Install: `pnpm i`\n");
    md.push_str("- Lint: `pnpm lint`\n");
    md.push_str("- Build: `pnpm build`\n");
    md.push_str("- Test: `pnpm test`\n");

    md.push_str("\n## Context imports\n");
    md.push_str("- @README.md\n");
    if let Some(path) = &config.standards_path {
        md.push_str(&format!("- @{}\n", path));
    }
    if config.has_mcp {
        md.push_str("- @.mcp.json\n");
    }

    ensure_trailing_newline(md)
}
