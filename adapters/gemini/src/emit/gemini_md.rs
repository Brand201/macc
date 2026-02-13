use crate::map::GeminiConfig;
use macc_adapter_shared::render::format::ensure_trailing_newline;

pub fn render_gemini_md(config: &GeminiConfig) -> String {
    let mut md = String::from("# Project Instructions (MACC)\n\n");

    md.push_str("## Standards (summary)\n");
    if config.standards_inline.is_empty() {
        md.push_str("- No inline standards configured.\n");
    } else {
        for (key, value) in &config.standards_inline {
            md.push_str(&format!("- {}: {}\n", key, value));
        }
    }

    md.push_str("\n## Styleguide\n");
    md.push_str("- See `.gemini/styleguide.md` for the full, structured rules.\n");
    if let Some(path) = &config.standards_path {
        md.push_str(&format!("- Additional standards: `{}`\n", path));
    }

    md.push_str("\n## Required Workflows\n");
    md.push_str("- Run tests before committing.\n");
    md.push_str("- Use English for code, docs, and commit messages.\n");

    md.push_str("\n## MACC Commands\n");
    md.push_str("- Use `/validate` to run the standard validation pipeline.\n");
    md.push_str("- Use `/implement` for full implementation workflow.\n");

    if !config.mcp_servers.is_empty() {
        md.push_str("\n## MCP Servers\n");
        md.push_str("- Selected MCP servers are configured in `.gemini/settings.json`.\n");
        md.push_str("- Secrets (API keys, tokens) are NOT stored in the configuration.\n");
        md.push_str("- You must populate the required environment variables locally (e.g., in your shell).\n");
    }

    ensure_trailing_newline(md)
}
