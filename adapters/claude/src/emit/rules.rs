use crate::map::ClaudeConfig;
use macc_adapter_shared::render::format::ensure_trailing_newline;

pub struct ClaudeRuleFile {
    pub path: String,
    pub content: String,
}

pub fn render_rules(config: &ClaudeConfig) -> Vec<ClaudeRuleFile> {
    let mut rules = Vec::new();

    let mut code_style = String::from("# Code Style\n\n");
    if config.standards_inline.is_empty() {
        code_style.push_str("- Follow project standards defined in CLAUDE.md.\n");
    } else {
        for (key, value) in &config.standards_inline {
            code_style.push_str(&format!("- {}: {}\n", key, value));
        }
    }
    rules.push(ClaudeRuleFile {
        path: ".claude/rules/code-style.md".to_string(),
        content: ensure_trailing_newline(code_style),
    });

    let mut testing =
        String::from("---\npaths:\n  - \"**/*.test.*\"\n  - \"**/*.spec.*\"\n---\n\n");
    testing.push_str("# Testing\n\n");
    testing.push_str("- Ensure tests cover changes.\n");
    testing.push_str("- Prefer running targeted tests when possible.\n");
    rules.push(ClaudeRuleFile {
        path: ".claude/rules/testing.md".to_string(),
        content: ensure_trailing_newline(testing),
    });

    rules
}
