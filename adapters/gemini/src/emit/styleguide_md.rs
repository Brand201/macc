use crate::map::GeminiConfig;
use macc_adapter_shared::render::format::ensure_trailing_newline;

pub fn render_styleguide_md(config: &GeminiConfig) -> String {
    let mut md = String::from("# MACC Styleguide\n\n");
    md.push_str("## Standards\n");

    if config.standards_inline.is_empty() {
        md.push_str("- No inline standards configured.\n");
    } else {
        for (key, value) in &config.standards_inline {
            md.push_str(&format!("- {}: {}\n", key, value));
        }
    }

    if let Some(path) = &config.standards_path {
        md.push_str("\n## Additional Standards\n");
        md.push_str(&format!("- See `{}`\n", path));
    }

    ensure_trailing_newline(md)
}
