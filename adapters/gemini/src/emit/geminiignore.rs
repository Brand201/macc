use macc_adapter_shared::render::format::ensure_trailing_newline;

pub fn render_geminiignore() -> String {
    let content = r###"# dependencies / builds
node_modules/
dist/
build/
.next/
.out/

# VCS
.git/
.worktrees/

# secrets
.env
.env.*
*.pem
*.key

# MACC internal backups
.macc/
.gemini/
"###;

    ensure_trailing_newline(content.to_string())
}
