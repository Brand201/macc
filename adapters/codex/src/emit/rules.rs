use macc_adapter_shared::render::format::ensure_trailing_newline;

pub fn render_default_rules() -> String {
    let content = r###"# Allow read-only git commands outside sandbox
prefix_rule(
  pattern=["git", ["status", "diff", "log"]],
  decision="allow",
  justification="Read-only git commands are safe.",
  match=["git status", "git diff"],
)

# Prompt before pushing
prefix_rule(
  pattern=["git", "push"],
  decision="prompt",
  justification="Pushing changes requires confirmation.",
  match=["git push"],
)

# Forbid catastrophic deletes
prefix_rule(
  pattern=["rm", "-rf", "/"],
  decision="forbidden",
  justification="Never delete system root.",
  match=["rm -rf /"],
)
"###;

    ensure_trailing_newline(content.to_string())
}
