# MACC × Codex CLI : Implementation Plan

**Goal:** integrate **OpenAI Codex CLI** into **MACC** so that Codex is fully configurable via MACC’s canonical config and usable across machines/projects with reproducible project-level files, optional user-level merges (backup + consent), and TUI control.

This plan aligns with:
- MACC’s v1 objectives: canonical “source of truth”, per-tool adapters, TUI (Rust + Ratatui), backups/consent, worktrees/parallelism.
- Codex CLI concepts: **Team Config** (`.codex/`), `config.toml` precedence, **skills**, **rules**, and **AGENTS.md** discovery.

---

## 1) Integration outcomes

### 1.1 What “fully usable by MACC” means
MACC must be able to, at minimum:

1. **Generate project-level Codex configuration** under `.codex/`:
   - `.codex/config.toml`
   - `.codex/skills/**/SKILL.md`
   - `.codex/rules/*.rules` (optional, but recommended for safety)

2. **Generate project instructions** as `AGENTS.md` (and optionally `AGENTS.override.md`) consistent with MACC standards and project conventions.

3. **Optionally merge user-level configuration** under `~/.codex/` (or `$CODEX_HOME`) with:
   - timestamped backups
   - preview (diff/summary) in TUI
   - explicit consent before writing

4. Support **configuration precedence** as Codex expects:
   - project `.codex/` (Team Config, repo-scoped)
   - user `.codex/` (Team Config, user-scoped)
   - command-line overrides (via MACC wrappers when needed)

5. Make Codex usable within **worktrees**:
   - `macc worktree run <id>` can launch Codex in that worktree using the generated Team Config.
   - `macc apply --cwd <worktree>` produces `.codex/` inside the worktree.

---

## 2) Codex CLI concepts MACC must model

### 2.1 Team Config locations and precedence
Codex merges `config.toml`, `rules/`, and `skills/` across “Team Config” directories in a fixed precedence order. MACC must treat `.codex/` as a standard Team Config unit and generate it accordingly.

**Implication for MACC:** project-level `.codex/` should be the primary reproducible layer; user-level `~/.codex/` should be an optional “defaults layer” with safe merge behavior.

### 2.2 `config.toml` stack, profiles, and one-off overrides
Codex resolves values in this order:
1. CLI flags (e.g., `--model`)
2. Profile values (`--profile <name>`)
3. `config.toml` values merged across Team Config locations
4. built-in defaults

**MACC implication:** MACC should:
- generate base values at root-level in `.codex/config.toml`
- generate optional profiles for common “modes” (e.g., deep-review, lightweight)
- optionally generate a default `profile = "..."` when the user selects it in TUI
- provide a wrapper to pass one-off overrides using `codex --config key=value` when needed

### 2.3 Skills (`.codex/skills/**/SKILL.md`)
Skills use YAML front matter (`name`, `description`) plus optional markdown body. Codex injects only name/description/path into runtime context; the body is injected only when invoked.

**MACC implication:** skills are a natural place to package MACC workflows (validate/implement/next-task/etc.) for Codex.

### 2.4 Rules (`.codex/rules/*.rules`)
Rules control which commands Codex can run outside sandbox; they are written in Starlark, typically using `prefix_rule(...)`. Rules can be tested with `codex execpolicy check ...`.

**MACC implication:** MACC should generate a default safe rule set aligned with MACC’s “explicit permissions” goal, and provide a TUI editor for adding/adjusting rules at the project or user layer.

### 2.5 Project instructions (`AGENTS.md`)
Codex reads `AGENTS.md` / `AGENTS.override.md` in a discovery chain (global then project path).

**MACC implication:** MACC should generate a canonical project `AGENTS.md` from MACC standards + selected skills/workflows, and optionally an `AGENTS.override.md` for locally overridden instructions in user-level merges (never committed).

---

## 3) MACC architecture additions

### 3.1 Canonical config: extend `.macc/macc.yaml`
Add a `tools.codex` section that captures *all* Codex config values MACC wants to support.

**Proposed schema (YAML):**
```yaml
tools:
  codex:
    enabled: true

    # Where MACC writes Team Config for Codex
    install_scope:
      project: true
      user: false  # user merges always require consent

    # Codex config.toml root keys
    model: "gpt-5.2" # gpt-5.2-codex, gpt-5.2, gpt-5.1-codex-mini, gpt-5.1-codex-max, gpt-5.1, gpt-5.1-codex, gpt-5-codex, gpt-5-codex-mini, gpt-5
    profile: null  # or "deep-review"
    approval_policy: "on-request"
    sandbox_mode: "workspace-write"
    model_reasoning_effort: "high"

    shell_environment_policy:
      include_only: ["PATH", "HOME"]

    features:
      exec_policy: true
      undo: true
      web_search_request: false
      shell_snapshot: false
      # allow unknown feature keys (future-proof)

    # Optional: profiles (CLI only)
    profiles:
      deep-review:
        model: "gpt-5-pro"
        model_reasoning_effort: "high"
        approval_policy: "never"
      lightweight:
        model: "gpt-4.1"
        approval_policy: "untrusted"

    # Optional: per-project trust levels
    projects:
      # absolute paths optional; MACC can set at user layer
      trust_level_default: "untrusted"

    # Optional telemetry (off by default)
    otel:
      enabled: false
      log_user_prompt: false
      environment: "dev"
      exporter: "none"
      trace_exporter: "none"

    # MCP server templates (no secrets in repo)
    mcp_servers:
      context7:
        enabled: true
        transport: "stdio"
        command: "context7-mcp"
        args: []
        env: {}
```

**Notes:**
- Keep the canonical config tool-agnostic where possible (e.g., MACC “permissions” map into Codex rules; “skills” map into `.codex/skills`).  
- Mark user-level sections clearly as “requires consent”, and always keep secrets out of versioned files.

---

## 4) Codex adapter: generation strategy

### 4.1 Outputs MACC will generate for Codex
**Project-level (versioned):**
- `.codex/config.toml`
- `.codex/skills/**/SKILL.md`
- `.codex/rules/*.rules` (recommended)
- `AGENTS.md` at repo root (or configured path)

**User-level (optional, non-versioned):**
- `~/.codex/config.toml` merge (or `$CODEX_HOME/config.toml`)
- `~/.codex/rules/default.rules` merge/append (optional)
- `~/.codex/skills/**` (optional)

### 4.2 `config.toml` generation rules
- Generate *root-level keys* for the “default” configuration.
- Generate `[profiles.*]` only when configured in `.macc/macc.yaml`.
- Generate `[features]` keys only if user sets them explicitly; otherwise omit to keep Codex defaults.
- Generate `shell_environment_policy` only if set; default should be conservative.

**Template strategy:**
- Use a typed struct model in Rust -> render into TOML with a TOML serializer (preferred), not string concatenation.
- Use stable ordering and comments so diffs are readable.

### 4.3 Skills generation
MACC’s skill catalog should include Codex variants. For v1, implement at least:
- `validate`
- `implement`
- `next-task`
- `refresh-context`
- `update-progress`
- `git-add-commit-push`
- `validate-update-push`
- `security-check`
- `validate-quick`

**Codex skill format:**
Each skill lives at `.codex/skills/<skill-name>/SKILL.md` with YAML front matter and optional body.
- `name`: keep short, stable, kebab-case (e.g., `validate`, `implement`, `next-task`)
- `description`: include trigger hints (“Use when user asks…”) and expected steps

**MACC rule:** skill body can reference additional files stored in the same skill folder (templates, scripts), but keep skills instruction-first.

### 4.4 Rules generation
Provide a default `default.rules` that aligns with MACC’s “permissions” philosophy:
- allow safe read-only commands without prompt (e.g., `rg`, `ls`, `cat`, `git status`, `git diff`)
- prompt for potentially dangerous or mutating actions outside sandbox (e.g., `git push`, `rm`, `curl`, `chmod`, package installs)
- forbid catastrophic patterns (e.g., `rm -rf /`)

**Rule authoring constraints:**
- Prefer `prefix_rule` rules with exact argument prefixes.
- Include `match` / `not_match` examples as inline unit tests.
- When possible, “prompt” over “allow” for ambiguous commands.

**Validation hook (optional):**
If `codex` binary is present, run:
```
codex execpolicy check --pretty --rules <generated rules file> -- <command args...>
```
against a small list of representative commands to ensure the rules parse and behave as expected.
If `codex` is missing, skip with a warning.

### 4.5 `AGENTS.md` generation
Generate a project `AGENTS.md` that contains:
- project coding standards (from MACC `standards.md` canonical file)
- the high-level workflow (BMAD-lite chain) used in the repo
- how to use skills (explicit mapping: “For validation use skill `validate`”)
- safety/permissions expectations
- where artifacts live (e.g., `memory-bank/`, `progress.md`)

**Layering guidance:**
- MACC should never overwrite `AGENTS.override.md` if it exists (treat it as user-owned).
- MACC may generate `AGENTS.md` and keep it reproducible.

---

## 5) `macc apply` pipeline changes for Codex

### 5.1 Apply stages (Codex-related)
1. Resolve effective configuration: `.macc/macc.yaml` + presets + TUI selections
2. Render outputs:
   - `.codex/config.toml`
   - `.codex/skills/*`
   - `.codex/rules/*`
   - `AGENTS.md`
3. Safety checks:
   - ensure no secrets (scan for common patterns; warn if present)
   - ensure `.gitignore` includes `~/.codex` artifacts only if they leak into repo (avoid)
4. Atomic writes + backups:
   - for project files: backup existing files to `.macc/backups/<timestamp>/...`
   - for user files: backup to `~/.macc/backups/<timestamp>/codex/...`
5. Write changes

### 5.2 Merge policies (user-level)
Implement a clear merge strategy per file type:
- `config.toml`: merge TOML tables (MACC-managed section) without deleting unknown keys
- `rules/*.rules`: append MACC-managed rules in a dedicated block with markers, or generate a separate file (preferred) so users can keep their own `default.rules`
- `skills/`: install MACC skills under a namespace folder or ensure non-conflicting names; do not overwrite user skills without consent

**Consent UX:**
- Show a summary:
  - which files change
  - backups created
  - whether content is overwritten/merged/appended
- Require an explicit confirmation step before writing user-level.

---

## 6) MACC CLI commands for Codex

### 6.1 Required commands
- `macc init`  
  Creates `.macc/` + baseline config enabling tools, including Codex scaffolding.

- `macc apply [--tools codex] [--user] [--cwd <path>]`  
  Generates `.codex/` + `AGENTS.md`.  
  `--user` enables user-level merges (with interactive consent or `--yes` if provided).

- `macc doctor codex`  
  Checks:
  - Codex CLI presence (`codex --version`)
  - Team Config presence in project (`.codex/`)
  - `AGENTS.md` presence
  - warnings about unsupported profiles in IDE (if using Codex IDE extension)

- `macc tool codex run [--cwd <path>] [--profile <name>] [--config <k=v>...]`  
  Wraps `codex` invocation and ensures working directory is set properly for Team Config discovery.
  - Use `--config` to pass one-off overrides (`codex --config key=value`)

### 6.2 Optional commands
- `macc tool codex exec ...`  
  Wraps `codex exec` for CI-style runs.

- `macc tool codex mcp sync`  
  Applies MCP server entries from MACC canonical config into the *user layer* (requires consent).

---

## 7) TUI (Rust + Ratatui) requirements for Codex

### 7.1 Codex-specific screens or panels
Add a “Codex” configuration section when Codex is selected:

1) **Model & profile**
- Model picker (string + recent values)
- Profile picker (from canonical config) + option to set default profile

2) **Safety**
- Approval policy (enum)
- Sandbox mode (enum)
- Reasoning effort (enum)
- Feature toggles: `exec_policy`, `undo`, `web_search_request`, `shell_snapshot`, etc.

3) **Environment policy**
- `shell_environment_policy.include_only` (list editor)

4) **Rules**
- Show generated rules preview (read-only)  
- Allow adding new `prefix_rule` templates via small form:
  - command prefix tokens
  - decision: allow/prompt/forbidden
  - justification
  - match/not_match examples

5) **Skills**
- Multi-select from MACC skills catalog **and remote Git/HTTP sources** (Fetch Unit + Selection model)
- Show skill descriptions, source (catalog vs URL), and whether installed at project vs user
- Preserve cache across different subpath selections (download once, install selected)

6) **MCP servers**
- Select from catalog **or remote Git/HTTP sources** (same Fetch Unit + Selection model)
- Show env placeholders only; never request or store secrets
- Indicate target scope: project `.codex/config.toml` vs optional user merge (consent-gated)

7) **Preview & apply**
- File tree diff/summary for:
  - `.codex/` and `AGENTS.md`
  - optional user-layer changes under `~/.codex/`

8) **Configure Codex via MACC TUI**
  - If you use MACC, you can edit Codex CLI settings from the TUI. These map to `.codex/config.toml`:

    * **Model** → `model`
    * **Reasoning effort** → `model_reasoning_effort`
    * **Approval policy** → `approval_policy`
    * **Sandbox mode** → `sandbox_mode`
    * **Shell env include list** → `shell_environment_policy.include_only` (comma-separated)
    * **Features** → `features.exec_policy`, `features.undo`, `features.web_search_request`, `features.shell_snapshot`
    * **Deep review profile** → `profiles.deep-review.model`, `profiles.deep-review.model_reasoning_effort`, `profiles.deep-review.approval_policy`

  - Edit these fields under the Codex tool settings screen; MACC writes them into `.codex/config.toml` on apply.

### 7.3 TUI configuration view (Codex) — concrete fields & behaviors
- **Top-level toggles**: enable/disable Codex tool; scope toggles for project vs user merge (user merge always consent-gated).
- **Model/profile panel**: `model` text input; `profile` picker (including "none"); default profile checkbox; show derived values in preview.
- **Safety panel**: dropdowns for `approval_policy`, `sandbox_mode`, `model_reasoning_effort`; feature toggles (`exec_policy`, `undo`, `web_search_request`, `shell_snapshot`, etc.) with inline help.
- **Rules panel**: list preview of generated `.codex/rules/*.rules`; “Add rule” mini-form (prefix, action allow/prompt/deny, note); edits persist into working config, not filesystem.
- **Skills panel**: table columns {id, source (catalog/git/http), subpath, scope}; multi-select; supports adding a remote source URL → parsed into Fetch Unit + optional subpath; cache is keyed without subpath so download-once/install-selected is honored.
- **MCP panel**: list catalog + “Add remote” (git/http) with subpath; shows env placeholders only; scope indicator (project vs user merge if enabled).
- **Preview pane**: summarizes planned writes/merges; highlights user-scope ops requiring consent; supports refresh; uses same deterministic plan as CLI.

### 7.2 UX constraints
- Keyboard-first navigation
- Clear “scope” markers: project vs user
- “Apply” always shows backups and asks consent for user-layer writes

---

## 8) Worktrees & parallelism

### 8.1 Worktree creation + apply
When `macc worktree create ... --tool codex`:
- create the worktree folder as per MACC conventions
- run `macc apply --cwd <worktree> --tools codex`
- write `.macc/worktree.json` including tool = codex

### 8.2 Launching Codex per worktree
`macc worktree run <id>`:
- cd into worktree folder
- run `codex` (interactive) by default
- optionally support `--exec` to run `codex exec` for headless tasks

**Important:** rely on Codex Team Config discovery from `$CWD/.codex` (the worktree), so each worktree can have isolated settings if desired.

---

## 9) Security & compliance

### 9.1 No secrets in git
- MACC must never write API keys or tokens into `.codex/config.toml` in a repo.
- MCP servers and telemetry headers must use placeholders or environment variable references.
- Add robust `.gitignore` entries where appropriate.

### 9.2 Requirements / managed environments
Codex supports `requirements.toml` constraints enforced by admins. MACC should:
- detect if a `requirements.toml` exists in higher-precedence locations (best-effort)
- avoid generating configs that conflict with constraints
- surface a clear warning in the TUI if the user selects disallowed values

### 9.3 Safe defaults
Default v1 settings recommendation:
- `approval_policy = "on-request"` (human-in-the-loop for command exec)
- `sandbox_mode = "workspace-write"`
- `features.exec_policy = true`
- include a conservative `shell_environment_policy.include_only`

---

## 10) Testing plan

### 10.1 Unit tests (Rust)
- TOML generation snapshot tests for `.codex/config.toml`
- Skills generation tests:
  - valid YAML front matter
  - name/description length constraints
- Rules generation tests:
  - ensure file compiles syntactically (basic parser heuristics, plus optional integration test)

### 10.2 Integration tests (optional, if `codex` available in CI)
- `codex --version`
- `codex execpolicy check` on generated rules
- Launch `codex` in a temporary directory with generated `.codex/` and verify no errors

### 10.3 Golden repo tests
Maintain a minimal fixture repo and run:
- `macc init`
- `macc apply --tools codex`
- verify produced tree matches expected fixtures

---

## 11) Documentation deliverables

1. `docs/codex.md`
   - how MACC maps canonical config -> `.codex/config.toml`
   - how to use profiles and `--config` overrides
   - how skills are structured and invoked
   - how rules are structured + examples
   - troubleshooting

2. `docs/worktrees.md` update
   - running Codex per worktree
   - isolating config when needed

3. Examples:
   - Next.js + pnpm workflow with validate/implement skills
   - Minimal repo with `AGENTS.md` + `.codex/`

---

## 12) Milestones (recommended)

### M0 — Foundations (Codex adapter skeleton)
- Add `tools.codex` schema to `.macc/macc.yaml`
- Implement `.codex/config.toml` generation (root keys only)
- Generate `AGENTS.md` from MACC standards

**Done when:**
- `macc apply --tools codex` writes `.codex/config.toml` + `AGENTS.md`

### M1 — Skills + minimal TUI support
- Implement `.codex/skills/**/SKILL.md` generation for at least 2 skills: `validate`, `implement`
- TUI: select Codex + skills, preview/apply

**Done when:**
- Codex skills appear in `.codex/skills/` and are selectable in TUI

### M2 — Rules + permissions model
- Implement `.codex/rules/default.rules`
- Optional `codex execpolicy check` validation
- TUI: rules preview + add rule template

**Done when:**
- rules are generated, readable, and validated (if codex exists)

### M3 — User-layer merges (backup + consent)
- Implement merging for `~/.codex/config.toml` (or `$CODEX_HOME/config.toml`)
- Backups + diff summary in TUI
- Opt-in flags: `--user`, `--yes`

**Done when:**
- user-layer merge works safely and predictably

### M4 — Worktrees + run wrappers
- `macc worktree run` supports Codex
- `macc tool codex run` wrapper with `--profile` and `--config` passthrough

**Done when:**
- Codex can be launched cleanly inside each worktree and respects config

---

## 13) Acceptance criteria (v1, Codex)
- `macc init` creates baseline `.macc/` config enabling Codex integration.
- `macc apply --tools codex` produces:
  - `.codex/config.toml`
  - `.codex/skills/validate/SKILL.md`
  - `.codex/skills/implement/SKILL.md`
  - `.codex/rules/default.rules` (or equivalent)
  - `AGENTS.md`
- TUI lets the user:
  - enable Codex
  - set model, approval_policy, sandbox_mode, reasoning effort
  - toggle features
  - select skills
  - preview/apply and see backups/consent prompts
- No secrets are written into versioned files.
- Optional user-layer merges always create backups and require explicit consent.
- Worktree flows:
  - `macc worktree create <slug> --tool codex --count 1`
  - `macc worktree run <id>` launches Codex using worktree `.codex/` Team Config.

---

## Appendix A — Example generated files

### A.1 `.codex/config.toml` (example)
```toml
model = "gpt-5.2"
approval_policy = "on-request"
sandbox_mode = "workspace-write"
model_reasoning_effort = "high"

[shell_environment_policy]
include_only = ["PATH", "HOME"]

[features]
exec_policy = true
undo = true
web_search_request = false
shell_snapshot = false

[profiles.deep-review]
model = "gpt-5-pro"
model_reasoning_effort = "high"
approval_policy = "never"
```

### A.2 `.codex/skills/validate/SKILL.md` (example)
```md
---
name: validate
description: Run the project validation workflow when the user asks to validate, run tests, or verify changes.
---

Run:
1) `pnpm lint`
2) `pnpm build`
3) `pnpm test:e2e`

If any step fails:
- report the first failing command and its output summary
- propose the smallest fix and rerun only the failing step

Never skip validation steps unless the user explicitly requests it.
```

### A.3 `.codex/rules/default.rules` (sketch)
```python
# Allow read-only git commands outside sandbox
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
```

---

## Appendix B — Open questions (track for v1+)
- Should MACC support generating/merging `requirements.toml` for managed environments, or treat it as read-only? => YES
- Do we want a “Codex profile per worktree” convention (e.g., `profile = "<worktree-id>"`)?  => YES
- Should MACC add a “codex exec” mode for `ralph` automation, emitting `--json` events for parsing? => YES
