# MACC × Claude Code : Implementation Plan

**Document purpose:** Implement a *fully configurable* Claude Code integration in **MACC** (Multi‑Assistant Code Config), based on the `claude.md` documentation and the `macc.md` project spec.

**Target outcome:** From a single MACC “source of truth” (`.macc/macc.yaml`), MACC can:
- generate all Claude Code *project* artifacts (`CLAUDE.md`, `.claude/settings.json`, skills, agents, rules, optional `.mcp.json`),
- optionally (with explicit user consent) manage *user* artifacts (`~/.claude/settings.json`, `~/.claude/agents/`, `~/.claude/CLAUDE.md`, `~/.claude.json` for MCP),
- support worktrees/parallel sessions in a predictable and safe way,
- expose the integration through CLI + TUI, including preview/diff and rollback.

### What uses scopes

Scopes apply to many Claude Code features:

| Feature         | User location                   | Project location                   | Local location                 |
| :-------------- | :-------------------------------| :--------------------------------- | :----------------------------- |
| **Settings**    | `~/.claude/settings.json`       | `.claude/settings.json`            | `.claude/settings.local.json`  |
| **Subagents**   | `~/.claude/agents/*.md`         | `.claude/agents/*.md`              | —                              |
| **MCP servers** | `~/.claude.json`                | `.mcp.json`                        | `~/.claude.json` (per-project) |
| **Plugins**     | `~/.claude/settings.json`       | `.claude/settings.json`            | `.claude/settings.local.json`  |
| **CLAUDE.md**   | `~/.claude/CLAUDE.md`           | `CLAUDE.md` or`.claude/CLAUDE.md`                | `CLAUDE.local.md`              |
| **Skills**      | `~/.claude/skills/**/SKILL.md`  | `.claude/skills/**/SKILL.md`       | —                              |

---

## 1) Ground truths from Claude Code and MACC (what we must respect)

### 1.1 Claude Code configuration scopes and precedence
Claude Code supports **Managed**, **User**, **Project**, and **Local** scopes. Higher precedence overrides lower precedence:
1. Managed (highest, system-level)
2. CLI arguments (session-only overrides)
3. Local (`.claude/settings.local.json`)
4. Project (`.claude/settings.json`)
5. User (`~/.claude/settings.json`) (lowest)

**Implication for MACC:** MACC should generate *project scope* by default, and apply *local* / *user* scope only with explicit consent and backups.

### 1.2 Where files live (must be generated/managed correctly)
**Memory / instructions**
- User: `~/.claude/CLAUDE.md`
- Project: `CLAUDE.md` or `.claude/CLAUDE.md`
- Local project: `CLAUDE.local.md`
- Modular project rules: `.claude/rules/*.md` (optionally path-scoped via YAML frontmatter)

**Settings**
- User: `~/.claude/settings.json`
- Project: `.claude/settings.json`
- Local: `.claude/settings.local.json` (gitignored)

**Agents (subagents)**
- User: `~/.claude/agents/*.md`
- Project: `.claude/agents/*.md`

**MCP servers**
- User & per-project: `~/.claude.json`
- Project: `.mcp.json`

### 1.3 MACC constraints that shape the design
From `macc.md`:
- **No secrets in repo** (API keys entered manually after).
- **Plugins enabled only at user level** (project may suggest marketplaces/plugins, but “enable/disable” is done user-side).
- **Backups + consent** for any user-level modification.
- Rust implementation, Ratatui TUI, safe atomic writes.
- `macc apply` must generate at least: `CLAUDE.md` + `.claude/settings.json` + **2 skills** (validate/implement).

---

## 2) High-level architecture

### 2.1 Components
1. **Canonical Config Loader**
   - Loads `.macc/macc.yaml` + presets
   - Produces an internal “resolved” configuration (tool selections, standards, skills, agents, MCP, permissions, etc.)

2. **Claude Adapter (Generator)**
   - Renders resolved config into:
     - `CLAUDE.md` (project memory)
     - `.claude/settings.json` (project settings)
     - `.claude/skills/**/SKILL.md` (MACC skills translated to Claude format)
     - `.claude/agents/**.md` (agents as Claude subagents)
     - `.claude/rules/*.md` (optional modular rules)
     - `.mcp.json` (optional, no secrets)

3. **User-level Manager (Consent‑gated)**
   - Reads and merges:
     - `~/.claude/settings.json` (plugins, user defaults, permissions)
     - `~/.claude/agents/*.md` (user agent library)
     - `~/.claude/CLAUDE.md` (global personal memory, optional)
     - `~/.claude.json` (MCP user/per-project entries)
   - Writes only after backup + diff + explicit consent.

4. **Worktree Support**
   - Applies per-worktree generation via `macc worktree apply`
   - Uses `CLAUDE.local.md` and/or imports for worktree-specific tweaks
   - Ensures “one worktree = one scope” policy and optional file locks

5. **TUI + CLI Exposure**
   - TUI screens for: tools, standards, skills, agents, plugins, MCP, worktrees, preview/apply
   - CLI subcommands to:
     - apply Claude config
     - run Claude (interactive) in a project/worktree
     - run Claude in `-p` mode (structured outputs) for automation scripts

---

## 3) Extend MACC’s canonical schema for Claude Code

### 3.1 Proposed YAML structure (`.macc/macc.yaml` excerpt)
```yaml
tools:
  claude:
    enabled: true
    project:
      # Generates CLAUDE.md and .claude/settings.json (+ rules/skills/agents)
      memory:
        imports:                  # optional @imports inside CLAUDE.md
          - "README.md"
          - "docs/architecture.md"
      rules:
        enabled: true
        files:
          - name: "code-style"
            paths: ["src/**/*.ts", "src/**/*.tsx"]
          - name: "testing"
            paths: ["**/*.test.ts", "**/*.spec.ts"]
      settings:
        language: "English"
        model: "opus"             # "sonnet" | "opus" | full model name
        env: {}                   # environment variables (no secrets)
        permissions:
          allow: []               # permission rules
          ask: []
          deny: []
      skills:
        enabled: true
        selection: ["implement", "validate"]
      agents:
        enabled: true
        selection: ["architect", "reviewer"]
      mcp:
        project_file: true        # generate .mcp.json (no secrets)
        selection: ["context7", "brave-search"]
      sandbox: {}                           # if used
      hooks: {}                             # settings.hooks
      fileSuggestion: null                  # command hook
      attribution: { commit: "", pr: "" }
      outputStyle: null
      extraKnownMarketplaces: {}            # marketplaces to offer 
      enabledPlugins: {}                    # if you allow project-level
    user:
      plugins:
        enabled: true             # user-level only, consent-gated
        marketplaces:
          add: ["anthropics/claude-code"]  # example marketplace repo
        enable:
          - "typescript-lsp@claude-plugins-official"
          - "context7@claude-plugins-official"
      merge_policy:
        mode: "consent"           # "never" | "consent" | "always" (MVP: "consent")
```

### 3.2 Mapping table (MACC → Claude)
| MACC concept | Claude Code artifact | Notes |
|---|---|---|
| Global standards | `CLAUDE.md` + `.claude/rules/*.md` | Prefer `CLAUDE.md` for “global + project overview”, rules for modular topics |
| Permissions | `.claude/settings.json` `permissions.allow/deny` | Deny secrets and dangerous commands by default |
| Skills catalog | `.claude/skills/**/SKILL.md` | Ensure at least `/validate` and `/implement` exist for MVP |
| Agents catalog | `.claude/agents/*.md` | YAML frontmatter + prompt, optionally tool restrictions |
| MCP templates | `.mcp.json` and/or `~/.claude.json` merge | Never commit API keys |
| Plugins | user-level `~/.claude/settings.json` `enabledPlugins` | MACC can suggest marketplaces in project settings, but enable plugins user-side only |

---

## 4) Claude Adapter: project generation (core deliverable)

### 4.1 File tree generated by `macc apply --tools claude`
```
<repo>/
├─ CLAUDE.md
├─ .claude/
│  ├─ settings.json
│  ├─ skills/
│  │  ├─ implement/SKILL.md
│  │  └─ validate/SKILL.md
│  ├─ agents/
│  │  ├─ architect.md
│  │  └─ reviewer.md
│  └─ rules/
│     ├─ code-style.md
│     └─ testing.md
└─ .mcp.json                       # optional, no secrets
```

### 4.2 Generate `CLAUDE.md`
**Responsibilities**
- Project overview (architecture, stack, commands)
- Enforce MACC standards in natural-language format
- Include common commands (build/test/lint) to reduce repeated questions
- Optionally include imports via `@path/to/file` lines (supports `@~/.claude/...` for personal add-ons)

**Implementation notes**
- If imports are configured, write them as plain lines (not in code blocks) so Claude evaluates them.
- Also generate/ensure `CLAUDE.local.md` is gitignored when requested by worktree logic.

### 4.3 Generate `.claude/settings.json`
**Responsibilities**
- Team-shared settings:
  - `language`, `model` (optional), `env` (no secrets)
  - `permissions.allow/deny`
  - optional `hooks` (MVP: omit or keep minimal)
  - optional plugin marketplaces (`extraKnownMarketplaces`) **only as suggestions**

**Security baseline (recommended default)**
- Deny reading secrets (`.env`, `.env.*`, `secrets/**`, credentials files)
- Deny dangerous commands by default (`rm -rf`, `curl|sh`, etc.)
- Allow common safe workflows (`pnpm`, `git status/diff/log`, tests) as *prompted or allowed* depending on MACC policy

### 4.4 Generate skills (`.claude/skills/**/SKILL.md`)

**Design requirements**
- Each SKILL is a reproducible operational workflow:
  - inputs (what user provides)
  - steps (what Claude does)
  - required tools/commands
  - completion criteria
- Skills should be deterministic, short, and aligned to MACC standards (e.g., pnpm, TS strict).

> Note: Claude Code skills are invoked via slash commands in interactive mode; MACC also supports non-interactive (`claude -p`) automation separately.

### 4.5 Generate agents (`.claude/agents/*.md`)
**Requirements**
- One file per agent (Markdown + YAML frontmatter)
- Provide at least: `architect`, `reviewer` for v0.1/v0.2 (configurable)
- Each agent should define:
  - `description`
  - core prompt (persona + constraints)
  - optional tool restrictions (if you choose to enforce in the agent definition)

### 4.6 Generate `.claude/rules/*.md` (optional but recommended)
**Purpose**
- Keep rules modular and path-specific when useful (YAML frontmatter `paths:`)
- Better maintainability than a single giant `CLAUDE.md`

**MVP suggestion**
- `code-style.md` (global or scoped)
- `testing.md` (scoped to test files)

### 4.7 Generate `.mcp.json` (optional, no secrets)
**Responsibilities**
- Provide MCP server entries with placeholders (URLs, commands, env var names)
- Never write API keys; only mention required environment variables.

---

## 5) User-level manager (consent-gated)

### 5.1 Goals
- Make Claude Code “fully usable” from MACC on a fresh machine by optionally:
  - adding marketplaces
  - enabling/disabling plugins
  - merging MCP servers
  - installing user agents/templates
- But do so **safely**: backup + diff + explicit consent.

### 5.2 `~/.claude/settings.json` merge rules
- Parse as JSON; preserve unknown keys
- Merge strategy:
  - objects: deep-merge
  - arrays: union or replace depending on field (configurable policy)
- Never remove user entries unless explicitly requested.

**Plugin handling**
- MACC can add to `enabledPlugins` (true/false)
- MACC can add to `extraKnownMarketplaces`
- If a managed policy restricts marketplaces (via `strictKnownMarketplaces`), MACC should detect errors and warn.

### 5.3 Marketplace and plugin lifecycle (how MACC drives Claude)
Two operational modes:
1. **Direct file merge only** (offline): adjust `~/.claude/settings.json` and let Claude load it
2. **CLI-driven** (preferred): use Claude’s plugin commands for correctness:
   - `claude plugin install <name>@<marketplace> --scope user`
   - `claude plugin enable <name>@<marketplace> --scope user`
   - `claude plugin disable ...`
   - `claude plugin uninstall ...`
   - `claude plugin marketplace add <source>` / `list` / `update` / `remove`

**MACC policy:** For MVP, implement *file merge* and provide CLI helper commands; add full CLI-driven plugin management in v0.3.

### 5.4 MCP merge (`~/.claude.json`)
- Preserve existing MCP servers
- Add missing servers from MACC template (no secrets)
- Support per-project MCP entries in `~/.claude.json` (Claude supports per-project entries there)
- Provide an option to generate `.mcp.json` instead/in addition (project scope)

### 5.5 User memory and agents
Optional:
- write/update `~/.claude/CLAUDE.md` (personal standards) if user opts in
- install user-level agents into `~/.claude/agents/`

MVP: do not modify user memory by default; focus on project generation first.

---

## 6) Worktrees & parallelism (Claude-focused design)

### 6.1 Worktree creation flow (from MACC spec)
`macc worktree create <slug> --tool claude --count N ...` should:
- create git worktrees under `.worktrees/` (or configured folder)
- add `.macc/worktree.json` metadata
- apply Claude config in each worktree (`macc apply --cwd <worktree> --tools claude`)

### 6.2 Avoiding conflicts across worktrees
- Prefer personal worktree tweaks in `CLAUDE.local.md` (auto-gitignored)
- Or use imports in root `CLAUDE.md` pointing to `@~/.claude/<project>-instructions.md` so it works across worktrees.
- Optionally generate worktree-scoped `.claude/settings.local.json` for per-worktree overrides (gitignored).

### 6.3 Launching Claude per worktree
Add convenience command:
- `macc claude run [--worktree <id>]` → runs `claude` with `cwd` set to worktree folder
- `macc claude print -p ...` wrapper for automation and scripts (see next section)

---

## 7) Automation and “headless” usage (Agent SDK via CLI)

Claude Code supports a print mode (`-p/--print`) plus structured outputs (`--output-format json`, `--json-schema`) and session continuation (`--continue`, `--resume`).

### 7.1 MACC CLI wrappers (recommended)
- `macc claude -p "<prompt>"` should:
  - run from repo/worktree directory
  - optionally pass `--setting-sources user,project` (or configured)
  - optionally pass `--allowedTools ...` in automation scripts
  - optionally pass `--mcp-config .mcp.json` (or generated location) and `--strict-mcp-config` for CI

### 7.2 ralph.sh compatibility (from MACC spec)
For automation loops, prefer:
- deterministic prompts
- limited `--max-turns`
- structured JSON outputs when integrating with scripts

---

## 8) TUI integration (Ratatui)

### 8.1 Required screens (Claude-related)
1. **Claude settings**
   - model alias (`sonnet`/`opus`/full)
   - language
   - permissions (select presets: “safe”, “dev”, “strict”)
2. **Skills**
   - choose skills to generate
3. **Agents**
   - choose agents to generate
4. **Plugins (user-level)**
   - show catalog, selected marketplaces/plugins (informational in MVP)
5. **MCP**
   - pick servers, show placeholders, warn about secrets
6. **Preview & Apply**
   - list files to be written (project + optional user)
   - show diffs where feasible
   - confirm consent prompts for user-level changes

### 8.2 Apply UX rules
- Never write user-level files without explicit confirmation
- Always create timestamped backups
- Always perform atomic writes (temp file then rename)

---

## 9) Implementation details (Rust)

### 9.1 Modules (suggested)
- `core/config` — parse `.macc/macc.yaml`, presets, compute resolved config
- `adapters/claude` — rendering logic for all Claude artifacts
- `io/atomic_write` — safe writes, backup manager, diff generator
- `integrations/claude_cli` — helper to run `claude`, detect version, check availability
- `merge/json_merge` — merge `settings.json` and `~/.claude.json` safely
- `tui/*` — screens, state machine, apply preview

### 9.2 Atomic write and backup strategy
- Backups stored under:
  - project: `.macc/backups/<timestamp>/...`
  - user: `~/.macc/backups/<timestamp>/...`
- Use “replace-only-if-changed” to avoid noisy file churn
- When writing `.claude/settings.local.json` or `CLAUDE.local.md`, ensure gitignore entries exist (MACC should add them safely)

### 9.3 Diff strategy (pragmatic)
- For text files: show unified diff
- For JSON: pretty-print then diff
- For directories: show file list + per-file diff summary

---

## 10) Testing strategy

### 10.1 Unit tests
- YAML parsing and schema validation
- Template rendering for:
  - `CLAUDE.md`
  - `.claude/settings.json`
  - skills, agents, rules
- JSON merge correctness (idempotent, stable ordering)

### 10.2 Integration tests (filesystem)
- `macc init` creates `.macc/`
- `macc apply --tools claude` creates all expected files
- Re-running apply is idempotent (same output, no extra changes)
- Backup + restore path correctness

### 10.3 “Golden” fixtures
- Fixture repos with expected outputs (snapshot testing)
- Worktree fixture to ensure per-worktree files are correct and isolated

---

## 11) Milestones (aligned with MACC roadmap)

### v0.1 (MVP)
- Claude adapter generates:
  - `CLAUDE.md`
  - `.claude/settings.json`
  - 2 skills (`implement`, `validate`)
- Worktree create/list/apply supports `--tool claude`
- No user-level writes by default; only project generation

### v0.2
- Full TUI flow for Claude selections + preview/apply
- Consent-gated user-level merge framework (but minimal use)
- MCP template generation in `.mcp.json`

### v0.3
- Claude plugin packaging support:
  - create a MACC marketplace repo
  - generate plugin skeletons for distributing skills/agents/hooks/MCP
- CLI-driven plugin management wrappers

### v1.0
- Stability, docs, presets marketplace, richer merge policies, cross-tool parity

---

## 12) Appendix — Minimal templates (copy/paste friendly)

### 12.1 `.claude/settings.json` (baseline example, no secrets)
```json
{
  "language": "English",
  "permissions": {
    "deny": [
      "Read(./.env)",
      "Read(./.env.*)",
      "Read(./secrets/**)"
    ],
    "allow": [
      "Bash(pnpm:*)",
      "Bash(git status:*)",
      "Bash(git diff:*)",
      "Bash(git log:*)"
    ]
  }
}
```

### 12.2 `CLAUDE.md` (baseline shape)
```md
# Project Instructions (MACC)

## Standards
- Use pnpm (never npm/yarn).
- TypeScript strict mode, avoid `any`.
- Prefer functional code (no classes).
- Write code/docs/commits in English.

## Common commands
- Install: `pnpm i`
- Lint: `pnpm lint`
- Build: `pnpm build`
- Test: `pnpm test`

## Context imports
- @README.md
- @docs/architecture.md
```

### 12.3 Skill template (`.claude/skills/validate/SKILL.md`)
```md
# /validate

## Goal
Run the project validation pipeline and report/fix failures.

## Steps
1) Run `pnpm lint`.
2) Run `pnpm build`.
3) Run unit tests (`pnpm test`) and report failures.
4) If failures exist: apply minimal fixes, re-run relevant steps.

## Done when
- All steps pass, or remaining failures are clearly explained with next actions.
```

### 12.4 Agent template (`.claude/agents/reviewer.md`)
```md
---
name: reviewer
description: Reviews code changes for correctness, security, and maintainability.
model: inherit
---

You are a meticulous code reviewer.
- Identify correctness issues, edge cases, and risky changes.
- Flag security pitfalls (secrets, injection, auth).
- Prefer small, actionable suggestions.
- Follow project standards from CLAUDE.md and rules.
```

---

## 13) Definition of “fully configurable and usable by MACC” (acceptance checklist)

- [ ] `macc apply --tools claude` produces all project artifacts deterministically
- [ ] All key Claude settings are configurable from `.macc/macc.yaml` (model, language, permissions, env, skills, agents, rules, MCP template)
- [ ] Worktrees can be created and configured reliably (`macc worktree create ... --tool claude`)
- [ ] No secrets are committed; placeholders only
- [ ] User-level modifications are always backup + diff + explicit consent
- [ ] The TUI allows selecting Claude components and previewing changes before apply
- [ ] The Claude integration can run in interactive mode and print (`-p`) mode via MACC wrappers

