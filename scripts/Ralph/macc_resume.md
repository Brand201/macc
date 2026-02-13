# MACC — Summary of `macc.md` (English)

> **MACC (Multi-Assistant Code Config)** is a project to **unify and automate configuration** for multiple AI developer tools (Claude Code, OpenAI Codex, Gemini Code Assist, etc.) so you get a **consistent development experience across machines and projects**.

---

## 1) Purpose and goals

### The problem
AI coding tools each have their own configuration formats and mechanisms (instructions, skills, agents, MCP, permissions). This leads to:
- inconsistent behavior across machines,
- duplicated conventions and workflows,
- friction to bootstrap projects and onboard contributors,
- difficulty running multiple AI sessions in parallel (sessions/worktrees).

### Goals
MACC should provide:
1) **Fast installation** on Linux and Windows, plus **easy project initialization**.  
2) A **single source of truth** for standards/workflows, then **tool-specific generation** via adapters.  
3) A **TUI (text UI)** to select tools, standards, skills, agents, MCP servers, and worktrees (mostly v0.2+).  
4) Built-in support for **parallel work** using **git worktrees** and automation scripts (e.g., “ralph”).  
5) **Safe user-level merges** (backup + explicit consent).

### Non-goals (v1)
- Re-implement the AI tools themselves (MACC configures them).
- Store/sync **secrets** in Git (API keys/tokens).
- Become a full CI/CD orchestrator.

---

## 2) Scope: supported tools (v1)

### Primary targets
- **Claude Code**
- **OpenAI Codex**
- **Gemini Code Assist**
- Extensible later (Copilot, Cursor, Windsurf, etc.)

### Two configuration levels
- **Project-level (versionable)**: committed files in the repo for reproducible behavior.
- **User-level (machine-local)**: plugins/MCP/global permissions; requires **backup + consent** before changes.

---

## 3) User experience (UX)

### Installation (machine)
- Linux/macOS: `install.sh`
- Windows: `install.ps1`
- Installs `macc` on the PATH.
- May optionally detect/install tool CLIs and manage user-level config (future/optional).

### Project bootstrap
- `macc init`: creates `.macc/` and the base canonical config.
- `macc` (no args): opens the **TUI** (primarily v0.2).
- `macc apply`: generates and installs per-tool project configuration.

### TUI screens (minimum set)
1) Tool selection (Claude/Codex/Gemini/…)  
2) Standards presets / conventions  
3) Skills catalog and selection  
4) Agents selection (where supported)  
5) Plugins (user-level)  
6) MCP servers (optional)  
7) Worktrees (create/scopes/run)  
8) Preview & Apply (what files will be written + backups + consent)

---

## 4) Coding standards as the “source of truth”

### Canonical file
- `config/standards.md` (or similar)

### Rendered outputs per tool
- Claude: `CLAUDE.md`
- Codex: `AGENTS.md`
- Gemini: `.gemini/styleguide.md`

### Example standards included
- Use **pnpm only** (no npm/yarn)
- Code/commits/docs in **English**
- TypeScript **strict**, avoid `any`
- Absolute imports via `@/`
- Functional/declarative style (avoid classes)
- Naming conventions (kebab-case folders, camelCase functions, PascalCase components)
- Next.js: prefer Server Components, minimize `'use client'`
- Prefer Zustand over Context for global state
- Prefer Server Actions over API Routes
- Tailwind + shadcn/ui
- Performance: Web Vitals, WebP, lazy loading
- No barrel imports, no async waterfalls (`Promise.all`), dedup with `React.cache()`

---

## 5) Adapter-based generation architecture

### Canonical MACC config
A project-level config (e.g., `.macc/macc.yaml`) defines:
- enabled tools,
- standards,
- selected skills and agents,
- selected MCP servers,
- user-level merge policies,
- worktree settings (folder, scopes, etc.).

### Project outputs (generated)
**Claude**
- `CLAUDE.md`
- `.claude/settings.json`
- `.claude/skills/**/SKILL.md`
- `.claude/agents/**.md`
- optional `.mcp.json`

**Codex**
- `AGENTS.md`
- `.codex/config.toml`
- `.codex/skills/**/SKILL.md`

**Gemini**
- `GEMINI.md`
- `.geminiignore`
- `.gemini/settings.json`
- `.gemini/skills/**/SKILL.md`
- `.gemini/commands/*.toml`
- `.gemini/styleguide.md`

---

## 6) `macc apply` pipeline (expected behavior)

1) Load `.macc/macc.yaml` (+ presets)  
2) Resolve selections (tools, skills, agents, MCP, plugins)  
3) Generate tool-specific files via adapters  
4) Write outputs with **safe backups** + **atomic writes**  
5) Optionally merge user-level config (only with **explicit consent**)

### Backups & consent (user-level)
Any changes to machine-local paths (e.g., `~/.claude/*`, `~/.codex/*`, `~/.gemini/*`) must:
- create a timestamped backup,
- show a diff/summary when possible,
- require confirmation (ideally via TUI) before writing.

---

## 7) “BMAD Lite” workflow (product → implementation)

A lightweight sequence:
- `/brainstorm` → `/prd` → `/tech-stack` → `/implementation-plan` → `/implement`

Artifacts are stored in versioned docs, e.g.:
- `memory-bank/`
- `memory-bank/features/<feature>/...`

---

## 8) Skills (multi-tool)

### Principles
- Skills are installable **per tool** (at least Claude and Codex in v0.1).
- Skills can support triggers (FR/EN) when the tool supports auto-discovery.
- Support a `--feature=<name>` flag to scope docs to `memory-bank/features/<name>/`.

### Core dev skills
- `/validate`: `pnpm lint` → `pnpm build` → `pnpm test:e2e`
- `/implement`: read docs → plan → code → validate → review → commit
- Plus helpers like `/next-task`, `/refresh-context`, `/update-progress`, `/git-add-commit-push`, `/validate-update-push`.

### Utility skills
Examples: `/db-check`, `/security-check`, `/seo-check`, `/permissions-allow`, `/design-principles`, `/validate-quick`, `/sync-config`.

---

## 9) Custom agents

Two main groups:
- **BMAD Lite (product discovery)**: `analyst`, `product-manager`, `architect` (often a stronger model for architecture).
- **Development agents**: `code-reviewer`, `nextjs-developer`, `supabase-developer`, `prompt-engineer`, `seo-specialist`.

Default rule: **inherit session model**, except for specific agents (e.g., `architect`) that may force a stronger model.

---

## 10) Plugins (user-level only)

Plugins extend tool capabilities (examples): semantic search (`mgrep`), UI generation, automated code review, simplification, TypeScript LSP, security guidance, up-to-date docs (Context7). Managed at user-level with warnings for conflicts/versions.

---

## 11) MCP servers (optional)

- Templates are stored **without secrets**.
- Activation can be project-level (`.mcp.json`) or user-level (merge with consent).
- API keys are added manually after install and must never be committed.

---

## 12) Automation scripts (“ralph”)

A loop script such as `scripts/ralph.sh <n>` can run a standardized autonomous iteration:
`/next-task` → `/implement` → `/validate` → `/update-progress` → `/git-add-commit-push`

Requires `memory-bank/` and a progress tracker.

---

## 13) Worktrees & parallel execution

### Core idea
Each worktree is an isolated **directory + branch**, enabling:
- multiple parallel sessions of the same tool (e.g., 3 Claude sessions),
- multiple different tools in parallel (Claude + Codex + Gemini).

### Conventions
- Worktree directory: `.worktrees/` (default) or `worktrees/`
- Branch naming: `ai/<tool>/<slug>-<NN>`

### Suggested commands
- `macc worktree create <slug> --tool <tool> --count N --base main --scope "glob,glob" --feature X`
- `macc worktree list`
- `macc worktree apply <id>|--all`
- `macc worktree prune`
- (optional QoL) `open`, `run`, `exec`, `status`, `doctor`, `merge`, `remove`

### Expected `create` flow
1) Compute IDs/branches/paths  
2) Run `git worktree add -b ...`  
3) Write metadata inside worktree:
   - `.macc/worktree.json`
   - `.macc/scope.md`
   - `.macc/selections.lock.json`
4) Apply config to the worktree:
   - `macc apply --cwd <worktree> --tools <tool>`

### Anti-conflict rule
**One worktree = one scope** (feature/area/task-type). Optional global locks for shared files (lockfile, migrations).

---

## 14) MVP acceptance criteria (v0.1)

### Installation
- Linux: `install.sh`
- Windows: `install.ps1`
- `macc --help` and `macc --version`

### Apply / project generation
- `macc init` creates `.macc/` + minimal config
- `macc apply` generates:
  - Claude: `CLAUDE.md` + `.claude/settings.json` + **at least 2 skills** (validate/implement)
  - Codex: `AGENTS.md` + `.codex/config.toml` + equivalent skills
  - Gemini: `.gemini/settings.json` + `.gemini/styleguide.md`

### Worktrees
- `macc worktree create <slug> --tool claude --count 2` creates 2 worktrees, applies config, writes `.macc/worktree.json`
- `macc worktree list`
- `macc worktree prune` removes merged/deleted worktrees

### Security
- No API keys written to the repo
- User-level changes require backup + consent

---

## 15) Suggested roadmap
- **v0.1**: init/apply + standards + 2 core skills + worktree create/list/apply/prune
- **v0.2**: full TUI + ralph + MCP templates + safe user-level merge
- **v0.3+**: presets marketplace, more tools (Copilot), richer automation
