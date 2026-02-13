# MACC (Multi-Assistant Code Config)

MACC is a tool-agnostic configuration manager for AI coding assistants. It keeps one canonical project config (`.macc/macc.yaml`) and generates tool-specific files (Claude, Codex, Gemini, etc.) through adapters.

## What MACC provides

- Canonical config and deterministic generation (`plan` then `apply`).
- Tool-agnostic TUI for tool selection, tool settings, skills, MCP, and automation coordinator settings.
- Embedded defaults for ToolSpecs and catalogs so clean machines are usable immediately.
- Project automation with embedded `coordinator.sh` + `performer.sh` + per-tool runners.
- Worktree orchestration for parallel task execution.
- Safe cleanup (`macc clear`) with confirmation: removes worktrees first, then MACC-managed project artifacts.

## Installation

### Quick install (recommended)

```bash
./scripts/install.sh
```

Options:

- `--release`: build optimized binary.
- `--prefix <dir>`: install `macc` into a custom directory.
- `--system`: install to `/usr/local/bin` (uses `sudo`).
- `--no-path`: do not update shell profile `PATH`.

### Uninstall

```bash
./scripts/uninstall.sh
```

Options:

- `--system`: remove `/usr/local/bin/macc`.
- `--prefix <dir>`: remove `<dir>/macc`.
- `--clean-profile`: remove installer-added `PATH` lines from shell profiles.

## Quick start

1. Initialize a project:

```bash
macc init
```

2. Open the TUI:

```bash
macc tui
```

3. Preview and apply changes:

```bash
macc plan
macc apply
```

4. If user-scope writes are needed (for example in `~/.claude`), explicitly allow them:

```bash
macc apply --allow-user-scope
```

### Default startup behavior

- `macc` (no subcommand) runs `init` if needed, then opens the TUI.
- `macc tui` also ensures initialization first.

## Operational runbook (blank machine -> full automation cycle)

### 1) Prepare a blank machine

1. Install base dependencies:

```bash
sudo apt-get update
sudo apt-get install -y git curl jq build-essential pkg-config libssl-dev
```

2. Clone MACC and install it:

```bash
git clone <your-macc-repo-url> macc
cd macc
./scripts/install.sh --release
```

3. Verify:

```bash
macc --version
```

### 2) Install AI tools (Codex / Claude / Gemini)

1. Open TUI:

```bash
macc tui
```

2. Go to `Tools`:
- missing tools are shown as not installed,
- press `i` to run install action for the selected tool.

3. Confirm account/API-key prerequisite when prompted.
4. Complete tool login/API setup when the installer opens the tool command.
5. Run health checks in Tools view (`d`) or:

```bash
macc doctor
```

### 3) Initialize a target project

In your project directory:

```bash
macc init
macc tui
```

In TUI:
- enable tools,
- set tool defaults (models, approvals, etc.),
- configure `Automation / Coordinator` (base/reference branch, max dispatch, max parallel, staleness policy),
- save.

Then apply:

```bash
macc apply
```

### 4) Run coordinator full cycle

Prepare task source (for example PRD JSON), then run:

```bash
macc coordinator
```

`macc coordinator` runs the full loop (`sync -> dispatch -> advance -> reconcile -> cleanup`) until convergence.

Useful commands during execution:

```bash
macc coordinator status
macc coordinator sync
macc coordinator reconcile
```

Logs:
- coordinator: `.macc/log/coordinator/`
- performer: `.macc/log/performer/`

### 5) Failure recovery playbook

When tasks fail/block:

1. Inspect status and logs:

```bash
macc coordinator status
ls -la .macc/log/coordinator
ls -la .macc/log/performer
```

2. Attempt deterministic recovery:

```bash
macc coordinator reconcile
macc coordinator unlock
macc coordinator cleanup
```

3. Resume cycle:

```bash
macc coordinator
```

4. Stop safely if needed:

```bash
macc coordinator stop --graceful
```

5. Hard stop and cleanup worktrees/branches if required:

```bash
macc coordinator stop --remove-worktrees --remove-branches
```

6. If project state must be reset to pre-MACC managed artifacts:

```bash
macc clear
```

`macc clear` asks confirmation, runs forced worktree cleanup first, then removes MACC-managed paths only.

## Core commands

### Project lifecycle

- `macc init [--force] [--wizard]`: create/update `.macc/` layout and default config (`--wizard` asks 3 setup questions).
- `macc quickstart [-y|--yes] [--apply] [--no-tui]`: zero-friction happy path (checks prerequisites, initializes, seeds defaults, opens TUI or runs plan+apply).
- `macc plan [--tools tool1,tool2] [--json] [--explain]`: build preview only (no writes), with machine-readable JSON/explanations when needed.
- `macc apply [--tools ...] [--dry-run] [--allow-user-scope] [--json] [--explain]`: apply planned writes (`--dry-run` behaves as plan with same preview modes).
- `macc backups list [--user]`: list available backup sets (project or user-level).
- `macc backups open <id>|--latest [--user] [--editor <cmd>]`: print/open a backup set location.
- `macc restore --latest [--user] [--dry-run] [-y]` (or `--backup <id>`): restore files from a backup set.
- `macc clear`: asks confirmation, removes all non-root worktrees with force, then removes MACC-managed files/directories in the current project.
- `macc migrate [--apply]`: migrate legacy config to current format.
- `macc doctor [--fix]`: actionable diagnostics (tools, paths/permissions, worktrees/sessions, cache health). `--fix` applies safe fixes only (create missing dirs, add `.macc/cache/` to `.gitignore`, repair session state file when corrupt).

### TUI and tools

- `macc tui`: open interactive UI.
- `macc tool install <tool_id> [-y|--yes]`: install local tool via ToolSpec install commands.

### Catalog and installs

- `macc catalog skills list|search|add|remove`
- `macc catalog mcp list|search|add|remove`
- `macc catalog import-url --kind <skill|mcp> ...`
- `macc catalog search-remote --kind <skill|mcp> --q <query> [--add|--add-ids ...]`
- `macc install skill --tool <tool_id> --id <skill_id>`
- `macc install mcp --id <mcp_id>`

`macc catalog import-url` now prints:
- parsed source understanding (kind/url/ref/subpath),
- immediate validation status (subpath/manifest when source can be materialized),
- trust hints (pinned ref/checksum presence).  
Hints are informational only and do not guarantee security.

### Worktrees

- `macc worktree create <slug> --tool <tool_id> [--count N] [--base BRANCH] [--scope CSV] [--feature LABEL] [--skip-apply] [--allow-user-scope]`
- `macc worktree list`
- `macc worktree status`
- `macc worktree open <id|path> [--editor <cmd>] [--terminal]`
- `macc worktree apply <id|path> [--allow-user-scope]`
- `macc worktree apply --all [--allow-user-scope]`
- `macc worktree doctor <id|path>`
- `macc worktree run <id|path>`
- `macc worktree exec <id|path> -- <cmd...>`
- `macc worktree remove <id|path> [--force] [--remove-branch]`
- `macc worktree remove --all [--force] [--remove-branch]`
- `macc worktree prune`

### Coordinator

- `macc coordinator` (default full cycle: sync -> dispatch -> advance -> reconcile -> cleanup in loop until convergence)
- `macc coordinator [run|dispatch|advance|sync|status|reconcile|unlock|cleanup|stop]`
- `macc coordinator stop [--graceful] [--remove-worktrees] [--remove-branches]`
- Coordinator options can override config at runtime:
  - `--prd`, `--registry`, `--coordinator-tool`
  - `--tool-priority`, `--max-parallel-per-tool-json`, `--tool-specializations-json`
  - `--max-dispatch`, `--max-parallel`, `--timeout-seconds`
  - `--phase-runner-max-attempts`
  - `--stale-claimed-seconds`, `--stale-in-progress-seconds`, `--stale-changes-requested-seconds`, `--stale-action`
- Use `--` to forward raw args directly to `coordinator.sh`.

## TUI overview

Main screens:

- Home
- Tools
- Tool Settings
- Automation / Coordinator
- Skills
- MCP
- Logs
- Preview
- Apply

Common keys:

- Navigation: `h` Home, `t` Tools, `o` Automation, `m` MCP, `g` Logs, `p` Preview
- Save/apply: `s` Save config, `x` Apply
- Help: `?`
- Back: `Backspace`
- Quit: `q` / `Esc`

Tools screen includes:

- Toggle enabled tools.
- Open tool-specific settings.
- Install missing tools (`i`) using ToolSpec-defined install workflow.
- Refresh doctor checks (`d`).

## Configuration model

Primary file:

- `.macc/macc.yaml`

Important paths:

- `.macc/backups/` for project backups.
- `.macc/tmp/` for temporary files.
- `.macc/cache/` for fetched packages.
- `.macc/skills/` for local skills.
- `.macc/catalog/skills.catalog.json` and `.macc/catalog/mcp.catalog.json`.
- `.macc/automation/` for embedded coordinator/performer scripts and runners.
- `.macc/log/coordinator/` and `.macc/log/performer/` for centralized runtime logs.
- `.macc/state/managed_paths.json` for safe cleanup tracking.
- `.macc/state/tool-sessions.json` for performer session leasing/reuse.

## ToolSpec and catalog layering

### ToolSpecs (effective precedence: low -> high)

1. Built-in ToolSpecs embedded in the binary.
2. User overrides in `~/.config/macc/tools.d`.
3. Project overrides in `.macc/tools.d`.

### Catalogs

- Built-in skills/MCP catalogs are embedded in the binary.
- Project catalog files in `.macc/catalog/*.catalog.json` are local overrides and editable.
- Local skills in `.macc/skills/<id>/` are auto-discovered and shown in TUI/selection.

## Automation: coordinator + performer

MACC installs embedded scripts into `.macc/automation/`:

- `coordinator.sh`: orchestration loop for task registry + dispatch + sync/reconcile/cleanup/unlock.
  - Includes `advance` phase for PR/review/CI/merge queue progression.
  - Supports optional VCS integration hook via `COORDINATOR_VCS_HOOK` for PR create/status, review status, CI status, queue status, and merge status.
  - If no hook is configured, local fallback can auto-progress and locally merge (`COORDINATOR_AUTOMERGE=true`).
- `performer.sh`: worktree executor.
- `runners/<tool>.performer.sh`: tool-specific execution scripts.
- All automation logs are written under `.macc/log/` (coordinator + performer).

Coordinator defaults live in:

- `.macc/macc.yaml` under `automation.coordinator`

You can edit these defaults in the TUI Automation screen or override with `macc coordinator` flags.

## Session strategy

Performer session management is project-level, tool-aware, and lease-based:

- Session state file: `.macc/state/tool-sessions.json`
- Default isolation scope: per worktree (prevents cross-worktree context contamination).
- Sessions are reused in serial execution when available and not leased by active work.
- If all known sessions are occupied (or none exist), a new session is created.
- Lease release happens on performer exit, so closed worktrees can donate reusable sessions.

## Safety guarantees

- Writes are atomic and idempotent.
- Backups are created for changed project files.
- User-scope writes require explicit `--allow-user-scope` plus an interactive confirmation showing touched paths, backup location, and restore commands.
- Secret checks block unsafe generated output.
- `macc clear` is a two-step cleanup: confirm, then run forced worktree cleanup before deleting MACC-managed paths.
- Pre-existing project files/directories are preserved; only MACC-managed artifacts are removed.

## Documentation map

- `docs/README.md`: documentation index (active vs historical docs).
- `MACC.md`: full architecture/specification.
- `CHANGELOG.md`: release notes by version (Keep a Changelog format).
- `SECURITY.md`: vulnerability disclosure and supported version policy.
- `docs/CONFIG.md`: canonical config schema and semantics.
- `docs/TOOLSPEC.md`: ToolSpec format and field kinds.
- `docs/CATALOGS.md`: catalog schemas and workflows.
- `docs/TOOL_ONBOARDING.md`: add a tool end-to-end.
- `docs/COMPATIBILITY.md`: OS/MSRV compatibility policy.
- `docs/RELEASE.md`: SemVer/tag/release process.
- `docs/ralph.md`: Ralph automation flow.
- `docs/ADDING_TOOLS.md`: adding new tools/adapters.
- `CONTRIBUTING.md`: contribution workflow and PR quality baseline.

## Quality and release model

- CI runs on GitHub Actions (`.github/workflows/ci.yml`) with:
  - quality checks (format, lint, tests, tool-agnostic guardrails),
  - cross-platform build matrix (Linux/macOS/Windows).
- Releases are tag-driven (`vX.Y.Z`) with SemVer policy.
- Release workflow: `docs/RELEASE.md`
- Compatibility policy (OS + MSRV): `docs/COMPATIBILITY.md`
