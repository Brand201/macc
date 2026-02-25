
# MACC

MACC (Multi-Agentic Coding Config) is a agentic coding tool configuration manager. It generates tool-specific files (Claude code, Codex, Gemini Cli, etc.) via adapters.

It also integrates an autonomous AI agent loop that runs Installed agentic coding tool. 
They can run on the same project in parallel (using worktrees) repeatedly until all assigned tasks are completed. All of this is managed by a coordinator and can be done autonomously or semi-autonomously.

## What MACC provides

- Canonical config and deterministic generation (`plan` then `apply`).
- Tool-agnostic TUI for tool selection, tool settings, skills, MCP, and automation coordinator settings.
- Embedded defaults for ToolSpecs and catalogs so clean machines are usable immediately.
- Project automation with embedded `coordinator.sh` + `performer.sh` + per-tool runners.
- Worktree orchestration for parallel task execution.
- Safe cleanup (`macc clear`) with confirmation: removes worktrees first, then MACC-managed project artifacts.

## Installation

### Recommended: install from GitHub (`curl -sSL`)

```bash
curl -sSL https://raw.githubusercontent.com/Brand201/macc/master/scripts/install.sh | bash -s -- --release
```

Pinned release/tag example:

```bash
curl -sSL https://raw.githubusercontent.com/Brand201/macc/v0.1.0/scripts/install.sh | bash -s -- --release --ref v0.1.0
```

### Alternative: install from a local clone

```bash
git clone https://github.com/Brand201/macc.git
cd macc
./scripts/install.sh --release
```

Options:

- `--release`: build optimized binary.
- `--prefix <dir>`: install `macc` into a custom directory.
- `--system`: install to `/usr/local/bin` (uses `sudo`).
- `--no-path`: do not update shell profile `PATH`.
- `--repo <url>`: install from another git repository URL.
- `--ref <ref>`: install from branch/tag/commit (default: `master`).

### Uninstall

Recommended:

```bash
macc-uninstall
```

Alternative from a local clone:

```bash
./scripts/uninstall.sh
```

Uninstall directly from GitHub script:

```bash
curl -sSL https://raw.githubusercontent.com/Brand201/macc/master/scripts/uninstall.sh | bash
```

Options:

- `--system`: remove `/usr/local/bin/macc`.
- `--prefix <dir>`: remove `<dir>/macc` and `<dir>/macc-uninstall`.
- `--clean-profile`: remove installer-added `PATH` lines from shell profiles.
- `--keep-helper`: keep `macc-uninstall` while removing `macc`.

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

2. Install MACC (recommended: GitHub installer):

```bash
curl -sSL https://raw.githubusercontent.com/Brand201/macc/master/scripts/install.sh | bash -s -- --release
```

Alternative (local clone):

```bash
git clone https://github.com/Brand201/macc.git
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
If a merge conflict stays unresolved (even after AI merge-fix hook), coordinator enters a paused state and waits for operator signal:
```bash
macc coordinator resume
```

Useful commands during execution:

```bash
macc coordinator status
macc coordinator sync
macc coordinator reconcile
```

## Error codes and auto-retry

MACC emits structured error codes when a performer or coordinator step fails. These codes are written into coordinator events and task runtime metadata.

### Error code schema (v1)

- `E100` Runner/Tool
  - `E101` Runner exited non-zero
  - `E102` Tool runner not found / not executable
  - `E103` Tool output malformed / parsing failed
- `E200` Capability/Contract
  - `E201` Requested unavailable tool
  - `E202` Capability guard triggered
- `E300` Worktree/FS
  - `E301` Worktree missing
  - `E302` PRD missing
  - `E303` tool.json missing
- `E400` Coordinator/Registry
  - `E401` Task registry read/write failure
  - `E402` Task state transition invalid
- `E500` Merge
  - `E501` Merge conflict
  - `E502` Merge worker failed
- `E900` Unknown/Unexpected
  - `E901` Unknown fatal error

### Auto-retry policy (coordinator)

Coordinator can auto-retry failed tasks based on error code:

- `ERROR_CODE_RETRY_LIST` default: `E101,E102,E103,E301,E302,E303`
- `ERROR_CODE_RETRY_MAX` default: `2`

When a failed task has an error code in the allow-list and retries are below the max, the task is requeued to `todo` with an `auto_retry:<code>` reason.

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
- `macc tool update <tool_id> [--check] [-y|--yes] [--force] [--rollback-on-fail]`: update one installed tool.
- `macc tool update --all [--only enabled|installed] [--check] [-y|--yes] [--force] [--rollback-on-fail]`: batch update tools.
- `macc tool outdated [--only enabled|installed]`: show installed/current/latest status and outdated tools.
- `macc context [--tool <tool_id>] [--from <file> ...] [--dry-run] [--print-prompt]`: sends a context prompt to the selected AI tool; the tool must edit its target context file in-place (for example `AGENTS.md`, `CLAUDE.md`, `GEMINI.md`).
- In TUI `Tools` screen, press `f` to generate context for the selected tool.
- To prevent `macc apply` from overwriting existing context files, set per-tool protection in `.macc/macc.yaml`:
  - `tools.config.<tool_id>.context.protect: true`

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

Worktrees let MACC run multiple isolated task branches and tool sessions in parallel without contaminating each other, while keeping the main repo clean and reviewable.

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

Coordinator orchestrates the end-to-end automation cycle: it reads the task registry, dispatches work to tools, tracks state transitions, supervises performers, and reconciles/cleans up until convergence.

- `macc coordinator` (default full cycle: sync -> dispatch -> advance -> reconcile -> cleanup in loop until convergence)
- `macc coordinator` opens the TUI `Coordinator Live` screen and starts coordinator run.
- `macc coordinator [run|dispatch|advance|sync|status|reconcile|unlock|cleanup|stop]`
- `macc coordinator run --no-tui` keeps the previous headless CLI behavior.
- `macc coordinator stop [--graceful] [--remove-worktrees] [--remove-branches]`
- Coordinator options can override config at runtime:
  - `--prd`, `--coordinator-tool`
  - `--tool-priority`, `--max-parallel-per-tool-json`, `--tool-specializations-json`
  - `--max-dispatch`, `--max-parallel`, `--timeout-seconds`
  - `--phase-runner-max-attempts`
  - `--stale-claimed-seconds`, `--stale-in-progress-seconds`, `--stale-changes-requested-seconds`, `--stale-action`
  - Heartbeat events update `task_runtime.last_heartbeat` from `events.jsonl`.
  - Runtime stale heartbeat policy via env: `STALE_HEARTBEAT_SECONDS`, `STALE_HEARTBEAT_ACTION=retry|block|requeue` (retry/requeue resets task to `todo`; retry also increments runtime retries).
- Task registry path is fixed to `.macc/automation/task/task_registry.json`.
- Coordinator emits event bus lines to `.macc/log/coordinator/events.jsonl` (used by TUI live screen).
- `run`, `dispatch`, `advance`, `reconcile`, and `cleanup` are executed by native Rust handlers (async supervision + retries/timeouts per phase).
- Legacy shell coordinator removed; all coordinator actions run natively in Rust.
- Worktrees are managed as a reusable worker pool (not task-named): a merged/clean slot is reset to the reference branch, switched to a fresh branch, updated (`worktree.prd.json` + apply), then reused for the next task.
- If no reusable slot is available, coordinator creates a new worker worktree; total pool size is bounded by `--max-parallel` / `automation.coordinator.max_parallel`.
- Realtime orchestrator target design (state model + event contract + rollout): `docs/COORDINATOR_REALTIME.md`.
- Use `--` only for coordinator subcommands that require raw passthrough args.

## TUI overview

Main screens:

- Home
- Tools
- Tool Settings
- Automation / Coordinator (settings only)
- Coordinator Live (runtime monitoring)
- Skills
- MCP
- Logs
- Preview
- Apply

Common keys:

- Navigation: `h` Home, `t` Tools, `o` Automation, `v` Coordinator Live, `m` MCP, `g` Logs, `p` Preview
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

MACC installs embedded automation assets into `.macc/automation/`:

- Native Rust coordinator control-plane (primary runtime path for `run` + core actions).
- `coordinator.sh`: thin wrapper for native Rust coordinator actions.
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
- `docs/COORDINATOR_REALTIME.md`: short design doc for event-driven coordinator evolution.
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
