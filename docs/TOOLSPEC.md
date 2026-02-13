# ToolSpec Schema (v1)

`ToolSpec` defines how MACC discovers, configures, validates, installs, and runs a tool.

Primary references:

- `docs/TOOL_ONBOARDING.md` (end-to-end integration flow)
- `docs/CONFIG.md` (canonical config roots and semantics)

## Top-level fields

- `api_version` (string, required): must be `v1`.
- `id` (string, required): unique kebab-case tool identifier.
- `display_name` (string, required): user-facing label.
- `description` (string, optional): long description used in TUI/details.
- `capabilities` (array<string>, optional): capability hints (`mcp`, `skills`, etc.).
- `gitignore` (array<string>, optional): project paths MACC may add to `.gitignore`.
- `fields` (array<FieldSpec>, required): TUI-editable settings.
- `doctor` (array<DoctorCheckSpec>, optional): install/health checks for `macc doctor`.
- `install` (ToolInstallSpec, optional): commands used by `macc tool install <tool>`.
- `performer` (ToolPerformerSpec, optional): runtime execution contract for worktree performer.
- `defaults` (object, optional): tool runtime defaults merged into `.macc/tool.json`.

Validation notes:

- `api_version` must be exactly `v1`.
- `id` must be kebab-case (`a-z`, `0-9`, `-`, no leading/trailing `-`, no `--`).
- `enum` fields must declare at least one option.

## FieldSpec

- `id` (string, required): stable field key.
- `label` (string, required): display label in TUI.
- `kind` (object, required):
  - `{ type: bool }`
  - `{ type: text }`
  - `{ type: number }`
  - `{ type: array }`
  - `{ type: enum, options: [..] }` (must be non-empty)
  - `{ type: action, action: ... }`
- `help` (string, optional): inline help text.
- `pointer` (string, optional, alias: `json_pointer`): canonical config path.
- `default` (typed value, optional): default value for non-action fields.

Validation notes:

- `pointer` must start with `/`.
- Only authorized roots are accepted:
  - `/tools/enabled`
  - `/tools/config/<tool-id>` and `/tools/config/<tool-id>/...`
  - `/selections/skills`, `/selections/agents`, `/selections/mcp`
  - `/standards/path`, `/standards/inline/...`
- A `default` requires a `pointer` and must match `kind`.
- `default` is not allowed for `kind.type: action`.

### Action kind

For `kind.type: action`, supported actions:

- `open_mcp` with `target_pointer`
- `open_skills` with `target_pointer`
- `open_agents` with `target_pointer`
- `custom` with `target`

Validation notes:

- `target_pointer` actions use the same pointer authorization rules.
- `custom` actions do not use `target_pointer`; they require `target`.

## Doctor checks

`doctor` entries:

- `kind`: `which` | `path_exists` | `custom`
- `value`: check target/command
- `severity`: `error` | `warning`

## Install spec

`install` is used by `macc tool install`:

- `commands`: array of commands run in order.
- `post_install`: optional command run after primary install.
- `confirm_message`: optional prompt shown before install.

Runtime behavior note:

- If `confirm_message` is omitted, MACC uses a default safety prompt.

Each command item:

- `command`: executable
- `args`: array of args

## Performer spec

Required performer fields:

- `runner`: script path executed by `performer.sh` (typically under `adapters/<tool>/`).
- `command`: tool executable (for runtime metadata/contract validation).
- `args`: optional default args.

Optional performer fields:

- `prompt`:
  - `mode`: prompt transport mode (for runner contract)
  - `arg`: optional CLI flag used for prompt mode
- `retry`:
  - `command`
  - `args`
- `session`:
  - `enabled` (default: `true`)
  - `scope`: `project` or `worktree`
  - `init_prompt`
  - `extract_regex`
  - `resume` (command + args)
  - `discover` (command + args)
  - `id_strategy`: `generated` or `discovered`

Validation notes:

- `prompt.mode` must be `stdin` or `arg`.
- If `prompt.mode` is `arg`, `prompt.arg` is required.
- `retry.command`, `session.resume.command`, and `session.discover.command` cannot be empty.
- `session.id_strategy` must be `generated` or `discovered`.

Runtime config note:

- `ToolSpec.performer` is optional for UI/config usage.
- Runtime automation (`macc worktree run`, coordinator/performer flow) requires `performer`.

## Minimal example

```yaml
api_version: v1
id: minimal-tool
display_name: Minimal Tool
fields:
  - id: enabled
    label: Enabled
    kind:
      type: bool
    pointer: /tools/config/minimal-tool/enabled
```

## Full example

```yaml
api_version: v1
id: gemini
display_name: Gemini CLI
description: Gemini tool integration for MACC.
capabilities: [mcp]
fields:
  - id: model
    label: Default Model
    kind:
      type: enum
      options: [gemini-2.0-pro-exp, gemini-1.5-pro]
    pointer: /tools/config/gemini/model
  - id: manage-mcp
    label: Manage MCP Servers
    kind:
      type: action
      action: open_mcp
      target_pointer: /selections/mcp
doctor:
  - kind: which
    value: gemini
    severity: error
install:
  confirm_message: You must already have an account/API key for this tool.
  commands:
    - command: bash
      args: ["-lc", "echo install gemini here"]
performer:
  runner: adapters/gemini/gemini.performer.sh
  command: gemini
  args: ["--model", "gemini-3-flash-preview"]
  prompt:
    mode: arg
    arg: "--prompt"
  retry:
    command: gemini
    args: ["--model", "gemini-3-pro"]
  session:
    enabled: true
    scope: worktree
    id_strategy: discovered
    discover:
      command: gemini
      args: ["--list-sessions"]
defaults:
  approval_mode: auto_edit
  sandbox: false
```
