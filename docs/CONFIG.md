# Canonical Configuration (`macc.yaml`)

MACC uses a single source of truth for all AI tool configurations, stored in `.macc/macc.yaml`. This file is tool-agnostic and allows MACC to generate tool-specific artifacts (like `CLAUDE.md`, `GEMINI.md`, etc.) deterministically.

## Structure

```yaml
version: v1

# Tool activation and per-tool settings
tools:
  enabled:
    - claude
    - gemini

  # Recommended: Structured tool-specific settings
  config:
    claude:
      model: sonnet
      language: English
      context:
        protect: true
    gemini:
      user_mcp_merge: true
      context:
        protect: true

  # Legacy/Flattened: Top-level keys are also mapped to tool IDs
  # (Supported for backward compatibility, but 'config' is preferred)
  codex:
    model: o1-preview

# Project-wide standards
standards:
  # Path to a file containing global instructions
  path: docs/STANDARDS.md
  
  # Inline standards (key-value pairs)
  inline:
    language: English
    style: functional

# Global feature selections
selections:
  skills:
    - implement
    - test
  agents:
    - architect
  mcp:
    - brave-search

# Custom MCP server templates
mcp_templates:
  - id: custom-server
    title: My Custom MCP
    description: A template for a custom MCP server
    command: node
    args: ["scripts/my-mcp.js"]
    env_placeholders:
      - name: API_KEY
        placeholder: "${MY_API_KEY}"

# Automation settings
automation:
  ralph:
    enabled: true
    iterations_default: 5
    branch_name: ralph
    stop_on_failure: true
  coordinator:
    coordinator_tool: codex
    reference_branch: main
    prd_file: prd.json
    task_registry_file: task_registry.json
    tool_priority: [codex, claude, gemini]
    max_parallel_per_tool:
      codex: 3
      claude: 2
    tool_specializations:
      frontend: [claude, gemini]
      rust: [codex]
    max_dispatch: 0
    max_parallel: 2
    timeout_seconds: 0
    phase_runner_max_attempts: 1
    stale_claimed_seconds: 0
    stale_in_progress_seconds: 0
    stale_changes_requested_seconds: 0
    stale_action: abandon
```

## Tools Configuration

### `tools.enabled`
A list of tool IDs that are active for the project. Adapters for these tools will run during `macc apply`.

### `tools.config`
A map where each key is a tool ID and the value is an arbitrary JSON/YAML object containing settings for that tool. This is the preferred way to store tool-specific configuration because it avoids naming collisions with core MACC fields and doesn't require updating MACC's internal Rust structs when a tool adds a new setting.

Recommended context protection flag per tool:

```yaml
tools:
  config:
    codex:
      context:
        protect: true
        fileName: AGENTS.md
    claude:
      context:
        protect: true
        fileName: CLAUDE.md
    gemini:
      context:
        protect: true
        fileName: GEMINI.md
```

When `context.protect: true` is set for a tool, `macc apply` will not overwrite an existing context file for that tool. This preserves files updated by `macc context` or manual edits.

### Legacy Flattened Settings
For compatibility, MACC also supports placing tool settings directly under the `tools` key (e.g., `tools.claude:`). These are merged into the resolved configuration but may be deprecated in the future in favor of the `config` map.

Resolution behavior:

- `tools.enabled` is normalized at resolve-time (sorted + deduplicated).
- `tools.config` and legacy flattened `tools.<id>` are both preserved in resolved output.

## Standards

Standards provide high-level context to all AI assistants.
- `standards.path`: Relative path to a markdown file with project rules.
- `standards.inline`: A dictionary of key-value pairs that adapters can inject into templates (e.g., `language: TypeScript`).

Resolution behavior:

- If `standards.inline.language` is missing, MACC injects `language: English` by default.

## Selections

Selections represent global choices made from the MACC catalog (skills, agents, MCPs).
- `skills`: Functional capabilities (e.g., `refactor`, `audit`).
- `agents`: Specialized personas.
- `mcp`: Model Context Protocol servers to be enabled across all tools.

Resolution behavior:

- `skills`, `agents`, and `mcp` are normalized at resolve-time (sorted + deduplicated).

## Automation

### `automation.ralph`
Controls Ralph loop script generation/behavior (`scripts/Ralph/ralph.sh`).

### `automation.coordinator`
Controls coordinator runtime defaults for `.macc/automation/coordinator.sh`.
- `coordinator_tool`: fixed tool for coordinator phase hooks (`review`/`fix`/`integrate`).
- `reference_branch`: default git base/reference branch used when a task does not define `base_branch`.
- `prd_file`: PRD source path (default commonly `prd.json`).
- `task_registry_file`: registry file path (default commonly `task_registry.json`).
- `tool_priority`: preferred tool order for task assignment.
- `max_parallel_per_tool`: per-tool concurrency limits.
- `tool_specializations`: category-to-tools routing map.
- `max_dispatch`: max tasks launched per `dispatch` run (`0` means no cap).
- `max_parallel`: max concurrent performer runs.
- `timeout_seconds`: lock wait timeout (`0` disables timeout).
- `phase_runner_max_attempts`: retry attempts for phase runner fallback.
- `stale_*_seconds`: stale thresholds for task states (`0` disables each threshold).
- `stale_action`: stale policy (`abandon`, `todo`, `blocked`).

These values are used by `macc coordinator` as defaults and can be overridden via CLI flags or environment variables.

## MCP Templates

`mcp_templates` defines reusable MCP server templates for the project.

- If omitted, MACC injects built-in templates at load time.
- Template IDs must be unique and non-empty.
- `command` is required.
- `env_placeholders` entries require both `name` and `placeholder`.

## JSON Pointers

ToolSpecs use JSON pointers to map these configuration values into tool-specific files. To ensure stability and avoid drift, MACC enforces the following canonical roots for pointers:

| Root | Description | Example |
|------|-------------|---------|
| `/tools/enabled` | The list of active tools. | `/tools/enabled` |
| `/tools/config/<id>/` | Tool-specific configuration. | `/tools/config/claude/model` |
| `/selections/skills` | Global skill selections. | `/selections/skills` |
| `/selections/agents` | Global agent selections. | `/selections/agents` |
| `/selections/mcp` | Global MCP server selections. | `/selections/mcp` |
| `/standards/path` | Path to global standards file. | `/standards/path` |
| `/standards/inline/` | Inline standards key-value pairs. | `/standards/inline/language` |

Pointers used in ToolSpecs that do not start with one of these roots will be rejected during validation.

Note:

- Exact root `/tools/config/<id>` (without trailing slash) is also valid.
