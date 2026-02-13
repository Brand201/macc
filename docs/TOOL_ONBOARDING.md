# Add a Tool End-to-End

This is the canonical, unified guide for integrating a new tool into MACC
without touching generic CLI/TUI/core flows.

## 1) Add ToolSpec

Create a new file in `registry/tools.d/<tool-id>.tool.yaml`:

- metadata (`id`, `display_name`, `description`)
- `fields` for tool settings exposed in TUI
- `doctor` checks
- `performer.runner` path
- optional `install` and `post_install` commands

Reference: `TOOLSPEC.md`.

Example:

```yaml
api_version: v1
id: example-tool
display_name: Example Tool
description: Workspace settings for the Example Tool assistant.
fields:
  - id: model
    label: Model
    kind:
      type: enum
      options: [fast, smart, turbo]
    help: Select the model preset.
    pointer: /tools/config/example-tool/model
  - id: enable_telemetry
    label: Enable Telemetry
    kind:
      type: bool
    help: Opt-in to anonymous telemetry.
    pointer: /tools/config/example-tool/telemetry
doctor:
  - kind: which
    value: example-tool-cli
    severity: warning
performer:
  runner: adapters/example/example.performer.sh
```

## 2) Add Performer Runner

Add runner script under adapter path, for example:

- `adapters/<tool>/<tool>.performer.sh`

Requirements:

- consume `--prompt-file`, `--tool-json`, task/worktree context args
- honor retry strategy and model escalation policy if relevant
- update session lease state via `.macc/state/tool-sessions.json` contract

## 3) Add Adapter Crate (if needed)

If the tool requires apply-time file generation:

1. create `adapters/<tool>/`
2. implement adapter logic
3. wire crate in `registry/Cargo.toml` so registry discovers it

If only runtime performer behavior changes, adapter crate changes may not be required.

Minimal adapter sketch:

```rust
pub struct ExampleToolAdapter;

impl ToolAdapter for ExampleToolAdapter {
    fn id(&self) -> String {
        "example-tool".to_string()
    }

    fn plan(&self, ctx: &PlanningContext) -> macc_core::Result<ActionPlan> {
        let mut plan = ActionPlan::new();

        let model = ctx
            .resolved
            .tools
            .get_value("/tools/config/example-tool/model")
            .and_then(|v| v.as_str())
            .unwrap_or("fast");

        let content = format!("model = {}\n", model);
        plan.add_action(Action::WriteFile {
            path: ".example-tool/config".to_string(),
            content: content.into_bytes(),
            scope: Scope::Project,
        });

        Ok(plan)
    }
}
```

If you add an adapter crate, ensure it is wired in `registry/src/lib.rs` so the registry discovers it.

## 4) Validate Tool-Agnostic Guardrails

Do not hardcode vendor names in generic UX paths:

- CLI (`cli/`)
- TUI (`tui/`)
- generic core flows (`core/`)

Run:

```bash
make check-generic
```

## 5) Add/Update Tests

Minimum:

- ToolSpec contract coverage (`macc-registry` tests).
- Adapter behavior tests (if adapter changed).
- Automation integration tests if coordinator/performer behavior changed.

Run:

```bash
make test
make test-contract
./automat/tests/run.sh
```

## 6) Update Documentation

Update all relevant docs in same PR:

- `README.md` command/behavior changes
- `MACC.md` architecture/operational behavior
- `CHANGELOG.md` (`Unreleased`)
- optional tool-specific docs/examples

## 7) Release Readiness

Before release, ensure:

- CI green on all required jobs
- changelog entry exists
- compatibility policy still accurate

Reference: `docs/RELEASE.md`.

## Quick checklist

1. Add ToolSpec (`registry/tools.d/...`).
2. Add performer runner (`adapters/<tool>/...performer.sh`).
3. Add/adjust adapter crate and registry wiring if apply-time generation is needed.
4. Run guardrails/tests (`make check-generic`, `make test`, `make test-contract`, `./automat/tests/run.sh`).
5. Update docs and `CHANGELOG.md`.
