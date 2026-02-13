# Adding a New Tool to MACC

`TOOL_ONBOARDING.md` is the canonical end-to-end guide.

Use this file only for implementation patterns and practical notes.

## Canonical flow

Follow:

1. `TOOL_ONBOARDING.md`
2. `TOOLSPEC.md`

## Practical patterns

### Pattern A: Runtime-only integration

Use this when the tool only needs performer execution (no new apply-time files):

- add ToolSpec in `registry/tools.d/`
- add runner script in `adapters/<tool>/`
- ensure doctor/install/post-install commands are in ToolSpec
- validate with:
  - `make check-generic`
  - `make test`
  - `./automat/tests/run.sh`

### Pattern B: Full integration (runtime + apply-time generation)

Use this when the tool also needs generated project files:

- all steps from Pattern A
- plus adapter crate implementation and registry wiring
- plus contract coverage:
  - `make test-contract`

## Common pitfalls

- Hardcoding tool names in generic CLI/TUI/core paths.
- Missing `performer.runner` in ToolSpec.
- Missing registry wiring for new adapter crates.
- Skipping changelog/docs updates for user-visible behavior.
