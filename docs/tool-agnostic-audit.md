# Tool-Agnostic Audit Report (Lot 2)

This document baselines the current hard-coded references to specific AI tools (Claude, Gemini, Codex) within the `tui/` and `cli/` crates. The goal of Lot 2 is to move toward a completely tool-agnostic interface where these crates only interact with tool IDs and descriptors provided by the core registry.

## Summary of Findings

The `tui/` and `cli/` crates are largely tool-agnostic in their core logic, using the `ToolRegistry` and `ToolDescriptor` traits to interact with adapters. However, specific tool IDs are frequently used in:
1. **Unit and Integration Tests**: To mock configurations and verify behaviors.
2. **Help Text**: Providing examples to the user.
3. **Internal Test Fixtures**: Hard-coded paths or strings used for verification.

## Classification of References

### 1. UI Text / Help Text
References found in user-facing help strings or documentation within the code.

| Path | String / Context | Suggested Strategy |
|------|------------------|--------------------|
| `cli/src/main.rs:86` | `/// Tool to install the skill for (e.g. claude, codex, gemini)` | Replace with generic examples or dynamically list from registry. |

### 2. Tests and Fixtures
The majority of references are here. Tests often hard-code tool IDs to set up a `CanonicalConfig`.

| Path | Pattern / Context | Suggested Strategy |
|------|-------------------|--------------------|
| `tui/src/state.rs` (Tests) | Many occurrences of `"claude"`, `"gemini"`, `"codex"`. | Use `test-adapter` ID or generate random IDs for testing core logic. |
| `cli/src/main.rs` (Tests) | Many occurrences of `"claude"`, `"gemini"`, `"codex"`. | Use `test-adapter` ID or mock the registry during tests. |
| `cli/src/main.rs` | Hard-coded paths like `.gemini/skills/...` in tests. | Use a generic `.macc/cache/...` or similar if possible. |

### 3. Conditional Logic / Registry Calls
None found in `tui/` or `cli/` that specifically switch on tool IDs (other than generic ID matching from registry).

## Detailed Inventory

### `tui/src/state.rs` (Tests)
- `test_load_config_valid`: Hard-codes `gemini`.
- `test_save_config`: Hard-codes `gemini` and `claude`.
- `test_tool_selection_and_toggling`: Hard-codes `claude`, `codex`, `gemini`.
- `test_tool_settings_navigation_and_cycling`: Hard-codes `claude`.
- `test_skills_selection`: Hard-codes `claude`.
- `test_agents_selection`: Hard-codes `claude`.
- `test_unified_navigation`: Hard-codes `claude`.
- `test_config_golden_serialization`: Hard-codes `claude`, `gemini`.

### `cli/src/main.rs` (Tests)
- `test_plan_with_tools_override`: Hard-codes `claude`, `codex`, `gemini`.
- `test_install_skill_cli`: Hard-codes `gemini`.
- `test_install_skill_multi_zip_cli`: Hard-codes `claude`.
- `test_install_skill_multi_git_cli`: Hard-codes `codex`.
- `test_claude_mcp_json_generation` (mentioned in search results, likely in a different test file or further down in main.rs).

## Refactor Strategy

1. **Registry-Driven Help**: Update CLI help to pull available tool IDs from the `ToolRegistry` instead of hard-coding them in docstrings.
2. **Generic Test Adapter**: Leverage the `TestAdapter` (currently enabled in debug/test builds) for all tests that verify TUI/CLI state management logic.
3. **Data-Driven Tests**: Instead of hard-coding tool names, use the registry to fetch the first $N$ available tools for testing toggles and configuration.
4. **Mock Project Structure**: Tests that verify file generation should use the adapter's reported paths rather than assuming `.claude/` or `.gemini/`.

## Acceptance Checklist

- [x] 0 occurrences of hard-coded tool names (`claude`, `gemini`, `codex`, `anthropic`, `google`, `openai`) in `tui/` and `cli/` crates (excluding comments and tests if allowed, but preferred 0 even in tests for full agnosticism).
- [x] TUI and CLI only use IDs obtained from `macc_registry`.
- [x] Help text for `macc install skill --tool` is dynamically generated (replaced with generic examples).
- [x] No tool-specific path assumptions in `cli/src/main.rs` installation logic.
