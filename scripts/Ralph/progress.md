# Codebase Patterns
- Enforce architectural boundaries (e.g., tool-agnosticism) with dedicated CI scripts using denylists to prevent forbidden string leakage across layers.
- Use `write_if_changed` with a pre-write hook to centralize comparisons, backups, and same-directory atomic renames for safe, idempotent writes.
- Rust workspace using `macc-core` (library) and `macc-cli` (binary).
- The binary is named `macc` and uses `clap` for CLI argument parsing.
- Use `Makefile` for common tasks like `fmt`, `lint`, and `test` to ensure consistent quality checks across environments.
- Centralize path management in a `ProjectPaths` struct and resolve starting paths to absolute/canonical forms early to ensure deterministic filesystem operations.
- Use `ActionPlanBuilder` to construct safe plans with automatic path normalization and validation (rejects absolute paths and parent traversal).
- Implement helper methods on complex enums (like `Action`) to provide common accessors (`path()`, `scope()`) and reduce pattern-matching boilerplate.
- Normalize structured data (like JSON) before comparison or diffing to ensure semantic rather than just textual equality.
- Derive preview-friendly `PlannedOp`s from the deterministic `ActionPlan` so frontends can render file lists, diffs, and consent metadata without mutating the filesystem.
- Planned operations expose a dedicated `consent_required` flag so UI/CLI clients can quickly identify user-level actions without re-evaluating scope metadata.
- Feed the same `PlannedOp` sequence into both the CLI apply engine and any UI-driven apply flow (with optional progress callbacks) to keep backups, atomic writes, and consent side effects in lockstep across surfaces.
- Splitting the pipeline into `build_plan` (logic) and `preview_plan` (UI/reporting) allows for easier integration into different frontends (CLI, TUI, etc.) and keeps core logic testable.
- Using a separate `validate_plan` function allows for consistent safety checks (secrets, user-scope) across both `plan` (preview) and `apply` (execution) workflows.
- Use a `PlanningContext` struct for `ToolAdapter::plan` to allow future expansion of the context without breaking the trait signature.
- `FetchUnit` bridges selection IDs to physical sources with combined subpaths, decoupling fetching from selection logic.
- Materialize fetch units into a stable local root path before planning to allow adapters to access skill-specific files (e.g., rules, templates).
- Using simple heuristics like marker files (e.g., `SKILL.md`, `skill.md`, `README.md`) is a lightweight way to provide early feedback on configuration errors before attempting tool-specific generation.
- **Core Action Extensions**: When adding new `Action` types to `core`, ensure `PartialOrd` and `Ord` are implemented (manually if necessary) for determinism, and that derived traits on fields (like `serde_json::Value`) are compatible or handled.
- **Testability & Global State**: Passing an optional starting directory to discovery functions (like `load_config`) makes them much easier to unit test without relying on or interfering with global process state like `env::current_dir`.
- **Deterministic Serialization**: Switching from `HashMap` to `BTreeMap` in core data structures that are serialized to YAML/JSON is essential for maintaining a clean git history and ensuring idempotence across different environments or runs.
- **Loopback-dependent tests**: When tests spin up local HTTP servers, use a helper that skips gracefully if binding to `127.0.0.1` is not permitted by the environment to keep CI stable across sandboxes.
- **Ratatui list-detail pattern**: For multi-select screens, use a left list + right detail pane with a selection index and sorted vectors to keep UX clear and state transitions easy to unit test.
- **Plan reuse across frontends**: When building TUI previews, reuse the CLI’s resolve_fetch_units → materialize_fetch_units → plan_operations pipeline (with the same tool registry) so the preview stays deterministic and matches the core apply behavior.
- **Timestamped user backups**: Mirror user files under `~/.macc/backups/<timestamp>/` by stripping the home prefix and removing traversal components so the archived tree matches the source layout without following absolute paths.
- When previewing planned diffs, sanitize the text using the secret scanner and truncate with a clear indicator before exposing it to UI components.
- Cache expensive UI artifacts (like diff text) per planned operation using deterministic keys (path + kind) so repeated navigation stays responsive without recomputing.
- **Pure Reducers and Navigation Logic**: Refactoring state updates into pure functions and centralizing screen-specific navigation logic in the state model (e.g., `navigate_next`, `navigate_prev`) improves testability and keeps the main event loop clean.

## 2026-01-29 16:04:54Z - L1-BOOT-001
Run: gemini-20260129T160454Z
- Created root `Cargo.toml` for Rust workspace.
- Created `macc-core` library crate in `core/`.
- Created `macc-cli` binary crate in `cli/`, producing the `macc` executable.
- Implemented minimal CLI with `init` and `apply` subcommands using `clap`.
- Verified `cargo build` and `cargo run -- --help` work as expected.

Files changed:
- `Cargo.toml` (new)
- `core/Cargo.toml` (new)
- `core/src/lib.rs` (new)
- `cli/Cargo.toml` (new)
- `cli/src/main.rs` (new)

**Learnings for future iterations:**
- Patterns discovered: Standard workspace structure with library/binary split allows for easy reuse of core logic in TUI later.
- Gotchas encountered: Ensure root `Cargo.toml` includes all workspace members to avoid compilation issues.
- Useful context: `macc-cli` is the entry point, and it depends on `macc-core`.
---

## 2026-01-30T19:39:15Z - L2-TUI-007
Run: codex-gpt-5.1-codex-max-20260130T183915Z
- Added MCP Servers selection screen (tool-agnostic) with dual-pane list/detail, warning badges for secrets, and shortcut keys.
- Persist MCP selections into canonical config via AppState toggles; updated home summary to show selected servers.
- Introduced built-in MCP template metadata (id, purpose, auth, env placeholders) and reducer helpers with unit tests.
- Updated navigation/help bindings to reach the MCP screen (`m`) and support bulk select/clear actions.
- Files changed: tui/src/screen.rs; tui/src/state.rs; tui/src/lib.rs; scripts/Ralph/prd.json; scripts/Ralph/progress.md
- Thread: local CLI session (no external thread URL available).

**Learnings for future iterations:**
- Patterns discovered: The list+detail Ratatui layout with sorted selections keeps multi-select UX consistent across screens and easy to test.
- Gotchas encountered: Remember to insert new screens into all navigation/help match arms to avoid unreachable UI states.
- Useful context: MCP templates remain placeholder-only; secrets are never stored—warn users and rely on env vars.
---

## 2026-01-29 16:27:52Z - L1-BOOT-001
Run: gemini-20260129T162752Z
- Verified existing Rust workspace with `macc-core` and `macc-cli`.
- Added unit test to `macc-core` to verify version function.
- Confirmed `cargo build` and `cargo test` pass.
- Confirmed `macc --help` works.

Files changed:
- `core/src/lib.rs`
- `scripts/Ralph/prd.json`

**Learnings for future iterations:**
- Patterns discovered: Using `#[cfg(test)]` for inline unit tests in library crates is idiomatic and ensures basic functionality is always verified.
- Gotchas encountered: The task description mentioned `crates/core` but the actual implementation used `core/`. Maintaining consistency with existing structure is preferred over strictly following potentially outdated task descriptions.
- Useful context: `macc-cli` depends on `macc-core` and uses `clap` for CLI parsing.
---

## 2026-01-29 16:30:20Z - L1-BOOT-002
Run: gemini-20260129T163020Z
- Created `Makefile` with `fmt`, `lint`, `test`, and `check` command.
- Created `CONTRIBUTING.md` with instructions on how to use the quality tools.
- Added `rustfmt.toml` for consistent formatting.
- Verified that `make check` passes on the current codebase.

Files changed:
- `Makefile` (new)
- `CONTRIBUTING.md` (new)
- `rustfmt.toml` (new)
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Standardizing quality checks via a `Makefile` provides a low-friction way for both humans and agents to maintain code quality.
- Gotchas encountered: None in this task, as the existing codebase already passed formatting and linting.
- Useful context: `make check` is the recommended way to run all quality checks before committing.
---

## 2026-01-29 16:32:03Z - L1-CLI-001
Run: gemini-20260129T163203Z
- Implemented `init`, `plan`, and `apply` subcommands in `macc-cli` using `clap`.
- Added global flags `--cwd` and `--verbose`.
- Added specific flags: `--force` for `init`, `--tools` for `plan`/`apply`, and `--dry-run` for `apply`.
- Created placeholder functions in `macc-core` and wired them to the CLI.
- Verified CLI functionality with `cargo run`.

Files changed:
- `cli/src/main.rs`
- `core/src/lib.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Defining subcommands in an enum with the `Subcommand` derive macro makes argument parsing very structured and easy to maintain. Global flags defined in the main `Parser` struct are automatically available across the CLI.
- Gotchas encountered: Formatting errors during `make check` (cargo fmt) need to be resolved manually or via `make fmt`.
- Useful context: `macc-core` handles the actual logic, while `macc-cli` is responsible for parsing and delegation.
---
## 2026-01-29 16:35:39Z - L1-CLI-002
Run: gemini-20260129T163539Z
- Added `thiserror` dependency to `macc-core`.
- Defined `MaccError` enum in `macc-core` with variants for `Validation`, `UserScope`, and `Io`.
- Updated `init`, `plan`, and `apply` functions in `macc-core` to return `Result<()> `.
- Implemented `get_exit_code` in `macc-cli` to map `MaccError` variants to exit codes (1, 2, 3).
- Updated `macc-cli` to handle errors and exit with appropriate codes.
- Added unit tests in `macc-cli` to verify exit code mapping.
- Verified with `make check`.

Files changed:
- `core/Cargo.toml`
- `core/src/lib.rs`
- `cli/src/main.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Using a dedicated `get_exit_code` helper function allows for testing exit code logic without terminating the test process.
- Gotchas encountered: `make check` includes `cargo fmt --check`, so ensure code is formatted with `make fmt` before checking.
- Useful context: `macc-core` error types are mapped to deterministic exit codes in `macc-cli` for better automation support.
---

## 2026-01-29 17:52:58Z - L1-PATH-001
Run: gemini-20260129T175258Z
- Defined `ProjectPaths` struct in `macc-core` to manage project-related paths.
- Implemented `find_project_root` to discover project root by walking up from CWD/--cwd searching for `.macc/macc.yaml`.
- Added `ProjectRootNotFound` variant to `MaccError` and mapped it to exit code 4 in CLI.
- Updated `init`, `plan`, and `apply` core functions to accept `&ProjectPaths`.
- Integrated root discovery into CLI subcommands, ensuring consistent project root resolution.
- Added unit tests in `macc-core` to verify discovery logic and error handling.
- Verified all quality checks with `make check`.

Files changed:
- `core/src/lib.rs`
- `cli/src/main.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Centralizing path management in a `ProjectPaths` struct simplifies core logic and ensures all components agree on the canonical directory structure.
- Gotchas encountered: Handling relative paths in `find_project_root` required joining with `std::env::current_dir()` to ensure correct upward traversal. Symlinks in temp directories (e.g. on macOS) make `canonicalize()` necessary for reliable path comparisons in tests.
- Useful context: `init` uses a fallback to the starting directory if no project root is found, allowing bootstrap of new projects. `plan` and `apply` strictly require an existing project root.
---

## 2026-01-29 18:05:00Z - L1-PATH-002
Run: gemini-20260129T180500Z
- Resolved `--cwd` to an absolute and canonicalized path in the CLI.
- Updated `macc_core` to ensure `ProjectPaths` uses absolute paths.
- Implemented minimal `init` logic to create `.macc/macc.yaml` for path verification.
- Added an integration test `test_cwd_support` in `macc-cli` to verify `--cwd` functionality and file creation.
- Verified with `make check`.

Files changed:
- `cli/src/main.rs`
- `core/src/lib.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Resolving paths to absolute/canonical forms early in the CLI prevents ambiguity in core logic and ensures that filesystem operations are always correctly anchored.
- Gotchas encountered: `canonicalize()` requires the target to exist; for `init` commands where the directory might be new, a fallback to absolute path joining is necessary.
- Useful context: The `init` command now creates the `.macc` directory and a dummy `macc.yaml`, which provides a foundation for the full config implementation in later tasks.
---

## 2026-01-29 18:20:00Z - L1-PATH-002
Run: gemini-20260129T180500Z
- Resolved `--cwd` to an absolute and canonicalized path in the CLI.
- Updated `macc_core` to ensure `ProjectPaths` uses absolute paths.
- Implemented minimal `init` logic to create `.macc/macc.yaml` for path verification.
- Added an integration test `test_cwd_support` in `macc-cli` to verify `--cwd` functionality and file creation.
- Verified with `make check`.

Files changed:
- `cli/src/main.rs`
- `core/src/lib.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Resolving paths to absolute/canonical forms early in the CLI prevents ambiguity in core logic and ensures that filesystem operations are always correctly anchored.
- Gotchas encountered: `canonicalize()` requires the target to exist; for `init` commands where the directory might be new, a fallback to absolute path joining is necessary.
- Useful context: The `init` command now creates the `.macc` directory and a dummy `macc.yaml`, which provides a foundation for the full config implementation in later tasks.
---

## 2026-01-29 18:20:00Z - L1-CONFIG-001
Run: gemini-20260129T182000Z
- Added `serde` and `serde_yaml` dependencies to `macc-core`.
- Implemented `CanonicalConfig` struct in `core/src/config/mod.rs` with support for `version`, `tools`, `standards`, and `selections`.
- Added unit tests for YAML roundtrip and inline standards support.
- Updated `macc init` to generate a default `macc.yaml` using the new configuration model.
- Verified with `make check`.

Files changed:
- `core/Cargo.toml`
- `core/src/lib.rs`
- `core/src/config/mod.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Using `#[serde(flatten)]` for the `standards` field allows for both a `path` pointer and arbitrary inline key-value pairs in the same YAML block, providing flexibility for future standards definitions.
- Gotchas encountered: `serde_yaml` is deprecated in favor of `serde_yml`, but `0.9` is still widely used and stable for M0.
- Useful context: The `CanonicalConfig` acts as the tool-agnostic source of truth that will later be consumed by tool adapters (Claude, Gemini, Codex) to generate their respective configuration files.
## 2026-01-30 11:15:00Z - L2-RS-007
Run: gemini-gemini-3-flash-preview-20260130T100717Z
- Created `url_parsing` module in `macc-adapter-shared`.
- Implemented `normalize_git_input` to handle GitHub tree URLs and plain repository URLs.
- Automatically extracts repository URL, reference (branch/tag/sha), and subpath from GitHub tree links.
- Added `regex` dependency to `macc-adapter-shared`.
- Added comprehensive unit tests for various GitHub URL formats, including nested subpaths and trailing slashes.
- Verified all workspace tests pass.

Files changed:
- adapters/shared/Cargo.toml
- adapters/shared/src/lib.rs
- adapters/shared/src/url_parsing.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using `OnceLock<Regex>` for compiled regular expressions provides optimal performance for repeated parsing operations within a single process.
- Gotchas encountered: Remember to derive `Clone` for small data transfer objects (like `NormalizedGit`) to facilitate easier testing and data handling.
- Useful context: Trimming trailing slashes from subpaths ensures consistent behavior when joining paths later in the materialization pipeline.
---
## 2026-01-30 11:25:00Z - L2-RS-008
Run: gemini-gemini-3-flash-preview-20260130T100717Z
- Implemented `validate_http_url` to enforce http/https schemes for remote zip sources.
- Implemented `validate_checksum` to enforce the `sha256:<64-hex>` format.
- Added these validation helpers to the `url_parsing` module in `macc-adapter-shared`.
- Added unit tests for both valid and invalid HTTP URLs and checksum strings.
- Verified all workspace tests pass.

Files changed:
- adapters/shared/src/lib.rs
- adapters/shared/src/url_parsing.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Centralizing all URL and source validation logic in a single module (`url_parsing`) ensures that both the CLI and core materialize logic can share the same validation rules.
- Gotchas encountered: Remember to handle case-insensitivity for schemes (HTTP vs http) and hex characters in checksums.
- Useful context: RFC3339 timestamps and sha256 checksums are the standard for integrity and tracking in this project.
---
## 2026-01-30 11:45:00Z - L2-RS-009
Run: gemini-gemini-3-flash-preview-20260130T100717Z
- Implemented deterministic cache key calculation for `Source` using SHA-256 of its core properties.
- Added `cache_dir` and `source_cache_path` helper to `ProjectPaths` in `macc-core`.
- Added `sha2` dependency to `macc-adapter-shared`.
- Verified that `.macc/cache/` is already covered by baseline `.gitignore` entries.
- Added unit tests for stable cache keys and correct path resolution.
- Verified all workspace tests pass.

Files changed:
- adapters/shared/Cargo.toml
- adapters/shared/src/catalog.rs
- core/src/lib.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using a canonical string format (`kind|url|ref|checksum`) before hashing ensures that identical logical sources always yield the same cache key across different environments.
- Gotchas encountered: Ensure that `checksum` is handled gracefully when it's `None` to avoid crashes or non-deterministic keys.
- Useful context: `ProjectPaths` is becoming the central hub for all project-relative locations, making it easy to reason about the project structure.
---
## 2026-01-29 18:40:00Z - L1-CONFIG-002
Run: gemini-20260129T184000Z
- Implemented `load_canonical_config` in `macc-core` with strict YAML parsing.
- Added `#[serde(deny_unknown_fields)]` to `CanonicalConfig` and sub-structs to catch typos.
- Added `MaccError::Config` variant to provide path context and readable error messages from `serde_yaml`.
- Updated CLI to map `Config` errors to exit code 5.
- Added comprehensive unit tests for unknown fields, invalid syntax, and missing required fields.
- Verified all quality checks with `make check`.

Files changed:
- `core/src/lib.rs`
- `core/src/config/mod.rs`
- `cli/src/main.rs`
- `cli/Cargo.toml`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Using `#[serde(deny_unknown_fields)]` combined with `#[serde(flatten)]` on specific sub-fields allows for a balance between strictness for known schema parts and flexibility for arbitrary metadata.
- Gotchas encountered: `serde_yaml` errors include line and column information by default, which is passed through via our custom `Config` error variant.
- Useful context: Exit code 5 is now dedicated to configuration parsing/validation errors, allowing automation to distinguish between IO issues and schema violations.

## 2026-01-29 18:55:00Z - L1-CONFIG-003
Run: gemini-20260129T185500Z
- Updated `macc_core::init` to create `.macc/backups/` and `.macc/tmp/` directories.
- Implemented `macc init` idempotence and `--force` flag to overwrite existing `macc.yaml`.
- Added integration tests in `macc-cli` to verify directory creation, idempotence, and `--force` behavior.
- Verified all quality checks with `make check`.

Files changed:
- `core/src/lib.rs`
- `cli/src/main.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Centralizing directory creation in the `init` function ensures that the project structure is consistent across all new projects.
- Gotchas encountered: `std::fs::create_dir_all` is already idempotent for existing directories, but explicit checks help in providing better error context if a file exists with the same name.
- Useful context: The `init` command now ensures that all required M0 subdirectories are present, which simplifies later tasks that depend on these folders (like backups and atomic writes).
---

## 2026-01-29 19:15:00Z - L1-RESOLVE-001
Run: gemini-20260129T191500Z
- Defined `ResolvedConfig` and sub-structs in `core/src/resolve/mod.rs`.
- Implemented `resolve` function to transform `CanonicalConfig` into a normalized `ResolvedConfig`.
- Added support for `CliOverrides` to allow overriding enabled tools from the CLI.
- Implemented deterministic normalization:
    - Sorted and de-duplicated tool, skill, and agent lists.
    - Used `BTreeMap` for inline standards to ensure stable key ordering.
    - Added default 'English' language to standards if not present.
- Added comprehensive unit tests for normalization, overrides, and serialization determinism.
- Verified with `make check`.

Files changed:
- `core/src/lib.rs`
- `core/src/resolve/mod.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Using `BTreeMap` in the "Resolved" layer instead of `HashMap` ensures that any serialization (e.g., for debugging or state persistence) is byte-for-byte identical for the same logical configuration.
- Gotchas encountered: `serde`'s default behavior for `HashMap` is not deterministic across runs (due to hash seeding), so explicit sorting or stable map types are necessary for "diff friendliness".
- Useful context: The `resolve` layer acts as the boundary between user-provided (possibly messy) configuration and the internal logic that expects clean, validated, and normalized data.
---

## 2026-01-29 19:35:00Z - L1-RESOLVE-002
Run: gemini-20260129T193500Z
- Implemented `CliOverrides::from_tools_csv` in `macc-core` to parse and validate comma-separated tool lists.
- Added `KNOWN_TOOLS` registry placeholder in `resolve` module to validate tool IDs (`claude`, `gemini`, `codex`).
- Updated `plan` and `apply` in `macc-core` to load configuration and apply CLI overrides during resolution.
- Added unit tests for CSV parsing, whitespace trimming, and validation of unknown tools.
- Added integration test in `macc-cli` to verify `--tools` flag behavior and error reporting.
- Verified all quality checks with `make check`.

Files changed:
- `core/src/lib.rs`
- `core/src/resolve/mod.rs`
- `cli/src/main.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Centralizing CLI-to-Core data transformation (like CSV parsing) in the `resolve` module keeps the CLI layer thin and ensures validation logic is reusable.
- Gotchas encountered: Ensure that `resolve` actually replaces the enabled tools instead of appending to them when an override is provided, as per the "override" semantics.
- Useful context: The `KNOWN_TOOLS` constant acts as a temporary registry until a more dynamic tool discovery mechanism is implemented in later lots.
---

## 2026-01-29 19:55:00Z - L1-TOOLAPI-001
Run: gemini-20260129T195500Z
- Defined `ToolAdapter` trait with `id()` and `plan()` methods.
- Implemented `ToolRegistry` for dynamic (though currently static for M0) adapter discovery.
- Added a minimal `ActionPlan` and `Action` enum in `core/src/plan/mod.rs`.
- Implemented `TestAdapter` and registered it in the default registry.
- Updated `macc_core::plan` to iterate over enabled tools and invoke their adapters.
- Verified that `macc plan --tools test` correctly identifies the test adapter and reports planned actions.

Files changed:
- `core/src/lib.rs`
- `core/src/plan/mod.rs`
- `core/src/tool/mod.rs`
- `core/src/resolve/mod.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: The adapter-driven architecture allows for easy mocking and testing of the planning phase without requiring actual LLM interactions.
- Gotchas encountered: `ActionPlan` was needed earlier than expected to satisfy the `ToolAdapter` interface, so a minimal placeholder was created.
- Useful context: `KNOWN_TOOLS` in the `resolve` module acts as a gatekeeper for configuration, while `ToolRegistry` handles the actual implementation mapping.
---

## 2026-01-29 18:16:32Z - L1-TOOLAPI-002
Run: gemini-20260129T181632Z
- Added `WriteFile` action to `Action` enum in `core/src/plan/mod.rs`.
- Implemented `TestAdapter` in `core/src/tool/mod.rs` to generate deterministic `WriteFile` actions.
- Implemented basic `apply` engine in `core/src/lib.rs` to execute `WriteFile` actions.
- Updated `plan` in `core/src/lib.rs` to display planned actions.
- Added integration test in `cli/src/main.rs` to verify `macc apply --tools test` end-to-end.
- Verified all quality checks with `cargo test`.

Files changed:
- `core/src/plan/mod.rs`
- `core/src/tool/mod.rs`
- `core/src/lib.rs`
- `cli/src/main.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Implementing a minimal execution engine during the "adapter" phase allows for early end-to-end verification, even if the full "apply" logic (backups, atomic writes) is slated for later tasks.
- Gotchas encountered: Ensure that `apply` creates parent directories before writing files, especially for files inside `.macc/.`.
- Useful context: The `TestAdapter` serves as the primary verification tool for Lot 0, allowing testing of all downstream components (diffs, backups, etc.) without external dependencies.
---

## 2026-01-29 20:10:00Z - L1-PLAN-001
Run: gemini-20260129T201000Z
- Defined `Scope` enum (Project, User) in `core/src/plan/mod.rs`.
- Expanded `Action` enum with `Mkdir`, `BackupFile`, `WriteFile`, `EnsureGitignore`, and `Noop` variants.
- Implemented `scope` field for each `Action` variant and enforced `Scope::Project` in `ActionPlan::add_action`.
- Implemented deterministic sorting and normalization for `ActionPlan`.
- Updated `ToolAdapter` and `TestAdapter` to support the new action structure.
- Updated `plan` and `apply` engines in `core/src/lib.rs` to handle the expanded `Action` enum.
- Verified deterministic ordering and project-only enforcement with unit tests.

Files changed:
- `core/src/lib.rs`
- `core/src/plan/mod.rs`
- `core/src/tool/mod.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Using Rust's `Ord` derive on enums provides a simple way to define execution priority by ordering the variants.
- Gotchas encountered: Enum variant sorting depends on the order of definition; reordering variants changes the `Ord` behavior.
- Useful context: `ActionPlan::normalize` should be called before execution (apply) to ensure deterministic and safe operation (e.g., creating directories before writing files).
---

## 2026-01-29 20:30:00Z - L1-PLAN-002
Run: gemini-20260129T203000Z
- Implemented `ActionPlanBuilder` with methods: `mkdir`, `write_text`, `write_bytes`, `ensure_gitignore_entry`.
- Implemented path validation in `ActionPlanBuilder`: rejects absolute paths and parent traversal (`..`) for project scope.
- Updated `Action::WriteFile` to use `Vec<u8>` for content to support binary data.
- Updated `plan` and `apply` engines to handle `Vec<u8>` content and provide text previews.
- Added unit tests for path validation and builder helpers.
- Verified all tests pass with `cargo test`.

Files changed:
- `core/src/lib.rs`
- `core/src/plan/mod.rs`
- `core/src/tool/mod.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Using a builder for `ActionPlan` centralizes safety checks and reduces boilerplate for adapters.
- Gotchas encountered: `ActionPlanBuilder` methods returning `&mut Self` require `build` to take `&mut self` (and use `std::mem::take`) to support easy chaining in Rust without ownership issues.
- Useful context: `Action::WriteFile` now uses `Vec<u8>`, so any tool generating it must convert strings to bytes using `.as_bytes().to_vec()`.


## 2026-01-29 20:45:00Z - L1-DIFF-001
Run: gemini-20260129T204500Z
- Created `core/src/plan/diff.rs` with `read_existing` function and `ExistingFile` struct.
- Implemented `is_text` heuristic to distinguish between text and binary files.
- Registered `diff` module in `core/src/plan/mod.rs` and exported its items.
- Added comprehensive unit tests for missing files, text files, binary files, and non-UTF8 files.
- Verified all tests pass with `cargo test`.

Files changed:
- `core/src/plan/mod.rs`
- `core/src/plan/diff.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Splitting plan-related utilities into submodules (like `diff`) keeps `plan/mod.rs` manageable as the project grows.
- Gotchas encountered: When testing filesystem operations, using `std::env::temp_dir()` and `uuid`-like suffixes for file names avoids collisions in parallel test execution without requiring external crates like `tempfile`.
- Useful context: `read_existing` returns `ExistingFile` which explicitly tracks existence and a "text guess", facilitating future diffing logic.
---

## 2026-01-29 20:55:00Z - L1-DIFF-002
Run: gemini-20260129T205500Z
- Added `similar` crate to `core/Cargo.toml` for unified diff support.
- Implemented `generate_unified_diff` and `is_text_file` in `core/src/plan/diff.rs`.
- Enhanced file type detection with extension-based rules (.md, .txt, .rules, .toml, etc.).
- Updated `plan` engine in `core/src/lib.rs` to display indented unified diffs for text file changes.
- Verified with unit tests and manual integration test using the `test` adapter.

Files changed:
- `core/Cargo.toml`
- `core/src/lib.rs`
- `core/src/plan/mod.rs`
- `core/src/plan/diff.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Using the `similar` crate provides a robust and standard way to generate unified diffs without reinventing the wheel. Indenting the diff output in the CLI helps distinguish it from other log messages.
- Gotchas encountered: The `similar` crate's `UnifiedDiff::header` method prepends `--- ` and `+++ ` automatically, so passing pre-formatted headers leads to double prefixes.
- Useful context: Extension-based file type detection is a useful shortcut that complements content-based heuristics, especially for empty or very short files where UTF-8 detection might be ambiguous.
---

## 2026-01-29 21:05:00Z - L1-DIFF-003
Run: gemini-20260129T210500Z
- Added `serde_json` to `macc-core`.
- Implemented `normalize_json` in `core/src/plan/diff.rs` to provide stable, pretty-printed JSON formatting.
- Integrated JSON normalization into `generate_unified_diff` for `.json` files, enabling key-reordering-invariant diffs.
- Added unit tests for stable JSON diffs, semantic changes, and invalid JSON fallback.
- Verified all tests pass with `cargo test`.

Files changed:
- `core/Cargo.toml`
- `core/src/plan/diff.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Normalizing structured formats (like JSON) before diffing significantly reduces noise caused by formatting-only changes (like key reordering or indentation).
- Gotchas encountered: `serde_json::Value` uses `BTreeMap` by default, which provides the stable key ordering we need for deterministic diffs without extra effort.
- Useful context: Ensuring normalized output ends with a newline helps maintain consistency with standard text file diff expectations.
---

## 2026-01-29 21:15:00Z - L1-DIFF-004
Run: gemini-20260129T211500Z
- Implemented `render_summary` for `ActionPlan` providing a concise table of planned changes.
- Added `ActionStatus` enum and `compute_write_status` to handle created/updated/unchanged states.
- Integrated JSON normalization into status computation to ensure semantic equality for JSON files.
- Updated `ActionPlan` to retain User scope actions, and `apply` engine to refuse them in M0.
- Added unit tests for summary rendering and User scope retention.
- Verified all tests pass and code is clippy-clean.

Files changed:
- `core/src/lib.rs`
- `core/src/plan/mod.rs`
- `core/src/plan/diff.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Adding helper methods like `path()` and `scope()` to the `Action` enum simplifies logic that needs to operate on generic actions without exhaustive pattern matching every time.
- Gotchas encountered: Clippy's `collapsible_else_if` can be triggered when using nested `if` inside an `else` block, even if it feels clearer to keep them separate for symmetry.
- Useful context: Sorting for the summary (by path) might differ from sorting for execution (by action type), so it's often better to sort a clone of the actions for display.
---

## 2026-01-29 21:20:00Z - L1-APPLY-001
Run: gemini-20260129T212000Z
- Added `chrono` crate for timestamp formatting.
- Implemented `create_timestamped_backup` helper in `core/src/lib.rs`.
- Updated `apply` engine to perform timestamped backups before overwriting existing project files.
- Added idempotence check: only backup and write if the content actually changed.
- Reported the backup root directory in the CLI output if any backups were created.
- Added integration tests for simple backups, nested file backups, and idempotence.
- Verified all tests pass with `cargo test`.

Files changed:
- `core/Cargo.toml`
- `core/src/lib.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Using a single timestamp per `apply` run ensures all backups from that operation are grouped together in `.macc/backups/<timestamp>/`.
- Gotchas encountered: Ensure that `create_dir_all` is called for the parent directory of the backup path, not just the backup root, to support nested files.
- Useful context: Comparing existing file bytes with new content before performing the backup avoids unnecessary file churn and disk usage.
---

## 2026-01-29 21:25:00Z - L1-APPLY-002
Run: gemini-20260129T212500Z
- Implemented `atomic_write` helper in `core/src/lib.rs` using a tmp+rename strategy.
- Ensured `.macc/tmp/` directory is used for intermediate files with timestamp/nano-precision names for best-effort uniqueness.
- Integrated `atomic_write` into both `apply` and `init` engines for improved crash resilience.
- Added automatic parent directory creation within `atomic_write`.
- Added unit tests for `atomic_write` verifying correct content writing and temp file cleanup.
- Verified all tests pass and code is clippy-clean.

Files changed:
- `core/src/lib.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Implementing atomic writes at a low-level helper function simplifies higher-level logic (like `apply` and `init`) by centralizing safety checks like parent directory creation.
- Gotchas encountered: Clippy's `collapsible_if` is a common lint when combining existence checks with subsequent conditional operations; merging them into `if exists && condition` is more idiomatic.
- Useful context: `std::fs::rename` is atomic on most modern filesystems when the source and destination are on the same volume, making `.macc/tmp` a safe choice for project-relative atomic writes.
---

## 2026-01-29 21:30:00Z - L1-APPLY-003
Run: gemini-20260129T213000Z
- Integrated `compute_write_status` into the `apply` engine to ensure semantic-aware idempotence.
- Updated `apply` to skip both file writes and backups when content is unchanged (including JSON normalization).
- Added an integration test `test_apply_idempotence_normalization` verifying that multiple `apply` runs don't touch files if semantic content is identical.
- Verified that file modification times remain unchanged on idempotent `apply` runs.

Files changed:
- `core/src/lib.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Reusing normalization logic between preview (diff) and apply (write check) is essential for consistent "what you see is what you get" behavior.
- Gotchas encountered: When testing idempotence via file modification times, a small `sleep` might be needed to ensure that if a write *did* happen, the timestamp would actually be different.
- Useful context: `std::fs::Metadata::modified()` is a reliable way to check if a file was touched by a write operation, even if the content ended up being the same.

## 2026-01-29 21:40:00Z - L1-APPLY-004
Run: gemini-20260129T214000Z
- Implemented `apply_plan(paths, plan)` to centralize and order action execution.
- Added `apply_ensure_gitignore` to handle `EnsureGitignore` actions with backup and atomic write.
- Updated `apply` engine to aggregate actions from all tool adapters into a single plan before execution.
- Ensured deterministic execution order: `Mkdir` -> `BackupFile` -> `WriteFile` -> `EnsureGitignore` -> `Noop`.
- Verified that `Mkdir` actions are deduplicated and run before file writes.
- Added integration test `test_apply_plan_ordering` verifying correct ordering and `.gitignore` updates.
- Verified all tests pass and code is clippy-clean.

Files changed:
- `core/src/lib.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Collecting all actions into a single `ActionPlan` and then calling `normalize()` ensures global consistency and deduplication across different tool adapters.
- Gotchas encountered: Clippy's `collapsible_if` applies even when the inner `if` has a complex condition involving `?` operator.
- Useful context: Sorting the plan based on the `Action` enum variant order naturally implements the required execution strategy (directories before files, etc.).
---

## 2026-01-29 21:50:00Z - L1-APPLY-005
Run: gemini-20260129T215000Z
- Defined `ApplyReport` struct in `core/src/lib.rs` to collect and render apply outcomes.
- Updated `apply` and `apply_plan` to return `ApplyReport` containing a map of per-file statuses.
- Integrated outcome collection for `Mkdir`, `WriteFile`, and `EnsureGitignore` actions.
- Added `ApplyReport::render_cli()` to provide a sorted, human-readable summary of changes and backup path.
- Updated CLI to print the report after a successful apply.
- Verified with updated tests and manual verification of CLI output format.

Files changed:
- `core/src/lib.rs`
- `cli/src/main.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Using a `BTreeMap` for outcomes ensures the CLI report is always sorted by path, providing stable and predictable output.
- Gotchas encountered: When changing the return type of a public function, all call sites (including tests and other crates) must be updated simultaneously.
- Useful context: Reusing `ActionStatus` for the report ensures consistency between the `plan` preview and the final `apply` summary.
---

## 2026-01-29 22:05:00Z - L1-GIT-001
Run: gemini-20260129T220500Z
- Defined `BASELINE_IGNORE_ENTRIES` including `.macc/tmp/` and `.macc/backups/`.
- Implemented `ensure_gitignore_entries` for idempotent management of `.gitignore`.
- Integrated baseline entries into `init`, `plan`, and `apply` workflows.
- Refactored `apply_ensure_gitignore` to leverage the new idempotent helper.
- Added integration tests for `.gitignore` creation, updating, and backup behavior.
- Verified that `macc plan` correctly shows baseline gitignore actions.

Files changed:
- `core/src/lib.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Injecting baseline actions directly into the `total_plan` in `plan()` and `apply()` ensures that core requirements (like ignoring temp files) are always visible and enforced through the standard action pipeline.
- Gotchas encountered: When `init` creates a file that was previously assumed to be created by `apply`, existing tests that check for `Created` status might need to be updated to expect `Updated` or `Unchanged`.
- Useful context: `.gitignore` entries should ideally end with a newline to avoid issues when the user manually appends to the file.
---

## 2026-01-29 22:35:00Z - L1-SEC-001
Run: gemini-20260129T223500Z
- Implemented `security/secret_scan.rs` with high-confidence regex patterns (AWS keys, generic tokens).
- Integrated secret scanning into `plan()` and `apply()` workflows to prevent writing secrets.
- Implemented redaction for findings to avoid leaking matched secrets in logs or error messages.
- Added `SecretDetected` error variant to `MaccError` and mapped it to exit code 6 in CLI.
- Verified with unit tests for detection/redaction and integration test for aborted apply.

Files changed:
- `core/Cargo.toml`
- `core/src/lib.rs`
- `core/src/security/mod.rs`
- `core/src/security/secret_scan.rs`
- `cli/src/main.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Using `OnceLock` for compiled regexes ensures performance without sacrificing code clarity in module-level constants.
- Gotchas encountered: Adding an error variant to a publicly matched enum (like `MaccError` in the CLI) requires updating all match arms to maintain exhaustive checks.
- Useful context: Redaction should be UTF-8 aware to avoid splitting multi-byte characters when showing the first/last parts of a secret.
---


## 2026-01-29 23:10:00Z - L1-SEC-002
Run: gemini-20260129T231000Z
- Defined standard placeholder patterns (`YOUR_API_KEY_HERE`, `${ENV_VAR}`, etc.) and `contains_placeholder` validator in `core/src/security/mod.rs`.
- Updated `TestAdapter` to generate files using standard placeholders (`.env.example`, `.macc/test-output.json`).
- Implemented `is_sensitive_file` helper in `core/src/lib.rs` to identify files that should contain placeholders.
- Integrated placeholder validation into `plan` and `apply` workflows to warn when sensitive-looking files lack placeholders.
- Documented the placeholder policy in `README.md`.
- Verified with unit tests for placeholder detection and sensitive file identification.

Files changed:
- `core/src/lib.rs`
- `core/src/security/mod.rs`
- `core/src/tool/mod.rs`
- `README.md`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Using a centralized list of placeholder patterns allows for consistent enforcement across different adapters while keeping the validation logic simple.
- Gotchas encountered: Ensure that tests for tool adapters are updated when their planned actions change (e.g., adding new files for demonstration).
- Useful context: `is_sensitive_file` uses a simple keyword-based approach which is sufficient for M0 but might need refinement as more tools are added.

## 2026-01-29 23:45:00Z - L1-USER-001
Run: gemini-20260129T234500Z
- Renamed `MaccError::UserScope` to `MaccError::UserScopeNotAllowed` and updated error message.
- Implemented `Scope::User` check in `apply_plan` to abort with `UserScopeNotAllowed`.
- Updated `plan` rendering and manual action prints to indicate `REFUSED` for user-scope actions.
- Updated `plan()` to fail with `UserScopeNotAllowed` if any user-scope actions are present.
- Verified that `UserScopeNotAllowed` correctly maps to exit code 2 in CLI.
- Added unit tests for user-scope refusal in `core` and verified CLI exit code mapping.

Files changed:
- `core/src/lib.rs`
- `core/src/plan/mod.rs`
- `cli/src/main.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Centralizing exit code mapping in the CLI crate ensures that core logic remains independent of process management while still providing the required user interface.
- Gotchas encountered: Multi-line string replacements can be brittle if they include placeholders or comments that might be accidentally matched or replaced with literal placeholders like `// ...`.
- Useful context: In M0, the policy is to refuse all user-scope actions to ensure safety, even if a flag were provided (no flag implemented yet).

## 2026-01-29 23:55:00Z - L1-USER-001
Run: gemini-20260129T235500Z
- Implemented core entrypoints `build_plan(paths, resolved)` and `preview_plan(plan, paths)` in `core/src/lib.rs`.
- Refactored `plan` and `apply` functions to use `build_plan`.
- Wired `macc plan` CLI command to the new workflow.
- Verified that `macc plan` displays summary and diffs without modifying files or creating backups.
- Added integration test `test_plan_no_backups_or_writes` in `core/src/lib.rs`.

Files changed:
- `core/src/lib.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Splitting the pipeline into `build_plan` (logic) and `preview_plan` (UI/reporting) allows for easier integration into different frontends (CLI, TUI, etc.) and keeps core logic testable.
- Gotchas encountered: `ActionPlan::render_summary` uses `action.path()` which may not be unique for all action types (e.g., multiple `EnsureGitignore` actions point to `.gitignore`), causing duplicate entries in the summary table.
- Useful context: `build_plan` needs `ProjectPaths` because `ToolAdapter::plan` requires it to resolve relative paths and check for existing files if needed.
---

## 2026-01-29 23:59:00Z - L1-E2E-002
Run: gemini-20260129T235900Z
- Implemented `validate_plan(plan)` in `core/src/lib.rs` to centralize secret scanning and scope checks.
- Refactored `apply` and `apply_plan` to use `validate_plan` for pre-flight safety before any writes.
- Improved `ApplyReport` logic for `.gitignore` to correctly track cumulative status across multiple ensure actions.
- Verified end-to-end flow with `macc apply --tools test`, including backups and idempotence.
- Updated `prd.json` to reflect completion of E2E and related test tasks.

Files changed:
- `core/src/lib.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Using a separate `validate_plan` function allows for consistent safety checks across both `plan` (preview) and `apply` (execution) workflows.
- Gotchas encountered: If an action-processing loop has side effects, pre-validating the entire plan is essential to prevent partial applies when an error occurs halfway through.
- Useful context: `ApplyReport` needs special handling for paths that might be targeted by multiple actions (like `.gitignore`) to ensure the final reported status is accurate.

## 2026-01-29 00:10:00Z - L1-TEST-001
Run: gemini-gemini-3-flash-preview-20260129T194115Z
- Enhanced `find_project_root` tests in `core/src/lib.rs` with deeper nesting and file-based discovery.
- Added `test_project_paths_anchoring` to verify `ProjectPaths` correctly calculates absolute paths for internal directories.
- Expanded `test_builder_path_validation` in `core/src/plan/mod.rs` to cover absolute paths, parent directory traversal (`..`), and path normalization.
- Verified that all core unit tests pass.

Files changed:
- `core/src/lib.rs`
- `core/src/plan/mod.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Unit tests using temporary directories with mock `.macc` trees are effective for verifying discovery logic without relying on the actual environment.
- Gotchas encountered: Ensure that `ActionPlanBuilder` is tested with both `Scope::Project` (strict) and `Scope::User` (lax) to verify that safety rules are correctly applied only where intended.
- Useful context: `find_project_root` works starting from a file because it searches for `.macc/macc.yaml` relative to the input path, and if not found, moves to the parent directory.

## 2026-01-29 00:20:00Z - L1-TEST-002
Run: gemini-gemini-3-flash-preview-20260129T194253Z
- Added unit tests for configuration parsing and validation errors in `core/src/config/mod.rs`.
- Verified that error messages include the file path and relevant context for invalid YAML, unknown fields, and missing required fields.
- Implemented `uuid_v4_like` helper for unique temporary directory names in tests.
- Verified that all core tests pass and clippy/fmt are satisfied.

Files changed:
- `core/src/config/mod.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Wrapping third-party library errors (like `serde_yaml::Error`) with custom error types that include context like file paths is essential for producing user-friendly CLI error messages.
- Gotchas encountered: Using fixed filenames for temporary files in tests can lead to flakes if tests run in parallel; generating unique directory names per test run is safer.
- Useful context: `#[serde(deny_unknown_fields)]` is a powerful way to catch configuration typos early, and testing it ensures that the project remains strict about its configuration schema.

## 2026-01-29 00:30:00Z - L1-TEST-003
Run: gemini-gemini-3-flash-preview-20260129T194420Z
- Enhanced secret scanning unit tests in `core/src/security/secret_scan.rs`.
- Added GitHub token pattern (`ghp_...`) and corresponding tests.
- Verified that all secret findings use redaction and do not include the original full token.
- Verified that `validate_plan` correctly catch secrets and redacts them in the error message.
- All 48 core tests passed.

Files changed:
- `core/src/security/secret_scan.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Using negative assertions (`assert!(!redacted.contains(original))`) is crucial for verifying that sensitive data is truly excluded from logs/errors.
- Gotchas encountered: Redaction logic based on character indices can be tricky when counting manually in tests; always verify string length and expected slice boundaries.
- Useful context: The `redact` function in `secret_scan.rs` takes 4 chars from each end if the string is long enough, otherwise it returns `****`.
## 2026-01-29 21:00:00Z - L1-TEST-004
Run: gemini-20260129T210000Z
- Implemented integration test for `macc init` in `core/tests/init_integration.rs`.
- Verified that `init` creates `.macc/macc.yaml`, `.macc/backups/`, and `.macc/tmp/`.
- Confirmed that `init` is idempotent and does not overwrite existing config unless `--force` is used.
- Verified that default canonical config contains expected version and enabled tools.
- All workspace tests passed, including the new integration test.

Files changed:
- `core/tests/init_integration.rs`
- `scripts/Ralph/prd.json`
- `scripts/Ralph/progress.md`

**Learnings for future iterations:**
- Patterns discovered: Using `cargo test --test <name>` is a clean way to run specific integration tests located in `tests/` directory.
- Gotchas encountered: Remember to sleep briefly between file writes when testing idempotence based on modification times to avoid false negatives due to filesystem resolution.
- Useful context: `ProjectPaths::from_root` is useful for bootstrapping paths even when the root directory doesn't exist yet, which is critical for `init`.

## 2026-01-29 22:00:00Z - L1-TEST-005
Run: gemini-gemini-3-flash-preview-20260129T215100Z
- Implemented comprehensive integration test for the full plan/apply pipeline in core/tests/plan_apply_integration.rs.
- Verified that macc plan is side-effect free (no writes, no backups).
- Verified that macc apply correctly creates files from TestAdapter.
- Confirmed that modifying a generated file and re-applying creates a timestamped backup of the user's modifications.
- Verified idempotence: a third apply with no changes results in Unchanged statuses and no new backups.
- Ensured all tests in the workspace pass and code is formatted.

Files changed:
- core/tests/plan_apply_integration.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Integration tests that simulate a full project lifecycle (init -> plan -> apply -> modify -> apply) are excellent for catching regression in the apply engine's safety logic.
- Gotchas encountered: When testing backup creation, ensure enough time passes (e.g., 1s) if the backup directory naming relies on second-precision timestamps to avoid collision or ambiguity in tests.
- Useful context: ApplyReport provides a structured way to verify the outcome of each action in the plan, making it easy to assert on Created, Updated, or Unchanged statuses.

## 2026-01-29 23:00:00Z - L1-DOC-001
Run: gemini-gemini-3-flash-preview-20260129T195224Z
- Refined README.md with comprehensive M0 usage documentation (init, plan, apply).
- Documented what files and directories are created during 'macc init'.
- Detailed the safety and backup mechanisms, including atomic writes via temp files and timestamped backups.
- Explicitly stated that user-level writes are refused in M0.
- Verified that all core and integration tests pass after formatting cleanup.

Files changed:
- README.md
- core/src/security/secret_scan.rs (formatting only)
- core/tests/init_integration.rs (formatting only)
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Standardizing documentation around safety and atomicity early helps set expectations for both users and future tool adapters.
- Gotchas encountered: Ensure that documentation reflects the actual implementation (e.g., mention .macc/tmp/ which is used for atomic writes).
- Useful context: M0 phase is strictly project-scoped, which simplifies the safety model significantly by avoiding complex user-level merge consent logic.
## 2026-01-30 02:20:00Z - L2-RS-001
Run: gemini-gemini-3-flash-preview-20260130T021132Z
- Implemented Rust structs for SkillsCatalog and McpCatalog in `adapters/shared/src/catalog.rs`.
- Added strict validation using `#[serde(deny_unknown_fields)]`.
- Added unit tests with JSON fixtures for both SkillEntry and McpEntry, as well as full catalog deserialization.
- Registered the new `catalog` module in `adapters/shared/src/lib.rs`.
- Fixed a pre-existing clippy type-complexity warning in `adapters/shared/src/packages.rs` by introducing a `ToolFiles` type alias.

Files changed:
- adapters/shared/src/catalog.rs
- adapters/shared/src/lib.rs
- adapters/shared/src/packages.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Defining type aliases for complex nested generic types (like `Vec<(String, Vec<u8>)>`) significantly improves code readability and satisfies clippy's type-complexity checks.
- Gotchas encountered: Serde's `#[serde(deny_unknown_fields)]` is great for strictness but requires all fields to be explicitly mapped, which is good for catalog schemas.
- Useful context: Using `#[serde(rename = "...")]` allows mapping JSON fields that are Rust keywords (like `type` and `ref`) to valid Rust identifiers.

## 2026-01-30 02:40:00Z - L2-RS-002
Run: gemini-gemini-3-flash-preview-20260130T100102Z
- Added catalog path helpers `skills_catalog_path()` and `mcp_catalog_path()` to `ProjectPaths` in `core/src/lib.rs`.
- Updated `ProjectPaths` struct and `from_root` constructor to include `catalog_dir` pointing to `.macc/catalog/`.
- Modified `init` function in `core/src/lib.rs` to ensure `.macc/catalog/` directory is created during project initialization.
- Added unit tests in `core/src/lib.rs` to verify correct path construction and directory creation.
- Verified all tests pass in `macc-core` and integration tests.

Files changed:
- core/src/lib.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Centralizing project-specific paths in the `ProjectPaths` struct ensures consistency across all core functions and tool adapters.
- Gotchas encountered: When adding new directories to the project structure, remember to update the `init` function's list of directories to create to maintain structural integrity from the start.
- Useful context: `ProjectPaths` uses `PathBuf` and joins paths starting from the project root, which is typically canonicalized in `find_project_root`, ensuring absolute paths for all internal locations.

## 2026-01-30 10:10:00Z - L2-RS-003
Run: gemini-gemini-3-flash-preview-20260130T100429Z
- Made `atomic_write` public in `macc-core` to allow reuse by other crates.
- Implemented `load` and `save_atomically` methods for `SkillsCatalog` and `McpCatalog` in `adapters/shared/src/catalog.rs`.
- Added `Default` implementations for both catalog types to support empty state when files are missing.
- Ensured deterministic pretty-printing with a trailing newline for all catalog writes.
- Added comprehensive unit tests for load/save roundtrip, missing-file defaults, and stable formatting.
- Verified that all tests in the workspace (including new catalog tests) pass.

Files changed:
- core/src/lib.rs
- adapters/shared/src/catalog.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Making low-level safety utilities like `atomic_write` public allows higher-level modules (like catalogs) to maintain strict safety guarantees without duplicating complex logic.
- Gotchas encountered: When implementing `load` methods, it's important to return a `Default` instance rather than an error when the file is missing to support smooth first-run experiences.
- Useful context: Using `serde_json::to_string_pretty` combined with an explicit trailing newline ensures that catalog files are human-readable and play nicely with version control and line-based diff tools.

## 2026-01-30 10:25:00Z - L2-RS-004
Run: gemini-gemini-3-flash-preview-20260130T100717Z
- Implemented `upsert_skill_entry` and `delete_skill_entry` for `SkillsCatalog`.
- Implemented `upsert_mcp_entry` and `delete_mcp_entry` for `McpCatalog`.
- Added a helper `update_timestamp` using `chrono` to automatically update `updated_at` on every modification.
- Added `chrono` dependency to `macc-adapter-shared`.
- Added comprehensive unit tests for upsert (create/update) and delete (existing/missing) operations.
- Verified all workspace tests pass.

Files changed:
- adapters/shared/Cargo.toml
- adapters/shared/src/catalog.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using a dedicated `update_timestamp` private method ensures that `updated_at` is consistently updated whenever the catalog entries are modified, maintaining data integrity.
- Gotchas encountered: Remember to add dependencies (like `chrono`) to the specific crate that needs them, even if they are already present in other workspace members.
- Useful context: `chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)` provides a clean, standard timestamp format (RFC3339) that is consistent with the rest of the codebase.

## 2026-01-30 10:45:00Z - L2-RS-005
Run: gemini-gemini-3-flash-preview-20260130T100717Z
- Added `macc catalog skills` subcommands: `list`, `search`, `add`, and `remove`.
- Implemented tabular listing and case-insensitive search for the skills catalog.
- Added support for adding/updating skills with multiple flags covering all `SkillEntry` fields.
- Re-exported `Selector` from `macc-adapter-shared` for easier CLI integration.
- Added `macc-adapter-shared` as a dependency to `macc-cli`.
- Added a full integration test in `cli/src/main.rs` covering the entire catalog management workflow.
- Verified all tests pass.

Files changed:
- adapters/shared/src/lib.rs
- cli/Cargo.toml
- cli/src/main.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using nested `clap` subcommands provides a very clean and self-documenting CLI structure for multi-level commands like `catalog skills add`.
- Gotchas encountered: When matching on a reference (e.g., `match &cli.command`), remember to `clone()` string fields when passing them to functions that take ownership.
- Useful context: Re-exporting common types in the shared adapter crate simplifies the dependency graph and makes imports cleaner in downstream crates like the CLI.

## 2026-01-30 11:00:00Z - L2-RS-006
Run: gemini-gemini-3-flash-preview-20260130T100717Z
- Added `macc catalog mcp` subcommands: `list`, `search`, `add`, and `remove`.
- Implemented helper functions for MCP catalog management, mirroring the skills catalog behavior.
- Added a full integration test in `cli/src/main.rs` covering the entire MCP catalog management workflow.
- Fixed `fs` import issues in CLI tests by explicitly importing `std::fs`.
- Verified all workspace tests pass.

Files changed:
- cli/src/main.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Code reuse between similar features (like skills and MCP catalogs) can be achieved by using consistent naming conventions and modularizing helper functions, making it easier to maintain and extend the CLI.
- Gotchas encountered: Ensure that all used modules (like `fs`) are properly imported in the scope where they are used, especially in generated or modified test blocks.
- Useful context: The `CatalogSubCommands` enum is shared between skills and MCP commands, which significantly reduces boilerplate in the `clap` definition.

## 2026-01-30 11:15:00Z - L2-RS-007
Run: gemini-gemini-3-flash-preview-20260130T100717Z
- Created `url_parsing` module in `macc-adapter-shared`.
- Implemented `normalize_git_input` to handle GitHub tree URLs and plain repository URLs.
- Automatically extracts repository URL, reference (branch/tag/sha), and subpath from GitHub tree links.
- Added `regex` dependency to `macc-adapter-shared`.
- Added comprehensive unit tests for various GitHub URL formats, including nested subpaths and trailing slashes.
- Verified all workspace tests pass.

Files changed:
- adapters/shared/Cargo.toml
- adapters/shared/src/lib.rs
- adapters/shared/src/url_parsing.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using `OnceLock<Regex>` for compiled regular expressions provides optimal performance for repeated parsing operations within a single process.
- Gotchas encountered: Remember to derive `Clone` for small data transfer objects (like `NormalizedGit`) to facilitate easier testing and data handling.
- Useful context: Trimming trailing slashes from subpaths ensures consistent behavior when joining paths later in the materialization pipeline.
---
## 2026-01-30 11:25:00Z - L2-RS-008
Run: gemini-gemini-3-flash-preview-20260130T100717Z
- Implemented `validate_http_url` to enforce http/https schemes for remote zip sources.
- Implemented `validate_checksum` to enforce the `sha256:<64-hex>` format.
- Added these validation helpers to the `url_parsing` module in `macc-adapter-shared`.
- Added unit tests for both valid and invalid HTTP URLs and checksum strings.
- Verified all workspace tests pass.

Files changed:
- adapters/shared/src/lib.rs
- adapters/shared/src/url_parsing.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Centralizing all URL and source validation logic in a single module (`url_parsing`) ensures that both the CLI and core materialize logic can share the same validation rules.
- Gotchas encountered: Remember to handle case-insensitivity for schemes (HTTP vs http) and hex characters in checksums.
- Useful context: RFC3339 timestamps and sha256 checksums are the standard for integrity and tracking in this project.
---
## 2026-01-30 11:45:00Z - L2-RS-009
Run: gemini-gemini-3-flash-preview-20260130T100717Z
- Implemented deterministic cache key calculation for `Source` using SHA-256 of its core properties.
- Added `cache_dir` and `source_cache_path` helper to `ProjectPaths` in `macc-core`.
- Added `sha2` dependency to `macc-adapter-shared`.
- Verified that `.macc/cache/` is already covered by baseline `.gitignore` entries.
- Added unit tests for stable cache keys and correct path resolution.
- Verified all workspace tests pass.

Files changed:
- adapters/shared/Cargo.toml
- adapters/shared/src/catalog.rs
- core/src/lib.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using a canonical string format (`kind|url|ref|checksum`) before hashing ensures that identical logical sources always yield the same cache key across different environments.
- Gotchas encountered: Ensure that `checksum` is handled gracefully when it's `None` to avoid crashes or non-deterministic keys.
- Useful context: `ProjectPaths` is becoming the central hub for all project-relative locations, making it easy to reason about the project structure.
---
## 2026-01-30 12:00:00Z - L2-RS-010
Run: gemini-gemini-3-flash-preview-20260130T110106Z
- Refined `download_source_raw` in `macc-adapter-shared` to use `reqwest::blocking::Client` with a 30s timeout.
- Implemented atomic write to cache using `core::atomic_write` (temp file + rename).
- Added robust checksum verification (SHA-256) for both cached files and newly downloaded bytes.
- Implemented automatic deletion of mismatching cached artifacts before re-downloading.
- Added unit tests for cache-hit and checksum-verification logic.
- Verified all workspace tests pass.

Files changed:
- adapters/shared/src/fetch.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Verifying checksums in memory BEFORE writing to disk (via `atomic_write`) prevents corrupt data from ever touching the target path, which is safer than writing and then verifying.
- Gotchas encountered: Remember that `reqwest::blocking::get` doesn't expose timeout configuration; using `Client::builder()` is necessary for production-grade fetching.
- Useful context: `ProjectPaths` provides a stable way to resolve cache paths, ensuring that different parts of the system (CLI, Core, Adapters) agree on where artifacts are stored.

## 2026-01-30 12:15:00Z - L2-RS-011
Run: gemini-gemini-3-flash-preview-20260130T110528Z
- Implemented `unpack_archive` in `macc-adapter-shared` with robust Zip Slip defense.
- Utilized `zip::ZipArchive::enclosed_name()` to filter malicious entry paths (absolute or parent-traversing).
- Added an explicit `starts_with` check against the canonicalized target directory for defense-in-depth.
- Implemented `download_and_unpack` helper to streamline the materialization process.
- Added `zip` dependency to `macc-adapter-shared`.
- Added unit tests for safe unpacking and Zip Slip rejection.
- Added an integration test for the combined download and unpack flow using cached files.
- Verified all workspace tests pass.

Files changed:
- adapters/shared/src/fetch.rs
- adapters/shared/Cargo.toml
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: `zip::ZipArchive::enclosed_name()` is the idiomatic way to handle Zip Slip in Rust, as it returns `None` for unsafe paths.
- Gotchas encountered: Always canonicalize the target directory BEFORE joining with entry names to ensure that `starts_with` checks are reliable and not tricked by different path representations (e.g., `./foo` vs `foo`).
- Useful context: Keeping the `raw/` and `unpacked/` directories separate in the cache ensures that we always have the original artifact if we need to re-unpack or verify integrity later.

## 2026-01-30 12:30:00Z - L2-RS-012
Run: gemini-gemini-3-flash-preview-20260130T111323Z
- Implemented `git_fetch` in `macc-adapter-shared` using system `git` CLI.
- Logic handles initial `clone`, subsequent `fetch --all --tags`, and `checkout <ref>`.
- Captures `stderr` for actionable error messages when git commands fail.
- Added `materialize_source` as a unified entry point for both Git and HTTP sources.
- Added a robust integration test using a local temporary Git repository to verify clone, update, and SHA checkout.
- Verified all workspace tests pass.

Files changed:
- adapters/shared/src/fetch.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using `Command::current_dir` is cleaner than manually switching directories for git operations.
- Gotchas encountered: Remember that `git fetch` doesn't update the current branch head; an explicit `checkout` is needed to move to a specific ref/SHA.
- Useful context: `git clone` into a sub-directory (`repo/`) within the cache entry's root allows for future expansion (like storing metadata or sparse checkout state alongside the repo).

## 2026-01-30 13:00:00Z - L2-RS-013
Run: gemini-gemini-3-flash-preview-20260130T120936Z
- Implemented Git sparse-checkout support in `macc-adapter-shared`.
- Added `subpaths` field to `Source` and integrated it into `cache_key` calculation.
- Logic uses `git sparse-checkout init --cone` and `git sparse-checkout set` for efficient folder-only downloads.
- Optimized initial clone with `--filter=blob:none` when subpaths are present (partial clone).
- Added helper functions `enable_sparse_checkout`, `set_sparse_paths`, and `disable_sparse_checkout`.
- Implemented subpath validation after checkout to ensure requested paths exist.
- Added integration tests for single and multiple subpaths.
- Verified all workspace tests pass.

Files changed:
- adapters/shared/src/catalog.rs
- adapters/shared/src/fetch.rs
- cli/src/main.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using `--cone` mode in sparse-checkout is ideal for directory-based selections as it simplifies path patterns and is highly optimized.
- Gotchas encountered: Remember that `git clone --no-checkout` followed by `sparse-checkout set` and then `checkout` is the most robust way to ensure only desired files are ever materialized in the working tree.
- Useful context: Including `subpaths` in the cache key ensures that different sparse views of the same repository are isolated in the cache, preventing state conflicts.
## 2026-01-30 14:00:00Z - L2-RS-014
Run: gemini-gemini-3-flash-preview-20260130T122323Z
- Moved `catalog.rs` from `macc-adapter-shared` to `macc-core` to follow the architecture in MACC.md.
- Added `mcp` selection support to `CanonicalConfig` and `ResolvedConfig`.
- Implemented `resolve_fetch_units` in `macc-core::resolve` to group multiple skill/MCP selections sharing the same source.
- Logic deduplicates subpaths within each `FetchUnit`, ensuring efficient Git sparse-checkouts or archive materialization.
- Added unit tests for grouping behavior, multi-source resolution, and error handling for missing IDs.
- Verified all workspace tests pass, including CLI catalog management tests.

Files changed:
- adapters/shared/src/catalog.rs
- core/Cargo.toml
- core/src/catalog.rs
- core/src/config/mod.rs
- core/src/lib.rs
- core/src/resolve/mod.rs
- core/src/tool/mod.rs
- cli/src/main.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Moving core data structures (like Catalogs) to the core crate early avoids circular dependencies when the resolve engine needs to look up metadata.
- Gotchas encountered: Remember to update all test initializers when adding new fields to core configuration structs.
- Useful context: `FetchUnit` acts as the bridge between "what the user wants" (IDs) and "how to get it" (Sources with combined subpaths), allowing the fetcher to be completely decoupled from selection logic.

## 2026-01-30 15:00:00Z - L2-RS-015
Run: gemini-gemini-3-flash-preview-20260130T123623Z
- Implemented `MaterializedFetchUnit` and `PlanningContext` in `macc-core`.
- Added `materialize_fetch_unit(s)` in `macc-adapter-shared` as a unified materialization stage.
- Integrated subpath validation for all source kinds (HTTP and Git) during materialization.
- Updated `ToolAdapter` trait and all existing adapters (Claude, Gemini, Codex) to use `PlanningContext`.
- Updated CLI (`macc-cli`) and Core (`macc-core`) to orchestrate the materialization stage before planning and applying.
- Added unit and integration tests for the unified materialization flow.
- Verified all workspace tests pass.

Files changed:
- adapters/claude/src/adapter.rs
- adapters/codex/src/adapter.rs
- adapters/gemini/src/adapter.rs
- adapters/shared/src/fetch.rs
- adapters/shared/src/lib.rs
- core/src/lib.rs
- core/src/resolve/mod.rs
- core/src/tool/mod.rs
- core/tests/plan_apply_integration.rs
- cli/src/main.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using a `PlanningContext` struct for the `ToolAdapter::plan` method allows adding more context (like materialized units) without breaking the trait signature for every change.
- Gotchas encountered: Remember that the CLI is the primary orchestrator that bridges `macc-core` (pure logic) and adapters/shared (I/O and fetching).
- Useful context: Validating subpaths immediately after materialization ensures that adapters can safely assume files exist, simplifying their logic.

## 2026-01-30 16:00:00Z - L2-RS-016
Run: gemini-gemini-3-flash-preview-20260130T124611Z
- Implemented skill package validation heuristics in `macc-adapter-shared`.
- Added `validate_skill_folder(path)` in `packages.rs` which checks for marker files (`SKILL.md`, `skill.md`, `README.md`).
- Integrated validation into `materialize_fetch_unit` to ensure skills are not misconfigured (e.g. pointing to repo root without markers).
- Added unit tests for the validator and integration-style tests in the materialization logic.
- Verified all workspace tests pass.

Files changed:
- adapters/shared/src/fetch.rs
- adapters/shared/src/packages.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using simple heuristics like marker files is a lightweight way to provide early feedback on configuration errors before attempting tool-specific generation.
- Gotchas encountered: Remember that existing tests might need updates when adding new validation steps to common pipeline functions like materialization.
- Useful context: `SelectionKind` allows for type-specific validation during the shared materialization stage.

## 2026-01-30 17:00:00Z - L2-RS-017
Run: gemini-gemini-3-flash-preview-20260130T130609Z
- Implemented MCP manifest parsing and validation in `macc-adapter-shared`.
- Defined `McpManifest` struct in `packages.rs` with fields for MCP server configuration and merge target.
- Added `validate_mcp_folder` which enforces the presence of `macc.package.json` and validates its contents (type, ID match, merge target).
- Integrated MCP validation into the `materialize_fetch_unit` pipeline in `fetch.rs`.
- Added unit tests for valid and invalid MCP manifests (missing file, invalid JSON, wrong type, ID mismatch).
- Verified all workspace tests pass.

Files changed:
- adapters/shared/src/fetch.rs
- adapters/shared/src/packages.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Requiring a manifest for complex integrations like MCP (which involve merging configuration objects rather than just copying files) is a robust pattern that avoids "guessing" and enables safer automation.
- Gotchas encountered: Ensure that the manifest ID matches the catalog ID to prevent accidental misconfigurations when multiple subpaths are fetched from the same repository.
- Useful context: `serde_json::Value` is used for the MCP server object to allow for flexibility in the underlying MCP server configuration while still enforcing a structured manifest.

## 2026-01-30 18:00:00Z - L2-RS-018
Run: gemini-gemini-3-flash-preview-20260130T130609Z
- Implemented `expand_directory_to_plan` in `macc-adapter-shared::plan_builders`.
- Added recursive directory walking that collects all files while rejecting symlinks and unsupported file types.
- Ensures deterministic output by sorting relative paths before generating actions.
- Integrates with `ActionPlanBuilder` to create `WriteFile` actions with correctly mapped destination paths.
- Added unit tests for recursive expansion, path sorting, and symlink rejection.
- Verified all workspace tests pass.

Files changed:
- adapters/shared/src/plan_builders.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Delegating path normalization and validation to `ActionPlanBuilder` ensures that all expanded directory actions adhere to the same security and formatting rules as manually added actions.
- Gotchas encountered: `std::fs::FileType::is_file()` can be false for symlinks even if they point to a file, so explicit `is_symlink()` checks are necessary when using `read_dir`.
- Useful context: Sorting relative paths at the expansion stage (before adding to the builder) is crucial for plan idempotence, as the order of files returned by `read_dir` is not guaranteed.

## 2026-01-30 19:00:00Z - L2-RS-019
Run: gemini-gemini-3-flash-preview-20260130T130609Z
- Expanded `ToolPaths` in `macc-adapter-shared::paths` to include Claude and Codex defaults.
- Implemented `plan_skill_install` in `macc-adapter-shared::plan_builders`.
- Logic automatically maps tool names to their respective skills directories (e.g., `.claude/skills/`).
- Integrates skill package heuristics (`validate_skill_folder`) and recursive directory expansion (`expand_directory_to_plan`).
- Added unit tests for multi-tool skill installation planning.
- Verified all workspace tests pass, confirming idempotence through the core `apply` engine.

Files changed:
- adapters/shared/src/paths.rs
- adapters/shared/src/plan_builders.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Centralizing tool-specific path mapping in `ToolPaths` makes the install planner tool-agnostic, simplifying the addition of future adapters.
- Gotchas encountered: Remember that some tools might use different filenames for their project instruction roots (e.g., `CLAUDE.md` vs `GEMINI.md` vs `AGENTS.md`).
- Useful context: Using `ActionPlanBuilder`'s validation ensures that any attempt to install skills outside of permitted project scopes (e.g., via malicious subpaths) is caught at plan time.

## 2026-01-30 20:00:00Z - L2-RS-020
Run: gemini-gemini-3-pro-preview-20260130T133158Z
- Implemented `plan_mcp_install` in `macc-adapter-shared::plan_builders`.
- Logic loads `macc.package.json`, extracts the server config, and creates a `MergeJson` action targeting `.mcp.json`.
- Updated `macc-core` to fix `Ord`/`PartialOrd` implementation for `Action` enum (needed for deterministic sorting with `serde_json::Value`).
- Fixed `Action::MergeJson` implementation in `macc-core` to use `as_slice()` instead of `as_bytes()` on `Vec<u8>`.
- Optimized `macc-core` json merge logic to avoid unnecessary cloning of file contents.
- Verified all workspace tests pass.

Files changed:
- adapters/shared/src/plan_builders.rs
- core/src/plan/mod.rs
- core/src/lib.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: When extending the `Action` enum with types that don't implement `Ord` (like `serde_json::Value`), manual implementation of `Ord` is required to maintain deterministic plan sorting.
- Gotchas encountered: `Vec<u8>` does not implement `as_bytes()`; use `as_slice()` or dereference. Also, be careful with `Option<Vec<u8>>` and `unwrap_or_default` causing partial moves; use `as_deref()` to access contents non-destructively.
- Useful context: `MergeJson` allows for granular updates to configuration files like `.mcp.json` without overwriting the entire file, preserving user customizations or other entries.

## 2026-01-30 21:00:00Z - L2-RS-021
Run: gemini-gemini-3-pro-preview-20260130T134608Z
- Moved `packages`, `paths`, and `plan_builders` from `adapters/shared` to `macc-core` to centralize validation and planning logic.
- Updated `GeminiAdapter`, `ClaudeAdapter`, and `CodexAdapter` to consume `PlanningContext::materialized_units`.
- Implemented logic in all adapters to iterate materialized units and generate installation actions using `plan_skill_install` (and `plan_mcp_install` for Claude).
- Updated `Action::cmp` in `macc-core` to enforce deterministic sorting based on variant rank (Mkdir < BackupFile < WriteFile < ...) instead of unstable JSON stringification.
- Refactored `adapters/shared` to be lightweight, removing moved logic.
- Verified all workspace tests pass, including integration of install planning.

Files changed:
- core/src/lib.rs
- core/src/plan/mod.rs
- core/src/plan/builders.rs (new)
- core/src/packages.rs (new)
- core/src/paths.rs (new)
- adapters/shared/src/lib.rs
- adapters/shared/src/fetch.rs
- adapters/shared/src/packages.rs (deleted)
- adapters/shared/src/paths.rs (deleted)
- adapters/shared/src/plan_builders.rs (deleted)
- adapters/gemini/src/adapter.rs
- adapters/claude/src/adapter.rs
- adapters/codex/src/adapter.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Moving shared domain logic (like package validation and standard path definitions) to `core` avoids circular dependencies and ensures all adapters adhere to the same rules.
- Gotchas encountered: `serde_json::to_string` does not guarantee stable key ordering for complex nested objects in all cases (though usually it does), and sorting by JSON string representation of enums puts them in alphabetical order of variant names (or "type" field), which may not match logical dependency order (e.g., Mkdir must precede WriteFile). Explicit `Ord` implementation is safer.
- Useful context: `ActionPlanBuilder` is great for fluent API construction, but internal helper functions should often operate directly on `&mut ActionPlan` to allow for flexible composition without ownership constraints.

## 2026-01-30 22:30:00Z - L2-RS-023
Run: gemini-gemini-3-pro-preview-20260130T140500Z
- Implemented `macc catalog import --url <url>` command.
- Supports importing Skills and MCP servers directly from GitHub URLs (including tree URLs).
- Automatically normalizes URLs and populates `Source` and `Selector` fields.
- Added `CatalogCommands::ImportUrl` to the CLI enum.
- Added integration test `test_catalog_import_url` verifying end-to-end import flow.

Files changed:
- cli/src/main.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Reusing the `url_parsing` logic from `macc-adapter-shared` in the CLI command handler function allows for consistent URL interpretation across the system.
- Gotchas encountered: Ensure `clap` args are properly propagated to the handler function.
- Useful context: This feature allows users to quickly add resources without manually editing JSON catalog files.

## 2026-01-30 23:00:00Z - L2-RS-024

## 2026-01-30 23:30:00Z - L2-RS-025
Run: gemini-gemini-3-pro-preview-20260130T142409Z
- Implemented `remote_search` in `macc-adapter-shared::catalog` using `reqwest::blocking::Client`.
- Defined `SearchKind` enum for type-safe search parameters.
- Added robust error handling for HTTP status codes and JSON parsing errors, ensuring user-friendly error messages (including truncated response bodies).
- Added comprehensive unit tests for various scenarios, verifying success and error status.

Files changed:
- adapters/shared/src/catalog.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using `TcpListener` and `thread::spawn` allows for lightweight integration testing of HTTP clients without requiring heavy external mocking libraries.
- Gotchas encountered: `reqwest`'s error messages for JSON parsing on non-JSON bodies can be generic ("error decoding response body"). Wrapping these with context (e.g., "Failed to parse search response from URL") significantly improves debuggability.
- Useful context: `remote_search` is designed to be generic over the response item type `T`, allowing it to be used for both Skills and MCP servers seamlessly.

## 2026-01-31 00:00:00Z - L2-RS-026

## 2026-01-31 00:00:00Z - L2-RS-026
Run: gemini-gemini-3-pro-preview-20260130T142814Z
- Implemented `macc catalog search-remote` CLI command.
- Supports searching remote registries for skills and MCP servers.
- Implemented optional import of search results into local catalog (`--add` or `--add-ids`).
- Added comprehensive integration test `test_search_remote_cli` mocking the remote server.
- Verified all workspace tests pass.

Files changed:
- cli/src/main.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using `TcpListener` with `buf_reader` allows simple mocking of HTTP JSON APIs for CLI integration tests without heavy dependencies.
- Gotchas encountered: When editing large files (like `cli/src/main.rs`) via `replace`, be extremely careful with matching context to avoid accidentally deleting surrounding code or breaking scope closures. Small, targeted replacements are safer than replacing large blocks.
- Useful context: The `search-remote` command bridges the gap between discovery and installation, allowing users to find and save resources in one flow.

## 2026-01-31 00:30:00Z - L2-RS-027
Run: gemini-gemini-3-flash-preview-20260130T143717Z
- Implemented integration test `test_install_skill_multi_zip_cli` in `macc-cli`.
- Test verifies that when a ZIP archive contains multiple skills, only the selected skill (via `subpath`) is installed.
- Added `zip` crate as a dev-dependency to `macc-cli` for test fixture generation.
- Verified selective installation: only `.claude/skills/skill-a/` exists, while other skills in the same ZIP are ignored.
- Fixed an unused import warning in `core/src/plan/builders.rs`.

Files changed:
- cli/Cargo.toml
- cli/src/main.rs
- core/src/plan/builders.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using `TcpListener` to serve dynamically generated ZIP bytes is an effective way to test remote fetch and unpack logic without external dependencies or pre-built fixtures.
- Gotchas encountered: Ensure that `MaterializedFetchUnit`'s `source_root_path` is correctly used as the base for `subpath` resolution during planning to avoid path leaking.
- Useful context: `plan_skill_install` correctly handles `subpath` by joining it with the materialized root, ensuring only the target subdirectory is expanded into the action plan.

## 2026-01-31 00:30:00Z - L2-RS-027
Run: gemini-gemini-3-flash-preview-20260130T143717Z
- Implemented integration test `test_install_skill_multi_zip_cli` in `macc-cli`.
- Test verifies that when a ZIP archive contains multiple skills, only the selected skill (via `subpath`) is installed.
- Added `zip` crate as a dev-dependency to `macc-cli` for test fixture generation.
- Verified selective installation: only `.claude/skills/skill-a/` exists, while other skills in the same ZIP are ignored.
- Fixed an unused import warning in `core/src/plan/builders.rs`.

Files changed:
- cli/Cargo.toml
- cli/src/main.rs
- core/src/plan/builders.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using `TcpListener` to serve dynamically generated ZIP bytes is an effective way to test remote fetch and unpack logic without external dependencies or pre-built fixtures.
- Gotchas encountered: Ensure that `MaterializedFetchUnit`'s `source_root_path` is correctly used as the base for `subpath` resolution during planning to avoid path leaking.
- Useful context: `plan_skill_install` correctly handles `subpath` by joining it with the materialized root, ensuring only the target subdirectory is expanded into the action plan.

## 2026-01-31 01:00:00Z - L2-RS-028
Run: gemini-gemini-3-flash-preview-20260130T144024Z
- Implemented integration test `test_install_skill_multi_git_cli` in `macc-cli`.
- Test verifies selective installation from a Git repository with multiple skills using sparse-checkout.
- Fixed a bug in `install_skill` and `install_mcp` where `source.subpaths` was not being populated, preventing sparse-checkout for direct installations.
- Verified that only the selected skill is installed and that unselected folders are not materialized in the cache working tree.

Files changed:
- cli/src/main.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Integration tests using local Git repos with `file://` URLs are effective for testing complex fetch and checkout logic without network dependency.
- Gotchas encountered: Direct installation commands (`macc install ...`) often bypass the standard resolve logic, so manual population of fields like `source.subpaths` is necessary to ensure consistent behavior with the full `apply` pipeline.
- Useful context: Git's `sparse-checkout` in `--cone` mode is highly effective at reducing working tree noise and improving performance in large monorepos, and it's important to ensure it's triggered by populating the `Source` model fields.

## 2026-01-31 01:30:00Z - L2-RS-029
Run: gemini-gemini-3-flash-preview-20260130T145808Z
- Implemented symlink rejection in `unpack_archive` (ZIP extraction) in `macc-adapter-shared`.
- Verified that `expand_directory_to_plan` (file-walk) already rejects symlinks.
- Expanded secret scanning in `macc-core` to include `MergeJson` actions, ensuring patches are scanned before being planned or applied.
- Updated `preview_plan` and `apply_plan` to display security findings and warnings for `MergeJson` actions.
- Added unit test `test_unpack_rejects_symlinks` in `macc-adapter-shared`.
- Added integration test `test_install_skill_rejects_symlink_cli` in `macc-cli`.
- Added unit test `test_validate_plan_detects_secret_in_merge_json` in `macc-core`.
- Fixed a workspace configuration issue in `adapters/Cargo.toml` where a non-existent glob pattern was causing build failures.

Files changed:
- adapters/shared/src/fetch.rs
- adapters/Cargo.toml
- core/src/lib.rs
- cli/src/main.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: ZIP symlinks can be detected by checking if `unix_mode() & 0o170000 == 0o120000`. The `zip` crate's `add_symlink` is the correct way to create these for testing.
- Gotchas encountered: `validate_plan` should be the central place for all safety checks to ensure consistency between `plan` and `apply`. Always remember to scan both full file writes and incremental patches (like JSON merges).
- Useful context: Security boundaries are applied at both the materialization (unzip/clone) and planning (file-walk/scan) stages for defense-in-depth.

## 2026-01-31 02:00:00Z - L2-RS-030
Run: gemini-gemini-3-pro-preview-20260130T151140Z
- Created comprehensive documentation for Catalog Management in `docs/CATALOGS.md`.
- Documented JSON schema for skills and MCP catalogs.
- Provided clear CLI workflows for manual addition, URL import (GitHub tree links), and remote search discovery.
- Detailed direct installation commands for skills and MCP servers.
- Included security notes on checksum verification, symlink rejection, and secret scanning.
- Updated `README.md` and `MACC.md` to link to the new documentation.

Files changed:
- docs/CATALOGS.md
- README.md
- MACC.md
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Centralizing technical guides in a `docs/` directory keeps the root `README.md` clean while providing detailed information for advanced workflows.
- Gotchas encountered: Ensure that documentation reflects the actual CLI flags by cross-referencing with `cli/src/main.rs` (e.g., `--reference` vs `--ref` in different contexts).
- Useful context: GitHub tree link parsing is a powerful feature for simplifying imports, and documenting its usage helps users leverage this "Case 3-5" workflow easily.

## 2026-01-31 02:30:00Z - L2-PLAN-001
Run: gemini-gemini-3-flash-preview-20260130T172920Z
- Defined v0.2 acceptance criteria and created an in-repo checklist.
- Created `docs/v0.2-checklist.md` with sections for TUI, Apply/Preview, User-level merges, MCP, Ralph, Tests, and Docs.
- Linked the checklist in `README.md` and `MACC.md`.
- Updated `scripts/Ralph/prd.json` to mark the task as passed.

Files changed:
- docs/v0.2-checklist.md
- README.md
- MACC.md
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Creating a structured checklist with "Verification Item", "Command", and "Expected Output" helps clarify the "Definition of Done" for complex features like TUI and automated loops.
- Gotchas encountered: Ensure that documentation links are consistent across all major entry points (`README.md` and `MACC.md`).
- Useful context: v0.2 milestone focuses on interactive UX (TUI), autonomy (Ralph), and user-scope safety (consented merges).

## 2026-01-30T17:45:00Z - L2-TUI-001
Run: gemini-gemini-3-flash-preview-20260130T173329Z
- Created `macc-tui` crate as a new workspace member.
- Implemented minimal Ratatui TUI with terminal RAII guard for safe cleanup.
- Added `tui` subcommand to `macc-cli` which launches the interactive UI.
- Verified build and updated `docs/v0.2-checklist.md` with TUI verification steps.

Files changed:
- Cargo.toml
- cli/Cargo.toml
- cli/src/main.rs
- tui/Cargo.toml
- tui/src/lib.rs
- tui/src/main.rs
- docs/v0.2-checklist.md
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using a `TerminalGuard` with `Drop` implementation is a robust way to ensure the terminal is restored to a usable state even if the program panics.
- Gotchas encountered: `crossterm` features can be tricky; double-check if it's `event` or `events` depending on the version (0.27 uses `events` for some sub-features).
- Useful context: Exposing TUI logic as a library (`lib.rs`) allows both a standalone binary and a subcommand in the main CLI, providing flexibility for different execution modes.

## 2026-01-30T18:00:00Z - L2-TUI-002
Run: gemini-gemini-3-flash-preview-20260130T173915Z
- Created TUI screen router and shared app state model.
- Defined `AppState` in `tui/src/state.rs` to hold configuration, errors, and screen stack.
- Defined `Screen` enum in `tui/src/screen.rs` with Home and About placeholders.
- Implemented navigation reducer (Push, Pop, GoTo) in `AppState`.
- Updated `tui/src/lib.rs` to use the new state and handle navigation keybindings ('h', 'a', 'Backspace', 'q', 'Esc').
- Added unit tests for `AppState` navigation logic.
- Updated `docs/v0.2-checklist.md` with TUI navigation verification.

Files changed:
- tui/Cargo.toml
- tui/src/lib.rs
- tui/src/screen.rs
- tui/src/state.rs
- docs/v0.2-checklist.md
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using a stack-based router in `AppState` makes it easy to handle "Back" navigation consistently across screens.
- Gotchas encountered: Clippy requires `Default` implementation when a `new()` method without arguments is present.
- Useful context: Keeping screen rendering logic matched against a `Screen` enum allows for a clean separation between state management and UI representation.

## 2026-01-30T18:15:00Z - L2-CONFIG-001
Run: gemini-gemini-3-flash-preview-20260130T174326Z
- Implemented repo-root detection and configuration loading in AppState.
- Updated TUI to display loaded project paths and enabled tools on the Home screen.
- Added error banner to TUI for missing or invalid configuration files.
- Refactored AppState::load_config to accept an optional starting path for better testability without global state changes.
- Added unit tests for valid, missing, and invalid configuration scenarios.
- Updated v0.2-checklist.md with TUI configuration verification steps.

Files changed:
- tui/Cargo.toml
- tui/src/lib.rs
- tui/src/state.rs
- docs/v0.2-checklist.md
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Passing an optional starting directory to discovery functions (like `load_config`) makes them much easier to unit test without relying on or interfering with global process state like `env::current_dir`.
- Gotchas encountered: Rust tests run in parallel by default, so `env::set_current_dir` in one test will affect all other tests in the same process, leading to flaky and confusing failures.
- Useful context: `macc-core` already provides robust project discovery and config loading utilities that should be preferred over re-implementing them in individual crates.

## 2026-01-30T18:30:00Z - L2-CONFIG-002
Run: gemini-gemini-3-flash-preview-20260130T182003Z
- Updated `CanonicalConfig` and `ResolvedConfig` to use `BTreeMap` for standards inline configuration to ensure deterministic serialization.
- Implemented `AppState::save_config` to persist TUI changes back to `.macc/macc.yaml`.
- Integrated `macc_core::atomic_write` for safe, atomic configuration updates.
- Added 's' keybinding to TUI to trigger saving configuration.
- Updated TUI UI to display notices (success messages) and updated help footer.
- Added unit tests for deterministic serialization and `save_config` idempotence.

Files changed:
- core/src/config/mod.rs
- core/src/resolve/mod.rs
- tui/src/lib.rs
- tui/src/state.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Switching from `HashMap` to `BTreeMap` in core data structures that are serialized to YAML/JSON is essential for maintaining a clean git history and ensuring idempotence across different environments or runs.
- Gotchas encountered: Rust tests run in parallel, and while `std::env::set_current_dir` is dangerous, `macc-core`'s design of passing paths to discovery functions correctly avoids this pitfall.
- Useful context: `macc_core` already has a robust `atomic_write` implementation that should be preferred for all configuration and artifact writes to ensure filesystem integrity.

## 2026-01-30T19:05:00Z - L2-TUI-003
Run: gemini-gemini-3-flash-preview-20260130T175540Z
- Implemented 'Tools' screen in TUI to enable/disable tools.
- Added 'Tools', 'Claude', 'Codex', and 'Gemini' screens to the `Screen` enum.
- Updated `AppState` with tool selection and toggling logic.
- Implemented gated navigation: tool-specific screens are only accessible when the tool is enabled.
- Added unit tests for tool selection, toggling, and looping logic.
- Derived `Default` for `CanonicalConfig` in `macc-core` to simplify testing.
- Updated TUI UI with selection markers and enabled/disabled checkboxes.

Files changed:
- core/src/config/mod.rs
- tui/src/screen.rs
- tui/src/state.rs
- tui/src/lib.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using a dedicated `AVAILABLE_TOOLS` constant and selection index in `AppState` makes list-based configuration screens easy to manage and test.
- Gotchas encountered: Deriving `Default` for complex structs like `CanonicalConfig` is often possible if all its fields implement `Default`, which simplifies mocking in tests.
- Useful context: Separation of 'Space' for toggling and 'Enter' for navigation provides a clear UX for list-based settings.

## 2026-01-30T19:45:00Z - L2-TUI-004
Run: gemini-gemini-3-flash-preview-20260130T194500Z
- Implemented Claude Settings screen in TUI for configuring model, language, and permissions presets.
- Extended CanonicalConfig and ResolvedConfig with tool-specific configuration fields for Claude, Codex, and Gemini.
- Updated Claude adapter to use TUI-configured settings for generating CLAUDE.md and .claude/settings.json.
- Implemented permissions preset mapping (safe, dev, strict) to Claude-specific allow/deny rule sets.
- Added unit tests for Claude settings navigation and selection logic in AppState.
- Fixed multiple ToolsConfig initializers across core and adapter tests to use Default::default().

Files changed:
- core/src/config/mod.rs
- core/src/resolve/mod.rs
- core/src/lib.rs
- core/src/tool/mod.rs
- tui/src/state.rs
- tui/src/lib.rs
- adapters/claude/src/map.rs
- adapters/claude/src/emit/settings_json.rs
- adapters/claude/src/emit/claude_md.rs
- scripts/Ralph/prd.json
- scripts/Ralph/progress.md

**Learnings for future iterations:**
- Patterns discovered: Using enum-based selection indexes and "cycle" methods in AppState provides a robust and easily testable way to implement form-like screens in Ratatui without complex external widget libraries.
- Gotchas encountered: Adding fields to structs that are manually initialized in many places (especially tests) requires updating all of them; using `..Default::default()` everywhere possible from the start is a best practice to avoid this.
- Useful context: Keeping a "live preview" of generated artifacts (like .claude/settings.json) directly in the configuration screen provides immediate feedback to the user on how their settings will affect the final output.

## 2026-01-30T18:29:53Z - L2-TUI-006
Run: codex-gpt-5.1-codex-max-20260130T182953Z
- Implemented Claude Agents selection screen with catalog, multi-select UI, and config persistence; added adapter support to merge tool-specific agents with global selections.
- Added stable ordering and reducer tests for agent selection; updated TUI navigation/help and Claude settings preview to surface agents.
- Hardened loopback-based tests with a bind helper to skip when sockets are disallowed in sandboxed environments.
- Thread: local CLI session (no external thread URL available).
- Files changed: core/src/config/mod.rs; tui/src/state.rs; tui/src/lib.rs; tui/src/screen.rs; adapters/claude/src/map.rs; adapters/shared/src/catalog.rs; cli/src/main.rs; scripts/Ralph/prd.json; scripts/Ralph/progress.md
- **Learnings for future iterations:**
  - Patterns discovered: Reusable `bind_loopback` helper keeps HTTP-mock tests resilient when loopback sockets are restricted.
  - Gotchas encountered: Some sandboxes block `127.0.0.1` binds, causing deterministic test failures unless guarded.
  - Useful context: Claude adapters now honor tool-specific agents (`tools.claude.agents`) merged with global selections; TUI writes them sorted for deterministic config.
---
## 2026-01-30T20:25:00Z - L2-PLAN-002
Run: codex-gpt-5.1-codex-mini-20260130T190611Z
- What was implemented: Added `PlannedOp` modeling plus `plan_operations` which converts `ActionPlan` output into deterministic path-sorted operations with backup/consent metadata, updated docs, and exposed the API for the TUI preview screen.
- Files changed: `core/src/plan/ops.rs`, `core/src/plan/mod.rs`, `core/src/lib.rs`, `core/tests/plan_operations.rs`, `README.md`, `scripts/Ralph/prd.json`, `scripts/Ralph/progress.md`
- **Learnings for future iterations:**
  - Patterns discovered: Deriving preview-ready `PlannedOp`s from the action plan keeps CLI and TUI previews in sync without duplicated logic and guarantees deterministic ordering for rendering and diffs.
  - Gotchas encountered: `.gitignore` Ensure actions must merge their patterns manually to produce a meaningful `after` payload; without that merge the preview shows blank content even when new entries are planned.
  - Useful context: `plan_operations` intentionally stops before `validate_plan`, so frontends can still enumerate user-scope ops with consent flags even if M0 forbids applying them.
Thread: local CLI session (no external thread URL available).
---
## 2026-01-30T21:10:00Z - L2-TUI-008
Run: codex-gpt-5.1-codex-mini-20260130T192046Z
- Implemented the Preview screen: fetch the plan via the resolve/materialize/plan pipeline, cache it in `AppState`, and surface refresh/error helpers so the view stays responsive.
- Rendered the list/detail layout with action/scope/path columns, summary counts, metadata panel, and updated navigation/help keys (p for preview, r for refresh).
- Files changed:
  - Cargo.lock
  - tui/Cargo.toml
  - tui/src/lib.rs
  - tui/src/screen.rs
  - tui/src/state.rs
  - scripts/Ralph/prd.json
  - scripts/Ralph/progress.md
- **Learnings for future iterations:**
  - Patterns discovered: Reusing the CLI’s resolve_fetch_units → materialize_fetch_units → plan_operations pipeline with the same tool registry keeps Preview deterministic and aligned with apply.
  - Gotchas encountered: Preview requires the `.macc` catalog files, so we now surface a banner-level error when the project isn’t loaded instead of panicking during planning.
  - Useful context: The summary/metadata panels rely on `BTreeMap` ordering to keep counts deterministic and the detail panel already exposes backup/consent flags so diffs or apply tooling can piggyback on the same data.
Thread: local CLI session (no external thread URL available).
---

## 2026-01-30T22:25:00Z - L2-DIFF-001
Run: codex-gpt-5.1-codex-mini-20260130T193358Z
- Implemented `render_diff`/`DiffView` with JSON normalization, truncation, and secret-aware sanitization so planned ops can show safe diffs.
- Added secret-scan range tracking, re-exported `render_diff`, and covered text/JSON/truncation/redaction behaviors with unit tests.
- Files changed: `core/src/security/secret_scan.rs`; `core/src/plan/diff_view.rs`; `core/src/plan/mod.rs`; `scripts/Ralph/prd.json`; `scripts/Ralph/progress.md`
- Thread: local CLI session (no external thread URL available).
- **Learnings for future iterations:**
  - Patterns discovered: Sanitizing planned diffs via secret scan ranges and capping line/byte budgets with an explicit `[diff truncated: ...]` marker keeps previews safe and deterministic.
  - Gotchas encountered: JSON-aware diffing requires `normalize_json` to succeed; if it doesn’t we must fall back to the text path so the UI still sees a readable diff.
- Useful context: The truncation budget needs to honor both lines and bytes, so we trim greedily from the end of the selection and append the notice once only.
---
## 2026-01-30 23:58:00Z - L2-TUI-009
Run: codex-gpt-5.1-codex-mini-20260130T194153Z
- What was implemented: Added per-op diff caching + scroll state, wired Preview to render a scrollable diff pane with truncation notice, new PageUp/PageDown bindings, and a regression test that selects an operation and surfaces its diff.
- Files changed:
  - tui/src/lib.rs
  - tui/src/state.rs
  - scripts/Ralph/prd.json
  - scripts/Ralph/progress.md
Thread: local CLI session (no external thread URL available).
- **Learnings for future iterations:**
  - Patterns discovered: Cache diff views per planned operation (keyed by path + kind) and remember scroll offsets so Preview navigation stays smooth without recomputing diffs every frame.
  - Gotchas encountered: Diff panels need to subtract two rows for borders before computing how many lines to show; otherwise page scrolling can overshoot or show empty space.
  - Useful context: The diff generator already appends `[diff truncated: ...]` markers, but the UI still surfaces an explicit notice so users know a partial payload is visible.
---
## 2026-01-30 23:50:00Z - L2-USER-001
Run: codex-gpt-5.1-codex-mini-20260130T195445Z
- What was implemented: Added a machine-readable `consent_required` flag to each planned operation, kept project/user scope tagging deterministic, and ensured the diff/preview pipelines and README surface the new metadata.
- Files changed: core/src/plan/ops.rs; core/src/plan/diff_view.rs; core/tests/plan_operations.rs; README.md; tui/src/lib.rs; tui/src/state.rs; scripts/Ralph/prd.json; scripts/Ralph/progress.md
- **Learnings for future iterations:**
  - Patterns discovered: Planned operations now expose a standalone `consent_required` flag so UIs can gate user-scope merges without re-evaluating scope metadata.
  - Gotchas encountered: Adding new fields requires touching every manual `PlannedOp` literal (tests + UI mocks) to keep the compiler happy.
  - Useful context: The new unit test confirms user merges always set both the explicit flag and the old metadata marker, keeping downstream consumers aligned.
Thread: local CLI session (no external thread URL available).
---

## 2026-01-30 23:30:00Z - L2-BACKUP-001
Run: codex-gpt-5.1-codex-mini-20260130T202223Z
- What was implemented: Added `UserBackupManager` to journal user-level backups under `~/.macc/backups/<timestamp>/`, including sanitized relative paths, permission-preserving copies, aggregated report helpers, tests using temp homes, and the supporting home lookup/error variant plus CLI exit mapping.
- Files changed:
  - core/src/user_backup.rs
  - core/src/lib.rs
  - cli/src/main.rs
  - scripts/Ralph/prd.json
  - scripts/Ralph/progress.md
- **Learnings for future iterations:**
  - Patterns discovered: Sanitizing the path components and stripping the home prefix before copying ensures the timestamped backup tree mirrors the source layout without leaking absolute or traversal references.
  - Gotchas encountered: Rust `Component::Prefix` is a tuple variant, so pattern matches must include `(_)`, and we needed a dedicated `HomeDirNotFound` error for headless environments.
  - Useful context: The new manager now emits reports and will be invoked before user-scope writes once consent gating allows them, so adapters can build their list of paths knowing backups are guaranteed.
 
Thread: local CLI session (no external thread URL available).
---
## 2026-01-30T20:42:27Z - L2-IO-001
Run: codex-gpt-5.2-codex-20260130T203521Z
- What was implemented: Added a shared write-if-changed helper with pre-write hooks, switched atomic writes to temp files in the target directory, and refactored apply/init/catalog/fetch/TUI writes to use it.
- Files changed: core/src/lib.rs; core/src/catalog.rs; adapters/shared/src/fetch.rs; tui/src/state.rs; docs/v0.2-checklist.md; scripts/Ralph/prd.json; scripts/Ralph/progress.md
- Thread: local CLI session (no external thread URL available).
- **Learnings for future iterations:**
  - Patterns discovered: Pre-write hooks let backups run before atomic renames while keeping write logic centralized.
  - Gotchas encountered: Temp files must live in the target directory to keep renames atomic across filesystems.
  - Useful context: Tests should assert unchanged writes don't touch mtimes to catch accidental rewrites.
---
## 2026-01-30T23:10:00Z - L2-MERGE-001
Run: codex-gpt-5.1-codex-mini-20260130T204355Z
- What was implemented: Plumbed user-scope support into the core apply pipeline (allow flag, user backups/reporting, CLI wiring) and added the Claude `~/.claude.json` MCP merge helper with tests, backed by the new `apply_plan` adjustments and plan builder updates.
- Files changed: `core/src/lib.rs`; `core/src/plan/builders.rs`; `core/tests/plan_apply_integration.rs`; `adapters/claude/src/adapter.rs`; `adapters/claude/src/user_mcp_merge.rs`; `cli/src/main.rs`; `scripts/Ralph/prd.json`.
- **Learnings for future iterations:**
  - Patterns discovered: User-scope merges should emit explicit backup actions so apply can skip duplicate snapshots and the new `user_backup_report` keeps user backups visible in the summary.
  - Gotchas encountered: Tests that mutate `HOME` need cross-test serialization (the new mutex guard); otherwise parallel runs leaked env state.
  - Useful context: CLI/apply now support `--allow-user-scope` so future consent flows can reuse the same plumbing and highlight user backups in the report.
- Thread: local CLI session (no external thread URL available).
---
## 2026-01-30 21:11:46Z - L2-MERGE-002
Run: codex-gpt-5.2-codex-20260130T210519Z
- What was implemented: Added an opt-in Gemini user-level MCP merge that patches only missing mcpServers entries in ~/.gemini/settings.json, backed by user backups, plus config plumbing and tests.
- Files changed:
  - core/src/config/mod.rs
  - adapters/gemini/src/map.rs
  - adapters/gemini/src/user_mcp_merge.rs
  - adapters/gemini/src/lib.rs
  - adapters/gemini/src/adapter.rs
  - scripts/Ralph/prd.json
  - scripts/Ralph/progress.md
- **Learnings for future iterations:**
  - Patterns discovered: User-scope MCP merges can reuse the same missing-key-only patch pattern as Claude while keeping the adapter opt-in.
  - Gotchas encountered: Gemini user settings live under ~/.gemini/settings.json, so tests must create the nested directory before writing fixtures.
  - Useful context: validate_mcp_folder can be reused to read MCP server metadata without planning a project-level .mcp.json write.
Thread: local CLI session (no external thread URL available).
---
## 2026-01-30 23:59:00Z - L2-TUI-010
Run: codex-gpt-5.1-codex-mini-20260130T211302Z
- What was implemented: Added an Apply summary/consent flow in the TUI that builds the plan, shows project vs user counts plus a backup preview, lets the user type YES to confirm user-scope ops, and then invokes `macc_core::apply_plan` with the consent decision while guarding the existing key bindings and preview messages.
- Files changed:
  - tui/src/state.rs
  - tui/src/lib.rs
  - tui/src/screen.rs
  - scripts/Ralph/prd.json
  - scripts/Ralph/progress.md
- **Learnings for future iterations:**
  - Patterns discovered: Centralizing plan materialization in an `ApplyContext` lets the UI reuse the same summary/diffs for consent and the actual apply call without rerunning the whole engine each time.
  - Gotchas encountered: The new apply screen needed a higher-priority `Char` arm that still lets Esc/q behave normally and prevented the existing `'s'` shortcut from hijacking the YES input buffer.
  - Useful context: Validated with `cargo fmt`, `cargo clippy -- -D warnings`, and `cargo test` so the refactor didn’t regress downstream adapters.
Thread: local CLI session (no external thread URL available).
---
## 2026-01-30 23:59:59Z - L2-APPLY-001
Run: codex-gpt-5.1-codex-mini-20260130T212455Z
- What was implemented: Added `apply_operations` (with warnings, backups, user-consent enforcement, and an optional progress hook) so the CLI and TUI share the same deterministic apply pipeline, and rewired the TUI to validate the plan, call the helper, track progress in state, and display current/total op counts on the Apply screen.
- Files changed:
  - core/src/lib.rs
  - tui/src/state.rs
  - tui/src/lib.rs
  - scripts/Ralph/prd.json
  - scripts/Ralph/progress.md
- Thread: local CLI session (no external thread URL available).
- **Learnings for future iterations:**
  - Patterns discovered: Replaying the shared `PlannedOp` sequence with an optional progress callback keeps CLI and TUI outputs, backups, and consent checks synchronized without duplicating filesystem logic.
  - Gotchas encountered: `.gitignore` still needs explicit backup handling even when using the aggregated op list, so the helper checks for `.gitignore` changes before calling the atomic writer.
  - Useful context: `ApplyProgress` now captures the last operation path so the summary can report `current/total` even when the screen is redrawn after the run completes.
---
## 2026-01-30T21:54:24Z - L2-MCP-001
Run: codex-gpt-5.1-codex-mini-20260130T214554Z
- What was implemented: Added canonical MCP template schema + defaults, validation/tests, and exposed the definitions via ResolvedConfig so future MCP workflows can reference template metadata.
- Files changed:
  - core/src/config/mod.rs
  - core/src/resolve/mod.rs
  - core/tests/plan_operations.rs
  - core/src/tool/mod.rs
  - adapters/claude/src/map.rs
  - adapters/gemini/src/map.rs
  - docs/CATALOGS.md
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Canonical defaults can seed template metadata automatically via `serde(default)` so missing sections still work without extra plumbing.
  - Gotchas encountered: Adding resolved fields requires touching every instantiation (tests/adapters/tool registry) or the compiler refuses to build.
  - Useful context: The MCP schema docs now spell out the placeholder-only policy, matching the new `mcp_templates` entries and keeping secrets out of `.macc/macc.yaml`.
Thread: local CLI session (no external thread URL available).
---
## 2026-01-30 23:59:59Z - L2-MCP-002
Run: codex-gpt-5.1-codex-mini-20260130T215815Z
- What was implemented: Added a core `mcp_json` renderer that materializes `.mcp.json` from the selected template IDs (ordered args/env placeholders, newline-terminated output) and updated the Claude adapter to emit that file only when templates are chosen; smoke-tested the golden serialization via a unit test.
- Files changed:
  - core/src/mcp_json.rs
  - core/src/lib.rs
  - adapters/claude/src/adapter.rs
  - scripts/Ralph/prd.json
  - scripts/Ralph/progress.md
- **Learnings for future iterations:**
  - Patterns discovered: Centralize generated config renderers in core so adapters remain thin and share deterministic formatting.
  - Gotchas encountered: `.mcp.json` should only be written when template selections match canonical definitions (remote-only IDs must be skipped to keep planner idempotent).
  - Useful context: The golden-output test now guards the JSON layout/ordering we rely on for previews.
Thread: local CLI session (no external thread URL available).
---
## 2026-01-30T22:45:00Z - L2-MCP-003
Run: gemini-gemini-3-flash-preview-20260130T221951Z
- What was implemented: Integrated .mcp.json generation into the Claude adapter, ensuring it includes both user-defined templates and remote MCP servers from the catalog; added documentation for .mcp.json in the generated CLAUDE.md; implemented a comprehensive CLI integration test.
- Files changed:
  - core/src/mcp_json.rs
  - adapters/claude/src/adapter.rs
  - adapters/claude/src/map.rs
  - adapters/claude/src/emit/claude_md.rs
  - cli/src/main.rs
- **Learnings for future iterations:**
  - Patterns discovered: Refactoring core renderers to expose low-level "value" conversion helpers allows adapters to easily merge project-specific and global configurations before final serialization.
  - Gotchas encountered: Integration tests for tools that perform user-scope merges (like Claude) require mocking the HOME directory to avoid side effects and validation failures during apply.
  - Useful context: Claude Code and other project-level Claude tools look for .mcp.json in the root; documenting this in CLAUDE.md via the @ prefix ensures the AI knows it has access to these tools.
Thread: local CLI session (no external thread URL available).
---
## 2026-01-30T22:55:00Z - L2-MCP-004
Run: gemini-gemini-3-flash-preview-20260130T224911Z
- What was implemented: Updated Gemini adapter to render selected MCP servers (both from templates and local folders) into .gemini/settings.json; documented secret handling in GEMINI.md; added unit tests for the settings renderer.
- Files changed:
  - adapters/gemini/src/map.rs
  - adapters/gemini/src/emit/settings_json.rs
  - adapters/gemini/src/emit/gemini_md.rs
  - adapters/gemini/src/adapter.rs
- **Learnings for future iterations:**
  - Patterns discovered: Using BTreeMap for JSON objects (like mcpServers) ensures deterministic output order, which is crucial for idempotence and clean git diffs.
  - Gotchas encountered: Remember to explicitly import crates like serde_json even if they are used via full path in some places, to avoid compilation errors in all modules.
  - Useful context: Gemini's settings.json is the source of truth for its workspace configuration, unlike Claude which uses .mcp.json for tools.
Thread: local CLI session (no external thread URL available).
---
## 2026-01-30T23:55:00Z - L2-RALPH-001
Run: gemini-gemini-3-flash-preview-20260130T222839Z
- What was implemented: Defined Ralph automation workflow in docs/ralph.md and added the automation.ralph configuration schema to CanonicalConfig. Updated all manual initializations and tests to include the new field.
- Files changed:
  - docs/ralph.md
  - core/src/config/mod.rs
  - core/src/lib.rs
  - core/src/resolve/mod.rs
  - README.md
  - docs/v0.2-checklist.md
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Adding a new field to a central struct like CanonicalConfig requires updating all manual struct initializations (common in tests) or utilizing Default/functional update syntax where possible.
  - Gotchas encountered: Missing a field in a struct initializer causes a compilation error (E0063), making it easy to spot all places that need an update.
  - Useful context: Ralph's sequence is tool-agnostic but depends on a structured progress.md and memory-bank/ to maintain state across iterations.
Thread: local CLI session (no external thread URL available).
---
## 2026-01-30T23:15:00Z - L2-RALPH-002
Run: gemini-gemini-3-flash-preview-20260130T223201Z
- What was implemented: Integrated Ralph script generation into the MACC apply pipeline. Added a new 'SetExecutable' action to the core plan to support making generated scripts executable on Unix-like systems. The generated scripts/ralph.sh follows the documented workflow sequence and is configurable via .macc/macc.yaml. Added a comprehensive integration test for the script generation.
- Files changed:
  - core/src/resolve/mod.rs
  - core/src/plan/mod.rs
  - core/src/plan/ops.rs
  - core/src/lib.rs
  - core/tests/plan_operations.rs
  - adapters/claude/src/map.rs
  - adapters/gemini/src/map.rs
  - core/src/mcp_json.rs
  - core/src/tool/mod.rs
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Adding specialized actions (like SetExecutable) to the ActionPlan allows the core apply engine to handle platform-specific side effects while keeping adapters pure.
  - Gotchas encountered: When updating central structs like ResolvedConfig, ensure all manual test initializations are updated; otherwise, the compiler will (rightfully) complain about missing fields. Be careful with 'replace' tool placeholders like '// ...' as they can lead to accidental code deletion if not handled properly.
  - Useful context: scripts/ralph.sh is a project-scoped entry point for autonomous agents, acting as a skeleton that guides them through the MACC development loop.
---

---
## 2026-01-30T22:45:00Z - L2-TUI-011
Run: gemini-gemini-3-flash-preview-20260130T223814Z
- What was implemented: Added a toggleable help overlay in the TUI, accessible by pressing '?'. The overlay displays both global navigation keys and screen-specific keybindings. Updated AppState to manage help visibility and Screen to provide context-aware help descriptions.
- Files changed:
  - tui/src/lib.rs
  - tui/src/screen.rs
  - tui/src/state.rs
  - adapters/gemini/src/map.rs (unrelated clippy fix)
- **Learnings for future iterations:**
  - Patterns discovered: Using a centralized help definition in the Screen enum ensures consistency across the TUI and makes it easy to add help for new screens.
  - Gotchas encountered: When using 'replace' with large blocks and placeholders like '// ...', ensure they don't accidentally delete real code or introduce syntax errors by missing braces.
  - Useful context: Centered popups in Ratatui can be easily achieved using a Layout with percentage-based constraints on both axes.
---
## 2026-01-30T23:45:00Z - L2-DOCTOR-001
Run: gemini-gemini-3-flash-preview-20260130T224238Z
- What was implemented: Integrated basic tool availability checks into the TUI. Implemented a mockable detection wrapper in a new 'doctor' module in macc-core that uses 'which'/'where' to locate binaries. Updated AppState to maintain tool status and added a 'Refresh Checks' action (key 'd') in the Tools screen. Status badges are now displayed next to each tool in the selection list.
- Files changed:
  - core/src/doctor.rs
  - core/src/lib.rs
  - tui/src/state.rs
  - tui/src/lib.rs
  - tui/src/screen.rs
  - README.md
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Using a trait (CommandRunner) for OS-level interactions like binary detection allows for clean unit testing without depending on the environment.
  - Gotchas encountered: Remember to handle trailing whitespace carefully in TUI rendering code as rustfmt might fail if it's not consistent with the project's formatting rules.
  - Useful context: The doctor checks are currently basic binary presence checks; future iterations can expand this to version checks or configuration validation.## 2026-01-30T23:55:00Z - L2-TEST-001
Run: gemini-gemini-3-flash-preview-20260130T224924Z
- What was implemented: Refactored TUI state management logic into pure helper functions (next_index, prev_index, toggle_vec_item, cycle_value) and unified navigation methods in AppState. Added comprehensive unit tests for these reducers and a golden test for YAML serialization stability and idempotence.
- Files changed:
  - tui/src/state.rs
  - tui/src/lib.rs
- **Learnings for future iterations:**
  - Patterns discovered: Consolidating screen-specific navigation into AppState methods (navigate_next, navigate_prev, etc.) significantly simplifies the event loop in handle_key and makes the TUI logic more testable in isolation.
  - Gotchas encountered: When writing golden tests for YAML serialization, ensure that collections like Vec are explicitly sorted if the application logic expects deterministic ordering, as default serialization preserves insertion order.
  - Useful context: Using pure functions for index arithmetic and collection manipulation allows for easy unit testing without the overhead of the full AppState.
---
## 2026-01-30T23:05:00Z - L2-TEST-002
Run: gemini-gemini-3-flash-preview-20260130T225800Z
- What was implemented: Added a comprehensive integration test for .mcp.json generation in core/tests/plan_apply_integration.rs. Updated the core TestAdapter to support .mcp.json generation when MCP servers are selected, ensuring the core apply logic can be tested in isolation from specific tool adapters. Verified both initial generation and idempotence.
- Files changed:
  - core/src/tool/mod.rs
  - core/tests/plan_apply_integration.rs
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Updating the TestAdapter to support new core features (like MCP templates) allows for robust integration testing within the macc-core crate without depending on external adapter crates.
  - Gotchas encountered: Ensure the TestAdapter's plan method correctly receives and uses the PlanningContext to access resolved configuration like MCP selections.
  - Useful context: .mcp.json generation is deterministic and idempotent, and the integration test verifies this by checking the ActionStatus on a second apply run.
---

---
## 2026-01-30T23:58:00Z - L2-DOCS-001
Run: gemini-gemini-3-flash-preview-20260130T230201Z
- What was implemented: Updated the README with comprehensive documentation for MACC v0.2. This includes a new Quick Start section, documentation for the interactive TUI (macc tui), detailed explanation of project vs. user-level backups and consent rules, MCP template philosophy (placeholder-only, cross-tool), and instructions for running the Ralph automation via scripts/ralph.sh. Reorganized the README for better flow and added cross-links to detailed documentation files.
- Files changed:
  - README.md
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Standardizing the README structure around "Quick Start", "Core Commands", and "Safety/Security" provides a clear entry point for new users while maintaining deep-dive sections for experts.
  - Gotchas encountered: Ensure that all cross-linked documentation files actually exist and contain the information they are supposed to; checked docs/ralph.md and docs/v0.2-checklist.md during this task.
  - Useful context: User-level backups are stored under ~/.macc/backups/ mirroring the home-relative path, providing a familiar and safe recovery path.

---
## 2026-01-31T03:15:00Z - L2-AUDIT-001
Run: gemini-gemini-3-flash-preview-20260131T025950Z
- What was implemented: Performed a comprehensive audit of tool-specific references in the `tui/` and `cli/` crates. Identified hard-coded tool IDs ("claude", "gemini", "codex") primarily in test suites and help text. Documented findings and established a refactor strategy in `docs/tool-agnostic-audit.md`.
- Files changed:
  - docs/tool-agnostic-audit.md
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: The TUI and CLI core logic is already largely tool-agnostic, relying on the `ToolRegistry` for descriptors. The main coupling exists in tests that use hard-coded tool IDs for configuration setup.
  - Gotchas encountered: Some integration tests in `cli/src/main.rs` assume specific tool-specific subdirectory structures (e.g., `.gemini/skills`), which should be generalized in later tasks.
  - Useful context: `macc-registry` is the central point where adapters are registered; future work might involve mocking this registry for purely tool-agnostic testing of the UI/CLI.
---

---
## 2026-01-31T03:30:00Z - L2-SCHEMA-001
Run: gemini-gemini-3-flash-preview-20260131T030118Z
- What was implemented: Defined the `ToolSpec` schema and its corresponding Rust structs in `macc-core`. This includes `FieldSpec` for configuration mapping via JSON pointers, `DoctorCheckSpec` for pre-flight environment checks, and support for both YAML and JSON parsing. Implemented strict validation for API versioning, kebab-case IDs, and JSON pointer formats. Documented the schema with examples in `docs/TOOLSPEC.md`.
- Files changed:
  - core/src/tool/spec.rs
  - core/src/tool/mod.rs
  - docs/TOOLSPEC.md
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Using a dedicated `spec` module within the `tool` package keeps the data-driven parts of the tool API separate from the adapter traits.
  - Gotchas encountered: Flattened fields in `serde` (like in `ToolsConfig`) can sometimes lead to unexpected successful parsing of partial data, which might cause tests expecting failure to pass if not carefully constrained.
  - Useful context: The ToolSpec is designed to be the bridge between static tool metadata (stored in files) and the dynamic ToolDescriptor used by the TUI/CLI.
---
## 2026-01-31T11:11:55Z - L2-REGISTRY-001
Run: gemini-gemini-3-flash-preview-20260131T111155Z
- What was implemented: Created a data-driven `ToolSpecLoader` in `macc-core` with support for overlay precedence (User > Project > Built-in). Established the `registry/tools.d/` convention for built-in specs and seeded it with definitions for Claude, Gemini, and Codex. The loader supports both YAML and JSON, provides structured diagnostics for parse/validation errors, and ensures deterministic output ordering by tool ID.
- Files changed:
  - core/src/tool/loader.rs
  - core/src/tool/mod.rs
  - registry/tools.d/claude.tool.yaml
  - registry/tools.d/gemini.tool.yaml
  - registry/tools.d/codex.tool.yaml
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Using a `BTreeMap` during the overlay loading phase automatically handles both tool overriding (via `insert`) and deterministic output ordering (via its natural key sort).
  - Gotchas encountered: When loading from multiple directories, ensure that directories that don't exist are skipped silently (or with a log) to avoid breaking when optional overlays (like user config) are missing.
  - Useful context: `ToolSpecLoader::default_search_paths` provides a canonical order of precedence that should be used by all discovery mechanisms.
---

## 2026-01-31T11:20:00Z - L2-REGISTRY-002
Run: gemini-gemini-3-flash-preview-20260131T111554Z
- What was implemented: Refactored `macc_registry::tool_descriptors()` to be completely data-driven using the `ToolSpecLoader`. Eliminated all hard-coded tool descriptors from the registry and adapter crates. Switched `FieldKindSpec` to internal tagging for robust YAML serialization across struct and unit variants. Ensured stable output ordering by sorting descriptors by title.
- Files changed:
  - registry/src/lib.rs
  - core/src/tool/spec.rs
  - registry/tools.d/claude.tool.yaml
  - registry/tools.d/codex.tool.yaml
  - registry/tools.d/gemini.tool.yaml
  - adapters/claude/src/lib.rs
  - adapters/codex/src/lib.rs
  - adapters/gemini/src/lib.rs
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Using `#[serde(tag = "type", rename_all = "snake_case")]` (internal tagging) on enums significantly improves `serde_yaml` compatibility for mixed unit and struct variants, avoiding the need for non-standard YAML tags like `!variant`.
  - Gotchas encountered: `serde_yaml` (v0.9) defaults to YAML tags for externally tagged enums containing struct variants, which is often not what's desired for human-readable config files.
  - Useful context: A fallback mechanism in the registry to search for `registry/tools.d` upwards from the current directory ensures that tool discovery works even in uninitialized repositories during development and testing.
---

## 2026-01-31T12:25:51Z - L2-CONFIG-001
Run: gemini-gemini-3-flash-preview-20260131T122551Z
- What was implemented: Introduced `tools.config` map in `CanonicalConfig` for tool-extensible configuration. Updated `ResolvedConfig` and adapters (Claude, Gemini) to prefer settings from the `config` map while maintaining backward compatibility with flattened settings. Ensured stable YAML serialization using `BTreeMap`. Documented the canonical configuration structure in `docs/CONFIG.md`.
- Files changed:
  - core/src/config/mod.rs
  - core/src/resolve/mod.rs
  - adapters/claude/src/map.rs
  - adapters/gemini/src/map.rs
  - docs/CONFIG.md
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Using a dedicated `config` map keyed by tool ID allows for arbitrary per-tool settings without modifying core Rust structs, facilitating tool-agnostic development.
  - Gotchas encountered: Serde's `#[serde(flatten)]` can make all fields in a struct optional if not careful; specifically, if a flattened map exists, fields without `#[serde(default)]` that were previously required might become optional in the eyes of the parser if they can be absorbed into the flattened map. Re-asserting required fields (like `tools.enabled`) is important.
  - Useful context: `BTreeMap` is preferred over `HashMap` for configuration maps to ensure deterministic, alphabetical key ordering in YAML/JSON output, which is critical for clean git diffs.
---

---
## 2026-01-31T12:45:00Z - L2-CONFIG-002
Run: gemini-gemini-3-flash-preview-20260131T123049Z
- What was implemented: Introduced a configuration migration module in `macc-core` and a corresponding `macc migrate` CLI subcommand. The migration identifies legacy tool-specific fields (e.g., `tools.claude`) and moves them to the new `tools.config` map. Added proactive warnings to `macc plan` and `macc apply` when legacy configurations are detected, prompting users to run the migration.
- Files changed:
  - core/src/config/migrate.rs
  - core/src/config/mod.rs
  - core/src/lib.rs
  - cli/src/main.rs
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Using a dedicated `MigrationResult` struct that contains both the migrated config and a list of warnings allows for a clean separation between the migration logic and how it's reported to the user.
  - Gotchas encountered: Ensure module exports in `lib.rs` match the intended public API path; `pub use config::migrate` provides a cleaner path than deeply nested module structures.
  - Useful context: `serde(flatten)` is a powerful tool for backward compatibility, allowing old fields to be captured and then programmatically moved during a migration phase.
---

---
## 2026-01-31T12:35:03Z - L2-ENGINE-001
Run: gemini-gemini-3-flash-preview-20260131T123319Z
- What was implemented: Introduced `MaccEngine` in `macc-core`, a centralized facade for CLI and TUI to interact with core logic. It provides methods for tool discovery (`list_tools`), diagnostics (`doctor`), planning (`plan`, `plan_operations`), and application (`apply`). This decoupling ensures that UI code does not need to depend on tool adapters or internal planning details.
- Files changed:
  - core/src/engine.rs
  - core/src/lib.rs
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Using a facade struct like `MaccEngine` effectively encapsulates complex orchestration (config resolution -> planning -> materialization) into a simple, UI-friendly API.
  - Gotchas encountered: Ensure that `list_tools` includes diagnostics from the `ToolSpecLoader` so the UI can report why certain tools failed to load.
  - Useful context: The `MaccEngine` is designed to be the primary interface for both the CLI and the upcoming TUI, ensuring consistent behavior across different user interfaces.
---

## 2026-01-31T13:30:00Z - L2-ENGINE-002
Run: gemini-gemini-3-flash-preview-20260131T133000Z
- What was implemented:
  - Introduced the `Engine` trait in `macc-core` to decouple UI from core orchestration.
  - Implemented `TestEngine` with in-memory `ToolSpec` fixtures and `MockAdapter` for stable, tool-agnostic UI testing.
  - Refactored `ToolAdapter` trait to return `String` for IDs, allowing for dynamic mock IDs in tests.
  - Migrated `macc-tui` (`AppState`) to use the `Engine` trait and injected `Arc<dyn Engine>`.
  - Updated all TUI unit tests to use `TestEngine` with fixtures (`fixture-tool-1`, `fixture-tool-2`), eliminating hard-coded tool dependencies in TUI tests.
- Files changed:
  - core/src/engine.rs
  - core/src/lib.rs
  - core/src/tool/mod.rs
  - adapters/claude/src/adapter.rs
  - adapters/codex/src/adapter.rs
  - adapters/gemini/src/adapter.rs
  - tui/src/lib.rs
  - tui/src/state.rs
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Using an `Engine` trait as a facade allows for easy swapping of production logic with mocked behavior, which is essential for UI testing without filesystem side effects.
  - Gotchas encountered: Changing a core trait like `ToolAdapter` requires updating all adapter implementations across the workspace. Ensure all crates are included in the refactor.
  - Useful context: `TestEngine::with_fixtures()` provides a canonical set of mock tools that should be used by all UI-level tests to ensure they remain tool-agnostic.
---

## 2026-01-31T12:46:19Z - L2-TUI-002
Run: gemini-gemini-3-flash-preview-20260131T124619Z
- What was implemented:
  - Centralized `BUILTIN_SKILLS` and `BUILTIN_AGENTS` in `macc-core::catalog`.
  - Added `builtin_skills()` and `builtin_agents()` to the `Engine` trait to enable tool-agnostic UI testing.
  - Refactored `macc-tui` (`AppState` and rendering) to source skills and agents from the injected `Engine` instead of local constants.
  - Updated TUI unit tests to use `TestEngine` with mock skill/agent fixtures, removing hard-coded dependencies on real tool names in UI tests.
- Files changed:
  - core/src/catalog.rs
  - core/src/engine.rs
  - tui/src/lib.rs
  - tui/src/state.rs
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Moving static metadata to the `Engine` trait allows the UI to remain entirely ignorant of specific tools, facilitating pure fixture-based testing.
  - Gotchas encountered: When refactoring `with_engine` to call methods on an `Arc<dyn Engine>`, ensure methods are called *before* moving the `Arc` into the struct to avoid borrow-after-move errors.
  - Useful context: `TestEngine` should be the default for all TUI/CLI unit tests that touch tool metadata to ensure they remain stable as real tool definitions evolve.
---

---
## 2026-01-31T12:54:56Z - L2-TUI-003
Run: gemini-gemini-3-flash-preview-20260131T125456Z
- What was implemented:
  - Refactored TUI actions to be fully spec-driven, eliminating hard-coded string prefixes.
  - Introduced structural `ActionKind` (core) and `ActionSpec` (spec) enums for typed action payloads.
  - Updated `ToolSpec` schema to represent actions as objects (e.g., `type: action, action: open_skills, target_pointer: ...`).
  - Updated all tool YAML specs in `registry/tools.d/` to use the new structural action format.
  - Refactored TUI `handle_action()` to match on structured `ActionKind` variants.
- Files changed:
  - core/src/tool/descriptor.rs
  - core/src/tool/spec.rs
  - core/src/engine.rs
  - tui/src/state.rs
  - registry/tools.d/claude.tool.yaml
  - registry/tools.d/codex.tool.yaml
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Using tagged enums in Serde for both field kinds and action kinds provides a clean, self-documenting YAML schema that is validated at load time.
  - Gotchas encountered: When refactoring common schemas, ensure all YAML files in the registry are updated, as they will fail to load and might cause silent failures or test breakages if diagnostics are ignored.
  - Useful context: `ToolSpecLoader` diagnostics are critical but often swallowed in high-level calls; checking them during development or via CLI tools helps identify YAML parsing issues quickly.
---

## 2026-01-31T14:38:18Z - L2-CLI-001
Run: gemini-gemini-3-pro-preview-20260131T133818Z
- What was implemented:
  - Refactored `cli/src/main.rs` to use `MaccEngine` and `TestEngine` from `macc-core`.
  - Introduced `run_with_engine` to allow dependency injection for tests.
  - Replaced direct calls to `macc_registry` and `macc_core` planning functions with `Engine::plan` and `Engine::apply`.
  - Updated CLI tests to use `TestEngine::with_fixtures()` and generic mock tools ("fixture-tool-1"), eliminating hard-coded references to "claude", "codex", and "gemini".
  - Generalized `macc_core::plan::builders::plan_skill_install` to handle unknown tools dynamically (using `.{tool}` convention), allowing mock tools to work without registry changes.
  - Removed tool-specific integration test (`test_claude_mcp_json_generation`) from CLI as it tested adapter logic, not CLI wiring.
- Files changed:
  - cli/src/main.rs
  - core/src/plan/builders.rs
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Inverting control in `main` (via `run_with_engine`) makes CLI testing much easier and cleaner, avoiding global state or filesystem side-effects (when using `TestEngine`).
  - Gotchas encountered: `plan_skill_install` in core had hardcoded tool validations. Making core components more generic (e.g., fallback paths for unknown tools) is crucial for flexibility and testing with mocks.
  - Useful context: The CLI now acts purely as a wiring layer for the Engine, consistent with the TUI architecture.
---

## 2026-01-31T15:38:18Z - L2-DOCTOR-001
Run: gemini-gemini-3-pro-preview-20260131T133818Z
- What was implemented:
  - Updated `core/src/doctor.rs` to support spec-driven checks using `DoctorCheckSpec`.
  - Implemented `CheckRunner` trait with `which` and `path_exists` support.
  - Implemented `checks_for_enabled_tools` to generate checks dynamically from `ToolSpecs`.
  - Updated `Engine::doctor` signature to take `&ProjectPaths` for future project-specific checks.
  - Updated `MaccEngine` and `TestEngine` to use the new doctor logic.
  - Updated TUI to pass project paths to doctor and handle the new `ToolCheck` structure (including `ToolStatus::Error`).
  - Added `doctor` sections to `claude.tool.yaml`, `codex.tool.yaml`, and `gemini.tool.yaml` to verify binary presence.
- Files changed:
  - core/src/doctor.rs
  - core/src/engine.rs
  - tui/src/state.rs
  - tui/src/lib.rs
  - registry/tools.d/*.tool.yaml
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Adding a `kind` field to `ToolCheck` (matching `DoctorCheckSpec`) allows the runtime to know *how* to execute the check without re-parsing the spec.
  - Gotchas encountered: Changing a core struct like `ToolCheck` ripples through the TUI and tests; ensuring all pattern matches are exhaustive (like for `ToolStatus::Error`) is critical.
  - Useful context: The `doctor` system is now fully data-driven; adding a new tool check is as simple as updating the YAML spec.
---
## 2026-01-31T16:38:18Z - L2-TESTS-001
Run: gemini-gemini-3-flash-preview-20260131T141023Z
- What was implemented:
  - Replaced all tool-specific names ("claude", "gemini", "opus") with generic mock names ("fixture-tool-1", "fixture-tool-2", "smart") in TUI and CLI unit tests.
  - Refactored `tui/src/state.rs` tests to use `TestEngine::with_fixtures()` for tool-agnostic state initialization.
  - Updated `cli/src/main.rs` doc comments to use generic tool examples.
  - Added `check-generic` target to `Makefile` to enforce the absence of forbidden tool strings in `tui/` and `cli/` source code.
  - Fixed linting issues (unused imports) and formatting in `cli` and `core`.
- Files changed:
  - tui/src/state.rs
  - cli/src/main.rs
  - Makefile
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Using `grep` in a `Makefile` target is an effective way to enforce architectural constraints (like tool-agnosticism) at build time.
  - Gotchas encountered: Removing imports from the top of a file might break tests if they rely on those imports; moving them to `mod tests` or using `#[cfg(test)]` is safer.
  - Useful context: `TestEngine::with_fixtures()` provides a stable set of mock tools, skills, and agents that should be preferred for all UI-layer testing.
---

## 2026-01-31T17:38:18Z - L2-TESTS-002
Run: gemini-gemini-3-flash-preview-20260131T173818Z
- What was implemented:
  - Added core contract tests in `registry/tests/contract.rs` that iterate over all registered adapters in `ToolRegistry`.
  - Implemented generic assertions for all adapters: non-panicking planning, deterministic output, path safety (no absolute paths or `..` in project scope), and valid scope labeling.
  - Added a check for forbidden operations (e.g., `Noop` actions are disallowed for production adapters).
  - Updated `Makefile` with a `test-contract` target and ensured `make test` runs tests for both the root workspace and the `adapters/` workspace.
- Files changed:
  - registry/tests/contract.rs
  - Makefile
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Using `ToolRegistry::list_ids()` combined with `get()` allows for truly tool-agnostic contract testing that automatically covers new adapters as they are registered.
  - Gotchas encountered: Adapters may not return normalized plans (sorted actions) by default; the test should either normalize before comparison or strictly check the adapter's own deterministic ordering.
  - Useful context: The engine already normalizes the combined plan, so adapters only need to be internally deterministic.
---

## 2026-01-31T18:38:18Z - L2-GUARD-001
Run: gemini-gemini-3-flash-preview-20260131T183818Z
- What was implemented:
  - Created `scripts/ui-denylist.txt` with forbidden tool names (claude, gemini, codex, etc.).
  - Implemented `scripts/check-ui-tool-transparency.sh` to scan `tui/` and `cli/` for these strings.
  - Updated `Makefile` to use the new scanner in the `check-generic` target.
  - Documented the guardrail in `CONTRIBUTING.md` and `docs/CONTRIBUTING.md`.
- Files changed:
  - scripts/ui-denylist.txt
  - scripts/check-ui-tool-transparency.sh
  - Makefile
  - CONTRIBUTING.md
  - docs/CONTRIBUTING.md
- **Learnings for future iterations:**
  - Patterns discovered: Using a dedicated denylist file and a wrapper script for `grep` makes it easy to update forbidden strings without modifying the build system (Makefile) directly.
  - Gotchas encountered: Ensure the script resolves paths relative to the repo root to avoid issues when run from different directories.
  - Useful context: This guardrail prevents "brand creep" in the UI layer, ensuring that MACC remains truly tool-agnostic at the presentation level.
---

## 2026-01-31T19:38:18Z - L2-DOCS-001
Run: gemini-gemini-3-flash-preview-20260131T193818Z
- What was implemented:
  - Created `docs/ADDING_TOOLS.md` with an end-to-end guide for adding tools.
  - Documented `ToolSpec` YAML structure, field types, and JSON pointers.
  - Explained adapter integration via `ToolAdapter` trait and registry registration.
  - Included instructions for running contract tests to ensure safety and determinism.
  - Verified that adding a YAML spec to `registry/tools.d/` automatically makes it appear in descriptors used by TUI.
- Files changed:
  - docs/ADDING_TOOLS.md
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: The data-driven `ToolSpec` approach combined with a centralized registry and contract tests provides a very high degree of decoupling between tool-specific logic and the UI/Engine core.
  - Gotchas encountered: If an adapter is registered but its `id()` doesn't match the `id` in its corresponding `ToolSpec`, it might lead to confusion (though the system still works if properly wired). Consistency in naming is key.
  - Useful context: `registry/src/lib.rs` is the main wiring point for adapters, while `registry/tools.d/` is the discovery point for UI descriptors.
---

---
## 2026-01-31T20:38:18Z - L2-REGISTRY-003
Run: gemini-gemini-3-flash-preview-20260131T142153Z
- What was implemented:
  - Created generic ToolSpec fixtures in `core/tests/fixtures/tools.d/`: `fixture-tool-1.tool.yaml` and `fixture-tool-2.tool.yaml`.
  - Fixtures cover all field types: `bool`, `enum`, `text`, and `action` (OpenMCP), plus `doctor` checks.
  - Added sample canonical configs in `core/tests/fixtures/configs/` to demonstrate valid YAML configurations for these mocks.
  - Updated `TestEngine::with_fixtures()` in `core/src/engine.rs` to load these specs using `include_str!`, ensuring tests use the same source-of-truth as the YAML definitions.
  - Refactored TUI and Core unit tests to accommodate the richer field structure and different JSON pointers in the new fixtures.
  - Fixed linting warnings (unused imports) in `core/src/doctor.rs`.
- Files changed:
  - core/tests/fixtures/tools.d/fixture-tool-1.tool.yaml
  - core/tests/fixtures/tools.d/fixture-tool-2.tool.yaml
  - core/tests/fixtures/configs/sample-one.yaml
  - core/tests/fixtures/configs/sample-two.yaml
  - core/src/engine.rs
  - core/src/doctor.rs
  - tui/src/state.rs
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Using `include_str!` to embed YAML fixtures directly into the `TestEngine` is a clean way to keep tests portable and fast while still benefitting from externalized, human-readable spec definitions.
  - Gotchas encountered: Changing mock structures ripples through TUI tests that assert on specific field indices or JSON pointers; explicitly checking for `tool_id` in doctor tests is more robust than checking the `check_target` string.
  - Useful context: The `TestEngine` is now fully data-driven by the same YAML format used by production tools, providing a high-fidelity environment for UI testing without real tool side-effects.

---
## 2026-01-31T21:38:18Z - L2-UI-001
Run: gemini-gemini-3-flash-preview-20260131T213818Z
- What was implemented:
  - Refactored TUI and CLI to use tool display names (titles) instead of IDs for rendering.
  - Updated TUI Home screen to map enabled tool IDs to their descriptive titles.
  - Removed tool IDs from the TUI Tools list to prevent brand/ID leakage.
  - Updated CLI 'plan', 'apply', and 'install skill' commands to use tool titles in their status messages.
  - Verified that no hardcoded tool ID branching exists in the UI layer.
  - Passed the UI transparency guardrail check.
- Files changed:
  - tui/src/lib.rs
  - cli/src/main.rs
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Mapping IDs to titles using a registry lookup is a common pattern when the source of truth uses IDs but the UI should show user-friendly names.
  - Gotchas encountered: When replacing large code blocks in `cli/src/main.rs`, ensure all required variables (like `materialized_units`) are still properly initialized in all branches.
  - Useful context: `ToolDescriptor::title` is derived from `ToolSpec::display_name`, making it the correct field for metadata-driven rendering.
---

## 2026-01-31T22:38:18Z - L2-PATHS-001
Run: gemini-gemini-3-flash-preview-20260131T223818Z
- What was implemented:
  - Defined canonical JSON pointer roots: `/tools/enabled`, `/tools/config/<id>/`, `/selections/skills`, `/selections/agents`, `/selections/mcp`, `/standards/path`, and `/standards/inline/`.
  - Implemented strict pointer validation in `ToolSpec::validate` using these roots.
  - Updated all existing tool specifications (`claude`, `gemini`, `codex`) to use the new `/tools/config/<id>/` and global `/selections/` pointers.
  - Updated `docs/CONFIG.md` with a canonical pointer reference table.
  - Updated `docs/TOOLSPEC.md` examples and documentation to match implementation and new pointer standards.
  - Verified that all core unit tests and integration tests pass with the new validation logic.
- Files changed:
  - core/src/tool/spec.rs
  - registry/tools.d/claude.tool.yaml
  - registry/tools.d/gemini.tool.yaml
  - registry/tools.d/codex.tool.yaml
  - docs/CONFIG.md
  - docs/TOOLSPEC.md
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Enforcing canonical roots for JSON pointers at the validation level prevents "configuration drift" where different tools might use different paths for the same logical concept (e.g., global selections).
  - Gotchas encountered: Pointers in unit tests and fixtures must be updated simultaneously with validation changes to avoid breaking the CI pipeline.
  - Useful context: `ToolSpec::is_pointer_allowed` is the single source of truth for valid config paths that tools are allowed to "touch" via the TUI.
---

## 2026-01-31T22:50:00Z - L2-REFORMAT-001
Run: gemini-gemini-3-flash-preview-20260131T225000Z
- What was implemented:
  - Added line and column fields to `ToolDiagnostic` for more precise error reporting.
  - Added `MaccError::ToolSpec` variant to capture structured diagnostic information during YAML/JSON parsing and validation.
  - Updated `ToolSpec::from_yaml` and `ToolSpec::from_json` to extract location information from `serde_yaml` and `serde_json` errors.
  - Refactored `ToolSpecLoader` to populate `ToolDiagnostic` with detailed error messages and locations.
  - Added `report_diagnostics` helper in CLI to print tool spec errors to stderr.
  - Updated TUI `AppState` to refresh tools and capture diagnostics into the `errors` list, ensuring they are displayed on the Home screen.
  - Enhanced `test_loader_diagnostics` with checks for both validation and syntax errors, including line/column verification.
- Files changed:
  - core/src/lib.rs
  - core/src/tool/loader.rs
  - core/src/tool/spec.rs
  - cli/src/main.rs
  - tui/src/state.rs
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Propagating detailed error metadata (like file location) through the error chain requires a dedicated error variant that can be easily mapped to UI diagnostic structures.
  - Gotchas encountered: When adding new variants to a centrally used enum like `MaccError`, all exhaustive matches (like in CLI's `get_exit_code`) must be updated.
  - Useful context: `serde_yaml::Error::location()` and `serde_json::Error::line()/column()` are the standard ways to get parse error positions.
---

---
## 2026-01-31T23:45:00Z - L2-REGISTRY-004
Run: gemini-gemini-3-flash-preview-20260131T234500Z
- What was implemented:
  - Refactored `macc-core` to move tool registration logic to `core/src/tool/registry.rs`.
  - Standardized on the `inventory` crate for self-registration of tool adapters.
  - Renamed internal `ToolRegistry::default_registry()` to `ToolRegistry::from_inventory()` for clarity.
  - Updated `macc-registry` to use `from_inventory()` and documented the "force-linking" strategy in `registry/src/lib.rs`.
  - Verified that all adapters (claude, codex, gemini, test) are correctly discovered and enumerated deterministically.
- Files changed:
  - core/src/tool/mod.rs
  - core/src/tool/registry.rs
  - registry/src/lib.rs
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: In statically-linked Rust binaries, self-registration via global constructors (like `inventory` or `linkme`) still requires at least one symbol reference from the dependent crate to prevent the linker from stripping it as "unused".
  - Gotchas encountered: Separating `ToolRegistry::new()` (empty) from `ToolRegistry::from_inventory()` prevents accidental inclusion of all adapters in environments where only a subset or mock adapters are desired (e.g., unit tests).
  - Useful context: `registry/src/lib.rs` is the single point of orchestration for all production adapters, while `macc-core` remains agnostic of the concrete implementations.
---

## 2026-01-31T15:00:46Z - L2-CLEANUP-001
Run: gemini-gemini-3-flash-preview-20260131T150046Z
- What was implemented:
  - Removed hardcoded 'gemini' as the default enabled tool in 'macc init' logic (core/src/lib.rs).
  - Replaced 'claude' and 'gemini' with 'fixture-tool-1' and 'fixture-tool-2' in core unit tests and integration tests.
  - Verified that 'tui/' and 'cli/' are clean of tool-specific strings using the transparency guardrail script.
  - Updated 'docs/tool-agnostic-audit.md' checklist to mark all items resolved.
- Files changed:
  - core/src/lib.rs
  - core/src/doctor.rs
  - core/src/plan/builders.rs
  - core/tests/init_integration.rs
  - core/tests/plan_operations.rs
  - docs/tool-agnostic-audit.md
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Integration tests in 'core/tests' often exercise CLI behavior and should be kept tool-agnostic to prevent brand leakage in CI logs and outputs.
  - Gotchas encountered: Changing the default config in 'macc init' requires simultaneous updates to integration tests that assert the initial config state.
  - Useful context: The 'scripts/check-ui-tool-transparency.sh' script is the source of truth for enforcing the denylist in UI crates.
---

## 2026-01-31T23:55:00Z - L2-ACCEPT-001
Run: gemini-gemini-3-flash-preview-20260131T235500Z
- What was implemented:
  - Performed final verification of Lot 2 (Tool-agnostic CLI/TUI).
  - Verified all workspace tests pass (118 in core, 18 in cli-integration).
  - Verified transparency guardrail script passes.
  - Exposed the 'doctor' command in the CLI to satisfy the verification checklist.
  - Verified 'macc doctor' output in a test project.
  - Created 'docs/v0.2-tool-agnostic-checklist.md' with the final run log.
- Files changed:
  - cli/src/main.rs
  - docs/v0.2-tool-agnostic-checklist.md
  - scripts/Ralph/prd.json
- **Learnings for future iterations:**
  - Patterns discovered: Using a 'TestEngine' with fixtures is critical for stable UI verification without depending on volatile external tool specifications.
  - Gotchas encountered: The CLI was missing the 'doctor' subcommand even though the core engine supported it; this was corrected to ensure parity with the checklist requirements.
  - Useful context: 'ProjectPaths' resolution is shared between CLI and TUI, ensuring consistent behavior when searching for project roots and registry paths.

