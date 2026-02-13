# Compatibility Policy

This document defines runtime and contributor compatibility targets for MACC.

## Operating Systems

### Linux
- Status: fully supported.
- CI: required.

### macOS
- Status: supported.
- CI: required.

### Windows
- Status: supported for Rust crates (`macc` CLI/TUI/core).
- Notes:
  - automation scripts (`coordinator.sh`, `performer.sh`, runner scripts) are Bash-based.
  - recommended runtime for automation on Windows is WSL2 or a Unix-compatible shell environment.
- CI: build validation required.

## Rust Version

- Minimum Supported Rust Version (MSRV): **1.78.0**
- Target channel in CI: stable

Contributors should avoid introducing features that require a newer compiler without explicitly updating this policy and CI.

## Toolchain and Dependencies

- `cargo fmt` and `clippy` are mandatory in CI.
- Shell tooling used by automation/docs:
  - `bash`
  - `git`
  - `jq`
  - `curl`

## Backward Compatibility

- CLI flags and top-level commands should remain backward-compatible across minor releases.
- Breaking changes require:
  - major version bump,
  - changelog entry,
  - migration notes in `README.md` and/or `MACC.md`.
