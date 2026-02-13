# MACC Adapters Workspace

This folder is a standalone Cargo workspace for tool adapters.

- Add a new tool adapter by creating a new crate under `adapters/`.
- The workspace uses a glob (`adapter-*`) so new adapter crates can be added without editing `Cargo.toml`.
- Shared utilities live in `shared/`.
