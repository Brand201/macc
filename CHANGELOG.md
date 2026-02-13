# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Coordinator full-cycle command and stop flow improvements.
- Worktree/performer logging improvements and `macc logs tail`.
- Embedded automation/tool catalog defaults for clean-machine bootstrap.
- TUI improvements (status/footer, search filtering, undo/redo).
- GitHub `curl -sSL` install path via `scripts/install.sh`, including source fetch (`--repo`/`--ref`) when not running from a local clone.
- Installed `macc-uninstall` helper alongside `macc`.

### Changed
- Documentation rationalization (`docs/README.md` as docs index, historical docs marked).
- `scripts/uninstall.sh` now supports installed-helper usage and removes both `macc` and `macc-uninstall` by default.

### Fixed
- Preview/TUI display stability by silencing fetch logs in quiet mode and improving redraw behavior.

## [0.1.0] - 2026-02-13

### Added
- Initial public baseline of MACC:
  - canonical config + `plan`/`apply`,
  - tool registry/adapters,
  - TUI flows,
  - worktree and coordinator automation,
  - backup/restore/doctor.
