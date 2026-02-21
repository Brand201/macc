#!/usr/bin/env bash
set -euo pipefail

# Thin compatibility wrapper (PR7): coordinator logic runs through Rust CLI.
# Legacy shell coordinator remains available via .macc/automation/coordinator_legacy.sh.

REPO_DIR="${REPO_DIR:-.}"
if [[ "${REPO_DIR}" != /* ]]; then
  REPO_DIR="$(cd "${REPO_DIR}" && pwd -P)"
fi

action="${1:-run}"
extra=()
if [[ "$action" == "run" ]]; then
  extra+=("--no-tui")
fi

exec macc --cwd "$REPO_DIR" coordinator "$@" "${extra[@]}"
