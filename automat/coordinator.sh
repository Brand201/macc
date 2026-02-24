#!/usr/bin/env bash
set -euo pipefail

# Thin compatibility wrapper: coordinator logic runs through Rust CLI.

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
