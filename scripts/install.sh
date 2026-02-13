#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./install.sh [options]

Options:
  --prefix <dir>     Install directory for the macc binary (default: ~/.local/bin)
  --no-path          Do not modify shell profile files
  --release          Build with --release and install target/release/macc
  --system           Install to /usr/local/bin (requires sudo)
  -h, --help         Show this help

Examples:
  ./install.sh
  ./install.sh --release
  ./install.sh --system --release
EOF
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Error: missing required command '$1'" >&2
    exit 1
  }
}

append_path_if_missing() {
  local profile="$1"
  local install_dir="$2"

  [[ -f "$profile" ]] || touch "$profile"
  if grep -Fq "$install_dir" "$profile"; then
    return 0
  fi

  {
    echo ""
    echo "# Added by MACC installer"
    echo "export PATH=\"$install_dir:\$PATH\""
  } >>"$profile"
}

update_shell_path() {
  local install_dir="$1"
  local updated=0

  if [[ ":$PATH:" != *":$install_dir:"* ]]; then
    if [[ -n "${HOME:-}" ]]; then
      append_path_if_missing "${HOME}/.bashrc" "$install_dir"
      append_path_if_missing "${HOME}/.zshrc" "$install_dir"
      updated=1
    fi
  fi

  if [[ "$updated" -eq 1 ]]; then
    echo "Updated PATH in ~/.bashrc and ~/.zshrc"
    echo "Open a new shell or run: export PATH=\"$install_dir:\$PATH\""
  fi
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

INSTALL_DIR="${HOME:-}/.local/bin"
UPDATE_PATH=1
BUILD_PROFILE="debug"
USE_SYSTEM=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix)
      [[ $# -ge 2 ]] || { echo "Error: --prefix requires a value" >&2; exit 1; }
      INSTALL_DIR="$2"
      shift 2
      ;;
    --no-path)
      UPDATE_PATH=0
      shift
      ;;
    --release)
      BUILD_PROFILE="release"
      shift
      ;;
    --system)
      USE_SYSTEM=1
      INSTALL_DIR="/usr/local/bin"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown argument '$1'" >&2
      usage
      exit 1
      ;;
  esac
done

need_cmd cargo

if [[ "$BUILD_PROFILE" == "release" ]]; then
  echo "Building macc (release)..."
  cargo build --release
  BIN_PATH="$PROJECT_ROOT/target/release/macc"
else
  echo "Building macc (debug)..."
  cargo build
  BIN_PATH="$PROJECT_ROOT/target/debug/macc"
fi

[[ -f "$BIN_PATH" ]] || {
  echo "Error: built binary not found at $BIN_PATH" >&2
  exit 1
}

if [[ "$USE_SYSTEM" -eq 1 ]]; then
  need_cmd sudo
  echo "Installing to $INSTALL_DIR/macc (sudo required)..."
  sudo install -m 0755 "$BIN_PATH" "$INSTALL_DIR/macc"
else
  mkdir -p "$INSTALL_DIR"
  install -m 0755 "$BIN_PATH" "$INSTALL_DIR/macc"
fi

if [[ "$UPDATE_PATH" -eq 1 && "$USE_SYSTEM" -eq 0 ]]; then
  update_shell_path "$INSTALL_DIR"
fi

echo "Installed: $INSTALL_DIR/macc"
echo "Verify with: macc --version"
echo "Then in a new project: macc init && macc tui"
