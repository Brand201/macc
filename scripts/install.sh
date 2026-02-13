#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/install.sh [options]

Options:
  --prefix <dir>      Install directory for macc and macc-uninstall (default: ~/.local/bin)
  --no-path           Do not modify shell profile files
  --release           Build with --release
  --system            Install to /usr/local/bin (requires sudo)
  --repo <url>        Git repository URL (default: https://github.com/Brand201/macc.git)
  --ref <ref>         Git ref (branch/tag/commit) when source must be fetched (default: master)
  --keep-src          Keep temporary fetched source directory
  -h, --help          Show this help

Examples:
  ./scripts/install.sh --release
  ./scripts/install.sh --repo https://github.com/Brand201/macc.git --ref v0.1.0 --release
  curl -sSL https://raw.githubusercontent.com/Brand201/macc/master/scripts/install.sh | bash -s -- --release
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
  if grep -Fq "export PATH=\"$install_dir:\$PATH\"" "$profile"; then
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
    append_path_if_missing "${HOME}/.bashrc" "$install_dir"
    append_path_if_missing "${HOME}/.zshrc" "$install_dir"
    updated=1
  fi
  if [[ "$updated" -eq 1 ]]; then
    echo "Updated PATH in ~/.bashrc and ~/.zshrc"
    echo "Open a new shell or run: export PATH=\"$install_dir:\$PATH\""
  fi
}

clone_source() {
  local repo_url="$1"
  local ref="$2"
  local keep_src="$3"
  local tmpdir
  tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/macc-install.XXXXXX")"
  if ! git clone --quiet --depth 1 --branch "$ref" "$repo_url" "$tmpdir/repo" 2>/dev/null; then
    git clone --quiet "$repo_url" "$tmpdir/repo"
    (
      cd "$tmpdir/repo"
      git checkout --quiet "$ref"
    )
  fi
  if [[ "$keep_src" -eq 1 ]]; then
    echo "Using fetched source (kept): $tmpdir/repo"
  else
    # shellcheck disable=SC2064
    trap "rm -rf '$tmpdir'" EXIT
  fi
  echo "$tmpdir/repo"
}

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"
LOCAL_PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

INSTALL_DIR="${HOME}/.local/bin"
UPDATE_PATH=1
BUILD_PROFILE="debug"
USE_SYSTEM=0
REPO_URL="https://github.com/Brand201/macc.git"
REPO_REF="master"
KEEP_SRC=0

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
    --repo)
      [[ $# -ge 2 ]] || { echo "Error: --repo requires a value" >&2; exit 1; }
      REPO_URL="$2"
      shift 2
      ;;
    --ref)
      [[ $# -ge 2 ]] || { echo "Error: --ref requires a value" >&2; exit 1; }
      REPO_REF="$2"
      shift 2
      ;;
    --keep-src)
      KEEP_SRC=1
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
need_cmd git
need_cmd mktemp

PROJECT_ROOT=""
if [[ -f "$LOCAL_PROJECT_ROOT/Cargo.toml" && -d "$LOCAL_PROJECT_ROOT/.git" ]]; then
  PROJECT_ROOT="$LOCAL_PROJECT_ROOT"
  echo "Using local source: $PROJECT_ROOT"
else
  echo "Fetching source from $REPO_URL ($REPO_REF)..."
  PROJECT_ROOT="$(clone_source "$REPO_URL" "$REPO_REF" "$KEEP_SRC")"
fi

cd "$PROJECT_ROOT"

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

UNINSTALL_SRC="$PROJECT_ROOT/scripts/uninstall.sh"
if [[ ! -f "$UNINSTALL_SRC" ]]; then
  echo "Error: uninstall script not found at $UNINSTALL_SRC" >&2
  exit 1
fi

if [[ "$USE_SYSTEM" -eq 1 ]]; then
  need_cmd sudo
  echo "Installing to $INSTALL_DIR (sudo required)..."
  sudo install -d "$INSTALL_DIR"
  sudo install -m 0755 "$BIN_PATH" "$INSTALL_DIR/macc"
  sudo install -m 0755 "$UNINSTALL_SRC" "$INSTALL_DIR/macc-uninstall"
else
  install -d "$INSTALL_DIR"
  install -m 0755 "$BIN_PATH" "$INSTALL_DIR/macc"
  install -m 0755 "$UNINSTALL_SRC" "$INSTALL_DIR/macc-uninstall"
fi

if [[ "$UPDATE_PATH" -eq 1 && "$USE_SYSTEM" -eq 0 ]]; then
  update_shell_path "$INSTALL_DIR"
fi

echo "Installed:"
echo "  - $INSTALL_DIR/macc"
echo "  - $INSTALL_DIR/macc-uninstall"
echo "Verify with: macc --version"
echo "Uninstall with: macc-uninstall"
echo "Then in a new project: macc init && macc tui"
