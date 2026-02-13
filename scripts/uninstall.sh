#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./uninstall.sh [options]

Options:
  --prefix <dir>    Remove binary from <dir> (default: ~/.local/bin)
  --system          Remove from /usr/local/bin (needs sudo)
  --clean-profile   Strip installer PATH entries from shell profiles
  -h, --help        Show this message
EOF
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Error: missing required command '$1'" >&2
    exit 1
  }
}

remove_path_entry() {
  local profile="$1"
  local pattern='^# Added by MACC installer$'
  [[ -f "$profile" ]] || return
  if grep -Fq "# Added by MACC installer" "$profile"; then
    sed -i.bak "/# Added by MACC installer/,+1d" "$profile"
    rm -f "${profile}.bak"
    echo "Cleaned PATH entry from $profile"
  fi
}

BIN_DIR="${HOME:-}/.local/bin"
REMOVE_PROFILE=0
SYSTEM=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix)
      [[ $# -ge 2 ]] || { echo "Error: --prefix needs a path" >&2; exit 1; }
      BIN_DIR="$2"
      shift 2
      ;;
    --system)
      SYSTEM=1
      BIN_DIR="/usr/local/bin"
      shift
      ;;
    --clean-profile)
      REMOVE_PROFILE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown option $1" >&2
      usage
      exit 1
      ;;
  esac
done

TARGET="$BIN_DIR/macc"

if [[ "$SYSTEM" -eq 1 ]]; then
  need_cmd sudo
  sudo rm -f "$TARGET"
else
  rm -f "$TARGET"
fi

echo "Removed binary $TARGET"

if [[ "$REMOVE_PROFILE" -eq 1 ]]; then
  remove_path_entry "${HOME}/.bashrc"
  remove_path_entry "${HOME}/.zshrc"
fi

echo "Uninstall complete."
