#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/uninstall.sh [options]
  macc-uninstall [options]

Options:
  --prefix <dir>      Remove binaries from <dir> (default: script directory if installed, else ~/.local/bin)
  --system            Remove from /usr/local/bin (needs sudo)
  --clean-profile     Strip installer PATH entries from shell profiles
  --keep-helper       Keep macc-uninstall binary/script
  -h, --help          Show this message
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
  [[ -f "$profile" ]] || return
  if grep -Fq "# Added by MACC installer" "$profile"; then
    sed -i.bak "/# Added by MACC installer/,+1d" "$profile"
    rm -f "${profile}.bak"
    echo "Cleaned PATH entry from $profile"
  fi
}

SELF_PATH="${BASH_SOURCE[0]:-$0}"
SELF_BASENAME="$(basename "$SELF_PATH")"
if [[ "$SELF_BASENAME" == "macc-uninstall" ]]; then
  BIN_DIR="$(cd "$(dirname "$SELF_PATH")" && pwd)"
else
  BIN_DIR="${HOME:-}/.local/bin"
fi
REMOVE_PROFILE=0
SYSTEM=0
KEEP_HELPER=0

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
    --keep-helper)
      KEEP_HELPER=1
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
HELPER="$BIN_DIR/macc-uninstall"

if [[ "$SYSTEM" -eq 1 ]]; then
  need_cmd sudo
  sudo rm -f "$TARGET"
  if [[ "$KEEP_HELPER" -eq 0 ]]; then
    sudo rm -f "$HELPER"
  fi
else
  rm -f "$TARGET"
  if [[ "$KEEP_HELPER" -eq 0 ]]; then
    rm -f "$HELPER"
  fi
fi

echo "Removed binary $TARGET"
if [[ "$KEEP_HELPER" -eq 0 ]]; then
  echo "Removed helper $HELPER"
fi

if [[ "$REMOVE_PROFILE" -eq 1 ]]; then
  remove_path_entry "${HOME}/.bashrc"
  remove_path_entry "${HOME}/.zshrc"
fi

echo "Uninstall complete."
