#!/usr/bin/env bash
# scripts/check-ui-tool-transparency.sh

set -euo pipefail

# Get the directory of the script to resolve paths relative to the repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DENYLIST_FILE="$REPO_ROOT/scripts/ui-denylist.txt"
TARGET_DIRS=("tui/src" "cli/src" "core/src")

if [ ! -f "$DENYLIST_FILE" ]; then
    echo "ERROR: Denylist file not found: $DENYLIST_FILE"
    exit 1
fi

mapfile -t TOKENS < <(grep -v '^#' "$DENYLIST_FILE" | grep -v '^$')
if [ "${#TOKENS[@]}" -eq 0 ]; then
    echo "Check skipped: Denylist is empty."
    exit 0
fi

escape_regex() {
    sed -E 's/[][(){}.^$+*?|\\-]/\\&/g'
}

PATTERN=""
for token in "${TOKENS[@]}"; do
    escaped="$(printf '%s' "$token" | escape_regex)"
    if [ -z "$PATTERN" ]; then
        PATTERN="$escaped"
    else
        PATTERN="$PATTERN|$escaped"
    fi
done

echo "Checking for forbidden tool strings in cli/tui/core sources..."

FAILED=0
cd "$REPO_ROOT"
for DIR in "${TARGET_DIRS[@]}"; do
    if [ -d "$DIR" ]; then
        MATCHES=$(
            rg -n -i --pcre2 "$PATTERN" "$DIR" \
                -g '*.rs' \
                -g '!**/target/**' \
                -g '!core/src/tool/loader.rs' \
                -g '!core/src/lib.rs' \
                || true
        )

        if [ -n "$MATCHES" ]; then
            echo "Forbidden strings found in $DIR/:
$MATCHES"
            FAILED=1
        fi
    fi
done

if [ $FAILED -eq 1 ]; then
    echo ""
    echo "ERROR: Tool-specific names found in source layers (core/cli/tui)."
    echo "Use generic IDs/capabilities and resolve concrete tools via ToolSpec + registry."
    exit 1
else
    echo "Check passed: source layers are tool-agnostic."
    exit 0
fi
