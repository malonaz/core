#!/bin/sh
set -eu
ROOT="$(git rev-parse --show-toplevel)"
KUBECTL="${KUBECTL:-kubectl}"

for SRC in $SRCS; do
    # kubectl annoyingly returns nonzero if the filter excludes everything :(
    if [ -n "${KUBECTL_FILTER:-}" ]; then
        "$KUBECTL" delete --ignore-not-found -f "$ROOT/plz-out/gen/$SRC" "$@" "$KUBECTL_FILTER" || true
    else
        "$KUBECTL" delete --ignore-not-found -f "$ROOT/plz-out/gen/$SRC" "$@"
    fi
done
