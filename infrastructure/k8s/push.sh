#!/bin/sh
set -eu
ROOT="$(git rev-parse --show-toplevel)"
KUBECTL="${KUBECTL:-kubectl}"
K8S_REPO="${K8S_REPO:-}"

for SRC in $SRCS; do

    # shellcheck disable=SC2124
    KUBECTL_COMMAND_START="$KUBECTL $@ "
    KUBECTL_COMMAND_END=" apply -f - "

    if [ -n "${KUBECTL_FILTER:-}" ]; then
        # "$KUBECTL" apply -f "$ROOT/plz-out/gen/$SRC" "$@" "$KUBECTL_FILTER" || true
        KUBECTL_COMMAND_START="$KUBECTL_COMMAND_START $KUBECTL_FILTER"
    fi

    KUBECTL_COMMAND="$KUBECTL_COMMAND_START $KUBECTL_COMMAND_END "
    if [ -n "${K8S_REPO}" ]; then
        echo "Patching in ${K8S_REPO} into $SRC"
        # kubectl annoyingly returns nonzero if the filter excludes everything :(
        # sed is a noop if there is no repo.
        # shellcheck disable=SC2002
        cat "$ROOT/plz-out/gen/$SRC" | sed -E "s/image\: \"?/&$K8S_REPO\//" | sed -E "s/Image\: \"?/&$K8S_REPO\//" |$KUBECTL_COMMAND || true
    else
        # shellcheck disable=SC2002
        cat "$ROOT/plz-out/gen/$SRC" | $KUBECTL_COMMAND || true
    fi


done
