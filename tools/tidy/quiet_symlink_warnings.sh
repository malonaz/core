#!/usr/bin/env bash
# Runs a command with cmd/go's "warning: ignoring symlink" lines filtered out of
# stderr. Those come from the node_modules symlinks in app/, which are not Go
# packages, so the walker is doing the right thing and the warning is noise.
# Everything else on stderr is passed through live and the exit status is kept.
set -uo pipefail

{ "$@" 2>&1 1>&3 3>&- | grep -v '^warning: ignoring symlink ' >&2; } 3>&1
exit "${PIPESTATUS[0]}"
