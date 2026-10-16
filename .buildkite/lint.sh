#!/usr/bin/env bash
#
# Lints the lines this build changes, as upstream CI does with golangci-lint
# --new-from-rev. A pull request is compared with its merge base and any other
# build with its parent commit. The diff is computed here because the module
# receives the source without .git.

set -euo pipefail

patch="$(mktemp)"
trap 'rm -f "$patch"' EXIT

if [[ "${BUILDKITE_PULL_REQUEST:-false}" != "false" ]]; then
	git fetch --quiet --no-tags origin "${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-main}"
	git diff "$(git merge-base FETCH_HEAD HEAD)" HEAD >"$patch"
else
	git diff HEAD~1 HEAD >"$patch"
fi

dagger --progress=plain call ci lint --patch "$patch"
