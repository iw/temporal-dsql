#!/usr/bin/env bash
#
# Regenerates modules/ci/src/dagger_generated after an edit to the module's
# Rust source or Cargo.toml. With the rust.4 SDK, `dagger generate` doesn't
# reach the Rust SDK's generator, so this script runs the generator that
# `dagger module init` uses. It skips loading the module, which works even
# while the module is stale.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
eval "$(.dagger/engine.sh)"

# A global git config that rewrites https://github.com/ to SSH breaks the
# engine's dependency fetches.
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1

dagger api query --no-load-module <<EOF
{ currentWorkspace { __sdkGenerators(sdk: "rust") { run { changes { export(path: "$root") } } } } }
EOF

# The engine exports files world-writable.
chmod -R go-w .dagger/modules/ci
