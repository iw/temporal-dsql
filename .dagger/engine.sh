#!/usr/bin/env bash
# Provisions the stock Dagger engine and CLI that the ci module runs on. Adapted from
# tokeira/dagger-rust-sdk's .dagger/engine.sh.
#
#   eval "$(.dagger/engine.sh)"      start the engine and put the matching CLI on PATH
#   eval "$(.dagger/engine.sh cli)"  put the CLI on PATH without starting an engine
#   .dagger/engine.sh stop           stop the engine and remove its container, keeping its state
#
# Both are pinned to the Dagger release that the Rust SDK in dagger.toml targets, because the
# SDK talks only to an engine of exactly that release. The CLI archive is checked against its
# SHA-256 before it is unpacked, and the engine image is pulled by digest. Only the export
# lines go to standard output, so the result can be evaluated.
#
# The engine keeps its state, build cache included, in the `<container>-state` volume. A CI
# system that keeps a directory between jobs can set TEMPORAL_DSQL_ENGINE_STATE to that
# directory's absolute path, and the state is kept there instead.
set -euo pipefail

version=1.0.0-beta.21
image=registry.dagger.io/engine:v$version@sha256:3444c48e871bedaa95f5b1e57b8c41de929d9e061dd75e9ac6f6deb1f0a74bc3
# The CLI removes other releases' `dagger-engine-*` containers, so this name avoids the prefix.
container=temporal-dsql-engine-v$version
state=${TEMPORAL_DSQL_ENGINE_STATE:-$container-state}
# The engine sizes its cache from the disk it sees, which in a virtual machine or on a CI
# cache volume can be far larger than the space really free. It keeps 10 GB and collects
# above 30 GB: reserved, minimum free, and maximum space in MB.
gc=10000,0,30000
cache=${XDG_CACHE_HOME:-$HOME/.cache}/temporal-dsql/dagger/$version

case $state in
/* | "$container-state") ;;
*)
	echo "engine.sh: TEMPORAL_DSQL_ENGINE_STATE must be an absolute path" >&2
	exit 2
	;;
esac

case "$(uname -s)/$(uname -m)" in
Darwin/arm64) platform=darwin_arm64 sha256=781f4862c022a19b9a4c498b59e1d9cfda1ee3cdc8ce210e4545bd8842a7416d ;;
Linux/x86_64) platform=linux_amd64 sha256=c5ac777a5c99089869660943381136e22177b59a46412e310c2fef594760a041 ;;
Linux/aarch64) platform=linux_arm64 sha256=ba9d55638636918a3404313511bceba5cc0af22f487c1c1b6a5ba60ce70a61bb ;;
*)
	echo "engine.sh: no pinned Dagger CLI for $(uname -s)/$(uname -m)" >&2
	exit 1
	;;
esac

install_cli() {
	[ -x "$cache/dagger" ] && return
	local archive=$cache/dagger_v${version}_$platform.tar.gz
	mkdir -p "$cache"
	curl -fsSLo "$archive" "https://dl.dagger.io/dagger/releases/$version/dagger_v${version}_$platform.tar.gz"
	echo "$sha256  $archive" | shasum -a 256 -c - >&2
	tar -xzf "$archive" -C "$cache" dagger
}

start_engine() {
	# The label records how the container was started, so changing a setting replaces it.
	local settings="$image $state $gc"
	if [ "$(docker inspect -f '{{.State.Running}} {{index .Config.Labels "temporal-dsql.engine"}}' "$container" 2>/dev/null)" != "true $settings" ]; then
		stop_engine
		docker run -d --privileged --name "$container" --label "temporal-dsql.engine=$settings" \
			-v "$state:/var/lib/dagger" "$image" --oci-worker-gc-keepstorage "$gc" >/dev/null
	fi
}

stop_engine() {
	# Stopping gracefully lets the engine close its databases before its state is reused.
	docker stop -t 30 "$container" >/dev/null 2>&1 || true
	docker rm -f "$container" >/dev/null 2>&1 || true
}

case ${1:-start} in
start)
	install_cli
	start_engine
	# shellcheck disable=SC2016 # $PATH expands where the output is evaluated
	printf 'export PATH=%q:"$PATH"\n' "$cache"
	printf 'export DAGGER_ENGINE=%q DAGGER_NO_NAG=1\n' "container+docker://$container"
	;;
cli)
	install_cli
	# shellcheck disable=SC2016 # $PATH expands where the output is evaluated
	printf 'export PATH=%q:"$PATH"\n' "$cache"
	;;
stop)
	stop_engine
	;;
*)
	echo "usage: engine.sh [start|cli|stop]" >&2
	exit 2
	;;
esac
