#!/usr/bin/env bash
#
# Starts the Dagger engine that the module in this directory is pinned to, and
# prints the environment that points the matching CLI at it:
#
#   eval "$(.dagger/engine.sh)"        start or reuse the engine
#   .dagger/engine.sh stop             remove the container, keep its cache volume
#
# The module uses the Rust SDK from the iw/dagger fork. The SDK only talks to
# an engine of exactly the same release, and that engine is not published to
# registry.dagger.io, so the CLI cannot provision it. Every download is checked
# against the release's SHA256SUMS, whose own digest is pinned below, and the
# loaded engine image must match its pinned digest. On Linux the CLI is copied
# out of the verified engine image, because the release ships only a
# darwin/arm64 CLI.
#
# Supported hosts: macOS arm64 (Docker Desktop or OrbStack) and Linux
# amd64/arm64 with a local Docker daemon. As a Buildkite repository hook, see
# ../.buildkite/hooks/pre-command.
#
# Environment overrides:
#   DAGGER_ENGINE_CACHE_DIR  downloads and CLI (default ~/.cache/temporal-dsql/dagger/<version>)
#   DAGGER_ENGINE_CONTAINER  engine container name (default temporal-dsql-dagger-<version>)

set -euo pipefail

# The release, CLI, engine and SDK move together: the SDK rev in Cargo.toml
# must be this commit.
readonly VERSION="v1.0.0-beta.11.rust.5"
readonly COMMIT="2b461c1d3e2daa5681b8ad216eccb00912719fd5"
readonly RELEASE_URL="https://github.com/iw/dagger/releases/download/sdk/rust/${VERSION}"
readonly SUMS_SHA256="4b16cae4c68c35422394731cd0b283396ea78f0b2dccdaeb51ba9dec9f702307"


readonly CACHE_DIR="${DAGGER_ENGINE_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/temporal-dsql/dagger/${VERSION}}"
# Stock Dagger CLIs remove every other container named dagger-engine-* when
# they connect, so the default name avoids that prefix.
readonly CONTAINER="${DAGGER_ENGINE_CONTAINER:-temporal-dsql-dagger-${VERSION}}"

log() { printf 'dagger-engine: %s\n' "$*" >&2; }
die() {
	log "error: $*"
	exit 1
}

sha256() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | cut -d' ' -f1
	else
		shasum -a 256 "$1" | cut -d' ' -f1
	fi
}

# Normalizes uname/Docker architecture names to Go's.
goarch() {
	case "$1" in
	x86_64 | amd64) echo amd64 ;;
	aarch64 | arm64) echo arm64 ;;
	*) die "unsupported architecture: $1" ;;
	esac
}

download() {
	local name="$1" dest="$2"
	curl --fail --silent --show-error --location --retry 3 --output "${dest}.partial" "${RELEASE_URL}/${name}"
	mv "${dest}.partial" "$dest"
}

# Downloads SHA256SUMS once and refuses it unless its digest is the pinned one.
fetch_sums() {
	local sums="${CACHE_DIR}/SHA256SUMS"
	if [[ ! -f "$sums" || "$(sha256 "$sums")" != "$SUMS_SHA256" ]]; then
		log "downloading SHA256SUMS for ${VERSION}"
		download SHA256SUMS "$sums"
	fi
	[[ "$(sha256 "$sums")" == "$SUMS_SHA256" ]] || die "SHA256SUMS does not match the pinned digest"
}

# Downloads a release asset unless a verified copy is cached, and prints its path.
fetch() {
	local name="$1" path="${CACHE_DIR}/$1" want
	want="$(awk -v n="$name" '$2 == n { print $1 }' "${CACHE_DIR}/SHA256SUMS")"
	[[ -n "$want" ]] || die "${name} is not listed in SHA256SUMS"
	if [[ -f "$path" && "$(sha256 "$path")" == "$want" ]]; then
		echo "$path"
		return
	fi
	log "downloading ${name}"
	download "$name" "$path"
	[[ "$(sha256 "$path")" == "$want" ]] || {
		rm -f "$path"
		die "${name} does not match SHA256SUMS"
	}
	echo "$path"
}

# Prints the engine image's config and manifest digests for an architecture.
# Docker's classic image store reports the config digest as the image ID; the
# containerd image store reports the manifest digest.
pinned_digests() {
	case "$1" in
	amd64) echo "sha256:1760b5fa19b521833ea18946119383b3dd308bff75f9b0353ee008a88d783e1a sha256:5596a090d169a9e4a20d9278495df068733a0541e42fa26caed2fda393449aae" ;;
	arm64) echo "sha256:4c8a8836eff5cad573058505ad43aaead8fc80f821719eb660825c8a9b537530 sha256:0b6fd3a71614542cb7dc52680dbe22fd59351d2e8a93fb772dd299debe402155" ;;
	esac
}

image_id() { docker image inspect --format '{{.Id}}' "$1" 2>/dev/null || true; }

# Loads and tags the engine image unless the tag already resolves to a pinned digest.
ensure_image() {
	local arch="$1" ref="$2" config manifest id loaded tar
	read -r config manifest <<<"$(pinned_digests "$arch")"

	id="$(image_id "$ref")"
	if [[ "$id" != "$config" && "$id" != "$manifest" ]]; then
		tar="$(fetch "dagger-engine-${VERSION}-linux-${arch}.oci.tar")"
		log "loading the engine image"
		loaded="$(docker load --quiet --input "$tar" | sed -n -e 's/^Loaded image ID: //p' -e 's/^Loaded image: //p' | tail -n 1)"
		[[ -n "$loaded" ]] || die "docker load reported no image ID"
		docker tag "$loaded" "$ref"
		id="$(image_id "$ref")"
	fi
	[[ "$id" == "$config" || "$id" == "$manifest" ]] || die "engine image ${id} does not match the pinned digest"
	echo "$id"
}

# Installs the CLI that matches the engine and prints its directory.
ensure_cli() {
	local os="$1" image="$2" bin="${CACHE_DIR}/bin" cli="${CACHE_DIR}/bin/dagger" tarball container
	mkdir -p "$bin"
	if [[ ! -x "$cli" ]] || ! cli_matches "$cli"; then
		rm -f "$cli"
		case "$os" in
		Darwin)
			tarball="$(fetch "dagger_${VERSION}_darwin_arm64.tar.gz")"
			tar -xzf "$tarball" -C "$bin" dagger
			;;
		Linux)
			container="$(docker create "$image")"
			docker cp "${container}:/usr/local/bin/dagger" "$cli" >/dev/null
			docker rm "$container" >/dev/null
			;;
		esac
		cli_matches "$cli" || die "the installed CLI does not report ${VERSION} at ${COMMIT:0:8}"
	fi
	echo "$bin"
}

cli_matches() {
	local out
	out="$("$1" version 2>/dev/null)" || return 1
	grep -q "^version: *${VERSION}$" <<<"$out" && grep -q "^commit: *${COMMIT:0:8}" <<<"$out"
}

ensure_container() {
	local image="$1" state
	state="$(docker container inspect --format '{{.Image}} {{.State.Running}}' "$CONTAINER" 2>/dev/null || true)"
	case "$state" in
	"$image true") log "reusing running container ${CONTAINER}" ;;
	"$image false")
		log "starting container ${CONTAINER}"
		docker start "$CONTAINER" >/dev/null
		;;
	*)
		if [[ -n "$state" ]]; then
			log "replacing container ${CONTAINER}, which runs a different image"
			docker rm --force "$CONTAINER" >/dev/null
		fi
		log "running container ${CONTAINER}"
		# Without a volume at /var/lib/dagger, BuildKit cannot use overlayfs inside
		# the container's own overlay root, and its fallback snapshotter copies
		# layers until the disk fills.
		docker run --detach --privileged --restart unless-stopped \
			--name "$CONTAINER" \
			--volume "${CONTAINER}-state:/var/lib/dagger" \
			"$image" >/dev/null
		;;
	esac
}

# Blocks until the engine answers a query, so the first real call does not race startup.
wait_ready() {
	local cli="$1/dagger" deadline=$((SECONDS + 120))
	until _EXPERIMENTAL_DAGGER_RUNNER_HOST="docker-container://${CONTAINER}" DAGGER_NO_NAG=1 \
		"$cli" --silent api query --no-load-module <<<'{ version }' >/dev/null 2>&1; do
		((SECONDS < deadline)) || die "the engine in ${CONTAINER} did not become ready; see: docker logs ${CONTAINER}"
		sleep 2
	done
}

start() {
	local os arch host_arch ref image bin
	command -v docker >/dev/null 2>&1 || die "docker is not on PATH"
	command -v curl >/dev/null 2>&1 || die "curl is not on PATH"

	os="$(uname -s)"
	arch="$(goarch "$(docker version --format '{{.Server.Arch}}')")"
	host_arch="$(goarch "$(uname -m)")"
	case "$os/$host_arch/$arch" in
	Darwin/arm64/arm64 | Linux/amd64/amd64 | Linux/arm64/arm64) ;;
	*) die "unsupported host ${os}/${host_arch} with a linux/${arch} Docker daemon" ;;
	esac

	mkdir -p "$CACHE_DIR"
	fetch_sums
	ref="temporal-dsql/dagger-engine:${VERSION}-linux-${arch}"
	image="$(ensure_image "$arch" "$ref")"
	bin="$(ensure_cli "$os" "$image")"
	ensure_container "$image"
	wait_ready "$bin"
	log "engine ${VERSION} (linux/${arch}) is ready in ${CONTAINER}"

	printf 'export _EXPERIMENTAL_DAGGER_RUNNER_HOST=%q\n' "docker-container://${CONTAINER}"
	# The caller's own PATH must expand when the output is evaluated.
	# shellcheck disable=SC2016
	printf 'export PATH=%q:"$PATH"\n' "$bin"
}

stop() {
	docker rm --force "$CONTAINER" >/dev/null 2>&1 || true
	log "removed ${CONTAINER}; its cache stays in the ${CONTAINER}-state volume"
}

case "${1:-start}" in
start) start ;;
stop) stop ;;
*) die "usage: $0 [start|stop]" ;;
esac
