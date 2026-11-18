#!/bin/bash
# Updates a multi-repository runner install (this fork) on macOS to the
# latest published release. Registrations (.runners/, .runner, .credentials),
# .env, .path and _work/ are left untouched.
#
# Usage: ./update-runner-macos.sh [runner-dir]   (default: ~/actions-runner)
#
# Stop the runner (./run.sh or your supervisor) before running this.

set -euo pipefail

REPO="apfritts/github-runner"
RUNNER_DIR="${1:-$HOME/actions-runner}"

fail() { echo "error: $*" >&2; exit 1; }

[ "$(uname -s)" = "Darwin" ] || fail "this script is for macOS"
[ "$(uname -m)" = "arm64" ] || fail "only osx-arm64 packages are published; build from source with src/dev.sh on Intel Macs"
[ -d "$RUNNER_DIR/bin" ] || fail "$RUNNER_DIR does not look like a runner install (no bin/)"
cd "$RUNNER_DIR"

if pgrep -f "$RUNNER_DIR/bin/Runner.Listener" > /dev/null || pgrep -f "$RUNNER_DIR/bin/Runner.Worker" > /dev/null; then
    fail "the runner is still running from $RUNNER_DIR; stop it (and let any job finish) first"
fi

echo "Looking up the latest release of $REPO ..."
RELEASE_JSON=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest") || fail "could not query the latest release"
TAG=$(echo "$RELEASE_JSON" | grep -m1 '"tag_name"' | cut -d'"' -f4 || true)
[ -n "$TAG" ] || fail "could not determine the latest release tag"
VERSION=${TAG#v}
VERSION=${VERSION%%-multi*}
PKG="actions-runner-osx-arm64-$VERSION.tar.gz"
BASE_URL="https://github.com/$REPO/releases/download/$TAG"

CURRENT=$(./bin/Runner.Listener --version 2>/dev/null || echo "unknown")
echo "Installed: $CURRENT   Latest: $VERSION ($TAG)"
if [ "$CURRENT" = "$VERSION" ] && [ "${FORCE:-}" != "1" ]; then
    echo "Already up to date. Set FORCE=1 to reinstall anyway."
    exit 0
fi

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

echo "Downloading $PKG ..."
curl -fL --progress-bar -o "$TMP_DIR/$PKG" "$BASE_URL/$PKG"
curl -fsSL -o "$TMP_DIR/$PKG.sha256" "$BASE_URL/$PKG.sha256"

echo "Verifying checksum ..."
(cd "$TMP_DIR" && shasum -a 256 -c "$PKG.sha256") || fail "checksum mismatch"

echo "Installing ..."
rm -rf bin.old externals.old
mv bin bin.old
[ -d externals ] && mv externals externals.old

rollback() {
    echo "Install failed; restoring previous version." >&2
    rm -rf bin externals
    mv bin.old bin
    [ -d externals.old ] && mv externals.old externals
    exit 1
}

tar xzf "$TMP_DIR/$PKG" || rollback
xattr -dr com.apple.quarantine . 2>/dev/null || true

NEW=$(./bin/Runner.Listener --version 2>/dev/null) || rollback
echo "Updated runner: $CURRENT -> $NEW"
echo
echo "Start it again with: cd \"$RUNNER_DIR\" && ./run.sh"
echo "Once a job runs fine, remove the backup: rm -rf \"$RUNNER_DIR/bin.old\" \"$RUNNER_DIR/externals.old\""
echo "To roll back: rm -rf bin externals && mv bin.old bin && mv externals.old externals"
