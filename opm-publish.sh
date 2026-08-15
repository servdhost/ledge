#!/usr/bin/env bash
#
# Builds and publishes this package to opm.openresty.org (the OpenResty
# package manager registry), using a disposable Docker container - no local
# opm/OpenResty install required.
#
# Once published, it can be installed in any OpenResty image with:
#   opm get <account>/ledge
#
# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------
# opm authenticates via a GitHub personal access token. You need:
#   OPM_GITHUB_ACCOUNT - your GitHub username or an org you belong to
#                         (e.g. servdhost - this becomes the opm package
#                         namespace, e.g. "servdhost/ledge")
#   OPM_GITHUB_TOKEN    - a token from https://github.com/settings/tokens
#                         with ONLY the `user:email` and `read:org` scopes
#
# Provide them either as environment variables, or once via a local
# .opm-credentials file (gitignored - never commit this) next to this
# script:
#   OPM_GITHUB_ACCOUNT=servdhost
#   OPM_GITHUB_TOKEN=ghp_xxxxxxxxxxxx
#
# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------
# opm reads the version to publish directly from lib/ledge.lua's _VERSION
# field, and will refuse to re-upload a version that's already published.
# Before running this script, bump _VERSION there - and, per this repo's
# own convention (checked by `make releng`), in every other lib/**/*.lua
# file too, so they stay consistent.
#
# Usage:
#   ./opm-publish.sh

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

CREDS_FILE="./.opm-credentials"
if [ -f "$CREDS_FILE" ]; then
    # shellcheck disable=SC1090
    source "$CREDS_FILE"
fi

: "${OPM_GITHUB_ACCOUNT:?Set OPM_GITHUB_ACCOUNT (see the comments at the top of this script)}"
: "${OPM_GITHUB_TOKEN:?Set OPM_GITHUB_TOKEN (see the comments at the top of this script)}"

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required to run this script" >&2
    exit 1
fi

version=$(grep -m1 '_VERSION' lib/ledge.lua | sed -E 's/.*"([^"]+)".*/\1/')
if [ -z "$version" ]; then
    echo "Could not detect _VERSION from lib/ledge.lua" >&2
    exit 1
fi

echo "Publishing ledge $version as ${OPM_GITHUB_ACCOUNT}/ledge ..."

mismatched=$(grep -rL "_VERSION = \"$version\"" lib --include="*.lua" || true)
if [ -n "$mismatched" ]; then
    echo "warning: these files don't have _VERSION = \"$version\" - did you mean to bump them too?" >&2
    echo "$mismatched" >&2
    echo >&2
fi

# Source is bind-mounted read-only and copied inside the container before
# building, so opm's build artifacts never touch the host checkout.
docker run --rm \
    -v "$(pwd)":/src:ro \
    -e OPM_GITHUB_ACCOUNT="$OPM_GITHUB_ACCOUNT" \
    -e OPM_GITHUB_TOKEN="$OPM_GITHUB_TOKEN" \
    openresty/openresty:alpine-fat sh -c '
        set -e

        cat > "$HOME/.opmrc" <<RC
github_account=$OPM_GITHUB_ACCOUNT
github_token=$OPM_GITHUB_TOKEN
upload_server=https://opm.openresty.org
download_server=https://opm.openresty.org
RC
        chmod 600 "$HOME/.opmrc"

        cp -r /src /tmp/build
        cd /tmp/build
        opm upload
    '

echo
echo "Published. Install it elsewhere with: opm get ${OPM_GITHUB_ACCOUNT}/ledge"
