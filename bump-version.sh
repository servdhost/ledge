#!/usr/bin/env bash
#
# Bumps _VERSION across every lib/**/*.lua file - the convention this
# repo's own `make releng` check enforces, and what opm-publish.sh reads
# to know what version to publish.
#
# Usage:
#   ./bump-version.sh patch|minor|major
#   ./bump-version.sh              # prompts interactively instead

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

current=$(grep -m1 -oE '_VERSION = "[0-9]+\.[0-9]+\.[0-9]+"' lib/ledge.lua \
    | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
if [ -z "$current" ]; then
    echo "Could not find _VERSION in lib/ledge.lua" >&2
    exit 1
fi

IFS='.' read -r major minor patch <<< "$current"

bump_type="${1:-}"
if [ -z "$bump_type" ]; then
    echo "Current version: $current"
    echo "Bump which part?"
    select choice in "patch" "minor" "major"; do
        case "$choice" in
            patch|minor|major) bump_type="$choice"; break ;;
            *) echo "Please choose 1, 2, or 3." ;;
        esac
    done
fi

case "$bump_type" in
    patch) patch=$((patch + 1)) ;;
    minor) minor=$((minor + 1)); patch=0 ;;
    major) major=$((major + 1)); minor=0; patch=0 ;;
    *)
        echo "Usage: $0 [patch|minor|major]" >&2
        exit 1
        ;;
esac

new_version="$major.$minor.$patch"

files=$(grep -rl "_VERSION = \"$current\"" lib --include="*.lua" || true)
if [ -z "$files" ]; then
    echo "No lib/**/*.lua files found with _VERSION = \"$current\"" >&2
    exit 1
fi

echo "Bumping version ($bump_type): $current -> $new_version"

while IFS= read -r f; do
    sed -i.bak "s/_VERSION = \"$current\"/_VERSION = \"$new_version\"/" "$f"
    rm -f "$f.bak"
done <<< "$files"

# Sanity check: every file we touched should now show the new version, and
# none should still show the old one.
remaining=$(grep -rl "_VERSION = \"$current\"" lib --include="*.lua" || true)
if [ -n "$remaining" ]; then
    echo "warning: these files still have the old version - update manually:" >&2
    echo "$remaining" >&2
fi

count=$(echo "$files" | wc -l | tr -d ' ')
echo "Updated $count file(s):"
echo "$files"
echo
echo "Review with: git diff"
