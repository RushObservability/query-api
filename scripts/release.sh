#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

version="${VERSION:-}"
version="${version#v}"
dry_run="${DRY_RUN:-0}"

if [[ -z "$version" ]]; then
  echo "Usage: make release VERSION=0.1.26" >&2
  exit 2
fi

if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "Invalid version: $version" >&2
  echo "Use a semantic version such as 0.1.26 or 0.2.0-rc.1." >&2
  exit 2
fi

for required_command in git cargo gh awk mktemp; do
  if ! command -v "$required_command" >/dev/null 2>&1; then
    echo "Required command not found: $required_command" >&2
    exit 1
  fi
done

read_version() {
  local file="$1"
  local package="$2"
  awk -v package="$package" '
    $0 == package { in_package = 1; next }
    in_package && /^\[/ { exit }
    in_package && /^version = "/ {
      value = $0
      sub(/^version = "/, "", value)
      sub(/".*$/, "", value)
      print value
      exit
    }
  ' "$file"
}

package_version="$(read_version Cargo.toml '[package]')"
lock_version="$(read_version Cargo.lock 'name = "rush-api"')"
release_tag="v${version}"
release_branch="release/${release_tag}"

if [[ -z "$package_version" || -z "$lock_version" ]]; then
  echo "Could not read the rush-api version from Cargo.toml and Cargo.lock." >&2
  exit 1
fi

if [[ "$package_version" == "$version" && "$lock_version" == "$version" ]]; then
  echo "Cargo.toml and Cargo.lock already use $version. Choose a new version." >&2
  exit 1
fi

echo "Release plan"
echo "  Current version: Cargo.toml=$package_version Cargo.lock=$lock_version"
echo "  New version:     $version"
echo "  Branch:          $release_branch"
echo "  Tag after merge: $release_tag"

if [[ "$dry_run" == "1" ]]; then
  echo "Dry run complete. No files, branches, commits, or pull requests were changed."
  exit 0
fi

current_branch="$(git branch --show-current)"
if [[ "$current_branch" != "main" ]]; then
  echo "Run this target from main. Current branch: ${current_branch:-detached HEAD}" >&2
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "The working tree is not clean. Commit or stash your changes before creating a release PR." >&2
  exit 1
fi

git fetch origin main --tags

if [[ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]]; then
  echo "Local main does not match origin/main. Pull the latest main branch and try again." >&2
  exit 1
fi

if git rev-parse --verify --quiet "refs/tags/${release_tag}" >/dev/null; then
  echo "Tag $release_tag already exists. Choose a new version." >&2
  exit 1
fi

if git show-ref --verify --quiet "refs/heads/${release_branch}"; then
  echo "Local branch $release_branch already exists." >&2
  exit 1
fi

if git ls-remote --exit-code --heads origin "refs/heads/${release_branch}" >/dev/null 2>&1; then
  echo "Remote branch $release_branch already exists." >&2
  exit 1
fi

if ! gh auth status >/dev/null 2>&1; then
  echo "GitHub CLI is not authenticated. Run: gh auth login" >&2
  exit 1
fi

git switch -c "$release_branch"

toml_tmp="$(mktemp "${TMPDIR:-/tmp}/rush-api-Cargo.toml.XXXXXX")"
trap 'if [[ -f "$toml_tmp" ]]; then rm -f "$toml_tmp"; fi' EXIT
awk -v version="$version" '
  $0 == "[package]" { in_package = 1 }
  in_package && !updated && /^version = "/ {
    print "version = \"" version "\""
    updated = 1
    next
  }
  { print }
  END { if (!updated) exit 42 }
' Cargo.toml > "$toml_tmp"
mv "$toml_tmp" Cargo.toml
trap - EXIT

cargo update -p rush-api --precise "$version"

updated_lock_version="$(read_version Cargo.lock 'name = "rush-api"')"
if [[ "$updated_lock_version" != "$version" ]]; then
  echo "Cargo.lock version $updated_lock_version does not match Cargo.toml version $version." >&2
  exit 1
fi

cargo fmt --check
cargo clippy --all-targets --all-features -- -D clippy::correctness -D clippy::suspicious
cargo test --all-targets --all-features

git add Cargo.toml Cargo.lock
if git diff --cached --quiet; then
  echo "The version command did not change Cargo.toml or Cargo.lock." >&2
  exit 1
fi

git commit -m "Release $release_tag"
git push --set-upstream origin "$release_branch"

pr_url="$(
  gh pr create \
    --base main \
    --head "$release_branch" \
    --title "Release $release_tag" \
    --body "Updates the Query API Cargo package version to $version. After this PR merges, the release workflow will run the security gate, publish the image, create $release_tag, and generate release notes from merged pull requests."
)"

echo "Release PR created: $pr_url"
