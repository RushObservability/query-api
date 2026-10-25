#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

version="${VERSION:-}"
dry_run="${DRY_RUN:-0}"
release_candidate="${RC:-0}"
rc_image="ghcr.io/rushobservability/query-api"
rc_platforms="${PLATFORMS:-linux/amd64,linux/arm64}"

for required_command in git cargo gh awk mktemp; do
  if ! command -v "$required_command" >/dev/null 2>&1; then
    echo "Required command not found: $required_command" >&2
    exit 1
  fi
done

# Release candidates are never a bump base: after v0.1.9-rc.1 the next final
# release is still 0.1.9.
latest_stable_tag() {
  local tag
  while IFS= read -r tag; do
    if [[ "${tag#v}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
      printf '%s\n' "$tag"
      return 0
    fi
  done < <(git tag --list --sort=-version:refname)
  return 1
}

if [[ -z "$version" || "$release_candidate" == "1" ]]; then
  git fetch origin --tags
fi

if [[ -z "$version" ]]; then
  if ! latest_tag="$(latest_stable_tag)"; then
    echo "No semantic-version tags were found. Set VERSION explicitly for the first release." >&2
    exit 2
  fi
  [[ "${latest_tag#v}" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]
  version="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.$((10#${BASH_REMATCH[3]} + 1))"
  echo "No VERSION supplied; bumping $latest_tag to v$version."
fi
version="${version#v}"

if [[ "$release_candidate" == "1" ]]; then
  if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "RC=1 needs a base version such as 0.2.0, not $version." >&2
    exit 2
  fi
  if git rev-parse --verify --quiet "refs/tags/v${version}" >/dev/null; then
    echo "v$version is already released; a release candidate must precede it." >&2
    exit 1
  fi
  last_rc="$(
    git tag --list "v${version}-rc.*" |
      sed -n "s/^v${version//./\\.}-rc\.\([0-9][0-9]*\)$/\1/p" | sort -n | tail -n 1
  )"
  version="${version}-rc.$((10#${last_rc:-0} + 1))"
  echo "Release candidate: v$version."
fi

if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "Invalid version: $version" >&2
  echo "Use a semantic version such as 0.1.26 or 0.2.0-rc.1." >&2
  exit 2
fi

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

# Replace the first version line after `package` (same lookup as read_version).
write_version() {
  local file="$1"
  local package="$2"
  local updated
  updated="$(awk -v package="$package" -v version="$version" '
    $0 == package { in_package = 1; print; next }
    in_package && /^\[/ { in_package = 0 }
    in_package && !done && /^version = "/ { print "version = \"" version "\""; done = 1; next }
    { print }
    END { if (!done) exit 42 }
  ' "$file")" || {
    echo "Could not set the rush-api version in $file." >&2
    exit 1
  }
  printf '%s\n' "$updated" > "$file"
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
build_context="."
if [[ "$release_candidate" == "1" ]]; then
  echo "  Image:           ${rc_image}:${version} (${rc_platforms})"
  echo "  Tag:             $release_tag at $(git rev-parse --short HEAD) ($(git branch --show-current))"
  if [[ "$dry_run" != "1" && -n "$(git status --porcelain)" ]]; then
    echo "The working tree is not clean. Commit or stash your changes; the candidate is built from HEAD." >&2
    exit 1
  fi
  # Build committed HEAD with the candidate version, without touching the checkout.
  build_context="$(mktemp -d "${TMPDIR:-/tmp}/rush-api-rc.XXXXXX")"
  trap 'rm -rf "$build_context"' EXIT
  git archive HEAD | tar -x -C "$build_context"
  write_version "$build_context/Cargo.toml" '[package]'
  write_version "$build_context/Cargo.lock" 'name = "rush-api"'
else
  echo "  Branch:          $release_branch"
  echo "  Tag after merge: $release_tag"
fi

# Security checks run only here, never in CI: whoever releases decides. They run
# before any branch exists, so a finding stops the release instead of surfacing
# after the release PR merges. fmt, Clippy, and tests run below, after the bump.
for gate_tool in cargo-audit cargo-deny python3 rg docker trivy; do
  if ! command -v "$gate_tool" >/dev/null 2>&1; then
    echo "Required for the release security gate: $gate_tool. Install with: cargo install --locked cargo-audit cargo-deny; brew install ripgrep trivy; and Docker" >&2
    exit 1
  fi
done
echo "Release security gate: make security"
make security

# shortcut: scans only this machine's architecture; amd64 on Apple Silicon would
# compile Rust under emulation. Both images share the base images and Cargo.lock.
scan_image="rush-api-release-scan:${version}"
echo "Release security gate: image scan ($(uname -m))"
docker build --tag "$scan_image" "$build_context"
trivy image --quiet --scanners vuln --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 "$scan_image"
docker image rm "$scan_image" >/dev/null

if [[ "$dry_run" == "1" ]]; then
  echo "Dry run complete. No files, branches, commits, images, or pull requests were changed."
  exit 0
fi

if [[ "$release_candidate" == "1" ]]; then
  docker buildx build \
    --platform "$rc_platforms" \
    --tag "${rc_image}:${version}" \
    --label "org.opencontainers.image.version=${version}" \
    --label "org.opencontainers.image.revision=$(git rev-parse HEAD)" \
    --label "org.opencontainers.image.source=https://github.com/RushObservability/query-api" \
    --push \
    "$build_context"
  # Tag only after the push, so a failed build never consumes an rc number.
  git tag "$release_tag" HEAD
  git push origin "refs/tags/${release_tag}"
  echo "Published ${rc_image}:${version} and tag ${release_tag}."
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
