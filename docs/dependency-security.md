# Dependency security policy

Query API dependency changes are gated by RustSec vulnerability scanning,
license/source/yank policy, formatting, Clippy, tests, and a release build.
The pull-request workflow runs when Rust, Cargo, workflow, Docker, or dependency
policy files change. A scheduled workflow refreshes the advisory database each
week, and the tag workflow repeats the security gates before publishing an
image.

Clippy's correctness and suspicious groups are release-blocking. Existing
style and complexity findings remain visible as warnings and can be reduced
incrementally without weakening the correctness gate.

## Local checks

Install the same tool versions used by CI:

```sh
cargo install --locked cargo-audit --version 0.22.2
cargo install --locked cargo-deny --version 0.20.2
make security
```

`cargo audit` fails on every RustSec vulnerability except the explicitly
reviewed item below. `cargo deny` additionally rejects new direct unmaintained
dependencies, unsound advisories, yanked crates, wildcard versions, unknown
registries, unknown Git sources, and licenses outside `deny.toml`.

## Temporary advisory exceptions

| Advisory | Dependency paths | Reachability decision | Owner | Expires |
|---|---|---|---|---|
| `RUSTSEC-2023-0071` | `jsonwebtoken -> rsa`; `opensaml -> bergshamra -> rsa` | Query API verifies OIDC JWT and SAML XML signatures with public keys. It does not perform RSA private-key decryption or signing, so the timing oracle needed for private-key recovery is not reachable. Remove the exception when the RustCrypto RSA fix is released and adopted. | Query API security maintainers | 2026-11-01 |

The exception must not be renewed without new reachability evidence, an owner,
and a new time-bounded expiry. CI prints ignored advisories so exceptions remain
visible.

## Tracked transitive maintenance warnings

These warnings do not represent known exploitable vulnerabilities, but they are
tracked for removal during dependency upgrades:

| Crate | Dependency path | Disposition |
|---|---|---|
| `backoff 0.4` / `instant 0.1` | `kube-runtime 0.98` | Remove during the planned kube major-version upgrade. Runtime retry behavior is covered by existing Kubernetes integration tests. |
| `rustls-pemfile 2.2` | `kube-client 0.98` | Remove with the same kube upgrade; query-api does not call it directly. |
| `bincode 1.3` | build-time parser generator under `promql-parser` | The direct application dependency was removed. Track upstream `promql-parser` migration; the crate is used to build generated parser tables, not to decode untrusted runtime payloads. |

Deprecated `serde_yaml` and direct `bincode` usage have been removed from Query
API. Managed collector configuration is emitted as JSON, which remains valid
YAML 1.2 input for the collector.

## Container release security

Every tag build publishes a multi-platform image and records its immutable
`ghcr.io/<owner>/<repo>@sha256:...` coordinate in the workflow summary. The
release fails when Trivy finds a fixable high or critical vulnerability in that
digest. BuildKit also publishes maximum-mode provenance and an image SBOM as OCI
referrers.

The workflow generates an SPDX JSON SBOM directly from the pushed digest,
stores it in the `rush-api-<tag>-sbom` workflow artifact for 90 days, and
attests both the build provenance and SBOM with GitHub's OIDC-backed artifact
attestation service. The same attestations are pushed to GHCR as registry
referrers. Every third-party action in the release path is pinned to an
immutable commit SHA.

Copy the image coordinate from the release workflow summary into Helm's
`queryApi.image.repository` and `queryApi.image.digest` values. Verify the
published attestation before rollout:

```sh
gh attestation verify \
  'oci://ghcr.io/RushObservability/query-api@sha256:<digest>' \
  --repo RushObservability/query-api
```

Download the `rush-api-<tag>-sbom` artifact from the same workflow run when an
offline SPDX record is required. The digest in its package source must match the
release summary and deployed image.
