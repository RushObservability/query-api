use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

pub const POLICY_ENV: &str = "SRE_AGENT_GITHUB_REPOSITORY_POLICY";

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GitHubRepositoryGrant {
    pub repository: String,
    pub installation_id: u64,
    pub repository_id: u64,
}

pub fn canonical_repository(value: &str) -> Result<String> {
    let trimmed = value.trim().trim_end_matches(".git");
    let repository = trimmed
        .strip_prefix("https://github.com/")
        .unwrap_or(trimmed);
    let parts: Vec<_> = repository.split('/').collect();
    if parts.len() != 2
        || parts.iter().any(|part| {
            part.is_empty()
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
    {
        bail!("repository must be a GitHub owner/name pair")
    }
    Ok(format!("{}/{}", parts[0], parts[1]).to_ascii_lowercase())
}

pub fn resolve_grant_from_json(
    policy_json: &str,
    tenant_id: &str,
    requested_repository: &str,
) -> Result<Option<GitHubRepositoryGrant>> {
    let policy: HashMap<String, Vec<GitHubRepositoryGrant>> =
        serde_json::from_str(policy_json).context("invalid GitHub repository policy JSON")?;
    let requested = canonical_repository(requested_repository)?;
    let mut result = None;
    let mut repositories = HashSet::new();

    for (tenant, grants) in policy {
        if tenant.is_empty() {
            bail!("GitHub repository policy tenant IDs cannot be empty")
        }
        for mut grant in grants {
            if grant.installation_id == 0 || grant.repository_id == 0 {
                bail!("GitHub repository policy IDs must be non-zero")
            }
            grant.repository = canonical_repository(&grant.repository)
                .context("invalid repository in GitHub repository policy")?;
            if !repositories.insert((tenant.clone(), grant.repository.clone())) {
                bail!("duplicate GitHub repository grant")
            }
            if tenant == tenant_id && grant.repository == requested {
                result = Some(grant);
            }
        }
    }
    Ok(result)
}

pub fn resolve_grant(
    tenant_id: &str,
    requested_repository: &str,
) -> Result<Option<GitHubRepositoryGrant>> {
    let policy = std::env::var(POLICY_ENV).with_context(|| format!("{POLICY_ENV} is not set"))?;
    resolve_grant_from_json(&policy, tenant_id, requested_repository)
}

#[cfg(test)]
mod tests {
    use super::*;

    const POLICY: &str = r#"{
        "tenant-a": [{"repository":"Acme/API","installationId":42,"repositoryId":101}],
        "tenant-b": [{"repository":"acme/other","installationId":84,"repositoryId":202}]
    }"#;

    #[test]
    fn resolves_only_the_requested_tenant_repository() {
        let grant = resolve_grant_from_json(POLICY, "tenant-a", "https://github.com/acme/api.git")
            .unwrap()
            .unwrap();
        assert_eq!(grant.repository, "acme/api");
        assert_eq!(grant.installation_id, 42);
        assert_eq!(grant.repository_id, 101);
        assert!(
            resolve_grant_from_json(POLICY, "tenant-b", "acme/api")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn malformed_or_ambiguous_policy_fails_closed() {
        assert!(resolve_grant_from_json("not-json", "tenant-a", "acme/api").is_err());
        assert!(
            resolve_grant_from_json(
                r#"{"tenant-a":[{"repository":"acme/api","installationId":0,"repositoryId":1}]}"#,
                "tenant-a",
                "acme/api"
            )
            .is_err()
        );
        assert!(resolve_grant_from_json(
            r#"{"tenant-a":[{"repository":"acme/api","installationId":1,"repositoryId":1},{"repository":"ACME/API","installationId":2,"repositoryId":2}]}"#,
            "tenant-a",
            "acme/api"
        )
        .is_err());
    }
}
