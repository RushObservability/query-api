//! Pure policy helpers for scoped API keys and secure tenant defaults.

use std::net::IpAddr;

pub const INGEST_SIGNALS: [&str; 5] = ["logs", "traces", "metrics", "rum", "collector"];

pub fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(false)
}

pub fn allow_anonymous_default() -> bool {
    env_flag("RUSH_ALLOW_ANONYMOUS_DEFAULT")
}

pub fn default_tenant_auth_required(allow_anonymous: bool) -> bool {
    !allow_anonymous
}

pub fn effective_ingest_auth_required(explicit: Option<bool>, legacy_query_auth: bool) -> bool {
    explicit.unwrap_or(legacy_query_auth)
}

/// Absence means production. Development must be selected deliberately.
pub fn production_mode() -> bool {
    is_production_environment(std::env::var("RUSH_ENVIRONMENT").ok().as_deref())
}

pub fn is_production_environment(environment: Option<&str>) -> bool {
    !matches!(
        environment
            .unwrap_or("production")
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "development" | "dev" | "local" | "test"
    )
}

pub fn normalize_signals(signals: &[String]) -> Result<Vec<String>, String> {
    let mut normalized = signals
        .iter()
        .map(|signal| signal.trim().to_ascii_lowercase())
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    if normalized.is_empty() {
        return Err("ingest keys require at least one signal".to_string());
    }
    if let Some(signal) = normalized
        .iter()
        .find(|signal| !INGEST_SIGNALS.contains(&signal.as_str()))
    {
        return Err(format!("unsupported ingest signal '{signal}'"));
    }
    Ok(normalized)
}

pub fn normalize_source_cidrs(cidrs: &[String]) -> Result<Vec<String>, String> {
    let mut normalized = Vec::new();
    for raw in cidrs {
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        parse_network(raw).ok_or_else(|| format!("invalid source IP/CIDR '{raw}'"))?;
        normalized.push(raw.to_string());
    }
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
}

enum Network {
    V4(u32, u8),
    V6(u128, u8),
}

fn parse_network(raw: &str) -> Option<Network> {
    let (address, prefix) = raw.split_once('/').unwrap_or((raw, ""));
    let ip: IpAddr = address.parse().ok()?;
    match ip {
        IpAddr::V4(ip) => {
            let prefix = if prefix.is_empty() {
                32
            } else {
                prefix.parse().ok()?
            };
            (prefix <= 32).then_some(Network::V4(u32::from(ip), prefix))
        }
        IpAddr::V6(ip) => {
            let prefix = if prefix.is_empty() {
                128
            } else {
                prefix.parse().ok()?
            };
            (prefix <= 128).then_some(Network::V6(u128::from(ip), prefix))
        }
    }
}

pub fn source_allowed(ip: IpAddr, cidrs: &[String]) -> bool {
    if cidrs.is_empty() {
        return true;
    }
    cidrs.iter().any(|cidr| match (ip, parse_network(cidr)) {
        (IpAddr::V4(ip), Some(Network::V4(network, prefix))) => {
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            u32::from(ip) & mask == network & mask
        }
        (IpAddr::V6(ip), Some(Network::V6(network, prefix))) => {
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            u128::from(ip) & mask == network & mask
        }
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signals_are_explicit_and_allowlisted() {
        assert_eq!(
            normalize_signals(&["metrics".into(), "logs".into(), "logs".into()]).unwrap(),
            vec!["logs", "metrics"]
        );
        assert!(normalize_signals(&[]).is_err());
        assert!(normalize_signals(&["query".into()]).is_err());
        assert_eq!(
            normalize_signals(&["collector".into()]).unwrap(),
            vec!["collector"]
        );
    }

    #[test]
    fn source_cidrs_match_ipv4_and_ipv6() {
        let cidrs =
            normalize_source_cidrs(&["10.20.0.0/16".into(), "2001:db8::/32".into()]).unwrap();
        assert!(source_allowed("10.20.4.8".parse().unwrap(), &cidrs));
        assert!(!source_allowed("10.21.4.8".parse().unwrap(), &cidrs));
        assert!(source_allowed("2001:db8::5".parse().unwrap(), &cidrs));
    }

    #[test]
    fn invalid_source_restrictions_are_rejected() {
        assert!(normalize_source_cidrs(&["10.0.0.0/99".into()]).is_err());
        assert!(normalize_source_cidrs(&["example.com".into()]).is_err());
    }

    #[test]
    fn production_is_the_fail_closed_default() {
        assert!(is_production_environment(None));
        assert!(is_production_environment(Some("production")));
        assert!(is_production_environment(Some("staging")));
        assert!(!is_production_environment(Some("development")));
        assert!(!is_production_environment(Some("local")));
    }

    #[test]
    fn new_default_tenant_is_locked_without_the_explicit_override() {
        assert!(default_tenant_auth_required(false));
        assert!(!default_tenant_auth_required(true));
    }

    #[test]
    fn legacy_tenants_preserve_open_ingest_until_explicitly_changed() {
        assert!(!effective_ingest_auth_required(None, false));
        assert!(effective_ingest_auth_required(None, true));
        assert!(effective_ingest_auth_required(Some(true), false));
        assert!(!effective_ingest_auth_required(Some(false), true));
    }
}
