//! Guarded outbound HTTP for user-configured notification endpoints.
//!
//! Notification URLs are administrator-controlled but must not become a path
//! into cluster-local services. We resolve and pin the destination addresses
//! before sending so redirects and DNS rebinding cannot bypass the checks.

use reqwest::{Method, RequestBuilder};
use std::net::IpAddr;
use std::time::Duration;
use url::Url;

pub(crate) fn blocked_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.octets()[0] == 0
                || ip.octets()[0] >= 224
                || (ip.octets()[0] == 100 && (64..=127).contains(&ip.octets()[1]))
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return blocked_address(IpAddr::V4(mapped));
            }
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
        }
    }
}

fn allow_private_notification_urls() -> bool {
    [
        "RUSH_ALLOW_PRIVATE_NOTIFICATION_URLS",
        // Keep the original local-dev flag as a compatibility alias.
        "RUSH_ALLOW_INSECURE_LOCAL_NOTIFICATIONS",
    ]
    .iter()
    .any(|name| {
        std::env::var(name)
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

/// Validate a configured notification endpoint without contacting it.
///
/// Reachability and DNS pinning belong to the send path. Keeping this check
/// side-effect free lets users save a channel before its endpoint is online.
/// HTTP is accepted for internal endpoints; the send path still requires an
/// explicit private-endpoint flag before it will connect to private or
/// loopback addresses.
pub fn validate_notification_url(raw_url: &str) -> Result<(), String> {
    let url = Url::parse(raw_url).map_err(|_| "notification URL is invalid".to_string())?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err("notification URL must not include user credentials".to_string());
    }
    url.host_str()
        .ok_or_else(|| "notification URL must include a host".to_string())?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("notification URL must use HTTP or HTTPS".to_string());
    }
    Ok(())
}

/// Create a guarded request to a notification endpoint. The returned client
/// has no redirects, bounded connect/total timeouts, and pinned DNS
/// resolution. Private/internal targets require explicit configuration.
pub async fn public_https_request(method: Method, raw_url: &str) -> Result<RequestBuilder, String> {
    guarded_request(method, raw_url, allow_private_notification_urls()).await
}

/// Strict public-HTTPS request path for identity-provider metadata and other
/// security-sensitive server-side fetches. Unlike notification delivery this
/// has no private-network compatibility override.
pub async fn strict_public_https_request(
    method: Method,
    raw_url: &str,
) -> Result<RequestBuilder, String> {
    guarded_request(method, raw_url, false).await
}

async fn guarded_request(
    method: Method,
    raw_url: &str,
    allow_private: bool,
) -> Result<RequestBuilder, String> {
    validate_notification_url(raw_url)?;
    let url = Url::parse(raw_url).map_err(|_| "notification URL is invalid".to_string())?;
    let host = url
        .host_str()
        .ok_or_else(|| "notification URL must include a host".to_string())?
        .to_string();
    let port = url.port_or_known_default().unwrap_or(443);

    let addresses: Vec<std::net::SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|_| "notification host could not be resolved".to_string())?
        .collect();
    if addresses.is_empty() {
        return Err("notification host could not be resolved".to_string());
    }
    if url.scheme() == "http" && !allow_private {
        return Err("endpoint must use HTTPS".to_string());
    }
    if !allow_private
        && addresses
            .iter()
            .any(|address| blocked_address(address.ip()))
    {
        return Err("notification URL must resolve only to public addresses".to_string());
    }

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .resolve_to_addrs(&host, &addresses)
        .build()
        .map_err(|_| "failed to prepare notification request".to_string())?;
    Ok(client.request(method, url))
}

/// Send a JSON notification through the guarded request path.
pub async fn post_json(raw_url: &str, payload: &serde_json::Value) -> Result<(), String> {
    public_https_request(Method::POST, raw_url)
        .await?
        .json(payload)
        .send()
        .await
        .map_err(|e| format!("notification request failed: {e}"))?
        .error_for_status()
        .map_err(|e| format!("notification endpoint returned an error: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::blocked_address;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn rejects_private_and_loopback_addresses() {
        assert!(blocked_address(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(blocked_address(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
        assert!(blocked_address(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(!blocked_address(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
    }
}
