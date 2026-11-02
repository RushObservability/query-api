//! Guarded outbound HTTP for user-configured notification endpoints.
//!
//! Notification URLs are administrator-controlled but must not become a path
//! into cluster-local services. We resolve and pin the destination addresses
//! before sending so redirects and DNS rebinding cannot bypass the checks.

use reqwest::{Method, RequestBuilder};
use std::net::IpAddr;
use std::time::Duration;
use url::Url;

const DNS_TIMEOUT: Duration = Duration::from_secs(5);

async fn resolve_with_timeout(
    lookup: impl std::future::Future<Output = std::io::Result<Vec<std::net::SocketAddr>>>,
    timeout: Duration,
) -> Result<Vec<std::net::SocketAddr>, String> {
    let addresses = tokio::time::timeout(timeout, lookup)
        .await
        .map_err(|_| "notification host resolution timed out".to_string())?
        .map_err(|_| "notification host could not be resolved".to_string())?;
    if addresses.is_empty() {
        return Err("notification host could not be resolved".to_string());
    }
    Ok(addresses)
}

fn pinned_client(
    host: &str,
    addresses: &[std::net::SocketAddr],
) -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        // A proxy would resolve the target independently of these checked IPs.
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .resolve_to_addrs(host, addresses)
        .build()
        .map_err(|_| "failed to prepare notification request".to_string())
}

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
    // A background engine that lost its leader lease must not notify anyone;
    // the replica that holds the lease now will.
    crate::leader::ensure_leader().map_err(|error| error.to_string())?;
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

    if url.scheme() == "http" && !allow_private {
        return Err("endpoint must use HTTPS".to_string());
    }
    let addresses = resolve_with_timeout(
        async {
            tokio::net::lookup_host((host.as_str(), port))
                .await
                .map(Iterator::collect)
        },
        DNS_TIMEOUT,
    )
    .await?;
    if !allow_private
        && addresses
            .iter()
            .any(|address| blocked_address(address.ip()))
    {
        return Err("notification URL must resolve only to public addresses".to_string());
    }

    let client = pinned_client(&host, &addresses)?;
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

    #[tokio::test]
    async fn dns_resolution_has_a_deadline_and_rejects_empty_results() {
        let result = super::resolve_with_timeout(
            std::future::pending(),
            std::time::Duration::from_millis(20),
        )
        .await;
        assert!(result.unwrap_err().contains("timed out"));
        assert!(
            super::resolve_with_timeout(async { Ok(vec![]) }, std::time::Duration::from_secs(1))
                .await
                .is_err()
        );
        assert!(
            super::strict_public_https_request(reqwest::Method::GET, "https://127.0.0.1/")
                .await
                .unwrap_err()
                .contains("public addresses")
        );
        assert!(
            super::strict_public_https_request(reqwest::Method::GET, "http://127.0.0.1/")
                .await
                .unwrap_err()
                .contains("HTTPS")
        );
    }

    #[tokio::test]
    async fn pinned_client_ignores_environment_proxies_and_redirects() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        if let Ok(address) = std::env::var("RUSH_TEST_PINNED_ADDRESS") {
            let address: std::net::SocketAddr = address.parse().unwrap();
            let client = super::pinned_client("pinned.invalid", &[address]).unwrap();
            let response = client
                .get(format!("http://pinned.invalid:{}/", address.port()))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::FOUND);
            return;
        }
        let destination = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = destination.local_addr().unwrap();
        let target = tokio::spawn(async move {
            let (mut stream, _) = destination.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            stream.write_all(b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:9/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
        });
        let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_url = format!("http://{}", proxy.local_addr().unwrap());
        let proxy_seen = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = proxy_seen.clone();
        let proxy_task = tokio::spawn(async move {
            let (mut stream, _) = proxy.accept().await.unwrap();
            seen.store(true, std::sync::atomic::Ordering::SeqCst);
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await;
        });
        // Isolate proxy variables in a child; mutating process environment
        // would race other tests and is unsafe in Rust 2024.
        let output = tokio::task::spawn_blocking(move || {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap());
            child
                .args([
                    "--exact",
                    "outbound::tests::pinned_client_ignores_environment_proxies_and_redirects",
                    "--nocapture",
                ])
                .env("RUSH_TEST_PINNED_ADDRESS", address.to_string())
                .env("NO_PROXY", "")
                .env("no_proxy", "");
            for variable in [
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "ALL_PROXY",
                "http_proxy",
                "https_proxy",
                "all_proxy",
            ] {
                child.env(variable, &proxy_url);
            }
            child.output().unwrap()
        })
        .await
        .unwrap();
        proxy_task.abort();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!proxy_seen.load(std::sync::atomic::Ordering::SeqCst));
        target.await.unwrap();
    }

    #[test]
    fn rejects_private_and_loopback_addresses() {
        assert!(blocked_address(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(blocked_address(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
        assert!(blocked_address(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(!blocked_address(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
    }
}
