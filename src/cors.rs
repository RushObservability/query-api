use axum::http::{HeaderValue, Method, header};
use std::collections::HashSet;
use tower_http::cors::{AllowCredentials, AllowOrigin, CorsLayer};
use url::Url;

/// An exact, validated browser origin. The URL form is used by CSRF checks and
/// the serialized header form is used by tower-http's CORS layer.
#[derive(Clone, Debug)]
struct ValidatedOrigin {
    url: Url,
    header: HeaderValue,
}

/// Startup-validated CORS policy. An empty policy means no cross-origin access;
/// same-origin browser requests do not need CORS response headers.
#[derive(Clone, Debug, Default)]
pub struct CorsPolicy {
    origins: Vec<ValidatedOrigin>,
}

/// Parse an HTTP(S) browser origin and reject URL features that are not part of
/// an Origin header. Default ports are normalized by `Url`.
pub fn parse_web_origin(raw: &str) -> Option<Url> {
    let parsed = Url::parse(raw).ok()?;
    let host = parsed.host_str()?;
    if !matches!(parsed.scheme(), "http" | "https")
        || host.contains('*')
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return None;
    }
    Some(parsed)
}

pub fn same_web_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

impl CorsPolicy {
    pub fn from_env() -> Result<Self, String> {
        match std::env::var("RUSH_ALLOWED_ORIGINS") {
            Ok(raw) => Self::parse(Some(&raw)),
            Err(std::env::VarError::NotPresent) => Self::parse(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err("RUSH_ALLOWED_ORIGINS must contain valid UTF-8".to_string())
            }
        }
    }

    pub fn parse(raw: Option<&str>) -> Result<Self, String> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        if raw.trim().is_empty() {
            return Ok(Self::default());
        }

        let mut origins = Vec::new();
        let mut seen = HashSet::new();
        for (index, entry) in raw.split(',').enumerate() {
            let entry = entry.trim();
            let position = index + 1;
            if entry.is_empty() {
                return Err(format!("RUSH_ALLOWED_ORIGINS entry {position} is empty"));
            }
            if entry.eq_ignore_ascii_case("null") {
                return Err(format!(
                    "RUSH_ALLOWED_ORIGINS entry {position} must not be the opaque null origin"
                ));
            }
            if entry == "*" || entry.contains("//*") {
                return Err(format!(
                    "RUSH_ALLOWED_ORIGINS entry {position} must not contain a wildcard"
                ));
            }

            let Some(url) = parse_web_origin(entry) else {
                return Err(format!(
                    "RUSH_ALLOWED_ORIGINS entry {position} must be an HTTP(S) origin without credentials, path, query, fragment, or wildcard"
                ));
            };
            let serialized = url.origin().ascii_serialization();
            let header = HeaderValue::from_str(&serialized).map_err(|_| {
                format!("RUSH_ALLOWED_ORIGINS entry {position} is not a valid HTTP header value")
            })?;
            if seen.insert(serialized) {
                origins.push(ValidatedOrigin { url, header });
            }
        }
        Ok(Self { origins })
    }

    pub fn is_empty(&self) -> bool {
        self.origins.is_empty()
    }

    pub fn allows(&self, candidate: &Url) -> bool {
        self.origins
            .iter()
            .any(|origin| same_web_origin(candidate, &origin.url))
    }

    pub fn layer(&self) -> CorsLayer {
        let layer = CorsLayer::new()
            .allow_methods([
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::DELETE,
                Method::PATCH,
                Method::OPTIONS,
            ])
            .allow_headers([
                header::CONTENT_TYPE,
                header::AUTHORIZATION,
                header::HeaderName::from_static("x-rush-tenant"),
                header::HeaderName::from_static("dd-api-key"),
            ])
            .vary([
                header::ORIGIN,
                header::ACCESS_CONTROL_REQUEST_METHOD,
                header::ACCESS_CONTROL_REQUEST_HEADERS,
            ]);

        if self.origins.is_empty() {
            layer
        } else {
            let allowed_headers = self
                .origins
                .iter()
                .map(|origin| origin.header.clone())
                .collect::<Vec<_>>();
            layer
                .allow_origin(AllowOrigin::list(allowed_headers.clone()))
                .allow_credentials(AllowCredentials::predicate(move |origin, _parts| {
                    allowed_headers.contains(origin)
                }))
        }
    }

    #[cfg(test)]
    fn serialized_origins(&self) -> Vec<&str> {
        self.origins
            .iter()
            .map(|origin| origin.header.to_str().expect("validated header"))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::CorsPolicy;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{HeaderValue, Request, StatusCode, header};
    use axum::routing::get;
    use tower::ServiceExt;

    #[test]
    fn unset_or_blank_allowlist_disables_cross_origin_access() {
        assert!(CorsPolicy::parse(None).unwrap().is_empty());
        assert!(CorsPolicy::parse(Some("  ")).unwrap().is_empty());
    }

    #[test]
    fn valid_origins_are_canonicalized_and_deduplicated() {
        let policy = CorsPolicy::parse(Some(
            "https://EXAMPLE.com:443, http://localhost:5173, https://example.com",
        ))
        .unwrap();
        assert_eq!(
            policy.serialized_origins(),
            vec!["https://example.com", "http://localhost:5173"]
        );
    }

    #[test]
    fn invalid_origins_fail_configuration_instead_of_being_dropped() {
        for raw in [
            "null",
            "NULL",
            "*",
            "https://*.example.com",
            "file:///tmp/app.html",
            "ftp://example.com",
            "https://user@example.com",
            "https://user:pass@example.com",
            "https://example.com/path",
            "https://example.com?query=yes",
            "https://example.com#fragment",
            "https://example.com,,https://app.example.com",
            "https://example.com,",
            "not an origin",
        ] {
            let error = CorsPolicy::parse(Some(raw))
                .expect_err(&format!("accepted invalid CORS origin {raw:?}"));
            assert!(
                error.contains("RUSH_ALLOWED_ORIGINS entry"),
                "unclear configuration error for {raw:?}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn configured_origin_gets_exact_credentialed_headers_and_vary() {
        let policy = CorsPolicy::parse(Some("https://app.example.com")).unwrap();
        let app = Router::new()
            .route("/", get(|| async { "ok" }))
            .layer(policy.layer());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header(header::ORIGIN, "https://app.example.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("https://app.example.com"))
        );
        assert_eq!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_CREDENTIALS),
            Some(&HeaderValue::from_static("true"))
        );
        let vary = response
            .headers()
            .get(header::VARY)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase();
        assert!(vary.contains("origin"));
    }

    #[tokio::test]
    async fn configured_origin_preflight_is_allowed_exactly() {
        let policy = CorsPolicy::parse(Some("https://app.example.com")).unwrap();
        let app = Router::new()
            .route("/", get(|| async { "ok" }))
            .layer(policy.layer());
        let response = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/")
                    .header(header::ORIGIN, "https://app.example.com")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "content-type")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("https://app.example.com"))
        );
        assert_eq!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_CREDENTIALS),
            Some(&HeaderValue::from_static("true"))
        );
        assert!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_METHODS)
                .is_some()
        );
    }

    #[tokio::test]
    async fn empty_policy_and_unconfigured_origins_get_no_cors_allowance() {
        for (policy, origin) in [
            (CorsPolicy::parse(None).unwrap(), "null"),
            (
                CorsPolicy::parse(Some("https://app.example.com")).unwrap(),
                "https://attacker.example.com",
            ),
        ] {
            let app = Router::new()
                .route("/", get(|| async { "ok" }))
                .layer(policy.layer());
            let response = app
                .oneshot(
                    Request::builder()
                        .uri("/")
                        .header(header::ORIGIN, origin)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(
                response
                    .headers()
                    .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                    .is_none(),
                "origin {origin:?} received a CORS allowance"
            );
            assert!(
                response
                    .headers()
                    .get(header::ACCESS_CONTROL_ALLOW_CREDENTIALS)
                    .is_none()
            );
        }
    }
}
