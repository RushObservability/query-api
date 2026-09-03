use axum::http::HeaderMap;

pub const INTERNAL_TOKEN_HEADER: &str = "x-rush-internal-token";

/// Authenticate a request made by the SRE agent. The shared token is only an
/// identity signal; route authorization remains an explicit middleware check.
pub fn sre_agent_token_matches(headers: &HeaderMap) -> bool {
    let Some(expected) = std::env::var("SRE_AGENT_INTERNAL_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return false;
    };
    let Some(actual) = headers
        .get(INTERNAL_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };

    if actual.len() != expected.len() {
        return false;
    }
    actual
        .bytes()
        .zip(expected.bytes())
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}
