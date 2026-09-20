//! Shared notification transport. Production requests retain the outbound URL guards.

use reqwest::{Method, RequestBuilder, Response};
use serde_json::Value;

pub async fn request(method: Method, url: &str, strict: bool) -> Result<RequestBuilder, String> {
    // A task-scoped loopback server exercises the real HTTP serialization in tests.
    // This override is not compiled into the application and never uses environment flags.
    #[cfg(test)]
    if let Ok(request) = super::notification_tests::HTTP_MOCK.try_with(|mock| {
        mock.destinations
            .lock()
            .unwrap()
            .push((url.to_string(), strict));
        mock.client.request(method.clone(), &mock.url)
    }) {
        return Ok(request);
    }
    if strict {
        crate::outbound::strict_public_https_request(method, url).await
    } else {
        crate::outbound::public_https_request(method, url).await
    }
}

pub async fn send_json(
    request: RequestBuilder,
    payload: &Value,
    channel: &str,
) -> Result<Response, String> {
    let response = request
        .json(payload)
        .send()
        .await
        .map_err(|_| format!("{channel} delivery failed; check connectivity and try again"))?;
    if !response.status().is_success() {
        // Webhook URLs and remote bodies may contain credentials. Do not log them.
        return Err(format!(
            "{channel} rejected the notification (HTTP {})",
            response.status().as_u16()
        ));
    }
    Ok(response)
}
