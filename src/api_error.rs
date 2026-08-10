use axum::{
    Json,
    body::Body,
    extract::Request,
    http::{HeaderName, HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use std::fmt::Display;
use uuid::Uuid;

pub const REQUEST_ID_HEADER: &str = "x-request-id";

#[derive(Clone, Copy, Debug)]
struct NormalizedPublicError;

tokio::task_local! {
    static REQUEST_ID: String;
}

#[derive(Debug, Serialize)]
struct ErrorEnvelope<'a> {
    error: ErrorDetail<'a>,
}

#[derive(Debug, Serialize)]
struct ErrorDetail<'a> {
    code: &'a str,
    message: &'a str,
    request_id: &'a str,
}

/// A public HTTP error whose message is safe to return to an API client.
///
/// Internal causes belong in structured server logs and are deliberately not
/// stored on this type, which prevents an error-chain formatter from exposing
/// them later by accident.
#[derive(Debug, Clone)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    request_id: String,
}

impl ApiError {
    pub fn public(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
            request_id: current_request_id(),
        }
    }

    pub fn internal(operation: &'static str, error: impl Display) -> Self {
        Self::internal_with_status(StatusCode::INTERNAL_SERVER_ERROR, operation, error)
    }

    pub fn internal_with_status(
        status: StatusCode,
        operation: &'static str,
        error: impl Display,
    ) -> Self {
        let request_id = current_request_id();
        tracing::error!(
            operation,
            request_id = %request_id,
            error = %error,
            "request failed"
        );
        let (code, message) = safe_server_error(status);
        Self {
            status,
            code,
            message,
            request_id,
        }
    }

    /// Transitional adapter for handlers that still return `(StatusCode,
    /// String)`. The outer public-error middleware converts the tuple response
    /// to the same structured envelope as [`IntoResponse`].
    pub fn into_legacy(self) -> (StatusCode, String) {
        (self.status, self.message.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(ErrorEnvelope {
                error: ErrorDetail {
                    code: self.code,
                    message: self.message,
                    request_id: &self.request_id,
                },
            }),
        )
            .into_response();
        if let Ok(value) = HeaderValue::from_str(&self.request_id) {
            response
                .headers_mut()
                .insert(HeaderName::from_static(REQUEST_ID_HEADER), value);
        }
        response.extensions_mut().insert(NormalizedPublicError);
        response
    }
}

pub fn internal_legacy(operation: &'static str, error: impl Display) -> (StatusCode, String) {
    ApiError::internal(operation, error).into_legacy()
}

pub fn internal_legacy_with_status(
    status: StatusCode,
    operation: &'static str,
    error: impl Display,
) -> (StatusCode, String) {
    ApiError::internal_with_status(status, operation, error).into_legacy()
}

pub fn internal_legacy_with_message(
    status: StatusCode,
    operation: &'static str,
    error: impl Display,
    public_message: &'static str,
) -> (StatusCode, String) {
    // Constructing the typed error performs the correlated internal logging;
    // retain the established endpoint-specific public copy for legacy callers.
    let logged = ApiError::internal_with_status(status, operation, error);
    (logged.status, public_message.to_string())
}

pub fn current_request_id() -> String {
    REQUEST_ID
        .try_with(Clone::clone)
        .unwrap_or_else(|_| Uuid::new_v4().to_string())
}

fn new_request_id() -> String {
    // Always use a server-generated value. Accepting a caller-provided ID would
    // let an attacker create collisions in logs and audit records.
    Uuid::new_v4().to_string()
}

fn safe_server_error(status: StatusCode) -> (&'static str, &'static str) {
    match status {
        StatusCode::NOT_IMPLEMENTED => ("not_implemented", "feature is not available"),
        StatusCode::BAD_GATEWAY => ("upstream_unavailable", "upstream service is unavailable"),
        StatusCode::SERVICE_UNAVAILABLE => {
            ("service_unavailable", "service is temporarily unavailable")
        }
        StatusCode::GATEWAY_TIMEOUT => ("upstream_timeout", "upstream service timed out"),
        StatusCode::INTERNAL_SERVER_ERROR => ("internal_error", "request could not be completed"),
        _ => ("server_error", "request could not be completed"),
    }
}

fn normalize_server_error(mut response: Response, request_id: &str) -> Response {
    if response
        .extensions_mut()
        .remove::<NormalizedPublicError>()
        .is_some()
    {
        return response;
    }

    let status = response.status();
    let (code, message) = safe_server_error(status);
    tracing::error!(
        request_id,
        status = status.as_u16(),
        error_code = code,
        "server error response normalized"
    );
    let body = serde_json::to_vec(&ErrorEnvelope {
        error: ErrorDetail {
            code,
            message,
            request_id,
        },
    })
    .expect("fixed public error envelope must serialize");

    let (mut parts, _) = response.into_parts();
    parts.headers.remove(header::CONTENT_LENGTH);
    parts.headers.remove(header::CONTENT_ENCODING);
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    Response::from_parts(parts, Body::from(body))
}

/// Assign a request ID and normalize every 5xx response, including legacy tuple
/// errors produced before [`ApiError`] was introduced. This is intentionally the
/// outermost application middleware so tenant/auth failures receive the same
/// non-sensitive response contract as routed handlers.
pub async fn public_error_middleware(mut req: Request, next: Next) -> Response {
    let request_id = new_request_id();
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        req.headers_mut()
            .insert(HeaderName::from_static(REQUEST_ID_HEADER), value);
    }

    REQUEST_ID
        .scope(request_id.clone(), async move {
            let mut response = next.run(req).await;
            if response.status().is_server_error() {
                response = normalize_server_error(response, &request_id);
            }
            if let Ok(value) = HeaderValue::from_str(&request_id) {
                response
                    .headers_mut()
                    .insert(HeaderName::from_static(REQUEST_ID_HEADER), value);
            }
            response
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, routing::get};
    use proptest::prelude::*;
    use std::sync::{Arc, Mutex};
    use tower::ServiceExt;
    use tracing::{Event, Subscriber, field::Visit};
    use tracing_subscriber::{Layer, layer::Context, prelude::*};

    #[derive(Clone)]
    struct CaptureLayer(Arc<Mutex<Vec<String>>>);

    struct CaptureVisitor<'a>(&'a mut String);

    impl Visit for CaptureVisitor<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.push_str(field.name());
            self.0.push('=');
            self.0.push_str(&format!("{value:?}"));
            self.0.push(' ');
        }
    }

    impl<S> Layer<S> for CaptureLayer
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let mut rendered = String::new();
            event.record(&mut CaptureVisitor(&mut rendered));
            self.0.lock().unwrap().push(rendered);
        }
    }

    #[test]
    fn api_error_never_formats_the_internal_cause() {
        let error = ApiError::internal(
            "test.database",
            "clickhouse host=db.internal password=secret table=users",
        );
        let serialized = serde_json::to_string(&ErrorEnvelope {
            error: ErrorDetail {
                code: error.code,
                message: error.message,
                request_id: &error.request_id,
            },
        })
        .unwrap();
        assert!(!serialized.contains("clickhouse"));
        assert!(!serialized.contains("secret"));
        assert!(!serialized.contains("users"));
    }

    #[test]
    fn internal_cause_and_request_id_are_kept_in_structured_logs() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(CaptureLayer(events.clone()));
        let error = tracing::subscriber::with_default(subscriber, || {
            ApiError::internal_with_status(
                StatusCode::BAD_GATEWAY,
                "test.upstream",
                "upstream host=llm.internal response=secret",
            )
        });

        let rendered = events.lock().unwrap().join("\n");
        assert!(rendered.contains("operation=\"test.upstream\""));
        assert!(rendered.contains("upstream host=llm.internal response=secret"));
        assert!(rendered.contains(&error.request_id));
    }

    #[tokio::test]
    async fn middleware_replaces_a_legacy_internal_error_body() {
        let app = Router::new()
            .route(
                "/failure",
                get(|| async {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "db.internal password=secret SELECT * FROM users",
                    )
                }),
            )
            .layer(axum::middleware::from_fn(public_error_middleware));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/failure")
                    .header(REQUEST_ID_HEADER, "test-request-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let request_id = response
            .headers()
            .get(REQUEST_ID_HEADER)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(Uuid::parse_str(&request_id).is_ok());
        assert_ne!(request_id, "test-request-123");
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("\"code\":\"internal_error\""));
        assert!(text.contains(&format!("\"request_id\":\"{request_id}\"")));
        assert!(!text.contains("db.internal"));
        assert!(!text.contains("secret"));
        assert!(!text.contains("SELECT"));
    }

    #[tokio::test]
    async fn middleware_never_trusts_a_caller_request_id() {
        let app = Router::new()
            .route("/ok", get(|| async { StatusCode::NO_CONTENT }))
            .layer(axum::middleware::from_fn(public_error_middleware));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ok")
                    .header(REQUEST_ID_HEADER, "invalid request id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let value = response
            .headers()
            .get(REQUEST_ID_HEADER)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(Uuid::parse_str(value).is_ok());
        assert_ne!(value, "invalid request id");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn arbitrary_internal_causes_never_enter_public_error_json(cause in ".{1,512}") {
            let sensitive = format!("SENSITIVE_CAUSE_BEGIN:{cause}:SENSITIVE_CAUSE_END");
            let error = ApiError::internal("property.internal", &sensitive);
            let json = serde_json::to_string(&ErrorEnvelope {
                error: ErrorDetail {
                    code: error.code,
                    message: error.message,
                    request_id: &error.request_id,
                },
            }).unwrap();
            prop_assert_eq!(error.code, "internal_error");
            prop_assert_eq!(error.message, "request could not be completed");
            prop_assert!(!json.contains(&sensitive));
        }
    }
}
