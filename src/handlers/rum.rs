use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};

use crate::AppState;
use crate::models::query::{TimeRange, Filter, FilterOp};
use crate::models::rum::RumRecord;
use crate::models::trace::WideEvent;
use crate::query_builder::{format_value, format_array_value};

// ── Field resolver ──

fn resolve_rum_field(field: &str) -> String {
    match field {
        "app_name" | "AppName" => "AppName".to_string(),
        "app_version" | "AppVersion" => "AppVersion".to_string(),
        "environment" | "Environment" => "Environment".to_string(),
        "session_id" | "SessionId" => "SessionId".to_string(),
        "user_id" | "UserId" => "UserId".to_string(),
        "page_url" | "PageUrl" => "PageUrl".to_string(),
        "page_path" | "PagePath" => "PagePath".to_string(),
        "view_name" | "ViewName" => "ViewName".to_string(),
        "referrer" | "Referrer" => "Referrer".to_string(),
        "browser_name" | "BrowserName" => "BrowserName".to_string(),
        "browser_version" | "BrowserVersion" => "BrowserVersion".to_string(),
        "os_name" | "OsName" => "OsName".to_string(),
        "os_version" | "OsVersion" => "OsVersion".to_string(),
        "device_type" | "DeviceType" => "DeviceType".to_string(),
        "event_type" | "EventType" => "EventType".to_string(),
        "event_name" | "EventName" => "EventName".to_string(),
        "vital_name" | "VitalName" => "VitalName".to_string(),
        "vital_rating" | "VitalRating" => "VitalRating".to_string(),
        "error_message" | "ErrorMessage" => "ErrorMessage".to_string(),
        "error_type" | "ErrorType" => "ErrorType".to_string(),
        "interaction_type" | "InteractionType" => "InteractionType".to_string(),
        "trace_id" | "TraceId" => "TraceId".to_string(),
        _ => {
            let escaped = field.replace('\'', "\\'");
            format!("'{escaped}'")
        }
    }
}

fn build_rum_where(filters: &[Filter], from: &str, to: &str) -> String {
    let mut conditions = vec![
        format!("Timestamp >= parseDateTimeBestEffort('{from}')"),
        format!("Timestamp <= parseDateTimeBestEffort('{to}')"),
    ];

    for filter in filters {
        let field = resolve_rum_field(&filter.field);
        let condition = match &filter.op {
            FilterOp::Eq => format!("{field} = {}", format_value(&filter.value)),
            FilterOp::Ne => format!("{field} != {}", format_value(&filter.value)),
            FilterOp::Gt => format!("{field} > {}", format_value(&filter.value)),
            FilterOp::Gte => format!("{field} >= {}", format_value(&filter.value)),
            FilterOp::Lt => format!("{field} < {}", format_value(&filter.value)),
            FilterOp::Lte => format!("{field} <= {}", format_value(&filter.value)),
            FilterOp::Like => format!("{field} LIKE {}", format_value(&filter.value)),
            FilterOp::NotLike => format!("{field} NOT LIKE {}", format_value(&filter.value)),
            FilterOp::In => format!("{field} IN {}", format_array_value(&filter.value)),
            FilterOp::NotIn => format!("{field} NOT IN {}", format_array_value(&filter.value)),
        };
        conditions.push(condition);
    }

    conditions.join(" AND ")
}

// ── Request / response types ──

#[derive(Debug, serde::Deserialize)]
pub struct RumIngestPayload {
    pub meta: RumIngestMeta,
    pub events: Vec<RumIngestEvent>,
}

#[derive(Debug, serde::Deserialize)]
pub struct RumIngestMeta {
    pub app_name: String,
    #[serde(default)]
    pub app_version: String,
    #[serde(default)]
    pub environment: String,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub user_id: String,
    #[serde(default)]
    pub page_url: String,
    #[serde(default)]
    pub page_path: String,
    #[serde(default)]
    pub view_name: String,
    #[serde(default)]
    pub referrer: String,
    #[serde(default)]
    pub browser_name: String,
    #[serde(default)]
    pub browser_version: String,
    #[serde(default)]
    pub os_name: String,
    #[serde(default)]
    pub os_version: String,
    #[serde(default)]
    pub device_type: String,
    #[serde(default)]
    pub screen_width: u16,
    #[serde(default)]
    pub screen_height: u16,
}

#[derive(Debug, serde::Deserialize)]
pub struct RumIngestEvent {
    pub event_type: String,
    #[serde(default)]
    pub event_name: String,
    #[serde(default)]
    pub timestamp: Option<i64>,
    #[serde(default)]
    pub vital_name: String,
    #[serde(default)]
    pub vital_value: f64,
    #[serde(default)]
    pub vital_rating: String,
    #[serde(default)]
    pub error_message: String,
    #[serde(default)]
    pub error_stack: String,
    #[serde(default)]
    pub error_type: String,
    #[serde(default)]
    pub interaction_target: String,
    #[serde(default)]
    pub interaction_type: String,
    #[serde(default)]
    pub duration_ms: f64,
    #[serde(default)]
    pub trace_id: String,
    #[serde(default)]
    pub span_id: String,
    #[serde(default)]
    pub attributes: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct RumQueryRequest {
    pub time_range: TimeRange,
    #[serde(default)]
    pub filters: Vec<Filter>,
    #[serde(default = "default_limit")]
    pub limit: u64,
    #[serde(default)]
    pub offset: u64,
}

fn default_limit() -> u64 { 100 }

#[derive(Debug, serde::Serialize, clickhouse::Row, serde::Deserialize)]
pub struct RumAppRow {
    #[serde(rename = "AppName")]
    pub app_name: String,
    #[serde(rename = "cnt")]
    pub count: u64,
}

#[derive(Debug, serde::Serialize, clickhouse::Row, serde::Deserialize)]
pub struct RumVitalRow {
    #[serde(rename = "VitalName")]
    pub vital_name: String,
    pub p75: f64,
    pub good_pct: f64,
    pub needs_improvement_pct: f64,
    pub poor_pct: f64,
}

#[derive(Debug, serde::Serialize, clickhouse::Row, serde::Deserialize)]
pub struct RumPageRow {
    #[serde(rename = "PagePath")]
    pub page_path: String,
    pub views: u64,
    pub unique_sessions: u64,
    pub avg_load_ms: f64,
    pub error_count: u64,
}

#[derive(Debug, serde::Serialize, clickhouse::Row, serde::Deserialize)]
pub struct RumErrorRow {
    #[serde(rename = "ErrorMessage")]
    pub error_message: String,
    #[serde(rename = "ErrorType")]
    pub error_type: String,
    pub count: u64,
    pub affected_sessions: u64,
    pub last_seen: String,
    pub sample_stack: String,
}

#[derive(Debug, serde::Serialize, clickhouse::Row, serde::Deserialize)]
pub struct RumSessionRow {
    #[serde(rename = "SessionId")]
    pub session_id: String,
    #[serde(rename = "UserId")]
    pub user_id: String,
    pub browser: String,
    pub page_count: u64,
    pub error_count: u64,
    pub duration_s: f64,
    pub first_seen: String,
}

// ── Handlers ──

/// POST /api/v1/rum/ingest — SDK sends batched events
pub async fn ingest(
    State(state): State<AppState>,
    Json(payload): Json<RumIngestPayload>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let meta = &payload.meta;
    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    let mut insert = state
        .ch
        .insert("rum_events")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("insert init: {e}")))?;

    for evt in &payload.events {
        let ts = evt.timestamp.unwrap_or(now_ns);
        let record = RumRecord {
            timestamp: ts,
            app_name: meta.app_name.clone(),
            app_version: meta.app_version.clone(),
            environment: meta.environment.clone(),
            session_id: meta.session_id.clone(),
            user_id: meta.user_id.clone(),
            page_url: meta.page_url.clone(),
            page_path: meta.page_path.clone(),
            view_name: meta.view_name.clone(),
            referrer: meta.referrer.clone(),
            browser_name: meta.browser_name.clone(),
            browser_version: meta.browser_version.clone(),
            os_name: meta.os_name.clone(),
            os_version: meta.os_version.clone(),
            device_type: meta.device_type.clone(),
            screen_width: meta.screen_width,
            screen_height: meta.screen_height,
            event_type: evt.event_type.clone(),
            event_name: evt.event_name.clone(),
            vital_name: evt.vital_name.clone(),
            vital_value: evt.vital_value,
            vital_rating: evt.vital_rating.clone(),
            error_message: evt.error_message.clone(),
            error_stack: evt.error_stack.clone(),
            error_type: evt.error_type.clone(),
            interaction_target: evt.interaction_target.clone(),
            interaction_type: evt.interaction_type.clone(),
            duration_ms: evt.duration_ms,
            trace_id: evt.trace_id.clone(),
            span_id: evt.span_id.clone(),
            attributes: evt.attributes.clone(),
        };
        insert.write(&record).await.map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("insert write: {e}"))
        })?;
    }

    insert.end().await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("insert end: {e}"))
    })?;

    // Insert synthetic spans into wide_events for RUM events with trace IDs.
    // This allows clicking "View trace" on pageview events in the RUM dashboard.
    let trace_events: Vec<&RumIngestEvent> = payload
        .events
        .iter()
        .filter(|e| !e.trace_id.is_empty() && !e.span_id.is_empty())
        .collect();

    if !trace_events.is_empty() {
        tracing::info!("RUM ingest: creating {} synthetic span(s) in wide_events", trace_events.len());
        match state.ch.insert("wide_events") {
            Ok(mut span_insert) => {
                for evt in &trace_events {
                    let ts = evt.timestamp.unwrap_or(now_ns);
                    let duration_ns = (evt.duration_ms * 1_000_000.0) as u64;
                    let attrs = serde_json::json!({
                        "rum.session_id": meta.session_id,
                        "rum.event_type": evt.event_type,
                        "browser.name": meta.browser_name,
                        "browser.version": meta.browser_version,
                        "os.name": meta.os_name,
                        "os.version": meta.os_version,
                        "device.type": meta.device_type,
                        "screen.width": meta.screen_width,
                        "screen.height": meta.screen_height,
                        "referrer": meta.referrer,
                    });

                    let span = WideEvent {
                        timestamp: ts,
                        trace_id: evt.trace_id.clone(),
                        span_id: evt.span_id.clone(),
                        parent_span_id: String::new(),
                        service_name: meta.app_name.clone(),
                        service_version: meta.app_version.clone(),
                        environment: meta.environment.clone(),
                        host_name: format!("{} {}", meta.browser_name, meta.browser_version),
                        http_method: "GET".to_string(),
                        http_path: meta.page_path.clone(),
                        http_status_code: 200,
                        duration_ns,
                        status: "OK".to_string(),
                        attributes: attrs.to_string(),
                        event_timestamps: vec![],
                        event_names: vec![],
                        event_attributes: vec![],
                        link_trace_ids: vec![],
                        link_span_ids: vec![],
                    };
                    if let Err(e) = span_insert.write(&span).await {
                        tracing::error!("RUM span write failed: {e}");
                    }
                }
                if let Err(e) = span_insert.end().await {
                    tracing::error!("RUM span insert end failed: {e}");
                } else {
                    tracing::info!("RUM synthetic spans committed OK");
                }
            }
            Err(e) => {
                tracing::error!("RUM span insert init failed: {e}");
            }
        }
    }

    Ok((StatusCode::OK, Json(serde_json::json!({ "accepted": payload.events.len() }))))
}

/// GET /api/v1/rum/apps — list known apps
pub async fn list_apps(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let sql = "SELECT AppName, count() as cnt FROM rum_events GROUP BY AppName ORDER BY cnt DESC";
    let rows = state
        .ch
        .query(sql)
        .fetch_all::<RumAppRow>()
        .await
        .map_err(|e| {
            tracing::error!("RUM apps query failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
        })?;

    Ok(Json(serde_json::json!({ "apps": rows })))
}

/// POST /api/v1/rum/query — raw event query
pub async fn query_events(
    State(state): State<AppState>,
    Json(req): Json<RumQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let where_clause = build_rum_where(&req.filters, &req.time_range.from, &req.time_range.to);

    let sql = format!(
        "SELECT Timestamp, AppName, AppVersion, Environment, SessionId, UserId, \
         PageUrl, PagePath, ViewName, Referrer, BrowserName, BrowserVersion, \
         OsName, OsVersion, DeviceType, ScreenWidth, ScreenHeight, \
         EventType, EventName, VitalName, VitalValue, VitalRating, \
         ErrorMessage, ErrorStack, ErrorType, InteractionTarget, InteractionType, \
         DurationMs, TraceId, SpanId, Attributes \
         FROM rum_events WHERE {where_clause} \
         ORDER BY Timestamp DESC LIMIT {} OFFSET {}",
        req.limit.min(1000),
        req.offset,
    );

    let rows = state
        .ch
        .query(&sql)
        .fetch_all::<RumRecord>()
        .await
        .map_err(|e| {
            tracing::error!("RUM query failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
        })?;

    let json_rows: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| serde_json::to_value(r).unwrap_or(serde_json::Value::Null))
        .collect();

    Ok(Json(serde_json::json!({ "rows": json_rows, "total": json_rows.len() })))
}

/// POST /api/v1/rum/vitals — web vitals aggregation
pub async fn vitals(
    State(state): State<AppState>,
    Json(req): Json<RumQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let mut filters = req.filters.clone();
    filters.push(Filter {
        field: "EventType".to_string(),
        op: FilterOp::Eq,
        value: serde_json::Value::String("web_vital".to_string()),
    });
    let where_clause = build_rum_where(&filters, &req.time_range.from, &req.time_range.to);

    let sql = format!(
        "SELECT \
           VitalName, \
           quantile(0.75)(VitalValue) as p75, \
           countIf(VitalRating = 'good') * 100.0 / count() as good_pct, \
           countIf(VitalRating = 'needs-improvement') * 100.0 / count() as needs_improvement_pct, \
           countIf(VitalRating = 'poor') * 100.0 / count() as poor_pct \
         FROM rum_events \
         WHERE {where_clause} \
         GROUP BY VitalName \
         ORDER BY VitalName"
    );

    let rows = state
        .ch
        .query(&sql)
        .fetch_all::<RumVitalRow>()
        .await
        .map_err(|e| {
            tracing::error!("RUM vitals query failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
        })?;

    Ok(Json(serde_json::json!({ "vitals": rows })))
}

/// POST /api/v1/rum/pages — page performance
pub async fn pages(
    State(state): State<AppState>,
    Json(req): Json<RumQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let where_clause = build_rum_where(&req.filters, &req.time_range.from, &req.time_range.to);

    let sql = format!(
        "SELECT \
           PagePath, \
           countIf(EventType = 'pageview') as views, \
           uniqExact(SessionId) as unique_sessions, \
           avgIf(DurationMs, DurationMs > 0) as avg_load_ms, \
           countIf(EventType = 'error') as error_count \
         FROM rum_events \
         WHERE {where_clause} AND PagePath != '' \
         GROUP BY PagePath \
         ORDER BY views DESC \
         LIMIT {}",
        req.limit.min(100),
    );

    let rows = state
        .ch
        .query(&sql)
        .fetch_all::<RumPageRow>()
        .await
        .map_err(|e| {
            tracing::error!("RUM pages query failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
        })?;

    Ok(Json(serde_json::json!({ "pages": rows })))
}

/// POST /api/v1/rum/errors — error groups
pub async fn errors(
    State(state): State<AppState>,
    Json(req): Json<RumQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let mut filters = req.filters.clone();
    filters.push(Filter {
        field: "EventType".to_string(),
        op: FilterOp::Eq,
        value: serde_json::Value::String("error".to_string()),
    });
    let where_clause = build_rum_where(&filters, &req.time_range.from, &req.time_range.to);

    let sql = format!(
        "SELECT \
           ErrorMessage, \
           ErrorType, \
           count() as count, \
           uniqExact(SessionId) as affected_sessions, \
           toString(max(Timestamp)) as last_seen, \
           any(ErrorStack) as sample_stack \
         FROM rum_events \
         WHERE {where_clause} \
         GROUP BY ErrorMessage, ErrorType \
         ORDER BY count DESC \
         LIMIT {}",
        req.limit.min(100),
    );

    let rows = state
        .ch
        .query(&sql)
        .fetch_all::<RumErrorRow>()
        .await
        .map_err(|e| {
            tracing::error!("RUM errors query failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
        })?;

    Ok(Json(serde_json::json!({ "errors": rows })))
}

/// POST /api/v1/rum/sessions — session list
pub async fn sessions(
    State(state): State<AppState>,
    Json(req): Json<RumQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let where_clause = build_rum_where(&req.filters, &req.time_range.from, &req.time_range.to);

    let sql = format!(
        "SELECT \
           SessionId, \
           any(UserId) as UserId, \
           concat(any(BrowserName), ' ', any(BrowserVersion)) as browser, \
           countIf(EventType = 'pageview') as page_count, \
           countIf(EventType = 'error') as error_count, \
           (max(Timestamp) - min(Timestamp)) / 1e9 as duration_s, \
           toString(min(Timestamp)) as first_seen \
         FROM rum_events \
         WHERE {where_clause} AND SessionId != '' \
         GROUP BY SessionId \
         ORDER BY first_seen DESC \
         LIMIT {} OFFSET {}",
        req.limit.min(100),
        req.offset,
    );

    let rows = state
        .ch
        .query(&sql)
        .fetch_all::<RumSessionRow>()
        .await
        .map_err(|e| {
            tracing::error!("RUM sessions query failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
        })?;

    Ok(Json(serde_json::json!({ "sessions": rows })))
}

/// GET /api/v1/rum/session/{id} — session timeline
pub async fn session_detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let escaped_id = id.replace('\'', "\\'");
    let sql = format!(
        "SELECT Timestamp, AppName, AppVersion, Environment, SessionId, UserId, \
         PageUrl, PagePath, ViewName, Referrer, BrowserName, BrowserVersion, \
         OsName, OsVersion, DeviceType, ScreenWidth, ScreenHeight, \
         EventType, EventName, VitalName, VitalValue, VitalRating, \
         ErrorMessage, ErrorStack, ErrorType, InteractionTarget, InteractionType, \
         DurationMs, TraceId, SpanId, Attributes \
         FROM rum_events \
         WHERE SessionId = '{escaped_id}' \
         ORDER BY Timestamp ASC \
         LIMIT 1000"
    );

    let rows = state
        .ch
        .query(&sql)
        .fetch_all::<RumRecord>()
        .await
        .map_err(|e| {
            tracing::error!("RUM session detail query failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
        })?;

    let json_rows: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| serde_json::to_value(r).unwrap_or(serde_json::Value::Null))
        .collect();

    Ok(Json(serde_json::json!({ "events": json_rows })))
}
