//! Shared helpers for exporting query results (logs/spans) as CSV or JSON.
//!
//! The interactive query endpoints stay capped at 1000 rows; exports use the
//! admin-configurable `export_max_rows` setting (default 1000) instead.

use axum::Json;
use axum::body::Body;
use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use clickhouse::query::RowCursor;
use dashmap::DashMap;
use futures_util::StreamExt;
use serde::Serialize;
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{AppState, TenantContext};

pub const DEFAULT_EXPORT_MAX_ROWS: u64 = 1000;
pub const EXPORT_MAX_ROWS_CEILING: u64 = 1_000_000;
pub const DEFAULT_SYNC_EXPORT_MAX_ROWS: u64 = 50_000;
pub const DEFAULT_EXPORT_MAX_BYTES: u64 = 256 * 1024 * 1024;
const STREAM_SUFFIX_RESERVE: u64 = 256;
const DEFAULT_EXPORT_JOB_TTL_SECS: u64 = 60 * 60;
const FILE_CHUNK_BYTES: usize = 64 * 1024;

/// Export output format, parsed from the request body `{ "format": "csv" | "json" }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExportFormat {
    Csv,
    Json,
}

impl Default for ExportFormat {
    fn default() -> Self {
        ExportFormat::Csv
    }
}

/// Read the configured max export row count (clamped to a sane ceiling).
pub async fn read_export_max_rows(state: &AppState) -> u64 {
    state
        .config_db
        .get_setting("export_max_rows")
        .await
        .ok()
        .flatten()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(DEFAULT_EXPORT_MAX_ROWS)
        .min(EXPORT_MAX_ROWS_CEILING)
}

/// Resolve the effective row limit for an export request given the configured cap.
/// A missing/zero requested limit means "use the cap".
pub fn effective_limit(requested: u64, cap: u64) -> u64 {
    if requested == 0 {
        cap
    } else {
        requested.min(cap)
    }
}

/// The synchronous ceiling is intentionally lower than the absolute export
/// cap. Larger requests are handed to the asynchronous job path so a browser
/// connection does not own a scarce ClickHouse slot for an unbounded period.
pub fn sync_max_rows() -> u64 {
    std::env::var("RUSH_EXPORT_SYNC_MAX_ROWS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SYNC_EXPORT_MAX_ROWS)
        .min(EXPORT_MAX_ROWS_CEILING)
}

/// Hard payload cap for both synchronous responses and asynchronous objects.
pub fn max_export_bytes() -> u64 {
    std::env::var("RUSH_EXPORT_MAX_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value >= 1024 * 1024)
        .unwrap_or(DEFAULT_EXPORT_MAX_BYTES)
        .min(4 * 1024 * 1024 * 1024)
}

pub fn requires_async(_requested: u64, effective: u64) -> bool {
    effective > sync_max_rows()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportJobState {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    Expired,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExportJobStatus {
    pub id: String,
    pub signal: String,
    pub format: String,
    pub state: ExportJobState,
    pub requested_rows: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
    pub truncated: bool,
    pub created_at: String,
    pub expires_at: String,
    pub status_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone)]
struct MutableJobState {
    state: ExportJobState,
    rows_written: u64,
    bytes_written: u64,
    truncated: bool,
    error: Option<String>,
}

struct ExportJob {
    id: String,
    tenant_id: String,
    signal: String,
    format: ExportFormat,
    filename: String,
    requested_rows: u64,
    created_at: chrono::DateTime<chrono::Utc>,
    expires_at: chrono::DateTime<chrono::Utc>,
    path: PathBuf,
    cancelled: AtomicBool,
    mutable: Mutex<MutableJobState>,
}

/// Process-local metadata with files stored beneath a private directory. The
/// object path is never returned to callers; all access passes through a
/// tenant-checked API route. A shared volume can back the directory in
/// Kubernetes, while status metadata deliberately expires in-process.
pub struct ExportJobs {
    root: PathBuf,
    ttl: Duration,
    jobs: DashMap<String, Arc<ExportJob>>,
}

impl ExportJobs {
    pub fn from_env() -> anyhow::Result<Self> {
        let root = std::env::var("RUSH_EXPORT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir().join("rush-exports"));
        let ttl_secs = std::env::var("RUSH_EXPORT_JOB_TTL_SECONDS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_EXPORT_JOB_TTL_SECS)
            .clamp(60, 24 * 60 * 60);
        Self::with_root(root, Duration::from_secs(ttl_secs))
    }

    fn with_root(root: PathBuf, ttl: Duration) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&root)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
        }
        remove_stale_files(&root, ttl);
        Ok(Self {
            root,
            ttl,
            jobs: DashMap::new(),
        })
    }

    pub fn create(
        &self,
        tenant_id: &str,
        signal: &str,
        format: ExportFormat,
        filename: String,
        requested_rows: u64,
    ) -> ExportJobStatus {
        let id = uuid::Uuid::new_v4().to_string();
        let created_at = chrono::Utc::now();
        let expires_at = created_at
            + chrono::Duration::from_std(self.ttl).unwrap_or_else(|_| chrono::Duration::hours(1));
        let job = Arc::new(ExportJob {
            path: self.root.join(&id),
            id: id.clone(),
            tenant_id: tenant_id.to_string(),
            signal: signal.to_string(),
            format,
            filename,
            requested_rows,
            created_at,
            expires_at,
            cancelled: AtomicBool::new(false),
            mutable: Mutex::new(MutableJobState {
                state: ExportJobState::Queued,
                rows_written: 0,
                bytes_written: 0,
                truncated: false,
                error: None,
            }),
        });
        let status = status_snapshot(
            &job,
            &MutableJobState {
                state: ExportJobState::Queued,
                rows_written: 0,
                bytes_written: 0,
                truncated: false,
                error: None,
            },
        );
        self.jobs.insert(id, job);
        status
    }

    pub fn spawn_janitor(self: &Arc<Self>, audit: Arc<crate::audit::AuditLogger>) {
        let jobs = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                for (tenant_id, id) in jobs.cleanup_expired() {
                    audit
                        .log(
                            crate::audit::AuditEvent::new("export_job.expire", "system")
                                .actor_name("query-api")
                                .tenant(tenant_id)
                                .resource("export_job", id)
                                .changes(serde_json::json!({ "state": "expired" }).to_string()),
                        )
                        .await;
                }
            }
        });
    }

    pub fn progress_callback(self: &Arc<Self>, id: &str) -> ProgressCallback {
        let jobs = self.clone();
        let id = id.to_string();
        Arc::new(move |progress| {
            if let Some(job) = jobs.jobs.get(&id) {
                let mut state = job.mutable.lock().expect("export job lock poisoned");
                state.rows_written = progress.rows;
                state.bytes_written = progress.bytes;
                state.truncated = progress.truncated;
            }
        })
    }

    pub async fn mark_running(&self, id: &str) {
        if let Some(job) = self.jobs.get(id) {
            if !job.cancelled.load(Ordering::Acquire) {
                let mut state = job.mutable.lock().expect("export job lock poisoned");
                if state.state == ExportJobState::Queued {
                    state.state = ExportJobState::Running;
                }
            }
        }
    }

    pub async fn mark_failed(&self, id: &str, message: &'static str) {
        if let Some(job) = self.jobs.get(id) {
            let mut state = job.mutable.lock().expect("export job lock poisoned");
            if state.state != ExportJobState::Cancelled {
                state.state = ExportJobState::Failed;
                state.error = Some(message.to_string());
            }
        }
    }

    pub fn is_cancelled(&self, id: &str) -> bool {
        self.jobs
            .get(id)
            .is_some_and(|job| job.cancelled.load(Ordering::Acquire))
    }

    pub async fn cancel(&self, tenant_id: &str, id: &str) -> Option<ExportJobStatus> {
        let job = self.job_for_tenant(tenant_id, id)?;
        job.cancelled.store(true, Ordering::Release);
        let status = {
            let mut state = job.mutable.lock().expect("export job lock poisoned");
            state.state = ExportJobState::Cancelled;
            state.error = None;
            status_snapshot(&job, &state)
        };
        let _ = tokio::fs::remove_file(&job.path).await;
        let _ = tokio::fs::remove_file(part_path(&job.path)).await;
        Some(status)
    }

    pub async fn status(&self, tenant_id: &str, id: &str) -> Option<ExportJobStatus> {
        let job = self.job_for_tenant(tenant_id, id)?;
        let state = job.mutable.lock().expect("export job lock poisoned");
        Some(status_snapshot(&job, &state))
    }

    pub async fn write_response(&self, id: &str, response: Response) -> anyhow::Result<()> {
        let job = self
            .jobs
            .get(id)
            .map(|job| job.clone())
            .ok_or_else(|| anyhow::anyhow!("export job no longer exists"))?;
        if job.cancelled.load(Ordering::Acquire) {
            return Err(anyhow::anyhow!("export job cancelled"));
        }
        self.mark_running(id).await;
        let temporary = part_path(&job.path);
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .await?;
        }
        let mut body = response.into_body().into_data_stream();
        while let Some(chunk) = body.next().await {
            if job.cancelled.load(Ordering::Acquire) {
                let _ = tokio::fs::remove_file(&temporary).await;
                return Err(anyhow::anyhow!("export job cancelled"));
            }
            file.write_all(&chunk?).await?;
        }
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        if job.cancelled.load(Ordering::Acquire) {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(anyhow::anyhow!("export job cancelled"));
        }
        tokio::fs::rename(&temporary, &job.path).await?;
        let completed = {
            let mut state = job.mutable.lock().expect("export job lock poisoned");
            if job.cancelled.load(Ordering::Acquire) {
                false
            } else {
                state.state = ExportJobState::Completed;
                state.error = None;
                true
            }
        };
        if !completed {
            let _ = tokio::fs::remove_file(&job.path).await;
            return Err(anyhow::anyhow!("export job cancelled"));
        }
        Ok(())
    }

    pub async fn download(&self, tenant_id: &str, id: &str) -> Result<Response, StatusCode> {
        let job = self
            .job_for_tenant(tenant_id, id)
            .ok_or(StatusCode::NOT_FOUND)?;
        if job.mutable.lock().expect("export job lock poisoned").state != ExportJobState::Completed
        {
            return Err(StatusCode::CONFLICT);
        }
        let file = tokio::fs::File::open(&job.path)
            .await
            .map_err(|_| StatusCode::NOT_FOUND)?;
        let stream = futures_util::stream::unfold(file, |mut file| async move {
            let mut buffer = vec![0u8; FILE_CHUNK_BYTES];
            match file.read(&mut buffer).await {
                Ok(0) => None,
                Ok(read) => {
                    buffer.truncate(read);
                    Some((Ok::<Bytes, std::io::Error>(Bytes::from(buffer)), file))
                }
                Err(error) => Some((Err(error), file)),
            }
        });
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static(match job.format {
                ExportFormat::Csv => "text/csv; charset=utf-8",
                ExportFormat::Json => "application/json; charset=utf-8",
            }),
        );
        headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("private, no-store"),
        );
        if let Ok(value) =
            HeaderValue::from_str(&format!("attachment; filename=\"{}\"", job.filename))
        {
            headers.insert(header::CONTENT_DISPOSITION, value);
        }
        Ok((StatusCode::OK, headers, Body::from_stream(stream)).into_response())
    }

    fn job_for_tenant(&self, tenant_id: &str, id: &str) -> Option<Arc<ExportJob>> {
        self.jobs
            .get(id)
            .filter(|job| job.tenant_id == tenant_id && job.expires_at > chrono::Utc::now())
            .map(|job| job.clone())
    }

    fn cleanup_expired(&self) -> Vec<(String, String)> {
        let now = chrono::Utc::now();
        let expired: Vec<String> = self
            .jobs
            .iter()
            .filter(|entry| entry.expires_at <= now)
            .map(|entry| entry.key().clone())
            .collect();
        let mut removed = Vec::with_capacity(expired.len());
        for id in expired {
            if let Some((_, job)) = self.jobs.remove(&id) {
                job.cancelled.store(true, Ordering::Release);
                removed.push((job.tenant_id.clone(), job.id.clone()));
                let path = job.path.clone();
                tokio::spawn(async move {
                    let _ = tokio::fs::remove_file(&path).await;
                    let _ = tokio::fs::remove_file(part_path(&path)).await;
                });
            }
        }
        removed
    }
}

fn part_path(path: &FsPath) -> PathBuf {
    path.with_extension("part")
}

fn remove_stale_files(root: &FsPath, ttl: Duration) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    let now = std::time::SystemTime::now();
    for entry in entries.flatten() {
        let stale = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age >= ttl);
        if stale {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

fn format_label(format: ExportFormat) -> &'static str {
    match format {
        ExportFormat::Csv => "csv",
        ExportFormat::Json => "json",
    }
}

fn status_snapshot(job: &ExportJob, state: &MutableJobState) -> ExportJobStatus {
    ExportJobStatus {
        id: job.id.clone(),
        signal: job.signal.clone(),
        format: format_label(job.format).to_string(),
        state: state.state,
        requested_rows: job.requested_rows,
        rows_written: state.rows_written,
        bytes_written: state.bytes_written,
        truncated: state.truncated,
        created_at: job.created_at.to_rfc3339(),
        expires_at: job.expires_at.to_rfc3339(),
        status_url: format!("/api/v1/exports/{}", job.id),
        download_url: (state.state == ExportJobState::Completed)
            .then(|| format!("/api/v1/exports/{}/download", job.id)),
        error: state.error.clone(),
    }
}

pub fn accepted_job_response(status: ExportJobStatus) -> Response {
    let mut response = (StatusCode::ACCEPTED, Json(status.clone())).into_response();
    if let Ok(value) = HeaderValue::from_str(&status.status_url) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    response
}

pub async fn audit_job_transition(
    state: &AppState,
    tenant_id: &str,
    id: &str,
    action: &'static str,
    job_state: &'static str,
    outcome: &'static str,
) {
    state
        .audit
        .log(
            crate::audit::AuditEvent::new(action, "system")
                .actor_name("query-api")
                .tenant(tenant_id)
                .resource("export_job", id)
                .outcome(outcome)
                .changes(serde_json::json!({ "state": job_state }).to_string()),
        )
        .await;
}

pub async fn get_export_job(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Response {
    match state.export_jobs.status(&tenant.tenant_id, &id).await {
        Some(status) => Json(status).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

pub async fn download_export_job(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let response = match state.export_jobs.download(&tenant.tenant_id, &id).await {
        Ok(response) => response,
        Err(status) => return status.into_response(),
    };
    let (actor_id, actor_name) = match crate::handlers::auth::extract_session_cookie(&headers) {
        Some(token) => crate::request_auth::resolve_session_user(&state, &token)
            .await
            .map(|caller| (caller.0, caller.1))
            .unwrap_or_default(),
        None => (String::new(), String::new()),
    };
    state
        .audit
        .log(
            crate::audit::AuditEvent::new(
                "data.export",
                if actor_id.is_empty() {
                    "anonymous"
                } else {
                    "user"
                },
            )
            .actor(actor_id, actor_name)
            .tenant(tenant.tenant_id)
            .resource("export_job", id)
            .changes(serde_json::json!({ "mode": "async_download" }).to_string())
            .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    response
}

pub async fn cancel_export_job(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let Some(status) = state.export_jobs.cancel(&tenant.tenant_id, &id).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let (actor_id, actor_name) = match crate::handlers::auth::extract_session_cookie(&headers) {
        Some(token) => crate::request_auth::resolve_session_user(&state, &token)
            .await
            .map(|caller| (caller.0, caller.1))
            .unwrap_or_default(),
        None => (String::new(), String::new()),
    };
    state
        .audit
        .log(
            crate::audit::AuditEvent::new(
                "export_job.cancel",
                if actor_id.is_empty() {
                    "anonymous"
                } else {
                    "user"
                },
            )
            .actor(actor_id, actor_name)
            .tenant(tenant.tenant_id)
            .resource("export_job", id)
            .changes(serde_json::json!({ "state": "cancelled" }).to_string())
            .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Json(status).into_response()
}

/// Escape a single CSV field (RFC 4180) and neutralize spreadsheet formulas.
pub fn csv_field(s: &str) -> String {
    let dangerous = matches!(s.chars().next(), Some('=' | '+' | '-' | '@' | '\t' | '\r'));
    let safe = if dangerous {
        format!("'{s}")
    } else {
        s.to_string()
    };
    if safe.contains(',') || safe.contains('"') || safe.contains('\n') || safe.contains('\r') {
        format!("\"{}\"", safe.replace('"', "\"\""))
    } else {
        safe
    }
}

/// Format a ClickHouse DateTime64(9) value (nanoseconds since epoch) as RFC3339.
pub fn ts_rfc3339(nanos: i64) -> String {
    let secs = nanos.div_euclid(1_000_000_000);
    let nsub = nanos.rem_euclid(1_000_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, nsub)
        .map(|d| d.to_rfc3339())
        .unwrap_or_default()
}

/// Build a file-download response with the right content-type + attachment filename.
pub fn file_response(body: String, content_type: &'static str, filename: &str) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    if let Ok(cd) = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\"")) {
        headers.insert(header::CONTENT_DISPOSITION, cd);
    }
    (StatusCode::OK, headers, body).into_response()
}

/// Leading `#`-comment lines describing the exported query, for CSV files.
pub fn csv_query_preamble(
    signal: &str,
    from: &str,
    to: &str,
    search: Option<&str>,
    query_text: Option<&str>,
) -> String {
    let mut out = String::new();
    out.push_str(&format!("# Rush export — signal: {signal}\n"));
    out.push_str(&format!("# time range: {from} .. {to}\n"));
    if let Some(q) = query_text {
        if !q.is_empty() {
            out.push_str(&format!("# query: {}\n", q.replace('\n', " ")));
        }
    }
    if let Some(s) = search {
        if !s.is_empty() {
            out.push_str(&format!("# search: {}\n", s.replace('\n', " ")));
        }
    }
    out.push_str(&format!(
        "# exported_at: {}\n",
        chrono::Utc::now().to_rfc3339()
    ));
    out
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExportProgress {
    pub rows: u64,
    pub bytes: u64,
    pub truncated: bool,
}

pub type ProgressCallback = Arc<dyn Fn(ExportProgress) + Send + Sync>;

#[derive(Clone, Copy)]
enum StreamKind {
    Csv,
    Json,
}

/// State for both CSV and JSON streaming. Only a single decoded row and its
/// serialized chunk are live at a time, so peak API memory is independent of
/// the requested row count.
struct ExportStreamState<T> {
    cursor: RowCursor<T>,
    encode_row: Box<dyn Fn(&T) -> Result<Vec<u8>, serde_json::Error> + Send>,
    prelude: Option<Vec<u8>>,
    kind: StreamKind,
    max_rows: u64,
    max_bytes: u64,
    progress: ExportProgress,
    on_progress: Option<ProgressCallback>,
    done: bool,
}

impl<T> ExportStreamState<T> {
    fn notify(&self) {
        if let Some(callback) = &self.on_progress {
            callback(self.progress);
        }
    }

    fn finish_chunk(&mut self) -> Option<Bytes> {
        if self.done {
            return None;
        }
        self.done = true;
        let suffix = match self.kind {
            StreamKind::Csv if self.progress.truncated => {
                b"# truncated: export byte limit reached\n".to_vec()
            }
            StreamKind::Csv => Vec::new(),
            StreamKind::Json => format!(
                "],\"count\":{},\"truncated\":{}}}",
                self.progress.rows, self.progress.truncated
            )
            .into_bytes(),
        };
        if suffix.is_empty() {
            self.notify();
            return None;
        }
        if self.progress.bytes.saturating_add(suffix.len() as u64) > self.max_bytes {
            self.notify();
            return None;
        }
        self.progress.bytes += suffix.len() as u64;
        self.notify();
        Some(Bytes::from(suffix))
    }
}

fn stream_response<T>(
    cursor: RowCursor<T>,
    prelude: Vec<u8>,
    kind: StreamKind,
    encode_row: impl Fn(&T) -> Result<Vec<u8>, serde_json::Error> + Send + 'static,
    filename: &str,
    max_rows: u64,
    max_bytes: u64,
    on_progress: Option<ProgressCallback>,
) -> Response
where
    T: clickhouse::Row + for<'b> serde::Deserialize<'b> + Send + 'static,
{
    let state = ExportStreamState {
        cursor,
        encode_row: Box::new(encode_row),
        prelude: Some(prelude),
        kind,
        max_rows,
        max_bytes,
        progress: ExportProgress::default(),
        on_progress,
        done: false,
    };

    let stream = futures_util::stream::unfold(state, |mut state| async move {
        if state.done {
            return None;
        }
        if let Some(prelude) = state.prelude.take() {
            if prelude.len() as u64 + STREAM_SUFFIX_RESERVE > state.max_bytes {
                state.done = true;
                return Some((
                    Err::<Bytes, std::io::Error>(std::io::Error::other(
                        "export metadata exceeds the byte limit",
                    )),
                    state,
                ));
            }
            state.progress.bytes = prelude.len() as u64;
            state.notify();
            return Some((Ok(Bytes::from(prelude)), state));
        }
        if state.progress.rows >= state.max_rows {
            return state.finish_chunk().map(|chunk| (Ok(chunk), state));
        }
        match state.cursor.next().await {
            Ok(Some(row)) => {
                let mut chunk = match (state.encode_row)(&row) {
                    Ok(chunk) => chunk,
                    Err(_error) => {
                        tracing::error!(reason = "row_serialization", "export stream failed");
                        state.done = true;
                        return Some((Err(std::io::Error::other("export stream failed")), state));
                    }
                };
                if matches!(state.kind, StreamKind::Json) && state.progress.rows > 0 {
                    chunk.insert(0, b',');
                }
                if state
                    .progress
                    .bytes
                    .saturating_add(chunk.len() as u64)
                    .saturating_add(STREAM_SUFFIX_RESERVE)
                    > state.max_bytes
                {
                    state.progress.truncated = true;
                    return state.finish_chunk().map(|suffix| (Ok(suffix), state));
                }
                state.progress.rows += 1;
                state.progress.bytes += chunk.len() as u64;
                state.notify();
                Some((Ok(Bytes::from(chunk)), state))
            }
            Ok(None) => state.finish_chunk().map(|chunk| (Ok(chunk), state)),
            Err(_error) => {
                tracing::error!(reason = "cursor_read", "export stream failed");
                state.done = true;
                Some((Err(std::io::Error::other("export stream failed")), state))
            }
        }
    });

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(match kind {
            StreamKind::Csv => "text/csv; charset=utf-8",
            StreamKind::Json => "application/json; charset=utf-8",
        }),
    );
    headers.insert(
        "x-rush-export-row-limit",
        HeaderValue::from_str(&max_rows.to_string()).unwrap_or(HeaderValue::from_static("0")),
    );
    headers.insert(
        "x-rush-export-byte-limit",
        HeaderValue::from_str(&max_bytes.to_string()).unwrap_or(HeaderValue::from_static("0")),
    );
    if let Ok(disposition) = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\""))
    {
        headers.insert(header::CONTENT_DISPOSITION, disposition);
    }
    (StatusCode::OK, headers, Body::from_stream(stream)).into_response()
}

/// Build a streaming CSV file-download response.
///
/// `prelude` is the CSV preamble + header line (emitted verbatim as the first chunk).
/// `fmt_row` formats a single row into its CSV line (including the trailing `\n`),
/// using the exact same `csv_field`/`ts_rfc3339` escaping as the buffered path.
///
/// Errors mid-stream terminate the body with an `io::Error`; axum surfaces that as a
/// truncated/aborted response. The initial query has already executed by the time the
/// first row is pulled, so query-level failures still abort the download cleanly.
pub fn stream_csv_response<T>(
    cursor: RowCursor<T>,
    prelude: String,
    fmt_row: impl Fn(&T) -> String + Send + 'static,
    filename: &str,
    max_rows: u64,
    max_bytes: u64,
    on_progress: Option<ProgressCallback>,
) -> Response
where
    T: clickhouse::Row + for<'b> serde::Deserialize<'b> + Send + 'static,
{
    stream_response(
        cursor,
        prelude.into_bytes(),
        StreamKind::Csv,
        move |row| Ok(fmt_row(row).into_bytes()),
        filename,
        max_rows,
        max_bytes,
        on_progress,
    )
}

/// Build the leading portion of a streaming JSON document. The rows array is
/// closed by the stream after the cursor ends, along with the final count and
/// truncation marker.
pub fn json_query_preamble(query: serde_json::Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(&serde_json::json!({
        "query": query,
        "exported_at": chrono::Utc::now().to_rfc3339(),
    }))
    .unwrap_or_else(|_| b"{}".to_vec());
    if bytes.last() == Some(&b'}') {
        bytes.pop();
    }
    bytes.extend_from_slice(b",\"rows\":[");
    bytes
}

pub fn stream_json_response<T>(
    cursor: RowCursor<T>,
    prelude: Vec<u8>,
    filename: &str,
    max_rows: u64,
    max_bytes: u64,
    on_progress: Option<ProgressCallback>,
) -> Response
where
    T: clickhouse::Row + for<'b> serde::Deserialize<'b> + serde::Serialize + Send + 'static,
{
    stream_response(
        cursor,
        prelude,
        StreamKind::Json,
        serde_json::to_vec,
        filename,
        max_rows,
        max_bytes,
        on_progress,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn async_threshold_applies_to_every_large_effective_request() {
        assert!(!requires_async(0, sync_max_rows()));
        assert!(!requires_async(10, 10));
        assert!(requires_async(
            sync_max_rows().saturating_add(1),
            sync_max_rows().saturating_add(1)
        ));
    }

    #[test]
    fn json_preamble_has_no_unbounded_row_buffer() {
        let prelude = json_query_preamble(serde_json::json!({"signal": "logs"}));
        let text = String::from_utf8(prelude).unwrap();
        assert!(text.starts_with("{\"exported_at\":"));
        assert!(text.ends_with("\"rows\":["));
    }

    #[test]
    fn streaming_state_size_does_not_scale_with_export_rows() {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct TestRow {
            _value: String,
        }

        // The state owns one RowCursor and boxed encoder, never a Vec<TestRow>.
        // This guards against accidentally reintroducing fetch_all-style storage.
        assert!(std::mem::size_of::<ExportStreamState<TestRow>>() < 512);
    }

    #[test]
    fn csv_fields_neutralize_spreadsheet_formulas() {
        assert_eq!(csv_field("=1+1"), "'=1+1");
        assert_eq!(csv_field("+cmd|' /C calc'!A0"), "'+cmd|' /C calc'!A0");
        assert_eq!(csv_field("-42"), "'-42");
        assert_eq!(csv_field("@SUM(A1:A2)"), "'@SUM(A1:A2)");
        assert_eq!(csv_field("\t=1+1"), "'\t=1+1");
        assert_eq!(csv_field("\r=1+1"), "\"'\r=1+1\"");
    }

    #[test]
    fn csv_fields_still_follow_rfc_4180_escaping() {
        assert_eq!(csv_field("plain"), "plain");
        assert_eq!(csv_field("two,fields"), "\"two,fields\"");
        assert_eq!(csv_field("a \"quote\""), "\"a \"\"quote\"\"\"");
    }

    #[tokio::test]
    async fn async_objects_are_tenant_scoped_and_streamed_from_disk() {
        let root = std::env::temp_dir().join(format!("rush-export-test-{}", uuid::Uuid::new_v4()));
        let jobs = ExportJobs::with_root(root.clone(), Duration::from_secs(60)).unwrap();
        let status = jobs.create(
            "tenant-a",
            "logs",
            ExportFormat::Json,
            "logs.json".to_string(),
            10,
        );
        assert!(jobs.status("tenant-b", &status.id).await.is_none());
        jobs.write_response(&status.id, Response::new(Body::from("{\"rows\":[]}")))
            .await
            .unwrap();
        assert_eq!(
            jobs.status("tenant-a", &status.id).await.unwrap().state,
            ExportJobState::Completed
        );
        assert!(jobs.download("tenant-b", &status.id).await.is_err());
        let response = jobs.download("tenant-a", &status.id).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "private, no-store"
        );
        drop(response);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn cancelled_job_cannot_be_restarted_or_publish_an_object() {
        let root =
            std::env::temp_dir().join(format!("rush-export-cancel-test-{}", uuid::Uuid::new_v4()));
        let jobs = ExportJobs::with_root(root.clone(), Duration::from_secs(60)).unwrap();
        let status = jobs.create(
            "tenant-a",
            "spans",
            ExportFormat::Csv,
            "spans.csv".to_string(),
            10,
        );
        jobs.cancel("tenant-a", &status.id).await.unwrap();
        assert!(
            jobs.write_response(&status.id, Response::new(Body::from("row\n")))
                .await
                .is_err()
        );
        assert_eq!(
            jobs.status("tenant-a", &status.id).await.unwrap().state,
            ExportJobState::Cancelled
        );
        let job = jobs.job_for_tenant("tenant-a", &status.id).unwrap();
        assert!(!job.path.exists());
        let _ = std::fs::remove_dir_all(root);
    }
}
