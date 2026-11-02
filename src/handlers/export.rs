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
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::{AppState, TenantContext};

pub const DEFAULT_EXPORT_MAX_ROWS: u64 = 1000;
pub const EXPORT_MAX_ROWS_CEILING: u64 = 1_000_000;
pub const DEFAULT_SYNC_EXPORT_MAX_ROWS: u64 = 50_000;
pub const DEFAULT_EXPORT_MAX_BYTES: u64 = 256 * 1024 * 1024;
const STREAM_SUFFIX_RESERVE: u64 = 256;
const DEFAULT_EXPORT_JOB_TTL_SECS: u64 = 60 * 60;

/// Export output format, parsed from the request body `{ "format": "csv" | "json" }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, Serialize)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
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

use super::export_store::{JobRecord, JobStore};
use futures_util::TryStreamExt;
use object_store::ObjectStoreExt;

const WORKER_LEASE_SECS: i64 = 120;
const ARTIFACT_CHUNK_BYTES: usize = 5 * 1024 * 1024;

struct LocalJob {
    tenant_id: String,
    cancelled: AtomicBool,
    progress: Mutex<ExportProgress>,
}

/// Durable metadata is authoritative. The local map contains only running
/// workers' progress and cancellation signals, never discoverability state.
pub struct ExportJobs {
    store: JobStore,
    ttl: Duration,
    instance: String,
    jobs: DashMap<String, Arc<LocalJob>>,
    audit: std::sync::OnceLock<Arc<crate::audit::AuditLogger>>,
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
        Ok(Self::with_store(
            JobStore::from_env(&root)?,
            Duration::from_secs(ttl_secs),
        ))
    }

    #[cfg(test)]
    fn with_root(root: PathBuf, ttl: Duration) -> anyhow::Result<Self> {
        Ok(Self::with_store(JobStore::local(&root)?, ttl))
    }

    fn with_store(store: JobStore, ttl: Duration) -> Self {
        Self {
            store,
            ttl,
            instance: uuid::Uuid::new_v4().to_string(),
            jobs: DashMap::new(),
            audit: std::sync::OnceLock::new(),
        }
    }

    pub async fn create(
        &self,
        tenant_id: &str,
        signal: &str,
        format: ExportFormat,
        filename: String,
        requested_rows: u64,
    ) -> anyhow::Result<ExportJobStatus> {
        let now = chrono::Utc::now();
        let record = JobRecord {
            id: uuid::Uuid::new_v4().to_string(),
            tenant_id: tenant_id.into(),
            signal: signal.into(),
            format,
            filename,
            requested_rows,
            created_at: now,
            expires_at: now + chrono::Duration::from_std(self.ttl)?,
            state: ExportJobState::Queued,
            rows_written: 0,
            bytes_written: 0,
            chunks: 0,
            truncated: false,
            error: None,
            owner: self.instance.clone(),
            lease_until: now + chrono::Duration::seconds(WORKER_LEASE_SECS),
            revision: 0,
        };
        if !self.store.save(&record, None).await? {
            anyhow::bail!("export job ID collision");
        }
        self.jobs.insert(
            record.id.clone(),
            Arc::new(LocalJob {
                tenant_id: tenant_id.into(),
                cancelled: AtomicBool::new(false),
                progress: Mutex::new(ExportProgress::default()),
            }),
        );
        Ok(status_snapshot(&record))
    }

    async fn update(
        &self,
        tenant: &str,
        id: &str,
        mut change: impl FnMut(&mut JobRecord) -> bool,
    ) -> anyhow::Result<Option<JobRecord>> {
        for _ in 0..8 {
            let Some((mut record, version)) = self.store.load(tenant, id).await? else {
                return Ok(None);
            };
            if !change(&mut record) {
                return Ok(None);
            }
            record.revision = record
                .revision
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("export revision overflow"))?;
            match self.store.save(&record, Some(version)).await {
                Ok(true) => return Ok(Some(record)),
                Ok(false) => {}
                Err(error) => {
                    // A store can commit a conditional write and lose its
                    // response. Confirm it before reporting a failed mutation.
                    if let Some((current, _)) = self.store.load(tenant, id).await?
                        && serde_json::to_vec(&current)? == serde_json::to_vec(&record)?
                    {
                        return Ok(Some(current));
                    }
                    return Err(error);
                }
            }
        }
        anyhow::bail!("export metadata update contended")
    }

    async fn audit_transition(
        &self,
        record: &JobRecord,
        action: &'static str,
        outcome: &'static str,
    ) {
        if let Some(audit) = self.audit.get() {
            audit
                .log(
                    crate::audit::AuditEvent::new(action, "system")
                        .actor_name("query-api")
                        .tenant(&record.tenant_id)
                        .resource("export_job", &record.id)
                        .outcome(outcome)
                        .changes(serde_json::json!({ "state": record.state }).to_string()),
                )
                .await;
        }
    }

    pub fn spawn_janitor(self: &Arc<Self>, audit: Arc<crate::audit::AuditLogger>) {
        let _ = self.audit.set(audit);
        let jobs = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let mut keys = jobs.store.keys();
                while let Some(meta) = keys.next().await {
                    let meta = match meta {
                        Ok(meta) => meta,
                        Err(error) => {
                            tracing::warn!(%error, "export cleanup listing failed");
                            break;
                        }
                    };
                    if !meta.location.as_ref().ends_with("/job.json") {
                        continue;
                    }
                    match jobs.store.record_at(&meta.location).await {
                        Ok(record) => {
                            if let Err(error) = jobs.reconcile(record).await {
                                tracing::warn!(%error, "export cleanup deferred");
                            }
                        }
                        Err(error) => {
                            tracing::warn!(%error, "export metadata could not be inspected")
                        }
                    }
                }
            }
        });
    }

    async fn reconcile(&self, mut record: JobRecord) -> anyhow::Result<JobRecord> {
        let now = chrono::Utc::now();
        let expired = record.expires_at <= now;
        let lost = matches!(
            record.state,
            ExportJobState::Queued | ExportJobState::Running
        ) && record.lease_until <= now;
        if (expired && record.state != ExportJobState::Expired) || lost {
            if let Some(updated) = self
                .update(&record.tenant_id, &record.id, |current| {
                    if current.expires_at <= now && current.state != ExportJobState::Expired {
                        current.state = ExportJobState::Expired;
                        current.error = None;
                        true
                    } else if matches!(
                        current.state,
                        ExportJobState::Queued | ExportJobState::Running
                    ) && current.lease_until <= now
                    {
                        current.state = ExportJobState::Failed;
                        current.error =
                            Some("export worker lease expired; retry the export".into());
                        true
                    } else {
                        false
                    }
                })
                .await?
            {
                let action = if updated.state == ExportJobState::Expired {
                    "export_job.expire"
                } else {
                    "export_job.fail"
                };
                self.audit_transition(
                    &updated,
                    action,
                    if updated.state == ExportJobState::Failed {
                        "failure"
                    } else {
                        "success"
                    },
                )
                .await;
                record = updated;
            } else if let Some((current, _)) =
                self.store.load(&record.tenant_id, &record.id).await?
            {
                record = current;
            }
        }
        if matches!(
            record.state,
            ExportJobState::Failed | ExportJobState::Cancelled | ExportJobState::Expired
        ) {
            if let Some(local) = self.jobs.get(&record.id) {
                local.cancelled.store(true, Ordering::Release);
            }
            self.store.delete_artifacts(&record).await?;
            // Keep a tombstone beyond the maximum workload duration so a
            // fenced old worker cannot leave an undiscoverable artifact.
            if record.expires_at + chrono::Duration::minutes(15) <= now {
                self.store.delete_metadata(&record).await?;
            }
            self.jobs.remove(&record.id);
        }
        Ok(record)
    }

    pub fn progress_callback(self: &Arc<Self>, id: &str) -> ProgressCallback {
        let jobs = self.clone();
        let id = id.to_string();
        Arc::new(move |progress| {
            if let Some(job) = jobs.jobs.get(&id) {
                *job.progress.lock().expect("export progress poisoned") = progress;
            }
        })
    }

    pub async fn mark_failed(&self, id: &str, message: &'static str) {
        let tenant = self.jobs.get(id).map(|job| job.tenant_id.clone());
        let Some(tenant) = tenant else {
            return;
        };
        match self
            .update(&tenant, id, |record| {
                if record.owner == self.instance
                    && matches!(
                        record.state,
                        ExportJobState::Queued | ExportJobState::Running
                    )
                {
                    record.state = ExportJobState::Failed;
                    record.error = Some(message.into());
                    true
                } else {
                    false
                }
            })
            .await
        {
            Ok(Some(record)) => {
                self.audit_transition(&record, "export_job.fail", "failure")
                    .await;
                let _ = self.reconcile(record).await;
            }
            Ok(None) => {
                self.jobs.remove(id);
            }
            Err(error) => tracing::error!(%error, "export failure state could not be persisted"),
        }
    }

    pub fn is_cancelled(&self, id: &str) -> bool {
        self.jobs
            .get(id)
            .is_none_or(|job| job.cancelled.load(Ordering::Acquire))
    }

    pub async fn cancel(&self, tenant: &str, id: &str) -> anyhow::Result<Option<ExportJobStatus>> {
        let Some((record, _)) = self.store.load(tenant, id).await? else {
            return Ok(None);
        };
        if record.expires_at <= chrono::Utc::now() {
            return Ok(None);
        }
        let updated = self
            .update(tenant, id, |record| {
                if matches!(
                    record.state,
                    ExportJobState::Expired | ExportJobState::Cancelled
                ) {
                    return false;
                }
                record.state = ExportJobState::Cancelled;
                record.error = None;
                true
            })
            .await?;
        if let Some(local) = self.jobs.get(id) {
            local.cancelled.store(true, Ordering::Release);
        }
        if let Some(record) = updated {
            let status = status_snapshot(&record);
            if let Err(error) = self.reconcile(record).await {
                // The cancellation already committed. Return it so the
                // caller records its audit event; cleanup retries separately.
                tracing::warn!(%error, "cancelled export cleanup deferred");
            }
            Ok(Some(status))
        } else {
            self.status(tenant, id).await
        }
    }

    pub async fn status(&self, tenant: &str, id: &str) -> anyhow::Result<Option<ExportJobStatus>> {
        let Some((record, _)) = self.store.load(tenant, id).await? else {
            return Ok(None);
        };
        let record = self.reconcile(record).await?;
        if record.expires_at <= chrono::Utc::now() {
            return Ok(None);
        }
        Ok(Some(status_snapshot(&record)))
    }

    async fn renew(
        &self,
        id: &str,
        tenant: &str,
        expected: ExportJobState,
    ) -> anyhow::Result<JobRecord> {
        let now = chrono::Utc::now();
        let progress = self
            .jobs
            .get(id)
            .map(|local| *local.progress.lock().expect("export progress poisoned"))
            .unwrap_or_default();
        let record = self
            .update(tenant, id, |record| {
                if record.owner != self.instance
                    || record.lease_until <= now
                    || record.expires_at <= now
                    || record.state != expected
                {
                    return false;
                }
                record.state = ExportJobState::Running;
                record.lease_until = now + chrono::Duration::seconds(WORKER_LEASE_SECS);
                record.rows_written = progress.rows;
                record.bytes_written = progress.bytes;
                record.truncated = progress.truncated;
                true
            })
            .await?
            .ok_or_else(|| anyhow::anyhow!("export lease was lost or cancelled"))?;
        self.audit_transition(
            &record,
            if expected == ExportJobState::Queued {
                "export_job.start"
            } else {
                "export_job.renew"
            },
            "success",
        )
        .await;
        Ok(record)
    }

    pub async fn write_response(
        self: &Arc<Self>,
        id: &str,
        response: Response,
    ) -> anyhow::Result<()> {
        let tenant = self
            .jobs
            .get(id)
            .map(|job| job.tenant_id.clone())
            .ok_or_else(|| anyhow::anyhow!("export worker not owned by this process"))?;
        let record = self.renew(id, &tenant, ExportJobState::Queued).await?;
        let jobs = self.clone();
        let id = id.to_string();
        let (mut sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let work = jobs.upload(&record, response);
            let result = tokio::select! { result = work => result, _ = sender.closed() => Err(anyhow::anyhow!("export worker cancelled")) };
            let result = match result {
                Ok((chunks, bytes)) => jobs.publish(&record, chunks, bytes).await,
                Err(error) => Err(error),
            };
            if result.is_err() {
                jobs.mark_failed(&id, "export could not be completed").await;
            }
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| anyhow::anyhow!("export worker stopped"))?
    }

    async fn upload(&self, record: &JobRecord, response: Response) -> anyhow::Result<(u64, u64)> {
        let prefix = self.store.artifact_key(record)?;
        let mut body = response.into_body().into_data_stream();
        let mut heartbeat = tokio::time::interval(Duration::from_secs(5));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;
        let mut buffer = Vec::with_capacity(ARTIFACT_CHUNK_BYTES);
        let mut chunks = 0;
        let mut bytes = 0_u64;
        loop {
            if self.is_cancelled(&record.id) {
                anyhow::bail!("export worker cancelled");
            }
            let chunk = tokio::select! {
                _ = heartbeat.tick() => { self.renew(&record.id, &record.tenant_id, ExportJobState::Running).await?; continue; },
                chunk = body.next() => chunk,
            };
            let Some(chunk) = chunk else {
                break;
            };
            let mut chunk = chunk?;
            bytes = bytes
                .checked_add(chunk.len() as u64)
                .filter(|bytes| *bytes <= max_export_bytes())
                .ok_or_else(|| anyhow::anyhow!("export exceeds byte limit"))?;
            while !chunk.is_empty() {
                let count = (ARTIFACT_CHUNK_BYTES - buffer.len()).min(chunk.len());
                buffer.extend_from_slice(&chunk.split_to(count));
                if buffer.len() == ARTIFACT_CHUNK_BYTES {
                    self.store
                        .objects
                        .put(
                            &prefix.clone().join(format!("{chunks:08}.part")),
                            Bytes::from(std::mem::take(&mut buffer)).into(),
                        )
                        .await?;
                    chunks += 1;
                }
            }
        }
        if !buffer.is_empty() || chunks == 0 {
            self.store
                .objects
                .put(
                    &prefix.clone().join(format!("{chunks:08}.part")),
                    Bytes::from(buffer).into(),
                )
                .await?;
            chunks += 1;
        }
        Ok((chunks, bytes))
    }

    async fn publish(&self, record: &JobRecord, chunks: u64, bytes: u64) -> anyhow::Result<()> {
        let progress = self
            .jobs
            .get(&record.id)
            .map(|local| *local.progress.lock().expect("export progress poisoned"))
            .unwrap_or_default();
        let now = chrono::Utc::now();
        let updated = self
            .update(&record.tenant_id, &record.id, |current| {
                if current.owner != self.instance
                    || current.lease_until <= now
                    || current.expires_at <= now
                    || current.state != ExportJobState::Running
                {
                    return false;
                }
                current.state = ExportJobState::Completed;
                current.chunks = chunks;
                current.bytes_written = bytes;
                current.rows_written = progress.rows;
                current.truncated = progress.truncated;
                true
            })
            .await?;
        let Some(updated) = updated else {
            anyhow::bail!("export publication was fenced");
        };
        self.audit_transition(&updated, "export_job.complete", "success")
            .await;
        self.jobs.remove(&record.id);
        Ok(())
    }

    pub async fn download(&self, tenant: &str, id: &str) -> Result<Response, StatusCode> {
        let (record, _) = self
            .store
            .load(tenant, id)
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
            .ok_or(StatusCode::NOT_FOUND)?;
        let record = self
            .reconcile(record)
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        if record.expires_at <= chrono::Utc::now() {
            return Err(StatusCode::NOT_FOUND);
        }
        if record.state != ExportJobState::Completed {
            return Err(StatusCode::CONFLICT);
        }
        let store = self.store.objects.clone();
        let prefix = self
            .store
            .artifact_key(&record)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let stream = futures_util::stream::iter(0..record.chunks)
            .then(move |index| {
                let store = store.clone();
                let path = prefix.clone().join(format!("{index:08}.part"));
                async move { store.get(&path).await.map(|result| result.into_stream()) }
            })
            .try_flatten();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static(match record.format {
                ExportFormat::Csv => "text/csv; charset=utf-8",
                ExportFormat::Json => "application/json; charset=utf-8",
            }),
        );
        headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("private, no-store"),
        );
        if let Ok(value) =
            HeaderValue::from_str(&format!("attachment; filename=\"{}\"", record.filename))
        {
            headers.insert(header::CONTENT_DISPOSITION, value);
        }
        Ok((StatusCode::OK, headers, Body::from_stream(stream)).into_response())
    }
}

fn status_snapshot(record: &JobRecord) -> ExportJobStatus {
    ExportJobStatus {
        id: record.id.clone(),
        signal: record.signal.clone(),
        format: match record.format {
            ExportFormat::Csv => "csv",
            ExportFormat::Json => "json",
        }
        .into(),
        state: record.state,
        requested_rows: record.requested_rows,
        rows_written: record.rows_written,
        bytes_written: record.bytes_written,
        truncated: record.truncated,
        created_at: record.created_at.to_rfc3339(),
        expires_at: record.expires_at.to_rfc3339(),
        status_url: format!("/api/v1/exports/{}", record.id),
        download_url: (record.state == ExportJobState::Completed)
            .then(|| format!("/api/v1/exports/{}/download", record.id)),
        error: record.error.clone(),
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
        Ok(Some(status)) => Json(status).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            crate::api_error::ApiError::internal("read export status", error).into_response()
        }
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
    let status = match state.export_jobs.cancel(&tenant.tenant_id, &id).await {
        Ok(Some(status)) => status,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            return crate::api_error::ApiError::internal("cancel export", error).into_response();
        }
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
    T: clickhouse::RowOwned + clickhouse::RowRead + Send + 'static,
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
    T: clickhouse::RowOwned + clickhouse::RowRead + Send + 'static,
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
    T: clickhouse::RowOwned + clickhouse::RowRead + serde::Serialize + Send + 'static,
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
    async fn completed_exports_survive_new_instances_and_stream_across_replicas() {
        let store =
            JobStore::shared(Arc::new(object_store::memory::InMemory::new()), "exports").unwrap();
        let a = Arc::new(ExportJobs::with_store(
            store.clone(),
            Duration::from_secs(60),
        ));
        let b = Arc::new(ExportJobs::with_store(
            store.clone(),
            Duration::from_secs(60),
        ));
        let status = a
            .create("tenant-a", "logs", ExportFormat::Csv, "logs.csv".into(), 10)
            .await
            .unwrap();
        assert_eq!(
            b.status("tenant-a", &status.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            ExportJobState::Queued
        );
        assert!(b.status("tenant-b", &status.id).await.unwrap().is_none());
        assert!(b.status("tenant-a", "../job.json").await.unwrap().is_none());
        assert!(
            b.write_response(&status.id, Response::new(Body::from("not owned")))
                .await
                .is_err()
        );
        let expected = vec![b'x'; ARTIFACT_CHUNK_BYTES * 2 + 123];
        a.write_response(&status.id, Response::new(Body::from(expected.clone())))
            .await
            .unwrap();
        assert!(a.jobs.is_empty());
        drop(a);
        let restarted = Arc::new(ExportJobs::with_store(store, Duration::from_secs(60)));
        let current = restarted
            .status("tenant-a", &status.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.state, ExportJobState::Completed);
        assert_eq!(current.bytes_written, expected.len() as u64);
        let response = b.download("tenant-a", &status.id).await.unwrap();
        let actual = axum::body::to_bytes(response.into_body(), expected.len() + 1)
            .await
            .unwrap();
        assert_eq!(actual.as_ref(), &expected);
        assert_eq!(
            restarted
                .download("tenant-b", &status.id)
                .await
                .unwrap_err(),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn persistent_local_metadata_uses_atomic_revision_checks() {
        let root =
            std::env::temp_dir().join(format!("rush-export-restart-{}", uuid::Uuid::new_v4()));
        let a = Arc::new(ExportJobs::with_root(root.clone(), Duration::from_secs(60)).unwrap());
        let status = a
            .create(
                "tenant-a",
                "spans",
                ExportFormat::Json,
                "spans.json".into(),
                1,
            )
            .await
            .unwrap();
        let (mut first, version) = a.store.load("tenant-a", &status.id).await.unwrap().unwrap();
        let mut second = first.clone();
        first.state = ExportJobState::Running;
        first.revision += 1;
        second.state = ExportJobState::Cancelled;
        second.revision += 1;
        let (left, right) = tokio::join!(
            a.store.save(&first, Some(version.clone())),
            a.store.save(&second, Some(version))
        );
        assert_ne!(
            left.unwrap(),
            right.unwrap(),
            "only one update from a shared revision can win"
        );
        let status = a
            .create(
                "tenant-a",
                "logs",
                ExportFormat::Json,
                "logs.json".into(),
                1,
            )
            .await
            .unwrap();
        a.write_response(&status.id, Response::new(Body::from("{}")))
            .await
            .unwrap();
        drop(a);
        let restarted =
            Arc::new(ExportJobs::with_root(root.clone(), Duration::from_secs(60)).unwrap());
        assert_eq!(
            restarted
                .status("tenant-a", &status.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            ExportJobState::Completed
        );
        let response = restarted.download("tenant-a", &status.id).await.unwrap();
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 10)
                .await
                .unwrap(),
            "{}"
        );
        drop(restarted);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn shared_cas_fences_duplicate_workers_and_expired_leases() {
        let store =
            JobStore::shared(Arc::new(object_store::memory::InMemory::new()), "exports").unwrap();
        let a = Arc::new(ExportJobs::with_store(
            store.clone(),
            Duration::from_secs(60),
        ));
        let b = Arc::new(ExportJobs::with_store(store, Duration::from_secs(60)));
        let status = a
            .create(
                "tenant-a",
                "logs",
                ExportFormat::Json,
                "logs.json".into(),
                1,
            )
            .await
            .unwrap();
        let (first, second) = tokio::join!(
            a.renew(&status.id, "tenant-a", ExportJobState::Queued),
            a.renew(&status.id, "tenant-a", ExportJobState::Queued)
        );
        assert_ne!(first.is_ok(), second.is_ok());
        let (mut old, version) = a.store.load("tenant-a", &status.id).await.unwrap().unwrap();
        old.lease_until = chrono::Utc::now() - chrono::Duration::seconds(1);
        old.revision += 1;
        assert!(a.store.save(&old, Some(version)).await.unwrap());
        let status = b.status("tenant-a", &status.id).await.unwrap().unwrap();
        assert_eq!(status.state, ExportJobState::Failed);
        assert!(status.error.unwrap().contains("lease expired"));
        assert!(a.publish(&old, 1, 2).await.is_err());
        assert_eq!(
            b.status("tenant-a", &old.id).await.unwrap().unwrap().state,
            ExportJobState::Failed
        );
    }

    struct BodyDrop(Arc<AtomicBool>);
    impl Drop for BodyDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn pending_export_body(dropped: Arc<AtomicBool>) -> Body {
        let marker = BodyDrop(dropped);
        Body::from_stream(futures_util::stream::once(async move {
            let _marker = marker;
            std::future::pending::<Result<Bytes, std::io::Error>>().await
        }))
    }

    #[tokio::test]
    async fn another_replica_can_cancel_a_stalled_worker() {
        let store =
            JobStore::shared(Arc::new(object_store::memory::InMemory::new()), "exports").unwrap();
        let a = Arc::new(ExportJobs::with_store(
            store.clone(),
            Duration::from_secs(60),
        ));
        let b = Arc::new(ExportJobs::with_store(store, Duration::from_secs(60)));
        let status = a
            .create(
                "tenant-a",
                "logs",
                ExportFormat::Json,
                "logs.json".into(),
                1,
            )
            .await
            .unwrap();
        let id = status.id.clone();
        let creator = a.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let observed = dropped.clone();
        let worker = tokio::spawn(async move {
            creator
                .write_response(&id, Response::new(pending_export_body(observed)))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while b
                .status("tenant-a", &status.id)
                .await
                .unwrap()
                .unwrap()
                .state
                != ExportJobState::Running
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        b.cancel("tenant-a", &status.id).await.unwrap().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(8), worker)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(
            b.status("tenant-a", &status.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            ExportJobState::Cancelled
        );
    }

    #[tokio::test]
    async fn cancelling_the_waiter_stops_upload_and_records_failure() {
        let store =
            JobStore::shared(Arc::new(object_store::memory::InMemory::new()), "exports").unwrap();
        let jobs = Arc::new(ExportJobs::with_store(store, Duration::from_secs(60)));
        let status = jobs
            .create(
                "tenant-a",
                "logs",
                ExportFormat::Json,
                "logs.json".into(),
                1,
            )
            .await
            .unwrap();
        let id = status.id.clone();
        let creator = jobs.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let observed = dropped.clone();
        let waiter = tokio::spawn(async move {
            creator
                .write_response(&id, Response::new(pending_export_body(observed)))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while jobs
                .status("tenant-a", &status.id)
                .await
                .unwrap()
                .unwrap()
                .state
                != ExportJobState::Running
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        waiter.abort();
        let _ = waiter.await;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let state = jobs
                    .status("tenant-a", &status.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .state;
                if state == ExportJobState::Failed && dropped.load(Ordering::SeqCst) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn corrupt_metadata_is_an_error_and_expired_jobs_remain_inaccessible() {
        let store =
            JobStore::shared(Arc::new(object_store::memory::InMemory::new()), "exports").unwrap();
        let jobs = Arc::new(ExportJobs::with_store(store, Duration::from_secs(60)));
        let status = jobs
            .create(
                "tenant-a",
                "logs",
                ExportFormat::Json,
                "logs.json".into(),
                1,
            )
            .await
            .unwrap();
        let (mut record, version) = jobs
            .store
            .load("tenant-a", &status.id)
            .await
            .unwrap()
            .unwrap();
        record.expires_at = chrono::Utc::now() - chrono::Duration::seconds(1);
        record.revision += 1;
        assert!(jobs.store.save(&record, Some(version)).await.unwrap());
        assert!(jobs.status("tenant-a", &status.id).await.unwrap().is_none());
        assert_eq!(
            jobs.download("tenant-a", &status.id).await.unwrap_err(),
            StatusCode::NOT_FOUND
        );
        let mut keys = jobs.store.keys();
        while let Some(Ok(meta)) = keys.next().await {
            if meta.location.as_ref().ends_with("/job.json") {
                jobs.store
                    .objects
                    .put(&meta.location, Bytes::from_static(b"corrupt").into())
                    .await
                    .unwrap();
            }
        }
        assert!(jobs.status("tenant-a", &status.id).await.is_err());
    }

    #[tokio::test]
    async fn async_objects_are_tenant_scoped_and_streamed_from_disk() {
        let root = std::env::temp_dir().join(format!("rush-export-test-{}", uuid::Uuid::new_v4()));
        let jobs = Arc::new(ExportJobs::with_root(root.clone(), Duration::from_secs(60)).unwrap());
        let status = jobs
            .create(
                "tenant-a",
                "logs",
                ExportFormat::Json,
                "logs.json".to_string(),
                10,
            )
            .await
            .unwrap();
        assert!(jobs.status("tenant-b", &status.id).await.unwrap().is_none());
        jobs.write_response(&status.id, Response::new(Body::from("{\"rows\":[]}")))
            .await
            .unwrap();
        assert_eq!(
            jobs.status("tenant-a", &status.id)
                .await
                .unwrap()
                .unwrap()
                .state,
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
        let jobs = Arc::new(ExportJobs::with_root(root.clone(), Duration::from_secs(60)).unwrap());
        let status = jobs
            .create(
                "tenant-a",
                "spans",
                ExportFormat::Csv,
                "spans.csv".to_string(),
                10,
            )
            .await
            .unwrap();
        jobs.cancel("tenant-a", &status.id).await.unwrap();
        assert!(
            jobs.write_response(&status.id, Response::new(Body::from("row\n")))
                .await
                .is_err()
        );
        assert_eq!(
            jobs.status("tenant-a", &status.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            ExportJobState::Cancelled
        );
        let (record, _) = jobs
            .store
            .load("tenant-a", &status.id)
            .await
            .unwrap()
            .unwrap();
        let prefix = jobs.store.artifact_key(&record).unwrap();
        assert!(
            jobs.store
                .objects
                .list(Some(&prefix))
                .next()
                .await
                .is_none()
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
