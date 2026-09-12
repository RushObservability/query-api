//! Built-in, tenant-scoped CPU profiling. No integration or license gate.
use crate::{
    AppState, TenantContext,
    ch_writer::{SpoolBatch, WriteError},
    ingest_limits::IngestLimits,
    models::profile::{ProfileRow, wire::*},
};
use axum::{
    Extension, Json,
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

type Error = (StatusCode, String);
const MAX_DEPTH: usize = 128;
const MAX_STACKS: usize = 2000;
fn invalid(message: &str) -> Error {
    (StatusCode::BAD_REQUEST, message.into())
}
fn entry<T>(items: &[T], index: i32) -> Result<&T, Error> {
    usize::try_from(index)
        .ok()
        .and_then(|i| items.get(i))
        .ok_or_else(|| invalid("invalid profile dictionary reference"))
}
fn string(d: &Dictionary, index: i32) -> Result<&str, Error> {
    let s = entry(&d.string_table, index)?.as_str();
    if s.len() > 2048 {
        return Err(invalid("profile dictionary string exceeds 2048 bytes"));
    }
    Ok(s)
}

fn frames(d: &Dictionary, index: i32) -> Result<Vec<String>, Error> {
    let stack = entry(&d.stack_table, index)?;
    if stack.location_indices.is_empty() || stack.location_indices.len() > MAX_DEPTH {
        return Err(invalid("profile stacks must contain 1 to 128 frames"));
    }
    let mut out = Vec::new();
    for index in stack.location_indices.iter().rev() {
        let loc = entry(&d.location_table, *index)?;
        if loc.lines.is_empty() {
            let module = if loc.mapping_index == 0
                && d.mapping_table
                    .first()
                    .is_none_or(|m| m.filename_strindex == 0 && m.memory_start == 0)
            {
                "unknown"
            } else {
                string(
                    d,
                    entry(&d.mapping_table, loc.mapping_index)?.filename_strindex,
                )?
            };
            out.push(format!("{module}!0x{:x} [unresolved]", loc.address));
        } else {
            for line in loc.lines.iter().rev() {
                if out.len() >= MAX_DEPTH {
                    return Err(invalid("profile stacks exceed 128 expanded frames"));
                }
                let f = entry(&d.function_table, line.function_index)?;
                let name = string(d, f.name_strindex)?;
                let name = if name.is_empty() {
                    string(d, f.system_name_strindex)?
                } else {
                    name
                };
                let file = string(d, f.filename_strindex)?;
                out.push(if file.is_empty() {
                    name.to_owned()
                } else {
                    format!("{name} ({file}:{})", line.line)
                });
            }
        }
        if out.len() > MAX_DEPTH {
            return Err(invalid("profile stacks exceed 128 expanded frames"));
        }
    }
    if out.iter().any(|f| f.len() > 4096) {
        return Err(invalid("profile frame exceeds 4096 bytes"));
    }
    Ok(out)
}

fn cpu_multiplier(d: &Dictionary, p: &Profile) -> Result<u64, Error> {
    let ty = p
        .sample_type
        .as_ref()
        .ok_or_else(|| invalid("profile sample_type is required"))?;
    match (string(d, ty.type_strindex)?, string(d, ty.unit_strindex)?) {
        ("cpu", "nanoseconds") => Ok(1),
        ("samples" | "cpu", "count") => {
            let period = p
                .period_type
                .as_ref()
                .ok_or_else(|| invalid("CPU counts require a CPU nanosecond period"))?;
            if string(d, period.type_strindex)? != "cpu"
                || string(d, period.unit_strindex)? != "nanoseconds"
                || p.period <= 0
            {
                return Err(invalid(
                    "CPU counts require a positive CPU nanosecond period",
                ));
            }
            Ok(p.period as u64)
        }
        _ => Err(invalid(
            "only CPU nanoseconds or CPU samples with a nanosecond period are supported",
        )),
    }
}

pub(crate) fn decode_rows(
    req: &ExportProfilesServiceRequest,
    tenant: &str,
    limits: &IngestLimits,
) -> Result<Vec<ProfileRow>, Error> {
    if req.resource_profiles.is_empty() {
        return Ok(Vec::new());
    }
    let d = req.dictionary.as_ref().ok_or_else(|| {
        invalid("profiles dictionary is required; expected OTLP profiles v1.10.0")
    })?;
    // Some pprof translators omit the zero-value dictionary sentinels.
    // Indices are still direct offsets; validate every referenced entry instead.
    if d.string_table.is_empty() {
        return Err(invalid("profile string dictionary is empty"));
    }
    let mut rows = Vec::new();
    let mut expanded_bytes = 0usize;
    use prost::Message;
    let request_hash = Sha256::digest(req.encode_to_vec());
    for rp in &req.resource_profiles {
        let attrs = rp
            .resource
            .as_ref()
            .map(|r| r.attributes.as_slice())
            .unwrap_or_default();
        let attr = |key: &str| -> String {
            use opentelemetry_proto::tonic::common::v1::any_value::Value;
            attrs
                .iter()
                .find(|a| a.key == key)
                .and_then(|a| a.value.as_ref())
                .and_then(|v| match &v.value {
                    Some(Value::StringValue(s)) => Some(s.clone()),
                    _ => None,
                })
                .unwrap_or_default()
        };
        let service = attr("service.name");
        let version = attr("service.version");
        let pod = attr("k8s.pod.name");
        if service.trim().is_empty() {
            return Err(invalid("service.name is required for profiles"));
        }
        for (name, value) in [
            ("service.name", &service),
            ("service.version", &version),
            ("k8s.pod.name", &pod),
        ] {
            limits.check_label("profiles", name, value)?;
        }
        for scope in &rp.scope_profiles {
            for p in &scope.profiles {
                let multiplier = cpu_multiplier(d, p)?;
                let profile_type =
                    if string(d, p.sample_type.as_ref().unwrap().unit_strindex)? == "nanoseconds" {
                        "cpu"
                    } else {
                        "sampled_cpu"
                    };
                let end = p
                    .time_unix_nano
                    .checked_add(p.duration_nano)
                    .filter(|e| *e <= i64::MAX as u64)
                    .ok_or_else(|| invalid("invalid profile time range"))?;
                if p.time_unix_nano == 0 {
                    return Err(invalid("profile timestamp is required"));
                }
                if !p.original_payload.is_empty() || !p.original_payload_format.is_empty() {
                    return Err(invalid(
                        "original profile payloads are not supported; send decoded CPU samples",
                    ));
                }
                if !p.profile_id.is_empty()
                    && (p.profile_id.len() != 16 || p.profile_id.iter().all(|b| *b == 0))
                {
                    return Err(invalid("invalid profile ID"));
                }
                // Include the request dictionary/resource context for ID-less exports.
                // Replays of identical protobuf requests retain the same identity.
                let id = if p.profile_id.is_empty() {
                    let mut hash = Sha256::new();
                    hash.update(request_hash);
                    hash.update(p.encode_to_vec());
                    hex::encode(hash.finalize())
                } else {
                    hex::encode(&p.profile_id)
                };
                for (sample_index, s) in p.samples.iter().enumerate() {
                    // Also rejects the incompatible v1.9 values field, which
                    // occupied the current attribute_indices field number.
                    for index in &s.attribute_indices {
                        entry(&d.attribute_table, *index)?;
                    }
                    let n = s.values.len().max(s.timestamps_unix_nano.len());
                    if n == 0
                        || (!s.values.is_empty()
                            && !s.timestamps_unix_nano.is_empty()
                            && s.values.len() != s.timestamps_unix_nano.len())
                    {
                        return Err(invalid(
                            "sample values and timestamps must be nonempty and have matching lengths when both are set",
                        ));
                    }
                    limits.check_count(
                        "profiles",
                        rows.len().saturating_add(n),
                        limits.max_samples.min(limits.max_entities),
                    )?;
                    let frames = frames(d, s.stack_index)?;
                    let row_bytes = frames.iter().map(String::len).sum::<usize>()
                        + service.len()
                        + version.len()
                        + pod.len()
                        + 256;
                    expanded_bytes = expanded_bytes.saturating_add(row_bytes.saturating_mul(n));
                    limits.check_decompressed("profiles", expanded_bytes)?;
                    let (trace_id, span_id) = if s.link_index == 0 {
                        (String::new(), String::new())
                    } else {
                        let link = entry(&d.link_table, s.link_index)?;
                        if link.trace_id.len() != 16
                            || link.span_id.len() != 8
                            || link.trace_id.iter().all(|b| *b == 0)
                            || link.span_id.iter().all(|b| *b == 0)
                        {
                            return Err(invalid("invalid profile trace/span link"));
                        }
                        (hex::encode(&link.trace_id), hex::encode(&link.span_id))
                    };
                    for value_index in 0..n {
                        let time = s
                            .timestamps_unix_nano
                            .get(value_index)
                            .copied()
                            .unwrap_or(p.time_unix_nano);
                        if time < p.time_unix_nano
                            || (p.duration_nano > 0 && time >= end)
                            || (p.duration_nano == 0 && time != p.time_unix_nano)
                        {
                            return Err(invalid(
                                "sample timestamp is outside the profile interval",
                            ));
                        }
                        let value = s.values.get(value_index).copied().unwrap_or(1);
                        let cpu = u64::try_from(value)
                            .ok()
                            .and_then(|v| v.checked_mul(multiplier))
                            .ok_or_else(|| invalid("CPU sample value is negative or overflows"))?;
                        rows.push(ProfileRow {
                            tenant_id: tenant.into(),
                            timestamp: time as i64,
                            service_name: service.clone(),
                            service_version: version.clone(),
                            pod: pod.clone(),
                            profile_type: profile_type.into(),
                            profile_id: id.clone(),
                            sample_index: sample_index as u64,
                            value_index: value_index as u64,
                            duration_nano: p.duration_nano,
                            cpu_nanoseconds: cpu,
                            frames: frames.clone(),
                            trace_id: trace_id.clone(),
                            span_id: span_id.clone(),
                        });
                    }
                }
            }
        }
    }
    Ok(rows)
}

pub async fn ingest(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, Error> {
    let req: ExportProfilesServiceRequest =
        super::otlp::decode_proto(&state.ingest_limits, &headers, body.clone()).await?;
    let permit = state.ingest_limits.acquire_decode("profiles").await?;
    let limits = state.ingest_limits.clone();
    let tenant_id = tenant.tenant_id.clone();
    let rows = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        decode_rows(&req, &tenant_id, &limits)
    })
    .await
    .map_err(|e| crate::api_error::internal_legacy("profiles.decode", e))??;
    let count = rows.len();
    super::ingest_gate::write_gated(&state, &tenant.tenant_id, SpoolBatch::Profiles(rows))
        .await
        .map_err(|e| match e {
            WriteError::Backpressure => (
                StatusCode::TOO_MANY_REQUESTS,
                "profile ingest buffer is full; retry later".into(),
            ),
            WriteError::Fatal(e) => crate::api_error::internal_legacy("profiles.write", e),
        })?;
    if state
        .config_db
        .tenant_signal_enabled(&tenant.tenant_id, "profiles")
        .await
    {
        state.usage_accumulator.record(
            &tenant.tenant_id,
            "profiles",
            count as u64,
            body.len() as u64,
        );
    }
    // An empty protobuf ExportProfilesServiceResponse represents full success.
    Ok((
        [("content-type", "application/x-protobuf")],
        Vec::<u8>::new(),
    ))
}

#[derive(Debug, Deserialize)]
pub struct ProfileQuery {
    /// RFC3339 on the wire; inclusive/exclusive Unix milliseconds internally.
    #[serde(deserialize_with = "profile_time")]
    pub from: i64,
    #[serde(deserialize_with = "profile_time")]
    pub to: i64,
    #[serde(default)]
    pub service: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub pod: String,
    #[serde(default = "default_profile_type")]
    pub profile_type: String,
    #[serde(default)]
    pub trace_id: String,
    #[serde(default)]
    pub span_id: String,
}
fn default_profile_type() -> String {
    "cpu".into()
}
fn profile_time<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    let value = String::deserialize(deserializer)?;
    chrono::DateTime::parse_from_rfc3339(&value)
        .map(|t| t.timestamp_millis())
        .map_err(serde::de::Error::custom)
}
impl ProfileQuery {
    fn validate(&self) -> Result<(), Error> {
        if !matches!(self.profile_type.as_str(), "cpu" | "sampled_cpu") {
            return Err(invalid("unsupported CPU profile type"));
        }
        if self.from < 0
            || self.to <= self.from
            || self.to > i64::MAX / 1_000_000
            || self.to - self.from > 31 * 86400 * 1000
        {
            return Err(invalid("select a valid profile range of at most 31 days"));
        }
        if [&self.service, &self.version, &self.pod]
            .iter()
            .any(|s| s.len() > 4096)
        {
            return Err(invalid("profile filter is too long"));
        }
        for (id, len) in [(&self.trace_id, 32), (&self.span_id, 16)] {
            if !id.is_empty() && (id.len() != len || !id.bytes().all(|b| b.is_ascii_hexdigit())) {
                return Err(invalid("invalid trace/span filter"));
            }
        }
        if !self.span_id.is_empty() && self.trace_id.is_empty() {
            return Err(invalid("span filter requires a trace ID"));
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize, clickhouse::Row)]
pub struct ProfileSeries {
    service: String,
    version: String,
    pod: String,
    profile_type: String,
}
pub async fn series(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Query(q): Query<ProfileQuery>,
) -> Result<impl IntoResponse, Error> {
    q.validate()?;
    let rows = crate::tenant_query(&state.ch, "SELECT DISTINCT service_name AS service, service_version AS version, pod, profile_type FROM profile_samples WHERE tenant_id = ? AND timestamp >= fromUnixTimestamp64Milli(?) AND timestamp < fromUnixTimestamp64Milli(?) ORDER BY service, version, pod, profile_type LIMIT 2001", &tenant.tenant_id).bind(&tenant.tenant_id).bind(q.from).bind(q.to).fetch_all::<ProfileSeries>().await.map_err(|e| crate::api_error::internal_legacy("profiles.series", e))?;
    let truncated = rows.len() > 2000;
    Ok(Json(
        serde_json::json!({"series": rows.into_iter().take(2000).collect::<Vec<_>>(), "truncated": truncated}),
    ))
}

#[derive(Deserialize, Serialize, clickhouse::Row)]
pub struct StackResult {
    frames: Vec<String>,
    cpu_seconds: f64,
}
pub async fn query(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Query(q): Query<ProfileQuery>,
) -> Result<impl IntoResponse, Error> {
    q.validate()?;
    if q.service.is_empty() {
        return Err(invalid("select a service to query profiles"));
    }
    let sql = "SELECT frames, sum(toFloat64(cpu_nanoseconds)) / 1e9 AS cpu_seconds FROM profile_samples FINAL WHERE tenant_id = ? AND timestamp >= fromUnixTimestamp64Milli(?) AND timestamp < fromUnixTimestamp64Milli(?) AND service_name = ? AND profile_type = ? AND (? = '' OR service_version = ?) AND (? = '' OR pod = ?) AND (? = '' OR trace_id = ?) AND (? = '' OR span_id = ?) GROUP BY frames ORDER BY cpu_seconds DESC LIMIT 2001";
    let stacks = crate::tenant_query(&state.ch, sql, &tenant.tenant_id)
        .bind(&tenant.tenant_id)
        .bind(q.from)
        .bind(q.to)
        .bind(&q.service)
        .bind(&q.profile_type)
        .bind(&q.version)
        .bind(&q.version)
        .bind(&q.pod)
        .bind(&q.pod)
        .bind(q.trace_id.to_lowercase())
        .bind(q.trace_id.to_lowercase())
        .bind(q.span_id.to_lowercase())
        .bind(q.span_id.to_lowercase())
        .fetch_all::<StackResult>()
        .await
        .map_err(|e| crate::api_error::internal_legacy("profiles.query", e))?;
    if stacks.len() > MAX_STACKS {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            "profile contains more than 2000 stacks; narrow the time range or pod filter".into(),
        ));
    }
    let total: f64 = stacks.iter().map(|s| s.cpu_seconds).sum();
    Ok(Json(
        serde_json::json!({"stacks": stacks, "total_cpu_seconds": total, "from": q.from, "to": q.to, "attribution": if q.trace_id.is_empty() { "service" } else { "linked_samples" }}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    #[ignore = "requires the pinned collector sending real CPU profiles to localhost:18082"]
    async fn accepts_real_collector_export() -> anyhow::Result<()> {
        use axum::routing::post;
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let router = axum::Router::new().route(
            "/v1development/profiles",
            post(move |headers: HeaderMap, body: Bytes| {
                let tx = tx.clone();
                async move {
                    let limits = IngestLimits::from_env(std::sync::Arc::new(
                        crate::self_metrics::SelfMetrics::new(),
                    ))
                    .unwrap();
                    let req: ExportProfilesServiceRequest =
                        super::super::otlp::decode_proto(&limits, &headers, body).await?;
                    let rows = decode_rows(&req, "test", &limits).map_err(|e| {
                        eprintln!("collector decode error: {}", e.1);
                        e
                    })?;
                    if !rows.is_empty() {
                        let _ = tx.send(rows).await;
                    }
                    Ok::<_, Error>((
                        [("content-type", "application/x-protobuf")],
                        Vec::<u8>::new(),
                    ))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:18082").await?;
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        eprintln!("Collector compatibility receiver ready on :18082");
        let result = tokio::time::timeout(std::time::Duration::from_secs(60), rx.recv()).await;
        server.abort();
        let rows = result?.ok_or_else(|| anyhow::anyhow!("collector sent no samples"))?;
        assert!(rows.iter().all(|r| r.service_name == "profile-test"));
        assert!(rows.iter().any(|r| r.cpu_nanoseconds > 0));
        assert!(
            rows.iter()
                .any(|r| r.frames.iter().any(|f| f.contains("main.work")))
        );
        eprintln!("Decoded {} real CPU rows from the collector", rows.len());
        Ok(())
    }
    #[test]
    fn official_v110_sample_field_numbers() {
        use prost::Message;
        // stack_index=1, attribute_indices=[0], link_index=2, values=[25].
        let sample = Sample::decode(&[8, 1, 18, 1, 0, 24, 2, 34, 1, 25][..]).unwrap();
        assert_eq!(sample.values, [25]);
        assert_eq!(sample.attribute_indices, [0]);
        assert_eq!(sample.link_index, 2);
    }
    #[test]
    fn cpu_count_profiles_remain_a_separate_measurement() {
        let mut f = fixture();
        f.dictionary
            .as_mut()
            .unwrap()
            .string_table
            .extend(["samples".into(), "count".into()]);
        let p = &mut f.resource_profiles[0].scope_profiles[0].profiles[0];
        p.sample_type = Some(ValueType {
            type_strindex: 5,
            unit_strindex: 6,
        });
        p.period_type = Some(ValueType {
            type_strindex: 1,
            unit_strindex: 2,
        });
        p.period = 10;
        let rows = decode_rows(&f, "t", &limits()).unwrap();
        assert_eq!(rows[0].cpu_nanoseconds, 250);
        assert_eq!(rows[0].profile_type, "sampled_cpu");
    }
    #[test]
    fn timestamps_only_samples_and_trace_links_are_preserved() {
        let mut f = fixture();
        f.dictionary.as_mut().unwrap().link_table = vec![
            Link::default(),
            Link {
                trace_id: vec![1; 16],
                span_id: vec![2; 8],
            },
        ];
        let s = &mut f.resource_profiles[0].scope_profiles[0].profiles[0].samples[0];
        s.values.clear();
        s.timestamps_unix_nano = vec![110, 120];
        s.link_index = 1;
        let rows = decode_rows(&f, "t", &limits()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp, 110);
        assert_eq!(rows[1].cpu_nanoseconds, 1);
        assert_eq!(rows[0].trace_id, "01".repeat(16));
        assert_eq!(rows[0].span_id, "02".repeat(8));
    }
    #[test]
    fn query_dates_match_the_shared_governor_format() {
        let q: ProfileQuery = serde_json::from_value(serde_json::json!({"from":"2026-09-12T10:00:00Z", "to":"2026-09-12T11:00:00Z", "service":"demo"})).unwrap();
        assert_eq!(q.to - q.from, 3_600_000);
        assert!(q.validate().is_ok());
    }
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue, any_value::Value},
        resource::v1::Resource,
    };
    fn fixture() -> ExportProfilesServiceRequest {
        ExportProfilesServiceRequest {
            dictionary: Some(Dictionary {
                string_table: vec![
                    "".into(),
                    "cpu".into(),
                    "nanoseconds".into(),
                    "main".into(),
                    "work".into(),
                ],
                function_table: vec![
                    Function::default(),
                    Function {
                        name_strindex: 3,
                        ..Default::default()
                    },
                    Function {
                        name_strindex: 4,
                        ..Default::default()
                    },
                ],
                location_table: vec![
                    Location::default(),
                    Location {
                        lines: vec![Line {
                            function_index: 1,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    Location {
                        lines: vec![Line {
                            function_index: 2,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                stack_table: vec![
                    Stack::default(),
                    Stack {
                        location_indices: vec![2, 1],
                    },
                ],
                ..Default::default()
            }),
            resource_profiles: vec![ResourceProfiles {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("demo".into())),
                        }),
                    }],
                    ..Default::default()
                }),
                scope_profiles: vec![ScopeProfiles {
                    profiles: vec![Profile {
                        sample_type: Some(ValueType {
                            type_strindex: 1,
                            unit_strindex: 2,
                        }),
                        time_unix_nano: 100,
                        duration_nano: 100,
                        samples: vec![Sample {
                            stack_index: 1,
                            values: vec![25],
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                }],
            }],
        }
    }
    fn limits() -> IngestLimits {
        IngestLimits::for_test(std::sync::Arc::new(crate::self_metrics::SelfMetrics::new()))
    }
    #[test]
    fn decodes_root_first_with_authenticated_tenant_and_stable_id() {
        let r = decode_rows(&fixture(), "tenant-a", &limits()).unwrap();
        assert_eq!(r[0].frames, ["main", "work"]);
        assert_eq!(r[0].cpu_nanoseconds, 25);
        assert_eq!(r[0].tenant_id, "tenant-a");
        assert_eq!(
            r[0].profile_id,
            decode_rows(&fixture(), "tenant-a", &limits()).unwrap()[0].profile_id
        );
    }
    #[test]
    fn accepts_pprof_dictionaries_without_empty_sentinels() {
        let mut f = fixture();
        let d = f.dictionary.as_mut().unwrap();
        d.string_table[0] = "binary".into();
        d.mapping_table = vec![Mapping {
            memory_start: 4096,
            filename_strindex: 0,
        }];
        d.location_table[0] = Location {
            address: 4097,
            ..Default::default()
        };
        d.stack_table[1].location_indices = vec![0];
        assert_eq!(
            decode_rows(&f, "t", &limits()).unwrap()[0].frames,
            ["binary!0x1001 [unresolved]"]
        );
        f.dictionary.as_mut().unwrap().string_table.clear();
        assert!(decode_rows(&f, "t", &limits()).is_err());
    }
    #[test]
    fn rejects_bad_references_and_unsupported_types() {
        let mut f = fixture();
        f.dictionary.as_mut().unwrap().stack_table[1].location_indices = vec![-1];
        assert!(decode_rows(&f, "t", &limits()).is_err());
        let mut f = fixture();
        f.dictionary.as_mut().unwrap().string_table[1] = "heap".into();
        assert!(decode_rows(&f, "t", &limits()).is_err());
    }
    #[test]
    fn rejects_value_time_and_expansion_errors() {
        for sample in [
            Sample {
                stack_index: 1,
                values: vec![-1],
                ..Default::default()
            },
            Sample {
                stack_index: 1,
                values: vec![1, 2],
                timestamps_unix_nano: vec![100],
                ..Default::default()
            },
            Sample {
                stack_index: 1,
                timestamps_unix_nano: vec![201],
                ..Default::default()
            },
            Sample {
                stack_index: 1,
                values: vec![1; 101],
                ..Default::default()
            },
        ] {
            let mut f = fixture();
            f.resource_profiles[0].scope_profiles[0].profiles[0].samples = vec![sample];
            assert!(decode_rows(&f, "t", &limits()).is_err());
        }
    }
    #[test]
    fn protobuf_round_trip() {
        use prost::Message;
        let f = fixture();
        assert_eq!(
            ExportProfilesServiceRequest::decode(f.encode_to_vec().as_slice()).unwrap(),
            f
        );
    }
    #[test]
    fn rejects_invalid_query_bounds_and_unscoped_span() {
        let mut q = ProfileQuery {
            from: 100,
            to: 99,
            service: "demo".into(),
            version: String::new(),
            pod: String::new(),
            profile_type: "cpu".into(),
            trace_id: String::new(),
            span_id: String::new(),
        };
        assert!(q.validate().is_err());
        q.to = 200;
        assert!(q.validate().is_ok());
        q.span_id = "a".repeat(16);
        assert!(q.validate().is_err());
    }
}
