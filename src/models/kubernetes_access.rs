use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct KubernetesAccessEvent {
    pub id: String,
    pub tenant_id: String,
    pub cluster_id: String,
    pub gateway_id: String,
    pub session_id: String,
    pub actor_user_id: String,
    pub actor_name: String,
    pub actor_type: String,
    pub kube_username: String,
    pub kube_groups: String,
    pub source_kind: String,
    pub client_reported: String,
    pub observed_network: String,
    pub http_method: String,
    pub verb: String,
    pub api_group: String,
    pub api_version: String,
    pub resource: String,
    pub subresource: String,
    pub namespace: String,
    pub name: String,
    pub request_query: String,
    pub user_agent: String,
    pub status_code: u16,
    pub duration_ms: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub result_summary: String,
    pub result_truncated: u8,
    pub redaction_count: u32,
    pub recording_state: String,
    pub created_at: String,
}

#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct KubernetesSessionChunk {
    pub id: String,
    pub tenant_id: String,
    pub session_id: String,
    pub event_id: String,
    pub gateway_id: String,
    pub sequence: u64,
    pub stream: String,
    pub encoding: String,
    pub provenance: String,
    pub recording_state: String,
    pub offset_ms: u64,
    pub data: String,
    pub byte_count: u64,
    pub redaction_count: u32,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct KubernetesSessionChunkView {
    pub id: String,
    pub session_id: String,
    pub event_id: String,
    pub gateway_id: String,
    pub sequence: u64,
    pub stream: String,
    pub encoding: String,
    pub provenance: serde_json::Value,
    pub recording_state: String,
    pub offset_ms: u64,
    pub data: String,
    pub byte_count: u64,
    pub redaction_count: u32,
    pub created_at: String,
}

impl From<KubernetesSessionChunk> for KubernetesSessionChunkView {
    fn from(row: KubernetesSessionChunk) -> Self {
        let created_at =
            chrono::NaiveDateTime::parse_from_str(&row.created_at, "%Y-%m-%d %H:%M:%S")
                .map(|value| value.and_utc().format("%Y-%m-%dT%H:%M:%SZ").to_string())
                .unwrap_or(row.created_at);
        Self {
            id: row.id,
            session_id: row.session_id,
            event_id: row.event_id,
            gateway_id: row.gateway_id,
            sequence: row.sequence,
            stream: row.stream,
            encoding: row.encoding,
            provenance: serde_json::from_str(&row.provenance)
                .unwrap_or_else(|_| serde_json::json!({})),
            recording_state: row.recording_state,
            offset_ms: row.offset_ms,
            data: row.data,
            byte_count: row.byte_count,
            redaction_count: row.redaction_count,
            created_at,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct KubernetesAccessFilter {
    pub tenant_id: String,
    pub from: String,
    pub to: String,
    pub actor: String,
    pub cluster: String,
    pub namespace: String,
    pub verb: String,
    pub resource: String,
    pub status_min: u16,
    pub status_max: u16,
    pub source_kind: String,
    pub recording_state: String,
    pub q: String,
    pub limit: u64,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct KubernetesSessionSummary {
    pub session_id: String,
    pub chunk_count: u64,
    pub total_bytes: u64,
    pub redaction_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct KubernetesAccessEventView {
    pub id: String,
    pub cluster_id: String,
    pub gateway_id: String,
    pub session_id: String,
    pub actor_user_id: String,
    pub actor_name: String,
    pub actor_type: String,
    pub kube_username: String,
    pub kube_groups: serde_json::Value,
    pub source_kind: String,
    pub client_reported: serde_json::Value,
    pub observed_network: serde_json::Value,
    pub http_method: String,
    pub verb: String,
    pub api_group: String,
    pub api_version: String,
    pub resource: String,
    pub subresource: String,
    pub namespace: String,
    pub name: String,
    pub request_query: serde_json::Value,
    pub user_agent: String,
    pub status_code: u16,
    pub duration_ms: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub result_summary: serde_json::Value,
    pub result_truncated: bool,
    pub redaction_count: u32,
    pub recording_state: String,
    pub created_at: String,
}

impl From<KubernetesAccessEvent> for KubernetesAccessEventView {
    fn from(row: KubernetesAccessEvent) -> Self {
        fn json(raw: &str, fallback: serde_json::Value) -> serde_json::Value {
            serde_json::from_str(raw).unwrap_or(fallback)
        }

        fn utc_timestamp(raw: &str) -> String {
            chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")
                .map(|value| value.and_utc().format("%Y-%m-%dT%H:%M:%SZ").to_string())
                .unwrap_or_else(|_| raw.to_string())
        }

        Self {
            id: row.id,
            cluster_id: row.cluster_id,
            gateway_id: row.gateway_id,
            session_id: row.session_id,
            actor_user_id: row.actor_user_id,
            actor_name: row.actor_name,
            actor_type: row.actor_type,
            kube_username: row.kube_username,
            kube_groups: json(&row.kube_groups, serde_json::json!([])),
            source_kind: row.source_kind,
            client_reported: json(&row.client_reported, serde_json::json!({})),
            observed_network: json(&row.observed_network, serde_json::json!({})),
            http_method: row.http_method,
            verb: row.verb,
            api_group: row.api_group,
            api_version: row.api_version,
            resource: row.resource,
            subresource: row.subresource,
            namespace: row.namespace,
            name: row.name,
            request_query: json(&row.request_query, serde_json::json!({})),
            user_agent: row.user_agent,
            status_code: row.status_code,
            duration_ms: row.duration_ms,
            request_bytes: row.request_bytes,
            response_bytes: row.response_bytes,
            result_summary: json(&row.result_summary, serde_json::Value::Null),
            result_truncated: row.result_truncated != 0,
            redaction_count: row.redaction_count,
            recording_state: row.recording_state,
            created_at: utc_timestamp(&row.created_at),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_view_marks_stored_timestamps_as_utc() {
        let event = KubernetesAccessEvent {
            id: "kar-1".to_string(),
            tenant_id: "tenant-a".to_string(),
            cluster_id: "prod".to_string(),
            gateway_id: "gateway-1".to_string(),
            session_id: String::new(),
            actor_user_id: "user-1".to_string(),
            actor_name: "operator".to_string(),
            actor_type: "user".to_string(),
            kube_username: "rush:user:operator".to_string(),
            kube_groups: "[]".to_string(),
            source_kind: "gateway".to_string(),
            client_reported: "{}".to_string(),
            observed_network: "{}".to_string(),
            http_method: "GET".to_string(),
            verb: "list".to_string(),
            api_group: String::new(),
            api_version: "v1".to_string(),
            resource: "pods".to_string(),
            subresource: String::new(),
            namespace: "default".to_string(),
            name: String::new(),
            request_query: "{}".to_string(),
            user_agent: "kubectl".to_string(),
            status_code: 200,
            duration_ms: 10,
            request_bytes: 0,
            response_bytes: 100,
            result_summary: "null".to_string(),
            result_truncated: 0,
            redaction_count: 0,
            recording_state: "complete".to_string(),
            created_at: "2026-08-21 12:34:56".to_string(),
        };

        assert_eq!(
            KubernetesAccessEventView::from(event).created_at,
            "2026-08-21T12:34:56Z"
        );
    }
}
