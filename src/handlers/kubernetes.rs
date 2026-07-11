use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use kube::api::DynamicObject;
use kube::discovery::ApiResource;
use kube::{Api, Client, api::ListParams};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::AppState;
use crate::handlers::users::require_auth;

// ---------------------------------------------------------------------------
// Read-only general Kubernetes resource browser.
//
// Lists common core/apps/batch/networking objects via the in-cluster kube client
// (DynamicObject + serde_json extraction, consistent with argocd.rs/fluxcd.rs).
// READ-ONLY: list/get only. Secrets expose key names + count, never values.
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct NsQuery {
    namespace: Option<String>,
}

async fn get_kube_client() -> Result<Client, (StatusCode, String)> {
    Client::try_default().await.map_err(|e| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("Kubernetes not available: {e}"),
        )
    })
}

async fn check_kubernetes_enabled(state: &AppState) -> Result<(), (StatusCode, String)> {
    let setting_enabled = state
        .config_db
        .get_setting("kubernetes_enabled")
        .await
        .ok()
        .flatten()
        .map(|v| v == "true")
        .unwrap_or(false);
    let env_enabled = std::env::var("KUBERNETES_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    if !setting_enabled && !env_enabled {
        return Err((
            StatusCode::NOT_FOUND,
            "Kubernetes integration is not enabled".to_string(),
        ));
    }
    Ok(())
}

// kind (URL key) → (group, version, Kind, plural, namespaced)
fn kind_info(kind: &str) -> Option<(&'static str, &'static str, &'static str, &'static str, bool)> {
    Some(match kind {
        "pods" => ("", "v1", "Pod", "pods", true),
        "deployments" => ("apps", "v1", "Deployment", "deployments", true),
        "statefulsets" => ("apps", "v1", "StatefulSet", "statefulsets", true),
        "daemonsets" => ("apps", "v1", "DaemonSet", "daemonsets", true),
        "jobs" => ("batch", "v1", "Job", "jobs", true),
        "cronjobs" => ("batch", "v1", "CronJob", "cronjobs", true),
        "services" => ("", "v1", "Service", "services", true),
        "ingresses" => ("networking.k8s.io", "v1", "Ingress", "ingresses", true),
        "configmaps" => ("", "v1", "ConfigMap", "configmaps", true),
        "secrets" => ("", "v1", "Secret", "secrets", true),
        "nodes" => ("", "v1", "Node", "nodes", false),
        "namespaces" => ("", "v1", "Namespace", "namespaces", false),
        "events" => ("", "v1", "Event", "events", true),
        _ => return None,
    })
}

fn api_resource(group: &str, version: &str, kind: &str, plural: &str) -> ApiResource {
    ApiResource {
        group: group.into(),
        version: version.into(),
        kind: kind.into(),
        api_version: if group.is_empty() {
            version.into()
        } else {
            format!("{group}/{version}")
        },
        plural: plural.into(),
    }
}

fn s(v: &Value, ptr: &str) -> String {
    v.pointer(ptr)
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string()
}
fn i(v: &Value, ptr: &str) -> i64 {
    v.pointer(ptr).and_then(|x| x.as_i64()).unwrap_or(0)
}

fn meta_name(o: &DynamicObject) -> String {
    o.metadata.name.clone().unwrap_or_default()
}
fn meta_ns(o: &DynamicObject) -> String {
    o.metadata.namespace.clone().unwrap_or_default()
}
fn creation_ts(o: &DynamicObject) -> String {
    o.metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_rfc3339())
        .unwrap_or_default()
}

// ── Pod health: phase + container readiness + waiting reason ──────────────
fn pod_status(d: &Value) -> (String, String, i64, bool) {
    // returns (ready "x/y", status_text, restarts, unhealthy)
    let phase = s(d, "/status/phase");
    let cs = d
        .pointer("/status/containerStatuses")
        .and_then(|v| v.as_array());
    let (mut ready, mut total, mut restarts) = (0i64, 0i64, 0i64);
    let mut waiting_reason = String::new();
    if let Some(arr) = cs {
        total = arr.len() as i64;
        for c in arr {
            if c.get("ready").and_then(|v| v.as_bool()).unwrap_or(false) {
                ready += 1;
            }
            restarts += c.get("restartCount").and_then(|v| v.as_i64()).unwrap_or(0);
            if waiting_reason.is_empty() {
                if let Some(r) = c.pointer("/state/waiting/reason").and_then(|v| v.as_str()) {
                    waiting_reason = r.to_string();
                }
            }
        }
    }
    let status_text = if !waiting_reason.is_empty() {
        waiting_reason.clone()
    } else {
        phase.clone()
    };
    let unhealthy = !waiting_reason.is_empty()
        || (phase != "Running" && phase != "Succeeded")
        || (total > 0 && ready < total && phase == "Running");
    (format!("{ready}/{total}"), status_text, restarts, unhealthy)
}

fn node_ready(d: &Value) -> bool {
    d.pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter().any(|c| {
                c.get("type").and_then(|t| t.as_str()) == Some("Ready")
                    && c.get("status").and_then(|s| s.as_str()) == Some("True")
            })
        })
        .unwrap_or(false)
}

/// Per-kind summary row: { kind, name, namespace, creation_ts, unhealthy, cols }
fn summarise(kind: &str, o: &DynamicObject) -> Value {
    let d = &o.data;
    let mut unhealthy = false;
    let cols: Value = match kind {
        "pods" => {
            let (ready, status, restarts, uh) = pod_status(d);
            unhealthy = uh;
            json!({ "ready": ready, "status": status, "restarts": restarts.to_string(), "node": s(d, "/spec/nodeName") })
        }
        "deployments" => {
            let desired = i(d, "/spec/replicas");
            let ready = i(d, "/status/readyReplicas");
            unhealthy = ready < desired;
            json!({ "ready": format!("{ready}/{desired}"), "uptodate": i(d, "/status/updatedReplicas").to_string(), "available": i(d, "/status/availableReplicas").to_string() })
        }
        "statefulsets" => {
            let desired = i(d, "/spec/replicas");
            let ready = i(d, "/status/readyReplicas");
            unhealthy = ready < desired;
            json!({ "ready": format!("{ready}/{desired}") })
        }
        "daemonsets" => {
            let desired = i(d, "/status/desiredNumberScheduled");
            let ready = i(d, "/status/numberReady");
            unhealthy = ready < desired;
            json!({ "desired": desired.to_string(), "current": i(d, "/status/currentNumberScheduled").to_string(), "ready": ready.to_string(), "available": i(d, "/status/numberAvailable").to_string() })
        }
        "jobs" => {
            let succeeded = i(d, "/status/succeeded");
            let completions = d
                .pointer("/spec/completions")
                .and_then(|v| v.as_i64())
                .unwrap_or(1);
            let failed = i(d, "/status/failed");
            unhealthy = failed > 0;
            json!({ "completions": format!("{succeeded}/{completions}"), "active": i(d, "/status/active").to_string(), "failed": failed.to_string() })
        }
        "cronjobs" => {
            json!({ "schedule": s(d, "/spec/schedule"), "suspend": d.pointer("/spec/suspend").and_then(|v| v.as_bool()).unwrap_or(false).to_string(), "active": d.pointer("/status/active").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0).to_string(), "last_schedule": s(d, "/status/lastScheduleTime") })
        }
        "services" => {
            let ports = d
                .pointer("/spec/ports")
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .map(|p| {
                            let port = p.get("port").and_then(|v| v.as_i64()).unwrap_or(0);
                            let proto = p.get("protocol").and_then(|v| v.as_str()).unwrap_or("TCP");
                            format!("{port}/{proto}")
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            json!({ "type": s(d, "/spec/type"), "cluster_ip": s(d, "/spec/clusterIP"), "ports": ports })
        }
        "ingresses" => {
            let hosts = d
                .pointer("/spec/rules")
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|r| r.get("host").and_then(|h| h.as_str()))
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            let addr = d
                .pointer("/status/loadBalancer/ingress/0/ip")
                .and_then(|v| v.as_str())
                .or_else(|| {
                    d.pointer("/status/loadBalancer/ingress/0/hostname")
                        .and_then(|v| v.as_str())
                })
                .unwrap_or("")
                .to_string();
            json!({ "class": s(d, "/spec/ingressClassName"), "hosts": hosts, "address": addr })
        }
        "configmaps" => {
            let keys = d
                .get("data")
                .and_then(|v| v.as_object())
                .map(|m| m.len())
                .unwrap_or(0);
            json!({ "keys": keys.to_string() })
        }
        "secrets" => {
            // keys/count only — never values
            let keys = d
                .get("data")
                .and_then(|v| v.as_object())
                .map(|m| m.len())
                .unwrap_or(0);
            json!({ "type": s(d, "/type"), "keys": keys.to_string() })
        }
        "nodes" => {
            let ready = node_ready(d);
            unhealthy = !ready;
            let roles = o
                .metadata
                .labels
                .as_ref()
                .map(|l| {
                    l.keys()
                        .filter_map(|k| k.strip_prefix("node-role.kubernetes.io/"))
                        .filter(|r| !r.is_empty())
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "<none>".into());
            json!({ "status": if ready { "Ready" } else { "NotReady" }, "roles": roles, "version": s(d, "/status/nodeInfo/kubeletVersion") })
        }
        "namespaces" => {
            let phase = s(d, "/status/phase");
            unhealthy = !phase.is_empty() && phase != "Active";
            json!({ "status": phase })
        }
        "events" => {
            let etype = s(d, "/type");
            unhealthy = etype == "Warning";
            let obj = format!(
                "{}/{}",
                s(d, "/involvedObject/kind"),
                s(d, "/involvedObject/name")
            );
            json!({ "type": etype, "reason": s(d, "/reason"), "object": obj, "message": s(d, "/message"), "count": i(d, "/count").to_string() })
        }
        _ => json!({}),
    };
    json!({
        "kind": kind,
        "name": meta_name(o),
        "namespace": meta_ns(o),
        "creation_ts": creation_ts(o),
        "unhealthy": unhealthy,
        "cols": cols,
    })
}

async fn list_kind(
    client: &Client,
    kind: &str,
    namespace: Option<&str>,
) -> Result<Vec<DynamicObject>, (StatusCode, String)> {
    let (g, v, k, p, namespaced) = kind_info(kind)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, format!("Unknown kind '{kind}'")))?;
    let ar = api_resource(g, v, k, p);
    let api: Api<DynamicObject> = match (namespaced, namespace) {
        (true, Some(ns)) if !ns.is_empty() => Api::namespaced_with(client.clone(), ns, &ar),
        _ => Api::all_with(client.clone(), &ar),
    };
    api.list(&ListParams::default())
        .await
        .map(|l| l.items)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to list {kind}: {e}"),
            )
        })
}

// ---------------------------------------------------------------------------
// GET /api/v1/kubernetes/summary
// ---------------------------------------------------------------------------
pub async fn summary(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    check_kubernetes_enabled(&state).await?;
    let client = get_kube_client().await?;

    let nodes = list_kind(&client, "nodes", None).await.unwrap_or_default();
    let nodes_ready = nodes.iter().filter(|n| node_ready(&n.data)).count();

    let pods = list_kind(&client, "pods", None).await.unwrap_or_default();
    let mut pods_running = 0usize;
    let mut pods_unhealthy = 0usize;
    for p in &pods {
        let (_, _, _, uh) = pod_status(&p.data);
        if s(&p.data, "/status/phase") == "Running" {
            pods_running += 1;
        }
        if uh {
            pods_unhealthy += 1;
        }
    }

    let namespaces = list_kind(&client, "namespaces", None)
        .await
        .unwrap_or_default();
    let events = list_kind(&client, "events", None).await.unwrap_or_default();
    let warnings = events
        .iter()
        .filter(|e| s(&e.data, "/type") == "Warning")
        .count();

    Ok(Json(json!({
        "nodes_ready": nodes_ready,
        "nodes_total": nodes.len(),
        "pods_running": pods_running,
        "pods_total": pods.len(),
        "pods_unhealthy": pods_unhealthy,
        "namespaces": namespaces.len(),
        "warnings": warnings,
    })))
}

// ---------------------------------------------------------------------------
// GET /api/v1/kubernetes/namespaces
// ---------------------------------------------------------------------------
pub async fn list_namespaces(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    check_kubernetes_enabled(&state).await?;
    let client = get_kube_client().await?;
    let items: Vec<Value> = list_kind(&client, "namespaces", None)
        .await?
        .iter()
        .map(|o| summarise("namespaces", o))
        .collect();
    Ok(Json(json!({ "namespaces": items })))
}

// ---------------------------------------------------------------------------
// GET /api/v1/kubernetes/resources/:kind?namespace=
// ---------------------------------------------------------------------------
pub async fn list_resources(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(kind): Path<String>,
    Query(q): Query<NsQuery>,
) -> Result<Json<Value>, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    check_kubernetes_enabled(&state).await?;
    if kind_info(&kind).is_none() {
        return Err((StatusCode::BAD_REQUEST, format!("Unknown kind '{kind}'")));
    }
    let client = get_kube_client().await?;
    let items: Vec<Value> = list_kind(&client, &kind, q.namespace.as_deref())
        .await?
        .iter()
        .map(|o| summarise(&kind, o))
        .collect();
    Ok(Json(json!({ "resources": items })))
}

// ---------------------------------------------------------------------------
// GET /api/v1/kubernetes/resources/:kind/:namespace/:name
// ---------------------------------------------------------------------------
pub async fn get_resource(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((kind, namespace, name)): Path<(String, String, String)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    check_kubernetes_enabled(&state).await?;
    let (g, v, k, p, namespaced) = kind_info(&kind)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, format!("Unknown kind '{kind}'")))?;
    let client = get_kube_client().await?;
    let ar = api_resource(g, v, k, p);

    let api: Api<DynamicObject> = if namespaced && !namespace.is_empty() && namespace != "_" {
        Api::namespaced_with(client.clone(), &namespace, &ar)
    } else {
        Api::all_with(client.clone(), &ar)
    };
    let obj = api.get(&name).await.map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            format!("{kind} '{name}' not found: {e}"),
        )
    })?;

    let d = &obj.data;
    let summary = summarise(&kind, &obj);

    // Generic conditions
    let conditions: Vec<Value> = d
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .map(|c| {
                    json!({
                        "type": c.get("type").and_then(|v| v.as_str()).unwrap_or(""),
                        "status": c.get("status").and_then(|v| v.as_str()).unwrap_or(""),
                        "reason": c.get("reason").and_then(|v| v.as_str()).unwrap_or(""),
                        "message": c.get("message").and_then(|v| v.as_str()).unwrap_or(""),
                        "last_transition_time": c.get("lastTransitionTime").and_then(|v| v.as_str()).unwrap_or(""),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    // Pod container states
    let containers: Vec<Value> = if kind == "pods" {
        d.pointer("/status/containerStatuses")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|c| {
                        let state = c
                            .get("state")
                            .and_then(|st| st.as_object())
                            .and_then(|m| m.keys().next().cloned())
                            .unwrap_or_else(|| "unknown".into());
                        let reason = c
                            .pointer(&format!("/state/{state}/reason"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        json!({
                            "name": c.get("name").and_then(|v| v.as_str()).unwrap_or(""),
                            "image": c.get("image").and_then(|v| v.as_str()).unwrap_or(""),
                            "ready": c.get("ready").and_then(|v| v.as_bool()).unwrap_or(false),
                            "restarts": c.get("restartCount").and_then(|v| v.as_i64()).unwrap_or(0),
                            "state": state,
                            "reason": reason,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    // Owner references
    let owners: Vec<Value> = obj
        .metadata
        .owner_references
        .as_ref()
        .map(|refs| {
            refs.iter()
                .map(|r| json!({ "kind": r.kind, "name": r.name }))
                .collect()
        })
        .unwrap_or_default();

    // Object-scoped events (best-effort)
    let events: Vec<Value> = if !namespace.is_empty() && namespace != "_" {
        let ev_ar = api_resource("", "v1", "Event", "events");
        let ev_api: Api<DynamicObject> = Api::namespaced_with(client.clone(), &namespace, &ev_ar);
        ev_api
            .list(&ListParams::default().fields(&format!("involvedObject.name={name}")))
            .await
            .map(|l| {
                l.items
                    .iter()
                    .map(|e| {
                        json!({
                            "type": s(&e.data, "/type"),
                            "reason": s(&e.data, "/reason"),
                            "message": s(&e.data, "/message"),
                            "count": i(&e.data, "/count"),
                            "last_timestamp": s(&e.data, "/lastTimestamp"),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    let labels = obj.metadata.labels.clone().unwrap_or_default();

    Ok(Json(json!({
        "summary": summary,
        "conditions": conditions,
        "containers": containers,
        "owners": owners,
        "events": events,
        "labels": labels,
    })))
}
