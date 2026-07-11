use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use kube::api::DynamicObject;
use kube::discovery::ApiResource;
use kube::{Api, Client, api::ListParams};
use serde_json::{Value, json};

use crate::AppState;
use crate::handlers::users::require_auth;

// ---------------------------------------------------------------------------
// Flux v2 GitOps Toolkit integration.
//
// Flux has no single "Application" CRD. The "deployments" are Kustomizations
// (kustomize.toolkit.fluxcd.io) and HelmReleases (helm.toolkit.fluxcd.io); the
// inputs are Sources (source.toolkit.fluxcd.io). Every resource carries a
// Kubernetes `Ready` condition and a `spec.suspend` flag, so the summary logic
// is uniform. Flux resources are multi-namespace, so we list across all
// namespaces (the reader ClusterRole is cluster-scoped).
// ---------------------------------------------------------------------------

async fn get_kube_client() -> Result<Client, (StatusCode, String)> {
    Client::try_default().await.map_err(|e| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("Kubernetes not available: {e}"),
        )
    })
}

async fn check_fluxcd_enabled(state: &AppState) -> Result<(), (StatusCode, String)> {
    // Enabled if the setting is true OR the FLUXCD_NAMESPACE env var is set (helm chart).
    let setting_enabled = state
        .config_db
        .get_setting("fluxcd_enabled")
        .await
        .ok()
        .flatten()
        .map(|v| v == "true")
        .unwrap_or(false);
    let env_enabled = std::env::var("FLUXCD_NAMESPACE").is_ok();
    if !setting_enabled && !env_enabled {
        return Err((
            StatusCode::NOT_FOUND,
            "FluxCD integration is not enabled".to_string(),
        ));
    }
    Ok(())
}

// ── CRD ApiResources ───────────────────────────────────────────────────────
fn ar(group: &str, version: &str, kind: &str, plural: &str) -> ApiResource {
    ApiResource {
        group: group.into(),
        version: version.into(),
        kind: kind.into(),
        api_version: format!("{group}/{version}"),
        plural: plural.into(),
    }
}

fn kustomization_ar() -> ApiResource {
    ar(
        "kustomize.toolkit.fluxcd.io",
        "v1",
        "Kustomization",
        "kustomizations",
    )
}
fn helmrelease_ar() -> ApiResource {
    ar(
        "helm.toolkit.fluxcd.io",
        "v2",
        "HelmRelease",
        "helmreleases",
    )
}
fn source_ars() -> Vec<(&'static str, ApiResource)> {
    vec![
        (
            "GitRepository",
            ar(
                "source.toolkit.fluxcd.io",
                "v1",
                "GitRepository",
                "gitrepositories",
            ),
        ),
        (
            "OCIRepository",
            ar(
                "source.toolkit.fluxcd.io",
                "v1beta2",
                "OCIRepository",
                "ocirepositories",
            ),
        ),
        (
            "HelmRepository",
            ar(
                "source.toolkit.fluxcd.io",
                "v1",
                "HelmRepository",
                "helmrepositories",
            ),
        ),
        (
            "Bucket",
            ar("source.toolkit.fluxcd.io", "v1", "Bucket", "buckets"),
        ),
    ]
}

// ── status extraction ────────────────────────────────────────────────────
/// The `Ready` condition: returns (status "True"/"False"/"Unknown", message, lastTransitionTime, reason).
fn ready_condition(status: &Value) -> (String, String, String, String) {
    let conds = status.get("conditions").and_then(|v| v.as_array());
    if let Some(arr) = conds {
        // Prefer Ready; fall back to the first condition.
        let ready = arr
            .iter()
            .find(|c| c.get("type").and_then(|t| t.as_str()) == Some("Ready"))
            .or_else(|| arr.first());
        if let Some(c) = ready {
            return (
                c.get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown")
                    .to_string(),
                c.get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                c.get("lastTransitionTime")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                c.get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
            );
        }
    }
    (
        "Unknown".into(),
        String::new(),
        String::new(),
        String::new(),
    )
}

fn is_reconciling(status: &Value) -> bool {
    status
        .get("conditions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter().any(|c| {
                c.get("type").and_then(|t| t.as_str()) == Some("Reconciling")
                    && c.get("status").and_then(|s| s.as_str()) == Some("True")
            })
        })
        .unwrap_or(false)
}

fn source_ref(spec: &Value) -> String {
    // Kustomization/HelmRelease point at a source via spec.sourceRef; HelmRelease
    // may instead use spec.chart.spec.sourceRef or spec.chartRef.
    let r = spec
        .get("sourceRef")
        .or_else(|| spec.pointer("/chart/spec/sourceRef"))
        .or_else(|| spec.get("chartRef"));
    if let Some(r) = r {
        let kind = r.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let name = r.get("name").and_then(|v| v.as_str()).unwrap_or("");
        if !name.is_empty() {
            return if kind.is_empty() {
                name.to_string()
            } else {
                format!("{kind}/{name}")
            };
        }
    }
    String::new()
}

/// Summarise any Flux deployment resource (Kustomization / HelmRelease) for the list view.
fn summarise_resource(kind: &str, obj: &DynamicObject) -> Value {
    let data = &obj.data;
    let spec = data.get("spec").unwrap_or(&Value::Null);
    let status = data.get("status").unwrap_or(&Value::Null);
    let (ready, message, last_transition, _reason) = ready_condition(status);
    let revision = status
        .get("lastAppliedRevision")
        .and_then(|v| v.as_str())
        .or_else(|| status.get("lastAttemptedRevision").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();
    json!({
        "kind": kind,
        "name": obj.metadata.name.clone().unwrap_or_default(),
        "namespace": obj.metadata.namespace.clone().unwrap_or_default(),
        "ready": ready,
        "message": message,
        "suspended": spec.get("suspend").and_then(|v| v.as_bool()).unwrap_or(false),
        "reconciling": is_reconciling(status),
        "source": source_ref(spec),
        "revision": revision,
        "last_reconciled_at": last_transition,
    })
}

/// Summarise a Flux source (GitRepository / OCIRepository / HelmRepository / Bucket).
fn summarise_source(kind: &str, obj: &DynamicObject) -> Value {
    let data = &obj.data;
    let spec = data.get("spec").unwrap_or(&Value::Null);
    let status = data.get("status").unwrap_or(&Value::Null);
    let (ready, message, last_transition, _reason) = ready_condition(status);
    let revision = status
        .pointer("/artifact/revision")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let url = spec
        .get("url")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    json!({
        "kind": kind,
        "name": obj.metadata.name.clone().unwrap_or_default(),
        "namespace": obj.metadata.namespace.clone().unwrap_or_default(),
        "ready": ready,
        "message": message,
        "suspended": spec.get("suspend").and_then(|v| v.as_bool()).unwrap_or(false),
        "url": url,
        "revision": revision,
        "last_reconciled_at": last_transition,
    })
}

async fn list_kind(client: &Client, kind: &str, ar: &ApiResource) -> Vec<DynamicObject> {
    let api: Api<DynamicObject> = Api::all_with(client.clone(), ar);
    match api.list(&ListParams::default()).await {
        Ok(l) => l.items,
        Err(e) => {
            // A missing CRD (Flux feature not installed) is not fatal — skip it.
            tracing::debug!(kind, error = %e, "flux list failed (skipped)");
            Vec::new()
        }
    }
}

// ---------------------------------------------------------------------------
// GET /api/v1/fluxcd/resources  → Kustomizations + HelmReleases
// ---------------------------------------------------------------------------
pub async fn list_resources(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    check_fluxcd_enabled(&state).await?;
    let client = get_kube_client().await?;

    let mut items: Vec<Value> = Vec::new();
    for k in list_kind(&client, "Kustomization", &kustomization_ar()).await {
        items.push(summarise_resource("Kustomization", &k));
    }
    for h in list_kind(&client, "HelmRelease", &helmrelease_ar()).await {
        items.push(summarise_resource("HelmRelease", &h));
    }
    Ok(Json(json!({ "resources": items })))
}

// ---------------------------------------------------------------------------
// GET /api/v1/fluxcd/sources  → Git/OCI/Helm repositories + buckets
// ---------------------------------------------------------------------------
pub async fn list_sources(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    check_fluxcd_enabled(&state).await?;
    let client = get_kube_client().await?;

    let mut items: Vec<Value> = Vec::new();
    for (kind, ar) in source_ars() {
        for s in list_kind(&client, kind, &ar).await {
            items.push(summarise_source(kind, &s));
        }
    }
    Ok(Json(json!({ "sources": items })))
}

// ---------------------------------------------------------------------------
// GET /api/v1/fluxcd/resources/:kind/:name  → full detail for one resource
// ---------------------------------------------------------------------------
pub async fn get_resource(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((kind, name)): Path<(String, String)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    check_fluxcd_enabled(&state).await?;
    let client = get_kube_client().await?;

    let ar = match kind.as_str() {
        "Kustomization" => kustomization_ar(),
        "HelmRelease" => helmrelease_ar(),
        other => source_ars()
            .into_iter()
            .find(|(k, _)| *k == other)
            .map(|(_, ar)| ar)
            .ok_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("Unknown Flux kind '{kind}'"),
                )
            })?,
    };

    // Resource is identified by name across namespaces — fetch by listing on a
    // field selector (name is unique enough; if duplicated across ns, take the first).
    let api: Api<DynamicObject> = Api::all_with(client, &ar);
    let obj = api
        .list(&ListParams::default().fields(&format!("metadata.name={name}")))
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get {kind} '{name}': {e}"),
            )
        })?
        .items
        .into_iter()
        .next()
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("{kind} '{name}' not found")))?;

    let data = &obj.data;
    let spec = data.get("spec").unwrap_or(&Value::Null);
    let status = data.get("status").unwrap_or(&Value::Null);
    let (ready, message, last_transition, reason) = ready_condition(status);

    let conditions: Vec<Value> = status
        .get("conditions")
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

    let depends_on: Vec<String> = spec
        .get("dependsOn")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|d| {
                    let n = d.get("name").and_then(|v| v.as_str())?;
                    let ns = d.get("namespace").and_then(|v| v.as_str());
                    Some(match ns {
                        Some(ns) => format!("{ns}/{n}"),
                        None => n.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    // HelmRelease chart info
    let chart = spec
        .pointer("/chart/spec/chart")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let chart_version = spec
        .pointer("/chart/spec/version")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    Ok(Json(json!({
        "kind": kind,
        "name": obj.metadata.name.clone().unwrap_or_default(),
        "namespace": obj.metadata.namespace.clone().unwrap_or_default(),
        "ready": ready,
        "ready_reason": reason,
        "message": message,
        "suspended": spec.get("suspend").and_then(|v| v.as_bool()).unwrap_or(false),
        "reconciling": is_reconciling(status),
        "source": source_ref(spec),
        "path": spec.get("path").and_then(|v| v.as_str()).unwrap_or(""),
        "last_applied_revision": status.get("lastAppliedRevision").and_then(|v| v.as_str()).unwrap_or(""),
        "last_attempted_revision": status.get("lastAttemptedRevision").and_then(|v| v.as_str()).unwrap_or(""),
        "last_reconciled_at": last_transition,
        "interval": spec.get("interval").and_then(|v| v.as_str()).unwrap_or(""),
        "chart": chart,
        "chart_version": chart_version,
        "depends_on": depends_on,
        "conditions": conditions,
    })))
}
