use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use kube::{Api, Client, api::ListParams};
use kube::api::DynamicObject;
use kube::discovery::ApiResource;
use serde_json::{Value, json};

use crate::AppState;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn get_kube_client() -> Result<Client, (StatusCode, String)> {
    Client::try_default().await.map_err(|e| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("Kubernetes not available: {e}"),
        )
    })
}

fn check_argocd_enabled(state: &AppState) -> Result<(), (StatusCode, String)> {
    // Enabled if either the setting is true OR the ARGOCD_NAMESPACE env var is set (helm chart)
    let setting_enabled = state
        .config_db
        .get_setting("argocd_enabled")
        .ok()
        .flatten()
        .map(|v| v == "true")
        .unwrap_or(false);
    let env_enabled = std::env::var("ARGOCD_NAMESPACE").is_ok();
    if !setting_enabled && !env_enabled {
        return Err((
            StatusCode::NOT_FOUND,
            "ArgoCD integration is not enabled".to_string(),
        ));
    }
    Ok(())
}

fn argocd_namespace(state: &AppState) -> String {
    // Check env var first (set by helm chart), then DB setting, then default
    if let Ok(ns) = std::env::var("ARGOCD_NAMESPACE") {
        return ns;
    }
    state
        .config_db
        .get_setting("argocd_namespace")
        .ok()
        .flatten()
        .unwrap_or_else(|| "argocd".to_string())
}

fn application_ar() -> ApiResource {
    ApiResource {
        group: "argoproj.io".into(),
        version: "v1alpha1".into(),
        kind: "Application".into(),
        api_version: "argoproj.io/v1alpha1".into(),
        plural: "applications".into(),
    }
}

fn applicationset_ar() -> ApiResource {
    ApiResource {
        group: "argoproj.io".into(),
        version: "v1alpha1".into(),
        kind: "ApplicationSet".into(),
        api_version: "argoproj.io/v1alpha1".into(),
        plural: "applicationsets".into(),
    }
}

/// Convenience accessor into a serde_json::Value using a dotted path.
fn jpath<'a>(v: &'a Value, path: &str) -> &'a Value {
    let mut cur = v;
    for seg in path.split('.') {
        cur = &cur[seg];
    }
    cur
}

fn jstr(v: &Value) -> Option<String> {
    v.as_str().map(|s| s.to_string())
}

// ---------------------------------------------------------------------------
// Summarise a single Application for the list endpoint.
// ---------------------------------------------------------------------------
/// Extract source info — handles both single-source (.spec.source) and multi-source (.spec.sources[])
fn extract_source(spec: &Value) -> (String, String, String, String) {
    // Try single source first
    if spec["source"]["repoURL"].is_string() {
        let repo = jstr(&spec["source"]["repoURL"]).unwrap_or_default();
        let path = jstr(&spec["source"]["path"]).unwrap_or_default();
        let chart = jstr(&spec["source"]["chart"]).unwrap_or_default();
        let target_rev = jstr(&spec["source"]["targetRevision"]).unwrap_or_default();
        return (repo, path, chart, target_rev);
    }
    // Multi-source: use the first source with a repoURL
    if let Some(sources) = spec["sources"].as_array() {
        for src in sources {
            if let Some(repo) = jstr(&src["repoURL"]) {
                if !repo.is_empty() {
                    let path = jstr(&src["path"]).unwrap_or_default();
                    let chart = jstr(&src["chart"]).unwrap_or_default();
                    let target_rev = jstr(&src["targetRevision"]).unwrap_or_default();
                    return (repo, path, chart, target_rev);
                }
            }
        }
    }
    (String::new(), String::new(), String::new(), String::new())
}

/// Extract sync revision — handles both .status.sync.revision and .status.sync.revisions[]
fn extract_sync_revision(sync: &Value) -> String {
    if let Some(rev) = jstr(&sync["revision"]) {
        if !rev.is_empty() { return rev; }
    }
    if let Some(revisions) = sync["revisions"].as_array() {
        let revs: Vec<&str> = revisions.iter().filter_map(|v| v.as_str()).filter(|s| !s.is_empty()).collect();
        if !revs.is_empty() { return revs.join(", "); }
    }
    String::new()
}

fn summarise_app(obj: &DynamicObject) -> Value {
    let data = &obj.data;
    let name = obj
        .metadata
        .name
        .as_deref()
        .unwrap_or_default()
        .to_string();

    let status = &data["status"];
    let spec = &data["spec"];

    let health_status = jstr(&status["health"]["status"]).unwrap_or_default();
    let health_message = jstr(&status["health"]["message"]).unwrap_or_default();
    let sync_status = jstr(&status["sync"]["status"]).unwrap_or_default();
    let sync_revision = extract_sync_revision(&status["sync"]);

    let (repo, path, _chart, _target_rev) = extract_source(spec);
    let dest_namespace = jstr(&spec["destination"]["namespace"]).unwrap_or_default();
    let dest_server = jstr(&spec["destination"]["server"]).unwrap_or_default();
    let project = jstr(&spec["project"]).unwrap_or_default();

    let last_synced_at = jstr(jpath(status, "operationState.finishedAt")).unwrap_or_default();

    let conditions_count = status["conditions"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);

    let images: Vec<String> = status["summary"]["images"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    json!({
        "name": name,
        "project": project,
        "health_status": health_status,
        "health_message": health_message,
        "sync_status": sync_status,
        "sync_revision": sync_revision,
        "repo": repo,
        "path": path,
        "dest_namespace": dest_namespace,
        "dest_server": dest_server,
        "last_synced_at": last_synced_at,
        "conditions_count": conditions_count,
        "images": images,
    })
}

// ---------------------------------------------------------------------------
// GET /api/v1/argocd/applications
// ---------------------------------------------------------------------------
pub async fn list_applications(
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    check_argocd_enabled(&state)?;
    let namespace = argocd_namespace(&state);
    let client = get_kube_client().await?;

    let apps: Api<DynamicObject> = Api::namespaced_with(client, &namespace, &application_ar());
    let list = apps
        .list(&ListParams::default())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to list ArgoCD applications: {e}")))?;

    let items: Vec<Value> = list.items.iter().map(summarise_app).collect();
    Ok(Json(json!({ "applications": items })))
}

// ---------------------------------------------------------------------------
// GET /api/v1/argocd/applications/:name
// ---------------------------------------------------------------------------
pub async fn get_application(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    check_argocd_enabled(&state)?;
    let namespace = argocd_namespace(&state);
    let client = get_kube_client().await?;

    let apps: Api<DynamicObject> = Api::namespaced_with(client, &namespace, &application_ar());
    let app = apps.get(&name).await.map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            format!("Application '{name}' not found: {e}"),
        )
    })?;

    let data = &app.data;
    let status = &data["status"];
    let spec = &data["spec"];

    // Health
    let health = &status["health"];

    // Sync
    let sync = &status["sync"];

    // Operation state
    let operation_state = &status["operationState"];

    // Conditions
    let conditions = status["conditions"]
        .as_array()
        .cloned()
        .unwrap_or_default();

    // Resources - filter to unhealthy only for brevity
    let resources: Vec<&Value> = status["resources"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter(|r| {
                    let h = r["health"]["status"].as_str().unwrap_or("Healthy");
                    h != "Healthy"
                })
                .collect()
        })
        .unwrap_or_default();

    // History - last 10, extract useful fields from both single and multi-source formats
    let history: Vec<Value> = status["history"]
        .as_array()
        .map(|arr| {
            arr.iter().rev().take(10).map(|h| {
                // Get revision: try .revision first, then .revisions[]
                let revision = if let Some(rev) = jstr(&h["revision"]) {
                    rev
                } else if let Some(revs) = h["revisions"].as_array() {
                    revs.iter().filter_map(|v| v.as_str()).filter(|s| !s.is_empty()).collect::<Vec<_>>().join(", ")
                } else {
                    String::new()
                };
                // Get source repo: try .source.repoURL, then .sources[0].repoURL
                let source_repo = jstr(&h["source"]["repoURL"])
                    .filter(|s| !s.is_empty())
                    .or_else(|| {
                        h["sources"].as_array().and_then(|srcs| {
                            srcs.iter().find_map(|s| jstr(&s["repoURL"]).filter(|r| !r.is_empty()))
                        })
                    })
                    .unwrap_or_default();
                json!({
                    "revision": revision,
                    "deployed_at": jstr(&h["deployedAt"]).unwrap_or_default(),
                    "source_repo": source_repo,
                })
            }).collect()
        })
        .unwrap_or_default();

    // Source — handle both single and multi-source
    let (repo, path, chart, target_revision) = extract_source(spec);

    // All sources for multi-source apps
    let sources: Vec<Value> = spec["sources"]
        .as_array()
        .map(|arr| {
            arr.iter().map(|s| json!({
                "repo": jstr(&s["repoURL"]).unwrap_or_default(),
                "path": jstr(&s["path"]).unwrap_or_default(),
                "chart": jstr(&s["chart"]).unwrap_or_default(),
                "target_revision": jstr(&s["targetRevision"]).unwrap_or_default(),
                "ref": jstr(&s["ref"]).unwrap_or_default(),
            })).collect()
        })
        .unwrap_or_default();

    // Destination
    let dest_namespace = jstr(&spec["destination"]["namespace"]).unwrap_or_default();
    let dest_server = jstr(&spec["destination"]["server"]).unwrap_or_default();
    let sync_revision = extract_sync_revision(sync);

    Ok(Json(json!({
        "name": app.metadata.name.as_deref().unwrap_or_default(),
        "project": jstr(&spec["project"]).unwrap_or_default(),
        "health_status": jstr(&health["status"]).unwrap_or_default(),
        "health_message": jstr(&health["message"]).unwrap_or_default(),
        "sync_status": jstr(&sync["status"]).unwrap_or_default(),
        "sync_revision": sync_revision,
        "operation_phase": jstr(&operation_state["phase"]).unwrap_or_default(),
        "operation_message": jstr(&operation_state["message"]).unwrap_or_default(),
        "conditions": conditions,
        "unhealthy_resources": resources,
        "history": history,
        "repo": repo,
        "path": path,
        "chart": chart,
        "target_revision": target_revision,
        "sources": sources,
        "dest_namespace": dest_namespace,
        "dest_server": dest_server,
        "last_synced_at": jstr(jpath(status, "operationState.finishedAt")).unwrap_or_default(),
        "images": status["summary"]["images"].as_array().map(|a| a.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>()).unwrap_or_default(),
    })))
}

// ---------------------------------------------------------------------------
// GET /api/v1/argocd/applicationsets
// ---------------------------------------------------------------------------
pub async fn list_applicationsets(
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    check_argocd_enabled(&state)?;
    let namespace = argocd_namespace(&state);
    let client = get_kube_client().await?;

    let appsets: Api<DynamicObject> =
        Api::namespaced_with(client, &namespace, &applicationset_ar());
    let list = appsets
        .list(&ListParams::default())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to list ApplicationSets: {e}")))?;

    let items: Vec<Value> = list
        .items
        .iter()
        .map(|obj| {
            let data = &obj.data;
            let name = obj
                .metadata
                .name
                .as_deref()
                .unwrap_or_default()
                .to_string();

            // Generator types: each key under spec.generators[]
            let generators: Vec<String> = data["spec"]["generators"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .flat_map(|g| {
                            g.as_object()
                                .map(|m| m.keys().cloned().collect::<Vec<_>>())
                                .unwrap_or_default()
                        })
                        .collect()
                })
                .unwrap_or_default();

            let conditions = data["status"]["conditions"]
                .as_array()
                .cloned()
                .unwrap_or_default();

            let health = jstr(&data["status"]["health"]["status"]).unwrap_or_default();

            json!({
                "name": name,
                "generators": generators,
                "conditions": conditions,
                "health": health,
            })
        })
        .collect();

    Ok(Json(json!({ "applicationsets": items })))
}
