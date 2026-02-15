use std::sync::Arc;
use crate::config_db::ConfigDb;
use crate::models::query::Filter;
use crate::query_builder::build_where_clause;
use clickhouse::Client;

#[derive(clickhouse::Row, serde::Deserialize)]
struct CountRow {
    count: u64,
}

pub fn spawn_slo_engine(config_db: Arc<ConfigDb>, ch: Client) {
    tokio::spawn(async move {
        let http_client = reqwest::Client::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
        loop {
            interval.tick().await;
            if let Err(e) = eval_slos(&config_db, &ch, &http_client).await {
                tracing::error!("slo engine error: {e}");
            }
        }
    });
}

fn window_minutes(window_type: &str) -> i64 {
    match window_type {
        "rolling_1h" => 60,
        "rolling_24h" => 1440,
        "rolling_7d" => 10080,
        "rolling_30d" => 43200,
        _ => 60,
    }
}

async fn eval_slos(
    config_db: &ConfigDb,
    ch: &Client,
    http_client: &reqwest::Client,
) -> anyhow::Result<()> {
    let now = chrono::Utc::now();
    let now_str = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let due_slos = config_db.get_due_slos(&now_str)?;

    for slo in due_slos {
        let minutes = window_minutes(&slo.window_type);
        let from = (now - chrono::Duration::minutes(minutes))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();

        // Parse good_filters
        let good_filters: Vec<Filter> = match serde_json::from_str(&slo.good_filters) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("slo {}: bad good_filters: {e}", slo.id);
                continue;
            }
        };

        // Parse total_filters
        let total_filters: Vec<Filter> = match serde_json::from_str(&slo.total_filters) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("slo {}: bad total_filters: {e}", slo.id);
                continue;
            }
        };

        // Query good count
        let good_where = build_where_clause(&good_filters, &from, &now_str);
        let good_sql = format!("SELECT count() as count FROM wide_events WHERE {good_where}");
        let good_count = match ch.query(&good_sql).fetch_one::<CountRow>().await {
            Ok(row) => row.count as i64,
            Err(e) => {
                tracing::warn!("slo {}: good query failed: {e}", slo.id);
                config_db.update_slo_state(&slo.id, "no_data", 0.0, 0, 0, &now_str, None)?;
                continue;
            }
        };

        // Query total count
        let total_where = build_where_clause(&total_filters, &from, &now_str);
        let total_sql = format!("SELECT count() as count FROM wide_events WHERE {total_where}");
        let total_count = match ch.query(&total_sql).fetch_one::<CountRow>().await {
            Ok(row) => row.count as i64,
            Err(e) => {
                tracing::warn!("slo {}: total query failed: {e}", slo.id);
                config_db.update_slo_state(&slo.id, "no_data", 0.0, 0, 0, &now_str, None)?;
                continue;
            }
        };

        // Calculate error budget
        let (new_state, error_budget_remaining) = if total_count == 0 {
            ("no_data", 0.0_f64)
        } else {
            let error_budget = 1.0 - slo.target_percentage / 100.0;
            let consumed = 1.0 - (good_count as f64 / total_count as f64);
            let remaining = error_budget - consumed;
            let state = if remaining > 0.0 { "compliant" } else { "breaching" };
            (state, remaining)
        };

        let old_state = slo.state.as_str();

        if new_state != old_state {
            // State changed — record event and notify
            let event_id = uuid::Uuid::new_v4().to_string();
            let message = format!(
                "SLO '{}': {} (good={}, total={}, budget_remaining={:.4}%)",
                slo.name,
                match new_state {
                    "breaching" => "BREACHING",
                    "compliant" => "COMPLIANT",
                    _ => "NO_DATA",
                },
                good_count,
                total_count,
                error_budget_remaining * 100.0,
            );

            config_db.create_slo_event(
                &event_id,
                &slo.id,
                new_state,
                good_count,
                total_count,
                error_budget_remaining,
                &message,
            )?;

            let breached_at = if new_state == "breaching" { Some(now_str.as_str()) } else { None };
            config_db.update_slo_state(
                &slo.id, new_state, error_budget_remaining,
                good_count, total_count, &now_str, breached_at,
            )?;

            // Send notifications
            let channel_ids: Vec<String> = serde_json::from_str(&slo.notification_channel_ids)
                .unwrap_or_default();
            for channel_id in &channel_ids {
                if let Ok(Some(channel)) = config_db.get_channel(channel_id) {
                    let config: serde_json::Value = serde_json::from_str(&channel.config)
                        .unwrap_or(serde_json::json!({}));
                    if let Some(url) = config.get("url").and_then(|u| u.as_str()) {
                        let payload = match channel.channel_type.as_str() {
                            "slack" => serde_json::json!({ "text": message }),
                            _ => serde_json::json!({
                                "slo": slo.name,
                                "state": new_state,
                                "good_count": good_count,
                                "total_count": total_count,
                                "error_budget_remaining": error_budget_remaining,
                                "message": message,
                            }),
                        };
                        if let Err(e) = http_client.post(url).json(&payload).send().await {
                            tracing::warn!("slo {}: notification to {} failed: {e}", slo.id, channel.name);
                        }
                    }
                }
            }

            tracing::info!("slo '{}' state: {} -> {}", slo.name, old_state, new_state);
        } else {
            config_db.update_slo_state(
                &slo.id, new_state, error_budget_remaining,
                good_count, total_count, &now_str, None,
            )?;
        }
    }

    Ok(())
}
