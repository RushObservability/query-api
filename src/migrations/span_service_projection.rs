use std::sync::Arc;

use clickhouse::Client;

use crate::audit::{AuditEvent, AuditLogger};

const PROJECTION_NAME: &str = "idx_spans_by_service";
// Cover service RED/latency/endpoint queries. A keys-only _part_offset
// projection was not selected for these reads in the benchmark. Keep large
// attributes, events, links and trace identifiers only in the base table.
const PROJECTION_SELECT: &str = "SELECT tenant_id, service_name, timestamp, duration_ns, kind, status, \
        http_status_code, http_method, http_path, span_name \
    ORDER BY (tenant_id, service_name, timestamp)";

/// Existing installations build the projection in the background. Queries keep
/// using base parts until ClickHouse can use the projection; startup never waits
/// for historical spans to be rewritten.
pub fn spawn_span_service_projection_maintenance(
    client: Client,
    database: String,
    audit: Arc<AuditLogger>,
) {
    tokio::spawn(async move {
        if let Err(error) = apply(&client, &database, &audit).await {
            tracing::warn!(%error, projection = PROJECTION_NAME, "span service projection maintenance failed");
        }
    });
}

async fn apply(client: &Client, database: &str, audit: &AuditLogger) -> anyhow::Result<()> {
    match drop_outdated_projection(client, database).await {
        Ok(true) => {
            audit
                .log(migration_event(database, "drop_outdated", "success"))
                .await;
        }
        Ok(false) => {}
        Err(error) => {
            audit
                .log(migration_event(database, "drop_outdated", "failure"))
                .await;
            return Err(error);
        }
    }
    match add_projection(client, database).await {
        Ok(true) => {
            audit
                .log(migration_event(database, "create", "success"))
                .await;
        }
        Ok(false) => {}
        Err(error) => {
            audit
                .log(migration_event(database, "create", "failure"))
                .await;
            return Err(error);
        }
    }
    match materialize_projection(client, database).await {
        Ok(true) => {
            audit
                .log(migration_event(database, "materialize", "success"))
                .await;
        }
        Ok(false) => {}
        Err(error) => {
            audit
                .log(migration_event(database, "materialize", "failure"))
                .await;
            return Err(error);
        }
    }
    Ok(())
}

async fn drop_outdated_projection(client: &Client, database: &str) -> anyhow::Result<bool> {
    let database_identifier = super::quote_table_identifier(database)?;
    let definitions = client
        .query("SELECT query FROM system.projections WHERE database = ? AND table = 'spans' AND name = ?")
        .bind(database)
        .bind(PROJECTION_NAME)
        .fetch_all::<String>()
        .await?;
    let Some(definition) = definitions.first() else {
        return Ok(false);
    };
    // Canonical ClickHouse formatting omits the ORDER BY parentheses. Compare
    // definitions so a previous projection under this name is not left in use.
    let normalize = |sql: &str| {
        sql.chars()
            .filter(|ch| !ch.is_ascii_whitespace() && !matches!(*ch, '(' | ')'))
            .collect::<String>()
    };
    if normalize(definition) == normalize(PROJECTION_SELECT) {
        return Ok(false);
    }
    // Drop only this managed secondary index. Stored spans remain available
    // through the base table while the replacement is built.
    client
        .query(&format!(
            "ALTER TABLE {database_identifier}.spans DROP PROJECTION IF EXISTS {PROJECTION_NAME}"
        ))
        .execute()
        .await?;
    Ok(true)
}

fn migration_event(database: &str, operation: &str, outcome: &str) -> AuditEvent {
    AuditEvent::new("schema.migrate", "system")
        .resource("table", format!("{database}.spans"))
        .outcome(outcome)
        .changes(
            serde_json::json!({
                "migration": "span_service_projection",
                "projection": PROJECTION_NAME,
                "operation": operation,
            })
            .to_string(),
        )
}

async fn add_projection(client: &Client, database: &str) -> anyhow::Result<bool> {
    let database_identifier = super::quote_table_identifier(database)?;
    let exists = client
        .query(
            "SELECT count() FROM system.projections \
             WHERE database = ? AND table = 'spans' AND name = ?",
        )
        .bind(database)
        .bind(PROJECTION_NAME)
        .fetch_one::<u64>()
        .await?;
    if exists != 0 {
        return Ok(false);
    }
    client
        .query(&format!(
            "ALTER TABLE {database_identifier}.spans ADD PROJECTION IF NOT EXISTS {PROJECTION_NAME} ({PROJECTION_SELECT})"
        ))
        .execute()
        .await?;
    Ok(true)
}

async fn materialize_projection(client: &Client, database: &str) -> anyhow::Result<bool> {
    let database_identifier = super::quote_table_identifier(database)?;
    let missing = client
        .query(
            "SELECT count() FROM system.parts \
             WHERE database = ? AND table = 'spans' AND active AND rows > 0 \
               AND name NOT IN (SELECT parent_name FROM system.projection_parts \
                   WHERE database = ? AND table = 'spans' AND active AND name = ?)",
        )
        .bind(database)
        .bind(database)
        .bind(PROJECTION_NAME)
        .fetch_one::<u64>()
        .await?;
    if missing == 0 {
        return Ok(false);
    }
    let pending = client
        .query(
            "SELECT count() FROM system.mutations \
             WHERE database = ? AND table = 'spans' AND NOT is_done \
               AND position(command, ?) > 0",
        )
        .bind(database)
        .bind(format!("MATERIALIZE PROJECTION {PROJECTION_NAME}"))
        .fetch_one::<u64>()
        .await?;
    if pending != 0 {
        return Ok(false);
    }
    tracing::info!(
        projection = PROJECTION_NAME,
        "materializing span service projection"
    );
    client
        .query(&format!(
            "ALTER TABLE {database_identifier}.spans MATERIALIZE PROJECTION {PROJECTION_NAME} \
             SETTINGS mutations_sync = 1"
        ))
        .execute()
        .await?;
    Ok(true)
}

#[cfg(test)]
mod tests;
