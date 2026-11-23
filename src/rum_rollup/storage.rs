use std::sync::{Arc, atomic::Ordering};

use clickhouse::Client;

use super::{Kind, RumRollups};
use crate::audit::{AuditEvent, AuditLogger};

#[derive(Clone, Copy)]
enum Operation {
    DropOutdated,
    Create,
    Materialize,
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::DropOutdated => "drop_outdated",
            Self::Create => "create",
            Self::Materialize => "materialize",
        }
    }
}

const OPERATIONS: [Operation; 3] = [
    Operation::DropOutdated,
    Operation::Create,
    Operation::Materialize,
];

pub fn spawn_maintenance(
    client: Client,
    database: String,
    audit: Arc<AuditLogger>,
    readiness: Arc<RumRollups>,
) {
    tokio::spawn(async move {
        loop {
            for kind in [Kind::Pages, Kind::Vitals] {
                if readiness.flag(kind).load(Ordering::Acquire) {
                    continue;
                }
                match apply(&client, &database, kind, &audit).await {
                    Ok(true) => {
                        readiness.flag(kind).store(true, Ordering::Release);
                        tracing::info!(projection = kind.name(), "RUM rollup ready");
                    }
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(%error, projection = kind.name(), "RUM rollup maintenance failed; reads remain raw");
                    }
                }
            }
            if readiness.pages.load(Ordering::Acquire) && readiness.vitals.load(Ordering::Acquire) {
                break;
            }
            // Another replica may already be materializing these parts. Poll
            // readiness without queuing duplicate mutations or delaying startup.
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    });
}

async fn apply(
    client: &Client,
    database: &str,
    kind: Kind,
    audit: &AuditLogger,
) -> anyhow::Result<bool> {
    for operation in OPERATIONS {
        let result = execute_operation(client, database, kind, operation).await;
        if matches!(&result, Ok(true) | Err(_)) {
            audit
                .log(
                    AuditEvent::new("schema.migrate", "system")
                        .resource("table", format!("{database}.rum"))
                        .outcome(if result.is_ok() { "success" } else { "failure" })
                        .changes(
                            serde_json::json!({
                                "migration": "rum_rollups",
                                "projection": kind.name(),
                                "operation": operation.name(),
                            })
                            .to_string(),
                        ),
                )
                .await;
        }
        result?;
    }
    Ok(missing_parts(client, database, kind).await? == 0)
}

async fn execute_operation(
    client: &Client,
    database: &str,
    kind: Kind,
    operation: Operation,
) -> anyhow::Result<bool> {
    let database_identifier = crate::migrations::quote_table_identifier(database)?;
    let name = kind.name();
    let ddl = match operation {
        Operation::DropOutdated | Operation::Create => {
            let definitions = client.query(
                "SELECT query FROM system.projections WHERE database = ? AND table = 'rum' AND name = ?"
            ).bind(database).bind(name).fetch_all::<String>().await?;
            match (operation, definitions.first()) {
                (Operation::Create, None) => Some(format!(
                    "ALTER TABLE {database_identifier}.rum ADD PROJECTION IF NOT EXISTS {}",
                    kind.projection_declaration()
                )),
                (Operation::DropOutdated, Some(definition)) => {
                    let normalize = |sql: &str| {
                        sql.chars()
                            .filter(|ch| !ch.is_ascii_whitespace() && !matches!(*ch, '(' | ')'))
                            .collect::<String>()
                    };
                    (normalize(definition) != normalize(&kind.projection_select())).then(|| {
                        format!(
                            "ALTER TABLE {database_identifier}.rum DROP PROJECTION IF EXISTS {name}"
                        )
                    })
                }
                _ => None,
            }
        }
        Operation::Materialize => {
            if missing_parts(client, database, kind).await? == 0 {
                return Ok(false);
            }
            let pending = client
                .query(
                    "SELECT count() FROM system.mutations WHERE database = ? AND table = 'rum' \
                 AND NOT is_done AND position(command, ?) > 0",
                )
                .bind(database)
                .bind(format!("MATERIALIZE PROJECTION {name}"))
                .fetch_one::<u64>()
                .await?;
            (pending == 0).then(|| format!(
                "ALTER TABLE {database_identifier}.rum MATERIALIZE PROJECTION {name} SETTINGS mutations_sync = 1"
            ))
        }
    };
    let Some(ddl) = ddl else {
        return Ok(false);
    };
    client.query(&ddl).execute().await?;
    Ok(true)
}

async fn missing_parts(client: &Client, database: &str, kind: Kind) -> anyhow::Result<u64> {
    Ok(client
        .query(
            "SELECT count() FROM system.parts \
         WHERE database = ? AND table = 'rum' AND active AND rows > 0 \
           AND name NOT IN (SELECT parent_name FROM system.projection_parts \
               WHERE database = ? AND table = 'rum' AND active AND name = ?)",
        )
        .bind(database)
        .bind(database)
        .bind(kind.name())
        .fetch_one::<u64>()
        .await?)
}

#[cfg(test)]
pub(super) async fn prepare(client: &Client, database: &str, kind: Kind) -> anyhow::Result<()> {
    for operation in OPERATIONS {
        execute_operation(client, database, kind, operation).await?;
    }
    Ok(())
}
