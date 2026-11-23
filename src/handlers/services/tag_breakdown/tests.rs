use super::*;
use crate::models::query::{Filter, FilterOp};
use crate::query_builder::build_where_clause_with_search;
use crate::service_tags::SCOPE_FIELD;
use serde_json::json;

fn tag_scope(selection: serde_json::Value) -> Filter {
    Filter {
        field: SCOPE_FIELD.into(),
        op: FilterOp::Eq,
        value: json!({
            "service": "payments",
            "dimension": "region",
            "selection": selection,
        })
        .to_string()
        .into(),
    }
}

#[test]
fn invalid_service_tag_scope_matches_nothing() {
    let invalid = tag_scope(json!({"kind": "other", "excluded": ["a", "b", "c", "d"]}));
    let clauses = build_where_clause_with_search(
        &[invalid],
        "2026-01-02T09:00:00.123Z",
        "2026-01-02T09:02:00.123Z",
        None,
    );
    assert_eq!(clauses.where_clause, "0");
}

#[tokio::test]
#[ignore = "requires RUSH_TEST_CLICKHOUSE_URL and permission to create a disposable database"]
async fn service_tag_buckets_and_trace_drilldown() -> anyhow::Result<()> {
    let db = format!("test_service_tags_{}", uuid::Uuid::new_v4().simple());
    let admin = clickhouse::Client::default()
        .with_url(std::env::var("RUSH_TEST_CLICKHOUSE_URL")?)
        .with_user(std::env::var("RUSH_TEST_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()))
        .with_password(std::env::var("RUSH_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default());
    admin
        .query(&format!("CREATE DATABASE {db}"))
        .execute()
        .await?;
    let result = check_tag_breakdown(&admin.clone().with_database(&db)).await;
    let cleanup = admin
        .query(&format!("DROP DATABASE {db} SYNC"))
        .execute()
        .await;
    result?;
    cleanup?;
    Ok(())
}

async fn check_tag_breakdown(client: &clickhouse::Client) -> anyhow::Result<()> {
    client
        .query(
            "CREATE TABLE spans (
                tenant_id String, timestamp DateTime64(9, 'UTC'), service_name String, trace_id String,
                parent_span_id String, kind String, duration_ns UInt64, status String,
                http_status_code UInt16, attributes String, resource_attributes String
             ) ENGINE = MergeTree ORDER BY (tenant_id, timestamp, service_name)",
        )
        .execute()
        .await?;

    let from = "2026-01-02T09:00:00.123Z";
    let to = "2026-01-02T09:02:00.123Z";
    let from_ms = DateTime::parse_from_rfc3339(from)?.timestamp_millis();
    let to_ms = DateTime::parse_from_rfc3339(to)?.timestamp_millis();
    client
        .query(&format!(
            r#"INSERT INTO spans
             SELECT tenant_id, fromUnixTimestamp64Milli({from_ms} + offset_ms), service, trace_id,
                parent_span_id, concat('SPAN_KIND_', span_kind), duration_ms * 1000000,
                status, http_status_code, attributes, resource_attributes
             FROM values(
               'tenant_id String, offset_ms Int64, service String, trace_id String, parent_span_id String, span_kind String, duration_ms UInt64, status String, http_status_code UInt16, attributes String, resource_attributes String',
               ('a', 0, 'gateway', 'gateway-trace', '', 'SERVER', 700, 'OK', 200, '{{}}', '{{}}'),
               ('a', 1, 'payments', 'gateway-trace', 'gateway', 'SERVER', 150, 'OK', 503, '{{"cloud.region":"ignored","http.route":"/charge"}}', '{{"cloud.region":"east","service.version":"1.2"}}'),
               ('a', 2, 'payments', 'gateway-trace', 'request', 'CLIENT', 1500, 'ERROR', 503, '{{"cloud.region":"ignored"}}', '{{}}'),
               ('a', 5000, 'payments', 'local', '', 'SERVER', 100, 'OK', 200, '{{"cloud.region":"east","service.version":"1.1","http.route":"/charge"}}', ''),
               ('a', 60001, 'payments', 'late', '', 'SERVER', 100, 'OK', 200, '{{}}', '{{"cloud":{{"region":"east"}}}}'),
               ('a', 10000, 'payments', 'trace-eu', '', 'SERVER', 200, 'ERROR', 200, '{{}}', '{{"cloud.region":"eu"}}'),
               ('a', 70000, 'payments', 'trace-west', '', 'SERVER', 300, 'OK', 200, '{{}}', '{{"cloud.region":"west"}}'),
               ('a', 65000, 'payments', 'trace-other', '', 'SERVER', 900, 'ERROR', 200, '{{}}', '{{"cloud.region":"z-other"}}'),
               ('a', 120000, 'payments', 'trace-empty', '', 'SERVER', 900, 'OK', 200, '{{}}', '{{}}'),
               ('a', 120001, 'payments', 'after', '', 'SERVER', 900, 'ERROR', 200, '{{"cloud.region":"east"}}', '{{}}'),
               ('a', -1, 'payments', 'before', '', 'SERVER', 900, 'ERROR', 200, '{{"cloud.region":"east"}}', '{{}}'),
               ('a', 500, 'gateway', 'cross-tenant', '', 'SERVER', 1, 'OK', 200, '{{}}', '{{}}'),
               ('b', 500, 'payments', 'cross-tenant', '', 'SERVER', 1, 'ERROR', 500, '{{"cloud.region":"east"}}', '{{}}')
             )"#
        ))
        .execute()
        .await?;

    let breakdown = |dimension| async move {
        let sql = breakdown_sql("a", "payments", dimension, from_ms, to_ms, 60);
        client
            .query(&sql)
            .fetch_all::<Aggregate>()
            .await
            .map(|rows| group_rows(60, rows))
    };

    let region = breakdown(ServiceTag::Region).await?;
    assert_eq!(region.total_requests, 7);
    assert_eq!(
        region
            .groups
            .iter()
            .map(|g| g.value.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("east"), Some("eu"), Some("west"), None],
    );
    let east = &region.groups[0];
    assert_eq!((east.requests, east.errors), (3, 1));
    assert_eq!(east.buckets.len(), 2);
    assert_eq!(east.buckets[0].timestamp_ms, from_ms);
    assert_eq!(east.buckets[1].timestamp_ms, from_ms + 60_000);
    let other = &region.groups[3];
    assert_eq!(
        (
            other.requests,
            other.errors,
            other.other_values,
            other.untagged_requests
        ),
        (2, 1, 1, 1)
    );
    assert_eq!(other.p95_ms, 900.0);
    assert_eq!(other.buckets.len(), 1);
    assert_eq!(other.buckets[0].requests, 2);

    let version = breakdown(ServiceTag::Version).await?;
    assert_eq!(version.groups[0].value.as_deref(), Some("1.1"));
    assert_eq!(version.groups[1].value.as_deref(), Some("1.2"));

    let route = breakdown(ServiceTag::Endpoint).await?;
    assert_eq!(route.groups[0].value.as_deref(), Some("/charge"));
    assert_eq!(route.groups[0].requests, 2);

    let root = Filter {
        field: "parent_span_id".into(),
        op: FilterOp::Eq,
        value: "".into(),
    };
    let check_traces = |filters: Vec<Filter>| async move {
        let clauses = build_where_clause_with_search(&filters, from, to, None)
            .with_prewhere_prefix("tenant_id = 'a'");
        let sql = format!(
            "SELECT trace_id FROM spans {} ORDER BY trace_id",
            clauses.to_sql()
        );
        client.query(&sql).fetch_all::<String>().await
    };
    let east = tag_scope(json!({"kind": "value", "value": "east"}));
    assert_eq!(
        check_traces(vec![root.clone(), east.clone()]).await?,
        vec!["gateway-trace", "late", "local"],
    );
    assert_eq!(
        check_traces(vec![east]).await?,
        vec!["gateway-trace", "late", "local"]
    );
    let other = tag_scope(json!({"kind": "other", "excluded": ["east", "eu", "west"]}));
    assert_eq!(
        check_traces(vec![root, other]).await?,
        vec!["trace-empty", "trace-other"],
    );
    Ok(())
}
