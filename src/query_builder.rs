use crate::models::query::{Filter, FilterOp};

/// Map a user-facing field name to the ClickHouse column expression.
/// OTel attributes use flat dotted keys (e.g. "gateway.route", "http.status_code"),
/// so we try the flat key first, falling back to nested path extraction.
pub fn resolve_field(field: &str) -> String {
    if let Some(attr_path) = field.strip_prefix("attributes.") {
        // OTel stores flat dotted keys like "gateway.route" — try flat first, then nested
        let flat = format!("JSONExtractString(attributes, '{attr_path}')");
        let parts: Vec<&str> = attr_path.split('.').collect();
        if parts.len() == 1 {
            return flat;
        }
        // COALESCE: flat key first, nested path as fallback
        let nested_args = parts
            .iter()
            .map(|p| format!("'{p}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let nested = format!("JSONExtractString(attributes, {nested_args})");
        format!("if({flat} != '', {flat}, {nested})")
    } else {
        field.to_string()
    }
}

/// Build a WHERE clause from filters, time range, and optional free-text search.
pub fn build_where_clause(filters: &[Filter], from: &str, to: &str) -> String {
    build_where_clause_with_search(filters, from, to, None)
}

/// Build a WHERE clause with optional free-text search across multiple columns.
pub fn build_where_clause_with_search(
    filters: &[Filter],
    from: &str,
    to: &str,
    search: Option<&str>,
) -> String {
    let mut conditions = vec![
        format!("timestamp >= parseDateTimeBestEffort('{from}')"),
        format!("timestamp <= parseDateTimeBestEffort('{to}')"),
    ];

    for filter in filters {
        let field = resolve_field(&filter.field);
        let condition = match &filter.op {
            FilterOp::Eq => format!("{field} = {}", format_value(&filter.value)),
            FilterOp::Ne => format!("{field} != {}", format_value(&filter.value)),
            FilterOp::Gt => format!("{field} > {}", format_value(&filter.value)),
            FilterOp::Gte => format!("{field} >= {}", format_value(&filter.value)),
            FilterOp::Lt => format!("{field} < {}", format_value(&filter.value)),
            FilterOp::Lte => format!("{field} <= {}", format_value(&filter.value)),
            FilterOp::Like => format!("{field} LIKE {}", format_value(&filter.value)),
            FilterOp::NotLike => format!("{field} NOT LIKE {}", format_value(&filter.value)),
            FilterOp::In => format!("{field} IN {}", format_array_value(&filter.value)),
            FilterOp::NotIn => format!("{field} NOT IN {}", format_array_value(&filter.value)),
        };
        conditions.push(condition);
    }

    // Free-text search across http_path, attributes, event_names, event_attributes
    if let Some(term) = search {
        let term = term.trim();
        if !term.is_empty() {
            let escaped = term.replace('\'', "\\'");
            conditions.push(format!(
                "(positionCaseInsensitive(http_path, '{escaped}') > 0 \
                 OR positionCaseInsensitive(attributes, '{escaped}') > 0 \
                 OR arrayExists(x -> positionCaseInsensitive(x, '{escaped}') > 0, event_names) \
                 OR arrayExists(x -> positionCaseInsensitive(x, '{escaped}') > 0, event_attributes))"
            ));
        }
    }

    conditions.join(" AND ")
}

pub fn format_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => {
            let escaped = s.replace('\'', "\\'");
            format!("'{escaped}'")
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => {
            if *b {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        _ => "''".to_string(),
    }
}

pub fn format_array_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_value).collect();
            format!("({})", items.join(", "))
        }
        _ => format!("({})", format_value(value)),
    }
}
