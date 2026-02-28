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

    // Free-text search with AND/OR boolean logic
    if let Some(term) = search {
        if let Some(sql) = build_span_search_sql(term) {
            conditions.push(sql);
        }
    }

    conditions.join(" AND ")
}

/// A parsed search expression supporting AND/OR boolean logic.
/// Default operator between terms is AND.  OR must be explicit.
///
/// Examples:
///   "pool OR response"      → OR(pool, response)
///   "pool response"         → AND(pool, response)
///   "pool AND response"     → AND(pool, response)
///   "error OR warn gateway" → AND(OR(error, warn), gateway)
#[derive(Debug)]
enum SearchExpr {
    Term(String),
    And(Vec<SearchExpr>),
    Or(Vec<SearchExpr>),
}

/// Tokenize a search string, keeping double-quoted phrases as single tokens.
/// e.g. `"slack message posted" OR error` → ["slack message posted", "OR", "error"]
fn tokenize_search(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }
        if ch == '"' {
            // Consume opening quote
            chars.next();
            let mut phrase = String::new();
            while let Some(&c) = chars.peek() {
                if c == '"' {
                    chars.next(); // consume closing quote
                    break;
                }
                phrase.push(c);
                chars.next();
            }
            if !phrase.is_empty() {
                tokens.push(phrase);
            }
        } else {
            // Regular word token
            let mut word = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_whitespace() || c == '"' {
                    break;
                }
                word.push(c);
                chars.next();
            }
            if !word.is_empty() {
                tokens.push(word);
            }
        }
    }

    tokens
}

/// Parse a search string into a boolean expression tree.
fn parse_search_expr(input: &str) -> Option<SearchExpr> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }

    // Tokenize: split on whitespace but keep quoted phrases as single tokens.
    // e.g. `"slack message posted" OR error` → ["slack message posted", "OR", "error"]
    let tokens = tokenize_search(input);
    if tokens.is_empty() {
        return None;
    }

    // Group into OR-separated AND-groups:
    // "a b OR c d" → AND(a, b) OR AND(c, d)
    let mut and_groups: Vec<Vec<String>> = vec![vec![]];
    for token in &tokens {
        if token.eq_ignore_ascii_case("OR") {
            and_groups.push(vec![]);
        } else if token.eq_ignore_ascii_case("AND") {
            // explicit AND — just continue the current group
        } else {
            if let Some(group) = and_groups.last_mut() {
                group.push(token.clone());
            }
        }
    }

    // Remove empty groups
    and_groups.retain(|g| !g.is_empty());
    if and_groups.is_empty() {
        return None;
    }

    // Convert groups to expressions
    let or_parts: Vec<SearchExpr> = and_groups
        .into_iter()
        .map(|group| {
            if group.len() == 1 {
                SearchExpr::Term(group.into_iter().next().unwrap())
            } else {
                SearchExpr::And(group.into_iter().map(SearchExpr::Term).collect())
            }
        })
        .collect();

    if or_parts.len() == 1 {
        Some(or_parts.into_iter().next().unwrap())
    } else {
        Some(SearchExpr::Or(or_parts))
    }
}

/// Generate a ClickHouse SQL fragment that checks if `term` appears in any of the given columns.
/// Supports `*` wildcards: `slack * posted` → ILIKE '%slack%posted%'.
/// For array columns, wraps with arrayExists.
fn term_match_sql(term: &str, columns: &[(&str, bool)]) -> String {
    let has_wildcard = term.contains('*');
    let escaped = term.replace('\'', "\\'");

    if has_wildcard {
        // Convert wildcard term to ILIKE pattern: escape %, _, then replace * with %
        let pattern = escaped
            .replace('%', "\\%")
            .replace('_', "\\_")
            .replace('*', "%");
        let like_pattern = format!("%{pattern}%");
        let parts: Vec<String> = columns
            .iter()
            .map(|(col, is_array)| {
                if *is_array {
                    format!("arrayExists(x -> x ILIKE '{like_pattern}', {col})")
                } else {
                    format!("{col} ILIKE '{like_pattern}'")
                }
            })
            .collect();
        format!("({})", parts.join(" OR "))
    } else {
        let parts: Vec<String> = columns
            .iter()
            .map(|(col, is_array)| {
                if *is_array {
                    format!("arrayExists(x -> positionCaseInsensitive(x, '{escaped}') > 0, {col})")
                } else {
                    format!("positionCaseInsensitive({col}, '{escaped}') > 0")
                }
            })
            .collect();
        format!("({})", parts.join(" OR "))
    }
}

/// Recursively generate SQL for a search expression tree.
fn search_expr_to_sql(expr: &SearchExpr, columns: &[(&str, bool)]) -> String {
    match expr {
        SearchExpr::Term(term) => term_match_sql(term, columns),
        SearchExpr::And(exprs) => {
            let parts: Vec<String> = exprs.iter().map(|e| search_expr_to_sql(e, columns)).collect();
            format!("({})", parts.join(" AND "))
        }
        SearchExpr::Or(exprs) => {
            let parts: Vec<String> = exprs.iter().map(|e| search_expr_to_sql(e, columns)).collect();
            format!("({})", parts.join(" OR "))
        }
    }
}

/// Build a SQL condition for free-text search on span columns (events table).
pub fn build_span_search_sql(search: &str) -> Option<String> {
    let expr = parse_search_expr(search)?;
    let columns: Vec<(&str, bool)> = vec![
        ("http_path", false),
        ("attributes", false),
        ("event_names", true),
        ("event_attributes", true),
    ];
    Some(search_expr_to_sql(&expr, &columns))
}

/// Build a SQL condition for free-text search on log columns (otel_logs table).
pub fn build_log_search_sql(search: &str) -> Option<String> {
    let expr = parse_search_expr(search)?;
    let columns: Vec<(&str, bool)> = vec![
        ("Body", false),
        ("toString(LogAttributes)", false),
    ];
    Some(search_expr_to_sql(&expr, &columns))
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

// ── Metrics query builder ──

/// Map a filter field name to the ClickHouse column expression for metric tables.
/// Metric tables use Map columns: `Attributes['key']`, `ResourceAttributes['key']`.
fn resolve_metric_field(field: &str) -> String {
    if let Some(attr_key) = field.strip_prefix("attributes.") {
        format!("Attributes['{attr_key}']")
    } else if let Some(res_key) = field.strip_prefix("resource.") {
        format!("ResourceAttributes['{res_key}']")
    } else {
        match field {
            "metric_name" | "MetricName" => "MetricName".to_string(),
            "service_name" | "ServiceName" => "ServiceName".to_string(),
            _ => field.to_string(),
        }
    }
}

/// Build a WHERE clause for metric tables (otel_metrics_gauge, _sum, etc.).
/// Time column is `TimeUnix`.
pub fn build_metrics_where_clause(filters: &[Filter], from: &str, to: &str) -> String {
    let mut conditions = vec![
        format!("toDateTime(TimeUnix) >= parseDateTimeBestEffort('{from}')"),
        format!("toDateTime(TimeUnix) <= parseDateTimeBestEffort('{to}')"),
    ];

    for filter in filters {
        let field = resolve_metric_field(&filter.field);
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

    conditions.join(" AND ")
}

// ── Logs query builder ──

/// Map a filter field name to the ClickHouse column expression for the otel_logs table.
/// Log tables use Map columns: `LogAttributes['key']`, `ResourceAttributes['key']`.
fn resolve_log_field(field: &str) -> String {
    if let Some(attr_key) = field.strip_prefix("attributes.") {
        format!("LogAttributes['{attr_key}']")
    } else if let Some(res_key) = field.strip_prefix("resource.") {
        format!("ResourceAttributes['{res_key}']")
    } else {
        match field {
            "service_name" | "ServiceName" => "ServiceName".to_string(),
            "severity" | "SeverityText" => "SeverityText".to_string(),
            "body" | "Body" => "Body".to_string(),
            _ => field.to_string(),
        }
    }
}

/// Build a WHERE clause for the otel_logs table. Time column is `Timestamp`.
pub fn build_logs_where_clause(filters: &[Filter], from: &str, to: &str) -> String {
    let mut conditions = vec![
        format!("Timestamp >= parseDateTimeBestEffort('{from}')"),
        format!("Timestamp <= parseDateTimeBestEffort('{to}')"),
    ];

    for filter in filters {
        let field = resolve_log_field(&filter.field);
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

    conditions.join(" AND ")
}
