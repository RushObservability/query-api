use crate::models::query::{Filter, FilterOp};

/// Sanitize a datetime string for safe embedding in SQL string literals.
/// Restricts to characters valid in ISO 8601 / ClickHouse datetime formats,
/// preventing single-quote injection in PREWHERE time-range conditions.
fn sanitize_datetime(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | ':' | '.' | '+' | ' '))
        .collect()
}

/// Return true if `s` is a safe SQL column identifier (letter/underscore start,
/// followed by alphanumerics and underscores only). Rejects any injection attempt.
fn is_safe_column_name(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Split SQL clauses for ClickHouse PREWHERE optimization.
///
/// `prewhere` holds conditions evaluated at the granule level before reading column data
/// (time ranges, low-cardinality keys like tenant_id). `where_clause` holds the remaining
/// conditions evaluated after decompression.
pub struct QueryClauses {
    pub prewhere: String,
    pub where_clause: String,
}

impl QueryClauses {
    /// Format as `PREWHERE x WHERE y`. Omits either part if empty.
    pub fn to_sql(&self) -> String {
        match (self.prewhere.is_empty(), self.where_clause.is_empty()) {
            (true, true) => String::new(),
            (true, false) => format!("WHERE {}", self.where_clause),
            (false, true) => format!("PREWHERE {}", self.prewhere),
            (false, false) => format!("PREWHERE {} WHERE {}", self.prewhere, self.where_clause),
        }
    }

    /// Returns `"PREWHERE x"` or `""` if prewhere is empty — for use with ARRAY JOIN.
    pub fn prewhere_sql(&self) -> String {
        if self.prewhere.is_empty() {
            String::new()
        } else {
            format!("PREWHERE {}", self.prewhere)
        }
    }

    /// Returns `"WHERE w AND extra"` (or `"WHERE extra"` if where_clause is empty) — for
    /// use when additional conditions must be appended after an ARRAY JOIN.
    pub fn where_with_extra(&self, extra: &str) -> String {
        match (self.where_clause.is_empty(), extra.is_empty()) {
            (_, true) => self.to_sql(),
            (true, false) => format!("WHERE {extra}"),
            (false, false) => format!("WHERE {} AND {extra}", self.where_clause),
        }
    }

    /// Prepend a condition to PREWHERE (e.g. `tenant_id = 'x'`).
    pub fn with_prewhere_prefix(&self, prefix: &str) -> Self {
        let prewhere = if self.prewhere.is_empty() {
            prefix.to_string()
        } else {
            format!("{prefix} AND {}", self.prewhere)
        };
        QueryClauses { prewhere, where_clause: self.where_clause.clone() }
    }

    /// Append a condition to WHERE (e.g. `Duration > threshold`).
    pub fn with_where_extra(&self, extra: &str) -> Self {
        let where_clause = if self.where_clause.is_empty() {
            extra.to_string()
        } else {
            format!("{} AND {extra}", self.where_clause)
        };
        QueryClauses { prewhere: self.prewhere.clone(), where_clause }
    }
}

/// Map a user-facing field name to the ClickHouse column expression.
/// OTel attributes use flat dotted keys (e.g. "gateway.route", "http.status_code"),
/// so we try the flat key first, falling back to nested path extraction.
pub fn resolve_field(field: &str) -> String {
    if let Some(attr_path) = field.strip_prefix("attributes.") {
        // Escape single quotes in every path segment to prevent SQL injection
        let flat_key = attr_path.replace('\'', "\\'");
        let flat = format!("JSONExtractString(attributes, '{flat_key}')");
        let parts: Vec<String> = attr_path.split('.').map(|p| p.replace('\'', "\\'")).collect();
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
    } else if is_safe_column_name(field) {
        field.to_string()
    } else {
        // Unknown field — return NULL so the condition is always false/NULL-safe
        "NULL".to_string()
    }
}

/// Build query clauses from filters, time range, and optional free-text search.
/// Time range goes into PREWHERE for efficient granule skipping; filters go into WHERE.
pub fn build_where_clause(filters: &[Filter], from: &str, to: &str) -> QueryClauses {
    build_where_clause_with_search(filters, from, to, None)
}

/// Build query clauses with optional free-text search across multiple columns.
/// Time range goes into PREWHERE for efficient granule skipping; filters+search go into WHERE.
pub fn build_where_clause_with_search(
    filters: &[Filter],
    from: &str,
    to: &str,
    search: Option<&str>,
) -> QueryClauses {
    let from = sanitize_datetime(from);
    let to = sanitize_datetime(to);
    let prewhere = format!(
        "timestamp >= parseDateTimeBestEffort('{from}') AND timestamp <= parseDateTimeBestEffort('{to}')"
    );

    let mut conditions = Vec::new();

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

    QueryClauses { prewhere, where_clause: conditions.join(" AND ") }
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
    KeyValue(String, String), // key=value attribute lookup
    And(Vec<SearchExpr>),
    Or(Vec<SearchExpr>),
}

/// Context for SQL generation — different tables have different attribute column shapes.
#[derive(Debug, Clone, Copy)]
enum SearchContext {
    Spans, // attributes is JSON string, no ResourceAttributes
    Logs,  // LogAttributes & ResourceAttributes are Map columns
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

    // Convert a token to a Term or KeyValue expression
    let token_to_expr = |tok: String| -> SearchExpr {
        if let Some((key, value)) = tok.split_once('=') {
            let key = key.trim();
            let value = value.trim();
            if !key.is_empty() && !value.is_empty() {
                return SearchExpr::KeyValue(key.to_string(), value.to_string());
            }
        }
        SearchExpr::Term(tok)
    };

    // Convert groups to expressions
    let or_parts: Vec<SearchExpr> = and_groups
        .into_iter()
        .map(|group| {
            if group.len() == 1 {
                token_to_expr(group.into_iter().next().unwrap())
            } else {
                SearchExpr::And(group.into_iter().map(token_to_expr).collect())
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

/// Generate SQL for a `key=value` attribute lookup.
/// Supports `*` wildcards in the value (e.g. `container.name=wide*`).
fn kv_match_sql(key: &str, value: &str, ctx: SearchContext) -> String {
    let ek = key.replace('\'', "\\'");
    let ev = value.replace('\'', "\\'");
    let has_wildcard = value.contains('*');

    match ctx {
        SearchContext::Logs => {
            if has_wildcard {
                let pattern = ev.replace('%', "\\%").replace('_', "\\_").replace('*', "%");
                format!(
                    "(LogAttributes['{ek}'] ILIKE '{pattern}' OR ResourceAttributes['{ek}'] ILIKE '{pattern}')"
                )
            } else {
                format!(
                    "(LogAttributes['{ek}'] = '{ev}' OR ResourceAttributes['{ek}'] = '{ev}')"
                )
            }
        }
        SearchContext::Spans => {
            if has_wildcard {
                let pattern = ev.replace('%', "\\%").replace('_', "\\_").replace('*', "%");
                format!("JSONExtractString(attributes, '{ek}') ILIKE '{pattern}'")
            } else {
                format!("JSONExtractString(attributes, '{ek}') = '{ev}'")
            }
        }
    }
}

/// Recursively generate SQL for a search expression tree.
fn search_expr_to_sql(expr: &SearchExpr, columns: &[(&str, bool)], ctx: SearchContext) -> String {
    match expr {
        SearchExpr::Term(term) => term_match_sql(term, columns),
        SearchExpr::KeyValue(key, value) => kv_match_sql(key, value, ctx),
        SearchExpr::And(exprs) => {
            let parts: Vec<String> = exprs.iter().map(|e| search_expr_to_sql(e, columns, ctx)).collect();
            format!("({})", parts.join(" AND "))
        }
        SearchExpr::Or(exprs) => {
            let parts: Vec<String> = exprs.iter().map(|e| search_expr_to_sql(e, columns, ctx)).collect();
            format!("({})", parts.join(" OR "))
        }
    }
}

/// Build a SQL condition for free-text search on span columns (events table).
pub fn build_span_search_sql(search: &str) -> Option<String> {
    let expr = parse_search_expr(search)?;
    let columns: Vec<(&str, bool)> = vec![
        ("trace_id", false),
        ("span_id", false),
        ("service_name", false),
        ("http_path", false),
        ("attributes", false),
        ("event_names", true),
        ("event_attributes", true),
    ];
    Some(search_expr_to_sql(&expr, &columns, SearchContext::Spans))
}

/// Build a SQL condition for free-text search on log columns (otel_logs table).
/// Body uses hasToken(lower(Body), ...) to leverage the tokenbf_v1 skip index,
/// falling back to lower(Body) LIKE ... for wildcard/substring searches (ngrambf_v1).
/// Other scalar columns use positionCaseInsensitive (short/LowCardinality, always fast).
/// Map columns are skipped — users search them via `key=value` syntax (direct map lookup).
pub fn build_log_search_sql(search: &str) -> Option<String> {
    let expr = parse_search_expr(search)?;
    Some(log_search_expr_to_sql(&expr))
}

/// Recursively generate SQL for a log search expression, using hasToken for Body.
fn log_search_expr_to_sql(expr: &SearchExpr) -> String {
    match expr {
        SearchExpr::Term(term) => log_term_match_sql(term),
        SearchExpr::KeyValue(key, value) => kv_match_sql(key, value, SearchContext::Logs),
        SearchExpr::And(exprs) => {
            let parts: Vec<String> = exprs.iter().map(log_search_expr_to_sql).collect();
            format!("({})", parts.join(" AND "))
        }
        SearchExpr::Or(exprs) => {
            let parts: Vec<String> = exprs.iter().map(log_search_expr_to_sql).collect();
            format!("({})", parts.join(" OR "))
        }
    }
}

/// Generate SQL for a single search term on log columns.
/// Body: hasToken(lower(Body), 'token') for each alphanumeric sub-token (uses tokenbf_v1).
/// Body wildcard: lower(Body) LIKE '%pattern%' (uses ngrambf_v1).
/// Other columns: positionCaseInsensitive (short strings, always fast).
fn log_term_match_sql(term: &str) -> String {
    let has_wildcard = term.contains('*');
    let escaped = term.replace('\'', "\\'");

    let other_cols = ["TraceId", "SpanId", "ServiceName", "SeverityText"];

    if has_wildcard {
        let pattern = escaped
            .replace('%', "\\%")
            .replace('_', "\\_")
            .replace('*', "%");
        let like_pattern = format!("%{pattern}%");
        let lower_pattern = like_pattern.to_lowercase();

        let mut parts: Vec<String> = other_cols
            .iter()
            .map(|col| format!("{col} ILIKE '{like_pattern}'"))
            .collect();
        // lower(Body) LIKE leverages ngrambf_v1 index
        parts.push(format!("lower(Body) LIKE '{lower_pattern}'"));
        format!("({})", parts.join(" OR "))
    } else {
        let mut parts: Vec<String> = other_cols
            .iter()
            .map(|col| format!("positionCaseInsensitive({col}, '{escaped}') > 0"))
            .collect();

        // Extract alphanumeric tokens for hasToken (matches tokenbf_v1 tokenisation)
        let lower_term = escaped.to_lowercase();
        let tokens: Vec<&str> = lower_term
            .split(|c: char| !c.is_alphanumeric())
            .filter(|t| !t.is_empty())
            .collect();

        if tokens.is_empty() {
            // No alphanumeric content — fall back to LIKE
            parts.push(format!("lower(Body) LIKE '%{lower_term}%'"));
        } else if tokens.len() == 1 {
            parts.push(format!("hasToken(lower(Body), '{}')", tokens[0]));
        } else {
            // Multi-token term (e.g. UUID with hyphens): hasToken matches whole tokens
            // only, so "f7d156a" won't match "2f7d156a". Use LIKE for the full term
            // so substring matches within tokens are found correctly.
            let lower_like = lower_term.replace('%', "\\%").replace('_', "\\_");
            parts.push(format!("lower(Body) LIKE '%{lower_like}%'"));
        }

        format!("({})", parts.join(" OR "))
    }
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
        let safe_key = attr_key.replace('\'', "\\'");
        format!("Attributes['{safe_key}']")
    } else if let Some(res_key) = field.strip_prefix("resource.") {
        let safe_key = res_key.replace('\'', "\\'");
        format!("ResourceAttributes['{safe_key}']")
    } else {
        match field {
            "metric_name" | "MetricName" => "MetricName".to_string(),
            "service_name" | "ServiceName" => "ServiceName".to_string(),
            _ => if is_safe_column_name(field) { field.to_string() } else { "NULL".to_string() },
        }
    }
}

/// Build query clauses for metric tables (otel_metrics_gauge, _sum, etc.).
/// Time column is `TimeUnix`. Time range goes into PREWHERE; filters go into WHERE.
pub fn build_metrics_where_clause(filters: &[Filter], from: &str, to: &str) -> QueryClauses {
    let from = sanitize_datetime(from);
    let to = sanitize_datetime(to);
    let prewhere = format!(
        "toDateTime(TimeUnix) >= parseDateTimeBestEffort('{from}') AND toDateTime(TimeUnix) <= parseDateTimeBestEffort('{to}')"
    );

    let mut conditions = Vec::new();

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

    QueryClauses { prewhere, where_clause: conditions.join(" AND ") }
}

// ── Logs query builder ──

/// Map a filter field name to the ClickHouse column expression for the otel_logs table.
/// Log tables use Map columns: `LogAttributes['key']`, `ResourceAttributes['key']`.
fn resolve_log_field(field: &str) -> String {
    if let Some(attr_key) = field.strip_prefix("attributes.") {
        let safe_key = attr_key.replace('\'', "\\'");
        format!("LogAttributes['{safe_key}']")
    } else if let Some(res_key) = field.strip_prefix("resource.") {
        let safe_key = res_key.replace('\'', "\\'");
        format!("ResourceAttributes['{safe_key}']")
    } else {
        match field {
            "service_name" | "ServiceName" => "ServiceName".to_string(),
            "severity" | "SeverityText" => "SeverityText".to_string(),
            "body" | "Body" => "Body".to_string(),
            _ => if is_safe_column_name(field) { field.to_string() } else { "NULL".to_string() },
        }
    }
}

/// Build query clauses for the otel_logs table. Time column is `Timestamp`.
/// Time range goes into PREWHERE; filters go into WHERE.
pub fn build_logs_where_clause(filters: &[Filter], from: &str, to: &str) -> QueryClauses {
    let from = sanitize_datetime(from);
    let to = sanitize_datetime(to);
    let prewhere = format!(
        "Timestamp >= parseDateTimeBestEffort('{from}') AND Timestamp <= parseDateTimeBestEffort('{to}')"
    );

    let mut conditions = Vec::new();

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

    QueryClauses { prewhere, where_clause: conditions.join(" AND ") }
}
