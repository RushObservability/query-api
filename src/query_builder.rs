use crate::models::query::{Filter, FilterOp};

/// Sanitize a datetime string for safe embedding in SQL string literals.
/// Restricts to characters valid in ISO 8601 / ClickHouse datetime formats,
/// preventing single-quote injection in PREWHERE time-range conditions.
pub(crate) fn sanitize_datetime(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | ':' | '.' | '+' | ' '))
        .collect()
}

/// Escape a string value for safe embedding inside a SQL single-quoted literal.
/// Escapes backslashes first (to prevent them from being interpreted as escape
/// characters when ClickHouse's allow_backslashes_escaping_in_strings is ON),
/// then uses SQL-standard quote doubling (`'` → `''`) which ClickHouse supports
/// unconditionally regardless of that setting.
///
/// Callers should still wrap the result in single quotes:
///   `format!("col = '{}'", escape_string_literal(value))`
/// Prefer parameterized queries via `.bind()` where the clickhouse driver supports
/// it; use this helper only for dynamic values that cannot be bound.
pub(crate) fn escape_string_literal(s: &str) -> String {
    // Backslash must be escaped first to avoid double-escaping the apostrophe step.
    s.replace('\\', "\\\\").replace('\'', "''")
}

/// Return true if `s` is a safe SQL column identifier (letter/underscore start,
/// followed by alphanumerics and underscores only). Rejects any injection attempt.
pub(crate) fn is_safe_column_name(s: &str) -> bool {
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
        let flat_key = escape_string_literal(attr_path);
        let flat = format!("JSONExtractString(attributes, '{flat_key}')");
        let parts: Vec<String> = attr_path.split('.').map(|p| escape_string_literal(p)).collect();
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
        match field {
            // `level` is a logs concept; in spans the equivalent is `status`
            // (values: "Ok", "Error", "Unset"). Lower-case both sides so that
            // `level=error`, `level=Error`, etc. all match correctly.
            "level" => "lower(status)".to_string(),
            _ if is_safe_column_name(field) => field.to_string(),
            _ => "NULL".to_string(),
        }
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

/// If `term` is an exact trace_id (32 hex) or span_id (16 hex), return an indexed
/// equality predicate. `trace_id = …` / `span_id = …` use the `bloom_filter(0.001)`
/// skip indexes (idx_trace_id / idx_span_id), letting ClickHouse drop nearly every
/// granule in the lookback window. Returns None for anything that isn't an exact ID.
fn id_lookup_sql(term: &str) -> Option<String> {
    let t = term.trim();
    if t.is_empty() || t.contains('*') || !t.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    match t.len() {
        32 => Some(format!("trace_id = '{}'", escape_string_literal(t))),
        16 => Some(format!("span_id = '{}'", escape_string_literal(t))),
        _ => None,
    }
}

/// The single combined free-text search expression, indexed by the native `text`
/// (inverted) index `idx_search_text` with the `ngrams(4)` tokenizer. It MUST match the
/// index DDL in migrations.rs character-for-character, or the planner can't use the
/// index and falls back to a full scan.
///
/// Why a single concatenated expression instead of `lower(attributes) LIKE … OR
/// lower(arrayStringConcat(event_attributes,' ')) LIKE …`:
/// ClickHouse can only prune granules across an `OR` when every branch resolves to the
/// SAME index. An OR of two *different* indexes prunes nothing (validated: 90/90
/// granules read). Folding both columns into one indexed expression makes the search a
/// single index probe that prunes (validated via EXPLAIN: ~17/135 granules for a needle)
/// and keeps event-attribute content searchable.
///
/// The `text(ngrams(4))` index keeps substring `LIKE '%term%'` semantics (ClickHouse
/// decomposes the pattern into 4-grams and intersects their posting lists) while
/// avoiding the bloom-filter saturation of the old ngrambf_v1 (~97k distinct 4-grams
/// per granule vs a 65536-bit filter).
pub(crate) const SEARCH_BLOB_EXPR: &str =
    "lower(concat(attributes, ' ', arrayStringConcat(event_attributes, ' ')))";

/// Generate a ClickHouse predicate for a single free-text span search term.
///
/// Index strategy — this is what makes a 7-day needle/haystack search prune granules
/// instead of full-scanning:
/// - Exact 32-hex / 16-hex terms route to `trace_id = …` / `span_id = …`, which use
///   the bloom_filter skip indexes (Idea 1: separate ID lookup from free text).
/// - Every other term searches the single `SEARCH_BLOB_EXPR`, backed by the
///   `idx_search_blob` ngrambf_v1 index, so the planner can drop granules.
///
/// Trade-off: free text no longer substring-matches service_name / span_name /
/// event_names. http/url fields are already inside `attributes` (serialized
/// SpanAttributes), so they remain searchable. service_name/span_name should be
/// queried via structured filters, which use their own bloom indexes.
fn term_match_sql(term: &str) -> String {
    // Idea 1: exact-ID fast path.
    if let Some(id_pred) = id_lookup_sql(term) {
        return id_pred;
    }

    // Free text → single index-backed LIKE pattern.
    let escaped_lower = escape_string_literal(&term.to_lowercase());
    // `*` wildcards map to `%`; literal `%`/`_` are escaped. Always wrapped in `%…%`
    // for substring semantics. ngrambf_v1 still prunes using the literal n-grams
    // between wildcards.
    let inner = escaped_lower
        .replace('%', "\\%")
        .replace('_', "\\_")
        .replace('*', "%");
    let pattern = format!("%{inner}%");

    format!("{SEARCH_BLOB_EXPR} LIKE '{pattern}'")
}

/// Generate SQL for a `key=value` attribute lookup.
/// Supports `*` wildcards in the value (e.g. `container.name=wide*`).
fn kv_match_sql(key: &str, value: &str, ctx: SearchContext) -> String {
    let ek = escape_string_literal(key);
    let ev = escape_string_literal(value);
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

/// Recursively generate SQL for a span search expression tree.
fn search_expr_to_sql(expr: &SearchExpr, ctx: SearchContext) -> String {
    match expr {
        SearchExpr::Term(term) => term_match_sql(term),
        SearchExpr::KeyValue(key, value) => kv_match_sql(key, value, ctx),
        SearchExpr::And(exprs) => {
            let parts: Vec<String> = exprs.iter().map(|e| search_expr_to_sql(e, ctx)).collect();
            format!("({})", parts.join(" AND "))
        }
        SearchExpr::Or(exprs) => {
            let parts: Vec<String> = exprs.iter().map(|e| search_expr_to_sql(e, ctx)).collect();
            format!("({})", parts.join(" OR "))
        }
    }
}

/// Build a SQL condition for free-text search on span columns (spans).
/// Free-text terms are restricted to ngrambf_v1-indexed expressions (see
/// `term_match_sql`) so a multi-day lookback can prune granules via skip indexes.
pub fn build_span_search_sql(search: &str) -> Option<String> {
    let expr = parse_search_expr(search)?;
    Some(search_expr_to_sql(&expr, SearchContext::Spans))
}

/// Build a SQL condition for free-text search on log columns (logs table).
/// Free text searches `lower(Body)` via `LIKE`, backed by the native `text` index
/// `idx_body_text` (ngrams(4)). Exact trace/span IDs route to indexed equality.
/// Map columns are searched via `key=value` syntax (direct map lookup).
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

/// Generate a ClickHouse predicate for a single free-text log search term.
///
/// Mirrors the span search strategy so a multi-day haystack scan prunes granules:
/// - Exact 32-hex / 16-hex terms route to `TraceId = …` / `SpanId = …`, using the
///   `idx_trace_id` bloom filter (Idea 1: separate ID lookup from free text).
/// - Every other term searches the single `lower(Body)` expression, backed by the
///   native `text` index `idx_body_text` (ngrams(4)) via `LIKE`. Previously this OR'd
///   in `positionCaseInsensitive(TraceId/SpanId/ServiceName/SeverityText, …)`, which are
///   non-indexed — and a granule can only be skipped when EVERY OR branch is provably
///   false, so those branches silently defeated the Body index and forced a full scan.
///
/// Trade-off: free text no longer substring-matches ServiceName / SeverityText; those
/// are queried via structured filters (which use their own bloom/set indexes).
fn log_term_match_sql(term: &str) -> String {
    // Idea 1: exact-ID fast path (TraceId is 32 hex, SpanId is 16 hex).
    let t = term.trim();
    if !t.is_empty() && !t.contains('*') && t.chars().all(|c| c.is_ascii_hexdigit()) {
        match t.len() {
            32 => return format!("TraceId = '{}'", escape_string_literal(t)),
            16 => return format!("SpanId = '{}'", escape_string_literal(t)),
            _ => {}
        }
    }

    // Free text → single index-backed LIKE on lower(Body) (matches idx_body_text).
    let escaped_lower = escape_string_literal(&term.to_lowercase());
    let inner = escaped_lower
        .replace('%', "\\%")
        .replace('_', "\\_")
        .replace('*', "%");
    format!("lower(Body) LIKE '%{inner}%'")
}

pub fn format_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => {
            let escaped = escape_string_literal(s);
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
        let safe_key = escape_string_literal(attr_key);
        format!("Attributes['{safe_key}']")
    } else if let Some(res_key) = field.strip_prefix("resource.") {
        let safe_key = escape_string_literal(res_key);
        format!("ResourceAttributes['{safe_key}']")
    } else {
        match field {
            "metric_name" | "MetricName" => "MetricName".to_string(),
            "service_name" | "ServiceName" => "ServiceName".to_string(),
            _ => if is_safe_column_name(field) { field.to_string() } else { "NULL".to_string() },
        }
    }
}

/// Build query clauses for metric tables (metrics_gauge, _sum, etc.).
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

/// Map a filter field name to the ClickHouse column expression for the logs table.
/// Log tables use Map columns: `LogAttributes['key']`, `ResourceAttributes['key']`.
fn resolve_log_field(field: &str) -> String {
    if let Some(attr_key) = field.strip_prefix("attributes.") {
        let safe_key = escape_string_literal(attr_key);
        format!("LogAttributes['{safe_key}']")
    } else if let Some(res_key) = field.strip_prefix("resource.") {
        let safe_key = escape_string_literal(res_key);
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

/// Build query clauses for the logs table. Time column is `Timestamp`.
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

/// Whitelisted time-bucket intervals (token, seconds), ascending.
/// Must stay in sync with the `interval_fn` match arms in the count/timeseries handlers.
const BUCKET_INTERVALS: &[(&str, u64)] = &[
    ("1s", 1),
    ("10s", 10),
    ("1m", 60),
    ("5m", 300),
    ("15m", 900),
    ("1h", 3600),
    ("1d", 86400),
];

/// Best-effort parse of the datetime formats accepted by the API (RFC3339, with or
/// without an explicit offset, or a plain `YYYY-MM-DD HH:MM:SS`).
fn parse_datetime_secs(s: &str) -> Option<i64> {
    let s = s.trim();
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp());
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&format!("{s}Z")) {
        return Some(dt.timestamp());
    }
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Some(naive.and_utc().timestamp());
        }
    }
    None
}

/// Clamp a client-supplied bucket interval so the expected bucket count
/// (time range / interval) stays <= `max_buckets`. The interval is untrusted:
/// a `1s` interval over 30 days would otherwise produce ~2.6M GROUP BY buckets.
///
/// - Unknown interval tokens fall back to `1m` (mirrors the handlers' default arm).
/// - If the bucket count would exceed `max_buckets`, the interval is snapped UP to
///   the smallest whitelisted interval that fits (i.e. interval >= range/max_buckets).
/// - Returns Err only on nonsensical input: a zero or negative time range.
/// - If the range cannot be parsed at all, the interval is returned unclamped and
///   ClickHouse's own parseDateTimeBestEffort handles (or rejects) the range.
pub fn clamp_bucket_interval(
    interval: &str,
    from: &str,
    to: &str,
    max_buckets: u64,
) -> Result<&'static str, String> {
    // Unknown tokens fall back to 1m, mirroring the handlers' default match arm.
    let (effective, requested_secs) = BUCKET_INTERVALS
        .iter()
        .find(|(tok, _)| *tok == interval)
        .copied()
        .unwrap_or(("1m", 60));

    let (Some(from_secs), Some(to_secs)) = (parse_datetime_secs(from), parse_datetime_secs(to)) else {
        // Unparsable range: leave as-is, the SQL layer validates the range itself.
        return Ok(effective);
    };
    let range_secs = to_secs - from_secs;
    if range_secs <= 0 {
        return Err("time range must be positive (to must be after from)".to_string());
    }

    let min_interval_secs = (range_secs as u64).div_ceil(max_buckets.max(1));
    if requested_secs >= min_interval_secs {
        return Ok(effective);
    }
    // Snap up to the smallest whitelisted interval that keeps buckets <= max_buckets.
    for (tok, secs) in BUCKET_INTERVALS {
        if *secs >= min_interval_secs {
            return Ok(tok);
        }
    }
    // Range too large even for the coarsest interval — use the coarsest.
    Ok(BUCKET_INTERVALS.last().map(|(tok, _)| *tok).unwrap_or("1d"))
}

// ── Explore keyset pagination ──

/// An opaque keyset cursor identifying the last row of a page: the row's
/// `(timestamp_nanos, span_id)`. Encoded as base64 of `"{ts}:{span_id}"` so the wire
/// token is opaque to clients; values are validated + bound/escaped before they ever
/// reach SQL (never naively interpolated).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeysetCursor {
    pub timestamp: i64,
    pub span_id: String,
}

impl KeysetCursor {
    /// Encode to the opaque base64 token returned in `next_cursor`.
    pub fn encode(&self) -> String {
        use base64::Engine;
        let raw = format!("{}:{}", self.timestamp, self.span_id);
        base64::engine::general_purpose::STANDARD.encode(raw.as_bytes())
    }

    /// Decode a client-supplied token. Rejects malformed tokens, non-numeric
    /// timestamps, and span_ids that aren't hex (so the value is always safe to embed
    /// as a SQL string literal even though we also escape it). Returns None on any
    /// invalid input — the handler then falls back to a fresh (offset 0) page rather
    /// than erroring, keeping a stale/garbage cursor non-fatal.
    pub fn decode(token: &str) -> Option<KeysetCursor> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD.decode(token.as_bytes()).ok()?;
        let s = String::from_utf8(bytes).ok()?;
        let (ts_str, span_id) = s.split_once(':')?;
        let timestamp: i64 = ts_str.parse().ok()?;
        // span_id must be hex (16 chars for OTel, but accept any hex length defensively).
        if span_id.is_empty() || !span_id.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }
        Some(KeysetCursor { timestamp, span_id: span_id.to_string() })
    }

    /// SQL predicate for "rows strictly before this cursor" under
    /// `ORDER BY timestamp DESC, span_id DESC`.
    ///
    /// The `spans.timestamp` column is `DateTime64(9)`. Comparing it directly against a
    /// bare nanosecond integer this large overflows ClickHouse's decimal arithmetic
    /// (`DECIMAL_OVERFLOW`, verified against live CH 26.1), so the cursor nanos are
    /// wrapped in `fromUnixTimestamp64Nano(...)` to produce a matching DateTime64(9).
    /// The integer is a parsed `i64` (never client text); the span_id is hex-validated
    /// at decode time AND escaped here, so the literal is injection-safe.
    pub fn before_predicate(&self) -> String {
        let span_id = escape_string_literal(&self.span_id);
        format!(
            "(timestamp < fromUnixTimestamp64Nano({ts}) OR (timestamp = fromUnixTimestamp64Nano({ts}) AND span_id < '{span_id}'))",
            ts = self.timestamp,
        )
    }
}

#[cfg(test)]
mod keyset_tests {
    use super::*;

    #[test]
    fn cursor_roundtrips() {
        let c = KeysetCursor { timestamp: 1_749_600_000_123_456_789, span_id: "a1b2c3d4e5f60718".to_string() };
        let token = c.encode();
        let decoded = KeysetCursor::decode(&token).unwrap();
        assert_eq!(decoded, c);
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(KeysetCursor::decode("not base64!!!").is_none());
        // valid base64 but wrong shape
        use base64::Engine;
        let bad = base64::engine::general_purpose::STANDARD.encode(b"no-colon-here");
        assert!(KeysetCursor::decode(&bad).is_none());
        let non_numeric = base64::engine::general_purpose::STANDARD.encode(b"abc:a1b2");
        assert!(KeysetCursor::decode(&non_numeric).is_none());
    }

    #[test]
    fn decode_rejects_non_hex_span_id() {
        use base64::Engine;
        // a span_id with a SQL-injection attempt is rejected at decode (non-hex chars).
        let inj = base64::engine::general_purpose::STANDARD.encode(b"123:' OR 1=1 --");
        assert!(KeysetCursor::decode(&inj).is_none());
    }

    #[test]
    fn before_predicate_binds_timestamp_and_escapes_span_id() {
        let c = KeysetCursor { timestamp: 42, span_id: "deadbeefcafe0001".to_string() };
        let pred = c.before_predicate();
        assert_eq!(
            pred,
            "(timestamp < fromUnixTimestamp64Nano(42) OR (timestamp = fromUnixTimestamp64Nano(42) AND span_id < 'deadbeefcafe0001'))"
        );
    }
}

#[cfg(test)]
mod search_tests {
    use super::*;

    // Idea 1: an exact 32-hex term is routed to an indexed trace_id equality lookup.
    #[test]
    fn full_trace_id_routes_to_indexed_equality() {
        let sql = build_span_search_sql("a1b2c3d4e5f6071829304a5b6c7d8e9f").unwrap();
        assert_eq!(sql, "trace_id = 'a1b2c3d4e5f6071829304a5b6c7d8e9f'");
        assert!(!sql.contains("positionCaseInsensitive"));
    }

    // Idea 1: an exact 16-hex term is routed to an indexed span_id equality lookup.
    #[test]
    fn full_span_id_routes_to_indexed_equality() {
        let sql = build_span_search_sql("a1b2c3d4e5f60718").unwrap();
        assert_eq!(sql, "span_id = 'a1b2c3d4e5f60718'");
    }

    // Free text uses the single combined SEARCH_BLOB_EXPR (one skip index), and never
    // the non-indexed columns or an OR of two indexes that would defeat granule skipping.
    #[test]
    fn free_text_uses_single_combined_index_expr() {
        let sql = build_span_search_sql("timeout").unwrap();
        assert_eq!(sql, format!("{SEARCH_BLOB_EXPR} LIKE '%timeout%'"));
        // No index-hostile predicates that would force a full scan.
        assert!(!sql.contains("positionCaseInsensitive"));
        assert!(!sql.contains("arrayExists"));
        assert!(!sql.contains("service_name"));
        assert!(!sql.contains("http_path"));
        // Not an OR of two different indexes (which cannot prune in ClickHouse).
        assert!(!sql.contains(" OR "));
    }

    // Wildcards still produce a LIKE pattern ngrambf can prune on, with no full-scan ops.
    #[test]
    fn wildcard_term_stays_index_friendly() {
        let sql = build_span_search_sql("slack*posted").unwrap();
        assert_eq!(sql, format!("{SEARCH_BLOB_EXPR} LIKE '%slack%posted%'"));
        assert!(!sql.contains("positionCaseInsensitive"));
    }

    // A non-hex / wrong-length token is treated as free text, not an ID lookup.
    #[test]
    fn non_id_token_is_free_text() {
        // 32 chars but contains a non-hex char 'z' → not an ID.
        let sql = build_span_search_sql("z1b2c3d4e5f6071829304a5b6c7d8e9f").unwrap();
        assert!(sql.contains("LIKE '%z1b2c3d4e5f6071829304a5b6c7d8e9f%'"));
        assert!(!sql.starts_with("trace_id ="));
    }

    // AND with a key=value term keeps the indexed branch (AND can still prune).
    #[test]
    fn and_with_kv_preserves_indexed_branch() {
        let sql = build_span_search_sql("error db.system=postgresql").unwrap();
        assert!(sql.contains(&format!("{SEARCH_BLOB_EXPR} LIKE '%error%'")));
        assert!(sql.contains("JSONExtractString(attributes, 'db.system') = 'postgresql'"));
    }

    // Log free text uses the single indexed lower(Body) LIKE, never the non-indexed
    // positionCaseInsensitive columns that previously defeated the Body text index.
    #[test]
    fn log_free_text_uses_only_body_index() {
        let sql = build_log_search_sql("timeout").unwrap();
        assert_eq!(sql, "lower(Body) LIKE '%timeout%'");
        assert!(!sql.contains("positionCaseInsensitive"));
        assert!(!sql.contains("ServiceName"));
        assert!(!sql.contains("hasToken"));
    }

    // Log search routes exact trace/span IDs to indexed equality.
    #[test]
    fn log_full_trace_id_routes_to_equality() {
        let sql = build_log_search_sql("a1b2c3d4e5f6071829304a5b6c7d8e9f").unwrap();
        assert_eq!(sql, "TraceId = 'a1b2c3d4e5f6071829304a5b6c7d8e9f'");
        let sql = build_log_search_sql("a1b2c3d4e5f60718").unwrap();
        assert_eq!(sql, "SpanId = 'a1b2c3d4e5f60718'");
    }
}

#[cfg(test)]
mod bucket_interval_tests {
    use super::*;

    // 1h range at 1s interval = 3600 buckets > 2000 → snaps up to 10s (360 buckets).
    #[test]
    fn snaps_interval_up_when_bucket_count_exceeds_cap() {
        let got = clamp_bucket_interval(
            "1s",
            "2026-06-10T00:00:00Z",
            "2026-06-10T01:00:00Z",
            2000,
        )
        .unwrap();
        assert_eq!(got, "10s");
    }

    // 30d range at 1s = 2.59M buckets → snaps far up (30d/2000 = 1296s → 1h).
    #[test]
    fn snaps_to_hour_for_month_range_at_one_second() {
        let got = clamp_bucket_interval(
            "1s",
            "2026-05-11T00:00:00Z",
            "2026-06-10T00:00:00Z",
            2000,
        )
        .unwrap();
        assert_eq!(got, "1h");
    }

    // Interval already coarse enough is returned unchanged.
    #[test]
    fn keeps_interval_when_within_cap() {
        let got = clamp_bucket_interval(
            "1m",
            "2026-06-10T00:00:00Z",
            "2026-06-10T06:00:00Z",
            2000,
        )
        .unwrap();
        assert_eq!(got, "1m");
    }

    // Unknown token falls back to the handlers' 1m default before clamping.
    #[test]
    fn unknown_token_defaults_to_one_minute() {
        let got = clamp_bucket_interval(
            "7m",
            "2026-06-10T00:00:00Z",
            "2026-06-10T01:00:00Z",
            2000,
        )
        .unwrap();
        assert_eq!(got, "1m");
    }

    // Zero / negative range is the only 400 case.
    #[test]
    fn rejects_non_positive_range() {
        assert!(clamp_bucket_interval("1m", "2026-06-10T01:00:00Z", "2026-06-10T01:00:00Z", 2000).is_err());
        assert!(clamp_bucket_interval("1m", "2026-06-10T02:00:00Z", "2026-06-10T01:00:00Z", 2000).is_err());
    }

    // Unparsable range strings are passed through (ClickHouse validates them later).
    #[test]
    fn unparsable_range_leaves_interval_unchanged() {
        let got = clamp_bucket_interval("1s", "not-a-date", "also-not-a-date", 2000).unwrap();
        assert_eq!(got, "1s");
    }

    // The clamp helper is shared by count_query AND timeseries_query; both handlers
    // share the same interval_fn match arms, so the tokens it can return must all be
    // recognized there. This guards that the whitelist stays in sync with both handlers.
    #[test]
    fn every_returnable_token_is_a_handler_interval_arm() {
        let handler_arms = ["1s", "10s", "1m", "5m", "15m", "1h", "1d"];
        for (tok, _) in BUCKET_INTERVALS {
            assert!(
                handler_arms.contains(tok),
                "interval token {tok} returned by clamp has no handler match arm"
            );
        }
    }

    // A range bigger than max_buckets days still resolves to the coarsest interval.
    #[test]
    fn huge_range_clamps_to_coarsest() {
        let got = clamp_bucket_interval(
            "1s",
            "2016-06-10T00:00:00Z",
            "2026-06-10T00:00:00Z",
            2000,
        )
        .unwrap();
        assert_eq!(got, "1d");
    }
}
