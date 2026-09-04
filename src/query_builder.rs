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

    /// Prepend a condition (e.g. `tenant_id = 'x'`) to the granule-level scope.
    ///
    /// When a PREWHERE exists, the prefix joins it. When there is NO PREWHERE — the
    /// case where the builder deliberately kept everything in WHERE so a skip index
    /// (text/bloom) isn't defeated — the prefix is prepended to WHERE instead of
    /// creating a fresh PREWHERE (which would re-defeat the index). `optimize_move_to_prewhere`
    /// promotes the tenant/time predicates to prewhere as appropriate.
    pub fn with_prewhere_prefix(&self, prefix: &str) -> Self {
        if self.prewhere.is_empty() {
            let where_clause = if self.where_clause.is_empty() {
                prefix.to_string()
            } else {
                format!("{prefix} AND {}", self.where_clause)
            };
            QueryClauses {
                prewhere: String::new(),
                where_clause,
            }
        } else {
            let prewhere = format!("{prefix} AND {}", self.prewhere);
            QueryClauses {
                prewhere,
                where_clause: self.where_clause.clone(),
            }
        }
    }

    /// Append a condition to WHERE (e.g. `Duration > threshold`).
    pub fn with_where_extra(&self, extra: &str) -> Self {
        let where_clause = if self.where_clause.is_empty() {
            extra.to_string()
        } else {
            format!("{} AND {extra}", self.where_clause)
        };
        QueryClauses {
            prewhere: self.prewhere.clone(),
            where_clause,
        }
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
        let parts: Vec<String> = attr_path
            .split('.')
            .map(|p| escape_string_literal(p))
            .collect();
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
    let time_range = format!(
        "timestamp >= parseDateTimeBestEffort('{from}') AND timestamp <= parseDateTimeBestEffort('{to}')"
    );

    // Whether the query carries a predicate backed by a skip index — a free-text
    // search (spans `idx_search_text` text index, or a trace/span-id bloom) or a
    // user LIKE on an indexed column. Such a predicate is DEFEATED by an explicit
    // PREWHERE: ClickHouse reads the entire skip index instead of using it to skip
    // granules (turning a ~150ms query into a multi-second/40GB scan). When present,
    // fold the time range into WHERE and let `optimize_move_to_prewhere` re-derive
    // the prewhere while keeping the index usable. Otherwise the explicit PREWHERE on
    // `timestamp` (leading PK column) is the efficient path.
    let mut uses_index_predicate = false;
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
        if matches!(filter.op, FilterOp::Like | FilterOp::NotLike) {
            uses_index_predicate = true;
        }
        conditions.push(condition);
    }

    // Free-text search with AND/OR boolean logic
    if let Some(term) = search {
        if let Some(sql) = build_span_search_sql(term) {
            conditions.push(sql);
            uses_index_predicate = true;
        }
    }

    if uses_index_predicate {
        let mut all = Vec::with_capacity(conditions.len() + 1);
        all.push(time_range);
        all.extend(conditions);
        QueryClauses {
            prewhere: String::new(),
            where_clause: all.join(" AND "),
        }
    } else {
        QueryClauses {
            prewhere: time_range,
            where_clause: conditions.join(" AND "),
        }
    }
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
    if input.is_empty()
        || input
            .chars()
            .any(|character| character.is_control() && !character.is_whitespace())
    {
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

/// Generate a ClickHouse predicate for a single free-text span search term.
///
/// Spans deliberately have NO full-text index. The `text(ngrams(4))` index over the
/// attribute blob cost ~66% of total span storage (8.4 GiB / 26M spans on delta) for a
/// path exercised ~2.5% of the time — untenable at high span volume. Strategy now:
/// - Exact 32-hex / 16-hex terms route to `trace_id = …` / `span_id = …`, using the
///   `idx_trace_id` / `idx_span_id` bloom filters (the dominant trace-lookup path).
/// - Every other term substring-matches the human-readable `span_name` / `service_name`
///   columns. These are `LowCardinality(String)`, so `ILIKE` evaluates against the
///   per-granule dictionary — cheap even with no skip index. Attribute *values* are
///   queried via structured `key=value` filters, not free text.
fn term_match_sql(term: &str) -> String {
    // Exact-ID fast path (trace_id is 32 hex, span_id is 16 hex).
    if let Some(id_pred) = id_lookup_sql(term) {
        return id_pred;
    }

    // Free text → substring match on the small name columns (no large index needed).
    // `*` wildcards map to `%`; literal `%`/`_` are escaped.
    let escaped_lower = escape_string_literal(&term.to_lowercase());
    let inner = escaped_lower
        .replace('%', "\\%")
        .replace('_', "\\_")
        .replace('*', "%");
    let pattern = format!("%{inner}%");

    format!("(span_name ILIKE '{pattern}' OR service_name ILIKE '{pattern}')")
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
                format!("(LogAttributes['{ek}'] = '{ev}' OR ResourceAttributes['{ek}'] = '{ev}')")
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
/// Whole words search `lower(Body)` through the native word text index using
/// `hasToken` predicates. Multi-word expressions deliberately expand to AND/OR
/// combinations of `hasToken` because ClickHouse 26.6 requires the
/// `enable_full_text_index` setting for `hasAllTokens` and `hasAnyTokens`. Wildcards use substring predicates,
/// exact trace/span IDs route to indexed equality, and map columns use `key=value`.
pub fn build_log_search_sql(search: &str) -> Option<String> {
    let expr = parse_search_expr(search)?;
    Some(log_search_expr_to_sql(&expr))
}

/// Recursively generate SQL for a log search expression.
///
/// Plain token groups expand to `hasToken` predicates instead of ClickHouse's
/// multi-token helpers. `hasAllTokens` and `hasAnyTokens` require
/// `enable_full_text_index`, which is not enabled on all supported ClickHouse versions
/// (including the supported 26.6 setup). Keeping the expression in terms of `hasToken`
/// preserves whole-word AND/OR semantics without a server setting. Mixed groups retain
/// their specialized ID, wildcard, and attribute predicates.
fn log_search_expr_to_sql(expr: &SearchExpr) -> String {
    match expr {
        SearchExpr::Term(term) => log_term_match_sql(term),
        SearchExpr::KeyValue(key, value) => kv_match_sql(key, value, SearchContext::Logs),
        SearchExpr::And(exprs) => {
            let mut tokens = Vec::new();
            let mut parts = Vec::new();

            for expr in exprs {
                match expr {
                    SearchExpr::Term(term) => match log_plain_tokens(term) {
                        Some(term_tokens) => tokens.extend(term_tokens),
                        None => parts.push(log_search_expr_to_sql(expr)),
                    },
                    _ => parts.push(log_search_expr_to_sql(expr)),
                }
            }

            if !tokens.is_empty() {
                parts.insert(0, log_token_predicate_sql(&tokens));
            }
            format!("({})", parts.join(" AND "))
        }
        SearchExpr::Or(exprs) => {
            // When every branch is a plain substring wildcard on Body (e.g. `*foo* OR *bar*`),
            // collapse the OR-of-LIKEs into a single multiSearchAny: one hyperscan-vectorized
            // pass over lower(Body) (computed once) instead of N separate LIKE evaluations.
            // Same substring semantics, and it still prunes via the
            // idx_body_ngram_g4 skip index.
            // for selective needles. Token/ID/key=value/internal-wildcard branches fall back to OR.
            //
            // Validated on 26.6.1 (ntt-japan-prod, 157k granules, needles reset/closed/refused):
            // the OR-of-LIKEs form is additionally analyzed against idx_body_text, but under OR
            // a granule survives if ANY needle might match, so for common needles both forms
            // prune ~0.3% — and the LIKE form's index analysis cost ~9× more (91s vs 10s).
            // Keep the collapse; do not dismantle it in favor of OR-of-LIKEs.
            let needles: Option<Vec<String>> = exprs.iter().map(body_substring_needle).collect();
            match needles {
                Some(ns) if ns.len() >= 2 => {
                    let arr = ns
                        .iter()
                        .map(|n| format!("'{}'", escape_string_literal(n)))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("multiSearchAny(lower(Body), [{arr}])")
                }
                _ => {
                    // `foo OR bar` can use one native direct read. Only collapse branches
                    // that each represent exactly one whole token: a quoted/multi-token
                    // branch means AND within that branch and cannot be flattened safely.
                    let tokens: Option<Vec<String>> = exprs
                        .iter()
                        .map(|expr| {
                            let SearchExpr::Term(term) = expr else {
                                return None;
                            };
                            let tokens = log_plain_tokens(term)?;
                            (tokens.len() == 1).then(|| tokens.into_iter().next().unwrap())
                        })
                        .collect();
                    if let Some(tokens) = tokens.filter(|tokens| tokens.len() >= 2) {
                        return log_any_token_predicate_sql(&tokens);
                    }

                    let parts: Vec<String> = exprs.iter().map(log_search_expr_to_sql).collect();
                    format!("({})", parts.join(" OR "))
                }
            }
        }
    }
}

/// If `expr` is a plain substring-wildcard term on the log body (e.g. `*foo*`, `foo*`,
/// `*foo`), return its literal needle, lowercased. Such terms compile to
/// `lower(Body) LIKE '%needle%'`, so an OR of them is equivalent to a single
/// `multiSearchAny(lower(Body), [needles])` — one vectorized pass instead of N LIKEs.
/// Returns None for bare token terms (which use hasToken), ID terms, key=value, empty
/// cores, or patterns with an internal wildcard (`a*b`) that aren't one literal substring.
fn body_substring_needle(expr: &SearchExpr) -> Option<String> {
    let SearchExpr::Term(term) = expr else {
        return None;
    };
    let t = term.trim();
    if !t.contains('*') {
        return None; // bare term → hasToken (whole-word), not a substring match
    }
    let lower = t.to_lowercase();
    let core = lower.trim_matches('*');
    if core.is_empty() || core.contains('*') {
        return None; // empty, or an internal-wildcard pattern (not a single literal substring)
    }
    Some(core.to_string())
}

/// Return the whole-word tokens represented by a plain log term. Wildcards and exact
/// trace/span IDs deliberately return `None` so their specialized predicates are kept.
fn log_plain_tokens(term: &str) -> Option<Vec<String>> {
    let term = term.trim();
    if term.is_empty() || term.contains('*') {
        return None;
    }
    if term.chars().all(|c| c.is_ascii_hexdigit()) && matches!(term.len(), 16 | 32) {
        return None;
    }

    let tokens: Vec<String> = term
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
        .collect();
    (!tokens.is_empty()).then_some(tokens)
}

fn log_token_predicate_sql(tokens: &[String]) -> String {
    if tokens.len() == 1 {
        format!(
            "hasToken(lower(Body), '{}')",
            escape_string_literal(&tokens[0])
        )
    } else {
        let predicates = tokens
            .iter()
            .map(|token| format!("hasToken(lower(Body), '{}')", escape_string_literal(token)))
            .collect::<Vec<_>>();
        format!("({})", predicates.join(" AND "))
    }
}

fn log_any_token_predicate_sql(tokens: &[String]) -> String {
    let predicates = tokens
        .iter()
        .map(|token| format!("hasToken(lower(Body), '{}')", escape_string_literal(token)))
        .collect::<Vec<_>>();
    format!("({})", predicates.join(" OR "))
}

/// Generate a ClickHouse predicate for a single free-text log search term.
///
/// Backed by the native `text` index `idx_body_text` on `lower(Body)`, which uses a
/// word tokenizer (`splitByNonAlpha`). Strategy:
/// - Exact 32-hex / 16-hex terms route to `TraceId = …` / `SpanId = …` (idx_trace_id
///   bloom filter) — separate ID lookup from free text.
/// - A `*` wildcard term falls back to a substring `lower(Body) LIKE` — on 26.6+
///   the LIKE pattern itself is analyzed against the text index (and the ngram
///   bloom index), so this is index-accelerated, not a full scan (see the
///   measurement note in the wildcard branch below).
/// - Every other term is split into word tokens the same way the index tokenizes
///   (`splitByNonAlpha`: maximal alphanumeric runs), and matched with `hasToken`
///   combinations. A multi-word/quoted phrase therefore matches rows containing all of
///   its words (not necessarily adjacent), without requiring `enable_full_text_index`.
///
/// Trade-offs vs the previous `ngrams(4)` + `LIKE '%term%'` approach: the index is ~6×
/// smaller and common-term scans are faster, but free text no longer substring-matches
/// inside a token (`proxy` won't match `g3proxy`) and phrases lose exact adjacency.
fn log_term_match_sql(term: &str) -> String {
    // Exact-ID fast path (TraceId is 32 hex, SpanId is 16 hex).
    let t = term.trim();
    if !t.is_empty() && !t.contains('*') && t.chars().all(|c| c.is_ascii_hexdigit()) {
        match t.len() {
            32 => return format!("TraceId = '{}'", escape_string_literal(t)),
            16 => return format!("SpanId = '{}'", escape_string_literal(t)),
            _ => {}
        }
    }

    let lower = t.to_lowercase();

    // Wildcard terms need substring semantics the token index can't provide → LIKE.
    //
    // Deliberately LIKE, not `multiSearchAny(lower(Body), ['needle'])`: measured on
    // 26.6.1 (ntt-japan-prod, 157k granules), `LIKE '%refused%'` is analyzed against
    // the idx_body_text token index (→ 26.5k granules, fast analysis) while the
    // single-needle multiSearchAny form is NOT — despite ClickHouse#106279 — and
    // falls back to idx_body_ngram_g4 only (→ 53.5k granules) with ~28s (warm) index
    // analysis. Do not "upgrade" this to multiSearchAny without re-measuring
    // EXPLAIN indexes=1 on a production-sized logs table.
    if lower.contains('*') {
        let inner = escape_string_literal(&lower)
            .replace('%', "\\%")
            .replace('_', "\\_")
            .replace('*', "%");
        return format!("lower(Body) LIKE '%{inner}%'");
    }

    let Some(tokens) = log_plain_tokens(t) else {
        // Nothing tokenizable (e.g. pure punctuation) → substring LIKE fallback.
        let inner = escape_string_literal(&lower)
            .replace('%', "\\%")
            .replace('_', "\\_");
        return format!("lower(Body) LIKE '%{inner}%'");
    };

    log_token_predicate_sql(&tokens)
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
        // A comma-separated string (e.g. "ERROR,FATAL") is the common way the UI
        // expresses an IN list — split into individual quoted literals instead of
        // matching the literal string "ERROR,FATAL" (which matches nothing).
        serde_json::Value::String(s) => {
            let items: Vec<String> = s
                .split(',')
                .map(|p| format_value(&serde_json::Value::String(p.trim().to_string())))
                .collect();
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
            _ => {
                if is_safe_column_name(field) {
                    field.to_string()
                } else {
                    "NULL".to_string()
                }
            }
        }
    }
}

/// Build query clauses for metric tables (metrics_gauge, _sum, etc.).
/// Time column is `TimeUnix`. Time range goes into PREWHERE; filters go into WHERE.
pub fn build_metrics_where_clause(filters: &[Filter], from: &str, to: &str) -> QueryClauses {
    let from = sanitize_datetime(from);
    let to = sanitize_datetime(to);
    // Compare the raw `TimeUnix` PK column (not `toDateTime(TimeUnix)`): wrapping it in
    // a function blocks primary-key granule pruning and partition pruning. CH promotes
    // the DateTime literal to DateTime64 for the comparison.
    let prewhere = format!(
        "TimeUnix >= parseDateTimeBestEffort('{from}') AND TimeUnix <= parseDateTimeBestEffort('{to}')"
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

    QueryClauses {
        prewhere,
        where_clause: conditions.join(" AND "),
    }
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
            _ => {
                if is_safe_column_name(field) {
                    field.to_string()
                } else {
                    "NULL".to_string()
                }
            }
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

    QueryClauses {
        prewhere,
        where_clause: conditions.join(" AND "),
    }
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
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
    ] {
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

    let (Some(from_secs), Some(to_secs)) = (parse_datetime_secs(from), parse_datetime_secs(to))
    else {
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
        // Avoid decoding attacker-controlled, arbitrarily large cursor strings.
        if token.len() > 256 {
            return None;
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(token.as_bytes())
            .ok()?;
        let s = String::from_utf8(bytes).ok()?;
        let (ts_str, span_id) = s.split_once(':')?;
        let timestamp: i64 = ts_str.parse().ok()?;
        // OTel span IDs are 16 hex characters. A bounded legacy range keeps
        // older imported data usable without allowing cursor amplification.
        if span_id.is_empty()
            || span_id.len() > 64
            || !span_id.chars().all(|c| c.is_ascii_hexdigit())
        {
            return None;
        }
        Some(KeysetCursor {
            timestamp,
            span_id: span_id.to_string(),
        })
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
        let c = KeysetCursor {
            timestamp: 1_749_600_000_123_456_789,
            span_id: "a1b2c3d4e5f60718".to_string(),
        };
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
    fn decode_rejects_oversized_tokens_and_span_ids() {
        use base64::Engine;
        assert!(KeysetCursor::decode(&"A".repeat(257)).is_none());
        let oversized =
            base64::engine::general_purpose::STANDARD.encode(format!("123:{}", "a".repeat(65)));
        assert!(KeysetCursor::decode(&oversized).is_none());
    }

    #[test]
    fn before_predicate_binds_timestamp_and_escapes_span_id() {
        let c = KeysetCursor {
            timestamp: 42,
            span_id: "deadbeefcafe0001".to_string(),
        };
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

    // Free text matches the small name columns (no full-text index on spans), and
    // never the big attribute blob or index-hostile ops that would force a wide scan.
    #[test]
    fn free_text_matches_name_columns() {
        let sql = build_span_search_sql("timeout").unwrap();
        assert_eq!(
            sql,
            "(span_name ILIKE '%timeout%' OR service_name ILIKE '%timeout%')"
        );
        assert!(!sql.contains("positionCaseInsensitive"));
        assert!(!sql.contains("arrayExists"));
        // Must NOT scan the attributes blob (the dropped full-text path).
        assert!(!sql.contains("concat(attributes"));
    }

    // Wildcards map to LIKE patterns on the name columns, no full-scan ops.
    #[test]
    fn wildcard_term_stays_index_friendly() {
        let sql = build_span_search_sql("slack*posted").unwrap();
        assert_eq!(
            sql,
            "(span_name ILIKE '%slack%posted%' OR service_name ILIKE '%slack%posted%')"
        );
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
        assert!(sql.contains("span_name ILIKE '%error%'"));
        assert!(sql.contains("JSONExtractString(attributes, 'db.system') = 'postgresql'"));
    }

    // Log free text uses the indexed token search on lower(Body), never the
    // non-indexed positionCaseInsensitive columns that previously defeated the index.
    #[test]
    fn log_free_text_uses_token_index() {
        // Single word → one hasToken.
        let sql = build_log_search_sql("timeout").unwrap();
        assert_eq!(sql, "hasToken(lower(Body), 'timeout')");
        assert!(!sql.contains("positionCaseInsensitive"));
        assert!(!sql.contains("ServiceName"));

        // Multi-word phrase → hasToken AND predicates (matches all words without
        // requiring enable_full_text_index).
        let sql = build_log_search_sql("\"Using passed request\"").unwrap();
        assert_eq!(
            sql,
            "(hasToken(lower(Body), 'using') AND hasToken(lower(Body), 'passed') AND hasToken(lower(Body), 'request'))"
        );
        assert!(!sql.contains("hasAllTokens"));
        assert!(!sql.contains("hasAnyTokens"));

        // Wildcards fall back to a substring LIKE — kept as LIKE deliberately:
        // on 26.6 the LIKE pattern is text-index analyzed, while single-needle
        // multiSearchAny is not (see log_term_match_sql). Not the token index.
        let sql = build_log_search_sql("time*").unwrap();
        assert_eq!(sql, "lower(Body) LIKE '%time%%'");
        assert!(!sql.contains("hasToken"));

        // Internal wildcards keep ordered `%a%b%` semantics.
        let sql = build_log_search_sql("time*out").unwrap();
        assert_eq!(sql, "lower(Body) LIKE '%time%out%'");
        assert!(!sql.contains("multiSearchAny"));
    }

    // An OR of substring wildcards collapses to ONE vectorized multiSearchAny
    // (single hyperscan pass over lower(Body)) instead of N separate LIKE scans.
    #[test]
    fn log_or_of_wildcards_collapses_to_multisearchany() {
        let sql = build_log_search_sql("*reset* OR *closed* OR *refused*").unwrap();
        assert_eq!(
            sql,
            "multiSearchAny(lower(Body), ['reset', 'closed', 'refused'])"
        );
    }

    // OR of bare tokens uses hasToken OR predicates — NOT substring multiSearchAny
    // (whole-word 'error' vs the substring 'error' are different).
    #[test]
    fn log_or_of_tokens_expands_to_has_token_or() {
        let sql = build_log_search_sql("error OR warn").unwrap();
        assert_eq!(
            sql,
            "(hasToken(lower(Body), 'error') OR hasToken(lower(Body), 'warn'))"
        );
        assert!(!sql.contains("multiSearchAny"));
        assert!(!sql.contains("hasAnyTokens"));
    }

    #[test]
    fn log_and_of_tokens_expands_to_has_token_and() {
        let sql = build_log_search_sql("connection timeout").unwrap();
        assert_eq!(
            sql,
            "((hasToken(lower(Body), 'connection') AND hasToken(lower(Body), 'timeout')))"
        );
        assert!(!sql.contains("hasAllTokens"));
    }

    // Mixed groups combine plain words into one direct read while retaining specialized
    // wildcard and attribute predicates.
    #[test]
    fn log_mixed_and_preserves_specialized_predicates() {
        let sql =
            build_log_search_sql("connection timeout *refused* db.system=postgresql").unwrap();
        assert_eq!(
            sql,
            "((hasToken(lower(Body), 'connection') AND hasToken(lower(Body), 'timeout')) AND lower(Body) LIKE '%%refused%%' AND (LogAttributes['db.system'] = 'postgresql' OR ResourceAttributes['db.system'] = 'postgresql'))"
        );
    }

    // A branch containing multiple required words cannot be flattened into a plain OR,
    // which would change `(foo AND bar) OR baz` into `foo OR bar OR baz`.
    #[test]
    fn log_or_with_multi_token_branch_preserves_grouping() {
        let sql = build_log_search_sql("\"connection timeout\" OR refused").unwrap();
        assert_eq!(
            sql,
            "((hasToken(lower(Body), 'connection') AND hasToken(lower(Body), 'timeout')) OR hasToken(lower(Body), 'refused'))"
        );
    }

    #[test]
    fn log_quoted_or_phrases_avoid_full_text_setting() {
        let sql = build_log_search_sql("\"charge declined\" OR \"refund rejected\"").unwrap();
        assert_eq!(
            sql,
            "((hasToken(lower(Body), 'charge') AND hasToken(lower(Body), 'declined')) OR (hasToken(lower(Body), 'refund') AND hasToken(lower(Body), 'rejected')))"
        );
        assert!(!sql.contains("hasAllTokens"));
        assert!(!sql.contains("hasAnyTokens"));
    }

    // A mixed OR (wildcard + bare token) does not collapse; falls back to OR.
    #[test]
    fn log_mixed_or_does_not_collapse() {
        let sql = build_log_search_sql("*reset* OR error").unwrap();
        assert!(!sql.contains("multiSearchAny"));
        assert!(sql.contains("reset") && sql.contains("LIKE"));
        assert!(sql.contains("hasToken(lower(Body), 'error')"));
    }

    // An internal-wildcard pattern (`re*set`) is not a single literal substring → no collapse.
    #[test]
    fn log_internal_wildcard_or_does_not_collapse() {
        let sql = build_log_search_sql("re*set OR *closed*").unwrap();
        assert!(!sql.contains("multiSearchAny"));
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
        let got = clamp_bucket_interval("1s", "2026-06-10T00:00:00Z", "2026-06-10T01:00:00Z", 2000)
            .unwrap();
        assert_eq!(got, "10s");
    }

    // 30d range at 1s = 2.59M buckets → snaps far up (30d/2000 = 1296s → 1h).
    #[test]
    fn snaps_to_hour_for_month_range_at_one_second() {
        let got = clamp_bucket_interval("1s", "2026-05-11T00:00:00Z", "2026-06-10T00:00:00Z", 2000)
            .unwrap();
        assert_eq!(got, "1h");
    }

    // Interval already coarse enough is returned unchanged.
    #[test]
    fn keeps_interval_when_within_cap() {
        let got = clamp_bucket_interval("1m", "2026-06-10T00:00:00Z", "2026-06-10T06:00:00Z", 2000)
            .unwrap();
        assert_eq!(got, "1m");
    }

    // Unknown token falls back to the handlers' 1m default before clamping.
    #[test]
    fn unknown_token_defaults_to_one_minute() {
        let got = clamp_bucket_interval("7m", "2026-06-10T00:00:00Z", "2026-06-10T01:00:00Z", 2000)
            .unwrap();
        assert_eq!(got, "1m");
    }

    // Zero / negative range is the only 400 case.
    #[test]
    fn rejects_non_positive_range() {
        assert!(
            clamp_bucket_interval("1m", "2026-06-10T01:00:00Z", "2026-06-10T01:00:00Z", 2000)
                .is_err()
        );
        assert!(
            clamp_bucket_interval("1m", "2026-06-10T02:00:00Z", "2026-06-10T01:00:00Z", 2000)
                .is_err()
        );
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
        let got = clamp_bucket_interval("1s", "2016-06-10T00:00:00Z", "2026-06-10T00:00:00Z", 2000)
            .unwrap();
        assert_eq!(got, "1d");
    }
}

#[cfg(test)]
mod adversarial_search_parser_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(192))]

        #[test]
        fn arbitrary_filter_text_never_panics_or_builds_unbounded_sql(input in ".{0,512}") {
            for sql in [build_log_search_sql(&input), build_span_search_sql(&input)]
                .into_iter()
                .flatten()
            {
                // Each input byte expands through a fixed number of predicates
                // and escaping characters; guard against accidental exponential
                // parser expansion.
                prop_assert!(sql.len() <= input.len().saturating_mul(48).saturating_add(1024));
                prop_assert!(!sql.contains('\0'));
            }
        }

        #[test]
        fn arbitrary_cursor_tokens_never_panic_or_escape_the_sql_literal(token in ".{0,300}") {
            if let Some(cursor) = KeysetCursor::decode(&token) {
                prop_assert!(cursor.span_id.len() <= 64);
                prop_assert!(cursor.span_id.chars().all(|c| c.is_ascii_hexdigit()));
                let predicate = cursor.before_predicate();
                prop_assert!(!predicate.contains("--"));
                prop_assert!(!predicate.contains("/*"));
            }
        }
    }
}
