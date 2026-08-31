//! Fail-closed compiler for tenant-scoped detection queries.
//!
//! Detection rules intentionally support a small ClickHouse SELECT subset. The
//! submitted template is parsed into an AST, checked structurally, and only then
//! receives a tenant predicate. Unsupported syntax is rejected rather than
//! forwarded to ClickHouse.

use std::ops::ControlFlow;

use clickhouse::Client;
use sqlparser::{
    ast::{
        BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
        GroupByExpr, Ident, LimitClause, ObjectNamePart, OrderByKind, Query, Select, SelectFlavor,
        SelectItem, SetExpr, Statement, TableFactor, UnaryOperator, Value, visit_expressions,
    },
    dialect::ClickHouseDialect,
    parser::Parser,
};

const MAX_LIMIT: u64 = 10_000;
const MAX_EXECUTION_SECONDS: &str = "10";
const MAX_ROWS_TO_READ: &str = "5000000";
const MAX_BYTES_TO_READ: &str = "536870912";
const MAX_MEMORY_USAGE: &str = "536870912";
const MAX_THREADS: &str = "4";

const ALLOWED_TABLES: &[&str] = &[
    "logs",
    "spans",
    "metrics_gauge",
    "metrics_sum",
    "metrics_histogram",
    "metrics_exp_histogram",
    "metrics_summary",
    "rum",
];

// Deliberately finite. New ClickHouse functions must be reviewed before they
// become part of the detection-query language.
const ALLOWED_FUNCTIONS: &[&str] = &[
    "abs",
    "avg",
    "avgif",
    "coalesce",
    "count",
    "countif",
    "endswith",
    "greatest",
    "has",
    "if",
    "least",
    "length",
    "lower",
    "max",
    "maxif",
    "min",
    "minif",
    "nullif",
    "position",
    "quantile",
    "quantileexact",
    "round",
    "startswith",
    "sum",
    "sumif",
    "todate",
    "todatetime",
    "todatetime64",
    "tofloat64",
    "toint64",
    "touint64",
    "uniq",
    "uniqexact",
    "uniqif",
    "upper",
];

#[derive(Debug, thiserror::Error)]
pub enum DetectionQueryError {
    #[error("{0}")]
    Invalid(String),
    #[error("detection query execution failed")]
    Execution(#[source] clickhouse::error::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledDetectionQuery {
    pub scoped_sql: String,
    pub count_sql: String,
}

fn invalid(message: impl Into<String>) -> DetectionQueryError {
    DetectionQueryError::Invalid(message.into())
}

/// Validate a stored rule template without executing it.
pub fn validate_template(query_sql: &str) -> Result<(), DetectionQueryError> {
    compile_count_query(
        query_sql,
        "validation-tenant",
        "2000-01-01 00:00:00",
        "2000-01-01 00:05:00",
    )?;
    Ok(())
}

/// Compile a rule template into the exact tenant-scoped count query used by
/// both preview and scheduled evaluation.
pub fn compile_count_query(
    query_sql: &str,
    tenant_id: &str,
    window_start: &str,
    window_end: &str,
) -> Result<CompiledDetectionQuery, DetectionQueryError> {
    let expanded = expand_window_placeholders(query_sql, window_start, window_end)?;
    let dialect = ClickHouseDialect {};
    let (mut statements, comments) = Parser::parse_sql_with_comments(&dialect, &expanded)
        .map_err(|_| invalid("query_sql is not valid supported ClickHouse SQL"))?;

    if comments.find(..).next().is_some() {
        return Err(invalid("SQL comments are not supported"));
    }
    if statements.len() != 1 {
        return Err(invalid(
            "query_sql must contain exactly one SELECT statement",
        ));
    }

    let statement = statements
        .pop()
        .ok_or_else(|| invalid("query_sql must contain one SELECT statement"))?;
    let mut query = match statement {
        Statement::Query(query) => query,
        _ => return Err(invalid("query_sql must be a SELECT statement")),
    };

    validate_query(&query)?;
    inject_tenant_predicate(&mut query, tenant_id)?;

    let scoped_sql = query.to_string();
    let count_sql = format!("SELECT count() AS _siem_count FROM ({scoped_sql}) AS _siem_sub");
    Ok(CompiledDetectionQuery {
        scoped_sql,
        count_sql,
    })
}

/// Execute a compiled query with detection-specific limits. The underlying
/// client is already the SELECT-only, row-policy-protected principal and
/// `tenant_query` adds the tenant setting used by the ClickHouse row policy.
pub async fn execute_count(
    ch: &Client,
    compiled: &CompiledDetectionQuery,
    tenant_id: &str,
) -> Result<u64, DetectionQueryError> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct CountRow {
        #[serde(rename = "_siem_count")]
        count: u64,
    }

    let row = crate::tenant_query(ch, &compiled.count_sql, tenant_id)
        .with_option("max_execution_time", MAX_EXECUTION_SECONDS)
        .with_option("max_rows_to_read", MAX_ROWS_TO_READ)
        .with_option("max_bytes_to_read", MAX_BYTES_TO_READ)
        .with_option("max_memory_usage", MAX_MEMORY_USAGE)
        .with_option("max_threads", MAX_THREADS)
        .with_option("max_result_rows", "1")
        .with_option("result_overflow_mode", "throw")
        .fetch_one::<CountRow>()
        .await
        .map_err(DetectionQueryError::Execution)?;
    Ok(row.count)
}

fn expand_window_placeholders(
    query_sql: &str,
    window_start: &str,
    window_end: &str,
) -> Result<String, DetectionQueryError> {
    if query_sql.trim().is_empty() {
        return Err(invalid("query_sql cannot be empty"));
    }

    let start = sql_string_literal(window_start);
    let end = sql_string_literal(window_end);
    let mut output = String::with_capacity(query_sql.len() + start.len() + end.len());
    let mut index = 0;
    let mut quote: Option<char> = None;

    while index < query_sql.len() {
        let rest = &query_sql[index..];
        let character = rest
            .chars()
            .next()
            .ok_or_else(|| invalid("query_sql could not be scanned"))?;
        let width = character.len_utf8();
        if let Some(active_quote) = quote {
            if character == '@'
                && (placeholder_at(rest, "@window_start") || placeholder_at(rest, "@window_end"))
            {
                return Err(invalid(
                    "window placeholders cannot appear inside quoted values",
                ));
            }
            if character == active_quote {
                let doubled = rest[width..].starts_with(active_quote);
                if doubled {
                    output.push(character);
                    output.push(character);
                    index += width * 2;
                    continue;
                }
                quote = None;
            }
            output.push(character);
            index += width;
            continue;
        }

        if matches!(character, '\'' | '"' | '`') {
            quote = Some(character);
            output.push(character);
            index += width;
            continue;
        }
        if character == ';' {
            return Err(invalid("semicolons are not supported"));
        }
        if character == '@' {
            let (replacement, consumed) = if placeholder_at(rest, "@window_start") {
                (&start, "@window_start".len())
            } else if placeholder_at(rest, "@window_end") {
                (&end, "@window_end".len())
            } else {
                return Err(invalid("unsupported query placeholder"));
            };
            output.push_str(replacement);
            index += consumed;
            continue;
        }

        output.push(character);
        index += width;
    }

    if quote.is_some() {
        return Err(invalid("query_sql contains an unterminated quoted value"));
    }
    Ok(output)
}

fn placeholder_at(rest: &str, placeholder: &str) -> bool {
    if !rest.starts_with(placeholder) {
        return false;
    }
    rest.as_bytes()
        .get(placeholder.len())
        .is_none_or(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn validate_query(query: &Query) -> Result<(), DetectionQueryError> {
    if query.with.is_some() {
        return Err(invalid("WITH/CTE queries are not supported"));
    }
    if query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(invalid("unsupported SELECT clause"));
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select,
        _ => {
            return Err(invalid(
                "set operations and nested queries are not supported",
            ));
        }
    };
    validate_select(select)?;
    validate_order_by(query)?;
    validate_limit(query)?;

    if let ControlFlow::Break(message) =
        visit_expressions(query, |expr| match validate_expression(expr) {
            Ok(()) => ControlFlow::Continue(()),
            Err(DetectionQueryError::Invalid(message)) => ControlFlow::Break(message),
            Err(_) => ControlFlow::Break("unsupported expression".to_string()),
        })
    {
        return Err(invalid(message));
    }
    Ok(())
}

fn validate_select(select: &Select) -> Result<(), DetectionQueryError> {
    if select.flavor != SelectFlavor::Standard
        || !select.optimizer_hints.is_empty()
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return Err(invalid("unsupported SELECT clause"));
    }
    if select.from.len() != 1 {
        return Err(invalid("query_sql must read exactly one telemetry table"));
    }
    if select.projection.is_empty() {
        return Err(invalid("SELECT must include at least one expression"));
    }
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. } => {}
            _ => {
                return Err(invalid(
                    "wildcards and multi-alias expressions are not supported",
                ));
            }
        }
    }

    let from = &select.from[0];
    if !from.joins.is_empty() {
        return Err(invalid("JOIN queries are not supported"));
    }
    let table_name = match &from.relation {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } if alias.is_none()
            && args.is_none()
            && with_hints.is_empty()
            && version.is_none()
            && !with_ordinality
            && partitions.is_empty()
            && json_path.is_none()
            && sample.is_none()
            && index_hints.is_empty() =>
        {
            table_name(name)?
        }
        TableFactor::Table { .. } => return Err(invalid("table modifiers are not supported")),
        _ => {
            return Err(invalid(
                "table functions and derived tables are not supported",
            ));
        }
    };
    if !ALLOWED_TABLES.contains(&table_name.as_str()) {
        return Err(invalid("query_sql references a table that is not allowed"));
    }

    match &select.group_by {
        GroupByExpr::Expressions(_, modifiers) if modifiers.is_empty() => {}
        GroupByExpr::Expressions(_, _) => {
            return Err(invalid("GROUP BY modifiers are not supported"));
        }
        GroupByExpr::All(_) => return Err(invalid("GROUP BY ALL is not supported")),
    }
    Ok(())
}

fn table_name(name: &sqlparser::ast::ObjectName) -> Result<String, DetectionQueryError> {
    if name.0.len() != 1 {
        return Err(invalid(
            "qualified and cross-database table names are not supported",
        ));
    }
    match &name.0[0] {
        ObjectNamePart::Identifier(identifier) if identifier.quote_style.is_none() => {
            Ok(identifier.value.to_ascii_lowercase())
        }
        _ => Err(invalid("computed or quoted table names are not supported")),
    }
}

fn validate_order_by(query: &Query) -> Result<(), DetectionQueryError> {
    let Some(order_by) = &query.order_by else {
        return Ok(());
    };
    if order_by.interpolate.is_some() {
        return Err(invalid("ORDER BY INTERPOLATE is not supported"));
    }
    match &order_by.kind {
        OrderByKind::Expressions(expressions)
            if expressions.iter().all(|expr| expr.with_fill.is_none()) =>
        {
            Ok(())
        }
        _ => Err(invalid("unsupported ORDER BY clause")),
    }
}

fn validate_limit(query: &Query) -> Result<(), DetectionQueryError> {
    let Some(limit_clause) = &query.limit_clause else {
        return Ok(());
    };
    let limit = match limit_clause {
        LimitClause::LimitOffset {
            limit: Some(limit),
            offset: None,
            limit_by,
        } if limit_by.is_empty() => limit,
        _ => return Err(invalid("only a literal LIMIT is supported")),
    };
    let Expr::Value(value) = limit else {
        return Err(invalid("LIMIT must be an integer literal"));
    };
    let Value::Number(number, false) = &value.value else {
        return Err(invalid("LIMIT must be an integer literal"));
    };
    let parsed = number
        .parse::<u64>()
        .map_err(|_| invalid("LIMIT must be an integer literal"))?;
    if parsed == 0 || parsed > MAX_LIMIT {
        return Err(invalid(format!("LIMIT must be between 1 and {MAX_LIMIT}")));
    }
    Ok(())
}

fn validate_expression(expr: &Expr) -> Result<(), DetectionQueryError> {
    match expr {
        Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::CompoundFieldAccess { .. }
        | Expr::IsFalse(_)
        | Expr::IsNotFalse(_)
        | Expr::IsTrue(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::InList { .. }
        | Expr::Between { .. }
        | Expr::BinaryOp { .. }
        | Expr::Like { .. }
        | Expr::ILike { .. }
        | Expr::Nested(_)
        | Expr::Tuple(_) => Ok(()),
        Expr::UnaryOp { op, .. }
            if matches!(
                op,
                UnaryOperator::Plus | UnaryOperator::Minus | UnaryOperator::Not
            ) =>
        {
            Ok(())
        }
        Expr::Value(value)
            if matches!(
                value.value,
                Value::Number(_, false)
                    | Value::SingleQuotedString(_)
                    | Value::Boolean(_)
                    | Value::Null
            ) =>
        {
            Ok(())
        }
        Expr::Function(function) => validate_function(function),
        _ => Err(invalid("query_sql contains an unsupported expression")),
    }
}

fn validate_function(function: &Function) -> Result<(), DetectionQueryError> {
    if function.name.0.len() != 1
        || function.uses_odbc_syntax
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return Err(invalid("unsupported function form"));
    }
    let function_name = match &function.name.0[0] {
        ObjectNamePart::Identifier(identifier) if identifier.quote_style.is_none() => {
            identifier.value.to_ascii_lowercase()
        }
        _ => {
            return Err(invalid(
                "computed or qualified function names are not supported",
            ));
        }
    };
    if !ALLOWED_FUNCTIONS.contains(&function_name.as_str()) {
        return Err(invalid(format!(
            "function '{function_name}' is not supported"
        )));
    }
    validate_function_parameters(&function.parameters)?;
    validate_function_arguments(&function.args, function_name == "count")?;
    Ok(())
}

fn validate_function_parameters(arguments: &FunctionArguments) -> Result<(), DetectionQueryError> {
    match arguments {
        FunctionArguments::None => Ok(()),
        FunctionArguments::List(_) => validate_function_arguments(arguments, false),
        FunctionArguments::Subquery(_) => {
            Err(invalid("subquery function parameters are not supported"))
        }
    }
}

fn validate_function_arguments(
    arguments: &FunctionArguments,
    wildcard_allowed: bool,
) -> Result<(), DetectionQueryError> {
    let FunctionArguments::List(arguments) = arguments else {
        return Err(invalid("unsupported function arguments"));
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return Err(invalid("function argument modifiers are not supported"));
    }
    for argument in &arguments.args {
        match argument {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(_)) => {}
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) if wildcard_allowed => {}
            _ => {
                return Err(invalid(
                    "named or wildcard function arguments are not supported",
                ));
            }
        }
    }
    Ok(())
}

fn inject_tenant_predicate(query: &mut Query, tenant_id: &str) -> Result<(), DetectionQueryError> {
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err(invalid("query_sql must be a single SELECT"));
    };
    let tenant_predicate = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("tenant_id"))),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(
            Value::SingleQuotedString(tenant_id.to_string()).into(),
        )),
    };
    select.selection = Some(match select.selection.take() {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(tenant_predicate),
            op: BinaryOperator::And,
            right: Box::new(Expr::Nested(Box::new(existing))),
        },
        None => tenant_predicate,
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compile(sql: &str) -> Result<CompiledDetectionQuery, DetectionQueryError> {
        compile_count_query(
            sql,
            "tenant'a",
            "2026-01-01 00:00:00",
            "2026-01-01 00:05:00",
        )
    }

    #[test]
    fn compiles_supported_grouped_detection_and_injects_tenant_ast() {
        let compiled = compile(
            "SELECT service_name, countIf(status = 'ERROR') AS errors, count() AS total \
             FROM spans WHERE timestamp BETWEEN @window_start AND @window_end \
             GROUP BY service_name HAVING errors / total > 0.05 ORDER BY errors DESC LIMIT 100",
        )
        .unwrap();
        assert!(compiled.scoped_sql.contains("tenant_id = 'tenant''a'"));
        assert!(
            compiled
                .scoped_sql
                .contains("timestamp BETWEEN '2026-01-01 00:00:00' AND '2026-01-01 00:05:00'")
        );
        assert!(
            compiled
                .count_sql
                .starts_with("SELECT count() AS _siem_count FROM (")
        );
    }

    #[test]
    fn supports_clickhouse_parametric_aggregate_and_map_access() {
        compile(
            "SELECT ServiceName, Attributes['host.name'] AS host, \
             quantile(0.99)(Value) AS p99 FROM metrics_gauge \
             WHERE TimeUnix >= @window_start GROUP BY ServiceName, host HAVING p99 > 0.9",
        )
        .unwrap();
    }

    #[test]
    fn injects_tenant_when_where_is_absent() {
        let compiled = compile("SELECT count() FROM logs").unwrap();
        assert!(
            compiled
                .scoped_sql
                .contains("WHERE tenant_id = 'tenant''a'")
        );
    }

    #[test]
    fn rejects_union_bypasses_regardless_of_case_or_whitespace() {
        for sql in [
            "SELECT Body FROM logs UNION ALL SELECT Body FROM logs",
            "select Body from logs\nUnIoN\nselect Body from logs",
            "SELECT Body FROM logs UNION/**/SELECT Body FROM logs",
        ] {
            assert!(compile(sql).is_err(), "accepted: {sql}");
        }
    }

    #[test]
    fn rejects_ctes_nested_queries_and_joins() {
        for sql in [
            "WITH x AS (SELECT Body FROM logs) SELECT Body FROM x",
            "SELECT Body FROM (SELECT Body FROM logs)",
            "SELECT Body FROM logs WHERE TraceId IN (SELECT trace_id FROM spans)",
            "SELECT l.Body FROM logs l JOIN spans s ON l.TraceId = s.trace_id",
        ] {
            assert!(compile(sql).is_err(), "accepted: {sql}");
        }
    }

    #[test]
    fn rejects_external_and_computed_table_sources() {
        for source in [
            "url('https://example.com')",
            "file('/etc/passwd')",
            "s3('https://bucket')",
            "remote('host', 'db', 'table')",
            "mysql('host', 'db', 'table', 'u', 'p')",
            "postgresql('host', 'db', 'table', 'u', 'p')",
            "numbers(10)",
        ] {
            let sql = format!("SELECT value FROM {source}");
            assert!(compile(&sql).is_err(), "accepted: {sql}");
        }
    }

    #[test]
    fn rejects_system_unknown_and_cross_database_tables() {
        for sql in [
            "SELECT name FROM system.tables",
            "SELECT Body FROM observability.logs",
            "SELECT value FROM tenant_usage",
            "SELECT value FROM unknown_table",
        ] {
            assert!(compile(sql).is_err(), "accepted: {sql}");
        }
    }

    #[test]
    fn rejects_comments_chaining_settings_and_output_clauses() {
        for sql in [
            "SELECT Body FROM logs -- hide a UNION",
            "SELECT Body FROM logs /* comment */",
            "SELECT Body FROM logs; SELECT Body FROM logs",
            "SELECT Body FROM logs SETTINGS max_threads=100",
            "SELECT Body FROM logs FORMAT JSON",
            "SELECT Body INTO OUTFILE '/tmp/x' FROM logs",
        ] {
            assert!(compile(sql).is_err(), "accepted: {sql}");
        }
    }

    #[test]
    fn rejects_unsupported_functions_and_expression_shapes() {
        for sql in [
            "SELECT sleep(10) FROM logs",
            "SELECT dictGet('x', 'y', 1) FROM logs",
            "SELECT arrayJoin([1, 2]) FROM logs",
            "SELECT * FROM logs",
            "SELECT row_number() OVER () FROM logs",
        ] {
            assert!(compile(sql).is_err(), "accepted: {sql}");
        }
    }

    #[test]
    fn rejects_ambiguous_placeholders_and_non_literal_limits() {
        for sql in [
            "SELECT Body FROM logs WHERE Timestamp > @unknown",
            "SELECT Body FROM logs WHERE Body = '@window_start'",
            "SELECT Body FROM logs LIMIT 10001",
            "SELECT Body FROM logs LIMIT count()",
            "SELECT Body FROM logs LIMIT 10 OFFSET 1",
        ] {
            assert!(compile(sql).is_err(), "accepted: {sql}");
        }
    }

    #[test]
    fn preview_and_scheduler_compile_identical_sql() {
        let sql = "SELECT ServiceName, count() AS n FROM logs \
                   WHERE Timestamp BETWEEN @window_start AND @window_end \
                   GROUP BY ServiceName HAVING n > 5";
        let preview = compile_count_query(sql, "acme", "start", "end").unwrap();
        let scheduled = compile_count_query(sql, "acme", "start", "end").unwrap();
        assert_eq!(preview, scheduled);
    }
}
