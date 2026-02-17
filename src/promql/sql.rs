use promql_parser::label::{MatchOp, Matcher};

/// Build ClickHouse WHERE clause fragments from promql-parser label matchers.
pub fn matchers_to_sql(matchers: &[Matcher]) -> Vec<String> {
    let mut conditions = Vec::new();
    for m in matchers {
        let col = match m.name.as_str() {
            "__name__" => "MetricName".to_string(),
            "service_name" | "job" => "ServiceName".to_string(),
            _ => format!("Attributes['{}']", m.name.replace('\'', "\\'")),
        };

        let escaped = m.value.replace('\'', "\\'");
        let cond = match &m.op {
            MatchOp::Equal => format!("{col} = '{escaped}'"),
            MatchOp::NotEqual => format!("{col} != '{escaped}'"),
            MatchOp::Re(_) => format!("match({col}, '{escaped}')"),
            MatchOp::NotRe(_) => format!("NOT match({col}, '{escaped}')"),
        };
        conditions.push(cond);
    }
    conditions
}
