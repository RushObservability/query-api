use promql_parser::label::{MatchOp, Matcher};

/// Build ClickHouse WHERE clause fragments from promql-parser label matchers.
pub fn matchers_to_sql(matchers: &[Matcher]) -> Vec<String> {
    let mut conditions = Vec::new();
    for m in matchers {
        let col = match m.name.as_str() {
            "__name__" => "MetricName".to_string(),
            "service_name" | "job" => "ServiceName".to_string(),
            _ => format!(
                "Attributes['{}']",
                crate::query_builder::escape_string_literal(&m.name)
            ),
        };

        let escaped = crate::query_builder::escape_string_literal(&m.value);
        // Prometheus regex matchers are fully anchored; ClickHouse `match` is not.
        let anchored = crate::query_builder::escape_string_literal(&format!("^(?:{})$", m.value));
        let cond = match &m.op {
            MatchOp::Equal => format!("{col} = '{escaped}'"),
            MatchOp::NotEqual => format!("{col} != '{escaped}'"),
            MatchOp::Re(_) => format!("match({col}, '{anchored}')"),
            MatchOp::NotRe(_) => format!("NOT match({col}, '{anchored}')"),
        };
        conditions.push(cond);
    }
    conditions
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conditions(query: &str) -> Vec<String> {
        let promql_parser::parser::Expr::VectorSelector(selector) =
            promql_parser::parser::parse(query).unwrap()
        else {
            panic!("not a vector selector: {query}");
        };
        let matchers: Vec<Matcher> = selector
            .matchers
            .matchers
            .into_iter()
            .filter(|m| m.name != "__name__")
            .collect();
        matchers_to_sql(&matchers)
    }

    #[test]
    fn regex_matchers_are_anchored_like_prometheus() {
        // `?` reaches ClickHouse as `\x3F`, which it decodes back inside the literal.
        assert_eq!(
            conditions(r#"up{service_name=~"api"}"#),
            [r"match(ServiceName, '^(\x3F:api)$')"]
        );
        assert_eq!(
            conditions(r#"up{host!~"a|b.*"}"#),
            [r"NOT match(Attributes['host'], '^(\x3F:a|b.*)$')"]
        );
    }
}
