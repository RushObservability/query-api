use serde::{Deserialize, Serialize};

use crate::models::query::{Filter, FilterOp};
use crate::query_builder::{escape_string_literal, json_attribute};

pub const SCOPE_FIELD: &str = "rush.service_tag";
pub const TOP_TAGS: usize = 3;
const MAX_TAG_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceTag {
    Region,
    Version,
    Endpoint,
}

pub const REQUEST_SPAN: &str = "kind = 'SPAN_KIND_SERVER'";

impl ServiceTag {
    pub(crate) fn expression(self) -> String {
        let attribute = match self {
            Self::Region => "cloud.region",
            Self::Version => "service.version",
            Self::Endpoint => "http.route",
        };
        let span = json_attribute("attributes", attribute);
        let value = match self {
            Self::Endpoint => span,
            Self::Region | Self::Version => {
                let resource = json_attribute("resource_attributes", attribute);
                format!("coalesce(nullIf({resource}, ''), {span})")
            }
        };
        // Oversized values still contribute to traffic and percentiles, but cannot
        // become named series or put an unbounded attribute value in a share URL.
        format!("if(length({value}) <= {MAX_TAG_BYTES}, {value}, '')")
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Selection {
    Value { value: String },
    Other { excluded: Vec<String> },
}

#[derive(Deserialize)]
struct Scope {
    service: String,
    dimension: ServiceTag,
    selection: Selection,
}

pub(crate) fn request_condition(filter: &Filter) -> Option<String> {
    if !matches!(filter.op, FilterOp::Eq) {
        return None;
    }
    let scope: Scope = serde_json::from_str(filter.value.as_str()?).ok()?;
    if scope.service.is_empty() || scope.service.len() > 1024 {
        return None;
    }
    let valid_value = |value: &str| !value.is_empty() && value.len() <= MAX_TAG_BYTES;
    let tag = scope.dimension.expression();
    let condition = match scope.selection {
        Selection::Value { value } if valid_value(&value) => {
            format!("{tag} = '{}'", escape_string_literal(&value))
        }
        Selection::Other { excluded } if excluded.is_empty() => "1".to_string(),
        Selection::Other { excluded }
            if excluded.len() <= TOP_TAGS && excluded.iter().all(|value| valid_value(value)) =>
        {
            let values = excluded
                .iter()
                .map(|value| format!("'{}'", escape_string_literal(value)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{tag} NOT IN ({values})")
        }
        _ => return None,
    };
    let service = escape_string_literal(&scope.service);
    Some(format!(
        "(service_name = '{service}' AND {REQUEST_SPAN} AND {condition})"
    ))
}
