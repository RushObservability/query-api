use promql_parser::parser::token::{self, TokenType};
use super::types::{AggOp, RangeFunc, ScalarFunc};

/// Map a function name (from promql-parser `Call.func.name`) to our internal RangeFunc.
pub fn to_range_func(name: &str) -> Option<RangeFunc> {
    match name {
        "rate" => Some(RangeFunc::Rate),
        "irate" => Some(RangeFunc::Irate),
        "increase" => Some(RangeFunc::Increase),
        "sum_over_time" => Some(RangeFunc::SumOverTime),
        "avg_over_time" => Some(RangeFunc::AvgOverTime),
        "min_over_time" => Some(RangeFunc::MinOverTime),
        "max_over_time" => Some(RangeFunc::MaxOverTime),
        "count_over_time" => Some(RangeFunc::CountOverTime),
        "stddev_over_time" => Some(RangeFunc::StddevOverTime),
        "stdvar_over_time" => Some(RangeFunc::StdvarOverTime),
        "quantile_over_time" => Some(RangeFunc::QuantileOverTime),
        "last_over_time" => Some(RangeFunc::LastOverTime),
        "delta" => Some(RangeFunc::Delta),
        "idelta" => Some(RangeFunc::Idelta),
        "deriv" => Some(RangeFunc::Deriv),
        "predict_linear" => Some(RangeFunc::PredictLinear),
        "changes" => Some(RangeFunc::Changes),
        "resets" => Some(RangeFunc::Resets),
        "absent_over_time" => Some(RangeFunc::AbsentOverTime),
        "present_over_time" => Some(RangeFunc::PresentOverTime),
        _ => None,
    }
}

/// Map a function name to our internal ScalarFunc.
pub fn to_scalar_func(name: &str) -> Option<ScalarFunc> {
    match name {
        "abs" => Some(ScalarFunc::Abs),
        "ceil" => Some(ScalarFunc::Ceil),
        "floor" => Some(ScalarFunc::Floor),
        "round" => Some(ScalarFunc::Round),
        "sqrt" => Some(ScalarFunc::Sqrt),
        "exp" => Some(ScalarFunc::Exp),
        "ln" => Some(ScalarFunc::Ln),
        "log2" => Some(ScalarFunc::Log2),
        "log10" => Some(ScalarFunc::Log10),
        "sgn" => Some(ScalarFunc::Sgn),
        "clamp_min" => Some(ScalarFunc::ClampMin),
        "clamp_max" => Some(ScalarFunc::ClampMax),
        "clamp" => Some(ScalarFunc::Clamp),
        "histogram_quantile" => Some(ScalarFunc::HistogramQuantile),
        "sin" => Some(ScalarFunc::Sin),
        "cos" => Some(ScalarFunc::Cos),
        "asin" => Some(ScalarFunc::Asin),
        "acos" => Some(ScalarFunc::Acos),
        "atan2" => Some(ScalarFunc::Atan2),
        "sinh" => Some(ScalarFunc::Sinh),
        "cosh" => Some(ScalarFunc::Cosh),
        "asinh" => Some(ScalarFunc::Asinh),
        "acosh" => Some(ScalarFunc::Acosh),
        "atanh" => Some(ScalarFunc::Atanh),
        "deg" => Some(ScalarFunc::Deg),
        "rad" => Some(ScalarFunc::Rad),
        "pi" => Some(ScalarFunc::Pi),
        "timestamp" => Some(ScalarFunc::Timestamp),
        _ => None,
    }
}

/// Map a promql-parser aggregation TokenType to our internal AggOp.
pub fn to_agg_op(tt: TokenType) -> Result<AggOp, String> {
    let t = tt.id();
    if t == token::T_SUM { return Ok(AggOp::Sum); }
    if t == token::T_AVG { return Ok(AggOp::Avg); }
    if t == token::T_MIN { return Ok(AggOp::Min); }
    if t == token::T_MAX { return Ok(AggOp::Max); }
    if t == token::T_COUNT { return Ok(AggOp::Count); }
    if t == token::T_STDDEV { return Ok(AggOp::Stddev); }
    if t == token::T_STDVAR { return Ok(AggOp::Stdvar); }
    if t == token::T_QUANTILE { return Ok(AggOp::Quantile); }
    if t == token::T_TOPK { return Ok(AggOp::Topk); }
    if t == token::T_BOTTOMK { return Ok(AggOp::Bottomk); }
    if t == token::T_GROUP { return Ok(AggOp::Group); }
    if t == token::T_COUNT_VALUES { return Ok(AggOp::CountValues); }
    Err(format!("unsupported aggregation token: {tt:?}"))
}

/// Extract the label list and whether it's "by" (include) or "without" (exclude)
/// from a promql-parser LabelModifier.
pub fn extract_label_modifier(
    modifier: &Option<promql_parser::parser::LabelModifier>,
) -> (Vec<String>, bool) {
    match modifier {
        Some(promql_parser::parser::LabelModifier::Include(labels)) => {
            (labels.labels.clone(), false) // by(labels) → without=false
        }
        Some(promql_parser::parser::LabelModifier::Exclude(labels)) => {
            (labels.labels.clone(), true) // without(labels) → without=true
        }
        None => (vec![], false),
    }
}
