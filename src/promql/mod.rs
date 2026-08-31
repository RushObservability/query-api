pub mod aggregate;
pub mod binary;
pub mod compute;
pub mod eval;
pub mod scalar;
pub mod sql;
pub mod translate;
pub mod types;

#[cfg(test)]
mod exec_tests;

// Re-export the public API
pub use eval::{evaluate_instant_query, evaluate_range_query, extract_metrics_from_expr};
pub use sql::matchers_to_sql;
pub use types::build_label_set;
