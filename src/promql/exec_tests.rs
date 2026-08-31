//! Tier 2: end-to-end scalar evaluation through the real parser + evaluator.
//!
//! These mirror VictoriaMetrics' `TestExecSuccess` scalar cases (app/vmselect/promql/
//! exec_test.go) that are valid *standard* PromQL. They exercise the full pipeline —
//! `promql_parser::parse` → recursive `evaluate` → binary/scalar-func application — with
//! NO ClickHouse and NO network: none of these queries contain a metric selector, so the
//! evaluator never reaches `query_clickhouse` (NumberLiteral, Paren, Unary, scalar Binary,
//! and scalar Call paths all stay in-process; see src/promql/eval.rs).
//!
//! The ClickHouse client is constructed with `Client::default()` and never connects.
//!
//! Grid: start=1000, end=2000, step=200 → 6 steps [1000,1200,1400,1600,1800,2000],
//! mirroring VM's evenly-spaced evaluation grid. A scalar query yields exactly one series
//! with EMPTY labels and the same value at every step.

use super::eval::{evaluate_instant_query, evaluate_range_query};

const START: f64 = 1000.0;
const END: f64 = 2000.0;
const STEP: f64 = 200.0;
const EXPECTED_STEPS: usize = 6; // 1000,1200,1400,1600,1800,2000

/// Build a never-connected ClickHouse client. Scalar-only queries never issue a query,
/// so this is never dialed.
fn client() -> clickhouse::Client {
    clickhouse::Client::default()
}

/// Assert that `query` evaluates (range) to exactly ONE empty-labeled series whose value
/// is ≈ `expected` at every step.
async fn f(query: &str, expected: f64) {
    let ch = client();
    let series = evaluate_range_query(&ch, query, START, END, STEP, "default")
        .await
        .unwrap_or_else(|e| panic!("query `{query}` failed: {e}"));
    assert_eq!(
        series.len(),
        1,
        "query `{query}`: expected 1 series, got {}",
        series.len()
    );
    assert!(
        series[0].labels.is_empty(),
        "query `{query}`: expected empty labels, got {:?}",
        series[0].labels
    );
    assert_eq!(
        series[0].samples.len(),
        EXPECTED_STEPS,
        "query `{query}`: expected {EXPECTED_STEPS} steps, got {}",
        series[0].samples.len()
    );
    for (t, v) in &series[0].samples {
        assert!(
            (v - expected).abs() < 1e-9,
            "query `{query}` at t={t}: expected {expected}, got {v}"
        );
    }
}

#[tokio::test]
async fn arithmetic_precedence() {
    // ^ is right-assoc and highest, then * / %, then + -.
    // 3^4=81, 2*81=162, 5%6=5, -1+162+5 = 166.
    f("-1 + 2 * 3 ^ 4 + 5 % 6", 166.0).await;
}

#[tokio::test]
async fn arithmetic_basics() {
    f("2 + 3 * 4", 14.0).await;
    f("(2 + 3) * 4", 20.0).await;
    f("2 ^ 3 ^ 2", 512.0).await; // right-assoc: 2^(3^2) = 2^9
    f("10 % 3", 1.0).await;
    f("7 / 2", 3.5).await;
}

// SKIPPED: scalar-function-on-literal cases cannot be exercised end-to-end here.
//
// SKIPPED (standard-PromQL semantics): abs(-5), sqrt(16), ceil(1.2), floor(1.8),
//   clamp_max(10, 3), clamp(5, 1, 3), ln(1). In standard PromQL (enforced by
//   promql-parser) these functions require an INSTANT-VECTOR argument; passing a bare
//   scalar literal is a parse error ("expected type vector ... got scalar"). They cannot
//   be exercised here without a metric selector (which would hit ClickHouse), so the
//   scalar math is covered directly at the unit level in scalar.rs (apply_scalar_op):
//   Abs(-5)=5, Sqrt(16)=4, Ceil(1.2)=2, Floor(1.8)=1, ClampMax(_,3)=3, Clamp(5,1,3)=3,
//   Ln(1)=0.
//
// SKIPPED (engine limitation): pi(). It parses (zero-arg function), but the evaluator's
//   scalar-call path requires at least one argument, so `pi()` errors before producing a
//   value. We must not modify production code, so the constant is asserted at the unit
//   level instead (scalar.rs test_scalar_pi_exp0_ln1: Pi() == std::f64::consts::PI).

#[tokio::test]
async fn bool_comparisons() {
    // `bool` modifier turns a comparison into a 0/1 result.
    f("2 > bool 1", 1.0).await;
    f("1 > bool 2", 0.0).await;
    f("1 == bool 1", 1.0).await;
}

#[tokio::test]
async fn instant_query_scalars() {
    let ch = client();
    // Instant query: single eval point at t=1000 with 300s lookback. Pure scalar.
    let series = evaluate_instant_query(&ch, "2 + 3 * 4", 1000.0, 300.0, "default")
        .await
        .unwrap();
    assert_eq!(series.len(), 1);
    assert!(series[0].labels.is_empty());
    assert_eq!(series[0].samples.len(), 1);
    assert!((series[0].samples[0].1 - 14.0).abs() < 1e-9);

    // A second pure-scalar instant query: precedence + right-assoc power.
    let series = evaluate_instant_query(&ch, "2 ^ 3 ^ 2", 1000.0, 300.0, "default")
        .await
        .unwrap();
    assert_eq!(series.len(), 1);
    assert!(series[0].labels.is_empty());
    assert!((series[0].samples[0].1 - 512.0).abs() < 1e-9);
    // NOTE: sqrt(81) etc. are NOT used here — scalar-arg scalar functions fail to parse
    // in standard PromQL (see scalar_functions SKIPPED comment).
}

// ── SKIPPED VictoriaMetrics cases ──
//
// These appear in VM's TestExecSuccess but are MetricsQL extensions, not standard PromQL,
// or use functions this engine does not implement. Left explicit so the gaps are visible.
//
// SKIPPED (VM/MetricsQL-only): bare duration scalars like `1h23m5s` — MetricsQL treats a
//   bare duration as a numeric scalar (seconds); standard PromQL / promql-parser does not.
// SKIPPED (VM/MetricsQL-only): `time()` — not in our ScalarFunc set (translate.rs).
// SKIPPED (VM/MetricsQL-only): `label_set(...)` — MetricsQL transform fn, not implemented.
// SKIPPED (VM/MetricsQL-only): `scalar(...)` — vector→scalar coercion fn, not implemented.
// SKIPPED (VM/MetricsQL-only): `label_replace(...)` — not implemented as a scalar/transform.
// SKIPPED (VM/MetricsQL-only): `union(...)` — MetricsQL multi-series union, not implemented.
// SKIPPED (VM/MetricsQL-only): `WITH(...)` templates — MetricsQL syntax, not standard PromQL.
// SKIPPED (standard-PromQL semantics): plain scalar comparison without `bool`
//   (e.g. `2 > 1`) — Prometheus rejects scalar-scalar comparison without `bool`; our impl
//   returns the lhs value instead, so we only assert the well-defined `bool` forms above.
