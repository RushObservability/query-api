//! In-memory evaluation bookkeeping shared by the rule-evaluation engines
//! (alerts, SLOs, monitors, SIEM detections).
//!
//! Problem: persisting `last_eval_at` to the ReplacingMergeTree config tables on
//! EVERY tick for EVERY rule costs a SELECT…FINAL + a full-row INSERT per rule
//! per tick even when nothing changed, steadily degrading the config tables.
//!
//! Each engine is the sole writer of its rules' `last_eval_at`, so the engine
//! keeps it in memory instead and:
//!   * computes due-ness as `max(db_last_eval_at, mem_last_eval_at) + interval
//!     <= now` — on restart the map is empty and the DB value (coarsely
//!     persisted) bounds re-evaluation to at most one early re-eval per rule;
//!   * persists the full state row immediately ONLY on real state transitions
//!     (notification semantics depend on that and are unchanged);
//!   * additionally flushes `last_eval_at` to the DB no more than once per
//!     `flush_every` evaluations per rule, so the UI's "last evaluated" display
//!     stays roughly fresh and restart replay is bounded.
//!
//! Entries are keyed by rule id; the map only grows with rule ids seen during
//! the process lifetime (rules are operator-created, bounded cardinality).

use std::collections::HashMap;

/// Per-rule in-memory evaluation state for one engine loop.
pub struct EvalState {
    /// rule_id → (last in-memory evaluation time, evals since last DB flush)
    map: HashMap<String, (chrono::DateTime<chrono::Utc>, u32)>,
    /// Flush `last_eval_at` to the DB once per this many evaluations.
    flush_every: u32,
}

impl EvalState {
    pub fn new(flush_every: u32) -> Self {
        Self { map: HashMap::new(), flush_every: flush_every.max(1) }
    }

    /// In-memory due check. Combined with the engine's existing DB-side check
    /// this yields `max(db, mem) + interval <= now` semantics: a rule is due
    /// only when BOTH the (coarse) DB value and the (exact) in-memory value say
    /// the interval has elapsed. Unknown rules (fresh start) are due.
    pub fn is_due(&self, rule_id: &str, now: chrono::DateTime<chrono::Utc>, interval_secs: i64) -> bool {
        match self.map.get(rule_id) {
            None => true,
            Some((last, _)) => (now - *last).num_seconds() >= interval_secs,
        }
    }

    /// Whether the upcoming evaluation should also flush `last_eval_at` to the
    /// DB (1-in-N cadence). First evaluation after a restart flushes, which
    /// bounds restart replay. Call BEFORE the evaluation; record the outcome
    /// with [`record`].
    pub fn should_flush(&self, rule_id: &str) -> bool {
        match self.map.get(rule_id) {
            None => true,
            Some((_, n)) => n + 1 >= self.flush_every,
        }
    }

    /// Record an evaluation outcome. `persisted` = the rule row was written to
    /// the DB this evaluation (state transition or cadence flush), which resets
    /// the flush counter.
    pub fn record(&mut self, rule_id: String, now: chrono::DateTime<chrono::Utc>, persisted: bool) {
        let entry = self.map.entry(rule_id).or_insert((now, 0));
        entry.0 = now;
        entry.1 = if persisted { 0 } else { entry.1.saturating_add(1) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};

    #[test]
    fn unknown_rule_is_due_and_flushes() {
        let st = EvalState::new(10);
        assert!(st.is_due("r1", Utc::now(), 60));
        assert!(st.should_flush("r1"), "first eval after restart must flush");
    }

    #[test]
    fn due_respects_interval() {
        let mut st = EvalState::new(10);
        let t0 = Utc::now();
        st.record("r1".into(), t0, true);
        assert!(!st.is_due("r1", t0 + Duration::seconds(30), 60));
        assert!(st.is_due("r1", t0 + Duration::seconds(60), 60));
    }

    #[test]
    fn flush_cadence_is_one_in_n() {
        let mut st = EvalState::new(3);
        let t0 = Utc::now();
        // First eval flushes (restart bound) → counter resets.
        assert!(st.should_flush("r1"));
        st.record("r1".into(), t0, true);
        // Next two evals don't flush, third does.
        assert!(!st.should_flush("r1"));
        st.record("r1".into(), t0, false);
        assert!(!st.should_flush("r1"));
        st.record("r1".into(), t0, false);
        assert!(st.should_flush("r1"));
        st.record("r1".into(), t0, true);
        assert!(!st.should_flush("r1"));
    }

    #[test]
    fn transition_persist_resets_counter() {
        let mut st = EvalState::new(3);
        let t0 = Utc::now();
        st.record("r1".into(), t0, true);
        st.record("r1".into(), t0, false);
        // State transition mid-cadence persists the row → counter resets:
        // two more non-persisting evals must pass before the next flush.
        st.record("r1".into(), t0, true);
        assert!(!st.should_flush("r1"));
        st.record("r1".into(), t0, false);
        assert!(!st.should_flush("r1"));
        st.record("r1".into(), t0, false);
        assert!(st.should_flush("r1"));
    }

    #[test]
    fn flush_every_zero_is_clamped() {
        let mut st = EvalState::new(0);
        let t0 = Utc::now();
        st.record("r1".into(), t0, false);
        assert!(st.should_flush("r1"), "flush_every is clamped to at least 1");
    }
}
