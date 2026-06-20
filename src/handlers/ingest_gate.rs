//! Per-tenant ingest signal gate.
//!
//! Every ingest write goes through [`write_gated`] instead of calling
//! `state.writer.write(...)` directly. If the tenant has the batch's signal
//! disabled the batch is silently accepted (returns `Ok(())`, so the sender
//! still gets a 2xx) but dropped, and the dropped row count is recorded under
//! `"<signal>_dropped"` so admins can see blocked volume. Tenants without an
//! explicit signal config default to all-enabled (backward compatible).

use crate::AppState;
use crate::ch_writer::{SpoolBatch, WriteError};

/// Write `batch` for `tenant_id`, unless the tenant has that signal disabled —
/// in which case the batch is dropped (counted) and `Ok(())` is returned.
pub async fn write_gated(
    state: &AppState,
    tenant_id: &str,
    batch: SpoolBatch,
) -> Result<(), WriteError> {
    let cat = batch.signal_category();
    let n = batch.len();

    if !state.config_db.tenant_signal_enabled(tenant_id, cat).await {
        // Accepted-but-dropped: no error to the sender, but count it so the
        // tenant-signals endpoint can surface the blocked volume.
        state.usage_accumulator.record_dropped(tenant_id, cat, n as u64, 0);
        tracing::debug!(
            tenant_id = %tenant_id,
            signal = %cat,
            dropped = n,
            "ingest dropped: signal disabled for tenant"
        );
        return Ok(());
    }

    state.writer.write(batch).await
}
