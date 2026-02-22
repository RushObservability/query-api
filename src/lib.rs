pub mod alert_engine;
pub mod anomaly_engine;
pub mod config_db;
pub mod handlers;
pub mod migrations;
pub mod models;
pub mod promql;
pub mod query_builder;
pub mod slo_engine;
pub mod usage_tracker;

use clickhouse::Client;
use std::sync::Arc;

use config_db::ConfigDb;
use usage_tracker::UsageTracker;

#[derive(Clone)]
pub struct AppState {
    pub ch: Client,
    pub config_db: Arc<ConfigDb>,
    pub usage: UsageTracker,
}
