use rusqlite::{Connection, params};
use std::sync::Mutex;

pub struct ConfigDb {
    conn: Mutex<Connection>,
}

impl ConfigDb {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let db = Self {
            conn: Mutex::new(conn),
        };
        db.run_migrations()?;
        Ok(db)
    }

    fn run_migrations(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS dashboards (
                id          TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );

            CREATE TABLE IF NOT EXISTS widgets (
                id             TEXT PRIMARY KEY,
                dashboard_id   TEXT NOT NULL REFERENCES dashboards(id) ON DELETE CASCADE,
                title          TEXT NOT NULL,
                widget_type    TEXT NOT NULL CHECK(widget_type IN ('timeseries','bar','table','counter')),
                query_config   TEXT NOT NULL,
                position       TEXT NOT NULL,
                display_config TEXT NOT NULL DEFAULT '{}',
                created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_widgets_dashboard ON widgets(dashboard_id);

            CREATE TABLE IF NOT EXISTS notification_channels (
                id           TEXT PRIMARY KEY,
                name         TEXT NOT NULL,
                channel_type TEXT NOT NULL CHECK(channel_type IN ('webhook','slack')),
                config       TEXT NOT NULL,
                created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );

            CREATE TABLE IF NOT EXISTS alert_rules (
                id                      TEXT PRIMARY KEY,
                name                    TEXT NOT NULL,
                description             TEXT NOT NULL DEFAULT '',
                enabled                 INTEGER NOT NULL DEFAULT 1,
                query_config            TEXT NOT NULL,
                condition_op            TEXT NOT NULL CHECK(condition_op IN ('>','>=','<','<=','=','!=')),
                condition_threshold     REAL NOT NULL,
                eval_interval_secs      INTEGER NOT NULL DEFAULT 60,
                notification_channel_ids TEXT NOT NULL DEFAULT '[]',
                state                   TEXT NOT NULL DEFAULT 'ok' CHECK(state IN ('ok','alerting','no_data')),
                last_eval_at            TEXT,
                last_triggered_at       TEXT,
                created_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );

            CREATE TABLE IF NOT EXISTS alert_events (
                id         TEXT PRIMARY KEY,
                rule_id    TEXT NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
                state      TEXT NOT NULL,
                value      REAL NOT NULL,
                threshold  REAL NOT NULL,
                message    TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_alert_events_rule ON alert_events(rule_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS deploy_markers (
                id           TEXT PRIMARY KEY,
                service_name TEXT NOT NULL,
                version      TEXT NOT NULL DEFAULT '',
                commit_sha   TEXT NOT NULL DEFAULT '',
                description  TEXT NOT NULL DEFAULT '',
                environment  TEXT NOT NULL DEFAULT '',
                deployed_by  TEXT NOT NULL DEFAULT '',
                deployed_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_deploy_service ON deploy_markers(service_name, deployed_at DESC);

            CREATE TABLE IF NOT EXISTS slos (
                id                      TEXT PRIMARY KEY,
                name                    TEXT NOT NULL,
                description             TEXT NOT NULL DEFAULT '',
                enabled                 INTEGER NOT NULL DEFAULT 1,
                service_name            TEXT NOT NULL,
                window_type             TEXT NOT NULL CHECK(window_type IN ('rolling_1h','rolling_24h','rolling_7d','rolling_30d')),
                target_percentage       REAL NOT NULL,
                good_filters            TEXT NOT NULL,
                total_filters           TEXT NOT NULL,
                eval_interval_secs      INTEGER NOT NULL DEFAULT 60,
                notification_channel_ids TEXT NOT NULL DEFAULT '[]',
                state                   TEXT NOT NULL DEFAULT 'compliant' CHECK(state IN ('compliant','breaching','no_data')),
                error_budget_remaining  REAL,
                good_count              INTEGER,
                total_count             INTEGER,
                last_eval_at            TEXT,
                last_breached_at        TEXT,
                created_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_slos_service ON slos(service_name);

            CREATE TABLE IF NOT EXISTS slo_events (
                id         TEXT PRIMARY KEY,
                slo_id     TEXT NOT NULL REFERENCES slos(id) ON DELETE CASCADE,
                state      TEXT NOT NULL,
                good_count INTEGER NOT NULL,
                total_count INTEGER NOT NULL,
                error_budget_remaining REAL NOT NULL,
                message    TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_slo_events_slo ON slo_events(slo_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS api_keys (
                id         TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
                key_hash   TEXT NOT NULL UNIQUE,
                prefix     TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );

            CREATE TABLE IF NOT EXISTS anomaly_rules (
                id                      TEXT PRIMARY KEY,
                name                    TEXT NOT NULL,
                description             TEXT NOT NULL DEFAULT '',
                enabled                 INTEGER NOT NULL DEFAULT 1,
                source                  TEXT NOT NULL CHECK(source IN ('prometheus','apm')),
                pattern                 TEXT NOT NULL DEFAULT '',
                query                   TEXT NOT NULL DEFAULT '',
                service_name            TEXT NOT NULL DEFAULT '',
                apm_metric              TEXT NOT NULL DEFAULT '',
                sensitivity             REAL NOT NULL DEFAULT 3.0,
                alpha                   REAL NOT NULL DEFAULT 0.25,
                eval_interval_secs      INTEGER NOT NULL DEFAULT 300,
                window_secs             INTEGER NOT NULL DEFAULT 3600,
                notification_channel_ids TEXT NOT NULL DEFAULT '[]',
                state                   TEXT NOT NULL DEFAULT 'normal' CHECK(state IN ('normal','anomalous','no_data')),
                last_eval_at            TEXT,
                last_triggered_at       TEXT,
                created_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );

            CREATE TABLE IF NOT EXISTS anomaly_events (
                id         TEXT PRIMARY KEY,
                rule_id    TEXT NOT NULL REFERENCES anomaly_rules(id) ON DELETE CASCADE,
                state      TEXT NOT NULL,
                metric     TEXT NOT NULL DEFAULT '',
                value      REAL NOT NULL,
                expected   REAL NOT NULL,
                deviation  REAL NOT NULL DEFAULT 0.0,
                message    TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_anomaly_events_rule ON anomaly_events(rule_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            ",
        )?;

        // Add split_labels column if it doesn't exist yet
        {
            let has_col: bool = conn
                .prepare("SELECT COUNT(*) FROM pragma_table_info('anomaly_rules') WHERE name = 'split_labels'")?
                .query_row([], |row| row.get(0))?;
            if !has_col {
                conn.execute_batch(
                    "ALTER TABLE anomaly_rules ADD COLUMN split_labels TEXT NOT NULL DEFAULT '[]';",
                )?;
            }
        }

        // Add signal_type column to alert_rules if it doesn't exist yet
        {
            let has_col: bool = conn
                .prepare("SELECT COUNT(*) FROM pragma_table_info('alert_rules') WHERE name = 'signal_type'")?
                .query_row([], |row| row.get(0))?;
            if !has_col {
                conn.execute_batch(
                    "ALTER TABLE alert_rules ADD COLUMN signal_type TEXT NOT NULL DEFAULT 'apm';",
                )?;
            }
        }

        Ok(())
    }

    // ── Dashboard operations ──

    pub fn list_dashboards(&self) -> anyhow::Result<Vec<crate::models::dashboard::Dashboard>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, created_at, updated_at FROM dashboards ORDER BY created_at DESC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(crate::models::dashboard::Dashboard {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    created_at: row.get(3)?,
                    updated_at: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_dashboard(&self, id: &str) -> anyhow::Result<Option<crate::models::dashboard::Dashboard>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, created_at, updated_at FROM dashboards WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            Ok(crate::models::dashboard::Dashboard {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                created_at: row.get(3)?,
                updated_at: row.get(4)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    pub fn create_dashboard(&self, id: &str, name: &str, description: &str) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO dashboards (id, name, description) VALUES (?1, ?2, ?3)",
            params![id, name, description],
        )?;
        Ok(())
    }

    pub fn update_dashboard(&self, id: &str, name: &str, description: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE dashboards SET name = ?2, description = ?3, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?1",
            params![id, name, description],
        )?;
        Ok(count > 0)
    }

    pub fn delete_dashboard(&self, id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute("DELETE FROM dashboards WHERE id = ?1", params![id])?;
        Ok(count > 0)
    }

    // ── Widget operations ──

    pub fn list_widgets(&self, dashboard_id: &str) -> anyhow::Result<Vec<crate::models::dashboard::Widget>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, dashboard_id, title, widget_type, query_config, position, display_config, created_at, updated_at \
             FROM widgets WHERE dashboard_id = ?1 ORDER BY created_at ASC",
        )?;
        let rows = stmt
            .query_map(params![dashboard_id], |row| {
                Ok(crate::models::dashboard::Widget {
                    id: row.get(0)?,
                    dashboard_id: row.get(1)?,
                    title: row.get(2)?,
                    widget_type: row.get(3)?,
                    query_config: row.get(4)?,
                    position: row.get(5)?,
                    display_config: row.get(6)?,
                    created_at: row.get(7)?,
                    updated_at: row.get(8)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn create_widget(
        &self,
        id: &str,
        dashboard_id: &str,
        title: &str,
        widget_type: &str,
        query_config: &str,
        position: &str,
        display_config: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO widgets (id, dashboard_id, title, widget_type, query_config, position, display_config) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![id, dashboard_id, title, widget_type, query_config, position, display_config],
        )?;
        Ok(())
    }

    pub fn update_widget(
        &self,
        id: &str,
        dashboard_id: &str,
        title: &str,
        widget_type: &str,
        query_config: &str,
        position: &str,
        display_config: &str,
    ) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE widgets SET title = ?3, widget_type = ?4, query_config = ?5, position = ?6, display_config = ?7, \
             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?1 AND dashboard_id = ?2",
            params![id, dashboard_id, title, widget_type, query_config, position, display_config],
        )?;
        Ok(count > 0)
    }

    pub fn delete_widget(&self, id: &str, dashboard_id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "DELETE FROM widgets WHERE id = ?1 AND dashboard_id = ?2",
            params![id, dashboard_id],
        )?;
        Ok(count > 0)
    }

    // ── Notification channel operations ──

    pub fn list_channels(&self) -> anyhow::Result<Vec<crate::models::alert::NotificationChannel>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, channel_type, config, created_at FROM notification_channels ORDER BY created_at DESC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(crate::models::alert::NotificationChannel {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    channel_type: row.get(2)?,
                    config: row.get(3)?,
                    created_at: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn create_channel(
        &self,
        id: &str,
        name: &str,
        channel_type: &str,
        config: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO notification_channels (id, name, channel_type, config) VALUES (?1, ?2, ?3, ?4)",
            params![id, name, channel_type, config],
        )?;
        Ok(())
    }

    pub fn delete_channel(&self, id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute("DELETE FROM notification_channels WHERE id = ?1", params![id])?;
        Ok(count > 0)
    }

    // ── Alert rule operations ──

    pub fn list_alerts(&self) -> anyhow::Result<Vec<crate::models::alert::AlertRule>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, \
             eval_interval_secs, notification_channel_ids, state, last_eval_at, last_triggered_at, \
             created_at, updated_at FROM alert_rules ORDER BY created_at DESC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(crate::models::alert::AlertRule {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    enabled: row.get(3)?,
                    signal_type: row.get(4)?,
                    query_config: row.get(5)?,
                    condition_op: row.get(6)?,
                    condition_threshold: row.get(7)?,
                    eval_interval_secs: row.get(8)?,
                    notification_channel_ids: row.get(9)?,
                    state: row.get(10)?,
                    last_eval_at: row.get(11)?,
                    last_triggered_at: row.get(12)?,
                    created_at: row.get(13)?,
                    updated_at: row.get(14)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_alert(&self, id: &str) -> anyhow::Result<Option<crate::models::alert::AlertRule>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, \
             eval_interval_secs, notification_channel_ids, state, last_eval_at, last_triggered_at, \
             created_at, updated_at FROM alert_rules WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            Ok(crate::models::alert::AlertRule {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                enabled: row.get(3)?,
                signal_type: row.get(4)?,
                query_config: row.get(5)?,
                condition_op: row.get(6)?,
                condition_threshold: row.get(7)?,
                eval_interval_secs: row.get(8)?,
                notification_channel_ids: row.get(9)?,
                state: row.get(10)?,
                last_eval_at: row.get(11)?,
                last_triggered_at: row.get(12)?,
                created_at: row.get(13)?,
                updated_at: row.get(14)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    pub fn create_alert(
        &self,
        id: &str,
        name: &str,
        description: &str,
        enabled: bool,
        signal_type: &str,
        query_config: &str,
        condition_op: &str,
        condition_threshold: f64,
        eval_interval_secs: i64,
        notification_channel_ids: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO alert_rules (id, name, description, enabled, signal_type, query_config, condition_op, \
             condition_threshold, eval_interval_secs, notification_channel_ids) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids],
        )?;
        Ok(())
    }

    pub fn update_alert(
        &self,
        id: &str,
        name: &str,
        description: &str,
        enabled: bool,
        signal_type: &str,
        query_config: &str,
        condition_op: &str,
        condition_threshold: f64,
        eval_interval_secs: i64,
        notification_channel_ids: &str,
    ) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE alert_rules SET name = ?2, description = ?3, enabled = ?4, signal_type = ?5, query_config = ?6, \
             condition_op = ?7, condition_threshold = ?8, eval_interval_secs = ?9, \
             notification_channel_ids = ?10, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?1",
            params![id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids],
        )?;
        Ok(count > 0)
    }

    pub fn delete_alert(&self, id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute("DELETE FROM alert_rules WHERE id = ?1", params![id])?;
        Ok(count > 0)
    }

    pub fn update_alert_state(
        &self,
        id: &str,
        state: &str,
        last_eval_at: &str,
        last_triggered_at: Option<&str>,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        match last_triggered_at {
            Some(t) => {
                conn.execute(
                    "UPDATE alert_rules SET state = ?2, last_eval_at = ?3, last_triggered_at = ?4 WHERE id = ?1",
                    params![id, state, last_eval_at, t],
                )?;
            }
            None => {
                conn.execute(
                    "UPDATE alert_rules SET state = ?2, last_eval_at = ?3 WHERE id = ?1",
                    params![id, state, last_eval_at],
                )?;
            }
        }
        Ok(())
    }

    pub fn get_due_alerts(&self, now: &str) -> anyhow::Result<Vec<crate::models::alert::AlertRule>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, \
             eval_interval_secs, notification_channel_ids, state, last_eval_at, last_triggered_at, \
             created_at, updated_at FROM alert_rules \
             WHERE enabled = 1 AND (last_eval_at IS NULL OR \
             strftime('%s', ?1) - strftime('%s', last_eval_at) >= eval_interval_secs)",
        )?;
        let rows = stmt
            .query_map(params![now], |row| {
                Ok(crate::models::alert::AlertRule {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    enabled: row.get(3)?,
                    signal_type: row.get(4)?,
                    query_config: row.get(5)?,
                    condition_op: row.get(6)?,
                    condition_threshold: row.get(7)?,
                    eval_interval_secs: row.get(8)?,
                    notification_channel_ids: row.get(9)?,
                    state: row.get(10)?,
                    last_eval_at: row.get(11)?,
                    last_triggered_at: row.get(12)?,
                    created_at: row.get(13)?,
                    updated_at: row.get(14)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // ── Alert event operations ──

    pub fn create_alert_event(
        &self,
        id: &str,
        rule_id: &str,
        state: &str,
        value: f64,
        threshold: f64,
        message: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO alert_events (id, rule_id, state, value, threshold, message) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, rule_id, state, value, threshold, message],
        )?;
        Ok(())
    }

    pub fn list_alert_events(
        &self,
        rule_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::alert::AlertEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, rule_id, state, value, threshold, message, created_at \
             FROM alert_events WHERE rule_id = ?1 ORDER BY created_at DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![rule_id, limit], |row| {
                Ok(crate::models::alert::AlertEvent {
                    id: row.get(0)?,
                    rule_id: row.get(1)?,
                    state: row.get(2)?,
                    value: row.get(3)?,
                    threshold: row.get(4)?,
                    message: row.get(5)?,
                    created_at: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn list_all_alert_events(
        &self,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::alert::AlertEventWithRule>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT e.id, e.rule_id, COALESCE(r.name, 'deleted rule') as rule_name, \
             e.state, e.value, e.threshold, e.message, e.created_at \
             FROM alert_events e \
             LEFT JOIN alert_rules r ON e.rule_id = r.id \
             ORDER BY e.created_at DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![limit], |row| {
                Ok(crate::models::alert::AlertEventWithRule {
                    id: row.get(0)?,
                    rule_id: row.get(1)?,
                    rule_name: row.get(2)?,
                    state: row.get(3)?,
                    value: row.get(4)?,
                    threshold: row.get(5)?,
                    message: row.get(6)?,
                    created_at: row.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_channel(&self, id: &str) -> anyhow::Result<Option<crate::models::alert::NotificationChannel>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, channel_type, config, created_at FROM notification_channels WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            Ok(crate::models::alert::NotificationChannel {
                id: row.get(0)?,
                name: row.get(1)?,
                channel_type: row.get(2)?,
                config: row.get(3)?,
                created_at: row.get(4)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    // ── Deploy marker operations ──

    pub fn create_deploy_marker(
        &self,
        id: &str,
        service_name: &str,
        version: &str,
        commit_sha: &str,
        description: &str,
        environment: &str,
        deployed_by: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO deploy_markers (id, service_name, version, commit_sha, description, environment, deployed_by) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![id, service_name, version, commit_sha, description, environment, deployed_by],
        )?;
        Ok(())
    }

    pub fn list_deploy_markers(
        &self,
        service_name: Option<&str>,
        from: Option<&str>,
        to: Option<&str>,
    ) -> anyhow::Result<Vec<crate::models::deploy::DeployMarker>> {
        let conn = self.conn.lock().unwrap();
        let mut sql = "SELECT id, service_name, version, commit_sha, description, environment, deployed_by, deployed_at FROM deploy_markers WHERE 1=1".to_string();
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(sn) = service_name {
            sql.push_str(&format!(" AND service_name = ?{}", param_values.len() + 1));
            param_values.push(Box::new(sn.to_string()));
        }
        if let Some(f) = from {
            sql.push_str(&format!(" AND deployed_at >= ?{}", param_values.len() + 1));
            param_values.push(Box::new(f.to_string()));
        }
        if let Some(t) = to {
            sql.push_str(&format!(" AND deployed_at <= ?{}", param_values.len() + 1));
            param_values.push(Box::new(t.to_string()));
        }
        sql.push_str(" ORDER BY deployed_at DESC LIMIT 100");

        let params_ref: Vec<&dyn rusqlite::types::ToSql> = param_values.iter().map(|p| p.as_ref()).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map(params_ref.as_slice(), |row| {
                Ok(crate::models::deploy::DeployMarker {
                    id: row.get(0)?,
                    service_name: row.get(1)?,
                    version: row.get(2)?,
                    commit_sha: row.get(3)?,
                    description: row.get(4)?,
                    environment: row.get(5)?,
                    deployed_by: row.get(6)?,
                    deployed_at: row.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // ── SLO operations ──

    pub fn list_slos(&self) -> anyhow::Result<Vec<crate::models::slo::Slo>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, enabled, service_name, window_type, target_percentage, \
             good_filters, total_filters, eval_interval_secs, notification_channel_ids, state, \
             error_budget_remaining, good_count, total_count, last_eval_at, last_breached_at, \
             created_at, updated_at FROM slos ORDER BY created_at DESC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(crate::models::slo::Slo {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    enabled: row.get(3)?,
                    service_name: row.get(4)?,
                    window_type: row.get(5)?,
                    target_percentage: row.get(6)?,
                    good_filters: row.get(7)?,
                    total_filters: row.get(8)?,
                    eval_interval_secs: row.get(9)?,
                    notification_channel_ids: row.get(10)?,
                    state: row.get(11)?,
                    error_budget_remaining: row.get(12)?,
                    good_count: row.get(13)?,
                    total_count: row.get(14)?,
                    last_eval_at: row.get(15)?,
                    last_breached_at: row.get(16)?,
                    created_at: row.get(17)?,
                    updated_at: row.get(18)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_slo(&self, id: &str) -> anyhow::Result<Option<crate::models::slo::Slo>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, enabled, service_name, window_type, target_percentage, \
             good_filters, total_filters, eval_interval_secs, notification_channel_ids, state, \
             error_budget_remaining, good_count, total_count, last_eval_at, last_breached_at, \
             created_at, updated_at FROM slos WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            Ok(crate::models::slo::Slo {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                enabled: row.get(3)?,
                service_name: row.get(4)?,
                window_type: row.get(5)?,
                target_percentage: row.get(6)?,
                good_filters: row.get(7)?,
                total_filters: row.get(8)?,
                eval_interval_secs: row.get(9)?,
                notification_channel_ids: row.get(10)?,
                state: row.get(11)?,
                error_budget_remaining: row.get(12)?,
                good_count: row.get(13)?,
                total_count: row.get(14)?,
                last_eval_at: row.get(15)?,
                last_breached_at: row.get(16)?,
                created_at: row.get(17)?,
                updated_at: row.get(18)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    pub fn create_slo(
        &self,
        id: &str,
        name: &str,
        description: &str,
        enabled: bool,
        service_name: &str,
        window_type: &str,
        target_percentage: f64,
        good_filters: &str,
        total_filters: &str,
        eval_interval_secs: i64,
        notification_channel_ids: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO slos (id, name, description, enabled, service_name, window_type, \
             target_percentage, good_filters, total_filters, eval_interval_secs, notification_channel_ids) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![id, name, description, enabled, service_name, window_type,
                    target_percentage, good_filters, total_filters, eval_interval_secs, notification_channel_ids],
        )?;
        Ok(())
    }

    pub fn update_slo(
        &self,
        id: &str,
        name: &str,
        description: &str,
        enabled: bool,
        service_name: &str,
        window_type: &str,
        target_percentage: f64,
        good_filters: &str,
        total_filters: &str,
        eval_interval_secs: i64,
        notification_channel_ids: &str,
    ) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE slos SET name = ?2, description = ?3, enabled = ?4, service_name = ?5, \
             window_type = ?6, target_percentage = ?7, good_filters = ?8, total_filters = ?9, \
             eval_interval_secs = ?10, notification_channel_ids = ?11, \
             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?1",
            params![id, name, description, enabled, service_name, window_type,
                    target_percentage, good_filters, total_filters, eval_interval_secs, notification_channel_ids],
        )?;
        Ok(count > 0)
    }

    pub fn delete_slo(&self, id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute("DELETE FROM slos WHERE id = ?1", params![id])?;
        Ok(count > 0)
    }

    pub fn get_due_slos(&self, now: &str) -> anyhow::Result<Vec<crate::models::slo::Slo>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, enabled, service_name, window_type, target_percentage, \
             good_filters, total_filters, eval_interval_secs, notification_channel_ids, state, \
             error_budget_remaining, good_count, total_count, last_eval_at, last_breached_at, \
             created_at, updated_at FROM slos \
             WHERE enabled = 1 AND (last_eval_at IS NULL OR \
             strftime('%s', ?1) - strftime('%s', last_eval_at) >= eval_interval_secs)",
        )?;
        let rows = stmt
            .query_map(params![now], |row| {
                Ok(crate::models::slo::Slo {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    enabled: row.get(3)?,
                    service_name: row.get(4)?,
                    window_type: row.get(5)?,
                    target_percentage: row.get(6)?,
                    good_filters: row.get(7)?,
                    total_filters: row.get(8)?,
                    eval_interval_secs: row.get(9)?,
                    notification_channel_ids: row.get(10)?,
                    state: row.get(11)?,
                    error_budget_remaining: row.get(12)?,
                    good_count: row.get(13)?,
                    total_count: row.get(14)?,
                    last_eval_at: row.get(15)?,
                    last_breached_at: row.get(16)?,
                    created_at: row.get(17)?,
                    updated_at: row.get(18)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn update_slo_state(
        &self,
        id: &str,
        state: &str,
        error_budget_remaining: f64,
        good_count: i64,
        total_count: i64,
        last_eval_at: &str,
        last_breached_at: Option<&str>,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        match last_breached_at {
            Some(t) => {
                conn.execute(
                    "UPDATE slos SET state = ?2, error_budget_remaining = ?3, good_count = ?4, \
                     total_count = ?5, last_eval_at = ?6, last_breached_at = ?7 WHERE id = ?1",
                    params![id, state, error_budget_remaining, good_count, total_count, last_eval_at, t],
                )?;
            }
            None => {
                conn.execute(
                    "UPDATE slos SET state = ?2, error_budget_remaining = ?3, good_count = ?4, \
                     total_count = ?5, last_eval_at = ?6 WHERE id = ?1",
                    params![id, state, error_budget_remaining, good_count, total_count, last_eval_at],
                )?;
            }
        }
        Ok(())
    }

    pub fn create_slo_event(
        &self,
        id: &str,
        slo_id: &str,
        state: &str,
        good_count: i64,
        total_count: i64,
        error_budget_remaining: f64,
        message: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO slo_events (id, slo_id, state, good_count, total_count, error_budget_remaining, message) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![id, slo_id, state, good_count, total_count, error_budget_remaining, message],
        )?;
        Ok(())
    }

    pub fn list_slo_events(
        &self,
        slo_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::slo::SloEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, slo_id, state, good_count, total_count, error_budget_remaining, message, created_at \
             FROM slo_events WHERE slo_id = ?1 ORDER BY created_at DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![slo_id, limit], |row| {
                Ok(crate::models::slo::SloEvent {
                    id: row.get(0)?,
                    slo_id: row.get(1)?,
                    state: row.get(2)?,
                    good_count: row.get(3)?,
                    total_count: row.get(4)?,
                    error_budget_remaining: row.get(5)?,
                    message: row.get(6)?,
                    created_at: row.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // ── API Key operations ──

    pub fn list_api_keys(&self) -> anyhow::Result<Vec<(String, String, String, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, prefix, created_at FROM api_keys ORDER BY created_at DESC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn create_api_key(&self, id: &str, name: &str, key_hash: &str, prefix: &str) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO api_keys (id, name, key_hash, prefix) VALUES (?1, ?2, ?3, ?4)",
            params![id, name, key_hash, prefix],
        )?;
        Ok(())
    }

    pub fn delete_api_key(&self, id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute("DELETE FROM api_keys WHERE id = ?1", params![id])?;
        Ok(count > 0)
    }

    // ── Anomaly rule operations ──

    pub fn list_anomaly_rules(&self) -> anyhow::Result<Vec<crate::models::anomaly::AnomalyRule>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, enabled, source, pattern, query, service_name, \
             apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, \
             split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, \
             created_at, updated_at FROM anomaly_rules ORDER BY created_at DESC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(crate::models::anomaly::AnomalyRule {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    enabled: row.get(3)?,
                    source: row.get(4)?,
                    pattern: row.get(5)?,
                    query: row.get(6)?,
                    service_name: row.get(7)?,
                    apm_metric: row.get(8)?,
                    sensitivity: row.get(9)?,
                    alpha: row.get(10)?,
                    eval_interval_secs: row.get(11)?,
                    window_secs: row.get(12)?,
                    split_labels: row.get(13)?,
                    notification_channel_ids: row.get(14)?,
                    state: row.get(15)?,
                    last_eval_at: row.get(16)?,
                    last_triggered_at: row.get(17)?,
                    created_at: row.get(18)?,
                    updated_at: row.get(19)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_anomaly_rule(&self, id: &str) -> anyhow::Result<Option<crate::models::anomaly::AnomalyRule>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, enabled, source, pattern, query, service_name, \
             apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, \
             split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, \
             created_at, updated_at FROM anomaly_rules WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            Ok(crate::models::anomaly::AnomalyRule {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                enabled: row.get(3)?,
                source: row.get(4)?,
                pattern: row.get(5)?,
                query: row.get(6)?,
                service_name: row.get(7)?,
                apm_metric: row.get(8)?,
                sensitivity: row.get(9)?,
                alpha: row.get(10)?,
                eval_interval_secs: row.get(11)?,
                window_secs: row.get(12)?,
                split_labels: row.get(13)?,
                notification_channel_ids: row.get(14)?,
                state: row.get(15)?,
                last_eval_at: row.get(16)?,
                last_triggered_at: row.get(17)?,
                created_at: row.get(18)?,
                updated_at: row.get(19)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    pub fn create_anomaly_rule(
        &self,
        id: &str,
        name: &str,
        description: &str,
        enabled: bool,
        source: &str,
        pattern: &str,
        query: &str,
        service_name: &str,
        apm_metric: &str,
        sensitivity: f64,
        alpha: f64,
        eval_interval_secs: i64,
        window_secs: i64,
        split_labels: &str,
        notification_channel_ids: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO anomaly_rules (id, name, description, enabled, source, pattern, query, \
             service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, \
             split_labels, notification_channel_ids) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![id, name, description, enabled, source, pattern, query,
                    service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs,
                    split_labels, notification_channel_ids],
        )?;
        Ok(())
    }

    pub fn update_anomaly_rule(
        &self,
        id: &str,
        name: &str,
        description: &str,
        enabled: bool,
        source: &str,
        pattern: &str,
        query: &str,
        service_name: &str,
        apm_metric: &str,
        sensitivity: f64,
        alpha: f64,
        eval_interval_secs: i64,
        window_secs: i64,
        split_labels: &str,
        notification_channel_ids: &str,
    ) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE anomaly_rules SET name = ?2, description = ?3, enabled = ?4, source = ?5, \
             pattern = ?6, query = ?7, service_name = ?8, apm_metric = ?9, sensitivity = ?10, \
             alpha = ?11, eval_interval_secs = ?12, window_secs = ?13, \
             split_labels = ?14, notification_channel_ids = ?15, \
             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?1",
            params![id, name, description, enabled, source, pattern, query,
                    service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs,
                    split_labels, notification_channel_ids],
        )?;
        Ok(count > 0)
    }

    pub fn delete_anomaly_rule(&self, id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute("DELETE FROM anomaly_rules WHERE id = ?1", params![id])?;
        Ok(count > 0)
    }

    pub fn get_due_anomaly_rules(&self, now: &str) -> anyhow::Result<Vec<crate::models::anomaly::AnomalyRule>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, enabled, source, pattern, query, service_name, \
             apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, \
             split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, \
             created_at, updated_at FROM anomaly_rules \
             WHERE enabled = 1 AND (last_eval_at IS NULL OR \
             strftime('%s', ?1) - strftime('%s', last_eval_at) >= eval_interval_secs)",
        )?;
        let rows = stmt
            .query_map(params![now], |row| {
                Ok(crate::models::anomaly::AnomalyRule {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    enabled: row.get(3)?,
                    source: row.get(4)?,
                    pattern: row.get(5)?,
                    query: row.get(6)?,
                    service_name: row.get(7)?,
                    apm_metric: row.get(8)?,
                    sensitivity: row.get(9)?,
                    alpha: row.get(10)?,
                    eval_interval_secs: row.get(11)?,
                    window_secs: row.get(12)?,
                    split_labels: row.get(13)?,
                    notification_channel_ids: row.get(14)?,
                    state: row.get(15)?,
                    last_eval_at: row.get(16)?,
                    last_triggered_at: row.get(17)?,
                    created_at: row.get(18)?,
                    updated_at: row.get(19)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn update_anomaly_state(
        &self,
        id: &str,
        state: &str,
        last_eval_at: &str,
        last_triggered_at: Option<&str>,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        match last_triggered_at {
            Some(t) => {
                conn.execute(
                    "UPDATE anomaly_rules SET state = ?2, last_eval_at = ?3, last_triggered_at = ?4 WHERE id = ?1",
                    params![id, state, last_eval_at, t],
                )?;
            }
            None => {
                conn.execute(
                    "UPDATE anomaly_rules SET state = ?2, last_eval_at = ?3 WHERE id = ?1",
                    params![id, state, last_eval_at],
                )?;
            }
        }
        Ok(())
    }

    // ── Anomaly event operations ──

    pub fn get_anomaly_event(&self, id: &str) -> anyhow::Result<Option<crate::models::anomaly::AnomalyEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, rule_id, state, metric, value, expected, deviation, message, created_at \
             FROM anomaly_events WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            Ok(crate::models::anomaly::AnomalyEvent {
                id: row.get(0)?,
                rule_id: row.get(1)?,
                state: row.get(2)?,
                metric: row.get(3)?,
                value: row.get(4)?,
                expected: row.get(5)?,
                deviation: row.get(6)?,
                message: row.get(7)?,
                created_at: row.get(8)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    pub fn create_anomaly_event(
        &self,
        id: &str,
        rule_id: &str,
        state: &str,
        metric: &str,
        value: f64,
        expected: f64,
        deviation: f64,
        message: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO anomaly_events (id, rule_id, state, metric, value, expected, deviation, message) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![id, rule_id, state, metric, value, expected, deviation, message],
        )?;
        Ok(())
    }

    pub fn list_anomaly_events(
        &self,
        rule_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::anomaly::AnomalyEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, rule_id, state, metric, value, expected, deviation, message, created_at \
             FROM anomaly_events WHERE rule_id = ?1 ORDER BY created_at DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![rule_id, limit], |row| {
                Ok(crate::models::anomaly::AnomalyEvent {
                    id: row.get(0)?,
                    rule_id: row.get(1)?,
                    state: row.get(2)?,
                    metric: row.get(3)?,
                    value: row.get(4)?,
                    expected: row.get(5)?,
                    deviation: row.get(6)?,
                    message: row.get(7)?,
                    created_at: row.get(8)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // ── Settings operations ──

    pub fn get_setting(&self, key: &str) -> anyhow::Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT value FROM settings WHERE key = ?1")?;
        let mut rows = stmt.query_map(params![key], |row| row.get::<_, String>(0))?;
        Ok(rows.next().transpose()?)
    }

    pub fn set_setting(&self, key: &str, value: &str) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = ?2",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn list_all_anomaly_events(
        &self,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::anomaly::AnomalyEventWithRule>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT e.id, e.rule_id, COALESCE(r.name, 'deleted rule') as rule_name, \
             e.state, e.metric, e.value, e.expected, e.deviation, e.message, e.created_at \
             FROM anomaly_events e \
             LEFT JOIN anomaly_rules r ON e.rule_id = r.id \
             ORDER BY e.created_at DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![limit], |row| {
                Ok(crate::models::anomaly::AnomalyEventWithRule {
                    id: row.get(0)?,
                    rule_id: row.get(1)?,
                    rule_name: row.get(2)?,
                    state: row.get(3)?,
                    metric: row.get(4)?,
                    value: row.get(5)?,
                    expected: row.get(6)?,
                    deviation: row.get(7)?,
                    message: row.get(8)?,
                    created_at: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}
