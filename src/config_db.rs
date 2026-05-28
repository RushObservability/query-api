use rusqlite::{Connection, params, OptionalExtension};
use std::sync::Mutex;
use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};

fn hash_password(password: &str) -> anyhow::Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| anyhow::anyhow!("argon2 hash failed: {e}"))
}

fn verify_password(password: &str, hash: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(hash) else { return false };
    Argon2::default().verify_password(password.as_bytes(), &parsed).is_ok()
}

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

        // Single canonical schema — all tables at their final column set.
        // For existing databases the CREATE TABLE IF NOT EXISTS statements are
        // no-ops; the ALTER TABLE blocks below add any missing columns.
        conn.execute_batch("
            -- ── Auth & multi-tenancy ─────────────────────────────────────────────
            CREATE TABLE IF NOT EXISTS tenants (
                id            TEXT PRIMARY KEY,
                name          TEXT NOT NULL UNIQUE,
                enabled       INTEGER NOT NULL DEFAULT 1,
                auth_required INTEGER NOT NULL DEFAULT 1,
                created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );

            CREATE TABLE IF NOT EXISTS groups (
                id          TEXT PRIMARY KEY,
                name        TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL DEFAULT '',
                scopes      TEXT NOT NULL DEFAULT '[\"all\"]',
                permissions TEXT NOT NULL DEFAULT '[\"read\"]',
                system      INTEGER NOT NULL DEFAULT 0,
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );

            CREATE TABLE IF NOT EXISTS users (
                id            TEXT PRIMARY KEY,
                username      TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                display_name  TEXT NOT NULL DEFAULT '',
                tenant_id     TEXT NOT NULL DEFAULT 'default' REFERENCES tenants(id),
                role          TEXT NOT NULL DEFAULT 'admin',
                enabled       INTEGER NOT NULL DEFAULT 1,
                auth_provider TEXT NOT NULL DEFAULT 'local',
                external_id   TEXT NOT NULL DEFAULT '',
                created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );

            CREATE TABLE IF NOT EXISTS sessions (
                token       TEXT PRIMARY KEY,
                user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                expires_at  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
            CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);

            CREATE TABLE IF NOT EXISTS group_tenants (
                group_id  TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
                tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
                PRIMARY KEY (group_id, tenant_id)
            );

            CREATE TABLE IF NOT EXISTS user_groups (
                user_id  TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                group_id TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
                PRIMARY KEY (user_id, group_id)
            );

            CREATE TABLE IF NOT EXISTS sso_providers (
                id                    TEXT PRIMARY KEY,
                name                  TEXT NOT NULL,
                protocol              TEXT NOT NULL,
                enabled               INTEGER NOT NULL DEFAULT 0,
                client_id             TEXT NOT NULL DEFAULT '',
                client_secret         TEXT NOT NULL DEFAULT '',
                issuer_url            TEXT NOT NULL DEFAULT '',
                oidc_scopes           TEXT NOT NULL DEFAULT 'openid profile email groups',
                groups_claim          TEXT NOT NULL DEFAULT 'groups',
                email_claim           TEXT NOT NULL DEFAULT 'email',
                first_name_claim      TEXT NOT NULL DEFAULT 'given_name',
                last_name_claim       TEXT NOT NULL DEFAULT 'family_name',
                jit_provisioning      INTEGER NOT NULL DEFAULT 1,
                default_group_id      TEXT NOT NULL DEFAULT '',
                saml_idp_metadata_url TEXT NOT NULL DEFAULT '',
                saml_idp_sso_url      TEXT NOT NULL DEFAULT '',
                saml_idp_cert         TEXT NOT NULL DEFAULT '',
                saml_sp_entity_id     TEXT NOT NULL DEFAULT '',
                created_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );

            CREATE TABLE IF NOT EXISTS idp_group_mappings (
                id            TEXT PRIMARY KEY,
                idp_group     TEXT NOT NULL,
                rush_group_id TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
                provider_id   TEXT NOT NULL DEFAULT 'default',
                created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                UNIQUE(idp_group, rush_group_id, provider_id)
            );

            CREATE TABLE IF NOT EXISTS sso_state (
                state      TEXT PRIMARY KEY,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );

            CREATE TABLE IF NOT EXISTS setup_tokens (
                token      TEXT PRIMARY KEY,
                purpose    TEXT NOT NULL,
                created_by TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                used       INTEGER NOT NULL DEFAULT 0,
                provider   TEXT NOT NULL DEFAULT '',
                hostname   TEXT NOT NULL DEFAULT ''
            );

            -- ── Core configuration ──────────────────────────────────────────────
            CREATE TABLE IF NOT EXISTS api_keys (
                id         TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
                key_hash   TEXT NOT NULL UNIQUE,
                prefix     TEXT NOT NULL,
                tenant_id  TEXT NOT NULL DEFAULT 'default' REFERENCES tenants(id),
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );

            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS custom_skills (
                id            TEXT PRIMARY KEY,
                name          TEXT NOT NULL UNIQUE,
                title         TEXT NOT NULL,
                description   TEXT NOT NULL,
                content       TEXT NOT NULL,
                allowed_tools TEXT NOT NULL DEFAULT '[]',
                enabled       INTEGER NOT NULL DEFAULT 1,
                created_by    TEXT NOT NULL DEFAULT '',
                created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );

            CREATE TABLE IF NOT EXISTS service_links (
                service_name   TEXT PRIMARY KEY,
                github_repo    TEXT NOT NULL,
                default_branch TEXT NOT NULL DEFAULT 'main',
                root_path      TEXT NOT NULL DEFAULT '',
                updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );

            -- ── Dashboards ──────────────────────────────────────────────────────
            CREATE TABLE IF NOT EXISTS dashboards (
                id          TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                tenant_id   TEXT NOT NULL DEFAULT 'default',
                owner_id    TEXT NOT NULL DEFAULT '',
                visibility  TEXT NOT NULL DEFAULT 'tenant'
                            CHECK(visibility IN ('private', 'tenant', 'global')),
                tags        TEXT NOT NULL DEFAULT '[]',
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

            CREATE TABLE IF NOT EXISTS dashboard_templates (
                id            TEXT PRIMARY KEY,
                name          TEXT NOT NULL,
                description   TEXT NOT NULL DEFAULT '',
                category      TEXT NOT NULL DEFAULT 'general',
                is_builtin    INTEGER NOT NULL DEFAULT 0,
                template_json TEXT NOT NULL,
                tags          TEXT NOT NULL DEFAULT '[]',
                created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_templates_category ON dashboard_templates(category);
            CREATE INDEX IF NOT EXISTS idx_templates_builtin   ON dashboard_templates(is_builtin);

            -- ── Notifications ───────────────────────────────────────────────────
            CREATE TABLE IF NOT EXISTS notification_channels (
                id           TEXT PRIMARY KEY,
                tenant_id    TEXT NOT NULL DEFAULT 'default',
                name         TEXT NOT NULL,
                channel_type TEXT NOT NULL CHECK(channel_type IN ('slack','email','webhook','pagerduty','opsgenie')),
                config       TEXT NOT NULL DEFAULT '{}',
                enabled      INTEGER NOT NULL DEFAULT 1,
                created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_notif_channels_tenant ON notification_channels(tenant_id, enabled);

            CREATE TABLE IF NOT EXISTS notification_log (
                id         TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL,
                tenant_id  TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                alert_name TEXT NOT NULL,
                severity   TEXT NOT NULL DEFAULT '',
                status     TEXT NOT NULL,
                error      TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_notif_log_tenant  ON notification_log(tenant_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_notif_log_channel ON notification_log(channel_id, created_at DESC);

            -- ── Alerting ────────────────────────────────────────────────────────
            CREATE TABLE IF NOT EXISTS alert_rules (
                id                       TEXT PRIMARY KEY,
                name                     TEXT NOT NULL,
                description              TEXT NOT NULL DEFAULT '',
                enabled                  INTEGER NOT NULL DEFAULT 1,
                signal_type              TEXT NOT NULL DEFAULT 'apm',
                query_config             TEXT NOT NULL,
                condition_op             TEXT NOT NULL CHECK(condition_op IN ('>','>=','<','<=','=','!=')),
                condition_threshold      REAL NOT NULL,
                eval_interval_secs       INTEGER NOT NULL DEFAULT 60,
                notification_channel_ids TEXT NOT NULL DEFAULT '[]',
                state                    TEXT NOT NULL DEFAULT 'ok' CHECK(state IN ('ok','alerting','no_data')),
                last_eval_at             TEXT,
                last_triggered_at        TEXT,
                created_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_alert_rules_state ON alert_rules(state, enabled);

            CREATE TABLE IF NOT EXISTS alert_events (
                id         TEXT PRIMARY KEY,
                rule_id    TEXT NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
                state      TEXT NOT NULL,
                value      REAL NOT NULL,
                threshold  REAL NOT NULL,
                message    TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_alert_events_rule  ON alert_events(rule_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_alert_events_state ON alert_events(state, created_at DESC);

            CREATE TABLE IF NOT EXISTS anomaly_rules (
                id                       TEXT PRIMARY KEY,
                name                     TEXT NOT NULL,
                description              TEXT NOT NULL DEFAULT '',
                enabled                  INTEGER NOT NULL DEFAULT 1,
                source                   TEXT NOT NULL CHECK(source IN ('prometheus','apm')),
                pattern                  TEXT NOT NULL DEFAULT '',
                query                    TEXT NOT NULL DEFAULT '',
                service_name             TEXT NOT NULL DEFAULT '',
                apm_metric               TEXT NOT NULL DEFAULT '',
                sensitivity              REAL NOT NULL DEFAULT 3.0,
                alpha                    REAL NOT NULL DEFAULT 0.25,
                eval_interval_secs       INTEGER NOT NULL DEFAULT 300,
                window_secs              INTEGER NOT NULL DEFAULT 3600,
                notification_channel_ids TEXT NOT NULL DEFAULT '[]',
                split_labels             TEXT NOT NULL DEFAULT '[]',
                state                    TEXT NOT NULL DEFAULT 'normal' CHECK(state IN ('normal','anomalous','no_data')),
                last_eval_at             TEXT,
                last_triggered_at        TEXT,
                created_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
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
            CREATE INDEX IF NOT EXISTS idx_anomaly_events_rule  ON anomaly_events(rule_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_anomaly_events_state ON anomaly_events(state, created_at DESC);

            CREATE TABLE IF NOT EXISTS monitors (
                id                    TEXT PRIMARY KEY,
                tenant_id             TEXT NOT NULL DEFAULT 'default',
                name                  TEXT NOT NULL,
                type                  TEXT NOT NULL CHECK(type IN ('metric','log','apm','composite')),
                query_config          TEXT NOT NULL,
                critical              REAL,
                critical_recovery     REAL,
                warning               REAL,
                warning_recovery      REAL,
                comparator            TEXT NOT NULL DEFAULT 'above' CHECK(comparator IN ('above','below')),
                eval_window_secs      INTEGER NOT NULL DEFAULT 300,
                eval_interval_secs    INTEGER NOT NULL DEFAULT 60,
                group_by              TEXT NOT NULL DEFAULT '[]',
                state                 TEXT NOT NULL DEFAULT 'ok' CHECK(state IN ('ok','warn','alert','no_data')),
                group_states          TEXT NOT NULL DEFAULT '{}',
                no_data_action        TEXT NOT NULL DEFAULT 'show' CHECK(no_data_action IN ('show','notify','resolve')),
                no_data_timeframe     INTEGER NOT NULL DEFAULT 600,
                auto_resolve_hours    INTEGER,
                message               TEXT NOT NULL DEFAULT '',
                notification_channels TEXT NOT NULL DEFAULT '[]',
                renotify_interval     INTEGER,
                tags                  TEXT NOT NULL DEFAULT '[]',
                priority              INTEGER CHECK(priority BETWEEN 1 AND 5),
                enabled               INTEGER NOT NULL DEFAULT 1,
                composite_formula     TEXT NOT NULL DEFAULT '',
                composite_monitor_ids TEXT NOT NULL DEFAULT '[]',
                last_eval_at          TEXT,
                last_triggered_at     TEXT,
                created_by            TEXT NOT NULL DEFAULT '',
                created_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_monitors_tenant  ON monitors(tenant_id);
            CREATE INDEX IF NOT EXISTS idx_monitors_state   ON monitors(state);
            CREATE INDEX IF NOT EXISTS idx_monitors_enabled ON monitors(enabled);

            CREATE TABLE IF NOT EXISTS monitor_events (
                id         TEXT PRIMARY KEY,
                monitor_id TEXT NOT NULL REFERENCES monitors(id) ON DELETE CASCADE,
                tenant_id  TEXT NOT NULL,
                group_key  TEXT NOT NULL DEFAULT '',
                prev_state TEXT NOT NULL,
                new_state  TEXT NOT NULL,
                value      REAL,
                threshold  REAL,
                message    TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_monitor_events_monitor ON monitor_events(monitor_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_monitor_events_tenant  ON monitor_events(tenant_id, created_at DESC);

            -- ── SLOs ────────────────────────────────────────────────────────────
            CREATE TABLE IF NOT EXISTS slos (
                id                       TEXT PRIMARY KEY,
                name                     TEXT NOT NULL,
                description              TEXT NOT NULL DEFAULT '',
                enabled                  INTEGER NOT NULL DEFAULT 1,
                tenant_id                TEXT NOT NULL DEFAULT 'default',
                slo_type                 TEXT NOT NULL DEFAULT 'trace' CHECK(slo_type IN ('trace','metric')),
                service_name             TEXT NOT NULL,
                metric_name              TEXT NOT NULL DEFAULT '',
                window_type              TEXT NOT NULL CHECK(window_type IN ('rolling_1h','rolling_24h','rolling_7d','rolling_30d')),
                target_percentage        REAL NOT NULL,
                error_filters            TEXT NOT NULL,
                total_filters            TEXT NOT NULL,
                eval_interval_secs       INTEGER NOT NULL DEFAULT 60,
                notification_channel_ids TEXT NOT NULL DEFAULT '[]',
                indicator_type           TEXT NOT NULL DEFAULT 'availability',
                threshold_ms             REAL,
                threshold_value          REAL,
                threshold_op             TEXT,
                state                    TEXT NOT NULL DEFAULT 'compliant' CHECK(state IN ('compliant','breaching','no_data')),
                error_budget_remaining   REAL,
                error_count              INTEGER,
                total_count              INTEGER,
                last_eval_at             TEXT,
                last_breached_at         TEXT,
                created_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_slos_service ON slos(service_name);

            CREATE TABLE IF NOT EXISTS slo_events (
                id                     TEXT PRIMARY KEY,
                slo_id                 TEXT NOT NULL REFERENCES slos(id) ON DELETE CASCADE,
                state                  TEXT NOT NULL,
                error_count            INTEGER NOT NULL,
                total_count            INTEGER NOT NULL,
                error_budget_remaining REAL NOT NULL,
                message                TEXT NOT NULL,
                created_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_slo_events_slo   ON slo_events(slo_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_slo_events_state ON slo_events(state, created_at DESC);

            -- ── Deploy markers ──────────────────────────────────────────────────
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

            -- ── Detection / SIEM ────────────────────────────────────────────────
            CREATE TABLE IF NOT EXISTS detection_rules (
                id                TEXT PRIMARY KEY,
                tenant_id         TEXT NOT NULL DEFAULT 'default' REFERENCES tenants(id),
                name              TEXT NOT NULL,
                description       TEXT NOT NULL DEFAULT '',
                query_sql         TEXT NOT NULL,
                interval_secs     INTEGER NOT NULL DEFAULT 300,
                threshold         INTEGER NOT NULL DEFAULT 1,
                severity          TEXT NOT NULL DEFAULT 'medium',
                window_secs       INTEGER NOT NULL DEFAULT 300,
                enabled           INTEGER NOT NULL DEFAULT 1,
                channels          TEXT NOT NULL DEFAULT '[]',
                created_by        TEXT NOT NULL DEFAULT '',
                last_eval_at      TEXT,
                last_triggered_at TEXT,
                created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_detection_rules_tenant ON detection_rules(tenant_id);

            CREATE TABLE IF NOT EXISTS detection_events (
                id          TEXT PRIMARY KEY,
                rule_id     TEXT NOT NULL REFERENCES detection_rules(id) ON DELETE CASCADE,
                tenant_id   TEXT NOT NULL,
                severity    TEXT NOT NULL,
                match_count INTEGER NOT NULL DEFAULT 0,
                sample_data TEXT NOT NULL DEFAULT '[]',
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_detection_events_rule   ON detection_events(rule_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_detection_events_tenant ON detection_events(tenant_id, created_at DESC);

            -- ── Retention ───────────────────────────────────────────────────────
            CREATE TABLE IF NOT EXISTS tenant_retention (
                tenant_id   TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
                signal      TEXT NOT NULL,
                retain_days INTEGER NOT NULL,
                PRIMARY KEY (tenant_id, signal)
            );

            -- ── Alert Maintenance Windows ────────────────────────────────────────
            CREATE TABLE IF NOT EXISTS alert_maintenance_windows (
                id          TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                scope       TEXT NOT NULL DEFAULT 'all',
                starts_at   TEXT NOT NULL,
                ends_at     TEXT NOT NULL,
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );

            -- ── Trace Funnels ────────────────────────────────────────────────────
            CREATE TABLE IF NOT EXISTS trace_funnels (
                id         TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
                steps_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
        ")?;

        // ── Backward-compat column additions for existing databases ──────────
        // Each block is a no-op on fresh installs (column already in CREATE TABLE).

        for (table, col, def) in [
            ("anomaly_rules", "split_labels",         "TEXT NOT NULL DEFAULT '[]'"),
            ("alert_rules",   "signal_type",          "TEXT NOT NULL DEFAULT 'apm'"),
            ("slos",          "slo_type",             "TEXT NOT NULL DEFAULT 'trace'"),
            ("slos",          "metric_name",          "TEXT NOT NULL DEFAULT ''"),
            ("slos",          "indicator_type",       "TEXT NOT NULL DEFAULT 'availability'"),
            ("slos",          "threshold_ms",         "REAL"),
            ("slos",          "threshold_value",      "REAL"),
            ("slos",          "threshold_op",         "TEXT"),
            ("slos",          "tenant_id",            "TEXT NOT NULL DEFAULT 'default'"),
            ("api_keys",      "tenant_id",            "TEXT NOT NULL DEFAULT 'default'"),
            ("tenants",       "enabled",              "INTEGER NOT NULL DEFAULT 1"),
            ("tenants",       "auth_required",        "INTEGER NOT NULL DEFAULT 1"),
            ("users",         "auth_provider",        "TEXT NOT NULL DEFAULT 'local'"),
            ("users",         "external_id",          "TEXT NOT NULL DEFAULT ''"),
            ("sso_providers", "saml_idp_metadata_url","TEXT NOT NULL DEFAULT ''"),
            ("sso_providers", "saml_idp_sso_url",     "TEXT NOT NULL DEFAULT ''"),
            ("sso_providers", "saml_idp_cert",        "TEXT NOT NULL DEFAULT ''"),
            ("sso_providers", "saml_sp_entity_id",    "TEXT NOT NULL DEFAULT ''"),
            ("sso_providers", "email_claim",          "TEXT NOT NULL DEFAULT 'email'"),
            ("sso_providers", "first_name_claim",     "TEXT NOT NULL DEFAULT 'given_name'"),
            ("sso_providers", "last_name_claim",      "TEXT NOT NULL DEFAULT 'family_name'"),
            ("setup_tokens",  "provider",             "TEXT NOT NULL DEFAULT ''"),
            ("setup_tokens",  "hostname",             "TEXT NOT NULL DEFAULT ''"),
            ("dashboards",    "tenant_id",            "TEXT NOT NULL DEFAULT 'default'"),
            ("dashboards",    "owner_id",             "TEXT NOT NULL DEFAULT ''"),
            ("dashboards",    "visibility",           "TEXT NOT NULL DEFAULT 'tenant'"),
            ("dashboards",    "tags",                 "TEXT NOT NULL DEFAULT '[]'"),
            ("trace_funnels", "tenant_id",            "TEXT NOT NULL DEFAULT 'default'"),
        ] {
            let has: bool = conn
                .prepare(&format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = '{col}'"))?
                .query_row([], |row| row.get(0))?;
            if !has {
                conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {col} {def};"))?;
            }
        }

        // Rename old column names to their current names.
        for (table, old_col, new_col) in [
            ("slos",       "good_filters", "error_filters"),
            ("slos",       "good_count",   "error_count"),
            ("slo_events", "good_count",   "error_count"),
        ] {
            let has_old: bool = conn
                .prepare(&format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = '{old_col}'"))?
                .query_row([], |row| row.get(0))?;
            if has_old {
                conn.execute_batch(&format!("ALTER TABLE {table} RENAME COLUMN {old_col} TO {new_col};"))?;
            }
        }

        // Ensure indexes that depend on migrated columns exist.
        conn.execute_batch("
            CREATE INDEX IF NOT EXISTS idx_slos_tenant_state  ON slos(tenant_id, state, enabled);
            CREATE INDEX IF NOT EXISTS idx_dashboards_tenant  ON dashboards(tenant_id);
            CREATE INDEX IF NOT EXISTS idx_dashboards_owner   ON dashboards(owner_id);
            CREATE INDEX IF NOT EXISTS idx_dashboards_visibility ON dashboards(visibility);
        ")?;

        Ok(())
    }


    // ── Dashboard operations ──

    /// List dashboards visible to the given user: own private + tenant-visible + global.
    pub fn list_dashboards(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> anyhow::Result<Vec<crate::models::dashboard::Dashboard>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, tenant_id, owner_id, visibility, tags, created_at, updated_at \
             FROM dashboards \
             WHERE (visibility = 'private' AND owner_id = ?1) \
                OR (visibility = 'tenant' AND tenant_id = ?2) \
                OR (visibility = 'global') \
             ORDER BY updated_at DESC",
        )?;
        let rows = stmt
            .query_map(params![user_id, tenant_id], |row| {
                let tags_str: String = row.get(6)?;
                let tags = serde_json::from_str(&tags_str)
                    .unwrap_or(serde_json::Value::Array(vec![]));
                Ok(crate::models::dashboard::Dashboard {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    tenant_id: row.get(3)?,
                    owner_id: row.get(4)?,
                    visibility: row.get(5)?,
                    tags,
                    created_at: row.get(7)?,
                    updated_at: row.get(8)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Get a dashboard, checking visibility for the given user/tenant.
    /// Returns None (treated as 404) if the dashboard exists but the user lacks visibility.
    pub fn get_dashboard(
        &self,
        id: &str,
        tenant_id: &str,
        user_id: &str,
    ) -> anyhow::Result<Option<crate::models::dashboard::Dashboard>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, tenant_id, owner_id, visibility, tags, created_at, updated_at \
             FROM dashboards \
             WHERE id = ?1 \
               AND ((visibility = 'private' AND owner_id = ?2) \
                 OR (visibility = 'tenant' AND tenant_id = ?3) \
                 OR (visibility = 'global'))",
        )?;
        let mut rows = stmt.query_map(params![id, user_id, tenant_id], |row| {
            let tags_str: String = row.get(6)?;
            let tags = serde_json::from_str(&tags_str)
                .unwrap_or(serde_json::Value::Array(vec![]));
            Ok(crate::models::dashboard::Dashboard {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                tenant_id: row.get(3)?,
                owner_id: row.get(4)?,
                visibility: row.get(5)?,
                tags,
                created_at: row.get(7)?,
                updated_at: row.get(8)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    /// Get a dashboard without visibility checks (for internal use like export).
    pub fn get_dashboard_unchecked(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<crate::models::dashboard::Dashboard>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, tenant_id, owner_id, visibility, tags, created_at, updated_at \
             FROM dashboards WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            let tags_str: String = row.get(6)?;
            let tags = serde_json::from_str(&tags_str)
                .unwrap_or(serde_json::Value::Array(vec![]));
            Ok(crate::models::dashboard::Dashboard {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                tenant_id: row.get(3)?,
                owner_id: row.get(4)?,
                visibility: row.get(5)?,
                tags,
                created_at: row.get(7)?,
                updated_at: row.get(8)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    pub fn create_dashboard(
        &self,
        id: &str,
        name: &str,
        description: &str,
        tenant_id: &str,
        owner_id: &str,
        visibility: &str,
        tags: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO dashboards (id, name, description, tenant_id, owner_id, visibility, tags) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![id, name, description, tenant_id, owner_id, visibility, tags],
        )?;
        Ok(())
    }

    pub fn update_dashboard(
        &self,
        id: &str,
        name: &str,
        description: &str,
        visibility: &str,
        tags: &str,
        tenant_id: &str,
        user_id: &str,
        user_role: &str,
    ) -> anyhow::Result<bool> {
        // First check the dashboard exists and user has edit permission
        let dash = self.get_dashboard(id, tenant_id, user_id)?;
        let dash = match dash {
            Some(d) => d,
            None => return Ok(false),
        };
        // Permission: owner can always edit; admin/editor can edit tenant dashboards
        let can_edit = dash.owner_id == user_id
            || (dash.visibility == "tenant" && dash.tenant_id == tenant_id && (user_role == "admin" || user_role == "editor"))
            || (dash.visibility == "global" && user_role == "admin")
            || dash.owner_id.is_empty(); // system dashboards editable by anyone for backward compat
        if !can_edit {
            return Ok(false);
        }
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE dashboards SET name = ?2, description = ?3, visibility = ?4, tags = ?5, \
             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?1",
            params![id, name, description, visibility, tags],
        )?;
        Ok(count > 0)
    }

    pub fn delete_dashboard(
        &self,
        id: &str,
        tenant_id: &str,
        user_id: &str,
        user_role: &str,
    ) -> anyhow::Result<bool> {
        // Check visibility and permission before deleting
        let dash = self.get_dashboard(id, tenant_id, user_id)?;
        let dash = match dash {
            Some(d) => d,
            None => return Ok(false),
        };
        let can_delete = dash.owner_id == user_id
            || user_role == "admin"
            || dash.owner_id.is_empty(); // system dashboards deletable by anyone for backward compat
        if !can_delete {
            return Ok(false);
        }
        let conn = self.conn.lock().unwrap();
        let count = conn.execute("DELETE FROM dashboards WHERE id = ?1", params![id])?;
        Ok(count > 0)
    }

    // ── Template operations ──

    pub fn list_dashboard_templates(&self) -> anyhow::Result<Vec<crate::models::dashboard::DashboardTemplate>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, category, is_builtin, template_json, tags, created_at \
             FROM dashboard_templates ORDER BY is_builtin DESC, name ASC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                let tj_str: String = row.get(5)?;
                let template_json = serde_json::from_str(&tj_str)
                    .unwrap_or(serde_json::Value::Object(Default::default()));
                let tags_str: String = row.get(6)?;
                let tags = serde_json::from_str(&tags_str)
                    .unwrap_or(serde_json::Value::Array(vec![]));
                Ok(crate::models::dashboard::DashboardTemplate {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    category: row.get(3)?,
                    is_builtin: row.get::<_, i32>(4)? != 0,
                    template_json,
                    tags,
                    created_at: row.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_dashboard_template(&self, id: &str) -> anyhow::Result<Option<crate::models::dashboard::DashboardTemplate>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, category, is_builtin, template_json, tags, created_at \
             FROM dashboard_templates WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            let tj_str: String = row.get(5)?;
            let template_json = serde_json::from_str(&tj_str)
                .unwrap_or(serde_json::Value::Object(Default::default()));
            let tags_str: String = row.get(6)?;
            let tags = serde_json::from_str(&tags_str)
                .unwrap_or(serde_json::Value::Array(vec![]));
            Ok(crate::models::dashboard::DashboardTemplate {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                category: row.get(3)?,
                is_builtin: row.get::<_, i32>(4)? != 0,
                template_json,
                tags,
                created_at: row.get(7)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    /// Seed built-in dashboard templates on startup. Skips if already present.
    pub fn ensure_default_templates(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .prepare("SELECT COUNT(*) FROM dashboard_templates WHERE is_builtin = 1")?
            .query_row([], |row| row.get(0))?;
        if count > 0 {
            return Ok(());
        }

        fn w(title: &str, wt: &str, qc: serde_json::Value, pos: (i32,i32,i32,i32), dc: serde_json::Value) -> serde_json::Value {
            serde_json::json!({
                "title": title,
                "widget_type": wt,
                "query_config": qc,
                "position": {"col": pos.0, "row": pos.1, "col_span": pos.2, "row_span": pos.3},
                "display_config": dc,
            })
        }

        fn qc_svc(agg: &str, interval: Option<&str>, extra_filters: Vec<serde_json::Value>, group_by: Option<Vec<&str>>, limit: Option<i32>) -> serde_json::Value {
            let mut filters = vec![serde_json::json!({"field":"ServiceName","op":"=","value":"{{service}}"})];
            filters.extend(extra_filters);
            let mut v = serde_json::json!({"time_range_minutes":60,"filters":filters,"aggregation":agg});
            if let Some(i) = interval { v["interval"] = serde_json::json!(i); }
            if let Some(g) = group_by { v["group_by"] = serde_json::json!(g); }
            if let Some(l) = limit { v["limit"] = serde_json::json!(l); }
            v
        }

        fn qc(agg: &str, interval: Option<&str>, filters: Vec<serde_json::Value>, group_by: Option<Vec<&str>>, limit: Option<i32>) -> serde_json::Value {
            let mut v = serde_json::json!({"time_range_minutes":60,"filters":filters,"aggregation":agg});
            if let Some(i) = interval { v["interval"] = serde_json::json!(i); }
            if let Some(g) = group_by { v["group_by"] = serde_json::json!(g); }
            if let Some(l) = limit { v["limit"] = serde_json::json!(l); }
            v
        }

        fn color(c: &str) -> serde_json::Value { serde_json::json!({"color": c}) }
        fn empty() -> serde_json::Value { serde_json::json!({}) }
        let err_filter = || vec![serde_json::json!({"field":"StatusCode","op":">=","value":"500"})];

        let templates: Vec<(&str, &str, &str, &str, serde_json::Value)> = vec![
            ("tpl-service-overview", "Service Overview",
             "Golden signals for a single service: request rate, error rate, and latency percentiles.", "apm",
             serde_json::json!({"widgets": [
                 w("Request Rate", "timeseries", qc_svc("count", Some("1m"), vec![], None, None), (0,0,6,4), color("#3b82f6")),
                 w("Error Rate", "timeseries", qc_svc("count", Some("1m"), err_filter(), None, None), (6,0,6,4), color("#ef4444")),
                 w("P50 Latency", "timeseries", qc_svc("p50", Some("1m"), vec![], None, None), (0,4,4,4), color("#22c55e")),
                 w("P99 Latency", "timeseries", qc_svc("p99", Some("1m"), vec![], None, None), (4,4,4,4), color("#f59e0b")),
                 w("Top Endpoints", "table", qc_svc("count", None, vec![], Some(vec!["SpanName"]), Some(10)), (8,4,4,4), empty()),
             ]})),

            ("tpl-error-analysis", "Error Analysis",
             "Error count by service, top error messages, and error rate timeline.", "apm",
             serde_json::json!({"widgets": [
                 w("Error Count", "counter", qc("count", None, err_filter(), None, None), (0,0,3,3), color("#ef4444")),
                 w("Error Rate Over Time", "timeseries", qc("count", Some("5m"), err_filter(), None, None), (3,0,9,3), color("#ef4444")),
                 w("Errors by Service", "bar", qc("count", None, err_filter(), Some(vec!["ServiceName"]), Some(10)), (0,3,6,4), empty()),
                 w("Top Error Messages", "table", qc("count", None, err_filter(), Some(vec!["StatusMessage"]), Some(20)), (6,3,6,4), empty()),
             ]})),

            ("tpl-latency-deep-dive", "Latency Deep-Dive",
             "P50/P99/P999 latency, latency by endpoint, and slow traces.", "apm",
             serde_json::json!({"widgets": [
                 w("P50 / P99 Latency", "timeseries", qc_svc("p50", Some("1m"), vec![], None, None), (0,0,12,4), color("#8b5cf6")),
                 w("Latency by Endpoint", "bar", qc_svc("p99", None, vec![], Some(vec!["SpanName"]), Some(10)), (0,4,6,4), empty()),
                 w("Slowest Traces", "table", qc_svc("max", None, vec![], None, Some(20)), (6,4,6,4), empty()),
             ]})),

            ("tpl-infra-overview", "Infrastructure Overview",
             "CPU, memory, pod count, and restart count for infrastructure monitoring.", "infrastructure",
             serde_json::json!({"widgets": [
                 w("Pod Count", "counter", qc("count", None, vec![], None, None), (0,0,3,3), color("#06b6d4")),
                 w("CPU Utilization", "timeseries", qc("avg", Some("1m"), vec![], None, None), (3,0,9,3), color("#3b82f6")),
                 w("Memory Usage", "timeseries", qc("avg", Some("1m"), vec![], None, None), (0,3,6,4), color("#22c55e")),
                 w("Disk I/O", "timeseries", qc("avg", Some("1m"), vec![], None, None), (6,3,6,4), color("#f59e0b")),
             ]})),

            ("tpl-log-volume", "Log Volume",
             "Log count by severity, by service, and timeline for understanding ingestion patterns.", "security",
             serde_json::json!({"widgets": [
                 w("Error/Fatal Count", "counter", qc("count", None, vec![serde_json::json!({"field":"SeverityText","op":"in","value":"ERROR,FATAL"})], None, None), (0,0,3,3), color("#ef4444")),
                 w("Log Volume Over Time", "timeseries", qc("count", Some("5m"), vec![], None, None), (3,0,9,3), color("#6366f1")),
                 w("Logs by Severity", "bar", qc("count", None, vec![], Some(vec!["SeverityText"]), Some(10)), (0,3,6,4), empty()),
                 w("Top Services by Log Count", "table", qc("count", None, vec![], Some(vec!["ServiceName"]), Some(20)), (6,3,6,4), empty()),
             ]})),
        ];

        for (id, name, desc, category, json_val) in &templates {
            let json_str = serde_json::to_string(json_val)?;
            conn.execute(
                "INSERT OR IGNORE INTO dashboard_templates (id, name, description, category, is_builtin, template_json) \
                 VALUES (?1, ?2, ?3, ?4, 1, ?5)",
                params![id, name, desc, category, json_str],
            )?;
        }

        Ok(())
    }

    // ── Import/Export operations ──

    /// Export a dashboard and all its widgets as a self-contained JSON value.
    pub fn export_dashboard(&self, id: &str, tenant_id: &str, user_id: &str) -> anyhow::Result<Option<serde_json::Value>> {
        let dash = match self.get_dashboard(id, tenant_id, user_id)? {
            Some(d) => d,
            None => return Ok(None),
        };
        let widgets = self.list_widgets(id)?;
        let widget_exports: Vec<serde_json::Value> = widgets.into_iter().map(|w| {
            serde_json::json!({
                "title": w.title,
                "widget_type": w.widget_type,
                "query_config": serde_json::from_str::<serde_json::Value>(&w.query_config).unwrap_or_default(),
                "position": serde_json::from_str::<serde_json::Value>(&w.position).unwrap_or_default(),
                "display_config": serde_json::from_str::<serde_json::Value>(&w.display_config).unwrap_or_default(),
            })
        }).collect();

        let export = serde_json::json!({
            "format_version": "v1",
            "exported_at": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "dashboard": {
                "name": dash.name,
                "description": dash.description,
                "visibility": dash.visibility,
                "tags": dash.tags,
            },
            "widgets": widget_exports,
        });
        Ok(Some(export))
    }

    /// Import a dashboard from JSON, creating the dashboard + widgets. Returns the new dashboard.
    pub fn import_dashboard(
        &self,
        import: &crate::models::dashboard::ImportDashboardRequest,
        tenant_id: &str,
        owner_id: &str,
        user_role: &str,
    ) -> anyhow::Result<crate::models::dashboard::Dashboard> {
        if import.format_version != "v1" {
            anyhow::bail!("unsupported format_version: {}", import.format_version);
        }
        // Downgrade global visibility for non-admins
        let visibility = if import.dashboard.visibility == "global" && user_role != "admin" {
            "tenant"
        } else {
            &import.dashboard.visibility
        };
        let tags_str = serde_json::to_string(&import.dashboard.tags)?;
        let dash_id = uuid::Uuid::new_v4().to_string();
        self.create_dashboard(
            &dash_id,
            &import.dashboard.name,
            &import.dashboard.description,
            tenant_id,
            owner_id,
            visibility,
            &tags_str,
        )?;
        // Create widgets
        for w in &import.widgets {
            let wid = uuid::Uuid::new_v4().to_string();
            let qc = serde_json::to_string(&w.query_config)?;
            let pos = serde_json::to_string(&w.position)?;
            let dc = serde_json::to_string(&w.display_config)?;
            self.create_widget(&wid, &dash_id, &w.title, &w.widget_type, &qc, &pos, &dc)?;
        }
        self.get_dashboard_unchecked(&dash_id)?
            .ok_or_else(|| anyhow::anyhow!("failed to read imported dashboard"))
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

    pub fn list_channels(&self, tenant_id: &str) -> anyhow::Result<Vec<crate::models::alert::NotificationChannel>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, tenant_id, name, channel_type, config, enabled, created_at FROM notification_channels WHERE tenant_id = ?1 ORDER BY created_at DESC",
        )?;
        let rows = stmt
            .query_map(params![tenant_id], |row| {
                Ok(crate::models::alert::NotificationChannel {
                    id: row.get(0)?,
                    tenant_id: row.get(1)?,
                    name: row.get(2)?,
                    channel_type: row.get(3)?,
                    config: row.get(4)?,
                    enabled: row.get(5)?,
                    created_at: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_channel(&self, id: &str, tenant_id: &str) -> anyhow::Result<Option<crate::models::alert::NotificationChannel>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, tenant_id, name, channel_type, config, enabled, created_at FROM notification_channels WHERE id = ?1 AND tenant_id = ?2",
        )?;
        let mut rows = stmt.query_map(params![id, tenant_id], |row| {
            Ok(crate::models::alert::NotificationChannel {
                id: row.get(0)?,
                tenant_id: row.get(1)?,
                name: row.get(2)?,
                channel_type: row.get(3)?,
                config: row.get(4)?,
                enabled: row.get(5)?,
                created_at: row.get(6)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    /// Get a channel by ID without tenant scoping (used by alert engine which iterates all tenants)
    pub fn get_channel_by_id(&self, id: &str) -> anyhow::Result<Option<crate::models::alert::NotificationChannel>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, tenant_id, name, channel_type, config, enabled, created_at FROM notification_channels WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            Ok(crate::models::alert::NotificationChannel {
                id: row.get(0)?,
                tenant_id: row.get(1)?,
                name: row.get(2)?,
                channel_type: row.get(3)?,
                config: row.get(4)?,
                enabled: row.get(5)?,
                created_at: row.get(6)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    pub fn create_channel(
        &self,
        id: &str,
        tenant_id: &str,
        name: &str,
        channel_type: &str,
        config: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO notification_channels (id, tenant_id, name, channel_type, config) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, tenant_id, name, channel_type, config],
        )?;
        Ok(())
    }

    pub fn update_channel(
        &self,
        id: &str,
        tenant_id: &str,
        name: &str,
        config: &str,
        enabled: bool,
    ) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE notification_channels SET name = ?3, config = ?4, enabled = ?5 WHERE id = ?1 AND tenant_id = ?2",
            params![id, tenant_id, name, config, enabled],
        )?;
        Ok(count > 0)
    }

    pub fn delete_channel(&self, id: &str, tenant_id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute("DELETE FROM notification_channels WHERE id = ?1 AND tenant_id = ?2", params![id, tenant_id])?;
        Ok(count > 0)
    }

    // ── Notification log operations ──

    pub fn create_notification_log(
        &self,
        channel_id: &str,
        tenant_id: &str,
        alert_type: &str,
        alert_name: &str,
        severity: &str,
        status: &str,
        error: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO notification_log (id, channel_id, tenant_id, alert_type, alert_name, severity, status, error) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![id, channel_id, tenant_id, alert_type, alert_name, severity, status, error],
        )?;
        Ok(())
    }

    pub fn list_notification_log(&self, tenant_id: &str, limit: i64) -> anyhow::Result<Vec<crate::models::alert::NotificationLogEntry>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, channel_id, tenant_id, alert_type, alert_name, severity, status, error, created_at FROM notification_log WHERE tenant_id = ?1 ORDER BY created_at DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![tenant_id, limit], |row| {
                Ok(crate::models::alert::NotificationLogEntry {
                    id: row.get(0)?,
                    channel_id: row.get(1)?,
                    tenant_id: row.get(2)?,
                    alert_type: row.get(3)?,
                    alert_name: row.get(4)?,
                    severity: row.get(5)?,
                    status: row.get(6)?,
                    error: row.get(7)?,
                    created_at: row.get(8)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
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
            "SELECT id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, \
             window_type, target_percentage, threshold_ms, threshold_value, threshold_op, \
             error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, \
             error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, \
             created_at, updated_at FROM slos ORDER BY created_at DESC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(crate::models::slo::Slo {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    enabled: row.get(3)?,
                    slo_type: row.get(4)?,
                    indicator_type: row.get(5)?,
                    service_name: row.get(6)?,
                    metric_name: row.get(7)?,
                    window_type: row.get(8)?,
                    target_percentage: row.get(9)?,
                    threshold_ms: row.get(10)?,
                    threshold_value: row.get(11)?,
                    threshold_op: row.get(12)?,
                    error_filters: row.get(13)?,
                    total_filters: row.get(14)?,
                    eval_interval_secs: row.get(15)?,
                    notification_channel_ids: row.get(16)?,
                    state: row.get(17)?,
                    error_budget_remaining: row.get(18)?,
                    error_count: row.get(19)?,
                    total_count: row.get(20)?,
                    last_eval_at: row.get(21)?,
                    last_breached_at: row.get(22)?,
                    created_at: row.get(23)?,
                    updated_at: row.get(24)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_slo(&self, id: &str) -> anyhow::Result<Option<crate::models::slo::Slo>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, \
             window_type, target_percentage, threshold_ms, threshold_value, threshold_op, \
             error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, \
             error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, \
             created_at, updated_at FROM slos WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            Ok(crate::models::slo::Slo {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                enabled: row.get(3)?,
                slo_type: row.get(4)?,
                indicator_type: row.get(5)?,
                service_name: row.get(6)?,
                metric_name: row.get(7)?,
                window_type: row.get(8)?,
                target_percentage: row.get(9)?,
                threshold_ms: row.get(10)?,
                threshold_value: row.get(11)?,
                threshold_op: row.get(12)?,
                error_filters: row.get(13)?,
                total_filters: row.get(14)?,
                eval_interval_secs: row.get(15)?,
                notification_channel_ids: row.get(16)?,
                state: row.get(17)?,
                error_budget_remaining: row.get(18)?,
                error_count: row.get(19)?,
                total_count: row.get(20)?,
                last_eval_at: row.get(21)?,
                last_breached_at: row.get(22)?,
                created_at: row.get(23)?,
                updated_at: row.get(24)?,
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
        slo_type: &str,
        indicator_type: &str,
        service_name: &str,
        metric_name: &str,
        window_type: &str,
        target_percentage: f64,
        threshold_ms: Option<f64>,
        threshold_value: Option<f64>,
        threshold_op: Option<&str>,
        error_filters: &str,
        total_filters: &str,
        eval_interval_secs: i64,
        notification_channel_ids: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO slos (id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, \
             window_type, target_percentage, threshold_ms, threshold_value, threshold_op, \
             error_filters, total_filters, eval_interval_secs, notification_channel_ids) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![id, name, description, enabled, slo_type, indicator_type, service_name, metric_name,
                    window_type, target_percentage, threshold_ms, threshold_value, threshold_op,
                    error_filters, total_filters, eval_interval_secs, notification_channel_ids],
        )?;
        Ok(())
    }

    pub fn update_slo(
        &self,
        id: &str,
        name: &str,
        description: &str,
        enabled: bool,
        slo_type: &str,
        indicator_type: &str,
        service_name: &str,
        metric_name: &str,
        window_type: &str,
        target_percentage: f64,
        threshold_ms: Option<f64>,
        threshold_value: Option<f64>,
        threshold_op: Option<&str>,
        error_filters: &str,
        total_filters: &str,
        eval_interval_secs: i64,
        notification_channel_ids: &str,
    ) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE slos SET name = ?2, description = ?3, enabled = ?4, slo_type = ?5, \
             indicator_type = ?6, service_name = ?7, metric_name = ?8, window_type = ?9, target_percentage = ?10, \
             threshold_ms = ?11, threshold_value = ?12, threshold_op = ?13, \
             error_filters = ?14, total_filters = ?15, \
             eval_interval_secs = ?16, notification_channel_ids = ?17, \
             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?1",
            params![id, name, description, enabled, slo_type, indicator_type, service_name, metric_name,
                    window_type, target_percentage, threshold_ms, threshold_value, threshold_op,
                    error_filters, total_filters, eval_interval_secs, notification_channel_ids],
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
            "SELECT id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, \
             window_type, target_percentage, threshold_ms, threshold_value, threshold_op, \
             error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, \
             error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, \
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
                    slo_type: row.get(4)?,
                    indicator_type: row.get(5)?,
                    service_name: row.get(6)?,
                    metric_name: row.get(7)?,
                    window_type: row.get(8)?,
                    target_percentage: row.get(9)?,
                    threshold_ms: row.get(10)?,
                    threshold_value: row.get(11)?,
                    threshold_op: row.get(12)?,
                    error_filters: row.get(13)?,
                    total_filters: row.get(14)?,
                    eval_interval_secs: row.get(15)?,
                    notification_channel_ids: row.get(16)?,
                    state: row.get(17)?,
                    error_budget_remaining: row.get(18)?,
                    error_count: row.get(19)?,
                    total_count: row.get(20)?,
                    last_eval_at: row.get(21)?,
                    last_breached_at: row.get(22)?,
                    created_at: row.get(23)?,
                    updated_at: row.get(24)?,
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
        error_count: i64,
        total_count: i64,
        last_eval_at: &str,
        last_breached_at: Option<&str>,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        match last_breached_at {
            Some(t) => {
                conn.execute(
                    "UPDATE slos SET state = ?2, error_budget_remaining = ?3, error_count = ?4, \
                     total_count = ?5, last_eval_at = ?6, last_breached_at = ?7 WHERE id = ?1",
                    params![id, state, error_budget_remaining, error_count, total_count, last_eval_at, t],
                )?;
            }
            None => {
                conn.execute(
                    "UPDATE slos SET state = ?2, error_budget_remaining = ?3, error_count = ?4, \
                     total_count = ?5, last_eval_at = ?6 WHERE id = ?1",
                    params![id, state, error_budget_remaining, error_count, total_count, last_eval_at],
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
        error_count: i64,
        total_count: i64,
        error_budget_remaining: f64,
        message: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO slo_events (id, slo_id, state, error_count, total_count, error_budget_remaining, message) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![id, slo_id, state, error_count, total_count, error_budget_remaining, message],
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
            "SELECT id, slo_id, state, error_count, total_count, error_budget_remaining, message, created_at \
             FROM slo_events WHERE slo_id = ?1 ORDER BY created_at DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![slo_id, limit], |row| {
                Ok(crate::models::slo::SloEvent {
                    id: row.get(0)?,
                    slo_id: row.get(1)?,
                    state: row.get(2)?,
                    error_count: row.get(3)?,
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

    // ── Service link operations ──

    pub fn list_service_links(&self) -> anyhow::Result<Vec<crate::models::service_link::ServiceLink>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT service_name, github_repo, default_branch, root_path, updated_at \
             FROM service_links ORDER BY service_name ASC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(crate::models::service_link::ServiceLink {
                    service_name: row.get(0)?,
                    github_repo: row.get(1)?,
                    default_branch: row.get(2)?,
                    root_path: row.get(3)?,
                    updated_at: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_service_link(&self, service_name: &str) -> anyhow::Result<Option<crate::models::service_link::ServiceLink>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT service_name, github_repo, default_branch, root_path, updated_at \
             FROM service_links WHERE service_name = ?1",
        )?;
        let mut rows = stmt.query_map(params![service_name], |row| {
            Ok(crate::models::service_link::ServiceLink {
                service_name: row.get(0)?,
                github_repo: row.get(1)?,
                default_branch: row.get(2)?,
                root_path: row.get(3)?,
                updated_at: row.get(4)?,
            })
        })?;
        match rows.next() {
            Some(Ok(link)) => Ok(Some(link)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    pub fn upsert_service_link(
        &self,
        service_name: &str,
        github_repo: &str,
        default_branch: &str,
        root_path: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO service_links (service_name, github_repo, default_branch, root_path, updated_at)
             VALUES (?1, ?2, ?3, ?4, strftime('%Y-%m-%dT%H:%M:%SZ','now'))
             ON CONFLICT(service_name) DO UPDATE SET
               github_repo = excluded.github_repo,
               default_branch = excluded.default_branch,
               root_path = excluded.root_path,
               updated_at = excluded.updated_at",
            params![service_name, github_repo, default_branch, root_path],
        )?;
        Ok(())
    }

    pub fn delete_service_link(&self, service_name: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "DELETE FROM service_links WHERE service_name = ?1",
            params![service_name],
        )?;
        Ok(count > 0)
    }

    // ── Custom skills operations ──

    pub fn list_custom_skills(
        &self,
    ) -> anyhow::Result<Vec<crate::models::custom_skills::CustomSkill>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, title, description, content, allowed_tools, enabled, \
             created_by, created_at, updated_at FROM custom_skills ORDER BY name ASC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                let allowed_tools_json: String = row.get(5)?;
                let enabled_int: i64 = row.get(6)?;
                Ok(crate::models::custom_skills::CustomSkill {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    title: row.get(2)?,
                    description: row.get(3)?,
                    content: row.get(4)?,
                    allowed_tools: serde_json::from_str(&allowed_tools_json)
                        .unwrap_or_else(|_| Vec::new()),
                    enabled: enabled_int != 0,
                    created_by: row.get(7)?,
                    created_at: row.get(8)?,
                    updated_at: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_custom_skill(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<crate::models::custom_skills::CustomSkill>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, title, description, content, allowed_tools, enabled, \
             created_by, created_at, updated_at FROM custom_skills WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            let allowed_tools_json: String = row.get(5)?;
            let enabled_int: i64 = row.get(6)?;
            Ok(crate::models::custom_skills::CustomSkill {
                id: row.get(0)?,
                name: row.get(1)?,
                title: row.get(2)?,
                description: row.get(3)?,
                content: row.get(4)?,
                allowed_tools: serde_json::from_str(&allowed_tools_json)
                    .unwrap_or_else(|_| Vec::new()),
                enabled: enabled_int != 0,
                created_by: row.get(7)?,
                created_at: row.get(8)?,
                updated_at: row.get(9)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    pub fn get_custom_skill_by_name(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<crate::models::custom_skills::CustomSkill>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, title, description, content, allowed_tools, enabled, \
             created_by, created_at, updated_at FROM custom_skills WHERE name = ?1",
        )?;
        let mut rows = stmt.query_map(params![name], |row| {
            let allowed_tools_json: String = row.get(5)?;
            let enabled_int: i64 = row.get(6)?;
            Ok(crate::models::custom_skills::CustomSkill {
                id: row.get(0)?,
                name: row.get(1)?,
                title: row.get(2)?,
                description: row.get(3)?,
                content: row.get(4)?,
                allowed_tools: serde_json::from_str(&allowed_tools_json)
                    .unwrap_or_else(|_| Vec::new()),
                enabled: enabled_int != 0,
                created_by: row.get(7)?,
                created_at: row.get(8)?,
                updated_at: row.get(9)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    pub fn create_custom_skill(
        &self,
        req: &crate::models::custom_skills::CreateCustomSkillRequest,
        created_by: &str,
    ) -> anyhow::Result<crate::models::custom_skills::CustomSkill> {
        let id = uuid::Uuid::new_v4().to_string();
        let allowed_tools_json = serde_json::to_string(&req.allowed_tools)?;
        let enabled_int: i64 = if req.enabled { 1 } else { 0 };

        {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO custom_skills (id, name, title, description, content, \
                 allowed_tools, enabled, created_by) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    id,
                    req.name,
                    req.title,
                    req.description,
                    req.content,
                    allowed_tools_json,
                    enabled_int,
                    created_by,
                ],
            )?;
        }

        self.get_custom_skill(&id)?
            .ok_or_else(|| anyhow::anyhow!("failed to fetch newly created custom skill"))
    }

    pub fn update_custom_skill(
        &self,
        id: &str,
        req: &crate::models::custom_skills::UpdateCustomSkillRequest,
    ) -> anyhow::Result<Option<crate::models::custom_skills::CustomSkill>> {
        let allowed_tools_json = serde_json::to_string(&req.allowed_tools)?;
        let enabled_int: i64 = if req.enabled { 1 } else { 0 };

        let count = {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "UPDATE custom_skills SET title = ?2, description = ?3, content = ?4, \
                 allowed_tools = ?5, enabled = ?6, \
                 updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?1",
                params![
                    id,
                    req.title,
                    req.description,
                    req.content,
                    allowed_tools_json,
                    enabled_int,
                ],
            )?
        };

        if count == 0 {
            return Ok(None);
        }
        self.get_custom_skill(id)
    }

    pub fn delete_custom_skill(&self, id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute("DELETE FROM custom_skills WHERE id = ?1", params![id])?;
        Ok(count > 0)
    }

    // ── Tenant operations ──

    /// Ensure the default tenant row exists. Called once at startup so that
    /// api_keys with `tenant_id = 'default'` always have a valid FK target.
    pub fn ensure_default_tenant(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO tenants (id, name) VALUES ('default', 'default')",
            [],
        )?;
        Ok(())
    }

    /// Look up which tenant an API key belongs to, given its sha256 hash.
    /// Returns `None` if no matching key is found.
    pub fn resolve_tenant_for_api_key(&self, key_hash: &str) -> anyhow::Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT tenant_id FROM api_keys WHERE key_hash = ?1",
        )?;
        let mut rows = stmt.query_map(params![key_hash], |row| row.get::<_, String>(0))?;
        Ok(rows.next().transpose()?)
    }

    /// (id, name, enabled, created_at)
    /// (id, name, enabled, auth_required, created_at)
    pub fn list_tenants(&self) -> anyhow::Result<Vec<(String, String, bool, bool, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, enabled, auth_required, created_at FROM tenants ORDER BY created_at ASC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn create_tenant(&self, id: &str, name: &str) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO tenants (id, name) VALUES (?1, ?2)",
            params![id, name],
        )?;
        Ok(())
    }

    /// (id, name, enabled, auth_required, created_at)
    pub fn get_tenant(&self, id: &str) -> anyhow::Result<Option<(String, String, bool, bool, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, enabled, auth_required, created_at FROM tenants WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, bool>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        Ok(rows.next().transpose()?)
    }

    pub fn get_tenant_id_by_name(&self, name: &str) -> anyhow::Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id FROM tenants WHERE name = ?1 AND enabled = 1",
        )?;
        let mut rows = stmt.query_map(params![name], |row| row.get::<_, String>(0))?;
        Ok(rows.next().transpose()?)
    }

    pub fn set_tenant_enabled(&self, id: &str, enabled: bool) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE tenants SET enabled = ?2 WHERE id = ?1",
            params![id, enabled],
        )?;
        Ok(count > 0)
    }

    /// Returns true if the given tenant exists and is enabled.
    pub fn is_tenant_enabled(&self, name_or_id: &str) -> bool {
        let conn = self.conn.lock().unwrap();
        conn.prepare("SELECT enabled FROM tenants WHERE id = ?1 OR name = ?1")
            .and_then(|mut s| {
                s.query_row(params![name_or_id], |row| row.get::<_, bool>(0))
            })
            .unwrap_or(false)
    }

    /// Returns true if the tenant requires API key authentication (locked mode).
    /// When false, the X-Rush-Tenant header is enough (open mode).
    pub fn is_tenant_auth_required(&self, name_or_id: &str) -> bool {
        let conn = self.conn.lock().unwrap();
        conn.prepare("SELECT auth_required FROM tenants WHERE id = ?1 OR name = ?1")
            .and_then(|mut s| {
                s.query_row(params![name_or_id], |row| row.get::<_, bool>(0))
            })
            .unwrap_or(false)
    }

    pub fn set_tenant_auth_required(&self, id: &str, auth_required: bool) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE tenants SET auth_required = ?2 WHERE id = ?1",
            params![id, auth_required],
        )?;
        Ok(count > 0)
    }

    pub fn delete_tenant(&self, id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute("DELETE FROM tenants WHERE id = ?1", params![id])?;
        Ok(count > 0)
    }

    // ── Tenant retention operations ──

    /// Get all retention overrides for a specific tenant.
    /// Returns Vec<(signal, retain_days)>.
    pub fn get_tenant_retention(&self, tenant_id: &str) -> anyhow::Result<Vec<(String, i32)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT signal, retain_days FROM tenant_retention WHERE tenant_id = ?1",
        )?;
        let rows = stmt
            .query_map(params![tenant_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Upsert a retention override for a tenant+signal pair.
    pub fn set_tenant_retention(
        &self,
        tenant_id: &str,
        signal: &str,
        days: i32,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO tenant_retention (tenant_id, signal, retain_days)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(tenant_id, signal) DO UPDATE SET retain_days = excluded.retain_days",
            params![tenant_id, signal, days],
        )?;
        Ok(())
    }

    /// Remove a retention override for a tenant+signal pair (falls back to global).
    pub fn delete_tenant_retention(&self, tenant_id: &str, signal: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "DELETE FROM tenant_retention WHERE tenant_id = ?1 AND signal = ?2",
            params![tenant_id, signal],
        )?;
        Ok(count > 0)
    }

    /// List all tenant retention overrides (for the background enforcer).
    /// Returns Vec<(tenant_id, signal, retain_days)>.
    pub fn list_all_tenant_retention(&self) -> anyhow::Result<Vec<(String, String, i32)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT tenant_id, signal, retain_days FROM tenant_retention ORDER BY tenant_id, signal",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i32>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // ── User & session operations ──

    /// On first boot, create a default admin user if no users exist.
    /// Password is argon2id-hashed. Logs creation or silently skips.
    pub fn ensure_default_admin(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .prepare("SELECT COUNT(*) FROM users")?
            .query_row([], |row| row.get(0))?;
        if count > 0 {
            return Ok(());
        }

        let id = uuid::Uuid::new_v4().to_string();
        let password_hash = hash_password("rushobservability")?;

        conn.execute(
            "INSERT INTO users (id, username, password_hash, display_name, tenant_id, role) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, "admin", password_hash, "Admin", "default", "admin"],
        )?;

        tracing::info!("default admin user created (admin/rushobservability)");
        Ok(())
    }

    /// Verify username + password against the argon2id hash stored in the users table.
    /// Returns (user_id, username, display_name, tenant_id, role) on success.
    pub fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Option<(String, String, String, String, String)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT u.id, u.username, u.password_hash, u.display_name, u.tenant_id, \
                 CASE \
                   WHEN EXISTS(SELECT 1 FROM user_groups ug JOIN groups g ON ug.group_id = g.id \
                               WHERE ug.user_id = u.id AND g.permissions LIKE '%\"admin\"%') THEN 'admin' \
                   WHEN EXISTS(SELECT 1 FROM user_groups ug JOIN groups g ON ug.group_id = g.id \
                               WHERE ug.user_id = u.id AND g.permissions LIKE '%\"write\"%') THEN 'write' \
                   ELSE 'viewer' END as role \
                 FROM users u WHERE u.username = ?1 AND u.enabled = 1",
            )
            .ok()?;
        let mut rows = stmt
            .query_map(params![username], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })
            .ok()?;

        let (user_id, uname, password_hash, display_name, tenant_id, role) =
            rows.next()?.ok()?;

        if verify_password(password, &password_hash) {
            Some((user_id, uname, display_name, tenant_id, role))
        } else {
            None
        }
    }

    /// Create a new session for a user. Returns a random 64-char hex token.
    /// Session expires 24 hours from now.
    pub fn create_session(&self, user_id: &str) -> anyhow::Result<String> {
        use rand::Rng;
        let mut rng = rand::rng();
        let bytes: [u8; 32] = rng.random();
        let token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO sessions (token, user_id, expires_at) \
             VALUES (?1, ?2, strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '+1 day'))",
            params![token, user_id],
        )?;
        Ok(token)
    }

    /// Look up a session token, verify it has not expired, and return the
    /// associated user info: (user_id, username, display_name, tenant_id, role).
    /// Role is derived from group membership (admins group = "admin", else "viewer").
    pub fn get_session_user(
        &self,
        token: &str,
    ) -> Option<(String, String, String, String, String)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT u.id, u.username, u.display_name, u.tenant_id, \
                 CASE \
                   WHEN EXISTS(SELECT 1 FROM user_groups ug JOIN groups g ON ug.group_id = g.id \
                               WHERE ug.user_id = u.id AND g.permissions LIKE '%\"admin\"%') THEN 'admin' \
                   WHEN EXISTS(SELECT 1 FROM user_groups ug JOIN groups g ON ug.group_id = g.id \
                               WHERE ug.user_id = u.id AND g.permissions LIKE '%\"write\"%') THEN 'write' \
                   ELSE 'viewer' END as role \
                 FROM sessions s \
                 JOIN users u ON s.user_id = u.id \
                 WHERE s.token = ?1 \
                   AND u.enabled = 1 \
                   AND s.expires_at > strftime('%Y-%m-%dT%H:%M:%SZ','now')",
            )
            .ok()?;
        let mut rows = stmt
            .query_map(params![token], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .ok()?;
        rows.next()?.ok()
    }

    /// Delete a session by token (logout).
    pub fn delete_session(&self, token: &str) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute("DELETE FROM sessions WHERE token = ?1", params![token]);
    }

    /// List all users. Returns (id, username, display_name, tenant_id, enabled, created_at).
    /// Does NOT return password_hash or role (role is derived from group membership).
    pub fn list_users(
        &self,
    ) -> anyhow::Result<Vec<(String, String, String, String, bool, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, username, display_name, tenant_id, enabled, created_at \
             FROM users ORDER BY created_at ASC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, bool>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Create a new user with an argon2id-hashed password. Returns the user id.
    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        display_name: &str,
    ) -> anyhow::Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let password_hash = hash_password(password)?;

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO users (id, username, password_hash, display_name) \
             VALUES (?1, ?2, ?3, ?4)",
            params![id, username, password_hash, display_name],
        )?;
        Ok(id)
    }

    /// Delete a user and all their sessions.
    pub fn delete_user(&self, id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        // Sessions have ON DELETE CASCADE, but be explicit for clarity
        conn.execute("DELETE FROM sessions WHERE user_id = ?1", params![id])?;
        let count = conn.execute("DELETE FROM users WHERE id = ?1", params![id])?;
        Ok(count > 0)
    }

    /// Get a single user by id. Returns (id, username, display_name, tenant_id, enabled, created_at).
    pub fn get_user(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<(String, String, String, String, bool, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, username, display_name, tenant_id, enabled, created_at \
             FROM users WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, bool>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;
        Ok(rows.next().transpose()?)
    }

    /// Change a user's password. Hashes the new password with argon2id.
    pub fn change_password(&self, user_id: &str, new_password: &str) -> anyhow::Result<bool> {
        let password_hash = hash_password(new_password)?;
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE users SET password_hash = ?2 WHERE id = ?1",
            params![user_id, password_hash],
        )?;
        Ok(count > 0)
    }

    /// Toggle a user's enabled status.
    pub fn set_user_enabled(&self, user_id: &str, enabled: bool) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE users SET enabled = ?2 WHERE id = ?1",
            params![user_id, enabled],
        )?;
        Ok(count > 0)
    }

    /// Get the username for a user by their id.
    pub fn get_username(&self, user_id: &str) -> anyhow::Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT username FROM users WHERE id = ?1")?;
        let mut rows = stmt.query_map(params![user_id], |row| row.get::<_, String>(0))?;
        Ok(rows.next().transpose()?)
    }

    // ── Group operations ──

    /// On startup, create built-in groups (admins, viewers) if they don't exist.
    /// Bind both to all existing tenants. Add existing admin users to admins group,
    /// all other users to viewers group.
    pub fn ensure_default_groups(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();

        // Upsert admins group
        conn.execute(
            "INSERT OR IGNORE INTO groups (id, name, description, scopes, permissions, system) \
             VALUES ('admins', 'admins', 'Full access administrators', '[\"all\"]', '[\"read\",\"write\",\"admin\"]', 1)",
            [],
        )?;

        // Upsert viewers group
        conn.execute(
            "INSERT OR IGNORE INTO groups (id, name, description, scopes, permissions, system) \
             VALUES ('viewers', 'viewers', 'Read-only viewers', '[\"all\"]', '[\"read\"]', 1)",
            [],
        )?;

        // Remove any stale auto-bindings of viewers to tenants (legacy behavior).
        // viewers group should have no tenant access by default.
        conn.execute(
            "DELETE FROM group_tenants WHERE group_id = 'viewers'",
            [],
        )?;

        // Bind only the admins group to all existing tenants.
        // The viewers group intentionally has no default tenant bindings — tenant access
        // must be granted explicitly so non-admin users only see their assigned tenants.
        let tenant_ids: Vec<String> = {
            let mut stmt = conn.prepare("SELECT id FROM tenants")?;
            stmt.query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };

        for tid in &tenant_ids {
            conn.execute(
                "INSERT OR IGNORE INTO group_tenants (group_id, tenant_id) VALUES ('admins', ?1)",
                params![tid],
            )?;
        }

        // Assign a default group only to users who have no group memberships yet.
        // Users with explicit group assignments are left untouched.
        let users: Vec<(String, String)> = {
            let mut stmt = conn.prepare("SELECT id, role FROM users")?;
            stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
        };

        for (uid, role) in &users {
            let already_assigned: i64 = conn.query_row(
                "SELECT COUNT(*) FROM user_groups WHERE user_id = ?1",
                params![uid],
                |row| row.get(0),
            ).unwrap_or(0);

            if already_assigned == 0 {
                if role == "admin" {
                    conn.execute(
                        "INSERT OR IGNORE INTO user_groups (user_id, group_id) VALUES (?1, 'admins')",
                        params![uid],
                    )?;
                } else {
                    conn.execute(
                        "INSERT OR IGNORE INTO user_groups (user_id, group_id) VALUES (?1, 'viewers')",
                        params![uid],
                    )?;
                }
            }
        }

        tracing::info!("default groups ensured (admins, viewers)");
        Ok(())
    }

    /// List all groups with their tenant bindings.
    /// Returns Vec of (id, name, description, scopes_json, permissions_json, system, created_at, tenant_ids).
    pub fn list_groups(
        &self,
    ) -> anyhow::Result<Vec<(String, String, String, String, String, bool, String, Vec<String>)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, scopes, permissions, system, created_at FROM groups ORDER BY created_at ASC",
        )?;
        let groups: Vec<(String, String, String, String, String, bool, String)> = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, bool>(5)?,
                    row.get::<_, String>(6)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let mut result = Vec::new();
        for g in groups {
            let mut tstmt = conn.prepare(
                "SELECT tenant_id FROM group_tenants WHERE group_id = ?1",
            )?;
            let tids: Vec<String> = tstmt
                .query_map(params![g.0], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            result.push((g.0, g.1, g.2, g.3, g.4, g.5, g.6, tids));
        }
        Ok(result)
    }

    /// Get a single group by id with tenant IDs.
    pub fn get_group(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<(String, String, String, String, String, bool, String, Vec<String>)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, scopes, permissions, system, created_at FROM groups WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, bool>(5)?,
                row.get::<_, String>(6)?,
            ))
        })?;

        match rows.next().transpose()? {
            None => Ok(None),
            Some(g) => {
                let mut tstmt = conn.prepare(
                    "SELECT tenant_id FROM group_tenants WHERE group_id = ?1",
                )?;
                let tids: Vec<String> = tstmt
                    .query_map(params![g.0], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Some((g.0, g.1, g.2, g.3, g.4, g.5, g.6, tids)))
            }
        }
    }

    /// Create a new group. Returns the group id.
    pub fn create_group(
        &self,
        name: &str,
        description: &str,
        scopes: &str,
        permissions: &str,
    ) -> anyhow::Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO groups (id, name, description, scopes, permissions) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, name, description, scopes, permissions],
        )?;
        Ok(id)
    }

    /// Update a group's description, scopes, and permissions.
    /// Returns false if not found.
    pub fn update_group(
        &self,
        id: &str,
        description: &str,
        scopes: &str,
        permissions: &str,
    ) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE groups SET description = ?2, scopes = ?3, permissions = ?4 WHERE id = ?1",
            params![id, description, scopes, permissions],
        )?;
        Ok(count > 0)
    }

    /// Delete a group. Refuses if the group is a system group. Returns Err(msg) for system groups,
    /// Ok(false) if not found, Ok(true) if deleted.
    pub fn delete_group(&self, id: &str) -> anyhow::Result<Result<bool, String>> {
        let conn = self.conn.lock().unwrap();
        let is_system: Option<bool> = {
            let mut stmt = conn.prepare("SELECT system FROM groups WHERE id = ?1")?;
            let mut rows = stmt.query_map(params![id], |row| row.get::<_, bool>(0))?;
            rows.next().transpose()?
        };
        match is_system {
            None => Ok(Ok(false)),
            Some(true) => Ok(Err("cannot delete a system group".to_string())),
            Some(false) => {
                let count = conn.execute("DELETE FROM groups WHERE id = ?1", params![id])?;
                Ok(Ok(count > 0))
            }
        }
    }

    /// Replace a group's tenant bindings.
    pub fn set_group_tenants(&self, group_id: &str, tenant_ids: &[String]) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM group_tenants WHERE group_id = ?1", params![group_id])?;
        for tid in tenant_ids {
            conn.execute(
                "INSERT INTO group_tenants (group_id, tenant_id) VALUES (?1, ?2)",
                params![group_id, tid],
            )?;
        }
        Ok(())
    }

    /// Get the group IDs for a user.
    pub fn get_user_groups(&self, user_id: &str) -> anyhow::Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT group_id FROM user_groups WHERE user_id = ?1")?;
        let rows = stmt
            .query_map(params![user_id], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Replace a user's group memberships.
    pub fn set_user_groups(&self, user_id: &str, group_ids: &[String]) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM user_groups WHERE user_id = ?1", params![user_id])?;
        for gid in group_ids {
            conn.execute(
                "INSERT INTO user_groups (user_id, group_id) VALUES (?1, ?2)",
                params![user_id, gid],
            )?;
        }
        Ok(())
    }

    /// Resolve a user's effective permissions by unioning all their groups.
    /// Returns (scopes, permissions, tenant_ids) -- each deduplicated.
    pub fn resolve_user_permissions(
        &self,
        user_id: &str,
    ) -> anyhow::Result<(Vec<String>, Vec<String>, Vec<String>)> {
        let conn = self.conn.lock().unwrap();

        // Get all groups the user belongs to
        let group_ids: Vec<String> = {
            let mut stmt = conn.prepare("SELECT group_id FROM user_groups WHERE user_id = ?1")?;
            stmt.query_map(params![user_id], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };

        let mut all_scopes = std::collections::HashSet::new();
        let mut all_permissions = std::collections::HashSet::new();
        let mut all_tenant_ids = std::collections::HashSet::new();

        for gid in &group_ids {
            // Get group scopes and permissions
            let mut gstmt = conn.prepare("SELECT scopes, permissions FROM groups WHERE id = ?1")?;
            if let Some((scopes_json, perms_json)) = gstmt
                .query_map(params![gid], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .next()
                .transpose()?
            {
                if let Ok(scopes) = serde_json::from_str::<Vec<String>>(&scopes_json) {
                    all_scopes.extend(scopes);
                }
                if let Ok(perms) = serde_json::from_str::<Vec<String>>(&perms_json) {
                    all_permissions.extend(perms);
                }
            }

            // Get group tenant bindings
            let mut tstmt = conn.prepare("SELECT tenant_id FROM group_tenants WHERE group_id = ?1")?;
            let tids: Vec<String> = tstmt
                .query_map(params![gid], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            all_tenant_ids.extend(tids);
        }

        // "all" scope supersedes individual signal scopes
        if all_scopes.contains("all") {
            all_scopes = std::collections::HashSet::from(["all".to_string()]);
        }
        // "admin" permission implies read + write
        if all_permissions.contains("admin") {
            all_permissions.insert("read".to_string());
            all_permissions.insert("write".to_string());
        }

        Ok((
            all_scopes.into_iter().collect(),
            all_permissions.into_iter().collect(),
            all_tenant_ids.into_iter().collect(),
        ))
    }

    // ── SSO Provider operations ──

    /// Get a single SSO provider by id.
    pub fn get_sso_provider(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<(String, String, String, bool, String, String, String, String, String, String, String, String, bool, String, String, String, String, String, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, protocol, enabled, client_id, client_secret, issuer_url, \
             oidc_scopes, groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, created_at, \
             saml_idp_metadata_url, saml_idp_sso_url, saml_idp_cert, saml_sp_entity_id \
             FROM sso_providers WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, bool>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, String>(11)?,
                row.get::<_, bool>(12)?,
                row.get::<_, String>(13)?,
                row.get::<_, String>(14)?,
                row.get::<_, String>(15)?,
                row.get::<_, String>(16)?,
                row.get::<_, String>(17)?,
                row.get::<_, String>(18)?,
            ))
        })?;
        Ok(rows.next().transpose()?)
    }

    /// List all SSO providers.
    pub fn list_sso_providers(
        &self,
    ) -> anyhow::Result<Vec<(String, String, String, bool, String, String, String, String, String, String, String, String, bool, String, String, String, String, String, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, protocol, enabled, client_id, client_secret, issuer_url, \
             oidc_scopes, groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, created_at, \
             saml_idp_metadata_url, saml_idp_sso_url, saml_idp_cert, saml_sp_entity_id \
             FROM sso_providers ORDER BY created_at ASC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, bool>(12)?,
                    row.get::<_, String>(13)?,
                    row.get::<_, String>(14)?,
                    row.get::<_, String>(15)?,
                    row.get::<_, String>(16)?,
                    row.get::<_, String>(17)?,
                    row.get::<_, String>(18)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Get the first enabled SSO provider (for login redirect).
    pub fn get_enabled_sso_provider(
        &self,
    ) -> anyhow::Result<Option<(String, String, String, bool, String, String, String, String, String, String, String, String, bool, String, String, String, String, String, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, protocol, enabled, client_id, client_secret, issuer_url, \
             oidc_scopes, groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, created_at, \
             saml_idp_metadata_url, saml_idp_sso_url, saml_idp_cert, saml_sp_entity_id \
             FROM sso_providers WHERE enabled = 1 LIMIT 1",
        )?;
        let mut rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, bool>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, String>(11)?,
                row.get::<_, bool>(12)?,
                row.get::<_, String>(13)?,
                row.get::<_, String>(14)?,
                row.get::<_, String>(15)?,
                row.get::<_, String>(16)?,
                row.get::<_, String>(17)?,
                row.get::<_, String>(18)?,
            ))
        })?;
        Ok(rows.next().transpose()?)
    }

    /// Create or update an SSO provider (includes SAML fields).
    pub fn upsert_sso_provider(
        &self,
        id: &str,
        name: &str,
        protocol: &str,
        enabled: bool,
        client_id: &str,
        client_secret: &str,
        issuer_url: &str,
        oidc_scopes: &str,
        groups_claim: &str,
        jit_provisioning: bool,
        default_group_id: &str,
        saml_idp_metadata_url: &str,
        saml_idp_sso_url: &str,
        saml_idp_cert: &str,
        saml_sp_entity_id: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO sso_providers (id, name, protocol, enabled, client_id, client_secret, \
             issuer_url, oidc_scopes, groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, \
             saml_idp_metadata_url, saml_idp_sso_url, saml_idp_cert, saml_sp_entity_id) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15) \
             ON CONFLICT(id) DO UPDATE SET \
             name=excluded.name, protocol=excluded.protocol, enabled=excluded.enabled, \
             client_id=excluded.client_id, client_secret=excluded.client_secret, \
             issuer_url=excluded.issuer_url, oidc_scopes=excluded.oidc_scopes, \
             groups_claim=excluded.groups_claim, email_claim=excluded.email_claim, first_name_claim=excluded.first_name_claim, last_name_claim=excluded.last_name_claim, jit_provisioning=excluded.jit_provisioning, \
             default_group_id=excluded.default_group_id, \
             saml_idp_metadata_url=excluded.saml_idp_metadata_url, \
             saml_idp_sso_url=excluded.saml_idp_sso_url, \
             saml_idp_cert=excluded.saml_idp_cert, \
             saml_sp_entity_id=excluded.saml_sp_entity_id",
            params![
                id,
                name,
                protocol,
                enabled,
                client_id,
                client_secret,
                issuer_url,
                oidc_scopes,
                groups_claim,
                jit_provisioning,
                default_group_id,
                saml_idp_metadata_url,
                saml_idp_sso_url,
                saml_idp_cert,
                saml_sp_entity_id,
            ],
        )?;
        Ok(())
    }

    /// Delete an SSO provider.
    pub fn delete_sso_provider(&self, id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute("DELETE FROM sso_providers WHERE id = ?1", params![id])?;
        Ok(count > 0)
    }

    // ── IdP Group Mapping operations ──

    /// List IdP group mappings, optionally filtered by provider_id.
    pub fn list_idp_group_mappings(
        &self,
        provider_id: Option<&str>,
    ) -> anyhow::Result<Vec<(String, String, String, String, String)>> {
        let conn = self.conn.lock().unwrap();
        let (sql, filter) = match provider_id {
            Some(pid) => (
                "SELECT id, idp_group, rush_group_id, provider_id, created_at \
                 FROM idp_group_mappings WHERE provider_id = ?1 ORDER BY created_at ASC",
                Some(pid.to_string()),
            ),
            None => (
                "SELECT id, idp_group, rush_group_id, provider_id, created_at \
                 FROM idp_group_mappings ORDER BY created_at ASC",
                None,
            ),
        };
        let mut stmt = conn.prepare(sql)?;
        let rows = match &filter {
            Some(pid) => stmt
                .query_map(params![pid], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?,
            None => stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?,
        };
        Ok(rows)
    }

    /// Create an IdP group mapping. Returns the mapping id.
    pub fn create_idp_group_mapping(
        &self,
        idp_group: &str,
        rush_group_id: &str,
        provider_id: &str,
    ) -> anyhow::Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO idp_group_mappings (id, idp_group, rush_group_id, provider_id) \
             VALUES (?1, ?2, ?3, ?4)",
            params![id, idp_group, rush_group_id, provider_id],
        )?;
        Ok(id)
    }

    /// Delete an IdP group mapping.
    pub fn delete_idp_group_mapping(&self, id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute("DELETE FROM idp_group_mappings WHERE id = ?1", params![id])?;
        Ok(count > 0)
    }

    /// Resolve IdP groups to Rush group_ids using mappings.
    /// Returns the set of Rush group_ids that match any of the given IdP group names.
    pub fn resolve_idp_groups(
        &self,
        idp_groups: &[String],
        provider_id: &str,
    ) -> anyhow::Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut result = std::collections::HashSet::new();
        for idp_group in idp_groups {
            let mut stmt = conn.prepare(
                "SELECT rush_group_id FROM idp_group_mappings \
                 WHERE idp_group = ?1 AND provider_id = ?2",
            )?;
            let ids: Vec<String> = stmt
                .query_map(params![idp_group, provider_id], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            result.extend(ids);
        }
        Ok(result.into_iter().collect())
    }

    // ── SSO User operations ──

    /// Find a user by external_id and auth_provider. Returns user id if found.
    pub fn find_user_by_external_id(
        &self,
        external_id: &str,
        auth_provider: &str,
    ) -> anyhow::Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id FROM users WHERE external_id = ?1 AND auth_provider = ?2",
        )?;
        let mut rows = stmt.query_map(params![external_id, auth_provider], |row| {
            row.get::<_, String>(0)
        })?;
        Ok(rows.next().transpose()?)
    }

    /// Create an SSO user with no password hash. Returns the user id.
    pub fn create_sso_user(
        &self,
        username: &str,
        display_name: &str,
        external_id: &str,
        auth_provider: &str,
        tenant_id: &str,
    ) -> anyhow::Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        // SSO users have no password — store a placeholder that can never match bcrypt verify
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO users (id, username, password_hash, display_name, tenant_id, role, auth_provider, external_id) \
             VALUES (?1, ?2, '!sso-no-password', ?3, ?4, 'viewer', ?5, ?6)",
            params![id, username, display_name, tenant_id, auth_provider, external_id],
        )?;
        Ok(id)
    }

    /// Replace a user's group memberships with the IdP-mapped set (SSO group sync).
    pub fn update_user_groups_from_idp(
        &self,
        user_id: &str,
        mapped_group_ids: &[String],
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM user_groups WHERE user_id = ?1", params![user_id])?;
        for gid in mapped_group_ids {
            conn.execute(
                "INSERT OR IGNORE INTO user_groups (user_id, group_id) VALUES (?1, ?2)",
                params![user_id, gid],
            )?;
        }
        Ok(())
    }

    // ── SSO CSRF state operations ──

    /// Store a random state value for CSRF protection during OIDC flow.
    pub fn store_sso_state(&self, state: &str) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        // Clean up stale states older than 10 minutes
        conn.execute(
            "DELETE FROM sso_state WHERE created_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-10 minutes')",
            [],
        )?;
        conn.execute(
            "INSERT INTO sso_state (state) VALUES (?1)",
            params![state],
        )?;
        Ok(())
    }

    /// Validate and consume an SSO state value. Returns true if the state was valid.
    pub fn validate_sso_state(&self, state: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "DELETE FROM sso_state WHERE state = ?1 \
             AND created_at > strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-10 minutes')",
            params![state],
        )?;
        Ok(count > 0)
    }

    // ── SIEM Detection Rule operations ──

    pub fn list_detection_rules(
        &self,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<Vec<crate::models::detection::DetectionRule>> {
        let conn = self.conn.lock().unwrap();
        let (sql, tenant_filter) = match tenant_id {
            Some(tid) => (
                "SELECT id, tenant_id, name, description, query_sql, interval_secs, threshold, \
                 severity, window_secs, enabled, channels, created_by, last_eval_at, \
                 last_triggered_at, created_at, updated_at \
                 FROM detection_rules WHERE tenant_id = ?1 ORDER BY created_at DESC",
                Some(tid.to_string()),
            ),
            None => (
                "SELECT id, tenant_id, name, description, query_sql, interval_secs, threshold, \
                 severity, window_secs, enabled, channels, created_by, last_eval_at, \
                 last_triggered_at, created_at, updated_at \
                 FROM detection_rules ORDER BY created_at DESC",
                None,
            ),
        };

        let mut stmt = conn.prepare(sql)?;
        let row_mapper = |row: &rusqlite::Row| {
            Ok(crate::models::detection::DetectionRule {
                id: row.get(0)?,
                tenant_id: row.get(1)?,
                name: row.get(2)?,
                description: row.get(3)?,
                query_sql: row.get(4)?,
                interval_secs: row.get(5)?,
                threshold: row.get(6)?,
                severity: row.get(7)?,
                window_secs: row.get(8)?,
                enabled: row.get(9)?,
                channels: row.get(10)?,
                created_by: row.get(11)?,
                last_eval_at: row.get(12)?,
                last_triggered_at: row.get(13)?,
                created_at: row.get(14)?,
                updated_at: row.get(15)?,
            })
        };

        let rows = match &tenant_filter {
            Some(tid) => stmt.query_map(params![tid], row_mapper)?,
            None => stmt.query_map([], row_mapper)?,
        };
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn get_detection_rule(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<crate::models::detection::DetectionRule>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, tenant_id, name, description, query_sql, interval_secs, threshold, \
             severity, window_secs, enabled, channels, created_by, last_eval_at, \
             last_triggered_at, created_at, updated_at \
             FROM detection_rules WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            Ok(crate::models::detection::DetectionRule {
                id: row.get(0)?,
                tenant_id: row.get(1)?,
                name: row.get(2)?,
                description: row.get(3)?,
                query_sql: row.get(4)?,
                interval_secs: row.get(5)?,
                threshold: row.get(6)?,
                severity: row.get(7)?,
                window_secs: row.get(8)?,
                enabled: row.get(9)?,
                channels: row.get(10)?,
                created_by: row.get(11)?,
                last_eval_at: row.get(12)?,
                last_triggered_at: row.get(13)?,
                created_at: row.get(14)?,
                updated_at: row.get(15)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    pub fn create_detection_rule(
        &self,
        id: &str,
        tenant_id: &str,
        name: &str,
        description: &str,
        query_sql: &str,
        interval_secs: i64,
        threshold: i64,
        severity: &str,
        window_secs: i64,
        enabled: bool,
        channels: &str,
        created_by: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO detection_rules (id, tenant_id, name, description, query_sql, \
             interval_secs, threshold, severity, window_secs, enabled, channels, created_by) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                id, tenant_id, name, description, query_sql, interval_secs, threshold,
                severity, window_secs, enabled, channels, created_by
            ],
        )?;
        Ok(())
    }

    pub fn update_detection_rule(
        &self,
        id: &str,
        name: &str,
        description: &str,
        query_sql: &str,
        interval_secs: i64,
        threshold: i64,
        severity: &str,
        window_secs: i64,
        enabled: bool,
        channels: &str,
    ) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE detection_rules SET name = ?2, description = ?3, query_sql = ?4, \
             interval_secs = ?5, threshold = ?6, severity = ?7, window_secs = ?8, \
             enabled = ?9, channels = ?10, \
             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?1",
            params![id, name, description, query_sql, interval_secs, threshold,
                    severity, window_secs, enabled, channels],
        )?;
        Ok(count > 0)
    }

    pub fn delete_detection_rule(&self, id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute("DELETE FROM detection_rules WHERE id = ?1", params![id])?;
        Ok(count > 0)
    }

    /// Get all enabled detection rules across all tenants (for the SIEM engine).
    pub fn list_enabled_detection_rules(
        &self,
    ) -> anyhow::Result<Vec<crate::models::detection::DetectionRule>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, tenant_id, name, description, query_sql, interval_secs, threshold, \
             severity, window_secs, enabled, channels, created_by, last_eval_at, \
             last_triggered_at, created_at, updated_at \
             FROM detection_rules WHERE enabled = 1 ORDER BY created_at ASC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(crate::models::detection::DetectionRule {
                    id: row.get(0)?,
                    tenant_id: row.get(1)?,
                    name: row.get(2)?,
                    description: row.get(3)?,
                    query_sql: row.get(4)?,
                    interval_secs: row.get(5)?,
                    threshold: row.get(6)?,
                    severity: row.get(7)?,
                    window_secs: row.get(8)?,
                    enabled: row.get(9)?,
                    channels: row.get(10)?,
                    created_by: row.get(11)?,
                    last_eval_at: row.get(12)?,
                    last_triggered_at: row.get(13)?,
                    created_at: row.get(14)?,
                    updated_at: row.get(15)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Update a detection rule's last_eval_at and optionally last_triggered_at.
    pub fn update_detection_rule_eval(
        &self,
        id: &str,
        last_eval_at: &str,
        last_triggered_at: Option<&str>,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        match last_triggered_at {
            Some(t) => {
                conn.execute(
                    "UPDATE detection_rules SET last_eval_at = ?2, last_triggered_at = ?3 WHERE id = ?1",
                    params![id, last_eval_at, t],
                )?;
            }
            None => {
                conn.execute(
                    "UPDATE detection_rules SET last_eval_at = ?2 WHERE id = ?1",
                    params![id, last_eval_at],
                )?;
            }
        }
        Ok(())
    }

    // ── SIEM Detection Event operations ──

    pub fn create_detection_event(
        &self,
        id: &str,
        rule_id: &str,
        tenant_id: &str,
        severity: &str,
        match_count: i64,
        sample_data: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO detection_events (id, rule_id, tenant_id, severity, match_count, sample_data) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, rule_id, tenant_id, severity, match_count, sample_data],
        )?;
        Ok(())
    }

    pub fn list_detection_events(
        &self,
        tenant_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::detection::DetectionEventWithRule>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT e.id, e.rule_id, COALESCE(r.name, 'deleted rule') as rule_name, \
             e.tenant_id, e.severity, e.match_count, e.sample_data, e.created_at \
             FROM detection_events e \
             LEFT JOIN detection_rules r ON e.rule_id = r.id \
             WHERE e.tenant_id = ?1 \
             ORDER BY e.created_at DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![tenant_id, limit], |row| {
                let sample_data_str: String = row.get(6)?;
                let sample_data_json: serde_json::Value = serde_json::from_str(&sample_data_str)
                    .unwrap_or(serde_json::json!([]));
                Ok(crate::models::detection::DetectionEventWithRule {
                    id: row.get(0)?,
                    rule_id: row.get(1)?,
                    rule_name: row.get(2)?,
                    tenant_id: row.get(3)?,
                    severity: row.get(4)?,
                    match_count: row.get(5)?,
                    sample_data: sample_data_json,
                    created_at: row.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Returns the count of existing detection rules.
    pub fn count_detection_rules(&self) -> anyhow::Result<i64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .prepare("SELECT COUNT(*) FROM detection_rules")?
            .query_row([], |row| row.get(0))?;
        Ok(count)
    }

    /// Check whether a built-in detection rule with the given name already
    /// exists for a specific tenant. Used by the additive seeder to avoid
    /// duplicating rules that were already created (or customized by the user).
    fn default_detection_rule_exists(&self, name: &str, tenant_id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .prepare(
                "SELECT COUNT(*) FROM detection_rules \
                 WHERE name = ?1 AND tenant_id = ?2 AND created_by = 'system'",
            )?
            .query_row(params![name, tenant_id], |row| row.get(0))?;
        Ok(count > 0)
    }

    /// Seed built-in detection rules additively. Each rule is checked by name
    /// before insertion so that upgrading Rush adds new built-in rules without
    /// wiping or re-creating existing ones. User-customized rules (created_by
    /// != 'system') are never touched.
    pub fn ensure_default_detection_rules(&self) -> anyhow::Result<()> {
        tracing::info!("SIEM: checking default detection rules");

        // (name, description, query_sql, severity, interval_secs, window_secs)
        let defaults: Vec<(&str, &str, &str, &str, i64, i64)> = vec![
            // ── Single-signal rules ──
            (
                "Failed login brute force",
                "Detects IPs with 10+ failed login attempts within the detection window.",
                "SELECT mat_source_ip, count() AS attempt_count \
                 FROM otel_logs \
                 WHERE Timestamp BETWEEN @window_start AND @window_end \
                   AND mat_action = 'login_failed' \
                 GROUP BY mat_source_ip \
                 HAVING attempt_count >= 10",
                "high",
                300,
                300,
            ),
            (
                "Error rate spike per service",
                "Fires when any service has an error rate above 5% with at least 100 spans.",
                "SELECT ServiceName, \
                   countIf(StatusCode = 'ERROR') AS errors, \
                   count() AS total, \
                   errors / total AS error_rate \
                 FROM otel_traces \
                 WHERE Timestamp BETWEEN @window_start AND @window_end \
                 GROUP BY ServiceName \
                 HAVING error_rate > 0.05 AND total > 100",
                "high",
                300,
                300,
            ),
            (
                "P99 latency regression",
                "Detects server spans where p99 latency exceeds 500ms with sufficient traffic.",
                "SELECT ServiceName, SpanName, \
                   quantile(0.99)(Duration) / 1000000 AS p99_ms, \
                   count() AS total \
                 FROM otel_traces \
                 WHERE Timestamp BETWEEN @window_start AND @window_end \
                   AND SpanKind = 'SPAN_KIND_SERVER' \
                 GROUP BY ServiceName, SpanName \
                 HAVING p99_ms > 500 AND total > 50",
                "high",
                300,
                300,
            ),
            (
                "CPU saturation",
                "Alerts when average CPU utilization exceeds 90% for any host.",
                "SELECT ServiceName, \
                   Attributes['host.name'] AS host, \
                   avg(Value) AS avg_cpu \
                 FROM otel_metrics_gauge \
                 WHERE TimeUnix BETWEEN @window_start AND @window_end \
                   AND MetricName = 'system.cpu.utilization' \
                 GROUP BY ServiceName, host \
                 HAVING avg_cpu > 0.9",
                "critical",
                300,
                300,
            ),
            (
                "Request rate drop",
                "Detects a 50%+ drop in request rate compared to the previous hour.",
                "WITH \
                   current AS ( \
                     SELECT ServiceName, sum(Value) AS current_rate \
                     FROM otel_metrics_sum \
                     WHERE TimeUnix BETWEEN @window_start AND @window_end \
                       AND MetricName = 'http.server.request.count' \
                     GROUP BY ServiceName \
                   ), \
                   previous AS ( \
                     SELECT ServiceName, sum(Value) AS prev_rate \
                     FROM otel_metrics_sum \
                     WHERE TimeUnix BETWEEN @window_start - INTERVAL 1 HOUR AND @window_start \
                       AND MetricName = 'http.server.request.count' \
                     GROUP BY ServiceName \
                   ) \
                 SELECT c.ServiceName, c.current_rate, p.prev_rate, \
                   (p.prev_rate - c.current_rate) / p.prev_rate AS drop_pct \
                 FROM current c \
                 JOIN previous p ON c.ServiceName = p.ServiceName \
                 WHERE p.prev_rate > 100 AND drop_pct > 0.5",
                "high",
                300,
                300,
            ),
            (
                "Error + latency correlation",
                "Detects services with both elevated error rates and high p99 latency.",
                "WITH \
                   error_services AS ( \
                     SELECT ServiceName FROM otel_traces \
                     WHERE Timestamp BETWEEN @window_start AND @window_end \
                     GROUP BY ServiceName \
                     HAVING countIf(StatusCode = 'ERROR') / count() > 0.05 \
                   ), \
                   slow_services AS ( \
                     SELECT ServiceName FROM otel_traces \
                     WHERE Timestamp BETWEEN @window_start AND @window_end \
                       AND SpanKind = 'SPAN_KIND_SERVER' \
                     GROUP BY ServiceName \
                     HAVING quantile(0.99)(Duration) / 1000000 > 500 \
                   ) \
                 SELECT es.ServiceName \
                 FROM error_services es \
                 INNER JOIN slow_services ss ON es.ServiceName = ss.ServiceName",
                "critical",
                300,
                300,
            ),
            (
                "High severity log volume",
                "Fires when ERROR/FATAL log volume exceeds 100 entries in the window.",
                "SELECT ServiceName, SeverityText, count() AS log_count \
                 FROM otel_logs \
                 WHERE Timestamp BETWEEN @window_start AND @window_end \
                   AND SeverityText IN ('ERROR', 'FATAL') \
                 GROUP BY ServiceName, SeverityText \
                 HAVING log_count >= 100",
                "medium",
                300,
                300,
            ),
            // ── Cross-signal correlation rules ──
            (
                "Log errors + trace failures correlation",
                "Correlates ERROR/FATAL log entries with trace span failures on the same \
                 service. Fires when a service has 5+ log errors AND 5+ span errors in the \
                 same window, indicating a confirmed incident across both signals.",
                "WITH \
                   error_logs AS ( \
                     SELECT ServiceName, count() AS log_errors \
                     FROM otel_logs \
                     WHERE Timestamp BETWEEN @window_start AND @window_end \
                       AND SeverityText IN ('ERROR', 'FATAL') \
                     GROUP BY ServiceName \
                     HAVING log_errors >= 5 \
                   ), \
                   trace_errors AS ( \
                     SELECT ServiceName, count() AS span_errors \
                     FROM otel_traces \
                     WHERE Timestamp BETWEEN @window_start AND @window_end \
                       AND StatusCode = 'ERROR' \
                     GROUP BY ServiceName \
                     HAVING span_errors >= 5 \
                   ) \
                 SELECT el.ServiceName, el.log_errors, te.span_errors \
                 FROM error_logs el \
                 INNER JOIN trace_errors te ON el.ServiceName = te.ServiceName",
                "critical",
                300,
                300,
            ),
            (
                "Latency spike + memory pressure",
                "Correlates high p99 latency from wide_events with elevated memory usage \
                 from metrics on the same service. Indicates resource exhaustion as the \
                 likely root cause of slow responses.",
                "WITH \
                   slow_services AS ( \
                     SELECT ServiceName \
                     FROM wide_events \
                     WHERE timestamp BETWEEN @window_start AND @window_end \
                       AND http_status_code > 0 \
                     GROUP BY ServiceName \
                     HAVING quantile(0.99)(duration_ns) / 1000000 > 500 AND count() > 50 \
                   ), \
                   mem_pressure AS ( \
                     SELECT ResourceAttributes['service.name'] AS ServiceName \
                     FROM otel_metrics_gauge \
                     WHERE TimeUnix BETWEEN @window_start AND @window_end \
                       AND MetricName IN ('process.runtime.jvm.memory.usage', \
                                          'container.memory.usage', \
                                          'process.memory.usage') \
                     GROUP BY ServiceName \
                     HAVING max(Value) > 0.85 * any( \
                       SELECT Value FROM otel_metrics_gauge \
                       WHERE MetricName LIKE '%memory.limit%' AND TimeUnix >= @window_start \
                       LIMIT 1 \
                     ) \
                   ) \
                 SELECT ss.ServiceName \
                 FROM slow_services ss \
                 INNER JOIN mem_pressure mp ON ss.ServiceName = mp.ServiceName",
                "high",
                300,
                300,
            ),
            (
                "Post-deploy error rate increase",
                "Detects deploy events followed by a significant error rate increase within \
                 30 minutes. Joins trace deploy spans with wide_events to identify \
                 deployments that caused regressions (>5% error rate with 20+ requests).",
                "WITH \
                   recent_deploys AS ( \
                     SELECT ServiceName, max(Timestamp) AS deploy_time \
                     FROM otel_traces \
                     WHERE Timestamp BETWEEN @window_start AND @window_end \
                       AND SpanName LIKE '%deploy%' \
                     GROUP BY ServiceName \
                   ), \
                   post_deploy_errors AS ( \
                     SELECT w.service_name AS ServiceName, \
                       countIf(w.http_status_code >= 500) AS errors, \
                       count() AS total \
                     FROM wide_events w \
                     INNER JOIN recent_deploys rd ON w.service_name = rd.ServiceName \
                     WHERE w.timestamp >= rd.deploy_time \
                       AND w.timestamp <= rd.deploy_time + INTERVAL 30 MINUTE \
                     GROUP BY w.service_name \
                     HAVING total > 20 AND errors / total > 0.05 \
                   ) \
                 SELECT ServiceName, errors, total FROM post_deploy_errors",
                "high",
                300,
                600,
            ),
            (
                "New error patterns (unseen in past 7 days)",
                "Identifies error log messages that appear 3+ times in the current window \
                 but have never been seen in the previous 7 days. Surfaces novel failure \
                 modes that may indicate new bugs, configuration drift, or dependency changes.",
                "WITH \
                   recent_errors AS ( \
                     SELECT ServiceName, Body, count() AS cnt \
                     FROM otel_logs \
                     WHERE Timestamp BETWEEN @window_start AND @window_end \
                       AND SeverityText IN ('ERROR', 'FATAL') \
                     GROUP BY ServiceName, Body \
                     HAVING cnt >= 3 \
                   ), \
                   historical_errors AS ( \
                     SELECT DISTINCT ServiceName, Body \
                     FROM otel_logs \
                     WHERE Timestamp BETWEEN @window_start - INTERVAL 7 DAY AND @window_start \
                       AND SeverityText IN ('ERROR', 'FATAL') \
                   ) \
                 SELECT re.ServiceName, re.Body, re.cnt \
                 FROM recent_errors re \
                 LEFT JOIN historical_errors he \
                   ON re.ServiceName = he.ServiceName AND re.Body = he.Body \
                 WHERE he.Body IS NULL",
                "medium",
                300,
                300,
            ),
            (
                "Cascading service failures (3+ services)",
                "Detects cascading failures where 3 or more services simultaneously \
                 have >10% error rates. A multi-service failure pattern strongly suggests \
                 a shared dependency issue or infrastructure-level incident.",
                "WITH \
                   failing_services AS ( \
                     SELECT service_name, \
                       countIf(status = 'ERROR' OR http_status_code >= 500) AS errors, \
                       count() AS total \
                     FROM wide_events \
                     WHERE timestamp BETWEEN @window_start AND @window_end \
                     GROUP BY service_name \
                     HAVING total > 10 AND errors / total > 0.1 \
                   ) \
                 SELECT count() AS failing_count \
                 FROM failing_services \
                 HAVING failing_count >= 3",
                "critical",
                300,
                300,
            ),
        ];

        let mut seeded = 0u32;
        for (name, description, query_sql, severity, interval, window) in &defaults {
            if self.default_detection_rule_exists(name, "default")? {
                continue;
            }
            let id = uuid::Uuid::new_v4().to_string();
            self.create_detection_rule(
                &id,
                "default",
                name,
                description,
                query_sql,
                *interval,
                1,       // threshold: fires when >= 1 row
                severity,
                *window,
                true,
                "[]",
                "system",
            )?;
            seeded += 1;
        }

        if seeded > 0 {
            tracing::info!("SIEM: seeded {seeded} new default detection rules ({} total built-in)", defaults.len());
        } else {
            tracing::debug!("SIEM: all {} default detection rules already present", defaults.len());
        }
        Ok(())
    }

    // ── Setup tokens ──

    /// Create a one-time setup token that expires in 48 hours.
    /// Returns the generated 32-char hex token.
    pub fn create_setup_token(
        &self,
        purpose: &str,
        created_by: &str,
        provider: &str,
        hostname: &str,
    ) -> anyhow::Result<String> {
        use rand::Rng;
        let mut rng = rand::rng();
        let bytes: [u8; 16] = rng.random();
        let token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO setup_tokens (token, purpose, created_by, expires_at, provider, hostname) \
             VALUES (?1, ?2, ?3, strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '+48 hours'), ?4, ?5)",
            params![token, purpose, created_by, provider, hostname],
        )?;
        Ok(token)
    }

    /// Validate a setup token: must exist, match purpose, not used, and not expired.
    /// Returns (valid, provider) so callers know which wizard flow to show.
    pub fn validate_setup_token(&self, token: &str, purpose: &str) -> anyhow::Result<(bool, String)> {
        let conn = self.conn.lock().unwrap();
        let result = conn
            .prepare(
                "SELECT provider FROM setup_tokens \
                 WHERE token = ?1 AND purpose = ?2 AND used = 0 \
                 AND expires_at > strftime('%Y-%m-%dT%H:%M:%SZ', 'now')",
            )?
            .query_row(params![token, purpose], |row| row.get::<_, String>(0));
        match result {
            Ok(provider) => Ok((true, provider)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok((false, String::new())),
            Err(e) => Err(e.into()),
        }
    }

    /// Mark a setup token as used after SSO configuration is saved via the magic link.
    pub fn mark_setup_token_used(&self, token: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "UPDATE setup_tokens SET used = 1 WHERE token = ?1 AND used = 0",
            params![token],
        )?;
        Ok(rows > 0)
    }

    // ── Monitor operations (Datadog-style monitors v2) ──

    /// Helper to map a SQLite row to a Monitor struct. Expects columns in canonical order.
    fn row_to_monitor(row: &rusqlite::Row<'_>) -> rusqlite::Result<crate::models::monitor::Monitor> {
        Ok(crate::models::monitor::Monitor {
            id: row.get(0)?,
            tenant_id: row.get(1)?,
            name: row.get(2)?,
            monitor_type: row.get(3)?,
            query_config: row.get(4)?,
            critical: row.get(5)?,
            critical_recovery: row.get(6)?,
            warning: row.get(7)?,
            warning_recovery: row.get(8)?,
            comparator: row.get(9)?,
            eval_window_secs: row.get(10)?,
            eval_interval_secs: row.get(11)?,
            group_by: row.get(12)?,
            state: row.get(13)?,
            group_states: row.get(14)?,
            no_data_action: row.get(15)?,
            no_data_timeframe: row.get(16)?,
            auto_resolve_hours: row.get(17)?,
            message: row.get(18)?,
            notification_channels: row.get(19)?,
            renotify_interval: row.get(20)?,
            tags: row.get(21)?,
            priority: row.get(22)?,
            enabled: row.get(23)?,
            composite_formula: row.get(24)?,
            composite_monitor_ids: row.get(25)?,
            last_eval_at: row.get(26)?,
            last_triggered_at: row.get(27)?,
            created_by: row.get(28)?,
            created_at: row.get(29)?,
            updated_at: row.get(30)?,
        })
    }

    const MONITOR_COLS: &'static str =
        "id, tenant_id, name, type, query_config, critical, critical_recovery, \
         warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, \
         group_by, state, group_states, no_data_action, no_data_timeframe, \
         auto_resolve_hours, message, notification_channels, renotify_interval, \
         tags, priority, enabled, composite_formula, composite_monitor_ids, \
         last_eval_at, last_triggered_at, created_by, created_at, updated_at";

    pub fn list_monitors(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<crate::models::monitor::Monitor>> {
        let conn = self.conn.lock().unwrap();
        let sql = format!(
            "SELECT {} FROM monitors WHERE tenant_id = ?1 ORDER BY created_at DESC",
            Self::MONITOR_COLS,
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map(params![tenant_id], Self::row_to_monitor)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_monitor(
        &self,
        id: &str,
        tenant_id: &str,
    ) -> anyhow::Result<Option<crate::models::monitor::Monitor>> {
        let conn = self.conn.lock().unwrap();
        let sql = format!(
            "SELECT {} FROM monitors WHERE id = ?1 AND tenant_id = ?2",
            Self::MONITOR_COLS,
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query_map(params![id, tenant_id], Self::row_to_monitor)?;
        Ok(rows.next().transpose()?)
    }

    /// Get a monitor by ID without tenant scoping (used by the engine which iterates all tenants).
    pub fn get_monitor_by_id(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<crate::models::monitor::Monitor>> {
        let conn = self.conn.lock().unwrap();
        let sql = format!(
            "SELECT {} FROM monitors WHERE id = ?1",
            Self::MONITOR_COLS,
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query_map(params![id], Self::row_to_monitor)?;
        Ok(rows.next().transpose()?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_monitor(
        &self,
        id: &str,
        tenant_id: &str,
        name: &str,
        monitor_type: &str,
        query_config: &str,
        critical: Option<f64>,
        critical_recovery: Option<f64>,
        warning: Option<f64>,
        warning_recovery: Option<f64>,
        comparator: &str,
        eval_window_secs: i64,
        eval_interval_secs: i64,
        group_by: &str,
        no_data_action: &str,
        no_data_timeframe: i64,
        auto_resolve_hours: Option<i64>,
        message: &str,
        notification_channels: &str,
        renotify_interval: Option<i64>,
        tags: &str,
        priority: Option<i64>,
        enabled: bool,
        composite_formula: &str,
        composite_monitor_ids: &str,
        created_by: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO monitors (
                id, tenant_id, name, type, query_config,
                critical, critical_recovery, warning, warning_recovery, comparator,
                eval_window_secs, eval_interval_secs, group_by,
                no_data_action, no_data_timeframe, auto_resolve_hours,
                message, notification_channels, renotify_interval,
                tags, priority, enabled,
                composite_formula, composite_monitor_ids, created_by
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5,
                ?6, ?7, ?8, ?9, ?10,
                ?11, ?12, ?13,
                ?14, ?15, ?16,
                ?17, ?18, ?19,
                ?20, ?21, ?22,
                ?23, ?24, ?25
            )",
            params![
                id, tenant_id, name, monitor_type, query_config,
                critical, critical_recovery, warning, warning_recovery, comparator,
                eval_window_secs, eval_interval_secs, group_by,
                no_data_action, no_data_timeframe, auto_resolve_hours,
                message, notification_channels, renotify_interval,
                tags, priority, enabled,
                composite_formula, composite_monitor_ids, created_by,
            ],
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_monitor(
        &self,
        id: &str,
        tenant_id: &str,
        name: &str,
        monitor_type: &str,
        query_config: &str,
        critical: Option<f64>,
        critical_recovery: Option<f64>,
        warning: Option<f64>,
        warning_recovery: Option<f64>,
        comparator: &str,
        eval_window_secs: i64,
        eval_interval_secs: i64,
        group_by: &str,
        no_data_action: &str,
        no_data_timeframe: i64,
        auto_resolve_hours: Option<i64>,
        message: &str,
        notification_channels: &str,
        renotify_interval: Option<i64>,
        tags: &str,
        priority: Option<i64>,
        enabled: bool,
        composite_formula: &str,
        composite_monitor_ids: &str,
    ) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE monitors SET
                name = ?3, type = ?4, query_config = ?5,
                critical = ?6, critical_recovery = ?7, warning = ?8, warning_recovery = ?9,
                comparator = ?10,
                eval_window_secs = ?11, eval_interval_secs = ?12, group_by = ?13,
                no_data_action = ?14, no_data_timeframe = ?15, auto_resolve_hours = ?16,
                message = ?17, notification_channels = ?18, renotify_interval = ?19,
                tags = ?20, priority = ?21, enabled = ?22,
                composite_formula = ?23, composite_monitor_ids = ?24,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
             WHERE id = ?1 AND tenant_id = ?2",
            params![
                id, tenant_id, name, monitor_type, query_config,
                critical, critical_recovery, warning, warning_recovery, comparator,
                eval_window_secs, eval_interval_secs, group_by,
                no_data_action, no_data_timeframe, auto_resolve_hours,
                message, notification_channels, renotify_interval,
                tags, priority, enabled,
                composite_formula, composite_monitor_ids,
            ],
        )?;
        Ok(count > 0)
    }

    pub fn delete_monitor(&self, id: &str, tenant_id: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "DELETE FROM monitors WHERE id = ?1 AND tenant_id = ?2",
            params![id, tenant_id],
        )?;
        Ok(count > 0)
    }

    /// Return all enabled monitors across all tenants. Used by the monitor engine.
    pub fn list_enabled_monitors(&self) -> anyhow::Result<Vec<crate::models::monitor::Monitor>> {
        let conn = self.conn.lock().unwrap();
        let sql = format!(
            "SELECT {} FROM monitors WHERE enabled = 1",
            Self::MONITOR_COLS,
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map([], Self::row_to_monitor)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Update monitor state after an evaluation cycle. Called by the engine.
    pub fn update_monitor_state(
        &self,
        id: &str,
        state: &str,
        group_states: &str,
        last_eval_at: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE monitors SET state = ?2, group_states = ?3, last_eval_at = ?4, \
             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?1",
            params![id, state, group_states, last_eval_at],
        )?;
        Ok(())
    }

    /// Update monitor last_triggered_at timestamp.
    pub fn update_monitor_triggered(
        &self,
        id: &str,
        last_triggered_at: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE monitors SET last_triggered_at = ?2 WHERE id = ?1",
            params![id, last_triggered_at],
        )?;
        Ok(())
    }

    pub fn create_monitor_event(
        &self,
        id: &str,
        monitor_id: &str,
        tenant_id: &str,
        group_key: &str,
        prev_state: &str,
        new_state: &str,
        value: Option<f64>,
        threshold: Option<f64>,
        message: &str,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO monitor_events (id, monitor_id, tenant_id, group_key, prev_state, new_state, value, threshold, message) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![id, monitor_id, tenant_id, group_key, prev_state, new_state, value, threshold, message],
        )?;
        Ok(())
    }

    pub fn list_monitor_events(
        &self,
        monitor_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::monitor::MonitorEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, monitor_id, tenant_id, group_key, prev_state, new_state, value, threshold, message, created_at \
             FROM monitor_events WHERE monitor_id = ?1 ORDER BY created_at DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![monitor_id, limit], |row| {
                Ok(crate::models::monitor::MonitorEvent {
                    id: row.get(0)?,
                    monitor_id: row.get(1)?,
                    tenant_id: row.get(2)?,
                    group_key: row.get(3)?,
                    prev_state: row.get(4)?,
                    new_state: row.get(5)?,
                    value: row.get(6)?,
                    threshold: row.get(7)?,
                    message: row.get(8)?,
                    created_at: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn count_monitors(&self, tenant_id: &str) -> anyhow::Result<i64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .prepare("SELECT COUNT(*) FROM monitors WHERE tenant_id = ?1")?
            .query_row(params![tenant_id], |row| row.get(0))?;
        Ok(count)
    }

    /// Set monitor enabled status (for mute/unmute).
    pub fn set_monitor_enabled(&self, id: &str, tenant_id: &str, enabled: bool) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE monitors SET enabled = ?3, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?1 AND tenant_id = ?2",
            params![id, tenant_id, enabled],
        )?;
        Ok(count > 0)
    }

    // ── Alert Maintenance Windows ─────────────────────────────────────────────

    pub fn create_maintenance_window(
        &self, id: &str, name: &str, scope: &str, starts_at: &str, ends_at: &str,
    ) -> anyhow::Result<()> {
        self.conn.lock().unwrap().execute(
            "INSERT INTO alert_maintenance_windows (id, name, scope, starts_at, ends_at) VALUES (?1,?2,?3,?4,?5)",
            params![id, name, scope, starts_at, ends_at],
        )?;
        Ok(())
    }

    pub fn list_maintenance_windows(&self) -> anyhow::Result<Vec<(String,String,String,String,String,String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, scope, starts_at, ends_at, created_at FROM alert_maintenance_windows ORDER BY starts_at DESC"
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?))
        })?.collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn delete_maintenance_window(&self, id: &str) -> anyhow::Result<bool> {
        let n = self.conn.lock().unwrap().execute(
            "DELETE FROM alert_maintenance_windows WHERE id = ?1",
            params![id],
        )?;
        Ok(n > 0)
    }

    /// Returns true if `now_str` (ISO 8601) falls within any active maintenance window
    /// that covers this alert_id (or all alerts if scope = 'all').
    pub fn is_in_maintenance(&self, now_str: &str, alert_id: Option<&str>) -> bool {
        let conn = self.conn.lock().unwrap();
        let alert_scope = alert_id.map(|id| format!("alert:{id}")).unwrap_or_default();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM alert_maintenance_windows
                 WHERE starts_at <= ?1 AND ends_at >= ?1
                   AND (scope = 'all' OR scope = ?2)",
                params![now_str, alert_scope],
                |r| r.get(0),
            )
            .unwrap_or(0);
        count > 0
    }

    // ── Trace Funnels ─────────────────────────────────────────────────────────

    pub fn create_funnel(&self, id: &str, name: &str, steps_json: &str, tenant_id: &str) -> anyhow::Result<()> {
        self.conn.lock().unwrap().execute(
            "INSERT INTO trace_funnels (id, name, steps_json, tenant_id) VALUES (?1,?2,?3,?4)",
            params![id, name, steps_json, tenant_id],
        )?;
        Ok(())
    }

    pub fn list_funnels(&self, tenant_id: &str) -> anyhow::Result<Vec<(String, String, String, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, steps_json, created_at FROM trace_funnels WHERE tenant_id = ?1 ORDER BY created_at DESC"
        )?;
        let rows = stmt.query_map(params![tenant_id], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })?.collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_funnel(&self, id: &str, tenant_id: &str) -> anyhow::Result<Option<(String, String, String, String)>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT id, name, steps_json, created_at FROM trace_funnels WHERE id = ?1 AND tenant_id = ?2",
            params![id, tenant_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        ).optional().map_err(Into::into)
    }

    pub fn delete_funnel(&self, id: &str, tenant_id: &str) -> anyhow::Result<bool> {
        let n = self.conn.lock().unwrap().execute(
            "DELETE FROM trace_funnels WHERE id = ?1 AND tenant_id = ?2",
            params![id, tenant_id],
        )?;
        Ok(n > 0)
    }
}
