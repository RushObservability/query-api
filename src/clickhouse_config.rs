use clickhouse::Client;
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

// ── Module-level row types used by helper methods ─────────────────────────────

pub type SsoProviderRow = (
    String, String, String, bool, String, String, String, String,
    String, String, String, String, bool, String, String,
    String, String, String, String,
);

#[derive(clickhouse::Row, serde::Deserialize)]
pub struct AlertRuleRow {
    pub id: String, pub name: String, pub description: String, pub enabled: u8,
    pub signal_type: String, pub query_config: String, pub condition_op: String,
    pub condition_threshold: f64, pub eval_interval_secs: i64,
    pub notification_channel_ids: String, pub runbook_url: String, pub state: String,
    pub last_eval_at: String, pub last_triggered_at: String,
    pub created_at: String, pub updated_at: String,
}

#[derive(clickhouse::Row, serde::Deserialize)]
pub struct SloRow {
    pub id: String, pub name: String, pub description: String, pub enabled: u8,
    pub slo_type: String, pub indicator_type: String, pub service_name: String,
    pub metric_name: String, pub window_type: String, pub target_percentage: f64,
    pub threshold_ms: Option<f64>, pub threshold_value: Option<f64>, pub threshold_op: String,
    pub error_filters: String, pub total_filters: String, pub eval_interval_secs: i64,
    pub notification_channel_ids: String, pub state: String,
    pub error_budget_remaining: Option<f64>, pub error_count: Option<i64>,
    pub total_count: Option<i64>, pub last_eval_at: String, pub last_breached_at: String,
    pub created_at: String, pub updated_at: String,
}

#[derive(clickhouse::Row, serde::Deserialize)]
pub struct AnomalyRuleRow {
    pub id: String, pub name: String, pub description: String, pub enabled: u8,
    pub source: String, pub pattern: String, pub query: String,
    pub service_name: String, pub apm_metric: String, pub sensitivity: f64,
    pub alpha: f64, pub eval_interval_secs: i64, pub window_secs: i64,
    pub split_labels: String, pub notification_channel_ids: String, pub state: String,
    pub last_eval_at: String, pub last_triggered_at: String,
    pub created_at: String, pub updated_at: String,
}

#[derive(clickhouse::Row, serde::Deserialize)]
pub struct MonitorRow {
    pub id: String, pub tenant_id: String, pub name: String, pub monitor_type: String,
    pub query_config: String, pub critical: Option<f64>, pub critical_recovery: Option<f64>,
    pub warning: Option<f64>, pub warning_recovery: Option<f64>, pub comparator: String,
    pub eval_window_secs: i64, pub eval_interval_secs: i64, pub group_by: String,
    pub state: String, pub group_states: String, pub no_data_action: String,
    pub no_data_timeframe: i64, pub auto_resolve_hours: Option<i64>,
    pub message: String, pub notification_channels: String,
    pub renotify_interval: Option<i64>, pub tags: String, pub priority: Option<i64>,
    pub enabled: u8, pub composite_formula: String, pub composite_monitor_ids: String,
    pub last_eval_at: String, pub last_triggered_at: String,
    pub created_by: String, pub created_at: String, pub updated_at: String,
}

pub struct ConfigDb {
    pub client: Client,
}

impl ConfigDb {
    pub async fn open(url: &str, user: &str, password: &str) -> anyhow::Result<Self> {
        let client = Client::default()
            .with_url(url)
            .with_user(user)
            .with_password(password);
        let db = Self { client };
        db.run_migrations().await?;
        Ok(db)
    }

    async fn run_migrations(&self) -> anyhow::Result<()> {
        let ddls = vec![
            // ── Tenants ──────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_tenants (
                id           String,
                name         String,
                enabled      UInt8 DEFAULT 1,
                auth_required UInt8 DEFAULT 1,
                created_at   String DEFAULT toString(now()),
                version      UInt64,
                is_deleted   UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── Groups ────────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_groups (
                id          String,
                name        String,
                description String DEFAULT '',
                scopes      String DEFAULT '[\"all\"]',
                permissions String DEFAULT '[\"read\"]',
                system      UInt8 DEFAULT 0,
                created_at  String DEFAULT toString(now()),
                version     UInt64,
                is_deleted  UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── Users ─────────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_users (
                id            String,
                username      String,
                password_hash String,
                display_name  String DEFAULT '',
                tenant_id     String DEFAULT 'default',
                role          String DEFAULT 'admin',
                enabled       UInt8 DEFAULT 1,
                auth_provider String DEFAULT 'local',
                external_id   String DEFAULT '',
                created_at    String DEFAULT toString(now()),
                version       UInt64,
                is_deleted    UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── Sessions ──────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_sessions (
                token      String,
                user_id    String,
                created_at String DEFAULT toString(now()),
                expires_at String
            ) ENGINE = MergeTree()
            ORDER BY (token)
            TTL parseDateTimeBestEffort(expires_at) + INTERVAL 0 SECOND",

            // ── Group tenants ─────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_group_tenants (
                group_id  String,
                tenant_id String,
                version   UInt64,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (group_id, tenant_id)",

            // ── User groups ───────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_user_groups (
                user_id   String,
                group_id  String,
                version   UInt64,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (user_id, group_id)",

            // ── SSO providers ─────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_sso_providers (
                id                    String,
                name                  String,
                protocol              String,
                enabled               UInt8 DEFAULT 0,
                client_id             String DEFAULT '',
                client_secret         String DEFAULT '',
                issuer_url            String DEFAULT '',
                oidc_scopes           String DEFAULT 'openid profile email groups',
                groups_claim          String DEFAULT 'groups',
                email_claim           String DEFAULT 'email',
                first_name_claim      String DEFAULT 'given_name',
                last_name_claim       String DEFAULT 'family_name',
                jit_provisioning      UInt8 DEFAULT 1,
                default_group_id      String DEFAULT '',
                saml_idp_metadata_url String DEFAULT '',
                saml_idp_sso_url      String DEFAULT '',
                saml_idp_cert         String DEFAULT '',
                saml_sp_entity_id     String DEFAULT '',
                created_at            String DEFAULT toString(now()),
                version               UInt64,
                is_deleted            UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── IdP group mappings ────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_idp_group_mappings (
                id            String,
                idp_group     String,
                rush_group_id String,
                provider_id   String DEFAULT 'default',
                created_at    String DEFAULT toString(now()),
                version       UInt64,
                is_deleted    UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── SSO state ─────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_sso_state (
                state      String,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (state)
            TTL created_at + INTERVAL 10 MINUTE",

            // ── Setup tokens ──────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_setup_tokens (
                token      String,
                purpose    String,
                created_by String,
                expires_at String,
                used       UInt8 DEFAULT 0,
                provider   String DEFAULT '',
                hostname   String DEFAULT '',
                version    UInt64,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (token)",

            // ── API keys ──────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_api_keys (
                id         String,
                name       String,
                key_hash   String,
                prefix     String,
                tenant_id  String DEFAULT 'default',
                created_at String DEFAULT toString(now()),
                version    UInt64,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── Settings ──────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_settings (
                key        String,
                value      String,
                version    UInt64,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (key)",

            // ── Custom skills ─────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_custom_skills (
                id            String,
                name          String,
                title         String,
                description   String,
                content       String,
                allowed_tools String DEFAULT '[]',
                enabled       UInt8 DEFAULT 1,
                created_by    String DEFAULT '',
                created_at    String DEFAULT toString(now()),
                updated_at    String DEFAULT toString(now()),
                version       UInt64,
                is_deleted    UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── Investigation sessions (owned by sre-agent; mutable → ReplacingMergeTree) ──
            "CREATE TABLE IF NOT EXISTS config_investigation_sessions (
                id                String,
                tenant_id         String DEFAULT 'default',
                title             String DEFAULT '',
                status            String DEFAULT 'active',
                template_id       String DEFAULT '',
                created_by        String DEFAULT '',
                created_at        String DEFAULT toString(now()),
                updated_at        String DEFAULT toString(now()),
                working_memory    String DEFAULT '{}',
                prompt_tokens     Int64 DEFAULT 0,
                completion_tokens Int64 DEFAULT 0,
                llm_model         String DEFAULT '',
                version           UInt64,
                is_deleted        UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── Investigation turns (owned by sre-agent; append-only) ──
            "CREATE TABLE IF NOT EXISTS config_investigation_turns (
                id          String,
                session_id  String,
                turn_index  Int64,
                role        String,
                content     String,
                tool_calls  String DEFAULT '[]',
                report_kind String DEFAULT '',
                created_at  String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (session_id, turn_index)",

            // ── Service links ─────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_service_links (
                service_name   String,
                github_repo    String,
                default_branch String DEFAULT 'main',
                root_path      String DEFAULT '',
                updated_at     String DEFAULT toString(now()),
                version        UInt64,
                is_deleted     UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (service_name)",

            // ── Dashboards ────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_dashboards (
                id          String,
                name        String,
                description String DEFAULT '',
                tenant_id   String DEFAULT 'default',
                owner_id    String DEFAULT '',
                visibility  String DEFAULT 'tenant',
                tags        String DEFAULT '[]',
                created_at  String DEFAULT toString(now()),
                updated_at  String DEFAULT toString(now()),
                version     UInt64,
                is_deleted  UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── Widgets ───────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_widgets (
                id             String,
                dashboard_id   String,
                title          String,
                widget_type    String,
                query_config   String,
                position       String,
                display_config String DEFAULT '{}',
                created_at     String DEFAULT toString(now()),
                updated_at     String DEFAULT toString(now()),
                version        UInt64,
                is_deleted     UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── Dashboard templates ───────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_dashboard_templates (
                id            String,
                name          String,
                description   String DEFAULT '',
                category      String DEFAULT 'general',
                is_builtin    UInt8 DEFAULT 0,
                template_json String,
                tags          String DEFAULT '[]',
                created_at    String DEFAULT toString(now()),
                version       UInt64,
                is_deleted    UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── Notification channels ─────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_notification_channels (
                id           String,
                tenant_id    String DEFAULT 'default',
                name         String,
                channel_type String,
                config       String DEFAULT '{}',
                enabled      UInt8 DEFAULT 1,
                created_at   String DEFAULT toString(now()),
                version      UInt64,
                is_deleted   UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── Notification log ──────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_notification_log (
                id         String,
                channel_id String,
                tenant_id  String,
                alert_type String,
                alert_name String,
                severity   String DEFAULT '',
                status     String,
                error      String DEFAULT '',
                created_at String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (tenant_id, created_at)",

            // ── Alert rules ───────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_alert_rules (
                id                       String,
                name                     String,
                description              String DEFAULT '',
                enabled                  UInt8 DEFAULT 1,
                signal_type              String DEFAULT 'apm',
                query_config             String,
                condition_op             String,
                condition_threshold      Float64,
                eval_interval_secs       Int64 DEFAULT 60,
                notification_channel_ids String DEFAULT '[]',
                runbook_url              String DEFAULT '',
                state                    String DEFAULT 'ok',
                last_eval_at             String DEFAULT '',
                last_triggered_at        String DEFAULT '',
                created_at               String DEFAULT toString(now()),
                updated_at               String DEFAULT toString(now()),
                version                  UInt64,
                is_deleted               UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── Alert events ──────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_alert_events (
                id         String,
                rule_id    String,
                state      String,
                value      Float64,
                threshold  Float64,
                message    String,
                created_at String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (rule_id, created_at)",

            // ── Anomaly rules ─────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_anomaly_rules (
                id                       String,
                name                     String,
                description              String DEFAULT '',
                enabled                  UInt8 DEFAULT 1,
                source                   String,
                pattern                  String DEFAULT '',
                query                    String DEFAULT '',
                service_name             String DEFAULT '',
                apm_metric               String DEFAULT '',
                sensitivity              Float64 DEFAULT 3.0,
                alpha                    Float64 DEFAULT 0.25,
                eval_interval_secs       Int64 DEFAULT 300,
                window_secs              Int64 DEFAULT 3600,
                notification_channel_ids String DEFAULT '[]',
                split_labels             String DEFAULT '[]',
                state                    String DEFAULT 'normal',
                last_eval_at             String DEFAULT '',
                last_triggered_at        String DEFAULT '',
                created_at               String DEFAULT toString(now()),
                updated_at               String DEFAULT toString(now()),
                version                  UInt64,
                is_deleted               UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── Anomaly events ────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_anomaly_events (
                id         String,
                rule_id    String,
                state      String,
                metric     String DEFAULT '',
                value      Float64,
                expected   Float64,
                deviation  Float64 DEFAULT 0.0,
                message    String,
                created_at String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (rule_id, created_at)",

            // ── Monitors ──────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_monitors (
                id                    String,
                tenant_id             String DEFAULT 'default',
                name                  String,
                monitor_type          String,
                query_config          String,
                critical              Nullable(Float64),
                critical_recovery     Nullable(Float64),
                warning               Nullable(Float64),
                warning_recovery      Nullable(Float64),
                comparator            String DEFAULT 'above',
                eval_window_secs      Int64 DEFAULT 300,
                eval_interval_secs    Int64 DEFAULT 60,
                group_by              String DEFAULT '[]',
                state                 String DEFAULT 'ok',
                group_states          String DEFAULT '{}',
                no_data_action        String DEFAULT 'show',
                no_data_timeframe     Int64 DEFAULT 600,
                auto_resolve_hours    Nullable(Int64),
                message               String DEFAULT '',
                notification_channels String DEFAULT '[]',
                renotify_interval     Nullable(Int64),
                tags                  String DEFAULT '[]',
                priority              Nullable(Int64),
                enabled               UInt8 DEFAULT 1,
                composite_formula     String DEFAULT '',
                composite_monitor_ids String DEFAULT '[]',
                last_eval_at          String DEFAULT '',
                last_triggered_at     String DEFAULT '',
                created_by            String DEFAULT '',
                created_at            String DEFAULT toString(now()),
                updated_at            String DEFAULT toString(now()),
                version               UInt64,
                is_deleted            UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── Monitor events ────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_monitor_events (
                id         String,
                monitor_id String,
                tenant_id  String,
                group_key  String DEFAULT '',
                prev_state String,
                new_state  String,
                value      Nullable(Float64),
                threshold  Nullable(Float64),
                message    String DEFAULT '',
                created_at String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (monitor_id, created_at)",

            // ── SLOs ──────────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_slos (
                id                       String,
                name                     String,
                description              String DEFAULT '',
                enabled                  UInt8 DEFAULT 1,
                tenant_id                String DEFAULT 'default',
                slo_type                 String DEFAULT 'trace',
                service_name             String,
                metric_name              String DEFAULT '',
                window_type              String,
                target_percentage        Float64,
                threshold_ms             Nullable(Float64),
                threshold_value          Nullable(Float64),
                threshold_op             String DEFAULT '',
                error_filters            String,
                total_filters            String,
                eval_interval_secs       Int64 DEFAULT 60,
                notification_channel_ids String DEFAULT '[]',
                indicator_type           String DEFAULT 'availability',
                state                    String DEFAULT 'compliant',
                error_budget_remaining   Nullable(Float64),
                error_count              Nullable(Int64),
                total_count              Nullable(Int64),
                last_eval_at             String DEFAULT '',
                last_breached_at         String DEFAULT '',
                created_at               String DEFAULT toString(now()),
                updated_at               String DEFAULT toString(now()),
                version                  UInt64,
                is_deleted               UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── SLO events ────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_slo_events (
                id                     String,
                slo_id                 String,
                state                  String,
                error_count            Int64,
                total_count            Int64,
                error_budget_remaining Float64,
                message                String,
                created_at             String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (slo_id, created_at)",

            // ── Deploy markers ────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_deploy_markers (
                id           String,
                service_name String,
                version      String DEFAULT '',
                commit_sha   String DEFAULT '',
                description  String DEFAULT '',
                environment  String DEFAULT '',
                deployed_by  String DEFAULT '',
                deployed_at  String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (service_name, deployed_at)",

            // ── Detection rules ───────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_detection_rules (
                id                String,
                tenant_id         String DEFAULT 'default',
                name              String,
                description       String DEFAULT '',
                query_sql         String,
                interval_secs     Int64 DEFAULT 300,
                threshold         Int64 DEFAULT 1,
                severity          String DEFAULT 'medium',
                window_secs       Int64 DEFAULT 300,
                enabled           UInt8 DEFAULT 1,
                channels          String DEFAULT '[]',
                created_by        String DEFAULT '',
                last_eval_at      String DEFAULT '',
                last_triggered_at String DEFAULT '',
                created_at        String DEFAULT toString(now()),
                updated_at        String DEFAULT toString(now()),
                version           UInt64,
                is_deleted        UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── Detection events ──────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_detection_events (
                id          String,
                rule_id     String,
                tenant_id   String,
                severity    String,
                match_count Int64 DEFAULT 0,
                sample_data String DEFAULT '[]',
                created_at  String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (tenant_id, created_at)",

            // ── Tenant retention ──────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_tenant_retention (
                tenant_id   String,
                signal      String,
                retain_days Int32,
                version     UInt64,
                is_deleted  UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (tenant_id, signal)",

            // ── Maintenance windows ───────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_maintenance_windows (
                id         String,
                name       String,
                scope      String DEFAULT 'all',
                starts_at  String,
                ends_at    String,
                created_at String DEFAULT toString(now()),
                version    UInt64,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",

            // ── Trace funnels ─────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_trace_funnels (
                id         String,
                name       String,
                steps_json String DEFAULT '[]',
                tenant_id  String DEFAULT 'default',
                created_at String DEFAULT toString(now()),
                version    UInt64,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
        ];

        for ddl in ddls {
            self.client.query(ddl).execute().await
                .map_err(|e| anyhow::anyhow!("DDL failed: {e}\nSQL: {ddl}"))?;
        }
        Ok(())
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn now_str() -> String {
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
    }

    fn next_version() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64
    }

    // ── Tenant operations ─────────────────────────────────────────────────────

    pub async fn ensure_default_tenant(&self) -> anyhow::Result<()> {
        let existing = self.get_tenant("default").await?;
        if existing.is_none() {
            let ver = Self::next_version();
            let now = Self::now_str();
            self.client
                .query("INSERT INTO config_tenants (id, name, enabled, auth_required, created_at, version, is_deleted) VALUES (?, ?, 1, 1, ?, ?, 0)")
                .bind("default")
                .bind("default")
                .bind(&now)
                .bind(ver)
                .execute()
                .await?;
        }
        Ok(())
    }

    pub async fn resolve_tenant_for_api_key(&self, key_hash: &str) -> anyhow::Result<Option<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { tenant_id: String }
        let result = self.client
            .query("SELECT tenant_id FROM config_api_keys FINAL WHERE key_hash = ? AND is_deleted = 0 LIMIT 1")
            .bind(key_hash)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(r.tenant_id)),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn list_tenants(&self) -> anyhow::Result<Vec<(String, String, bool, bool, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, name: String, enabled: u8, auth_required: u8, created_at: String }
        let rows = self.client
            .query("SELECT id, name, enabled, auth_required, created_at FROM config_tenants FINAL WHERE is_deleted = 0 ORDER BY created_at ASC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| (r.id, r.name, r.enabled != 0, r.auth_required != 0, r.created_at)).collect())
    }

    pub async fn create_tenant(&self, id: &str, name: &str) -> anyhow::Result<()> {
        let ver = Self::next_version();
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_tenants (id, name, enabled, auth_required, created_at, version, is_deleted) VALUES (?, ?, 1, 1, ?, ?, 0)")
            .bind(id)
            .bind(name)
            .bind(&now)
            .bind(ver)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn get_tenant(&self, id: &str) -> anyhow::Result<Option<(String, String, bool, bool, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, name: String, enabled: u8, auth_required: u8, created_at: String }
        let result = self.client
            .query("SELECT id, name, enabled, auth_required, created_at FROM config_tenants FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some((r.id, r.name, r.enabled != 0, r.auth_required != 0, r.created_at))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_tenant_id_by_name(&self, name: &str) -> anyhow::Result<Option<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String }
        let result = self.client
            .query("SELECT id FROM config_tenants FINAL WHERE name = ? AND enabled = 1 AND is_deleted = 0 LIMIT 1")
            .bind(name)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(r.id)),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn set_tenant_enabled(&self, id: &str, enabled: bool) -> anyhow::Result<bool> {
        let existing = self.get_tenant(id).await?;
        let (_, name, _, auth_required, created_at) = match existing {
            Some(t) => t,
            None => return Ok(false),
        };
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_tenants (id, name, enabled, auth_required, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, 0)")
            .bind(id)
            .bind(&name)
            .bind(if enabled { 1u8 } else { 0u8 })
            .bind(if auth_required { 1u8 } else { 0u8 })
            .bind(&created_at)
            .bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn is_tenant_enabled(&self, name_or_id: &str) -> bool {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { enabled: u8 }
        let result = self.client
            .query("SELECT enabled FROM config_tenants FINAL WHERE (id = ? OR name = ?) AND is_deleted = 0 LIMIT 1")
            .bind(name_or_id)
            .bind(name_or_id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => r.enabled != 0,
            Err(_) => false,
        }
    }

    pub async fn is_tenant_auth_required(&self, name_or_id: &str) -> bool {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { auth_required: u8 }
        let result = self.client
            .query("SELECT auth_required FROM config_tenants FINAL WHERE (id = ? OR name = ?) AND is_deleted = 0 LIMIT 1")
            .bind(name_or_id)
            .bind(name_or_id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => r.auth_required != 0,
            Err(_) => false,
        }
    }

    pub async fn set_tenant_auth_required(&self, id: &str, auth_required: bool) -> anyhow::Result<bool> {
        let existing = self.get_tenant(id).await?;
        let (_, name, enabled, _, created_at) = match existing {
            Some(t) => t,
            None => return Ok(false),
        };
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_tenants (id, name, enabled, auth_required, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, 0)")
            .bind(id)
            .bind(&name)
            .bind(if enabled { 1u8 } else { 0u8 })
            .bind(if auth_required { 1u8 } else { 0u8 })
            .bind(&created_at)
            .bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn delete_tenant(&self, id: &str) -> anyhow::Result<bool> {
        let existing = self.get_tenant(id).await?;
        let (_, name, enabled, auth_required, created_at) = match existing {
            Some(t) => t,
            None => return Ok(false),
        };
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_tenants (id, name, enabled, auth_required, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, 1)")
            .bind(id)
            .bind(&name)
            .bind(if enabled { 1u8 } else { 0u8 })
            .bind(if auth_required { 1u8 } else { 0u8 })
            .bind(&created_at)
            .bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    // ── Tenant retention operations ───────────────────────────────────────────

    pub async fn get_tenant_retention(&self, tenant_id: &str) -> anyhow::Result<Vec<(String, i32)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { signal: String, retain_days: i32 }
        let rows = self.client
            .query("SELECT signal, retain_days FROM config_tenant_retention FINAL WHERE tenant_id = ? AND is_deleted = 0")
            .bind(tenant_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| (r.signal, r.retain_days)).collect())
    }

    pub async fn set_tenant_retention(&self, tenant_id: &str, signal: &str, days: i32) -> anyhow::Result<()> {
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_tenant_retention (tenant_id, signal, retain_days, version, is_deleted) VALUES (?, ?, ?, ?, 0)")
            .bind(tenant_id)
            .bind(signal)
            .bind(days)
            .bind(ver)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn delete_tenant_retention(&self, tenant_id: &str, signal: &str) -> anyhow::Result<bool> {
        let existing = self.get_tenant_retention(tenant_id).await?;
        let found = existing.iter().find(|(s, _)| s == signal);
        if found.is_none() { return Ok(false); }
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_tenant_retention (tenant_id, signal, retain_days, version, is_deleted) VALUES (?, ?, 0, ?, 1)")
            .bind(tenant_id)
            .bind(signal)
            .bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn list_all_tenant_retention(&self) -> anyhow::Result<Vec<(String, String, i32)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { tenant_id: String, signal: String, retain_days: i32 }
        let rows = self.client
            .query("SELECT tenant_id, signal, retain_days FROM config_tenant_retention FINAL WHERE is_deleted = 0 ORDER BY tenant_id, signal")
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| (r.tenant_id, r.signal, r.retain_days)).collect())
    }

    // ── User & session operations ──────────────────────────────────────────────

    pub async fn ensure_default_admin(&self) -> anyhow::Result<()> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Count { n: u64 }
        let row = self.client
            .query("SELECT count() AS n FROM config_users FINAL WHERE is_deleted = 0")
            .fetch_one::<Count>()
            .await?;
        if row.n > 0 { return Ok(()); }

        let initial_password = std::env::var("INITIAL_ADMIN_PASSWORD")
            .unwrap_or_else(|_| {
                use rand::Rng;
                let mut rng = rand::rng();
                (0..24).map(|_| rng.sample(rand::distr::Alphanumeric) as char).collect()
            });

        let id = uuid::Uuid::new_v4().to_string();
        let password_hash = hash_password(&initial_password)?;
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_users (id, username, password_hash, display_name, tenant_id, role, enabled, auth_provider, external_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, 1, 'local', '', ?, ?, 0)")
            .bind(&id)
            .bind("admin")
            .bind(&password_hash)
            .bind("Admin")
            .bind("default")
            .bind("admin")
            .bind(&now)
            .bind(ver)
            .execute()
            .await?;

        tracing::warn!(
            username = "admin",
            "Rush initial admin credentials — Username: admin, Password: {initial_password} — \
             Change this password immediately after first login."
        );
        tracing::info!("default admin user created");
        Ok(())
    }

    pub async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Option<(String, String, String, String, String)> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            username: String,
            password_hash: String,
            display_name: String,
            tenant_id: String,
        }
        let result = self.client
            .query("SELECT id, username, password_hash, display_name, tenant_id FROM config_users FINAL WHERE username = ? AND enabled = 1 AND is_deleted = 0 LIMIT 1")
            .bind(username)
            .fetch_one::<Row>()
            .await;
        let row = result.ok()?;
        if !verify_password(password, &row.password_hash) {
            return None;
        }
        // Derive role from group membership
        let role = self.derive_user_role(&row.id).await.unwrap_or_else(|_| "viewer".to_string());
        Some((row.id, row.username, row.display_name, row.tenant_id, role))
    }

    async fn derive_user_role(&self, user_id: &str) -> anyhow::Result<String> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { permissions: String }
        let rows = self.client
            .query("SELECT g.permissions FROM config_user_groups ug FINAL JOIN config_groups g FINAL ON ug.group_id = g.id WHERE ug.user_id = ? AND ug.is_deleted = 0 AND g.is_deleted = 0")
            .bind(user_id)
            .fetch_all::<Row>()
            .await?;
        for row in &rows {
            if let Ok(perms) = serde_json::from_str::<Vec<String>>(&row.permissions) {
                if perms.contains(&"admin".to_string()) { return Ok("admin".to_string()); }
            }
        }
        for row in &rows {
            if let Ok(perms) = serde_json::from_str::<Vec<String>>(&row.permissions) {
                if perms.contains(&"write".to_string()) { return Ok("write".to_string()); }
            }
        }
        Ok("viewer".to_string())
    }

    pub async fn create_session(&self, user_id: &str) -> anyhow::Result<String> {
        let token: String = {
            use rand::Rng;
            let mut rng = rand::rng();
            let bytes: [u8; 32] = rng.random();
            bytes.iter().map(|b| format!("{b:02x}")).collect()
        };

        let created_at = Self::now_str();
        let expires_at = (chrono::Utc::now() + chrono::Duration::hours(24))
            .format("%Y-%m-%d %H:%M:%S").to_string();
        self.client
            .query("INSERT INTO config_sessions (token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)")
            .bind(&token)
            .bind(user_id)
            .bind(&created_at)
            .bind(&expires_at)
            .execute()
            .await?;
        Ok(token)
    }

    pub async fn get_session_user(&self, token: &str) -> Option<(String, String, String, String, String)> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, username: String, display_name: String, tenant_id: String, expires_at: String, user_id: String }
        let now = Self::now_str();
        let result = self.client
            .query("SELECT u.id, u.username, u.display_name, u.tenant_id, s.expires_at, s.user_id FROM config_sessions s JOIN config_users u FINAL ON s.user_id = u.id WHERE s.token = ? AND u.enabled = 1 AND u.is_deleted = 0 AND s.expires_at > ? LIMIT 1")
            .bind(token)
            .bind(&now)
            .fetch_one::<Row>()
            .await;
        let row = result.ok()?;
        let role = self.derive_user_role(&row.id).await.unwrap_or_else(|_| "viewer".to_string());
        Some((row.id, row.username, row.display_name, row.tenant_id, role))
    }

    pub async fn delete_session(&self, token: &str) {
        // Sessions use MergeTree with TTL; we delete by inserting a tombstone via ALTER DELETE
        // For simplicity we use the lightweight mutation approach
        let _ = self.client
            .query("ALTER TABLE config_sessions DELETE WHERE token = ?")
            .bind(token)
            .execute()
            .await;
    }

    pub async fn list_users(&self) -> anyhow::Result<Vec<(String, String, String, String, bool, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, username: String, display_name: String, tenant_id: String, enabled: u8, created_at: String }
        let rows = self.client
            .query("SELECT id, username, display_name, tenant_id, enabled, created_at FROM config_users FINAL WHERE is_deleted = 0 ORDER BY created_at ASC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| (r.id, r.username, r.display_name, r.tenant_id, r.enabled != 0, r.created_at)).collect())
    }

    pub async fn create_user(&self, username: &str, password: &str, display_name: &str) -> anyhow::Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let password_hash = hash_password(password)?;
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_users (id, username, password_hash, display_name, tenant_id, role, enabled, auth_provider, external_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, 'default', 'viewer', 1, 'local', '', ?, ?, 0)")
            .bind(&id)
            .bind(username)
            .bind(&password_hash)
            .bind(display_name)
            .bind(&now)
            .bind(ver)
            .execute()
            .await?;
        Ok(id)
    }

    pub async fn delete_user(&self, id: &str) -> anyhow::Result<bool> {
        let existing = self.get_user(id).await?;
        if existing.is_none() { return Ok(false); }
        // Remove sessions
        let _ = self.client
            .query("ALTER TABLE config_sessions DELETE WHERE user_id = ?")
            .bind(id)
            .execute()
            .await;
        // Soft-delete user
        let (_, username, display_name, tenant_id, _, created_at) = existing.unwrap();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_users (id, username, password_hash, display_name, tenant_id, role, enabled, auth_provider, external_id, created_at, version, is_deleted) VALUES (?, ?, '!deleted', ?, ?, 'viewer', 0, 'local', '', ?, ?, 1)")
            .bind(id)
            .bind(&username)
            .bind(&display_name)
            .bind(&tenant_id)
            .bind(&created_at)
            .bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn get_user(&self, id: &str) -> anyhow::Result<Option<(String, String, String, String, bool, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, username: String, display_name: String, tenant_id: String, enabled: u8, created_at: String }
        let result = self.client
            .query("SELECT id, username, display_name, tenant_id, enabled, created_at FROM config_users FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some((r.id, r.username, r.display_name, r.tenant_id, r.enabled != 0, r.created_at))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn change_password(&self, user_id: &str, new_password: &str) -> anyhow::Result<bool> {
        let existing = self.get_user(user_id).await?;
        let (_, username, display_name, tenant_id, enabled, created_at) = match existing {
            Some(u) => u,
            None => return Ok(false),
        };
        let password_hash = hash_password(new_password)?;
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_users (id, username, password_hash, display_name, tenant_id, role, enabled, auth_provider, external_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, 'viewer', ?, 'local', '', ?, ?, 0)")
            .bind(user_id)
            .bind(&username)
            .bind(&password_hash)
            .bind(&display_name)
            .bind(&tenant_id)
            .bind(if enabled { 1u8 } else { 0u8 })
            .bind(&created_at)
            .bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn delete_sessions_for_user(&self, user_id: &str) -> anyhow::Result<()> {
        self.client
            .query("ALTER TABLE config_sessions DELETE WHERE user_id = ?")
            .bind(user_id)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn set_user_enabled(&self, user_id: &str, enabled: bool) -> anyhow::Result<bool> {
        let existing = self.get_user(user_id).await?;
        let (_, username, display_name, tenant_id, _, created_at) = match existing {
            Some(u) => u,
            None => return Ok(false),
        };
        // Need password hash — fetch it separately
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct PwRow { password_hash: String }
        let pw = self.client
            .query("SELECT password_hash FROM config_users FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(user_id)
            .fetch_one::<PwRow>()
            .await?;
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_users (id, username, password_hash, display_name, tenant_id, role, enabled, auth_provider, external_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, 'viewer', ?, 'local', '', ?, ?, 0)")
            .bind(user_id)
            .bind(&username)
            .bind(&pw.password_hash)
            .bind(&display_name)
            .bind(&tenant_id)
            .bind(if enabled { 1u8 } else { 0u8 })
            .bind(&created_at)
            .bind(ver)
            .execute()
            .await?;
        // Invalidate all sessions when disabling a user
        if !enabled {
            let _ = self.delete_sessions_for_user(user_id).await;
        }
        Ok(true)
    }

    pub async fn get_username(&self, user_id: &str) -> anyhow::Result<Option<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { username: String }
        let result = self.client
            .query("SELECT username FROM config_users FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(user_id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(r.username)),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    // ── Group operations ───────────────────────────────────────────────────────

    pub async fn ensure_default_groups(&self) -> anyhow::Result<()> {
        // Upsert admins group
        let ver = Self::next_version();
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_groups (id, name, description, scopes, permissions, system, created_at, version, is_deleted) VALUES ('admins', 'admins', 'Full access administrators', '[\"all\"]', '[\"read\",\"write\",\"admin\"]', 1, ?, ?, 0)")
            .bind(&now)
            .bind(ver)
            .execute()
            .await?;

        let ver2 = Self::next_version();
        self.client
            .query("INSERT INTO config_groups (id, name, description, scopes, permissions, system, created_at, version, is_deleted) VALUES ('viewers', 'viewers', 'Read-only viewers', '[\"all\"]', '[\"read\"]', 1, ?, ?, 0)")
            .bind(&now)
            .bind(ver2)
            .execute()
            .await?;

        // Bind admins to all tenants
        let tenants = self.list_tenants().await?;
        for (tid, _, _, _, _) in &tenants {
            let ver3 = Self::next_version();
            self.client
                .query("INSERT INTO config_group_tenants (group_id, tenant_id, version, is_deleted) VALUES ('admins', ?, ?, 0)")
                .bind(tid)
                .bind(ver3)
                .execute()
                .await?;
        }

        // Assign default groups to users with no existing groups
        let users = self.list_users().await?;
        for (uid, _, _, _, _, _) in &users {
            #[derive(clickhouse::Row, serde::Deserialize)]
            struct Count { n: u64 }
            let count = self.client
                .query("SELECT count() AS n FROM config_user_groups FINAL WHERE user_id = ? AND is_deleted = 0")
                .bind(uid)
                .fetch_one::<Count>()
                .await
                .map(|r| r.n)
                .unwrap_or(0);
            if count == 0 {
                // Fetch role from users table
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct RoleRow { role: String }
                let role = self.client
                    .query("SELECT role FROM config_users FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
                    .bind(uid)
                    .fetch_one::<RoleRow>()
                    .await
                    .map(|r| r.role)
                    .unwrap_or_else(|_| "viewer".to_string());
                let group_id = if role == "admin" { "admins" } else { "viewers" };
                let ver4 = Self::next_version();
                self.client
                    .query("INSERT INTO config_user_groups (user_id, group_id, version, is_deleted) VALUES (?, ?, ?, 0)")
                    .bind(uid)
                    .bind(group_id)
                    .bind(ver4)
                    .execute()
                    .await?;
            }
        }
        tracing::info!("default groups ensured (admins, viewers)");
        Ok(())
    }

    pub async fn list_groups(&self) -> anyhow::Result<Vec<(String, String, String, String, String, bool, String, Vec<String>)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct GRow { id: String, name: String, description: String, scopes: String, permissions: String, system: u8, created_at: String }
        let groups = self.client
            .query("SELECT id, name, description, scopes, permissions, system, created_at FROM config_groups FINAL WHERE is_deleted = 0 ORDER BY created_at ASC")
            .fetch_all::<GRow>()
            .await?;

        let mut result = Vec::new();
        for g in groups {
            let tids = self.get_group_tenant_ids(&g.id).await?;
            result.push((g.id, g.name, g.description, g.scopes, g.permissions, g.system != 0, g.created_at, tids));
        }
        Ok(result)
    }

    async fn get_group_tenant_ids(&self, group_id: &str) -> anyhow::Result<Vec<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { tenant_id: String }
        let rows = self.client
            .query("SELECT tenant_id FROM config_group_tenants FINAL WHERE group_id = ? AND is_deleted = 0")
            .bind(group_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| r.tenant_id).collect())
    }

    pub async fn get_group(&self, id: &str) -> anyhow::Result<Option<(String, String, String, String, String, bool, String, Vec<String>)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct GRow { id: String, name: String, description: String, scopes: String, permissions: String, system: u8, created_at: String }
        let result = self.client
            .query("SELECT id, name, description, scopes, permissions, system, created_at FROM config_groups FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<GRow>()
            .await;
        match result {
            Ok(g) => {
                let tids = self.get_group_tenant_ids(&g.id).await?;
                Ok(Some((g.id, g.name, g.description, g.scopes, g.permissions, g.system != 0, g.created_at, tids)))
            }
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_group(&self, name: &str, description: &str, scopes: &str, permissions: &str) -> anyhow::Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_groups (id, name, description, scopes, permissions, system, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, 0, ?, ?, 0)")
            .bind(&id)
            .bind(name)
            .bind(description)
            .bind(scopes)
            .bind(permissions)
            .bind(&now)
            .bind(ver)
            .execute()
            .await?;
        Ok(id)
    }

    pub async fn update_group(&self, id: &str, description: &str, scopes: &str, permissions: &str) -> anyhow::Result<bool> {
        let existing = self.get_group(id).await?;
        let (_, name, _, _, _, system, created_at, _) = match existing {
            Some(g) => g,
            None => return Ok(false),
        };
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_groups (id, name, description, scopes, permissions, system, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id)
            .bind(&name)
            .bind(description)
            .bind(scopes)
            .bind(permissions)
            .bind(if system { 1u8 } else { 0u8 })
            .bind(&created_at)
            .bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn delete_group(&self, id: &str) -> anyhow::Result<Result<bool, String>> {
        let existing = self.get_group(id).await?;
        let (_, name, description, scopes, permissions, system, created_at, _) = match existing {
            Some(g) => g,
            None => return Ok(Ok(false)),
        };
        if system { return Ok(Err("cannot delete a system group".to_string())); }
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_groups (id, name, description, scopes, permissions, system, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, 0, ?, ?, 1)")
            .bind(id)
            .bind(&name)
            .bind(&description)
            .bind(&scopes)
            .bind(&permissions)
            .bind(&created_at)
            .bind(ver)
            .execute()
            .await?;
        Ok(Ok(true))
    }

    pub async fn set_group_tenants(&self, group_id: &str, tenant_ids: &[String]) -> anyhow::Result<()> {
        // Soft-delete existing bindings
        let existing_tids = self.get_group_tenant_ids(group_id).await?;
        for tid in &existing_tids {
            let ver = Self::next_version();
            self.client
                .query("INSERT INTO config_group_tenants (group_id, tenant_id, version, is_deleted) VALUES (?, ?, ?, 1)")
                .bind(group_id)
                .bind(tid)
                .bind(ver)
                .execute()
                .await?;
        }
        // Insert new bindings
        for tid in tenant_ids {
            let ver = Self::next_version();
            self.client
                .query("INSERT INTO config_group_tenants (group_id, tenant_id, version, is_deleted) VALUES (?, ?, ?, 0)")
                .bind(group_id)
                .bind(tid)
                .bind(ver)
                .execute()
                .await?;
        }
        Ok(())
    }

    pub async fn get_user_groups(&self, user_id: &str) -> anyhow::Result<Vec<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { group_id: String }
        let rows = self.client
            .query("SELECT group_id FROM config_user_groups FINAL WHERE user_id = ? AND is_deleted = 0")
            .bind(user_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| r.group_id).collect())
    }

    pub async fn set_user_groups(&self, user_id: &str, group_ids: &[String]) -> anyhow::Result<()> {
        let existing = self.get_user_groups(user_id).await?;
        for gid in &existing {
            let ver = Self::next_version();
            self.client
                .query("INSERT INTO config_user_groups (user_id, group_id, version, is_deleted) VALUES (?, ?, ?, 1)")
                .bind(user_id)
                .bind(gid)
                .bind(ver)
                .execute()
                .await?;
        }
        for gid in group_ids {
            let ver = Self::next_version();
            self.client
                .query("INSERT INTO config_user_groups (user_id, group_id, version, is_deleted) VALUES (?, ?, ?, 0)")
                .bind(user_id)
                .bind(gid)
                .bind(ver)
                .execute()
                .await?;
        }
        Ok(())
    }

    pub async fn resolve_user_permissions(&self, user_id: &str) -> anyhow::Result<(Vec<String>, Vec<String>, Vec<String>)> {
        let group_ids = self.get_user_groups(user_id).await?;
        let mut all_scopes = std::collections::HashSet::new();
        let mut all_permissions = std::collections::HashSet::new();
        let mut all_tenant_ids = std::collections::HashSet::new();

        for gid in &group_ids {
            #[derive(clickhouse::Row, serde::Deserialize)]
            struct GRow { scopes: String, permissions: String }
            if let Ok(g) = self.client
                .query("SELECT scopes, permissions FROM config_groups FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
                .bind(gid)
                .fetch_one::<GRow>()
                .await
            {
                if let Ok(s) = serde_json::from_str::<Vec<String>>(&g.scopes) { all_scopes.extend(s); }
                if let Ok(p) = serde_json::from_str::<Vec<String>>(&g.permissions) { all_permissions.extend(p); }
            }
            let tids = self.get_group_tenant_ids(gid).await?;
            all_tenant_ids.extend(tids);
        }

        if all_scopes.contains("all") {
            all_scopes = std::collections::HashSet::from(["all".to_string()]);
        }
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

    // ── SSO provider operations ────────────────────────────────────────────────

    async fn fetch_sso_provider_row(
        &self, sql: &str, bind_id: Option<&str>,
    ) -> anyhow::Result<Option<SsoProviderRow>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String, name: String, protocol: String, enabled: u8,
            client_id: String, client_secret: String, issuer_url: String,
            oidc_scopes: String, groups_claim: String, email_claim: String,
            first_name_claim: String, last_name_claim: String, jit_provisioning: u8,
            default_group_id: String, created_at: String,
            saml_idp_metadata_url: String, saml_idp_sso_url: String,
            saml_idp_cert: String, saml_sp_entity_id: String,
        }
        let result = match bind_id {
            Some(id) => self.client.query(sql).bind(id).fetch_one::<Row>().await,
            None => self.client.query(sql).fetch_one::<Row>().await,
        };
        match result {
            Ok(r) => Ok(Some((
                r.id, r.name, r.protocol, r.enabled != 0,
                r.client_id, r.client_secret, r.issuer_url, r.oidc_scopes,
                r.groups_claim, r.email_claim, r.first_name_claim, r.last_name_claim,
                r.jit_provisioning != 0, r.default_group_id, r.created_at,
                r.saml_idp_metadata_url, r.saml_idp_sso_url, r.saml_idp_cert, r.saml_sp_entity_id,
            ))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_sso_provider(&self, id: &str) -> anyhow::Result<Option<SsoProviderRow>> {
        self.fetch_sso_provider_row(
            "SELECT id, name, protocol, enabled, client_id, client_secret, issuer_url, oidc_scopes, groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, created_at, saml_idp_metadata_url, saml_idp_sso_url, saml_idp_cert, saml_sp_entity_id FROM config_sso_providers FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1",
            Some(id),
        ).await
    }

    pub async fn list_sso_providers(&self) -> anyhow::Result<Vec<SsoProviderRow>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String, name: String, protocol: String, enabled: u8,
            client_id: String, client_secret: String, issuer_url: String,
            oidc_scopes: String, groups_claim: String, email_claim: String,
            first_name_claim: String, last_name_claim: String, jit_provisioning: u8,
            default_group_id: String, created_at: String,
            saml_idp_metadata_url: String, saml_idp_sso_url: String,
            saml_idp_cert: String, saml_sp_entity_id: String,
        }
        let rows = self.client
            .query("SELECT id, name, protocol, enabled, client_id, client_secret, issuer_url, oidc_scopes, groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, created_at, saml_idp_metadata_url, saml_idp_sso_url, saml_idp_cert, saml_sp_entity_id FROM config_sso_providers FINAL WHERE is_deleted = 0 ORDER BY created_at ASC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| (
            r.id, r.name, r.protocol, r.enabled != 0,
            r.client_id, r.client_secret, r.issuer_url, r.oidc_scopes,
            r.groups_claim, r.email_claim, r.first_name_claim, r.last_name_claim,
            r.jit_provisioning != 0, r.default_group_id, r.created_at,
            r.saml_idp_metadata_url, r.saml_idp_sso_url, r.saml_idp_cert, r.saml_sp_entity_id,
        )).collect())
    }

    pub async fn get_enabled_sso_provider(&self) -> anyhow::Result<Option<SsoProviderRow>> {
        self.fetch_sso_provider_row(
            "SELECT id, name, protocol, enabled, client_id, client_secret, issuer_url, oidc_scopes, groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, created_at, saml_idp_metadata_url, saml_idp_sso_url, saml_idp_cert, saml_sp_entity_id FROM config_sso_providers FINAL WHERE enabled = 1 AND is_deleted = 0 LIMIT 1",
            None,
        ).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_sso_provider(
        &self,
        id: &str, name: &str, protocol: &str, enabled: bool,
        client_id: &str, client_secret: &str, issuer_url: &str,
        oidc_scopes: &str, groups_claim: &str, jit_provisioning: bool,
        default_group_id: &str, saml_idp_metadata_url: &str,
        saml_idp_sso_url: &str, saml_idp_cert: &str, saml_sp_entity_id: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_sso_providers (id, name, protocol, enabled, client_id, client_secret, issuer_url, oidc_scopes, groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, saml_idp_metadata_url, saml_idp_sso_url, saml_idp_cert, saml_sp_entity_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'email', 'given_name', 'family_name', ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(name).bind(protocol)
            .bind(if enabled { 1u8 } else { 0u8 })
            .bind(client_id).bind(client_secret).bind(issuer_url)
            .bind(oidc_scopes).bind(groups_claim)
            .bind(if jit_provisioning { 1u8 } else { 0u8 })
            .bind(default_group_id)
            .bind(saml_idp_metadata_url).bind(saml_idp_sso_url)
            .bind(saml_idp_cert).bind(saml_sp_entity_id)
            .bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn delete_sso_provider(&self, id: &str) -> anyhow::Result<bool> {
        let existing = self.get_sso_provider(id).await?;
        if existing.is_none() { return Ok(false); }
        let ver = Self::next_version();
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_sso_providers (id, name, protocol, enabled, client_id, client_secret, issuer_url, oidc_scopes, groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, saml_idp_metadata_url, saml_idp_sso_url, saml_idp_cert, saml_sp_entity_id, created_at, version, is_deleted) VALUES (?, '', '', 0, '', '', '', '', '', '', '', '', 0, '', '', '', '', '', ?, ?, 1)")
            .bind(id).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    // ── IdP group mapping operations ───────────────────────────────────────────

    pub async fn list_idp_group_mappings(&self, provider_id: Option<&str>) -> anyhow::Result<Vec<(String, String, String, String, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, idp_group: String, rush_group_id: String, provider_id: String, created_at: String }
        let rows = match provider_id {
            Some(pid) => self.client
                .query("SELECT id, idp_group, rush_group_id, provider_id, created_at FROM config_idp_group_mappings FINAL WHERE provider_id = ? AND is_deleted = 0 ORDER BY created_at ASC")
                .bind(pid)
                .fetch_all::<Row>()
                .await?,
            None => self.client
                .query("SELECT id, idp_group, rush_group_id, provider_id, created_at FROM config_idp_group_mappings FINAL WHERE is_deleted = 0 ORDER BY created_at ASC")
                .fetch_all::<Row>()
                .await?,
        };
        Ok(rows.into_iter().map(|r| (r.id, r.idp_group, r.rush_group_id, r.provider_id, r.created_at)).collect())
    }

    pub async fn create_idp_group_mapping(&self, idp_group: &str, rush_group_id: &str, provider_id: &str) -> anyhow::Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_idp_group_mappings (id, idp_group, rush_group_id, provider_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, 0)")
            .bind(&id).bind(idp_group).bind(rush_group_id).bind(provider_id).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(id)
    }

    pub async fn delete_idp_group_mapping(&self, id: &str) -> anyhow::Result<bool> {
        let mappings = self.list_idp_group_mappings(None).await?;
        let found = mappings.iter().find(|(mid, _, _, _, _)| mid == id);
        if found.is_none() { return Ok(false); }
        let ver = Self::next_version();
        let (_, idp_group, rush_group_id, provider_id, created_at) = found.unwrap().clone();
        self.client
            .query("INSERT INTO config_idp_group_mappings (id, idp_group, rush_group_id, provider_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&idp_group).bind(&rush_group_id).bind(&provider_id).bind(&created_at).bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn resolve_idp_groups(&self, idp_groups: &[String], provider_id: &str) -> anyhow::Result<Vec<String>> {
        let mut result = std::collections::HashSet::new();
        for idp_group in idp_groups {
            #[derive(clickhouse::Row, serde::Deserialize)]
            struct Row { rush_group_id: String }
            let rows = self.client
                .query("SELECT rush_group_id FROM config_idp_group_mappings FINAL WHERE idp_group = ? AND provider_id = ? AND is_deleted = 0")
                .bind(idp_group)
                .bind(provider_id)
                .fetch_all::<Row>()
                .await?;
            for r in rows { result.insert(r.rush_group_id); }
        }
        Ok(result.into_iter().collect())
    }

    // ── SSO user operations ────────────────────────────────────────────────────

    pub async fn find_user_by_external_id(&self, external_id: &str, auth_provider: &str) -> anyhow::Result<Option<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String }
        let result = self.client
            .query("SELECT id FROM config_users FINAL WHERE external_id = ? AND auth_provider = ? AND is_deleted = 0 LIMIT 1")
            .bind(external_id)
            .bind(auth_provider)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(r.id)),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_sso_user(&self, username: &str, display_name: &str, external_id: &str, auth_provider: &str, tenant_id: &str) -> anyhow::Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_users (id, username, password_hash, display_name, tenant_id, role, enabled, auth_provider, external_id, created_at, version, is_deleted) VALUES (?, ?, '!sso-no-password', ?, ?, 'viewer', 1, ?, ?, ?, ?, 0)")
            .bind(&id).bind(username).bind(display_name).bind(tenant_id)
            .bind(auth_provider).bind(external_id)
            .bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(id)
    }

    pub async fn update_user_groups_from_idp(&self, user_id: &str, mapped_group_ids: &[String]) -> anyhow::Result<()> {
        self.set_user_groups(user_id, mapped_group_ids).await
    }

    // ── SSO CSRF state operations ──────────────────────────────────────────────

    pub async fn store_sso_state(&self, state: &str) -> anyhow::Result<()> {
        self.client
            .query("INSERT INTO config_sso_state (state, created_at) VALUES (?, now())")
            .bind(state)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn validate_sso_state(&self, state: &str) -> anyhow::Result<bool> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { state: String }
        // ClickHouse TTL handles expiry; just check existence and delete
        let result = self.client
            .query("SELECT state FROM config_sso_state WHERE state = ? AND created_at > now() - INTERVAL 10 MINUTE LIMIT 1")
            .bind(state)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(_) => {
                // Delete the consumed state via lightweight delete
                let _ = self.client
                    .query("ALTER TABLE config_sso_state DELETE WHERE state = ?")
                    .bind(state)
                    .execute()
                    .await;
                Ok(true)
            }
            Err(clickhouse::error::Error::RowNotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    // ── API key operations ─────────────────────────────────────────────────────

    pub async fn list_api_keys(&self) -> anyhow::Result<Vec<(String, String, String, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, name: String, prefix: String, created_at: String }
        let rows = self.client
            .query("SELECT id, name, prefix, created_at FROM config_api_keys FINAL WHERE is_deleted = 0 ORDER BY created_at DESC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| (r.id, r.name, r.prefix, r.created_at)).collect())
    }

    pub async fn create_api_key(&self, id: &str, name: &str, key_hash: &str, prefix: &str) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_api_keys (id, name, key_hash, prefix, tenant_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, 'default', ?, ?, 0)")
            .bind(id).bind(name).bind(key_hash).bind(prefix).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn delete_api_key(&self, id: &str) -> anyhow::Result<bool> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { name: String, key_hash: String, prefix: String, tenant_id: String, created_at: String }
        let result = self.client
            .query("SELECT name, key_hash, prefix, tenant_id, created_at FROM config_api_keys FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<Row>()
            .await;
        let row = match result {
            Ok(r) => r,
            Err(clickhouse::error::Error::RowNotFound) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_api_keys (id, name, key_hash, prefix, tenant_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&row.name).bind(&row.key_hash).bind(&row.prefix)
            .bind(&row.tenant_id).bind(&row.created_at).bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    // ── Settings operations ────────────────────────────────────────────────────

    pub async fn get_setting(&self, key: &str) -> anyhow::Result<Option<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { value: String }
        let result = self.client
            .query("SELECT value FROM config_settings FINAL WHERE key = ? AND is_deleted = 0 LIMIT 1")
            .bind(key)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(r.value)),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn set_setting(&self, key: &str, value: &str) -> anyhow::Result<()> {
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_settings (key, value, version, is_deleted) VALUES (?, ?, ?, 0)")
            .bind(key).bind(value).bind(ver)
            .execute()
            .await?;
        Ok(())
    }

    // ── Setup token operations ─────────────────────────────────────────────────

    pub async fn create_setup_token(&self, purpose: &str, created_by: &str, provider: &str, hostname: &str) -> anyhow::Result<String> {
        let token: String = {
            use rand::Rng;
            let mut rng = rand::rng();
            let bytes: [u8; 16] = rng.random();
            bytes.iter().map(|b| format!("{b:02x}")).collect()
        };

        let expires_at = (chrono::Utc::now() + chrono::Duration::hours(48))
            .format("%Y-%m-%d %H:%M:%S").to_string();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_setup_tokens (token, purpose, created_by, expires_at, used, provider, hostname, version, is_deleted) VALUES (?, ?, ?, ?, 0, ?, ?, ?, 0)")
            .bind(&token).bind(purpose).bind(created_by).bind(&expires_at)
            .bind(provider).bind(hostname).bind(ver)
            .execute()
            .await?;
        Ok(token)
    }

    pub async fn validate_setup_token(&self, token: &str, purpose: &str) -> anyhow::Result<(bool, String)> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { provider: String }
        let now = Self::now_str();
        let result = self.client
            .query("SELECT provider FROM config_setup_tokens FINAL WHERE token = ? AND purpose = ? AND used = 0 AND expires_at > ? AND is_deleted = 0 LIMIT 1")
            .bind(token).bind(purpose).bind(&now)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok((true, r.provider)),
            Err(clickhouse::error::Error::RowNotFound) => Ok((false, String::new())),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn mark_setup_token_used(&self, token: &str) -> anyhow::Result<bool> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { purpose: String, created_by: String, expires_at: String, provider: String, hostname: String }
        let result = self.client
            .query("SELECT purpose, created_by, expires_at, provider, hostname FROM config_setup_tokens FINAL WHERE token = ? AND used = 0 AND is_deleted = 0 LIMIT 1")
            .bind(token)
            .fetch_one::<Row>()
            .await;
        let row = match result {
            Ok(r) => r,
            Err(clickhouse::error::Error::RowNotFound) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_setup_tokens (token, purpose, created_by, expires_at, used, provider, hostname, version, is_deleted) VALUES (?, ?, ?, ?, 1, ?, ?, ?, 0)")
            .bind(token).bind(&row.purpose).bind(&row.created_by).bind(&row.expires_at)
            .bind(&row.provider).bind(&row.hostname).bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    // ── Dashboard operations ───────────────────────────────────────────────────

    pub async fn list_dashboards(&self, tenant_id: &str, user_id: &str) -> anyhow::Result<Vec<crate::models::dashboard::Dashboard>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String, name: String, description: String, tenant_id: String,
            owner_id: String, visibility: String, tags: String,
            created_at: String, updated_at: String,
        }
        let rows = self.client
            .query("SELECT id, name, description, tenant_id, owner_id, visibility, tags, created_at, updated_at FROM config_dashboards FINAL WHERE is_deleted = 0 AND ((visibility = 'private' AND owner_id = ?) OR (visibility = 'tenant' AND tenant_id = ?) OR (visibility = 'global')) ORDER BY updated_at DESC")
            .bind(user_id).bind(tenant_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| crate::models::dashboard::Dashboard {
            id: r.id, name: r.name, description: r.description,
            tenant_id: r.tenant_id, owner_id: r.owner_id, visibility: r.visibility,
            tags: serde_json::from_str(&r.tags).unwrap_or(serde_json::json!([])),
            created_at: r.created_at, updated_at: r.updated_at,
        }).collect())
    }

    pub async fn get_dashboard(&self, id: &str, tenant_id: &str, user_id: &str) -> anyhow::Result<Option<crate::models::dashboard::Dashboard>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String, name: String, description: String, tenant_id: String,
            owner_id: String, visibility: String, tags: String,
            created_at: String, updated_at: String,
        }
        let result = self.client
            .query("SELECT id, name, description, tenant_id, owner_id, visibility, tags, created_at, updated_at FROM config_dashboards FINAL WHERE id = ? AND is_deleted = 0 AND ((visibility = 'private' AND owner_id = ?) OR (visibility = 'tenant' AND tenant_id = ?) OR (visibility = 'global')) LIMIT 1")
            .bind(id).bind(user_id).bind(tenant_id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(crate::models::dashboard::Dashboard {
                id: r.id, name: r.name, description: r.description,
                tenant_id: r.tenant_id, owner_id: r.owner_id, visibility: r.visibility,
                tags: serde_json::from_str(&r.tags).unwrap_or(serde_json::json!([])),
                created_at: r.created_at, updated_at: r.updated_at,
            })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_dashboard_unchecked(&self, id: &str) -> anyhow::Result<Option<crate::models::dashboard::Dashboard>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String, name: String, description: String, tenant_id: String,
            owner_id: String, visibility: String, tags: String,
            created_at: String, updated_at: String,
        }
        let result = self.client
            .query("SELECT id, name, description, tenant_id, owner_id, visibility, tags, created_at, updated_at FROM config_dashboards FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(crate::models::dashboard::Dashboard {
                id: r.id, name: r.name, description: r.description,
                tenant_id: r.tenant_id, owner_id: r.owner_id, visibility: r.visibility,
                tags: serde_json::from_str(&r.tags).unwrap_or(serde_json::json!([])),
                created_at: r.created_at, updated_at: r.updated_at,
            })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_dashboard(&self, id: &str, name: &str, description: &str, tenant_id: &str, owner_id: &str, visibility: &str, tags: &str) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_dashboards (id, name, description, tenant_id, owner_id, visibility, tags, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(name).bind(description).bind(tenant_id)
            .bind(owner_id).bind(visibility).bind(tags)
            .bind(&now).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn update_dashboard(&self, id: &str, name: &str, description: &str, visibility: &str, tags: &str, tenant_id: &str, user_id: &str, user_role: &str) -> anyhow::Result<bool> {
        let dash = match self.get_dashboard(id, tenant_id, user_id).await? {
            Some(d) => d,
            None => return Ok(false),
        };
        let can_edit = dash.owner_id == user_id
            || (dash.visibility == "tenant" && dash.tenant_id == tenant_id && (user_role == "admin" || user_role == "editor"))
            || (dash.visibility == "global" && user_role == "admin")
            || dash.owner_id.is_empty();
        if !can_edit { return Ok(false); }
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_dashboards (id, name, description, tenant_id, owner_id, visibility, tags, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(name).bind(description).bind(&dash.tenant_id)
            .bind(&dash.owner_id).bind(visibility).bind(tags)
            .bind(&dash.created_at).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn delete_dashboard(&self, id: &str, tenant_id: &str, user_id: &str, user_role: &str) -> anyhow::Result<bool> {
        let dash = match self.get_dashboard(id, tenant_id, user_id).await? {
            Some(d) => d,
            None => return Ok(false),
        };
        let can_delete = dash.owner_id == user_id || user_role == "admin" || dash.owner_id.is_empty();
        if !can_delete { return Ok(false); }
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_dashboards (id, name, description, tenant_id, owner_id, visibility, tags, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&dash.name).bind(&dash.description).bind(&dash.tenant_id)
            .bind(&dash.owner_id).bind(&dash.visibility)
            .bind(serde_json::to_string(&dash.tags).unwrap_or_else(|_| "[]".to_string()))
            .bind(&dash.created_at).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn export_dashboard(&self, id: &str, tenant_id: &str, user_id: &str) -> anyhow::Result<Option<serde_json::Value>> {
        let dash = match self.get_dashboard(id, tenant_id, user_id).await? {
            Some(d) => d,
            None => return Ok(None),
        };
        let widgets = self.list_widgets(id).await?;
        let widget_exports: Vec<serde_json::Value> = widgets.into_iter().map(|w| serde_json::json!({
            "title": w.title,
            "widget_type": w.widget_type,
            "query_config": serde_json::from_str::<serde_json::Value>(&w.query_config).unwrap_or_default(),
            "position": serde_json::from_str::<serde_json::Value>(&w.position).unwrap_or_default(),
            "display_config": serde_json::from_str::<serde_json::Value>(&w.display_config).unwrap_or_default(),
        })).collect();
        Ok(Some(serde_json::json!({
            "format_version": "v1",
            "exported_at": Self::now_str(),
            "dashboard": {"name": dash.name, "description": dash.description, "visibility": dash.visibility, "tags": dash.tags},
            "widgets": widget_exports,
        })))
    }

    pub async fn import_dashboard(&self, import: &crate::models::dashboard::ImportDashboardRequest, tenant_id: &str, owner_id: &str, user_role: &str) -> anyhow::Result<crate::models::dashboard::Dashboard> {
        if import.format_version != "v1" { anyhow::bail!("unsupported format_version: {}", import.format_version); }
        let visibility = if import.dashboard.visibility == "global" && user_role != "admin" { "tenant" } else { &import.dashboard.visibility };
        let tags_str = serde_json::to_string(&import.dashboard.tags)?;
        let dash_id = uuid::Uuid::new_v4().to_string();
        self.create_dashboard(&dash_id, &import.dashboard.name, &import.dashboard.description, tenant_id, owner_id, visibility, &tags_str).await?;
        for w in &import.widgets {
            let wid = uuid::Uuid::new_v4().to_string();
            self.create_widget(&wid, &dash_id, &w.title, &w.widget_type, &serde_json::to_string(&w.query_config)?, &serde_json::to_string(&w.position)?, &serde_json::to_string(&w.display_config)?).await?;
        }
        self.get_dashboard_unchecked(&dash_id).await?.ok_or_else(|| anyhow::anyhow!("failed to read imported dashboard"))
    }

    // ── Widget operations ──────────────────────────────────────────────────────

    pub async fn list_widgets(&self, dashboard_id: &str) -> anyhow::Result<Vec<crate::models::dashboard::Widget>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String, dashboard_id: String, title: String, widget_type: String,
            query_config: String, position: String, display_config: String,
            created_at: String, updated_at: String,
        }
        let rows = self.client
            .query("SELECT id, dashboard_id, title, widget_type, query_config, position, display_config, created_at, updated_at FROM config_widgets FINAL WHERE dashboard_id = ? AND is_deleted = 0 ORDER BY created_at ASC")
            .bind(dashboard_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| crate::models::dashboard::Widget {
            id: r.id, dashboard_id: r.dashboard_id, title: r.title, widget_type: r.widget_type,
            query_config: r.query_config, position: r.position, display_config: r.display_config,
            created_at: r.created_at, updated_at: r.updated_at,
        }).collect())
    }

    pub async fn create_widget(&self, id: &str, dashboard_id: &str, title: &str, widget_type: &str, query_config: &str, position: &str, display_config: &str) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_widgets (id, dashboard_id, title, widget_type, query_config, position, display_config, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(dashboard_id).bind(title).bind(widget_type)
            .bind(query_config).bind(position).bind(display_config)
            .bind(&now).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn update_widget(&self, id: &str, dashboard_id: &str, title: &str, widget_type: &str, query_config: &str, position: &str, display_config: &str) -> anyhow::Result<bool> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { created_at: String }
        let result = self.client
            .query("SELECT created_at FROM config_widgets FINAL WHERE id = ? AND dashboard_id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id).bind(dashboard_id)
            .fetch_one::<Row>()
            .await;
        let row = match result {
            Ok(r) => r,
            Err(clickhouse::error::Error::RowNotFound) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_widgets (id, dashboard_id, title, widget_type, query_config, position, display_config, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(dashboard_id).bind(title).bind(widget_type)
            .bind(query_config).bind(position).bind(display_config)
            .bind(&row.created_at).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn delete_widget(&self, id: &str, dashboard_id: &str) -> anyhow::Result<bool> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { title: String, widget_type: String, query_config: String, position: String, display_config: String, created_at: String }
        let result = self.client
            .query("SELECT title, widget_type, query_config, position, display_config, created_at FROM config_widgets FINAL WHERE id = ? AND dashboard_id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id).bind(dashboard_id)
            .fetch_one::<Row>()
            .await;
        let row = match result {
            Ok(r) => r,
            Err(clickhouse::error::Error::RowNotFound) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_widgets (id, dashboard_id, title, widget_type, query_config, position, display_config, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(dashboard_id).bind(&row.title).bind(&row.widget_type)
            .bind(&row.query_config).bind(&row.position).bind(&row.display_config)
            .bind(&row.created_at).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    // ── Dashboard template operations ─────────────────────────────────────────

    pub async fn list_dashboard_templates(&self) -> anyhow::Result<Vec<crate::models::dashboard::DashboardTemplate>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, name: String, description: String, category: String, is_builtin: u8, template_json: String, tags: String, created_at: String }
        let rows = self.client
            .query("SELECT id, name, description, category, is_builtin, template_json, tags, created_at FROM config_dashboard_templates FINAL WHERE is_deleted = 0 ORDER BY is_builtin DESC, name ASC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| crate::models::dashboard::DashboardTemplate {
            id: r.id, name: r.name, description: r.description, category: r.category,
            is_builtin: r.is_builtin != 0,
            template_json: serde_json::from_str(&r.template_json).unwrap_or_default(),
            tags: serde_json::from_str(&r.tags).unwrap_or(serde_json::json!([])),
            created_at: r.created_at,
        }).collect())
    }

    pub async fn get_dashboard_template(&self, id: &str) -> anyhow::Result<Option<crate::models::dashboard::DashboardTemplate>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, name: String, description: String, category: String, is_builtin: u8, template_json: String, tags: String, created_at: String }
        let result = self.client
            .query("SELECT id, name, description, category, is_builtin, template_json, tags, created_at FROM config_dashboard_templates FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(crate::models::dashboard::DashboardTemplate {
                id: r.id, name: r.name, description: r.description, category: r.category,
                is_builtin: r.is_builtin != 0,
                template_json: serde_json::from_str(&r.template_json).unwrap_or_default(),
                tags: serde_json::from_str(&r.tags).unwrap_or(serde_json::json!([])),
                created_at: r.created_at,
            })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn ensure_default_templates(&self) -> anyhow::Result<()> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Count { n: u64 }
        let count = self.client
            .query("SELECT count() AS n FROM config_dashboard_templates FINAL WHERE is_builtin = 1 AND is_deleted = 0")
            .fetch_one::<Count>()
            .await?.n;
        if count > 0 { return Ok(()); }

        fn w(title: &str, wt: &str, qc: serde_json::Value, pos: (i32,i32,i32,i32), dc: serde_json::Value) -> serde_json::Value {
            serde_json::json!({"title":title,"widget_type":wt,"query_config":qc,"position":{"col":pos.0,"row":pos.1,"col_span":pos.2,"row_span":pos.3},"display_config":dc})
        }
        fn qc_svc(agg: &str, interval: Option<&str>, extra: Vec<serde_json::Value>, group_by: Option<Vec<&str>>, limit: Option<i32>) -> serde_json::Value {
            let mut filters = vec![serde_json::json!({"field":"ServiceName","op":"=","value":"{{service}}"})];
            filters.extend(extra);
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
        fn color(c: &str) -> serde_json::Value { serde_json::json!({"color":c}) }
        fn empty() -> serde_json::Value { serde_json::json!({}) }
        let ef = || vec![serde_json::json!({"field":"StatusCode","op":">=","value":"500"})];

        let templates: Vec<(&str, &str, &str, &str, serde_json::Value)> = vec![
            ("tpl-service-overview","Service Overview","Golden signals for a single service: request rate, error rate, and latency percentiles.","apm",serde_json::json!({"widgets":[w("Request Rate","timeseries",qc_svc("count",Some("1m"),vec![],None,None),(0,0,6,4),color("#3b82f6")),w("Error Rate","timeseries",qc_svc("count",Some("1m"),ef(),None,None),(6,0,6,4),color("#ef4444")),w("P50 Latency","timeseries",qc_svc("p50",Some("1m"),vec![],None,None),(0,4,4,4),color("#22c55e")),w("P99 Latency","timeseries",qc_svc("p99",Some("1m"),vec![],None,None),(4,4,4,4),color("#f59e0b")),w("Top Endpoints","table",qc_svc("count",None,vec![],Some(vec!["SpanName"]),Some(10)),(8,4,4,4),empty())]})),
            ("tpl-error-analysis","Error Analysis","Error count by service, top error messages, and error rate timeline.","apm",serde_json::json!({"widgets":[w("Error Count","counter",qc("count",None,ef(),None,None),(0,0,3,3),color("#ef4444")),w("Error Rate Over Time","timeseries",qc("count",Some("5m"),ef(),None,None),(3,0,9,3),color("#ef4444")),w("Errors by Service","bar",qc("count",None,ef(),Some(vec!["ServiceName"]),Some(10)),(0,3,6,4),empty()),w("Top Error Messages","table",qc("count",None,ef(),Some(vec!["StatusMessage"]),Some(20)),(6,3,6,4),empty())]})),
            ("tpl-latency-deep-dive","Latency Deep-Dive","P50/P99/P999 latency, latency by endpoint, and slow traces.","apm",serde_json::json!({"widgets":[w("P50 / P99 Latency","timeseries",qc_svc("p50",Some("1m"),vec![],None,None),(0,0,12,4),color("#8b5cf6")),w("Latency by Endpoint","bar",qc_svc("p99",None,vec![],Some(vec!["SpanName"]),Some(10)),(0,4,6,4),empty()),w("Slowest Traces","table",qc_svc("max",None,vec![],None,Some(20)),(6,4,6,4),empty())]})),
            ("tpl-infra-overview","Infrastructure Overview","CPU, memory, pod count, and restart count for infrastructure monitoring.","infrastructure",serde_json::json!({"widgets":[w("Pod Count","counter",qc("count",None,vec![],None,None),(0,0,3,3),color("#06b6d4")),w("CPU Utilization","timeseries",qc("avg",Some("1m"),vec![],None,None),(3,0,9,3),color("#3b82f6")),w("Memory Usage","timeseries",qc("avg",Some("1m"),vec![],None,None),(0,3,6,4),color("#22c55e")),w("Disk I/O","timeseries",qc("avg",Some("1m"),vec![],None,None),(6,3,6,4),color("#f59e0b"))]})),
            ("tpl-log-volume","Log Volume","Log count by severity, by service, and timeline for understanding ingestion patterns.","security",serde_json::json!({"widgets":[w("Error/Fatal Count","counter",qc("count",None,vec![serde_json::json!({"field":"SeverityText","op":"in","value":"ERROR,FATAL"})],None,None),(0,0,3,3),color("#ef4444")),w("Log Volume Over Time","timeseries",qc("count",Some("5m"),vec![],None,None),(3,0,9,3),color("#6366f1")),w("Logs by Severity","bar",qc("count",None,vec![],Some(vec!["SeverityText"]),Some(10)),(0,3,6,4),empty()),w("Top Services by Log Count","table",qc("count",None,vec![],Some(vec!["ServiceName"]),Some(20)),(6,3,6,4),empty())]})),
        ];

        for (id, name, desc, category, json_val) in &templates {
            let json_str = serde_json::to_string(json_val)?;
            let now = Self::now_str();
            let ver = Self::next_version();
            self.client
                .query("INSERT INTO config_dashboard_templates (id, name, description, category, is_builtin, template_json, tags, created_at, version, is_deleted) VALUES (?, ?, ?, ?, 1, ?, '[]', ?, ?, 0)")
                .bind(*id).bind(*name).bind(*desc).bind(*category)
                .bind(&json_str).bind(&now).bind(ver)
                .execute()
                .await?;
        }
        Ok(())
    }

    // ── Notification channel operations ───────────────────────────────────────

    pub async fn list_channels(&self, tenant_id: &str) -> anyhow::Result<Vec<crate::models::alert::NotificationChannel>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, tenant_id: String, name: String, channel_type: String, config: String, enabled: u8, created_at: String }
        let rows = self.client
            .query("SELECT id, tenant_id, name, channel_type, config, enabled, created_at FROM config_notification_channels FINAL WHERE tenant_id = ? AND is_deleted = 0 ORDER BY created_at DESC")
            .bind(tenant_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| crate::models::alert::NotificationChannel { id: r.id, tenant_id: r.tenant_id, name: r.name, channel_type: r.channel_type, config: r.config, enabled: r.enabled != 0, created_at: r.created_at }).collect())
    }

    pub async fn get_channel(&self, id: &str, tenant_id: &str) -> anyhow::Result<Option<crate::models::alert::NotificationChannel>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, tenant_id: String, name: String, channel_type: String, config: String, enabled: u8, created_at: String }
        let result = self.client
            .query("SELECT id, tenant_id, name, channel_type, config, enabled, created_at FROM config_notification_channels FINAL WHERE id = ? AND tenant_id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id).bind(tenant_id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(crate::models::alert::NotificationChannel { id: r.id, tenant_id: r.tenant_id, name: r.name, channel_type: r.channel_type, config: r.config, enabled: r.enabled != 0, created_at: r.created_at })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_channel_by_id(&self, id: &str) -> anyhow::Result<Option<crate::models::alert::NotificationChannel>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, tenant_id: String, name: String, channel_type: String, config: String, enabled: u8, created_at: String }
        let result = self.client
            .query("SELECT id, tenant_id, name, channel_type, config, enabled, created_at FROM config_notification_channels FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(crate::models::alert::NotificationChannel { id: r.id, tenant_id: r.tenant_id, name: r.name, channel_type: r.channel_type, config: r.config, enabled: r.enabled != 0, created_at: r.created_at })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_channel(&self, id: &str, tenant_id: &str, name: &str, channel_type: &str, config: &str) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_notification_channels (id, tenant_id, name, channel_type, config, enabled, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, 1, ?, ?, 0)")
            .bind(id).bind(tenant_id).bind(name).bind(channel_type).bind(config).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn update_channel(&self, id: &str, tenant_id: &str, name: &str, config: &str, enabled: bool) -> anyhow::Result<bool> {
        let existing = self.get_channel(id, tenant_id).await?;
        let row = match existing { Some(r) => r, None => return Ok(false) };
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_notification_channels (id, tenant_id, name, channel_type, config, enabled, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(tenant_id).bind(name).bind(&row.channel_type).bind(config)
            .bind(if enabled { 1u8 } else { 0u8 }).bind(&row.created_at).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn delete_channel(&self, id: &str, tenant_id: &str) -> anyhow::Result<bool> {
        let existing = self.get_channel(id, tenant_id).await?;
        let row = match existing { Some(r) => r, None => return Ok(false) };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_notification_channels (id, tenant_id, name, channel_type, config, enabled, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(tenant_id).bind(&row.name).bind(&row.channel_type).bind(&row.config)
            .bind(if row.enabled { 1u8 } else { 0u8 }).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    // ── Notification log operations ────────────────────────────────────────────

    pub async fn create_notification_log(&self, channel_id: &str, tenant_id: &str, alert_type: &str, alert_name: &str, severity: &str, status: &str, error: &str) -> anyhow::Result<()> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_notification_log (id, channel_id, tenant_id, alert_type, alert_name, severity, status, error, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&id).bind(channel_id).bind(tenant_id).bind(alert_type).bind(alert_name).bind(severity).bind(status).bind(error).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_notification_log(&self, tenant_id: &str, limit: i64) -> anyhow::Result<Vec<crate::models::alert::NotificationLogEntry>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, channel_id: String, tenant_id: String, alert_type: String, alert_name: String, severity: String, status: String, error: String, created_at: String }
        let rows = self.client
            .query("SELECT id, channel_id, tenant_id, alert_type, alert_name, severity, status, error, created_at FROM config_notification_log WHERE tenant_id = ? ORDER BY created_at DESC LIMIT ?")
            .bind(tenant_id).bind(limit as u64)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| crate::models::alert::NotificationLogEntry { id: r.id, channel_id: r.channel_id, tenant_id: r.tenant_id, alert_type: r.alert_type, alert_name: r.alert_name, severity: r.severity, status: r.status, error: r.error, created_at: r.created_at }).collect())
    }

    // ── Alert rule operations ──────────────────────────────────────────────────

    fn map_alert_row(r: AlertRuleRow) -> crate::models::alert::AlertRule {
        crate::models::alert::AlertRule {
            id: r.id, name: r.name, description: r.description,
            enabled: r.enabled != 0, signal_type: r.signal_type,
            query_config: r.query_config, condition_op: r.condition_op,
            condition_threshold: r.condition_threshold,
            eval_interval_secs: r.eval_interval_secs,
            notification_channel_ids: r.notification_channel_ids,
            runbook_url: r.runbook_url,
            state: r.state,
            last_eval_at: if r.last_eval_at.is_empty() { None } else { Some(r.last_eval_at) },
            last_triggered_at: if r.last_triggered_at.is_empty() { None } else { Some(r.last_triggered_at) },
            created_at: r.created_at, updated_at: r.updated_at,
        }
    }

    pub async fn list_alerts(&self) -> anyhow::Result<Vec<crate::models::alert::AlertRule>> {
        let rows = self.client
            .query("SELECT id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids, runbook_url, state, last_eval_at, last_triggered_at, created_at, updated_at FROM config_alert_rules FINAL WHERE is_deleted = 0 ORDER BY created_at DESC")
            .fetch_all::<AlertRuleRow>()
            .await?;
        Ok(rows.into_iter().map(Self::map_alert_row).collect())
    }

    pub async fn get_alert(&self, id: &str) -> anyhow::Result<Option<crate::models::alert::AlertRule>> {
        let result = self.client
            .query("SELECT id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids, runbook_url, state, last_eval_at, last_triggered_at, created_at, updated_at FROM config_alert_rules FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<AlertRuleRow>()
            .await;
        match result {
            Ok(r) => Ok(Some(Self::map_alert_row(r))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_alert(&self, id: &str, name: &str, description: &str, enabled: bool, signal_type: &str, query_config: &str, condition_op: &str, condition_threshold: f64, eval_interval_secs: i64, notification_channel_ids: &str, runbook_url: &str) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_alert_rules (id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids, runbook_url, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ok', '', '', ?, ?, ?, 0)")
            .bind(id).bind(name).bind(description).bind(if enabled { 1u8 } else { 0u8 })
            .bind(signal_type).bind(query_config).bind(condition_op)
            .bind(condition_threshold).bind(eval_interval_secs).bind(notification_channel_ids)
            .bind(runbook_url).bind(&now).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn update_alert(&self, id: &str, name: &str, description: &str, enabled: bool, signal_type: &str, query_config: &str, condition_op: &str, condition_threshold: f64, eval_interval_secs: i64, notification_channel_ids: &str, runbook_url: &str) -> anyhow::Result<bool> {
        let existing = match self.get_alert(id).await? { Some(r) => r, None => return Ok(false) };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_alert_rules (id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids, runbook_url, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(name).bind(description).bind(if enabled { 1u8 } else { 0u8 })
            .bind(signal_type).bind(query_config).bind(condition_op)
            .bind(condition_threshold).bind(eval_interval_secs).bind(notification_channel_ids)
            .bind(runbook_url).bind(&existing.state)
            .bind(existing.last_eval_at.unwrap_or_default())
            .bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn delete_alert(&self, id: &str) -> anyhow::Result<bool> {
        let existing = match self.get_alert(id).await? { Some(r) => r, None => return Ok(false) };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_alert_rules (id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids, runbook_url, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&existing.name).bind(&existing.description)
            .bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.signal_type).bind(&existing.query_config).bind(&existing.condition_op)
            .bind(existing.condition_threshold).bind(existing.eval_interval_secs)
            .bind(&existing.notification_channel_ids).bind(&existing.runbook_url).bind(&existing.state)
            .bind(existing.last_eval_at.unwrap_or_default())
            .bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn update_alert_state(&self, id: &str, state: &str, last_eval_at: &str, last_triggered_at: Option<&str>) -> anyhow::Result<()> {
        let existing = match self.get_alert(id).await? { Some(r) => r, None => return Ok(()) };
        let now = Self::now_str();
        let ver = Self::next_version();
        let lta = last_triggered_at.map(|s| s.to_string()).unwrap_or_else(|| existing.last_triggered_at.clone().unwrap_or_default());
        self.client
            .query("INSERT INTO config_alert_rules (id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids, runbook_url, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.name).bind(&existing.description)
            .bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.signal_type).bind(&existing.query_config).bind(&existing.condition_op)
            .bind(existing.condition_threshold).bind(existing.eval_interval_secs)
            .bind(&existing.notification_channel_ids).bind(&existing.runbook_url)
            .bind(state).bind(last_eval_at).bind(&lta)
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn get_due_alerts(&self, now: &str) -> anyhow::Result<Vec<crate::models::alert::AlertRule>> {
        let rows = self.client
            .query("SELECT id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids, runbook_url, state, last_eval_at, last_triggered_at, created_at, updated_at FROM config_alert_rules FINAL WHERE enabled = 1 AND is_deleted = 0 AND (last_eval_at = '' OR toUnixTimestamp(parseDateTimeBestEffort(?)) - toUnixTimestamp(parseDateTimeBestEffort(last_eval_at)) >= eval_interval_secs)")
            .bind(now)
            .fetch_all::<AlertRuleRow>()
            .await?;
        Ok(rows.into_iter().map(Self::map_alert_row).collect())
    }

    // ── Alert event operations ─────────────────────────────────────────────────

    pub async fn create_alert_event(&self, id: &str, rule_id: &str, state: &str, value: f64, threshold: f64, message: &str) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_alert_events (id, rule_id, state, value, threshold, message, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(id).bind(rule_id).bind(state).bind(value).bind(threshold).bind(message).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_alert_events(&self, rule_id: &str, limit: i64) -> anyhow::Result<Vec<crate::models::alert::AlertEvent>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, rule_id: String, state: String, value: f64, threshold: f64, message: String, created_at: String }
        let rows = self.client
            .query("SELECT id, rule_id, state, value, threshold, message, created_at FROM config_alert_events WHERE rule_id = ? ORDER BY created_at DESC LIMIT ?")
            .bind(rule_id).bind(limit as u64)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| crate::models::alert::AlertEvent { id: r.id, rule_id: r.rule_id, state: r.state, value: r.value, threshold: r.threshold, message: r.message, created_at: r.created_at }).collect())
    }

    pub async fn list_all_alert_events(&self, limit: i64) -> anyhow::Result<Vec<crate::models::alert::AlertEventWithRule>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, rule_id: String, rule_name: String, state: String, value: f64, threshold: f64, message: String, created_at: String }
        let rows = self.client
            .query("SELECT e.id, e.rule_id, coalesce(r.name, 'deleted rule') AS rule_name, e.state, e.value, e.threshold, e.message, e.created_at FROM config_alert_events e LEFT JOIN (SELECT id, name FROM config_alert_rules FINAL WHERE is_deleted = 0) r ON e.rule_id = r.id ORDER BY e.created_at DESC LIMIT ?")
            .bind(limit as u64)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| crate::models::alert::AlertEventWithRule { id: r.id, rule_id: r.rule_id, rule_name: r.rule_name, state: r.state, value: r.value, threshold: r.threshold, message: r.message, created_at: r.created_at }).collect())
    }

    // ── Deploy marker operations ───────────────────────────────────────────────

    pub async fn create_deploy_marker(&self, id: &str, service_name: &str, version: &str, commit_sha: &str, description: &str, environment: &str, deployed_by: &str) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_deploy_markers (id, service_name, version, commit_sha, description, environment, deployed_by, deployed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id).bind(service_name).bind(version).bind(commit_sha).bind(description).bind(environment).bind(deployed_by).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_deploy_markers(&self, service_name: Option<&str>, from: Option<&str>, to: Option<&str>) -> anyhow::Result<Vec<crate::models::deploy::DeployMarker>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, service_name: String, version: String, commit_sha: String, description: String, environment: String, deployed_by: String, deployed_at: String }
        // Build query dynamically; ClickHouse doesn't support optional parameters so we build different SQL
        let sql = {
            let mut s = "SELECT id, service_name, version, commit_sha, description, environment, deployed_by, deployed_at FROM config_deploy_markers WHERE 1=1".to_string();
            if service_name.is_some() { s.push_str(" AND service_name = ?"); }
            if from.is_some() { s.push_str(" AND deployed_at >= ?"); }
            if to.is_some() { s.push_str(" AND deployed_at <= ?"); }
            s.push_str(" ORDER BY deployed_at DESC LIMIT 100");
            s
        };
        let mut q = self.client.query(&sql);
        if let Some(sn) = service_name { q = q.bind(sn); }
        if let Some(f) = from { q = q.bind(f); }
        if let Some(t) = to { q = q.bind(t); }
        let rows = q.fetch_all::<Row>().await?;
        Ok(rows.into_iter().map(|r| crate::models::deploy::DeployMarker { id: r.id, service_name: r.service_name, version: r.version, commit_sha: r.commit_sha, description: r.description, environment: r.environment, deployed_by: r.deployed_by, deployed_at: r.deployed_at }).collect())
    }

    // ── SLO operations ─────────────────────────────────────────────────────────

    fn map_slo_row(r: SloRow) -> crate::models::slo::Slo {
        crate::models::slo::Slo {
            id: r.id, name: r.name, description: r.description, enabled: r.enabled != 0,
            slo_type: r.slo_type, indicator_type: r.indicator_type,
            service_name: r.service_name, metric_name: r.metric_name,
            window_type: r.window_type, target_percentage: r.target_percentage,
            threshold_ms: r.threshold_ms, threshold_value: r.threshold_value,
            threshold_op: if r.threshold_op.is_empty() { None } else { Some(r.threshold_op) },
            error_filters: r.error_filters, total_filters: r.total_filters,
            eval_interval_secs: r.eval_interval_secs,
            notification_channel_ids: r.notification_channel_ids,
            state: r.state, error_budget_remaining: r.error_budget_remaining,
            error_count: r.error_count, total_count: r.total_count,
            last_eval_at: if r.last_eval_at.is_empty() { None } else { Some(r.last_eval_at) },
            last_breached_at: if r.last_breached_at.is_empty() { None } else { Some(r.last_breached_at) },
            created_at: r.created_at, updated_at: r.updated_at,
        }
    }

    pub async fn list_slos(&self) -> anyhow::Result<Vec<crate::models::slo::Slo>> {
        let rows = self.client
            .query("SELECT id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, window_type, target_percentage, threshold_ms, threshold_value, threshold_op, error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, created_at, updated_at FROM config_slos FINAL WHERE is_deleted = 0 ORDER BY created_at DESC")
            .fetch_all::<SloRow>()
            .await?;
        Ok(rows.into_iter().map(Self::map_slo_row).collect())
    }

    pub async fn get_slo(&self, id: &str) -> anyhow::Result<Option<crate::models::slo::Slo>> {
        let result = self.client
            .query("SELECT id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, window_type, target_percentage, threshold_ms, threshold_value, threshold_op, error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, created_at, updated_at FROM config_slos FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<SloRow>()
            .await;
        match result {
            Ok(r) => Ok(Some(Self::map_slo_row(r))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_slo(&self, id: &str, name: &str, description: &str, enabled: bool, slo_type: &str, indicator_type: &str, service_name: &str, metric_name: &str, window_type: &str, target_percentage: f64, threshold_ms: Option<f64>, threshold_value: Option<f64>, threshold_op: Option<&str>, error_filters: &str, total_filters: &str, eval_interval_secs: i64, notification_channel_ids: &str) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_slos (id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, window_type, target_percentage, threshold_ms, threshold_value, threshold_op, error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'compliant', NULL, NULL, NULL, '', '', ?, ?, ?, 0)")
            .bind(id).bind(name).bind(description).bind(if enabled { 1u8 } else { 0u8 })
            .bind(slo_type).bind(indicator_type).bind(service_name).bind(metric_name)
            .bind(window_type).bind(target_percentage).bind(threshold_ms).bind(threshold_value)
            .bind(threshold_op.unwrap_or("")).bind(error_filters).bind(total_filters)
            .bind(eval_interval_secs).bind(notification_channel_ids)
            .bind(&now).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_slo(&self, id: &str, name: &str, description: &str, enabled: bool, slo_type: &str, indicator_type: &str, service_name: &str, metric_name: &str, window_type: &str, target_percentage: f64, threshold_ms: Option<f64>, threshold_value: Option<f64>, threshold_op: Option<&str>, error_filters: &str, total_filters: &str, eval_interval_secs: i64, notification_channel_ids: &str) -> anyhow::Result<bool> {
        let existing = match self.get_slo(id).await? { Some(r) => r, None => return Ok(false) };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_slos (id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, window_type, target_percentage, threshold_ms, threshold_value, threshold_op, error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(name).bind(description).bind(if enabled { 1u8 } else { 0u8 })
            .bind(slo_type).bind(indicator_type).bind(service_name).bind(metric_name)
            .bind(window_type).bind(target_percentage).bind(threshold_ms).bind(threshold_value)
            .bind(threshold_op.unwrap_or("")).bind(error_filters).bind(total_filters)
            .bind(eval_interval_secs).bind(notification_channel_ids)
            .bind(&existing.state).bind(existing.error_budget_remaining).bind(existing.error_count).bind(existing.total_count)
            .bind(existing.last_eval_at.unwrap_or_default()).bind(existing.last_breached_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn delete_slo(&self, id: &str) -> anyhow::Result<bool> {
        let existing = match self.get_slo(id).await? { Some(r) => r, None => return Ok(false) };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_slos (id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, window_type, target_percentage, threshold_ms, threshold_value, threshold_op, error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&existing.name).bind(&existing.description).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.slo_type).bind(&existing.indicator_type).bind(&existing.service_name).bind(&existing.metric_name)
            .bind(&existing.window_type).bind(existing.target_percentage).bind(existing.threshold_ms).bind(existing.threshold_value)
            .bind(existing.threshold_op.unwrap_or_default()).bind(&existing.error_filters).bind(&existing.total_filters)
            .bind(existing.eval_interval_secs).bind(&existing.notification_channel_ids)
            .bind(&existing.state).bind(existing.error_budget_remaining).bind(existing.error_count).bind(existing.total_count)
            .bind(existing.last_eval_at.unwrap_or_default()).bind(existing.last_breached_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn get_due_slos(&self, now: &str) -> anyhow::Result<Vec<crate::models::slo::Slo>> {
        let rows = self.client
            .query("SELECT id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, window_type, target_percentage, threshold_ms, threshold_value, threshold_op, error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, created_at, updated_at FROM config_slos FINAL WHERE enabled = 1 AND is_deleted = 0 AND (last_eval_at = '' OR toUnixTimestamp(parseDateTimeBestEffort(?)) - toUnixTimestamp(parseDateTimeBestEffort(last_eval_at)) >= eval_interval_secs)")
            .bind(now)
            .fetch_all::<SloRow>()
            .await?;
        Ok(rows.into_iter().map(Self::map_slo_row).collect())
    }

    pub async fn update_slo_state(&self, id: &str, state: &str, error_budget_remaining: f64, error_count: i64, total_count: i64, last_eval_at: &str, last_breached_at: Option<&str>) -> anyhow::Result<()> {
        let existing = match self.get_slo(id).await? { Some(r) => r, None => return Ok(()) };
        let now = Self::now_str();
        let ver = Self::next_version();
        let lba = last_breached_at.map(|s| s.to_string()).unwrap_or_else(|| existing.last_breached_at.clone().unwrap_or_default());
        self.client
            .query("INSERT INTO config_slos (id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, window_type, target_percentage, threshold_ms, threshold_value, threshold_op, error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.name).bind(&existing.description).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.slo_type).bind(&existing.indicator_type).bind(&existing.service_name).bind(&existing.metric_name)
            .bind(&existing.window_type).bind(existing.target_percentage).bind(existing.threshold_ms).bind(existing.threshold_value)
            .bind(existing.threshold_op.unwrap_or_default()).bind(&existing.error_filters).bind(&existing.total_filters)
            .bind(existing.eval_interval_secs).bind(&existing.notification_channel_ids)
            .bind(state).bind(error_budget_remaining).bind(error_count).bind(total_count)
            .bind(last_eval_at).bind(&lba)
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn create_slo_event(&self, id: &str, slo_id: &str, state: &str, error_count: i64, total_count: i64, error_budget_remaining: f64, message: &str) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_slo_events (id, slo_id, state, error_count, total_count, error_budget_remaining, message, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id).bind(slo_id).bind(state).bind(error_count).bind(total_count).bind(error_budget_remaining).bind(message).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_slo_events(&self, slo_id: &str, limit: i64) -> anyhow::Result<Vec<crate::models::slo::SloEvent>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, slo_id: String, state: String, error_count: i64, total_count: i64, error_budget_remaining: f64, message: String, created_at: String }
        let rows = self.client
            .query("SELECT id, slo_id, state, error_count, total_count, error_budget_remaining, message, created_at FROM config_slo_events WHERE slo_id = ? ORDER BY created_at DESC LIMIT ?")
            .bind(slo_id).bind(limit as u64)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| crate::models::slo::SloEvent { id: r.id, slo_id: r.slo_id, state: r.state, error_count: r.error_count, total_count: r.total_count, error_budget_remaining: r.error_budget_remaining, message: r.message, created_at: r.created_at }).collect())
    }

    // ── Anomaly rule operations ────────────────────────────────────────────────

    fn map_anomaly_rule(r: AnomalyRuleRow) -> crate::models::anomaly::AnomalyRule {
        crate::models::anomaly::AnomalyRule {
            id: r.id, name: r.name, description: r.description, enabled: r.enabled != 0,
            source: r.source, pattern: r.pattern, query: r.query,
            service_name: r.service_name, apm_metric: r.apm_metric,
            sensitivity: r.sensitivity, alpha: r.alpha,
            eval_interval_secs: r.eval_interval_secs, window_secs: r.window_secs,
            split_labels: r.split_labels, notification_channel_ids: r.notification_channel_ids,
            state: r.state,
            last_eval_at: if r.last_eval_at.is_empty() { None } else { Some(r.last_eval_at) },
            last_triggered_at: if r.last_triggered_at.is_empty() { None } else { Some(r.last_triggered_at) },
            created_at: r.created_at, updated_at: r.updated_at,
        }
    }

    pub async fn list_anomaly_rules(&self) -> anyhow::Result<Vec<crate::models::anomaly::AnomalyRule>> {
        let rows = self.client
            .query("SELECT id, name, description, enabled, source, pattern, query, service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, created_at, updated_at FROM config_anomaly_rules FINAL WHERE is_deleted = 0 ORDER BY created_at DESC")
            .fetch_all::<AnomalyRuleRow>()
            .await?;
        Ok(rows.into_iter().map(Self::map_anomaly_rule).collect())
    }

    pub async fn get_anomaly_rule(&self, id: &str) -> anyhow::Result<Option<crate::models::anomaly::AnomalyRule>> {
        let result = self.client
            .query("SELECT id, name, description, enabled, source, pattern, query, service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, created_at, updated_at FROM config_anomaly_rules FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<AnomalyRuleRow>()
            .await;
        match result {
            Ok(r) => Ok(Some(Self::map_anomaly_rule(r))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_anomaly_rule(&self, id: &str, name: &str, description: &str, enabled: bool, source: &str, pattern: &str, query: &str, service_name: &str, apm_metric: &str, sensitivity: f64, alpha: f64, eval_interval_secs: i64, window_secs: i64, split_labels: &str, notification_channel_ids: &str) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_anomaly_rules (id, name, description, enabled, source, pattern, query, service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'normal', '', '', ?, ?, ?, 0)")
            .bind(id).bind(name).bind(description).bind(if enabled { 1u8 } else { 0u8 })
            .bind(source).bind(pattern).bind(query).bind(service_name).bind(apm_metric)
            .bind(sensitivity).bind(alpha).bind(eval_interval_secs).bind(window_secs)
            .bind(split_labels).bind(notification_channel_ids)
            .bind(&now).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_anomaly_rule(&self, id: &str, name: &str, description: &str, enabled: bool, source: &str, pattern: &str, query: &str, service_name: &str, apm_metric: &str, sensitivity: f64, alpha: f64, eval_interval_secs: i64, window_secs: i64, split_labels: &str, notification_channel_ids: &str) -> anyhow::Result<bool> {
        let existing = match self.get_anomaly_rule(id).await? { Some(r) => r, None => return Ok(false) };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_anomaly_rules (id, name, description, enabled, source, pattern, query, service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(name).bind(description).bind(if enabled { 1u8 } else { 0u8 })
            .bind(source).bind(pattern).bind(query).bind(service_name).bind(apm_metric)
            .bind(sensitivity).bind(alpha).bind(eval_interval_secs).bind(window_secs)
            .bind(split_labels).bind(notification_channel_ids).bind(&existing.state)
            .bind(existing.last_eval_at.unwrap_or_default()).bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn delete_anomaly_rule(&self, id: &str) -> anyhow::Result<bool> {
        let existing = match self.get_anomaly_rule(id).await? { Some(r) => r, None => return Ok(false) };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_anomaly_rules (id, name, description, enabled, source, pattern, query, service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&existing.name).bind(&existing.description).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.source).bind(&existing.pattern).bind(&existing.query)
            .bind(&existing.service_name).bind(&existing.apm_metric)
            .bind(existing.sensitivity).bind(existing.alpha)
            .bind(existing.eval_interval_secs).bind(existing.window_secs)
            .bind(&existing.split_labels).bind(&existing.notification_channel_ids)
            .bind(&existing.state).bind(existing.last_eval_at.unwrap_or_default())
            .bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn get_due_anomaly_rules(&self, now: &str) -> anyhow::Result<Vec<crate::models::anomaly::AnomalyRule>> {
        let rows = self.client
            .query("SELECT id, name, description, enabled, source, pattern, query, service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, created_at, updated_at FROM config_anomaly_rules FINAL WHERE enabled = 1 AND is_deleted = 0 AND (last_eval_at = '' OR toUnixTimestamp(parseDateTimeBestEffort(?)) - toUnixTimestamp(parseDateTimeBestEffort(last_eval_at)) >= eval_interval_secs)")
            .bind(now)
            .fetch_all::<AnomalyRuleRow>()
            .await?;
        Ok(rows.into_iter().map(Self::map_anomaly_rule).collect())
    }

    pub async fn update_anomaly_state(&self, id: &str, state: &str, last_eval_at: &str, last_triggered_at: Option<&str>) -> anyhow::Result<()> {
        let existing = match self.get_anomaly_rule(id).await? { Some(r) => r, None => return Ok(()) };
        let now = Self::now_str();
        let ver = Self::next_version();
        let lta = last_triggered_at.map(|s| s.to_string()).unwrap_or_else(|| existing.last_triggered_at.clone().unwrap_or_default());
        self.client
            .query("INSERT INTO config_anomaly_rules (id, name, description, enabled, source, pattern, query, service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.name).bind(&existing.description).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.source).bind(&existing.pattern).bind(&existing.query)
            .bind(&existing.service_name).bind(&existing.apm_metric)
            .bind(existing.sensitivity).bind(existing.alpha)
            .bind(existing.eval_interval_secs).bind(existing.window_secs)
            .bind(&existing.split_labels).bind(&existing.notification_channel_ids)
            .bind(state).bind(last_eval_at).bind(&lta)
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    // ── Anomaly event operations ───────────────────────────────────────────────

    pub async fn get_anomaly_event(&self, id: &str) -> anyhow::Result<Option<crate::models::anomaly::AnomalyEvent>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, rule_id: String, state: String, metric: String, value: f64, expected: f64, deviation: f64, message: String, created_at: String }
        let result = self.client
            .query("SELECT id, rule_id, state, metric, value, expected, deviation, message, created_at FROM config_anomaly_events WHERE id = ? LIMIT 1")
            .bind(id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(crate::models::anomaly::AnomalyEvent { id: r.id, rule_id: r.rule_id, state: r.state, metric: r.metric, value: r.value, expected: r.expected, deviation: r.deviation, message: r.message, created_at: r.created_at })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_anomaly_event(&self, id: &str, rule_id: &str, state: &str, metric: &str, value: f64, expected: f64, deviation: f64, message: &str) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_anomaly_events (id, rule_id, state, metric, value, expected, deviation, message, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id).bind(rule_id).bind(state).bind(metric).bind(value).bind(expected).bind(deviation).bind(message).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_anomaly_events(&self, rule_id: &str, limit: i64) -> anyhow::Result<Vec<crate::models::anomaly::AnomalyEvent>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, rule_id: String, state: String, metric: String, value: f64, expected: f64, deviation: f64, message: String, created_at: String }
        let rows = self.client
            .query("SELECT id, rule_id, state, metric, value, expected, deviation, message, created_at FROM config_anomaly_events WHERE rule_id = ? ORDER BY created_at DESC LIMIT ?")
            .bind(rule_id).bind(limit as u64)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| crate::models::anomaly::AnomalyEvent { id: r.id, rule_id: r.rule_id, state: r.state, metric: r.metric, value: r.value, expected: r.expected, deviation: r.deviation, message: r.message, created_at: r.created_at }).collect())
    }

    pub async fn list_all_anomaly_events(&self, limit: i64) -> anyhow::Result<Vec<crate::models::anomaly::AnomalyEventWithRule>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, rule_id: String, rule_name: String, state: String, metric: String, value: f64, expected: f64, deviation: f64, message: String, created_at: String }
        let rows = self.client
            .query("SELECT e.id, e.rule_id, coalesce(r.name, 'deleted rule') AS rule_name, e.state, e.metric, e.value, e.expected, e.deviation, e.message, e.created_at FROM config_anomaly_events e LEFT JOIN (SELECT id, name FROM config_anomaly_rules FINAL WHERE is_deleted = 0) r ON e.rule_id = r.id ORDER BY e.created_at DESC LIMIT ?")
            .bind(limit as u64)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| crate::models::anomaly::AnomalyEventWithRule { id: r.id, rule_id: r.rule_id, rule_name: r.rule_name, state: r.state, metric: r.metric, value: r.value, expected: r.expected, deviation: r.deviation, message: r.message, created_at: r.created_at }).collect())
    }

    // ── Custom skills operations ───────────────────────────────────────────────

    async fn fetch_custom_skill_row(&self, sql: &str, bind_val: Option<&str>) -> anyhow::Result<Option<crate::models::custom_skills::CustomSkill>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, name: String, title: String, description: String, content: String, allowed_tools: String, enabled: u8, created_by: String, created_at: String, updated_at: String }
        let result = match bind_val {
            Some(v) => self.client.query(sql).bind(v).fetch_one::<Row>().await,
            None => self.client.query(sql).fetch_one::<Row>().await,
        };
        match result {
            Ok(r) => Ok(Some(crate::models::custom_skills::CustomSkill {
                id: r.id, name: r.name, title: r.title, description: r.description, content: r.content,
                allowed_tools: serde_json::from_str(&r.allowed_tools).unwrap_or_default(),
                enabled: r.enabled != 0, created_by: r.created_by,
                created_at: r.created_at, updated_at: r.updated_at,
            })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn list_custom_skills(&self) -> anyhow::Result<Vec<crate::models::custom_skills::CustomSkill>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, name: String, title: String, description: String, content: String, allowed_tools: String, enabled: u8, created_by: String, created_at: String, updated_at: String }
        let rows = self.client
            .query("SELECT id, name, title, description, content, allowed_tools, enabled, created_by, created_at, updated_at FROM config_custom_skills FINAL WHERE is_deleted = 0 ORDER BY name ASC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| crate::models::custom_skills::CustomSkill {
            id: r.id, name: r.name, title: r.title, description: r.description, content: r.content,
            allowed_tools: serde_json::from_str(&r.allowed_tools).unwrap_or_default(),
            enabled: r.enabled != 0, created_by: r.created_by,
            created_at: r.created_at, updated_at: r.updated_at,
        }).collect())
    }

    pub async fn get_custom_skill(&self, id: &str) -> anyhow::Result<Option<crate::models::custom_skills::CustomSkill>> {
        self.fetch_custom_skill_row("SELECT id, name, title, description, content, allowed_tools, enabled, created_by, created_at, updated_at FROM config_custom_skills FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1", Some(id)).await
    }

    pub async fn get_custom_skill_by_name(&self, name: &str) -> anyhow::Result<Option<crate::models::custom_skills::CustomSkill>> {
        self.fetch_custom_skill_row("SELECT id, name, title, description, content, allowed_tools, enabled, created_by, created_at, updated_at FROM config_custom_skills FINAL WHERE name = ? AND is_deleted = 0 LIMIT 1", Some(name)).await
    }

    pub async fn create_custom_skill(&self, req: &crate::models::custom_skills::CreateCustomSkillRequest, created_by: &str) -> anyhow::Result<crate::models::custom_skills::CustomSkill> {
        let id = uuid::Uuid::new_v4().to_string();
        let allowed_tools_json = serde_json::to_string(&req.allowed_tools)?;
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_custom_skills (id, name, title, description, content, allowed_tools, enabled, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(&id).bind(&req.name).bind(&req.title).bind(&req.description).bind(&req.content)
            .bind(&allowed_tools_json).bind(if req.enabled { 1u8 } else { 0u8 })
            .bind(created_by).bind(&now).bind(&now).bind(ver)
            .execute().await?;
        self.get_custom_skill(&id).await?.ok_or_else(|| anyhow::anyhow!("failed to fetch newly created custom skill"))
    }

    pub async fn update_custom_skill(&self, id: &str, req: &crate::models::custom_skills::UpdateCustomSkillRequest) -> anyhow::Result<Option<crate::models::custom_skills::CustomSkill>> {
        let existing = match self.get_custom_skill(id).await? { Some(r) => r, None => return Ok(None) };
        let allowed_tools_json = serde_json::to_string(&req.allowed_tools)?;
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_custom_skills (id, name, title, description, content, allowed_tools, enabled, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.name).bind(&req.title).bind(&req.description).bind(&req.content)
            .bind(&allowed_tools_json).bind(if req.enabled { 1u8 } else { 0u8 })
            .bind(&existing.created_by).bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        self.get_custom_skill(id).await
    }

    pub async fn delete_custom_skill(&self, id: &str) -> anyhow::Result<bool> {
        let existing = match self.get_custom_skill(id).await? { Some(r) => r, None => return Ok(false) };
        let allowed_tools_json = serde_json::to_string(&existing.allowed_tools)?;
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_custom_skills (id, name, title, description, content, allowed_tools, enabled, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&existing.name).bind(&existing.title).bind(&existing.description).bind(&existing.content)
            .bind(&allowed_tools_json).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.created_by).bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    // ── Service link operations ────────────────────────────────────────────────

    pub async fn list_service_links(&self) -> anyhow::Result<Vec<crate::models::service_link::ServiceLink>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { service_name: String, github_repo: String, default_branch: String, root_path: String, updated_at: String }
        let rows = self.client
            .query("SELECT service_name, github_repo, default_branch, root_path, updated_at FROM config_service_links FINAL WHERE is_deleted = 0 ORDER BY service_name ASC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| crate::models::service_link::ServiceLink { service_name: r.service_name, github_repo: r.github_repo, default_branch: r.default_branch, root_path: r.root_path, updated_at: r.updated_at }).collect())
    }

    pub async fn get_service_link(&self, service_name: &str) -> anyhow::Result<Option<crate::models::service_link::ServiceLink>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { service_name: String, github_repo: String, default_branch: String, root_path: String, updated_at: String }
        let result = self.client
            .query("SELECT service_name, github_repo, default_branch, root_path, updated_at FROM config_service_links FINAL WHERE service_name = ? AND is_deleted = 0 LIMIT 1")
            .bind(service_name)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(crate::models::service_link::ServiceLink { service_name: r.service_name, github_repo: r.github_repo, default_branch: r.default_branch, root_path: r.root_path, updated_at: r.updated_at })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn upsert_service_link(&self, service_name: &str, github_repo: &str, default_branch: &str, root_path: &str) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_service_links (service_name, github_repo, default_branch, root_path, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, 0)")
            .bind(service_name).bind(github_repo).bind(default_branch).bind(root_path).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn delete_service_link(&self, service_name: &str) -> anyhow::Result<bool> {
        let existing = match self.get_service_link(service_name).await? { Some(r) => r, None => return Ok(false) };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_service_links (service_name, github_repo, default_branch, root_path, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, 1)")
            .bind(service_name).bind(&existing.github_repo).bind(&existing.default_branch).bind(&existing.root_path).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    // ── Monitor operations ─────────────────────────────────────────────────────

    async fn fetch_monitors(&self, sql: &str, bind_vals: &[&str]) -> anyhow::Result<Vec<crate::models::monitor::Monitor>> {
        let mut q = self.client.query(sql);
        for v in bind_vals { q = q.bind(*v); }
        let rows = q.fetch_all::<MonitorRow>().await?;
        Ok(rows.into_iter().map(Self::map_monitor_row).collect())
    }

    fn map_monitor_row(r: MonitorRow) -> crate::models::monitor::Monitor {
        crate::models::monitor::Monitor {
            id: r.id, tenant_id: r.tenant_id, name: r.name, monitor_type: r.monitor_type,
            query_config: r.query_config, critical: r.critical, critical_recovery: r.critical_recovery,
            warning: r.warning, warning_recovery: r.warning_recovery, comparator: r.comparator,
            eval_window_secs: r.eval_window_secs, eval_interval_secs: r.eval_interval_secs,
            group_by: r.group_by, state: r.state, group_states: r.group_states,
            no_data_action: r.no_data_action, no_data_timeframe: r.no_data_timeframe,
            auto_resolve_hours: r.auto_resolve_hours, message: r.message,
            notification_channels: r.notification_channels, renotify_interval: r.renotify_interval,
            tags: r.tags, priority: r.priority, enabled: r.enabled != 0,
            composite_formula: r.composite_formula, composite_monitor_ids: r.composite_monitor_ids,
            last_eval_at: if r.last_eval_at.is_empty() { None } else { Some(r.last_eval_at) },
            last_triggered_at: if r.last_triggered_at.is_empty() { None } else { Some(r.last_triggered_at) },
            created_by: r.created_by, created_at: r.created_at, updated_at: r.updated_at,
        }
    }

    const MONITOR_SELECT: &'static str = "SELECT id, tenant_id, name, monitor_type, query_config, critical, critical_recovery, warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, group_by, state, group_states, no_data_action, no_data_timeframe, auto_resolve_hours, message, notification_channels, renotify_interval, tags, priority, enabled, composite_formula, composite_monitor_ids, last_eval_at, last_triggered_at, created_by, created_at, updated_at FROM config_monitors FINAL WHERE is_deleted = 0";

    pub async fn list_monitors(&self, tenant_id: &str) -> anyhow::Result<Vec<crate::models::monitor::Monitor>> {
        self.fetch_monitors(&format!("{} AND tenant_id = ? ORDER BY created_at DESC", Self::MONITOR_SELECT), &[tenant_id]).await
    }

    pub async fn get_monitor(&self, id: &str, tenant_id: &str) -> anyhow::Result<Option<crate::models::monitor::Monitor>> {
        let mut q = self.client.query(&format!("{} AND id = ? AND tenant_id = ? LIMIT 1", Self::MONITOR_SELECT)).bind(id).bind(tenant_id);
        let result = q.fetch_one::<MonitorRow>().await;
        match result {
            Ok(r) => Ok(Some(Self::map_monitor_row(r))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_monitor_by_id(&self, id: &str) -> anyhow::Result<Option<crate::models::monitor::Monitor>> {
        let result = self.client.query(&format!("{} AND id = ? LIMIT 1", Self::MONITOR_SELECT)).bind(id).fetch_one::<MonitorRow>().await;
        match result {
            Ok(r) => Ok(Some(Self::map_monitor_row(r))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_monitor(&self, id: &str, tenant_id: &str, name: &str, monitor_type: &str, query_config: &str, critical: Option<f64>, critical_recovery: Option<f64>, warning: Option<f64>, warning_recovery: Option<f64>, comparator: &str, eval_window_secs: i64, eval_interval_secs: i64, group_by: &str, no_data_action: &str, no_data_timeframe: i64, auto_resolve_hours: Option<i64>, message: &str, notification_channels: &str, renotify_interval: Option<i64>, tags: &str, priority: Option<i64>, enabled: bool, composite_formula: &str, composite_monitor_ids: &str, created_by: &str) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_monitors (id, tenant_id, name, monitor_type, query_config, critical, critical_recovery, warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, group_by, state, group_states, no_data_action, no_data_timeframe, auto_resolve_hours, message, notification_channels, renotify_interval, tags, priority, enabled, composite_formula, composite_monitor_ids, last_eval_at, last_triggered_at, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ok', '{}', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, 0)")
            .bind(id).bind(tenant_id).bind(name).bind(monitor_type).bind(query_config)
            .bind(critical).bind(critical_recovery).bind(warning).bind(warning_recovery)
            .bind(comparator).bind(eval_window_secs).bind(eval_interval_secs).bind(group_by)
            .bind(no_data_action).bind(no_data_timeframe).bind(auto_resolve_hours)
            .bind(message).bind(notification_channels).bind(renotify_interval)
            .bind(tags).bind(priority).bind(if enabled { 1u8 } else { 0u8 })
            .bind(composite_formula).bind(composite_monitor_ids)
            .bind(created_by).bind(&now).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_monitor(&self, id: &str, tenant_id: &str, name: &str, monitor_type: &str, query_config: &str, critical: Option<f64>, critical_recovery: Option<f64>, warning: Option<f64>, warning_recovery: Option<f64>, comparator: &str, eval_window_secs: i64, eval_interval_secs: i64, group_by: &str, no_data_action: &str, no_data_timeframe: i64, auto_resolve_hours: Option<i64>, message: &str, notification_channels: &str, renotify_interval: Option<i64>, tags: &str, priority: Option<i64>, enabled: bool, composite_formula: &str, composite_monitor_ids: &str) -> anyhow::Result<bool> {
        let existing = match self.get_monitor(id, tenant_id).await? { Some(r) => r, None => return Ok(false) };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_monitors (id, tenant_id, name, monitor_type, query_config, critical, critical_recovery, warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, group_by, state, group_states, no_data_action, no_data_timeframe, auto_resolve_hours, message, notification_channels, renotify_interval, tags, priority, enabled, composite_formula, composite_monitor_ids, last_eval_at, last_triggered_at, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(tenant_id).bind(name).bind(monitor_type).bind(query_config)
            .bind(critical).bind(critical_recovery).bind(warning).bind(warning_recovery)
            .bind(comparator).bind(eval_window_secs).bind(eval_interval_secs).bind(group_by)
            .bind(&existing.state).bind(&existing.group_states)
            .bind(no_data_action).bind(no_data_timeframe).bind(auto_resolve_hours)
            .bind(message).bind(notification_channels).bind(renotify_interval)
            .bind(tags).bind(priority).bind(if enabled { 1u8 } else { 0u8 })
            .bind(composite_formula).bind(composite_monitor_ids)
            .bind(existing.last_eval_at.unwrap_or_default())
            .bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_by).bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn delete_monitor(&self, id: &str, tenant_id: &str) -> anyhow::Result<bool> {
        let existing = match self.get_monitor(id, tenant_id).await? { Some(r) => r, None => return Ok(false) };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_monitors (id, tenant_id, name, monitor_type, query_config, critical, critical_recovery, warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, group_by, state, group_states, no_data_action, no_data_timeframe, auto_resolve_hours, message, notification_channels, renotify_interval, tags, priority, enabled, composite_formula, composite_monitor_ids, last_eval_at, last_triggered_at, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(tenant_id).bind(&existing.name).bind(&existing.monitor_type).bind(&existing.query_config)
            .bind(existing.critical).bind(existing.critical_recovery).bind(existing.warning).bind(existing.warning_recovery)
            .bind(&existing.comparator).bind(existing.eval_window_secs).bind(existing.eval_interval_secs).bind(&existing.group_by)
            .bind(&existing.state).bind(&existing.group_states)
            .bind(&existing.no_data_action).bind(existing.no_data_timeframe).bind(existing.auto_resolve_hours)
            .bind(&existing.message).bind(&existing.notification_channels).bind(existing.renotify_interval)
            .bind(&existing.tags).bind(existing.priority).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.composite_formula).bind(&existing.composite_monitor_ids)
            .bind(existing.last_eval_at.unwrap_or_default()).bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_by).bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn list_enabled_monitors(&self) -> anyhow::Result<Vec<crate::models::monitor::Monitor>> {
        self.fetch_monitors(&format!("{} AND enabled = 1", Self::MONITOR_SELECT), &[]).await
    }

    pub async fn update_monitor_state(&self, id: &str, state: &str, group_states: &str, last_eval_at: &str) -> anyhow::Result<()> {
        let existing = match self.get_monitor_by_id(id).await? { Some(r) => r, None => return Ok(()) };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_monitors (id, tenant_id, name, monitor_type, query_config, critical, critical_recovery, warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, group_by, state, group_states, no_data_action, no_data_timeframe, auto_resolve_hours, message, notification_channels, renotify_interval, tags, priority, enabled, composite_formula, composite_monitor_ids, last_eval_at, last_triggered_at, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.tenant_id).bind(&existing.name).bind(&existing.monitor_type).bind(&existing.query_config)
            .bind(existing.critical).bind(existing.critical_recovery).bind(existing.warning).bind(existing.warning_recovery)
            .bind(&existing.comparator).bind(existing.eval_window_secs).bind(existing.eval_interval_secs).bind(&existing.group_by)
            .bind(state).bind(group_states)
            .bind(&existing.no_data_action).bind(existing.no_data_timeframe).bind(existing.auto_resolve_hours)
            .bind(&existing.message).bind(&existing.notification_channels).bind(existing.renotify_interval)
            .bind(&existing.tags).bind(existing.priority).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.composite_formula).bind(&existing.composite_monitor_ids)
            .bind(last_eval_at).bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_by).bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn update_monitor_triggered(&self, id: &str, last_triggered_at: &str) -> anyhow::Result<()> {
        let existing = match self.get_monitor_by_id(id).await? { Some(r) => r, None => return Ok(()) };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_monitors (id, tenant_id, name, monitor_type, query_config, critical, critical_recovery, warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, group_by, state, group_states, no_data_action, no_data_timeframe, auto_resolve_hours, message, notification_channels, renotify_interval, tags, priority, enabled, composite_formula, composite_monitor_ids, last_eval_at, last_triggered_at, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.tenant_id).bind(&existing.name).bind(&existing.monitor_type).bind(&existing.query_config)
            .bind(existing.critical).bind(existing.critical_recovery).bind(existing.warning).bind(existing.warning_recovery)
            .bind(&existing.comparator).bind(existing.eval_window_secs).bind(existing.eval_interval_secs).bind(&existing.group_by)
            .bind(&existing.state).bind(&existing.group_states)
            .bind(&existing.no_data_action).bind(existing.no_data_timeframe).bind(existing.auto_resolve_hours)
            .bind(&existing.message).bind(&existing.notification_channels).bind(existing.renotify_interval)
            .bind(&existing.tags).bind(existing.priority).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.composite_formula).bind(&existing.composite_monitor_ids)
            .bind(existing.last_eval_at.unwrap_or_default()).bind(last_triggered_at)
            .bind(&existing.created_by).bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn create_monitor_event(&self, id: &str, monitor_id: &str, tenant_id: &str, group_key: &str, prev_state: &str, new_state: &str, value: Option<f64>, threshold: Option<f64>, message: &str) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_monitor_events (id, monitor_id, tenant_id, group_key, prev_state, new_state, value, threshold, message, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id).bind(monitor_id).bind(tenant_id).bind(group_key).bind(prev_state).bind(new_state).bind(value).bind(threshold).bind(message).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_monitor_events(&self, monitor_id: &str, limit: i64) -> anyhow::Result<Vec<crate::models::monitor::MonitorEvent>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, monitor_id: String, tenant_id: String, group_key: String, prev_state: String, new_state: String, value: Option<f64>, threshold: Option<f64>, message: String, created_at: String }
        let rows = self.client
            .query("SELECT id, monitor_id, tenant_id, group_key, prev_state, new_state, value, threshold, message, created_at FROM config_monitor_events WHERE monitor_id = ? ORDER BY created_at DESC LIMIT ?")
            .bind(monitor_id).bind(limit as u64)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| crate::models::monitor::MonitorEvent { id: r.id, monitor_id: r.monitor_id, tenant_id: r.tenant_id, group_key: r.group_key, prev_state: r.prev_state, new_state: r.new_state, value: r.value, threshold: r.threshold, message: r.message, created_at: r.created_at }).collect())
    }

    pub async fn count_monitors(&self, tenant_id: &str) -> anyhow::Result<i64> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Count { n: u64 }
        let row = self.client
            .query("SELECT count() AS n FROM config_monitors FINAL WHERE tenant_id = ? AND is_deleted = 0")
            .bind(tenant_id)
            .fetch_one::<Count>()
            .await?;
        Ok(row.n as i64)
    }

    pub async fn set_monitor_enabled(&self, id: &str, tenant_id: &str, enabled: bool) -> anyhow::Result<bool> {
        let existing = match self.get_monitor(id, tenant_id).await? { Some(r) => r, None => return Ok(false) };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_monitors (id, tenant_id, name, monitor_type, query_config, critical, critical_recovery, warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, group_by, state, group_states, no_data_action, no_data_timeframe, auto_resolve_hours, message, notification_channels, renotify_interval, tags, priority, enabled, composite_formula, composite_monitor_ids, last_eval_at, last_triggered_at, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(tenant_id).bind(&existing.name).bind(&existing.monitor_type).bind(&existing.query_config)
            .bind(existing.critical).bind(existing.critical_recovery).bind(existing.warning).bind(existing.warning_recovery)
            .bind(&existing.comparator).bind(existing.eval_window_secs).bind(existing.eval_interval_secs).bind(&existing.group_by)
            .bind(&existing.state).bind(&existing.group_states)
            .bind(&existing.no_data_action).bind(existing.no_data_timeframe).bind(existing.auto_resolve_hours)
            .bind(&existing.message).bind(&existing.notification_channels).bind(existing.renotify_interval)
            .bind(&existing.tags).bind(existing.priority).bind(if enabled { 1u8 } else { 0u8 })
            .bind(&existing.composite_formula).bind(&existing.composite_monitor_ids)
            .bind(existing.last_eval_at.unwrap_or_default()).bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_by).bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    // ── SIEM Detection Rule operations ──

    pub async fn list_detection_rules(
        &self,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<Vec<crate::models::detection::DetectionRule>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String, tenant_id: String, name: String, description: String,
            query_sql: String, interval_secs: i64, threshold: i64, severity: String,
            window_secs: i64, enabled: u8, channels: String, created_by: String,
            last_eval_at: String, last_triggered_at: String,
            created_at: String, updated_at: String,
        }
        let map_row = |r: Row| crate::models::detection::DetectionRule {
            id: r.id, tenant_id: r.tenant_id, name: r.name, description: r.description,
            query_sql: r.query_sql, interval_secs: r.interval_secs, threshold: r.threshold,
            severity: r.severity, window_secs: r.window_secs, enabled: r.enabled != 0,
            channels: r.channels, created_by: r.created_by,
            last_eval_at: if r.last_eval_at.is_empty() { None } else { Some(r.last_eval_at) },
            last_triggered_at: if r.last_triggered_at.is_empty() { None } else { Some(r.last_triggered_at) },
            created_at: r.created_at, updated_at: r.updated_at,
        };
        let rows = if let Some(tid) = tenant_id {
            self.client
                .query("SELECT id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at FROM config_detection_rules FINAL WHERE tenant_id = ? AND is_deleted = 0 ORDER BY created_at DESC")
                .bind(tid)
                .fetch_all::<Row>()
                .await?
        } else {
            self.client
                .query("SELECT id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at FROM config_detection_rules FINAL WHERE is_deleted = 0 ORDER BY created_at DESC")
                .fetch_all::<Row>()
                .await?
        };
        Ok(rows.into_iter().map(map_row).collect())
    }

    pub async fn get_detection_rule(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<crate::models::detection::DetectionRule>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String, tenant_id: String, name: String, description: String,
            query_sql: String, interval_secs: i64, threshold: i64, severity: String,
            window_secs: i64, enabled: u8, channels: String, created_by: String,
            last_eval_at: String, last_triggered_at: String,
            created_at: String, updated_at: String,
        }
        let rows = self.client
            .query("SELECT id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at FROM config_detection_rules FINAL WHERE id = ? AND is_deleted = 0")
            .bind(id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().next().map(|r| crate::models::detection::DetectionRule {
            id: r.id, tenant_id: r.tenant_id, name: r.name, description: r.description,
            query_sql: r.query_sql, interval_secs: r.interval_secs, threshold: r.threshold,
            severity: r.severity, window_secs: r.window_secs, enabled: r.enabled != 0,
            channels: r.channels, created_by: r.created_by,
            last_eval_at: if r.last_eval_at.is_empty() { None } else { Some(r.last_eval_at) },
            last_triggered_at: if r.last_triggered_at.is_empty() { None } else { Some(r.last_triggered_at) },
            created_at: r.created_at, updated_at: r.updated_at,
        }))
    }

    pub async fn create_detection_rule(
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
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_detection_rules (id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, 0)")
            .bind(id).bind(tenant_id).bind(name).bind(description).bind(query_sql)
            .bind(interval_secs).bind(threshold).bind(severity).bind(window_secs)
            .bind(if enabled { 1u8 } else { 0u8 }).bind(channels).bind(created_by)
            .bind(&now).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn update_detection_rule(
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
        let existing = match self.get_detection_rule(id).await? { Some(r) => r, None => return Ok(false) };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_detection_rules (id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.tenant_id).bind(name).bind(description).bind(query_sql)
            .bind(interval_secs).bind(threshold).bind(severity).bind(window_secs)
            .bind(if enabled { 1u8 } else { 0u8 }).bind(channels).bind(&existing.created_by)
            .bind(&existing.last_eval_at.unwrap_or_default())
            .bind(&existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn delete_detection_rule(&self, id: &str) -> anyhow::Result<bool> {
        let existing = match self.get_detection_rule(id).await? { Some(r) => r, None => return Ok(false) };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_detection_rules (id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&existing.tenant_id).bind(&existing.name).bind(&existing.description)
            .bind(&existing.query_sql).bind(existing.interval_secs).bind(existing.threshold)
            .bind(&existing.severity).bind(existing.window_secs)
            .bind(if existing.enabled { 1u8 } else { 0u8 }).bind(&existing.channels)
            .bind(&existing.created_by)
            .bind(&existing.last_eval_at.unwrap_or_default())
            .bind(&existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn list_enabled_detection_rules(
        &self,
    ) -> anyhow::Result<Vec<crate::models::detection::DetectionRule>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String, tenant_id: String, name: String, description: String,
            query_sql: String, interval_secs: i64, threshold: i64, severity: String,
            window_secs: i64, enabled: u8, channels: String, created_by: String,
            last_eval_at: String, last_triggered_at: String,
            created_at: String, updated_at: String,
        }
        let rows = self.client
            .query("SELECT id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at FROM config_detection_rules FINAL WHERE enabled = 1 AND is_deleted = 0 ORDER BY created_at ASC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| crate::models::detection::DetectionRule {
            id: r.id, tenant_id: r.tenant_id, name: r.name, description: r.description,
            query_sql: r.query_sql, interval_secs: r.interval_secs, threshold: r.threshold,
            severity: r.severity, window_secs: r.window_secs, enabled: r.enabled != 0,
            channels: r.channels, created_by: r.created_by,
            last_eval_at: if r.last_eval_at.is_empty() { None } else { Some(r.last_eval_at) },
            last_triggered_at: if r.last_triggered_at.is_empty() { None } else { Some(r.last_triggered_at) },
            created_at: r.created_at, updated_at: r.updated_at,
        }).collect())
    }

    pub async fn update_detection_rule_eval(
        &self,
        id: &str,
        last_eval_at: &str,
        last_triggered_at: Option<&str>,
    ) -> anyhow::Result<()> {
        let existing = match self.get_detection_rule(id).await? { Some(r) => r, None => return Ok(()) };
        let now = Self::now_str();
        let ver = Self::next_version();
        let triggered = last_triggered_at.unwrap_or_else(|| existing.last_triggered_at.as_deref().unwrap_or(""));
        self.client
            .query("INSERT INTO config_detection_rules (id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.tenant_id).bind(&existing.name).bind(&existing.description)
            .bind(&existing.query_sql).bind(existing.interval_secs).bind(existing.threshold)
            .bind(&existing.severity).bind(existing.window_secs)
            .bind(if existing.enabled { 1u8 } else { 0u8 }).bind(&existing.channels)
            .bind(&existing.created_by).bind(last_eval_at).bind(triggered)
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn count_detection_rules(&self) -> anyhow::Result<i64> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Count { n: u64 }
        let row = self.client
            .query("SELECT count() AS n FROM config_detection_rules FINAL WHERE is_deleted = 0")
            .fetch_one::<Count>()
            .await?;
        Ok(row.n as i64)
    }

    async fn default_detection_rule_exists(&self, name: &str, tenant_id: &str) -> anyhow::Result<bool> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Count { n: u64 }
        let row = self.client
            .query("SELECT count() AS n FROM config_detection_rules FINAL WHERE name = ? AND tenant_id = ? AND created_by = 'system' AND is_deleted = 0")
            .bind(name).bind(tenant_id)
            .fetch_one::<Count>()
            .await?;
        Ok(row.n > 0)
    }

    pub async fn ensure_default_detection_rules(&self) -> anyhow::Result<()> {
        tracing::info!("SIEM: checking default detection rules");

        // (name, description, query_sql, severity, interval_secs, window_secs)
        let defaults: Vec<(&str, &str, &str, &str, i64, i64)> = vec![
            (
                "Failed login brute force",
                "Detects IPs with 10+ failed login attempts within the detection window.",
                "SELECT mat_source_ip, count() AS attempt_count \
                 FROM otel_logs \
                 WHERE Timestamp BETWEEN @window_start AND @window_end \
                   AND mat_action = 'login_failed' \
                 GROUP BY mat_source_ip \
                 HAVING attempt_count >= 10",
                "high", 300, 300,
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
                "high", 300, 300,
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
                "high", 300, 300,
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
                "critical", 300, 300,
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
                "high", 300, 300,
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
                "critical", 300, 300,
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
                "medium", 300, 300,
            ),
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
                "critical", 300, 300,
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
                "high", 300, 300,
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
                "high", 300, 600,
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
                "medium", 300, 300,
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
                "critical", 300, 300,
            ),
        ];

        let mut seeded = 0u32;
        for (name, description, query_sql, severity, interval, window) in &defaults {
            if self.default_detection_rule_exists(name, "default").await? {
                continue;
            }
            let id = uuid::Uuid::new_v4().to_string();
            self.create_detection_rule(
                &id, "default", name, description, query_sql,
                *interval, 1, severity, *window, true, "[]", "system",
            ).await?;
            seeded += 1;
        }

        if seeded > 0 {
            tracing::info!("SIEM: seeded {seeded} new default detection rules ({} total built-in)", defaults.len());
        } else {
            tracing::debug!("SIEM: all {} default detection rules already present", defaults.len());
        }
        Ok(())
    }

    // ── SIEM Detection Event operations ──

    pub async fn create_detection_event(
        &self,
        id: &str,
        rule_id: &str,
        tenant_id: &str,
        severity: &str,
        match_count: i64,
        sample_data: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_detection_events (id, rule_id, tenant_id, severity, match_count, sample_data, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(id).bind(rule_id).bind(tenant_id).bind(severity).bind(match_count).bind(sample_data).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_detection_events(
        &self,
        tenant_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::detection::DetectionEventWithRule>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String, rule_id: String, rule_name: String, tenant_id: String,
            severity: String, match_count: i64, sample_data: String, created_at: String,
        }
        let rows = self.client
            .query("SELECT e.id, e.rule_id, coalesce(r.name, 'deleted rule') AS rule_name, e.tenant_id, e.severity, e.match_count, e.sample_data, e.created_at FROM config_detection_events e LEFT JOIN (SELECT id, name FROM config_detection_rules FINAL WHERE is_deleted = 0) r ON e.rule_id = r.id WHERE e.tenant_id = ? ORDER BY e.created_at DESC LIMIT ?")
            .bind(tenant_id).bind(limit as u64)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| {
            let sample_data_json: serde_json::Value = serde_json::from_str(&r.sample_data).unwrap_or(serde_json::json!([]));
            crate::models::detection::DetectionEventWithRule {
                id: r.id, rule_id: r.rule_id, rule_name: r.rule_name, tenant_id: r.tenant_id,
                severity: r.severity, match_count: r.match_count, sample_data: sample_data_json,
                created_at: r.created_at,
            }
        }).collect())
    }

    // ── Alert Maintenance Windows ──────────────────────────────────────────────

    pub async fn create_maintenance_window(
        &self,
        id: &str,
        name: &str,
        scope: &str,
        starts_at: &str,
        ends_at: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_maintenance_windows (id, name, scope, starts_at, ends_at, created_at) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(id).bind(name).bind(scope).bind(starts_at).bind(ends_at).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_maintenance_windows(
        &self,
    ) -> anyhow::Result<Vec<(String, String, String, String, String, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, name: String, scope: String, starts_at: String, ends_at: String, created_at: String }
        let rows = self.client
            .query("SELECT id, name, scope, starts_at, ends_at, created_at FROM config_maintenance_windows ORDER BY starts_at DESC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| (r.id, r.name, r.scope, r.starts_at, r.ends_at, r.created_at)).collect())
    }

    pub async fn delete_maintenance_window(&self, id: &str) -> anyhow::Result<bool> {
        self.client
            .query("ALTER TABLE config_maintenance_windows DELETE WHERE id = ?")
            .bind(id)
            .execute().await?;
        Ok(true)
    }

    /// Returns true if `now_str` (ISO 8601) falls within any active maintenance window
    /// that covers this alert_id (or all alerts if scope = 'all').
    pub async fn is_in_maintenance(&self, now_str: &str, alert_id: Option<&str>) -> bool {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Count { n: u64 }
        let alert_scope = alert_id.map(|id| format!("alert:{id}")).unwrap_or_default();
        let result = self.client
            .query("SELECT count() AS n FROM config_maintenance_windows WHERE starts_at <= ? AND ends_at >= ? AND (scope = 'all' OR scope = ?)")
            .bind(now_str).bind(now_str).bind(&alert_scope)
            .fetch_one::<Count>()
            .await;
        result.map(|r| r.n > 0).unwrap_or(false)
    }

    // ── Trace Funnels ──────────────────────────────────────────────────────────

    pub async fn create_funnel(
        &self,
        id: &str,
        name: &str,
        steps_json: &str,
        tenant_id: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_trace_funnels (id, name, steps_json, tenant_id, created_at) VALUES (?, ?, ?, ?, ?)")
            .bind(id).bind(name).bind(steps_json).bind(tenant_id).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_funnels(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<(String, String, String, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, name: String, steps_json: String, created_at: String }
        let rows = self.client
            .query("SELECT id, name, steps_json, created_at FROM config_trace_funnels WHERE tenant_id = ? ORDER BY created_at DESC")
            .bind(tenant_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| (r.id, r.name, r.steps_json, r.created_at)).collect())
    }

    pub async fn get_funnel(
        &self,
        id: &str,
        tenant_id: &str,
    ) -> anyhow::Result<Option<(String, String, String, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row { id: String, name: String, steps_json: String, created_at: String }
        let rows = self.client
            .query("SELECT id, name, steps_json, created_at FROM config_trace_funnels WHERE id = ? AND tenant_id = ?")
            .bind(id).bind(tenant_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().next().map(|r| (r.id, r.name, r.steps_json, r.created_at)))
    }

    pub async fn delete_funnel(&self, id: &str, tenant_id: &str) -> anyhow::Result<bool> {
        self.client
            .query("ALTER TABLE config_trace_funnels DELETE WHERE id = ? AND tenant_id = ?")
            .bind(id).bind(tenant_id)
            .execute().await?;
        Ok(true)
    }
}
