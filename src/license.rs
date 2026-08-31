//! License verification.
//!
//! A license is an Ed25519-signed JWT supplied via the `RUSH_LICENSE_KEY` env
//! var (typically a Kubernetes Secret). We verify it offline against an embedded
//! public key — no license server required. This is honest-customer licensing:
//! it gates entitlements and proves a legit, unexpired license, not unbreakable
//! DRM. Mint licenses with the `mint-license` binary using the private key.
use serde::{Deserialize, Serialize};

/// Embedded Ed25519 public key (PEM). Override with `RUSH_LICENSE_PUBKEY` only
/// for development against a different signing key.
const EMBEDDED_PUBKEY_PEM: &str = "-----BEGIN PUBLIC KEY-----\n\
MCowBQYDK2VwAyEAwb1wsHS0gi1aKKSykN+XfQYHAFGz4A8/HVgSKi4HDKM=\n\
-----END PUBLIC KEY-----\n";

/// Claims signed into a license token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LicenseClaims {
    /// Customer / organization name.
    pub sub: String,
    /// Plan tier (e.g. "community", "pro", "enterprise").
    #[serde(default)]
    pub plan: String,
    /// Entitled add-ons (e.g. ["postgres", "cloudtrail"]).
    #[serde(default)]
    pub entitlements: Vec<String>,
    /// Expiry (unix seconds) — verified by jsonwebtoken.
    pub exp: i64,
    /// Issued-at (unix seconds).
    #[serde(default)]
    pub iat: i64,
}

/// Resolved license status returned to the API/UI.
#[derive(Debug, Clone, Serialize)]
pub struct LicenseStatus {
    /// True only when a signed, unexpired license is present.
    pub valid: bool,
    /// "active" | "expired" | "invalid" | "unlicensed".
    pub status: String,
    pub customer: Option<String>,
    pub plan: String,
    pub entitlements: Vec<String>,
    /// RFC3339 expiry, if a license is present.
    pub expires_at: Option<String>,
    /// Human-readable reason when not active (e.g. "license expired").
    pub reason: Option<String>,
}

impl LicenseStatus {
    fn unlicensed() -> Self {
        Self {
            valid: false,
            status: "unlicensed".into(),
            customer: None,
            plan: "community".into(),
            entitlements: Vec::new(),
            expires_at: None,
            reason: Some("no RUSH_LICENSE_KEY set — running community edition".into()),
        }
    }

    fn invalid(reason: impl Into<String>) -> Self {
        Self {
            valid: false,
            status: "invalid".into(),
            customer: None,
            plan: "community".into(),
            entitlements: Vec::new(),
            expires_at: None,
            reason: Some(reason.into()),
        }
    }

    /// Is a specific add-on entitled under a valid license?
    pub fn has_entitlement(&self, addon: &str) -> bool {
        self.valid && self.entitlements.iter().any(|e| e == addon)
    }
}

fn pubkey_pem() -> String {
    std::env::var("RUSH_LICENSE_PUBKEY").unwrap_or_else(|_| EMBEDDED_PUBKEY_PEM.to_string())
}

/// Verify the license in `RUSH_LICENSE_KEY` (offline). Cheap enough to call per
/// request; never panics — bad input maps to an invalid/unlicensed status.
pub fn evaluate() -> LicenseStatus {
    let token = match std::env::var("RUSH_LICENSE_KEY") {
        Ok(t) if !t.trim().is_empty() => t,
        _ => return LicenseStatus::unlicensed(),
    };
    verify(token.trim(), &pubkey_pem())
}

fn verify(token: &str, pubkey: &str) -> LicenseStatus {
    use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};

    let key = match DecodingKey::from_ed_pem(pubkey.as_bytes()) {
        Ok(k) => k,
        Err(e) => return LicenseStatus::invalid(format!("bad license public key: {e}")),
    };
    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.validate_aud = false; // licenses carry no audience
    validation.required_spec_claims = ["exp"].into_iter().map(String::from).collect();

    match decode::<LicenseClaims>(token, &key, &validation) {
        Ok(data) => {
            let c = data.claims;
            let expires_at = chrono::DateTime::from_timestamp(c.exp, 0).map(|dt| dt.to_rfc3339());
            LicenseStatus {
                valid: true,
                status: "active".into(),
                customer: Some(c.sub),
                plan: if c.plan.is_empty() {
                    "licensed".into()
                } else {
                    c.plan
                },
                entitlements: c.entitlements,
                expires_at,
                reason: None,
            }
        }
        Err(e) => {
            use jsonwebtoken::errors::ErrorKind;
            match e.kind() {
                ErrorKind::ExpiredSignature => {
                    let mut s = LicenseStatus::invalid("license expired");
                    s.status = "expired".into();
                    s
                }
                _ => LicenseStatus::invalid(format!("invalid license: {e}")),
            }
        }
    }
}
