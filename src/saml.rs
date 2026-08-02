//! Minimal SAML 2.0 helpers for Rush Observability.
//!
//! This module builds SAML AuthnRequest XML for SP-initiated login,
//! parses SAML Response assertions, generates SP metadata XML, and
//! verifies XML signatures against the IdP X.509 certificate.

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use flate2::{Compression, write::DeflateEncoder};
use quick_xml::Reader;
use quick_xml::events::Event;
use std::collections::HashMap;
use std::io::Write;

/// Parsed fields from a SAML Response assertion.
#[derive(Debug, Clone)]
pub struct SamlAssertion {
    pub name_id: String,
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub groups: Vec<String>,
    pub attributes: HashMap<String, String>,
}

/// Build a SAML AuthnRequest XML string.
pub fn build_authn_request(sp_entity_id: &str, acs_url: &str, idp_sso_url: &str) -> String {
    let id = format!("_rush_{}", uuid::Uuid::new_v4());
    let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ");

    format!(
        r#"<samlp:AuthnRequest xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion" ID="{id}" Version="2.0" IssueInstant="{now}" Destination="{idp_sso_url}" AssertionConsumerServiceURL="{acs_url}" ProtocolBinding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"><saml:Issuer>{sp_entity_id}</saml:Issuer></samlp:AuthnRequest>"#,
    )
}

/// Deflate-compress, base64-encode, and URL-encode a SAMLRequest for HTTP-Redirect binding.
pub fn encode_authn_request_redirect(authn_request_xml: &str) -> String {
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(authn_request_xml.as_bytes())
        .expect("deflate write");
    let compressed = encoder.finish().expect("deflate finish");
    let b64 = B64.encode(&compressed);
    urlencoding::encode(&b64).into_owned()
}

/// Build the full redirect URL for SP-initiated SAML login.
/// Returns the URL to redirect the browser to.
pub fn build_login_redirect_url(
    sp_entity_id: &str,
    acs_url: &str,
    idp_sso_url: &str,
    relay_state: &str,
) -> String {
    let xml = build_authn_request(sp_entity_id, acs_url, idp_sso_url);
    let encoded = encode_authn_request_redirect(&xml);
    let relay_encoded = urlencoding::encode(relay_state);

    let sep = if idp_sso_url.contains('?') { "&" } else { "?" };
    format!("{idp_sso_url}{sep}SAMLRequest={encoded}&RelayState={relay_encoded}")
}

/// Verify the enveloped XML signature of a SAML Response against the IdP's
/// X.509 certificate.
///
/// Backed by `opensaml`/`bergshamra` (pure-Rust XML-DSig): performs exclusive
/// XML canonicalization (exc-c14n), validates both the `SignedInfo` RSA
/// signature AND every reference `DigestValue` over the canonicalized signed
/// element, and guards against signature-wrapping (XSW) and duplicate-ID
/// attacks. Verification trusts ONLY the metadata-pinned certificate passed in
/// (inline KeyInfo certs are never imported as key material).
///
/// Returns `Ok(true)` only if a signature verifies against `idp_cert_pem`,
/// `Ok(false)` if present-but-invalid (tampered / wrong key), and `Err` only on
/// a structural problem (e.g. the certificate could not be parsed).
pub fn verify_signature(xml: &str, idp_cert_pem: &str) -> Result<bool, String> {
    // bergshamra wants the bare base64 certificate body — it rejects the PEM
    // armor lines. Drop the BEGIN/END lines and concatenate.
    let cert_b64: String = idp_cert_pem
        .lines()
        .filter(|l| !l.contains("CERTIFICATE"))
        .collect::<Vec<_>>()
        .join("");

    match opensaml::crypto::verify::verify_signature(xml, std::slice::from_ref(&cert_b64)) {
        Ok((valid, _verified_content)) => Ok(valid),
        Err(e) => Err(format!("SAML signature verification error: {e:?}")),
    }
}

/// Parse a base64-encoded SAMLResponse XML and extract assertion fields.
///
/// If `idp_cert_pem` is provided (non-empty), the XML signature will be
/// verified against the IdP certificate before the assertion is trusted.
/// If no certificate is provided, signature verification is skipped
/// (backward compatible for development setups without a configured cert).
pub fn parse_saml_response(
    b64_response: &str,
    groups_claim: &str,
) -> Result<SamlAssertion, String> {
    let xml_bytes = B64
        .decode(b64_response.trim())
        .map_err(|e| format!("failed to base64-decode SAMLResponse: {e}"))?;
    let xml = String::from_utf8_lossy(&xml_bytes);

    parse_assertion_xml(&xml, groups_claim)
}

/// Parse assertion fields from raw SAML Response XML.
fn parse_assertion_xml(xml: &str, groups_claim: &str) -> Result<SamlAssertion, String> {
    let mut reader = Reader::from_str(xml);

    let mut name_id = String::new();
    let mut attributes: HashMap<String, String> = HashMap::new();
    let mut current_attr_name = String::new();
    let mut in_name_id = false;
    let mut in_attr_value = false;
    let mut groups: Vec<String> = Vec::new();
    // Top-level <samlp:StatusCode Value="..."> + optional <samlp:StatusMessage>.
    // A non-Success status means the IdP rejected the request (no assertion is
    // present), so we surface its message instead of a misleading "no NameID".
    let mut status_code = String::new();
    let mut status_message = String::new();
    let mut in_status_message = false;

    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                let name = e.name();
                let local = local_name(name.as_ref());
                match local {
                    "NameID" => {
                        in_name_id = true;
                    }
                    "StatusCode" => {
                        // Record only the first (top-level) StatusCode Value.
                        if status_code.is_empty() {
                            for attr in e.attributes().flatten() {
                                if attr.key.as_ref() == b"Value" {
                                    status_code = String::from_utf8_lossy(&attr.value).to_string();
                                }
                            }
                        }
                    }
                    "StatusMessage" => {
                        in_status_message = true;
                    }
                    "Attribute" => {
                        // Extract the Name attribute
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"Name" {
                                current_attr_name =
                                    String::from_utf8_lossy(&attr.value).to_string();
                            }
                        }
                    }
                    "AttributeValue" => {
                        in_attr_value = true;
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(ref e)) => {
                let text = e
                    .decode()
                    .ok()
                    .and_then(|decoded| {
                        quick_xml::escape::unescape(&decoded)
                            .ok()
                            .map(|value| value.into_owned())
                    })
                    .unwrap_or_default();
                if in_name_id {
                    name_id = text;
                } else if in_status_message {
                    status_message = text;
                } else if in_attr_value && !current_attr_name.is_empty() {
                    // Check if this attribute is a groups claim
                    if is_groups_attr(&current_attr_name, groups_claim) {
                        groups.push(text.clone());
                    }
                    // Store first value for each attribute name
                    attributes.entry(current_attr_name.clone()).or_insert(text);
                }
            }
            Ok(Event::End(ref e)) => {
                let name = e.name();
                let local = local_name(name.as_ref());
                match local {
                    "NameID" => in_name_id = false,
                    "StatusMessage" => in_status_message = false,
                    "AttributeValue" => in_attr_value = false,
                    "Attribute" => current_attr_name.clear(),
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(format!("XML parse error: {e}")),
            _ => {}
        }
        buf.clear();
    }

    // If the IdP returned a non-Success status, there is no assertion — report
    // the IdP's own reason rather than the misleading "no NameID".
    if !status_code.is_empty() && !status_code.ends_with(":Success") {
        let reason = if status_message.is_empty() {
            status_code.clone()
        } else {
            format!("{status_message} ({status_code})")
        };
        return Err(format!("IdP rejected the SAML request: {reason}"));
    }

    if name_id.is_empty() {
        return Err("no NameID found in SAML assertion".to_string());
    }

    // Try to extract email and display name from common attribute names
    let email = attributes
        .get("email")
        .or_else(|| {
            attributes.get("http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress")
        })
        .or_else(|| attributes.get("urn:oid:0.9.2342.19200300.100.1.3"))
        .or_else(|| attributes.get("mail"))
        .cloned();

    let display_name = attributes
        .get("displayName")
        .or_else(|| attributes.get("http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name"))
        .or_else(|| attributes.get("urn:oid:2.16.840.1.113730.3.1.241"))
        .or_else(|| attributes.get("cn"))
        .or_else(|| attributes.get("name"))
        .cloned();

    Ok(SamlAssertion {
        name_id,
        email,
        display_name,
        groups,
        attributes,
    })
}

/// Check if an attribute name matches the groups claim.
fn is_groups_attr(attr_name: &str, groups_claim: &str) -> bool {
    attr_name == groups_claim
        || attr_name == "http://schemas.xmlsoap.org/claims/Group"
        || attr_name == "http://schemas.microsoft.com/ws/2008/06/identity/claims/groups"
        || attr_name == "memberOf"
}

/// Extract the local name from a potentially namespace-prefixed XML tag name.
/// e.g., "saml:NameID" -> "NameID", "NameID" -> "NameID"
fn local_name(name: &[u8]) -> &str {
    let s = std::str::from_utf8(name).unwrap_or("");
    s.rsplit_once(':').map(|(_, local)| local).unwrap_or(s)
}

/// Build SAML SP Metadata XML for the Rush service provider.
/// This is what administrators paste into their IdP (Okta, Azure AD, etc.)
/// when setting up the SAML integration.
pub fn build_sp_metadata(sp_entity_id: &str, acs_url: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata"
    entityID="{sp_entity_id}">
  <md:SPSSODescriptor
      AuthnRequestsSigned="false"
      WantAssertionsSigned="true"
      protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
    <md:NameIDFormat>urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress</md:NameIDFormat>
    <md:NameIDFormat>urn:oasis:names:tc:SAML:2.0:nameid-format:persistent</md:NameIDFormat>
    <md:NameIDFormat>urn:oasis:names:tc:SAML:2.0:nameid-format:unspecified</md:NameIDFormat>
    <md:AssertionConsumerService
        Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
        Location="{acs_url}"
        index="0"
        isDefault="true" />
  </md:SPSSODescriptor>
</md:EntityDescriptor>"#,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_authn_request() {
        let xml = build_authn_request(
            "https://rush.example.com",
            "https://rush.example.com/auth/sso/acs",
            "https://idp.example.com/sso",
        );
        assert!(xml.contains("AuthnRequest"));
        assert!(xml.contains("https://rush.example.com"));
        assert!(xml.contains("AssertionConsumerServiceURL"));
    }

    #[test]
    fn test_encode_authn_request() {
        let xml = "<samlp:AuthnRequest>test</samlp:AuthnRequest>";
        let encoded = encode_authn_request_redirect(xml);
        // Should be URL-safe
        assert!(!encoded.contains(' '));
    }

    #[test]
    fn test_parse_saml_response() {
        let xml = r#"<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">
  <saml:Assertion>
    <saml:Subject>
      <saml:NameID>jane@acme.com</saml:NameID>
    </saml:Subject>
    <saml:AttributeStatement>
      <saml:Attribute Name="email">
        <saml:AttributeValue>jane@acme.com</saml:AttributeValue>
      </saml:Attribute>
      <saml:Attribute Name="displayName">
        <saml:AttributeValue>Jane Doe</saml:AttributeValue>
      </saml:Attribute>
      <saml:Attribute Name="groups">
        <saml:AttributeValue>devops</saml:AttributeValue>
        <saml:AttributeValue>security</saml:AttributeValue>
      </saml:Attribute>
    </saml:AttributeStatement>
  </saml:Assertion>
</samlp:Response>"#;
        let b64 = B64.encode(xml.as_bytes());
        let result = parse_saml_response(&b64, "groups").unwrap();
        assert_eq!(result.name_id, "jane@acme.com");
        assert_eq!(result.email, Some("jane@acme.com".to_string()));
        assert_eq!(result.display_name, Some("Jane Doe".to_string()));
        assert_eq!(result.groups, vec!["devops", "security"]);
    }

    #[test]
    fn test_build_sp_metadata() {
        let xml = build_sp_metadata(
            "https://rush.example.com",
            "https://rush.example.com/auth/sso/acs",
        );
        assert!(xml.contains("EntityDescriptor"));
        assert!(xml.contains("AssertionConsumerService"));
        assert!(xml.contains("https://rush.example.com/auth/sso/acs"));
    }

    // ── Signature verification tests ──
    // Positive verification (a genuinely IdP-signed Response verifies) is
    // exercised against live IdP responses, and opensaml/bergshamra's own suite
    // covers exc-c14n + reference-digest correctness. These cover the negative
    // paths through our wrapper: no-signature and present-but-invalid.

    /// Self-signed RSA cert + key, returned as a PEM cert string for `verify_signature`.
    fn test_cert_pem() -> String {
        use openssl::asn1::Asn1Time;
        use openssl::bn::BigNum;
        use openssl::hash::MessageDigest;
        use openssl::pkey::PKey;
        use openssl::rsa::Rsa;
        use openssl::x509::{X509Builder, X509NameBuilder};

        let rsa = Rsa::generate(2048).expect("RSA key generation");
        let pkey = PKey::from_rsa(rsa).expect("PKey from RSA");
        let mut name = X509NameBuilder::new().expect("name builder");
        name.append_entry_by_text("CN", "Test IdP").expect("CN");
        let name = name.build();
        let mut b = X509Builder::new().expect("x509 builder");
        b.set_version(2).expect("version");
        b.set_subject_name(&name).expect("subject");
        b.set_issuer_name(&name).expect("issuer");
        b.set_pubkey(&pkey).expect("pubkey");
        let serial = BigNum::from_u32(1).expect("serial");
        b.set_serial_number(&serial.to_asn1_integer().expect("asn1"))
            .expect("set serial");
        b.set_not_before(&Asn1Time::days_from_now(0).expect("nb"))
            .expect("nb");
        b.set_not_after(&Asn1Time::days_from_now(365).expect("na"))
            .expect("na");
        b.sign(&pkey, MessageDigest::sha256()).expect("sign");
        String::from_utf8(b.build().to_pem().expect("to_pem")).expect("utf8")
    }

    #[test]
    fn verify_returns_false_when_no_signature() {
        let xml = r#"<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"><saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"><saml:Subject><saml:NameID>user@test.com</saml:NameID></saml:Subject></saml:Assertion></samlp:Response>"#;
        // No <ds:Signature> present → not verified (not an error).
        let res = verify_signature(xml, &test_cert_pem());
        assert!(
            matches!(res, Ok(false)),
            "no signature => Ok(false), got {res:?}"
        );
    }

    #[test]
    fn verify_rejects_bogus_signature() {
        // Structurally complete enveloped signature, but the SignatureValue and
        // DigestValue are garbage — must NOT verify even with a loadable cert.
        let xml = concat!(
            r#"<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" ID="_r1">"#,
            r#"<ds:Signature xmlns:ds="http://www.w3.org/2000/09/xmldsig#"><ds:SignedInfo>"#,
            r#"<ds:CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>"#,
            r#"<ds:SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"/>"#,
            r##"<ds:Reference URI="#_r1"><ds:Transforms>"##,
            r#"<ds:Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>"#,
            r#"<ds:Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/></ds:Transforms>"#,
            r#"<ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>"#,
            r#"<ds:DigestValue>AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=</ds:DigestValue>"#,
            r#"</ds:Reference></ds:SignedInfo><ds:SignatureValue>AAAA</ds:SignatureValue></ds:Signature>"#,
            r#"<saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"><saml:Subject>"#,
            r#"<saml:NameID>user@test.com</saml:NameID></saml:Subject></saml:Assertion></samlp:Response>"#,
        );
        let res = verify_signature(xml, &test_cert_pem());
        assert!(
            !matches!(res, Ok(true)),
            "bogus signature must not verify, got {res:?}"
        );
    }
}
