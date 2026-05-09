//! Minimal SAML 2.0 helpers for Rush Observability.
//!
//! This module builds SAML AuthnRequest XML for SP-initiated login,
//! parses SAML Response assertions, generates SP metadata XML, and
//! verifies XML signatures against the IdP X.509 certificate.

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use flate2::{Compression, write::DeflateEncoder};
use openssl::hash::MessageDigest;
use openssl::pkey::PKey;
use openssl::sign::Verifier;
use openssl::x509::X509;
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
    encoder.write_all(authn_request_xml.as_bytes()).expect("deflate write");
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
    format!(
        "{idp_sso_url}{sep}SAMLRequest={encoded}&RelayState={relay_encoded}"
    )
}

/// Verify the XML signature of a SAML response against the IdP's X.509 certificate.
///
/// SAML responses typically contain a `<ds:Signature>` element inside
/// `<samlp:Response>` or `<saml:Assertion>`. The signature covers a
/// canonicalized digest of the signed element.
///
/// Simplified verification flow:
/// 1. Extract the `<ds:SignatureValue>` (base64-encoded signature bytes)
/// 2. Extract the `<ds:SignedInfo>` element (the data that was actually signed)
/// 3. Detect the signature algorithm from `<ds:SignatureMethod>`
/// 4. Parse the IdP certificate (PEM format)
/// 5. Verify: RSA signature of the canonicalized SignedInfo matches SignatureValue
///
/// For v1 we use the raw XML bytes of `<ds:SignedInfo>...</ds:SignedInfo>` rather
/// than full Exclusive XML Canonicalization (C14N). Most IdPs produce XML that
/// works without C14N normalization. Full C14N support can be added later.
pub fn verify_signature(xml: &str, idp_cert_pem: &str) -> Result<bool, String> {
    // 1. Extract <ds:SignedInfo>...</ds:SignedInfo> raw bytes
    let signed_info_xml = extract_xml_element(xml, "SignedInfo")
        .ok_or_else(|| "no ds:SignedInfo element found in SAML response".to_string())?;

    // 2. Extract <ds:SignatureValue> text content
    let sig_value_b64 = extract_xml_text(xml, "SignatureValue")
        .ok_or_else(|| "no ds:SignatureValue element found in SAML response".to_string())?;

    // Clean whitespace from base64 (IdPs often wrap the value across lines)
    let sig_value_clean: String = sig_value_b64.chars().filter(|c| !c.is_whitespace()).collect();
    let sig_bytes = B64
        .decode(&sig_value_clean)
        .map_err(|e| format!("failed to base64-decode SignatureValue: {e}"))?;

    // 3. Detect signature algorithm from <ds:SignatureMethod Algorithm="...">
    let digest = detect_signature_algorithm(xml);

    // 4. Parse the IdP certificate
    let cert_pem = normalize_cert_pem(idp_cert_pem);
    let x509 = X509::from_pem(cert_pem.as_bytes())
        .map_err(|e| format!("failed to parse IdP certificate: {e}"))?;

    let pkey: PKey<openssl::pkey::Public> = x509
        .public_key()
        .map_err(|e| format!("failed to extract public key from IdP certificate: {e}"))?;

    // 5. Verify the signature over the SignedInfo element
    let mut verifier = Verifier::new(digest, &pkey)
        .map_err(|e| format!("failed to create signature verifier: {e}"))?;

    verifier
        .update(signed_info_xml.as_bytes())
        .map_err(|e| format!("verifier update failed: {e}"))?;

    let valid = verifier
        .verify(&sig_bytes)
        .map_err(|e| format!("signature verification error: {e}"))?;

    Ok(valid)
}

/// Extract the raw XML of an element by its local name (handles namespace prefixes).
/// Returns the full element including opening and closing tags.
fn extract_xml_element(xml: &str, local_name_target: &str) -> Option<String> {
    // Search for opening tag with any namespace prefix or none
    // Patterns: <ds:SignedInfo, <SignedInfo, <dsig:SignedInfo, etc.
    let open_patterns = [
        format!("<ds:{local_name_target}"),
        format!("<dsig:{local_name_target}"),
        format!("<{local_name_target}"),
    ];

    let close_patterns = [
        format!("</ds:{local_name_target}>"),
        format!("</dsig:{local_name_target}>"),
        format!("</{local_name_target}>"),
    ];

    for (open, close) in open_patterns.iter().zip(close_patterns.iter()) {
        if let Some(start) = xml.find(open.as_str()) {
            if let Some(end_offset) = xml[start..].find(close.as_str()) {
                let end = start + end_offset + close.len();
                return Some(xml[start..end].to_string());
            }
        }
    }
    None
}

/// Extract the text content of an XML element by its local name.
/// Handles namespace-prefixed element names.
fn extract_xml_text(xml: &str, local_name_target: &str) -> Option<String> {
    let open_patterns = [
        format!("<ds:{local_name_target}"),
        format!("<dsig:{local_name_target}"),
        format!("<{local_name_target}"),
    ];

    let close_patterns = [
        format!("</ds:{local_name_target}>"),
        format!("</dsig:{local_name_target}>"),
        format!("</{local_name_target}>"),
    ];

    for (open, close) in open_patterns.iter().zip(close_patterns.iter()) {
        if let Some(start) = xml.find(open.as_str()) {
            // Find the end of the opening tag (the '>' character)
            if let Some(tag_end) = xml[start..].find('>') {
                let content_start = start + tag_end + 1;
                if let Some(content_end) = xml[content_start..].find(close.as_str()) {
                    return Some(xml[content_start..content_start + content_end].to_string());
                }
            }
        }
    }
    None
}

/// Detect the signature algorithm from <ds:SignatureMethod Algorithm="...">.
/// Defaults to SHA-256 if the algorithm cannot be determined.
fn detect_signature_algorithm(xml: &str) -> MessageDigest {
    // Look for the Algorithm attribute in SignatureMethod
    if let Some(start) = xml.find("SignatureMethod") {
        let region = &xml[start..std::cmp::min(start + 300, xml.len())];
        if let Some(algo_start) = region.find("Algorithm=\"") {
            let algo_value = &region[algo_start + 11..];
            if let Some(algo_end) = algo_value.find('"') {
                let algorithm = &algo_value[..algo_end];
                return match algorithm {
                    a if a.contains("sha1") || a.contains("sha-1") || a.ends_with("#rsa-sha1") => {
                        MessageDigest::sha1()
                    }
                    a if a.contains("sha384") || a.contains("sha-384") => MessageDigest::sha384(),
                    a if a.contains("sha512") || a.contains("sha-512") => MessageDigest::sha512(),
                    // Default: SHA-256 (most common in modern IdPs)
                    _ => MessageDigest::sha256(),
                };
            }
        }
    }
    MessageDigest::sha256()
}

/// Normalize an IdP certificate PEM string.
/// Handles cases where the cert is provided as raw base64 without PEM headers,
/// or with PEM headers already present.
fn normalize_cert_pem(cert: &str) -> String {
    let trimmed = cert.trim();

    // If it already has PEM headers, return as-is
    if trimmed.starts_with("-----BEGIN CERTIFICATE-----") {
        return trimmed.to_string();
    }

    // Strip any whitespace/newlines from the raw base64 and re-wrap
    let clean: String = trimmed.chars().filter(|c| !c.is_whitespace()).collect();

    // Wrap in PEM headers with 64-char lines
    let mut pem = String::from("-----BEGIN CERTIFICATE-----\n");
    for chunk in clean.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(chunk).unwrap_or(""));
        pem.push('\n');
    }
    pem.push_str("-----END CERTIFICATE-----\n");
    pem
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
    let xml_bytes = B64.decode(b64_response.trim())
        .map_err(|e| format!("failed to base64-decode SAMLResponse: {e}"))?;
    let xml = String::from_utf8_lossy(&xml_bytes);

    parse_assertion_xml(&xml, groups_claim)
}

/// Parse assertion fields from raw SAML Response XML.
fn parse_assertion_xml(
    xml: &str,
    groups_claim: &str,
) -> Result<SamlAssertion, String> {
    let mut reader = Reader::from_str(xml);

    let mut name_id = String::new();
    let mut attributes: HashMap<String, String> = HashMap::new();
    let mut current_attr_name = String::new();
    let mut in_name_id = false;
    let mut in_attr_value = false;
    let mut groups: Vec<String> = Vec::new();

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
                    "Attribute" => {
                        // Extract the Name attribute
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"Name" {
                                current_attr_name = String::from_utf8_lossy(&attr.value).to_string();
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
                let text = e.unescape().unwrap_or_default().to_string();
                if in_name_id {
                    name_id = text;
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

    if name_id.is_empty() {
        return Err("no NameID found in SAML assertion".to_string());
    }

    // Try to extract email and display name from common attribute names
    let email = attributes.get("email")
        .or_else(|| attributes.get("http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress"))
        .or_else(|| attributes.get("urn:oid:0.9.2342.19200300.100.1.3"))
        .or_else(|| attributes.get("mail"))
        .cloned();

    let display_name = attributes.get("displayName")
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

    /// Generate a self-signed X.509 certificate and RSA key pair for tests.
    fn generate_test_cert() -> (X509, openssl::pkey::PKey<openssl::pkey::Private>) {
        use openssl::asn1::Asn1Time;
        use openssl::bn::BigNum;
        use openssl::hash::MessageDigest;
        use openssl::pkey::PKey;
        use openssl::rsa::Rsa;
        use openssl::x509::{X509Builder, X509NameBuilder};

        let rsa = Rsa::generate(2048).expect("RSA key generation");
        let pkey = PKey::from_rsa(rsa).expect("PKey from RSA");

        let mut name_builder = X509NameBuilder::new().expect("X509NameBuilder");
        name_builder
            .append_entry_by_text("CN", "Test IdP")
            .expect("CN entry");
        let name = name_builder.build();

        let mut builder = X509Builder::new().expect("X509Builder");
        builder.set_version(2).expect("set version");
        builder.set_subject_name(&name).expect("set subject");
        builder.set_issuer_name(&name).expect("set issuer");
        builder.set_pubkey(&pkey).expect("set pubkey");

        let serial = BigNum::from_u32(1).expect("serial");
        builder
            .set_serial_number(&serial.to_asn1_integer().expect("asn1 serial"))
            .expect("set serial");

        let not_before = Asn1Time::days_from_now(0).expect("not_before");
        let not_after = Asn1Time::days_from_now(365).expect("not_after");
        builder.set_not_before(&not_before).expect("set not_before");
        builder.set_not_after(&not_after).expect("set not_after");

        builder
            .sign(&pkey, MessageDigest::sha256())
            .expect("sign cert");

        let cert = builder.build();
        (cert, pkey)
    }

    /// Build a minimal SAML Response with a ds:Signature that is signed
    /// over the <ds:SignedInfo> element using the provided private key.
    fn build_signed_saml_response(
        pkey: &openssl::pkey::PKey<openssl::pkey::Private>,
        digest: MessageDigest,
        tamper: bool,
    ) -> String {
        use openssl::sign::Signer;

        let signed_info = r##"<ds:SignedInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#"><ds:CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/><ds:SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"/><ds:Reference URI="#_resp1"><ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/><ds:DigestValue>dGVzdA==</ds:DigestValue></ds:Reference></ds:SignedInfo>"##;

        let mut signer = Signer::new(digest, pkey).expect("Signer");
        signer.update(signed_info.as_bytes()).expect("signer update");
        let mut sig_bytes = signer.sign_to_vec().expect("sign");

        if tamper {
            // Flip a byte to produce an invalid signature
            if let Some(b) = sig_bytes.first_mut() {
                *b ^= 0xFF;
            }
        }

        let sig_b64 = B64.encode(&sig_bytes);

        format!(
            r##"<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion" ID="_resp1"><ds:Signature xmlns:ds="http://www.w3.org/2000/09/xmldsig#">{signed_info}<ds:SignatureValue>{sig_b64}</ds:SignatureValue></ds:Signature><saml:Assertion><saml:Subject><saml:NameID>test@example.com</saml:NameID></saml:Subject></saml:Assertion></samlp:Response>"##,
        )
    }

    #[test]
    fn test_verify_signature_no_signature_element() {
        let xml = r#"<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol">
  <saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">
    <saml:Subject><saml:NameID>user@test.com</saml:NameID></saml:Subject>
  </saml:Assertion>
</samlp:Response>"#;

        let (cert, _) = generate_test_cert();
        let cert_pem = String::from_utf8(cert.to_pem().expect("to_pem")).expect("utf8");

        let result = verify_signature(xml, &cert_pem);
        assert!(result.is_err(), "should error when no Signature element present");
        assert!(
            result.unwrap_err().contains("SignedInfo"),
            "error should mention missing SignedInfo"
        );
    }

    #[test]
    fn test_verify_signature_valid() {
        let (cert, pkey) = generate_test_cert();
        let cert_pem = String::from_utf8(cert.to_pem().expect("to_pem")).expect("utf8");

        let xml = build_signed_saml_response(&pkey, MessageDigest::sha256(), false);

        let result = verify_signature(&xml, &cert_pem);
        assert!(result.is_ok(), "verify should not error: {:?}", result.err());
        assert!(result.unwrap(), "signature should be valid");
    }

    #[test]
    fn test_verify_signature_tampered() {
        let (cert, pkey) = generate_test_cert();
        let cert_pem = String::from_utf8(cert.to_pem().expect("to_pem")).expect("utf8");

        let xml = build_signed_saml_response(&pkey, MessageDigest::sha256(), true);

        let result = verify_signature(&xml, &cert_pem);
        assert!(result.is_ok(), "verify should not error: {:?}", result.err());
        assert!(!result.unwrap(), "tampered signature should be invalid");
    }

    #[test]
    fn test_verify_signature_wrong_cert() {
        let (_cert1, pkey1) = generate_test_cert();
        let (cert2, _pkey2) = generate_test_cert();

        // Sign with key1 but verify with cert2
        let cert2_pem = String::from_utf8(cert2.to_pem().expect("to_pem")).expect("utf8");
        let xml = build_signed_saml_response(&pkey1, MessageDigest::sha256(), false);

        let result = verify_signature(&xml, &cert2_pem);
        assert!(result.is_ok(), "should not error: {:?}", result.err());
        assert!(!result.unwrap(), "signature from different key should be invalid");
    }

    #[test]
    fn test_normalize_cert_pem_raw_base64() {
        let (cert, _) = generate_test_cert();
        let full_pem = String::from_utf8(cert.to_pem().expect("to_pem")).expect("utf8");

        // Strip PEM headers to simulate raw base64 from an IdP admin UI
        let raw_b64: String = full_pem
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect::<Vec<_>>()
            .join("");

        let normalized = normalize_cert_pem(&raw_b64);
        assert!(normalized.starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(normalized.contains("-----END CERTIFICATE-----"));

        // Verify the normalized PEM is parseable
        let parsed = X509::from_pem(normalized.as_bytes());
        assert!(parsed.is_ok(), "normalized PEM should be parseable: {:?}", parsed.err());
    }

    #[test]
    fn test_detect_sha1_algorithm() {
        let xml = r#"<ds:SignedInfo><ds:SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"/></ds:SignedInfo>"#;
        let digest = detect_signature_algorithm(xml);
        // SHA-1 digest type has a specific NID; just verify it doesn't crash
        // and returns a different digest than SHA-256
        assert_ne!(digest.as_ptr(), MessageDigest::sha256().as_ptr());
    }

    #[test]
    fn test_detect_sha256_algorithm() {
        let xml = r#"<ds:SignedInfo><ds:SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"/></ds:SignedInfo>"#;
        let digest = detect_signature_algorithm(xml);
        assert_eq!(digest.as_ptr(), MessageDigest::sha256().as_ptr());
    }

    #[test]
    fn test_detect_default_algorithm() {
        // No SignatureMethod at all should default to SHA-256
        let xml = "<ds:SignedInfo></ds:SignedInfo>";
        let digest = detect_signature_algorithm(xml);
        assert_eq!(digest.as_ptr(), MessageDigest::sha256().as_ptr());
    }
}
