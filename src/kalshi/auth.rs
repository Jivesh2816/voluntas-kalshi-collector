//! Kalshi API-key request signing.
//!
//! Every authenticated request (including the WebSocket handshake) carries:
//!
//! * `KALSHI-ACCESS-KEY`: the key ID;
//! * `KALSHI-ACCESS-TIMESTAMP`: Unix time in milliseconds;
//! * `KALSHI-ACCESS-SIGNATURE`: base64 signature of
//!   `timestamp + METHOD + path` (path without query string).
//!
//! Ed25519 keys sign that text directly; RSA keys use RSA-PSS with SHA-256 and
//! a salt as long as the digest. Kalshi issues Ed25519 keys as PKCS#8
//! (`BEGIN PRIVATE KEY`) and RSA keys as PKCS#1 (`BEGIN RSA PRIVATE KEY`), but
//! RSA keys can also be PKCS#8, so the type is detected from the parsed key.

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, RSA_PSS_SHA256, RsaKeyPair};

pub const KEY_ID_ENV: &str = "KALSHI_API_KEY_ID";
pub const KEY_PATH_ENV: &str = "KALSHI_PRIVATE_KEY_PATH";

enum Key {
    Ed25519(Ed25519KeyPair),
    Rsa(RsaKeyPair),
}

pub struct Signer {
    key_id: String,
    key: Key,
    rng: SystemRandom,
}

impl std::fmt::Debug for Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print key material.
        f.debug_struct("Signer")
            .field("key_id", &self.key_id)
            .field("kind", &self.kind())
            .finish()
    }
}

impl Signer {
    pub fn from_pem(key_id: impl Into<String>, pem: &str) -> anyhow::Result<Self> {
        let (label, der) = decode_pem(pem)?;
        let key = match label.as_str() {
            "PRIVATE KEY" => {
                if let Ok(key) = Ed25519KeyPair::from_pkcs8_maybe_unchecked(&der) {
                    Key::Ed25519(key)
                } else {
                    Key::Rsa(RsaKeyPair::from_pkcs8(&der).map_err(|e| {
                        anyhow::anyhow!("PKCS#8 key is neither Ed25519 nor usable RSA: {e}")
                    })?)
                }
            }
            "RSA PRIVATE KEY" => Key::Rsa(
                RsaKeyPair::from_der(&der)
                    .map_err(|e| anyhow::anyhow!("invalid PKCS#1 RSA key: {e}"))?,
            ),
            "ENCRYPTED PRIVATE KEY" => bail!("encrypted private keys are not supported"),
            other => bail!("unsupported PEM block {other:?}"),
        };
        Ok(Self {
            key_id: key_id.into(),
            key,
            rng: SystemRandom::new(),
        })
    }

    /// Loads credentials from `KALSHI_API_KEY_ID` and the PEM file named by
    /// `KALSHI_PRIVATE_KEY_PATH`. Returns `Ok(None)` if neither is set.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let key_id = std::env::var(KEY_ID_ENV).ok().filter(|s| !s.is_empty());
        let key_path = std::env::var(KEY_PATH_ENV).ok().filter(|s| !s.is_empty());
        match (key_id, key_path) {
            (None, None) => Ok(None),
            (Some(key_id), Some(path)) => {
                let pem = std::fs::read_to_string(&path)
                    .with_context(|| format!("reading {KEY_PATH_ENV}={path}"))?;
                Self::from_pem(key_id, &pem).map(Some)
            }
            _ => bail!("set both {KEY_ID_ENV} and {KEY_PATH_ENV}, or neither"),
        }
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn kind(&self) -> &'static str {
        match self.key {
            Key::Ed25519(_) => "ed25519",
            Key::Rsa(_) => "rsa-pss",
        }
    }

    /// Base64 signature of `timestamp_ms + method + path`. `path` must not
    /// include a query string.
    pub fn sign(&self, timestamp_ms: i64, method: &str, path: &str) -> anyhow::Result<String> {
        let message = format!("{timestamp_ms}{method}{path}");
        let signature = match &self.key {
            Key::Ed25519(key) => key.sign(message.as_bytes()).as_ref().to_vec(),
            Key::Rsa(key) => {
                let mut sig = vec![0; key.public().modulus_len()];
                key.sign(&RSA_PSS_SHA256, &self.rng, message.as_bytes(), &mut sig)
                    .map_err(|_| anyhow::anyhow!("RSA signing failed"))?;
                sig
            }
        };
        Ok(BASE64.encode(signature))
    }

    /// The three authentication headers for a request made at `timestamp_ms`.
    pub fn headers(
        &self,
        timestamp_ms: i64,
        method: &str,
        path: &str,
    ) -> anyhow::Result<[(&'static str, String); 3]> {
        Ok([
            ("KALSHI-ACCESS-KEY", self.key_id.clone()),
            (
                "KALSHI-ACCESS-SIGNATURE",
                self.sign(timestamp_ms, method, path)?,
            ),
            ("KALSHI-ACCESS-TIMESTAMP", timestamp_ms.to_string()),
        ])
    }

    #[cfg(test)]
    fn public_key(&self) -> Vec<u8> {
        use ring::signature::KeyPair;
        match &self.key {
            Key::Ed25519(key) => key.public_key().as_ref().to_vec(),
            Key::Rsa(key) => key.public().as_ref().to_vec(),
        }
    }
}

/// Decodes the first PEM block, returning its label and DER bytes.
fn decode_pem(pem: &str) -> anyhow::Result<(String, Vec<u8>)> {
    let mut lines = pem
        .lines()
        .map(str::trim)
        .skip_while(|l| !l.starts_with("-----BEGIN "));
    let label = lines
        .next()
        .and_then(|l| l.strip_prefix("-----BEGIN "))
        .and_then(|l| l.strip_suffix("-----"))
        .context("no PEM BEGIN line")?
        .to_owned();
    let end = format!("-----END {label}-----");
    let mut body = String::new();
    for line in lines.by_ref() {
        if line == end {
            let der = BASE64.decode(body).context("invalid base64 in PEM body")?;
            return Ok((label, der));
        }
        if line.contains(':') {
            bail!("PEM headers (encrypted legacy keys) are not supported");
        }
        body.push_str(line);
    }
    bail!("no PEM END line for {label:?}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{ED25519, RSA_PSS_2048_8192_SHA256, UnparsedPublicKey};

    fn to_pem(label: &str, der: &[u8]) -> String {
        let b64 = BASE64.encode(der);
        let body: Vec<_> = b64
            .as_bytes()
            .chunks(64)
            .map(|c| std::str::from_utf8(c).unwrap())
            .collect();
        format!(
            "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
            body.join("\n")
        )
    }

    #[test]
    fn ed25519_signature_verifies_and_signs_the_documented_text() {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let signer = Signer::from_pem("key-id", &to_pem("PRIVATE KEY", pkcs8.as_ref())).unwrap();
        assert_eq!(signer.kind(), "ed25519");

        let headers = signer
            .headers(1_700_000_000_123, "GET", "/trade-api/ws/v2")
            .unwrap();
        assert_eq!(headers[0], ("KALSHI-ACCESS-KEY", "key-id".to_owned()));
        assert_eq!(
            headers[2],
            ("KALSHI-ACCESS-TIMESTAMP", "1700000000123".to_owned())
        );
        let sig = BASE64.decode(&headers[1].1).unwrap();
        UnparsedPublicKey::new(&ED25519, signer.public_key())
            .verify(b"1700000000123GET/trade-api/ws/v2", &sig)
            .expect("signature must cover timestamp + method + path");
        assert!(
            UnparsedPublicKey::new(&ED25519, signer.public_key())
                .verify(b"1700000000124GET/trade-api/ws/v2", &sig)
                .is_err()
        );
    }

    /// RSA keys can't be generated with `ring`; use `openssl` when available.
    #[test]
    fn rsa_pss_signatures_verify_for_pkcs1_and_pkcs8() {
        let dir = tempfile::tempdir().unwrap();
        let pkcs8 = dir.path().join("k8.pem");
        let pkcs1 = dir.path().join("k1.pem");
        let ok = std::process::Command::new("openssl")
            .args([
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
                "-out",
            ])
            .arg(&pkcs8)
            .output()
            .is_ok_and(|o| o.status.success())
            && std::process::Command::new("openssl")
                .args(["rsa", "-traditional", "-in"])
                .arg(&pkcs8)
                .arg("-out")
                .arg(&pkcs1)
                .output()
                .is_ok_and(|o| o.status.success());
        if !ok {
            eprintln!("openssl unavailable; skipping RSA test");
            return;
        }
        for path in [&pkcs8, &pkcs1] {
            let pem = std::fs::read_to_string(path).unwrap();
            let signer = Signer::from_pem("rsa", &pem).unwrap();
            assert_eq!(signer.kind(), "rsa-pss");
            let sig = BASE64
                .decode(signer.sign(42, "GET", "/p").unwrap())
                .unwrap();
            UnparsedPublicKey::new(&RSA_PSS_2048_8192_SHA256, signer.public_key())
                .verify(b"42GET/p", &sig)
                .unwrap();
        }
    }

    #[test]
    fn rejects_garbage_and_debug_hides_key() {
        assert!(Signer::from_pem("k", "not a pem").is_err());
        assert!(Signer::from_pem("k", &to_pem("PRIVATE KEY", b"junk")).is_err());
        assert!(Signer::from_pem("k", &to_pem("CERTIFICATE", b"junk")).is_err());
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let signer =
            Signer::from_pem("visible-id", &to_pem("PRIVATE KEY", pkcs8.as_ref())).unwrap();
        assert_eq!(
            format!("{signer:?}"),
            r#"Signer { key_id: "visible-id", kind: "ed25519" }"#
        );
    }
}
