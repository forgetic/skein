//! The shell's bounded trust bytes composed with TLS's own parser (tls.md, 8).

use std::fs;

use skein_scratch::Scratch;
use skein_shell::{RootBounds, Trust, read_trust_roots};
use skein_tls::{Config, Parsed, Refusal};
use skein_tls_world::pki;

fn certificate(bytes: &[u8]) -> Vec<u8> {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut text = Vec::from(b"-----BEGIN CERTIFICATE-----\n".as_slice());
    for part in bytes.chunks(3) {
        let a = part[0];
        let b = part.get(1).copied().unwrap_or(0);
        let c = part.get(2).copied().unwrap_or(0);
        text.push(ALPHABET[usize::from(a >> 2)]);
        text.push(ALPHABET[usize::from(((a & 3) << 4) | (b >> 4))]);
        text.push(if part.len() > 1 { ALPHABET[usize::from(((b & 15) << 2) | (c >> 6))] } else { b'=' });
        text.push(if part.len() > 2 { ALPHABET[usize::from(c & 63)] } else { b'=' });
    }
    text.extend_from_slice(b"\n-----END CERTIFICATE-----\n");
    text
}

#[test]
fn fixture_roots_read_at_startup_share_tls_configuration_and_report_all_skip_counts() {
    let scratch = Scratch::new("tls-startup");
    let path = scratch.path().join("roots.pem");
    let mut text = certificate(pki::ROOT);
    text.extend_from_slice(&certificate(b"well-framed garbage DER"));
    text.extend_from_slice(b"-----BEGIN CERTIFICATE-----\ninvalid!\n-----END CERTIFICATE-----\n");
    text.extend_from_slice(b"-----BEGIN PRIVATE KEY-----\nnever decoded!\n-----END PRIVATE KEY-----\n");
    fs::write(&path, &text).expect("plant fixture bundle");
    let bounds = RootBounds {
        bundle_bytes: u32::try_from(text.len()).expect("bundle fits"),
        certificates: 2,
        certificate_bytes: u32::try_from(pki::ROOT.len()).expect("root fits"),
    };
    let roots = read_trust_roots(&Trust::Machine { ssl_cert_file: Some(path) }, &bounds).expect("bounded PEM roots");
    assert_eq!((roots.malformed, roots.other_blocks), (1, 1));
    let (_, parsed) = Config::from_der(&roots.certificates, &[b"h2"]).expect("actual fixture root remains");
    assert_eq!(parsed, Parsed { taken: 1, skipped: 1 });
    let roots = read_trust_roots(&Trust::Der("fixtures/root.der".into()), &bounds).expect("actual DER file");
    let (_, parsed) = Config::from_der(&roots.certificates, &[]).expect("DER root remains");
    assert_eq!(parsed, Parsed { taken: 1, skipped: 0 });
    fs::write(scratch.path().join("empty.pem"), b"no certificate blocks").expect("empty roots bundle");
    let roots = read_trust_roots(&Trust::Machine { ssl_cert_file: Some(scratch.path().join("empty.pem")) }, &bounds)
        .expect("shell passes empty roots to TLS");
    assert_eq!(Config::from_der(&roots.certificates, &[]).expect_err("TLS refuses no roots"), Refusal::Roots);
}
