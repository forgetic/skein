//! The certificates the worlds use, kept in `fixtures/` and made by
//! `fixtures/make.sh`, and the configurations made of them: the client's,
//! which trusts the test root, and the server's, which presents a chain.
//!
//! Every certificate is valid from 2026-01-01 to 2036-01-01. A world checks
//! them at the wall time it chooses: [`VALID`], [`EXPIRED`] or [`EARLY`].

use std::sync::Arc;

use rustls::crypto::cipher::{MessageEncrypter, OutboundChunks, OutboundPlainMessage};
use rustls::crypto::{CryptoProvider, ring};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::ServerConfig;
use rustls::version::{TLS12, TLS13};
use rustls::{
    CipherSuiteCommon, ConnectionTrafficSecrets, ContentType, ProtocolVersion, RootCertStore, SupportedCipherSuite,
    SupportedProtocolVersion, Tls13CipherSuite,
};
use skein_lib::Wall;
use skein_tls::{Config, Name};

/// The root the client trusts.
pub const ROOT: &[u8] = include_bytes!("../fixtures/root.der");
/// The intermediate the root signed, which signed the server's certificates.
pub const INTERMEDIATE: &[u8] = include_bytes!("../fixtures/intermediate.der");
/// The server's certificate, for skein.test and 127.0.0.1.
pub const LEAF: &[u8] = include_bytes!("../fixtures/leaf.der");
/// The server's certificate for big.skein.test and 1,500 more names.
pub const BIG: &[u8] = include_bytes!("../fixtures/big.der");
/// A certificate for skein.test that a root no one trusts signed.
pub const OTHER: &[u8] = include_bytes!("../fixtures/other.der");
/// A certificate for skein.test that signed itself.
pub const SELF: &[u8] = include_bytes!("../fixtures/self.der");
/// A CA's certificate for skein.test, which the root signed.
pub const CA: &[u8] = include_bytes!("../fixtures/ca.der");
/// A certificate for skein.test, for client authentication only.
pub const CLIENT: &[u8] = include_bytes!("../fixtures/client.der");
/// The key of every one of them.
pub const KEY: &[u8] = include_bytes!("../fixtures/leaf.key");

/// 2030-01-01: every certificate is valid.
pub const VALID: Wall = Wall::from_nanos(1_893_456_000 * 1_000_000_000);
/// 2037-01-01: every certificate has expired.
pub const EXPIRED: Wall = Wall::from_nanos(2_114_380_800 * 1_000_000_000);
/// 2025-06-01: no certificate is valid yet.
pub const EARLY: Wall = Wall::from_nanos(1_748_736_000 * 1_000_000_000);

/// The roots the client trusts: the test root.
#[must_use]
pub fn roots() -> RootCertStore {
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(ROOT)).expect("the test root parses");
    roots
}

/// The client's configuration: it trusts the test root, and offers `alpn`.
#[must_use]
pub fn client(alpn: &[&[u8]]) -> Config {
    Config::new(roots(), alpn).expect("a configuration of the test root")
}

/// The suite of a server whose keys are taken out: TLS 1.2's, so that a
/// test can seal what TLS 1.3 forbids, a renegotiation request.
pub const EXTRACTABLE: SupportedCipherSuite = ring::cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256;

/// What seals records as a TLS 1.2 server would, with its keys taken out.
pub struct Sealer {
    encrypter: Box<dyn MessageEncrypter>,
    seq: u64,
}

impl std::fmt::Debug for Sealer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sealer").field("seq", &self.seq).finish_non_exhaustive()
    }
}

impl Sealer {
    /// A sealer of the direction `secrets` are for, from its next sequence
    /// number on.
    #[must_use]
    pub fn new(secrets: (u64, ConnectionTrafficSecrets)) -> Sealer {
        let (seq, ConnectionTrafficSecrets::Aes128Gcm { key, iv }) = secrets else { panic!("AES-128-GCM's keys") };
        let SupportedCipherSuite::Tls12(suite) = EXTRACTABLE else { panic!("a suite of TLS 1.2") };
        let iv = iv.as_ref();
        Sealer { encrypter: suite.aead_alg.encrypter(key, &iv[..4], &iv[4..]), seq }
    }

    /// `payload` sealed in a record of `typ`.
    pub fn seal(&mut self, typ: ContentType, payload: &[u8]) -> Vec<u8> {
        let message =
            OutboundPlainMessage { typ, version: ProtocolVersion::TLSv1_2, payload: OutboundChunks::Single(payload) };
        let record = self.encrypter.encrypt(message, self.seq).expect("sealed").encode();
        self.seq += 1;
        record
    }
}

/// The client's configuration as [`client`]'s, with TLS 1.3's AES-128-GCM
/// alone, its keys good for `records` records: rustls asks for new ones
/// itself as they reach their limit (RFC 8446, 5.5).
#[must_use]
pub fn short_lived(records: u64) -> Config {
    let SupportedCipherSuite::Tls13(suite) = ring::cipher_suite::TLS13_AES_128_GCM_SHA256 else {
        panic!("a suite of TLS 1.3")
    };
    let short = Box::leak(Box::new(Tls13CipherSuite {
        common: CipherSuiteCommon {
            suite: suite.common.suite,
            hash_provider: suite.common.hash_provider,
            confidentiality_limit: records,
        },
        hkdf_provider: suite.hkdf_provider,
        aead_alg: suite.aead_alg,
        quic: suite.quic,
    }));
    let provider =
        CryptoProvider { cipher_suites: vec![SupportedCipherSuite::Tls13(short)], ..ring::default_provider() };
    Config::with_provider(roots(), &[], provider).expect("a suite of TLS 1.3")
}

/// The server's name the certificates are for.
#[must_use]
pub fn name() -> Name {
    Name::new("skein.test").expect("a DNS name")
}

/// The chain a server presents.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Chain {
    /// The leaf and the intermediate: trusted for skein.test.
    Leaf,
    /// The big certificate and the intermediate: about 40 KB in all.
    Big,
    /// A certificate a root the client does not trust signed.
    Untrusted,
    /// A certificate for skein.test that signed itself.
    SelfSigned,
    /// A CA's certificate for skein.test, which the root signed: not a leaf.
    CaAsLeaf,
    /// A leaf for skein.test, the intermediate's, for client authentication
    /// only.
    ClientOnly,
    /// The leaf and the intermediate, then this many empty certificates: a
    /// hostile server's, which rustls decodes into an element each.
    Padded(usize),
}

/// The versions a server allows.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Versions {
    Both,
    Tls12,
    Tls13,
}

/// How a server is configured.
#[derive(Clone, Debug)]
pub struct Server {
    pub chain: Chain,
    pub versions: Versions,
    /// Whether it accepts only a key share the client does not send first,
    /// so that it asks for another with a `HelloRetryRequest`.
    pub retry: bool,
    /// The protocols it accepts by ALPN, in order of preference.
    pub alpn: Vec<Vec<u8>>,
    /// Whether its keys may be taken out once the handshake is done, to
    /// seal its records by hand: TLS 1.2's ECDHE-ECDSA with AES-128-GCM
    /// only, then.
    pub extractable: bool,
}

impl Server {
    /// A server of the leaf, either version, no retry, no ALPN.
    #[must_use]
    pub fn plain() -> Server {
        Server { chain: Chain::Leaf, versions: Versions::Both, retry: false, alpn: Vec::new(), extractable: false }
    }

    /// rustls's configuration for it.
    #[must_use]
    pub fn config(&self) -> Arc<ServerConfig> {
        let mut provider = ring::default_provider();
        if self.retry {
            provider = CryptoProvider { kx_groups: vec![ring::kx_group::SECP384R1], ..provider };
        }
        if self.extractable {
            provider = CryptoProvider { cipher_suites: vec![EXTRACTABLE], ..provider };
        }
        let versions: &[&SupportedProtocolVersion] = match self.versions {
            Versions::Both => &[&TLS13, &TLS12],
            Versions::Tls12 => &[&TLS12],
            Versions::Tls13 => &[&TLS13],
        };
        let chain = match self.chain {
            Chain::Leaf => vec![CertificateDer::from(LEAF), CertificateDer::from(INTERMEDIATE)],
            Chain::Big => vec![CertificateDer::from(BIG), CertificateDer::from(INTERMEDIATE)],
            Chain::Untrusted => vec![CertificateDer::from(OTHER)],
            Chain::SelfSigned => vec![CertificateDer::from(SELF)],
            Chain::CaAsLeaf => vec![CertificateDer::from(CA)],
            Chain::ClientOnly => vec![CertificateDer::from(CLIENT), CertificateDer::from(INTERMEDIATE)],
            Chain::Padded(empty) => {
                let mut chain = vec![CertificateDer::from(LEAF), CertificateDer::from(INTERMEDIATE)];
                chain.resize(empty + 2, CertificateDer::from(Vec::new()));
                chain
            }
        };
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(KEY));
        let mut config = ServerConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(versions)
            .expect("ring's provider has suites for each version")
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .expect("the key is the certificate's");
        config.alpn_protocols.clone_from(&self.alpn);
        config.enable_secret_extraction = self.extractable;
        Arc::new(config)
    }
}
