//! The certificates the worlds use, kept in `fixtures/` and made by
//! `fixtures/make.sh`, and the configurations made of them: the client's,
//! which trusts the test root, and the server's, which presents a chain.
//!
//! Every certificate is valid from 2026-01-01 to 2036-01-01. A world checks
//! them at the wall time it chooses: [`VALID`], [`EXPIRED`] or [`EARLY`].

use std::sync::Arc;

use rustls::crypto::{CryptoProvider, ring};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::ServerConfig;
use rustls::version::{TLS12, TLS13};
use rustls::{RootCertStore, SupportedProtocolVersion};
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
}

impl Server {
    /// A server of the leaf, either version, no retry, no ALPN.
    #[must_use]
    pub fn plain() -> Server {
        Server { chain: Chain::Leaf, versions: Versions::Both, retry: false, alpn: Vec::new() }
    }

    /// rustls's configuration for it.
    #[must_use]
    pub fn config(&self) -> Arc<ServerConfig> {
        let mut provider = ring::default_provider();
        if self.retry {
            provider = CryptoProvider { kx_groups: vec![ring::kx_group::SECP384R1], ..provider };
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
        };
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(KEY));
        let mut config = ServerConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(versions)
            .expect("ring's provider has suites for each version")
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .expect("the key is the certificate's");
        config.alpn_protocols.clone_from(&self.alpn);
        Arc::new(config)
    }
}
