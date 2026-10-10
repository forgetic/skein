//! What a service hands the TLS client as data (tls.md, 3.4; shell.md, 6):
//! the roots it trusts and the protocols it offers by ALPN, made once at
//! startup into a [`Config`] every connection shares, and each connection's
//! server name, a [`Name`].

#![expect(
    clippy::disallowed_types,
    reason = "rustls takes its configuration in an Arc that every connection shares, its time from a provider behind one, and its protocols in Vecs (tls.md, 4)"
)]

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::time::Duration;

use rustls::client::Resumption;
use rustls::crypto::{CryptoProvider, ring};
use rustls::pki_types::{CertificateDer, ServerName, TrustAnchor, UnixTime};
use rustls::time_provider::TimeProvider;
use rustls::{ClientConfig, RootCertStore};
use skein_lib::Wall;

/// The most bytes the protocols offered by ALPN take in a `ClientHello`, each
/// with its length (RFC 7301, 3.1): a list of many more names than a client
/// of HTTP/1.1 offers, short enough to bound the `ClientHello`
/// ([`FLIGHT`](crate::client::FLIGHT)).
pub const ALPN: usize = 256;

/// What every connection of a service shares, made at startup from its
/// configuration (shell.md, 6): the roots it trusts, the protocols it offers
/// by ALPN, and rustls's configuration built from them, with ring for its
/// cryptography.
///
/// Both TLS 1.2 and 1.3 are offered, and no session is resumed: a session
/// cache would be state shared between connections. A connection checks
/// certificates against the wall time of the step that starts its handshake
/// (`env.wall`), not the clock: each takes this configuration with a time
/// of its own ([`Config::at`]).
#[derive(Clone, Debug)]
pub struct Config {
    client: Arc<ClientConfig>,
}

/// What startup's [`Config::new`] and [`Config::from_der`] refuse.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Refusal {
    /// No root is trusted: every handshake would fail.
    Roots,
    /// A protocol offered by ALPN is empty or longer than 255 bytes, or all
    /// of them take more than [`ALPN`] bytes.
    Alpn,
    /// The crypto provider has no suite of TLS 1.2 or 1.3: ring's has both.
    Provider,
}

/// What startup's [`Config::from_der`] took and skipped from its DER roots.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Parsed {
    pub taken: u32,
    pub skipped: u32,
}

/// The shared configuration's heap from at most `count` roots of `each`
/// bytes (tls.md, 3.4 and 5), counted once by its service. Includes its
/// fixed construction peak and longest ALPN, owned anchor bytes and the root
/// vector's growth; excludes the caller's DER input. `None` on overflow.
#[must_use]
pub fn roots_worst_case(count: u32, each: u32) -> Option<u64> {
    let anchor = u64::try_from(size_of::<TrustAnchor<'static>>()).ok()?;
    // A growing root vector can hold its old capacity beside twice that
    // capacity; the owned byte fields are at most the input certificate.
    let per = u64::from(each).checked_add(anchor)?.checked_mul(3)?;
    // The counting allocator measures a 19 KB construction peak with one
    // root and the maximum ALPN element count; 24 KB bounds that fixed work.
    u64::from(count).checked_mul(per)?.checked_add(24 * 1_024)
}

impl Config {
    /// Trusts the DER roots rustls parses with supported key algorithms,
    /// offering `alpn`; counts the rest as skipped (tls.md, 3.4).
    /// Refuses `Roots` when none were taken, and keeps the ALPN refusals.
    /// The shell bounds the input count to `u32`; a larger slice is a bug.
    pub fn from_der(roots: &[Box<[u8]>], alpn: &[&[u8]]) -> Result<(Config, Parsed), Refusal> {
        let count = u32::try_from(roots.len()).expect("startup roots have a u32 count bound");
        let provider = ring::default_provider();
        let mut store = RootCertStore::empty();
        let mut taken: u32 = 0;
        for certificate in roots {
            if store.add(CertificateDer::from(certificate.as_ref())).is_ok() {
                let anchor = store.roots.last().expect("add retained an anchor");
                if supports_key(anchor.subject_public_key_info.as_ref(), &provider) {
                    taken = taken.checked_add(1).expect("within input count");
                } else {
                    store.roots.pop();
                }
            }
        }
        let parsed = Parsed { taken, skipped: count.checked_sub(taken).expect("within input count") };
        let config = Config::with_provider(store, alpn, provider)?;
        Ok((config, parsed))
    }

    /// The configuration that trusts `roots` and offers `alpn`, in order of
    /// preference, or none (RFC 7301).
    pub fn new(roots: RootCertStore, alpn: &[&[u8]]) -> Result<Config, Refusal> {
        Config::with_provider(roots, alpn, ring::default_provider())
    }

    /// As [`Config::new`], with `provider` for the cryptography in place of
    /// ring's own: for a test that changes a suite, as its keys' limit. Not
    /// for a service, whose exception is ring (tls.md, 4).
    #[doc(hidden)]
    pub fn with_provider(roots: RootCertStore, alpn: &[&[u8]], provider: CryptoProvider) -> Result<Config, Refusal> {
        if roots.is_empty() {
            return Err(Refusal::Roots);
        }
        let mut wire: usize = 0;
        let mut protocols = Vec::with_capacity(alpn.len());
        for protocol in alpn {
            if protocol.is_empty() || protocol.len() > usize::from(u8::MAX) {
                return Err(Refusal::Alpn);
            }
            wire = wire.saturating_add(protocol.len()).saturating_add(1);
            protocols.push(Vec::from(*protocol));
        }
        if wire > ALPN {
            return Err(Refusal::Alpn);
        }
        let builder = ClientConfig::builder_with_details(
            Arc::new(provider),
            Arc::new(At(UnixTime::since_unix_epoch(Duration::ZERO))),
        );
        let Ok(builder) = builder.with_safe_default_protocol_versions() else {
            return Err(Refusal::Provider);
        };
        let mut client = builder.with_root_certificates(roots).with_no_client_auth();
        client.alpn_protocols = protocols;
        client.resumption = Resumption::disabled();
        Ok(Config { client: Arc::new(client) })
    }

    /// rustls's configuration for one connection: this one, with `wall` its
    /// time, read once.
    pub(crate) fn at(&self, wall: Wall) -> Arc<ClientConfig> {
        let mut client = ClientConfig::clone(&self.client);
        client.time_provider = Arc::new(At(UnixTime::since_unix_epoch(Duration::from_secs(wall.as_secs()))));
        Arc::new(client)
    }
}

/// rustls has parsed the SPKI sequence's contents. Its first element is
/// the key's `AlgorithmIdentifier`; compare that element with ring's actual
/// verification algorithms, rather than accepting an unknown algorithm
/// merely because the trust-anchor parser accepted its DER.
fn supports_key(spki: &[u8], provider: &CryptoProvider) -> bool {
    for algorithm in provider.signature_verification_algorithms.all {
        let identifier = algorithm.public_key_alg_id();
        let bytes = identifier.as_ref();
        // The pinned ring provider's identifiers use DER's short length.
        if bytes.len() < 128 {
            let length = u8::try_from(bytes.len()).expect("short DER length");
            let end = bytes.len().checked_add(2).expect("short identifier");
            if spki.get(..2) == Some([0x30, length].as_slice()) && spki.get(2..end) == Some(bytes) {
                return true;
            }
        }
    }
    false
}

/// The name of the server a connection is for: a DNS name, which goes in the
/// `ClientHello` (SNI) and which its certificate must be valid for, or an IP
/// address, which its certificate must name.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Name(ServerName<'static>);

impl Name {
    /// `text` as a DNS name or an IP address, or `None` if it is neither.
    #[must_use]
    pub fn new(text: &str) -> Option<Name> {
        match ServerName::try_from(text) {
            Ok(name) => Some(Name(name.to_owned())),
            Err(_) => None,
        }
    }

    pub(crate) fn into_server_name(self) -> ServerName<'static> {
        self.0
    }
}

/// The time a connection checks certificates against: the wall time of the
/// step that started its handshake. rustls asks a provider for it.
#[derive(Debug)]
struct At(UnixTime);

impl TimeProvider for At {
    fn current_time(&self) -> Option<UnixTime> {
        Some(self.0)
    }
}

/// Real TLS 1.2 peer for the lowest-tier native Ready/exhaustion control.
/// This configuration uses the same documented rustls Arc/Vec configuration
/// exception and actual checked-in PKI fixtures, read only by this test from
/// Cargo's crate-root working directory (tls.md, 3.6 and 4).
#[cfg(test)]
pub(crate) fn native_test_peer() -> (Config, rustls::server::UnbufferedServerConnection) {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(
            std::fs::read("../../tests/tls/fixtures/root.der").expect("actual fixture root bytes"),
        ))
        .expect("actual fixture root");
    let client = Config::new(roots, &[]).expect("actual client roots");
    let certificates = Vec::from([
        CertificateDer::from(std::fs::read("../../tests/tls/fixtures/leaf.der").expect("actual fixture leaf bytes")),
        CertificateDer::from(
            std::fs::read("../../tests/tls/fixtures/intermediate.der").expect("actual fixture intermediate bytes"),
        ),
    ]);
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        std::fs::read("../../tests/tls/fixtures/leaf.key").expect("actual fixture private key bytes"),
    ));
    let server = rustls::server::ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_protocol_versions(&[&rustls::version::TLS12])
        .expect("TLS 1.2 provider")
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .expect("actual fixture key");
    let peer = rustls::server::UnbufferedServerConnection::new(Arc::new(server)).expect("actual rustls peer");
    (client, peer)
}
