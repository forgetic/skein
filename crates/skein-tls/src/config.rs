//! What a service hands the TLS client as data (tls.md, 3; shell.md, 6):
//! the roots it trusts and the protocols it offers by ALPN, made once at
//! startup into a [`Config`] every connection shares, and each connection's
//! server name, a [`Name`].

#![expect(
    clippy::disallowed_types,
    reason = "rustls takes its configuration in an Arc that every connection shares, its time from a provider behind one, and its protocols in Vecs (tls.md, 4)"
)]

use alloc::sync::Arc;
use alloc::vec::Vec;
use core::time::Duration;

use rustls::client::Resumption;
use rustls::crypto::ring;
use rustls::pki_types::{ServerName, UnixTime};
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

/// What [`Config::new`] refuses.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Refusal {
    /// No root is trusted: every handshake would fail.
    Roots,
    /// A protocol offered by ALPN is empty or longer than 255 bytes, or all
    /// of them take more than [`ALPN`] bytes.
    Alpn,
}

impl Config {
    /// The configuration that trusts `roots` and offers `alpn`, in order of
    /// preference, or none (RFC 7301).
    pub fn new(roots: RootCertStore, alpn: &[&[u8]]) -> Result<Config, Refusal> {
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
        let provider = Arc::new(ring::default_provider());
        let mut client =
            ClientConfig::builder_with_details(provider, Arc::new(At(UnixTime::since_unix_epoch(Duration::ZERO))))
                .with_safe_default_protocol_versions()
                .expect("ring's provider has suites for TLS 1.2 and 1.3")
                .with_root_certificates(roots)
                .with_no_client_auth();
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
