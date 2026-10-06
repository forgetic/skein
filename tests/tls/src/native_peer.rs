//! Authenticated `TLS1.2` control peer made from an actual completed rustls
//! connection's traffic secrets. It seals the `HelloRequest` rustls's normal
//! server never sends and authenticates every client alert/data record using
//! the real extracted receive key and sequence, rather than inventing TLS
//! observations (tls.md, 3.6; testing-strategy.md, 2.4). Actual IO capacity proof
//! remains native.rs. Its bounded test payloads/record logs are caller-owned.

use rustls::crypto::cipher::{InboundOpaqueMessage, MessageDecrypter};
use rustls::{ConnectionTrafficSecrets, ContentType, ProtocolVersion, SupportedCipherSuite};
use skein_tls::client;

use crate::{pki, server::Server};

/// An authenticated peer after consuming a real extractable `TLS1.2` server.
/// Keeps its two actual directional keys/sequences, generated ciphertext and
/// observed plaintext/alerts; no TLS readiness or output winner is synthesized.
/// Test callers bound total records/payloads and charge this peer separately
/// from the native client (tls.md, 3.6 and 5).
pub struct Tls12 {
    sealer: pki::Sealer,
    opener: Box<dyn MessageDecrypter>,
    sequence: u64,
    outgoing: Vec<u8>,
    /// Exact client application plaintext after AEAD authentication (tls.md, 3.6).
    pub received: Vec<u8>,
    /// Exact authenticated client alerts, including warning refusal and close
    /// notification in arrival order (tls.md, 3.6).
    pub alerts: Vec<[u8; 2]>,
    /// Every actual authenticated record type in order, independent of the
    /// requested scenario (tls.md, 3.6; testing-strategy.md, 6).
    pub records: Vec<ContentType>,
}

impl Tls12 {
    /// Consume a genuinely completed extractable `AES128-GCM` rustls server;
    /// its real rx/tx sequences continue without reset (tls.md, 3.6).
    #[must_use]
    pub fn new(server: Server) -> Self {
        assert!(!server.handshaking(), "actual completed peer before extracting keys");
        let secrets = server.into_secrets();
        let (sequence, ConnectionTrafficSecrets::Aes128Gcm { key, iv }) = secrets.rx else {
            panic!("actual extractable TLS1.2 AES128-GCM receive keys")
        };
        let SupportedCipherSuite::Tls12(suite) = pki::EXTRACTABLE else { panic!("actual TLS1.2 suite") };
        Self {
            sealer: pki::Sealer::new(secrets.tx),
            opener: suite.aead_alg.decrypter(key, &iv.as_ref()[..4]),
            sequence,
            outgoing: Vec::new(),
            received: Vec::new(),
            alerts: Vec::new(),
            records: Vec::new(),
        }
    }

    /// Seal the one admitted `HelloRequest` with the actual peer's current key;
    /// native rustls decides and emits its own refusal (RFC5246 7.4.1.1;
    /// tls.md, 3.6). This method invents no lower Room, alert or output terminal.
    pub fn hello_request(&mut self) {
        self.outgoing.extend(self.sealer.seal(ContentType::Handshake, &[0, 0, 0, 0]));
    }

    /// Authenticate whole actual native Sends record by record; reject altered
    /// ciphertext, wrong sequence, malformed headers or unexpected record types.
    /// All recorded bytes came from the native engine (tls.md, 3.6).
    pub fn receive(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            let [typ, 3, 3, high, low, rest @ ..] = bytes else { panic!("actual complete TLS1.2 header") };
            let count = usize::from(u16::from_be_bytes([*high, *low]));
            assert!(count <= usize::try_from(client::MAX_BODY).expect("finite record"));
            let (body, remaining) = rest.split_at(count);
            let mut body = body.to_vec();
            let record = self
                .opener
                .decrypt(
                    InboundOpaqueMessage::new(ContentType::from(*typ), ProtocolVersion::TLSv1_2, &mut body),
                    self.sequence,
                )
                .expect("actual native ciphertext authenticates under real peer sequence");
            self.sequence = self.sequence.checked_add(1).expect("bounded peer transcript");
            self.records.push(record.typ);
            if let ContentType::ApplicationData = record.typ {
                self.received.extend_from_slice(record.payload);
            } else if let ContentType::Alert = record.typ {
                let [level, description] = record.payload else { panic!("authenticated two-byte TLS alert") };
                self.alerts.push([*level, *description]);
            } else {
                panic!("actual post-handshake data/refusal/close records only");
            }
            bytes = remaining;
        }
    }

    /// Seal the actual test response with the continuing peer key/sequence,
    /// keeping each record within the original plaintext maximum (tls.md, 3.6).
    pub fn write(&mut self, plaintext: &[u8]) {
        for piece in plaintext.chunks(usize::try_from(client::MAX_PLAINTEXT).expect("finite plaintext")) {
            self.outgoing.extend(self.sealer.seal(ContentType::ApplicationData, piece));
        }
    }

    /// Move only actually sealed peer ciphertext to the independent read wire;
    /// sequence/random ciphertext is never asserted replayable (tls.md, 3.6).
    #[must_use]
    pub fn transmit(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.outgoing)
    }
}
