//! Record boundaries through the fake's real buffered cryptography, without
//! host IO (fake-llm.md, section 3; testing-strategy.md, section 2.1).

use std::io::{Read as _, Write as _};
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore};
use skein_lib::stream::{Down, OutputDown, OutputOutcome, OutputUp, Read, Up};
use skein_lib::{Queue, Token};

use super::{Limits, Transport, Wire, config};

fn limits() -> Limits {
    Limits {
        io: skein_io::Limits {
            sockets: 2,
            refusals: 1,
            intake: 32_768,
            receive: 4096,
            output: 32_768,
            sends: 4,
            accepts: 1,
            backlog: 1,
            close_timeout: skein_lib::Duration::from_secs(1),
            retry: skein_lib::Duration::from_millis(1),
        },
        connections: 1,
        queue: 64,
        plaintext: 32_768,
        ciphertext: 32_768,
        observations: 16,
        observation_bytes: 32_768,
    }
}

struct Pair {
    wire: Wire,
    client: ClientConnection,
    above: Queue<Up>,
    requests: Queue<skein_io::Request>,
}

impl Pair {
    fn new() -> Self {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from(include_bytes!("../../../../tests/tls/fixtures/root.der").as_slice()))
            .expect("fixed root");
        let mut server = config().as_ref().clone();
        // Both directions exercise a full 16 KiB record, independent of the
        // hosted peer's usual fragment preference; its buffer caps are unchanged.
        server.max_fragment_size = None;
        let client = ClientConfig::builder_with_provider(server.crypto_provider().clone())
            .with_safe_default_protocol_versions()
            .expect("fixed versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        let mut wire = Wire::new(Token::new(1), Transport::Tls, limits());
        let mut connection = rustls::ServerConnection::new(Arc::new(server)).expect("fixed server");
        connection.set_buffer_limit(Some(usize::try_from(limits().ciphertext).expect("fixed cap")));
        wire.tls = Some(connection);
        let mut pair = Self {
            wire,
            client: ClientConnection::new(Arc::new(client), ServerName::try_from("skein.test").expect("fixed name"))
                .expect("fixed client"),
            above: Queue::with_capacity(64),
            requests: Queue::with_capacity(64),
        };
        for _ in 0..20 {
            let mut bytes = Vec::new();
            while pair.client.wants_write() {
                assert!(pair.client.write_tls(&mut bytes).expect("vector writer") > 0);
            }
            pair.wire.receive(&bytes).expect("client ciphertext");
            let bytes = pair.output();
            let plain = client_receive(&mut pair.client, &bytes);
            assert!(plain.is_empty(), "handshake only");
            if !pair.client.is_handshaking() && !pair.wire.tls.as_ref().expect("TLS").is_handshaking() {
                return pair;
            }
        }
        panic!("fixed handshake progresses");
    }

    fn output(&mut self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for _ in 0..20 {
            self.wire.pump(&mut self.above, &mut self.requests);
            while let Some(request) = self.requests.pop() {
                match request {
                    skein_io::Request::Output { down: OutputDown::Room { right, .. }, .. } => self.wire.output(
                        OutputUp::Settled { right, outcome: OutputOutcome::Granted },
                        &mut self.above,
                        &mut self.requests,
                    ),
                    skein_io::Request::Output { down: OutputDown::Send { bytes: sent, .. }, .. } => {
                        bytes.extend_from_slice(&sent);
                    }
                    skein_io::Request::Stream { down: Down::Demand { .. }, .. } => {}
                    other @ (skein_io::Request::Listen { .. }
                    | skein_io::Request::Connect { .. }
                    | skein_io::Request::Bind { .. }
                    | skein_io::Request::Reject { .. }
                    | skein_io::Request::Stream { .. }
                    | skein_io::Request::Output { .. }
                    | skein_io::Request::Spawn { .. }
                    | skein_io::Request::Signal { .. }
                    | skein_io::Request::Usage { .. }
                    | skein_io::Request::Close { .. }
                    | skein_io::Request::Abort { .. }) => panic!("unexpected wire request: {other:?}"),
                }
            }
            if !self.wire.tls.as_ref().expect("TLS").wants_write() && !self.wire.output_room {
                return bytes;
            }
        }
        panic!("bounded output drains");
    }

    fn send(&mut self, plain: &[u8]) -> Vec<u8> {
        self.wire.down(
            Down::Demand { read: Read::Nothing, room: u32::try_from(plain.len()).expect("record size") },
            &mut self.above,
            &mut self.requests,
        );
        assert_eq!(self.above.pop(), Some(Up::Room));
        self.wire.down(Down::Send(plain.into()), &mut self.above, &mut self.requests);
        self.output()
    }
}

fn client_receive(client: &mut ClientConnection, mut bytes: &[u8]) -> Vec<u8> {
    let mut plain = Vec::new();
    while !bytes.is_empty() {
        assert!(client.read_tls(&mut bytes).expect("slice reader") > 0);
        let state = client.process_new_packets().expect("valid server records");
        let start = plain.len();
        plain.resize(start + state.plaintext_bytes_to_read(), 0);
        client.reader().read_exact(&mut plain[start..]).expect("available plaintext");
    }
    plain
}

fn client_send(client: &mut ClientConnection, plain: &[u8]) -> Vec<u8> {
    client.writer().write_all(plain).expect("bounded fixture");
    let mut bytes = Vec::new();
    while client.wants_write() {
        assert!(client.write_tls(&mut bytes).expect("vector writer") > 0);
    }
    bytes
}

#[test]
fn every_record_boundary_and_two_cuts_preserve_both_directions() {
    for size in [1, 4095, 4096, 4097, 16_384] {
        for cut in [0, 1, size / 2] {
            let mut pair = Pair::new();
            let plain = vec![b'x'; size];
            let bytes = client_send(&mut pair.client, &plain);
            let cut = cut.min(bytes.len());
            pair.wire.receive(&bytes[..cut]).expect("partial record waits");
            if cut > 0 {
                assert!(pair.wire.input.is_empty(), "no plaintext from an incomplete record");
            }
            pair.wire.receive(&bytes[cut..]).expect("remaining record");
            assert_eq!(
                pair.wire.input.meet(Read::Fill(u32::try_from(size).expect("record"))),
                Some(plain.clone().into())
            );
            let bytes = pair.send(&plain);
            let cut = cut.min(bytes.len());
            let mut received = client_receive(&mut pair.client, &bytes[..cut]);
            received.extend(client_receive(&mut pair.client, &bytes[cut..]));
            assert_eq!(received, plain);
        }
    }
}

#[test]
fn each_record_coalesced_with_the_next_preserves_both_directions() {
    for size in [1, 4095, 4096, 4097, 16_384] {
        let mut pair = Pair::new();
        let mut expected = vec![b'x'; size];
        expected.push(b'y');
        let mut bytes = client_send(&mut pair.client, &expected[..size]);
        bytes.extend(client_send(&mut pair.client, b"y"));
        pair.wire.receive(&bytes).expect("coalesced records");
        assert_eq!(
            pair.wire.input.meet(Read::Fill(u32::try_from(expected.len()).expect("records"))),
            Some(expected.clone().into())
        );
        let mut bytes = pair.send(&expected[..size]);
        bytes.extend(pair.send(b"y"));
        assert_eq!(client_receive(&mut pair.client, &bytes), expected);
    }
}
