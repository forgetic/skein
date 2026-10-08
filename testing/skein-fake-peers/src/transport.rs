//! Per-socket plaintext face or bounded buffered rustls server. TLS consumes
//! one record at a time and owns plaintext until the HTTP demand can be met.

use std::io::{Read as _, Write as _};
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{ServerConfig, ServerConnection};
use skein_io as io;
use skein_lib::stream::{Down, Fault, Read, Up};
use skein_lib::{Intake, Queue, Token};

use crate::{Error, Limits};

const RECORD: u32 = 18_432;

/// Transport selected by a world; TLS presents Skein's fixed test certificates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    /// A replayable local byte stream selected by a simulated world.
    Plaintext,
    /// Real rustls cryptography selected by a TLS or real-loop world.
    Tls,
}

#[expect(clippy::struct_excessive_bools, reason = "independent read, write and settlement flags")]
pub(crate) struct Wire {
    socket: Token,
    tls: Option<ServerConnection>,
    input: Intake,
    demand: Read,
    room: u32,
    granted: u32,
    header: Option<[u8; 5]>,
    reading: bool,
    output: Vec<u8>,
    output_room: bool,
    ended: bool,
    notified: bool,
    closing: bool,
    limits: Limits,
}

impl Wire {
    pub fn new(socket: Token, transport: Transport, limits: Limits) -> Self {
        let tls = match transport {
            Transport::Plaintext => None,
            Transport::Tls => {
                let mut connection = ServerConnection::new(config()).expect("the fixed test chain");
                connection.set_buffer_limit(Some(usize::try_from(limits.ciphertext).expect("u32 fits")));
                Some(connection)
            }
        };
        Self {
            socket,
            tls,
            input: Intake::with_capacity(limits.plaintext),
            demand: Read::Nothing,
            room: 0,
            granted: 0,
            header: None,
            reading: false,
            output: Vec::with_capacity(usize::try_from(limits.ciphertext).expect("u32 fits")),
            output_room: false,
            ended: false,
            notified: false,
            closing: false,
            limits,
        }
    }

    pub fn up(&mut self, up: Up, above: &mut Queue<Up>, requests: &mut Queue<io::Request>) {
        if self.closing {
            return;
        }
        if self.tls.is_none() {
            above.push(up);
            return;
        }
        match up {
            Up::Bytes(bytes) => {
                self.reading = false;
                match self.header.take() {
                    None => {
                        let header: [u8; 5] = bytes.as_ref().try_into().expect("a demanded TLS header");
                        let size = u32::from(u16::from_be_bytes([header[3], header[4]]));
                        if size == 0 || size > RECORD {
                            self.fail(above, requests);
                            return;
                        }
                        self.header = Some(header);
                    }
                    Some(header) => {
                        let tls = self.tls.as_mut().expect("TLS face");
                        let mut record = Vec::with_capacity(bytes.len().checked_add(5).expect("bounded record"));
                        record.extend_from_slice(&header);
                        record.extend_from_slice(&bytes);
                        if tls.read_tls(&mut record.as_slice()).is_err() {
                            self.fail(above, requests);
                            return;
                        }
                        let Ok(state) = tls.process_new_packets() else {
                            self.fail(above, requests);
                            return;
                        };
                        let size = state.plaintext_bytes_to_read();
                        if size > usize::try_from(self.input.room()).expect("u32 fits") {
                            self.fail(above, requests);
                            return;
                        }
                        let mut plain = vec![0; size];
                        if tls.reader().read_exact(&mut plain).is_err() {
                            self.fail(above, requests);
                            return;
                        }
                        self.input.append(&plain).expect("admitted plaintext");
                        self.ended |= state.peer_has_closed();
                    }
                }
            }
            Up::Room => {
                assert!(self.output_room, "one network output demand");
                self.output_room = false;
                let bytes = self.output.clone().into_boxed_slice();
                self.output.clear();
                requests.push(io::Request::Stream { stream: self.socket, down: Down::Send(bytes) });
            }
            Up::End => {
                self.reading = false;
                self.ended = true;
            }
            Up::Failed(_) => {
                self.fail(above, requests);
                return;
            }
        }
        self.pump(above, requests);
    }

    pub fn down(&mut self, down: Down, above: &mut Queue<Up>, requests: &mut Queue<io::Request>) {
        if self.tls.is_none() {
            requests.push(io::Request::Stream { stream: self.socket, down });
            return;
        }
        match down {
            Down::Demand { read, room } => {
                self.demand = read;
                self.room = room;
            }
            Down::Send(bytes) => {
                assert!(
                    bytes.len() <= usize::try_from(self.granted).expect("u32 fits"),
                    "plaintext within granted room"
                );
                self.granted = 0;
                if self.tls.as_mut().expect("TLS face").writer().write_all(&bytes).is_err() {
                    self.fail(above, requests);
                    return;
                }
            }
            Down::Finish => self.tls.as_mut().expect("TLS face").send_close_notify(),
        }
        self.pump(above, requests);
    }

    pub fn pump(&mut self, above: &mut Queue<Up>, requests: &mut Queue<io::Request>) {
        if self.closing || self.tls.is_none() || above.room() < 3 || requests.room() < 3 {
            return;
        }
        let tls = self.tls.as_mut().expect("TLS face");
        if !self.output_room && tls.wants_write() {
            // A fixed slice writer refuses before a ciphertext buffer can grow.
            let cap = usize::try_from(self.limits.ciphertext).expect("u32 fits");
            let mut scratch = vec![0; cap];
            let mut writer = scratch.as_mut_slice();
            if tls.write_tls(&mut writer).is_err() {
                self.fail(above, requests);
                return;
            }
            let length = cap.checked_sub(writer.len()).expect("slice writer advanced");
            assert!(self.output.is_empty(), "a previous ciphertext send was handed down");
            self.output.extend_from_slice(&scratch[..length]);
            self.output_room = true;
            requests.push(io::Request::Stream {
                stream: self.socket,
                down: Down::Demand {
                    read: if self.reading {
                        Read::Fill(
                            self.header.map_or(5, |header| u32::from(u16::from_be_bytes([header[3], header[4]]))),
                        )
                    } else {
                        Read::Nothing
                    },
                    room: u32::try_from(length).expect("bounded ciphertext"),
                },
            });
        }
        if !tls.is_handshaking() {
            if let Some(bytes) = self.input.meet(self.demand) {
                self.demand = Read::Nothing;
                above.push(Up::Bytes(bytes));
            }
            if self.room > 0 && self.output.is_empty() && !tls.wants_write() {
                self.granted = self.room;
                self.room = 0;
                above.push(Up::Room);
            }
            if self.ended && !self.notified {
                self.notified = true;
                above.push(Up::End);
            }
        }
        if !self.ended && !self.reading && !self.output_room && self.input.room() >= 16_384 {
            let size = self.header.map_or(5, |header| u32::from(u16::from_be_bytes([header[3], header[4]])));
            requests.push(io::Request::Stream {
                stream: self.socket,
                down: Down::Demand { read: Read::Fill(size), room: 0 },
            });
            self.reading = true;
        }
    }

    pub fn close(&mut self, requests: &mut Queue<io::Request>) {
        if !self.closing && requests.room() > 0 {
            self.closing = true;
            requests.push(io::Request::Abort { entity: self.socket });
        }
    }

    fn fail(&mut self, above: &mut Queue<Up>, requests: &mut Queue<io::Request>) {
        if !self.closing {
            above.push(Up::Failed(Fault::Invalid));
        }
        self.close(requests);
    }

    pub fn work_pending(&self) -> bool {
        self.tls.as_ref().is_some_and(|tls| {
            !self.closing
                && ((!self.output_room && tls.wants_write())
                    || (!tls.is_handshaking() && self.room > 0 && self.output.is_empty())
                    || (!self.reading && !self.output_room && !self.ended && self.input.room() >= 16_384))
        })
    }
}

pub(crate) fn worst_case(limits: &Limits, transport: Transport) -> Option<u64> {
    let base = u64::from(limits.plaintext).checked_add(u64::from(limits.ciphertext).checked_mul(4)?)?;
    match transport {
        Transport::Plaintext => Some(base),
        // rustls limits a handshake message to 64 KiB, plaintext records to
        // 16 KiB, and buffers to ciphertext. The fixed unauthenticated server
        // chain/configuration, decoded hello extensions and crypto workspace
        // share this conservative 1 MiB envelope. The worst-case tests meter
        // construction, handshake and full buffers (testing-strategy.md, 6).
        Transport::Tls => base.checked_add(1_048_576),
    }
}

fn config() -> Arc<ServerConfig> {
    const LEAF: &[u8] = include_bytes!("../../../tests/tls/fixtures/leaf.der");
    const INTERMEDIATE: &[u8] = include_bytes!("../../../tests/tls/fixtures/intermediate.der");
    const KEY: &[u8] = include_bytes!("../../../tests/tls/fixtures/leaf.key");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("fixed TLS versions")
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(LEAF), CertificateDer::from(INTERMEDIATE)],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(KEY)),
        )
        .expect("fixed test certificate");
    config.max_fragment_size = Some(4096);
    Arc::new(config)
}

pub(crate) fn validate(limits: &Limits, transport: Transport) -> Result<(), Error> {
    if transport == Transport::Tls && limits.plaintext < 16_384 {
        return Err(Error::Limits);
    }
    Ok(())
}
