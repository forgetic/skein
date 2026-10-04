//! A rustls server in memory: rustls's buffered connection, fed the
//! client's ciphertext and drained of its own, over byte slices.
//!
//! It is ordinary Rust, the peer of the worlds: what it deciphers it keeps,
//! what it writes it hands back, and it never touches a socket.

use std::io::{Read, Write};
use std::sync::Arc;

use rustls::server::{ServerConfig, ServerConnection};

/// A server's connection.
#[derive(Debug)]
pub struct Server {
    tls: ServerConnection,
    /// The plaintext it read from the client.
    pub received: Vec<u8>,
    /// Whether it read the client's `close_notify`.
    pub closed: bool,
    /// The error the client's bytes caused, if they broke TLS.
    pub failed: Option<rustls::Error>,
}

impl Server {
    #[must_use]
    pub fn new(config: Arc<ServerConfig>) -> Server {
        let mut tls = ServerConnection::new(config).expect("a server's connection");
        tls.set_buffer_limit(None);
        Server { tls, received: Vec::new(), closed: false, failed: None }
    }

    /// Reads the client's `bytes`, and what they decipher to.
    pub fn receive(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() && self.failed.is_none() {
            self.tls.read_tls(&mut bytes).expect("reading from a slice");
            match self.tls.process_new_packets() {
                Ok(state) => {
                    let mut plain = vec![0; state.plaintext_bytes_to_read()];
                    self.tls.reader().read_exact(&mut plain).expect("what rustls says it holds");
                    self.received.extend_from_slice(&plain);
                    self.closed |= state.peer_has_closed();
                }
                Err(error) => self.failed = Some(error),
            }
        }
    }

    /// What it has to send, all of it.
    pub fn transmit(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        while self.tls.wants_write() {
            self.tls.write_tls(&mut out).expect("writing to a vector");
        }
        out
    }

    /// Writes `plain` to the client, once the handshake is done.
    pub fn write(&mut self, plain: &[u8]) {
        self.tls.writer().write_all(plain).expect("an unlimited buffer");
    }

    /// Sends `close_notify`.
    pub fn close_notify(&mut self) {
        self.tls.send_close_notify();
    }

    /// Asks the client to update its keys, as it updates its own (RFC 8446,
    /// 4.6.3).
    pub fn key_update(&mut self) {
        self.tls.refresh_traffic_keys().expect("a key update in TLS 1.3");
    }

    #[must_use]
    pub fn handshaking(&self) -> bool {
        self.tls.is_handshaking()
    }
}
