//! The ledger: io's contract with the layer above, checked as a world goes
//! (testing-strategy.md, 6; io.md, 3.1 and 3.3). Every request io takes and
//! every event it tells goes through it, in the order io sees them.
//!
//! - One `Closed` per entity with an owner, the last event naming it, and
//!   `Failed` and `End` once each; nothing but `Closed` after the owner's own
//!   close; `Connecting` before `Connected`.
//! - Each demand answered once, by `Bytes` exactly what it reads, or by
//!   `Room` if it asks for room; no `Bytes` after `End`: no buffer past its
//!   cap.
//! - Every socket announced is answered once.

use std::collections::{BTreeMap, BTreeSet};

use skein_io::{Event, Request};
use skein_lib::Token;
use skein_lib::stream::{Down, Read, Up};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Made {
    Listen,
    Connect,
    Bind,
}

/// What an owner token has been told, and what it asked.
#[derive(Debug)]
#[expect(clippy::struct_excessive_bools, reason = "what was told, one flag a fact, each checked alone")]
struct Told {
    made: Made,
    /// `Listening` or `Connecting` told, or bound.
    named: bool,
    connected: bool,
    failed: bool,
    ended: bool,
    closing: bool,
    closed: bool,
    /// io's token for the entity.
    socket: Option<Token>,
    /// The demand io holds: a read, and room.
    read: Read,
    room: u32,
}

/// io's contract, kept for one process.
#[derive(Debug, Default)]
pub struct Ledger {
    owners: BTreeMap<Token, Told>,
    /// io's tokens for its entities, and their owners'.
    sockets: BTreeMap<Token, Token>,
    announced: BTreeSet<Token>,
}

impl Told {
    fn new(made: Made) -> Told {
        Told {
            made,
            named: made == Made::Bind,
            connected: made == Made::Bind,
            failed: false,
            ended: false,
            closing: false,
            closed: false,
            socket: None,
            read: Read::Nothing,
            room: 0,
        }
    }
}

impl Ledger {
    /// A request, as io takes it.
    pub fn request(&mut self, request: &Request) {
        match request {
            Request::Listen { owner, .. } => self.made(*owner, Made::Listen),
            Request::Connect { owner, .. } => self.made(*owner, Made::Connect),
            Request::Bind { socket, owner } => {
                assert!(self.announced.remove(socket), "a socket is bound once, after it is announced");
                self.made(*owner, Made::Bind);
                self.owners.get_mut(owner).expect("just made").socket = Some(*socket);
                self.sockets.insert(*socket, *owner);
            }
            Request::Reject { socket } => {
                assert!(self.announced.remove(socket), "a socket is rejected once, after it is announced");
            }
            Request::Stream { stream, down } => {
                let owner = self.sockets.get(stream).expect("a stream request names a socket io told");
                let told = self.owners.get_mut(owner).expect("a socket's owner");
                match down {
                    Down::Demand { read, room } if !told.closing && !told.closed => {
                        told.read = *read;
                        told.room = *room;
                    }
                    Down::Demand { .. } | Down::Send(_) | Down::Finish => {}
                }
            }
            Request::Close { entity } | Request::Abort { entity } => {
                if let Some(owner) = self.sockets.get(entity) {
                    self.owners.get_mut(owner).expect("a socket's owner").closing = true;
                }
            }
        }
    }

    fn made(&mut self, owner: Token, made: Made) {
        assert!(!self.owners.contains_key(&owner), "an owner token names one entity: {owner:?}");
        self.owners.insert(owner, Told::new(made));
    }

    /// An event, as io tells it.
    pub fn event(&mut self, event: &Event) {
        let owner = match event {
            Event::Listening { owner, .. }
            | Event::Accepted { owner, .. }
            | Event::Connecting { owner, .. }
            | Event::Connected { owner }
            | Event::Stream { owner, .. }
            | Event::Failed { owner, .. }
            | Event::Closed { owner } => *owner,
        };
        let told = self.owners.get_mut(&owner).unwrap_or_else(|| panic!("{event:?} names an owner that asked"));
        assert!(!told.closed, "nothing after Closed: {event:?}");
        match event {
            Event::Listening { listener, .. } => {
                assert!(told.made == Made::Listen && !told.named, "Listening once, to a listen");
                told.named = true;
                told.socket = Some(*listener);
                self.sockets.insert(*listener, owner);
            }
            Event::Accepted { socket, .. } => {
                assert!(told.made == Made::Listen && told.named && !told.closing, "announced to a listener open");
                assert!(self.announced.insert(*socket), "a socket is announced once");
            }
            Event::Connecting { socket, .. } => {
                assert!(told.made == Made::Connect && !told.named, "Connecting once, first, to a connect");
                told.named = true;
                told.socket = Some(*socket);
                self.sockets.insert(*socket, owner);
            }
            Event::Connected { .. } => {
                assert!(told.made == Made::Connect && told.named, "Connected after Connecting");
                assert!(!told.connected && !told.failed && !told.closing, "Connected once, unless failed or closed");
                told.connected = true;
            }
            Event::Stream { up, .. } => {
                assert!(told.connected, "a stream event once connected or bound: {event:?}");
                assert!(!told.failed && !told.closing, "only Closed after Failed or the owner's close: {event:?}");
                match up {
                    // Either answer ends the demand (lib.md, 7).
                    Up::Bytes(bytes) => {
                        assert!(!told.ended, "no Bytes after End");
                        assert!(
                            met(told.read, bytes),
                            "Bytes exactly as demanded: {:?}, {} bytes",
                            told.read,
                            bytes.len()
                        );
                        (told.read, told.room) = (Read::Nothing, 0);
                    }
                    Up::Room => {
                        assert!(told.room > 0, "Room only when room was asked for");
                        (told.read, told.room) = (Read::Nothing, 0);
                    }
                    Up::End => {
                        assert!(!told.ended, "End once");
                        told.ended = true;
                    }
                    Up::Failed(_) => told.failed = true,
                }
            }
            Event::Failed { .. } => {
                assert!(told.made != Made::Bind && !told.failed && !told.connected, "Failed once, before Connected");
                told.failed = true;
            }
            Event::Closed { .. } => told.closed = true,
        }
    }

    /// Once the world settled: every entity with an owner closed, every
    /// socket announced answered.
    pub fn settled(&self) {
        for (owner, told) in &self.owners {
            assert!(told.closed, "{owner:?} was told Closed: {told:?}");
        }
        assert!(self.announced.is_empty(), "every socket announced was answered: {:?}", self.announced);
    }
}

/// Whether `bytes` are exactly what `read` demands (lib.md, 7).
fn met(read: Read, bytes: &[u8]) -> bool {
    match read {
        Read::Nothing => false,
        Read::Fill(n) => bytes.len() == n as usize,
        Read::Scan { until, max } => {
            let until = until.as_bytes();
            let max = max as usize;
            let first = bytes.windows(until.len()).position(|window| window == until);
            match first {
                Some(at) => at + until.len() == bytes.len() && bytes.len() <= max,
                None => bytes.len() == max,
            }
        }
    }
}
