//! The TLS client (tls.md): a step machine that wraps rustls's unbuffered
//! client connection between two streams (lib.md, 7).
//!
//! # Its two sides
//!
//! Below, the ciphertext: a `lib::stream` to the server, a socket's or a
//! pipe's. The client reads it a record at a time, its header (a fill of 5)
//! then its body (a fill of the length the header gives), and hands each to
//! rustls, which deciphers it in place. It writes within room it asks for:
//! TLS's own output (a flight of the handshake, `close_notify`) alone, and
//! what the side above sends, encrypted, in one `Send` a grant.
//!
//! Above, the service's protocol layer ([`Request`] down, [`Event`] up), and
//! the plaintext: a `lib::stream` of which the client is the side below,
//! and which the machine above cannot tell from a socket.
//!
//! - **`Handshake` starts the connection,** first and once. `Ready`
//!   answers it, with what was agreed, or `Failed`. The plaintext stream
//!   may be demanded before `Ready`: its demands wait for the handshake.
//! - **The plaintext stream** keeps the contract of a stream (lib.md, 7):
//!   each demand answered at most once, exactly; `End` once the server's
//!   `close_notify` is read and nothing held meets a demand; `Finish` sends
//!   `close_notify`, then finishes the stream below.
//! - **`Failed` says why the connection failed,** once, after the
//!   plaintext stream was told it failed, `Failed(fault)`: a record that
//!   fails to decrypt, or any other error of the peer's data, is
//!   `Fault::Invalid`. A stream below that ends before the server's
//!   `close_notify` is a truncation, which an attacker can cause (RFC 8446,
//!   6.1): what the client deciphered before it still meets demands, and
//!   then the stream fails, never ends.
//! - **`Close` ends the client in any state.** Once the handshake is done
//!   it sends `close_notify` first, unless it was sent, and answers `Closed`
//!   once it went below; otherwise at once. It withdraws what it demanded
//!   below, and does not close the stream below, which its owner closes.
//!
//! # Bounds
//!
//! The plaintext the side above has not demanded is held in an intake of
//! [`Limits::read`] and one record's plaintext beside it; the ciphertext
//! rustls has not discarded, within [`Limits::records`]; TLS's own output,
//! within [`FLIGHT`]. The client reads a record only when the intake has
//! room for its plaintext, so rustls never holds plaintext of its own
//! between steps. Each entry point emits at most [`UP_MAX_OUT`] or
//! [`DOWN_MAX_OUT`]; [`worst_case`] is what a client holds, rustls's heap
//! included; [`LARGEST_READ`] and [`largest_room`] are what whoever stacks
//! it checks against the caps of the stream below at startup.

use core::mem;

use alloc::boxed::Box;

use skein_lib::stream::{Down, Fault, Read, Up};
use skein_lib::{Env, Intake, Queue};

use crate::held::Held;
use crate::session::{self, Connection};
use crate::{Config, MaxOut, Name};

/// The most plaintext a record carries (RFC 8446, 5.1; RFC 5246, 6.2.1).
pub const MAX_PLAINTEXT: u32 = 16_384;

/// A record's header: its type, its version and its length.
pub const HEADER: u32 = 5;

/// The longest record body TLS allows, a TLS 1.2 ciphertext's (RFC 5246,
/// 6.2.3); TLS 1.3's is shorter.
pub const MAX_BODY: u32 = MAX_PLAINTEXT + 2_048;

/// A record of the longest body, with its header.
pub const MAX_RECORD: u32 = HEADER + MAX_BODY;

/// The most bytes the client demands below at once: a record's body.
/// Whoever stacks it checks at startup that the stream below's intake holds
/// it (lib.md, 7): a demand past that cap could never be met.
pub const LARGEST_READ: u32 = MAX_BODY;

/// The most of TLS's own output held at once: a flight of the handshake, or
/// an alert and `close_notify`. The longest flight is a `ClientHello` of a
/// server name of 253 bytes and protocols of [`ALPN`](crate::ALPN) bytes,
/// measured at 738 bytes, and 809 once a `HelloRetryRequest` asks for a
/// larger key share (tls.md, 5).
pub const FLIGHT: u32 = 2_048;

/// The most a record adds to the plaintext it carries, of ring's suites:
/// TLS 1.2's AES-GCM, a header, an explicit nonce and a tag (RFC 5288, 3);
/// TLS 1.3's adds 22.
const OVERHEAD: u32 = HEADER + 8 + 16;

/// What rustls may write before the next data it encrypts, besides the
/// records of the data: in TLS 1.3, the key update that answers the
/// peer's, and one it asks for itself as its keys near their limit, each a
/// record of 27 bytes (RFC 8446, 4.6.3); in TLS 1.2, a refusal of a
/// renegotiation the client owes, 31 bytes.
const SLACK: u32 = 2 * 27;

/// rustls's own heap besides the server's certificates and the record it
/// works on: its states, keys, transcript and configuration, measured at
/// 10 KB at most, with the longest ALPN list; and what a step that
/// deciphers or encrypts a record of 16 KB holds past that record, as
/// [`LARGEST_READ`] is counted for it, about 5 KB more (tls.md, 5).
const RUSTLS: u64 = 16 * 1_024;

/// rustls holds the server's certificates twice while it reads them, as
/// measured: the message that carries them, which the records hold whole,
/// decoded, and the chain it keeps.
const CERTIFICATES: u64 = 2;

/// The client's limits (programming-model.md, 7): the same for every step
/// and for [`Client::new`], which allocates by them.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// The most the side above demands of the plaintext at once, a fill's
    /// count or a scan's maximum. The intake holds it, and one record's
    /// plaintext beside it. At least 1.
    pub read: u32,
    /// The most room the side above demands at once, in plaintext. At
    /// least 1.
    pub send: u32,
    /// The most ciphertext held at once: a record, and the records of a
    /// handshake message that spans several, the server's certificates
    /// among them. A longer message fails the handshake with
    /// [`Error::TooLong`]. At least [`MAX_RECORD`].
    pub records: u32,
}

/// The room the client demands below for the side above's demand of `send`
/// bytes: their records, each with its overhead, and rustls's slack.
#[must_use]
pub fn room_for(send: u32) -> Option<u32> {
    let records = send.checked_add(MAX_PLAINTEXT - 1)? / MAX_PLAINTEXT;
    send.checked_add(records.checked_mul(OVERHEAD)?)?.checked_add(SLACK)
}

/// The most room the client demands below at once: TLS's own output, or
/// what the side above's largest demand of room takes in records. Whoever
/// stacks the client checks it against the stream below's output cap at
/// startup.
#[must_use]
pub fn largest_room(limits: &Limits) -> u32 {
    match room_for(limits.send) {
        Some(room) => room.max(FLIGHT),
        None => u32::MAX,
    }
}

/// The intake's cap: the side above's largest read, and a record's
/// plaintext beside it, so that a demand the intake does not meet always
/// leaves room for the next record.
fn intake_cap(limits: &Limits) -> Option<u32> {
    limits.read.checked_add(MAX_PLAINTEXT)
}

/// The most memory a client holds under `limits`, in bytes
/// (programming-model.md, 6.3), rustls's heap included, or `None` if it does
/// not fit a `u64` or the limits cannot be honoured: a read or room of
/// nothing, or records shorter than one.
///
/// Its own: the intake, the records held, the output it owes, all allocated
/// with the client, and the delivery it reads, at most [`LARGEST_READ`]: a
/// delivery is made to the client's demand, so it counts it (testing.md,
/// 5); a step that encrypts holds as much of rustls's instead. What it sends
/// up or down is handed out when it is emitted. rustls allocates as it
/// pleases, so its part is measured against the counting allocator, over
/// handshakes of either version, a retry, the longest ALPN list, the
/// longest chain the records hold, and records of every size each way
/// (tls.md, 5): its own state ([`RUSTLS`]), and twice the server's
/// certificates, which the records held bound, as they hold the message
/// that carries them whole.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    if limits.read == 0 || limits.send == 0 || limits.records < MAX_RECORD {
        return None;
    }
    room_for(limits.send)?;
    let intake = Intake::worst_case(intake_cap(limits)?)?;
    let records = u64::from(limits.records);
    let certificates = records.checked_mul(CERTIFICATES)?;
    intake
        .checked_add(records)?
        .checked_add(u64::from(FLIGHT))?
        .checked_add(u64::from(LARGEST_READ))?
        .checked_add(RUSTLS)?
        .checked_add(certificates)
}

/// [`up`]'s: above, `Ready`; an answer on the plaintext stream; or the
/// stream told it failed and `Failed`; or `Closed`. Below, what TLS owes
/// sent in the room that came, the stream finished once that was
/// `close_notify`, and the next demand.
pub const UP_MAX_OUT: MaxOut = MaxOut { above: 2, below: 3 };

/// [`down`]'s: above, an answer on the plaintext stream, the stream told it
/// failed and `Failed`, or `Closed`. Below, a withdrawal or a send, then a
/// demand or the stream finished.
pub const DOWN_MAX_OUT: MaxOut = MaxOut { above: 2, below: 2 };

/// From the side above.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Request {
    /// Starts the handshake, first and once. `Ready` or `Failed` answers it.
    Handshake,
    /// The plaintext stream (lib.md, 7), once the handshake is asked for: a
    /// demand of at most [`Limits::read`] and [`Limits::send`], waiting for
    /// the handshake if it is not done; a `Send` within the room granted,
    /// one a grant, empty or not; `Finish`, with no demand outstanding,
    /// which sends `close_notify`. A demand of room comes only once the last
    /// grant was sent within, and none after `Finish`. A read that crosses
    /// `End` stays outstanding until it is withdrawn, as io's does.
    Stream(Down),
    /// Closes the client, in any state. `Closed` answers it.
    Close,
}

/// To the side above.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Event {
    /// For `Handshake`: the handshake is done, and this was agreed.
    Ready(Agreed),
    /// The plaintext stream: `Bytes`, `Room`, `End` once the server's
    /// `close_notify` was read, or `Failed`.
    Stream(Up),
    /// The connection failed: for `Handshake`, or after `Ready`. The
    /// plaintext stream was told first, unless its demand was withdrawn.
    /// Nothing follows but `Closed`.
    Failed(Error),
    /// For `Close`: the client is closed. Terminal.
    Closed,
}

/// What a handshake agreed.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Agreed {
    pub version: Version,
    /// The protocol the server chose of those offered by ALPN, if it chose
    /// one (RFC 7301).
    pub alpn: Option<Box<[u8]>>,
}

/// The version of TLS a handshake agreed.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Version {
    Tls12,
    Tls13,
}

/// Why a connection failed.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Error {
    /// The server's certificate was refused.
    Certificate(Certificate),
    /// A record failed to decrypt: the peer is broken or hostile, or its
    /// bytes were changed on the way.
    Decrypt,
    /// The peer sent a fatal alert, of this description (RFC 8446, 6): it
    /// gave up on the connection.
    Alert(u8),
    /// The peer broke TLS: a message malformed or out of place, a version,
    /// suite or protocol it was not offered.
    Protocol,
    /// A handshake message longer than [`Limits::records`] holds.
    TooLong,
    /// The stream below ended before the server's `close_notify`: during the
    /// handshake, or after it, a truncation (RFC 8446, 6.1).
    Truncated,
    /// The stream below failed.
    Stream(Fault),
    /// rustls failed for a reason of its own: no entropy from the kernel,
    /// or its keys exhausted.
    Other,
}

impl Error {
    /// What the plaintext stream is told when the connection fails with
    /// this (lib.md, 7): the stream's own fault; a reset for the peer's
    /// alert, by which it gave up; invalid for anything wrong with the
    /// peer's data, a truncation among it, which a clean end would hide.
    #[must_use]
    pub fn fault(self) -> Fault {
        match self {
            Error::Certificate(_) | Error::Decrypt | Error::Protocol | Error::TooLong | Error::Truncated => {
                Fault::Invalid
            }
            Error::Alert(_) => Fault::Reset,
            Error::Stream(fault) => fault,
            Error::Other => Fault::Other,
        }
    }
}

/// Why the server's certificate was refused, at the wall time of the step
/// that started the handshake, for the server's name.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Certificate {
    /// It expired.
    Expired,
    /// It is not valid yet.
    NotYetValid,
    /// It is not valid for the server's name.
    Name,
    /// It does not chain to a root the client trusts.
    Issuer,
    /// Anything else: a bad signature or encoding, a usage it does not
    /// allow.
    Invalid,
}

/// What the client is waiting for. Machines keep no timers
/// (programming-model.md, 4): each says what it waits for, and the
/// connection arms the deadlines.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Waiting {
    /// For the side above to start the handshake.
    Handshake,
    /// For the handshake: the server's records, or room for TLS's own. The
    /// connection arms its handshake deadline.
    Handshaking,
    /// For room below: the peer is not reading what the client sends, or,
    /// closing, its `close_notify`.
    Room,
    /// For the server's records, for a demand of the side above's.
    Bytes,
    /// For the side above: to demand, send, finish or close.
    Above,
    /// For the side above to close it: it failed.
    Close,
    /// For nothing: it is closed.
    Nothing,
}

/// A TLS client connection: its state for this machine.
#[derive(Debug)]
pub struct Client {
    state: State,
    buffers: Buffers,
}

/// What a client holds whatever its state, allocated with it.
#[derive(Debug)]
struct Buffers {
    /// The plaintext the side above has not demanded yet (lib.md, 7).
    intake: Intake,
    /// The ciphertext received, which rustls has not discarded.
    records: Held,
    /// What TLS owes the stream below: its flights, `close_notify`.
    owed: Held,
}

impl Client {
    /// A client of the server `name`, with `config`, under `limits`, the
    /// limits its steps will be given. It demands nothing until the
    /// handshake is asked for.
    #[must_use]
    pub fn new(config: &Config, name: Name, limits: &Limits) -> Client {
        assert!(worst_case(limits).is_some(), "the limits are honoured: a read and room of a byte, a record held");
        let cap = intake_cap(limits).expect("checked by worst_case");
        let buffers = Buffers {
            intake: Intake::with_capacity(cap),
            records: Held::with_capacity(limits.records),
            owed: Held::with_capacity(FLIGHT),
        };
        Client { state: State::Fresh(Fresh { config: config.clone(), name, line: Line::Open }), buffers }
    }

    /// What it is waiting for: a function of its state alone.
    #[must_use]
    pub fn waiting(&self) -> Waiting {
        match &self.state {
            State::Fresh(_) => Waiting::Handshake,
            State::Open(open) => match open.handshake {
                Handshake::Running => Waiting::Handshaking,
                Handshake::Done => match open.below.demand {
                    Some(demand) if demand.room.bytes() > 0 => Waiting::Room,
                    Some(_) => Waiting::Bytes,
                    None => Waiting::Above,
                },
            },
            State::Closing(_) => Waiting::Room,
            State::Failed(_) => Waiting::Close,
            State::Closed => Waiting::Nothing,
        }
    }
}

/// An event from the stream below. Emits at most [`UP_MAX_OUT`]. It has
/// the shape of every entry point, though nothing below reads `env`: the
/// wall time was read when the handshake started.
pub fn up(client: &mut Client, _env: &Env<Limits>, ev: Up, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    let buffers = &mut client.buffers;
    let state = mem::replace(&mut client.state, State::Closed);
    client.state = match state {
        State::Fresh(fresh) => State::Fresh(fresh_up(fresh, ev)),
        State::Open(open) => open_up(open, buffers, ev, above, below),
        State::Failed(demand) => State::Failed(failed_up(demand, ev)),
        State::Closing(closing) => closing_up(closing, &mut buffers.owed, ev, above, below),
        // An answer to the demand the close withdrew, on its way (lib.md, 7);
        // an end or a failure, which change nothing now.
        State::Closed => State::Closed,
    };
}

/// A request from the side above. Emits at most [`DOWN_MAX_OUT`].
pub fn down(client: &mut Client, env: &Env<Limits>, rq: Request, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    let limits = &env.limits;
    let buffers = &mut client.buffers;
    let state = mem::replace(&mut client.state, State::Closed);
    client.state = match rq {
        Request::Handshake => match state {
            State::Fresh(fresh) => start(fresh, buffers, env, above, below),
            State::Open(_) | State::Failed(_) => unreachable!("one Handshake"),
            State::Closing(_) | State::Closed => unreachable!("a Handshake after Close"),
        },
        Request::Stream(down) => match state {
            State::Open(open) => stream(open, down, buffers, limits, above, below),
            // On its way when the failure went up: dropped.
            State::Failed(demand) => State::Failed(demand),
            State::Fresh(_) => unreachable!("the plaintext stream before the handshake was asked for"),
            State::Closing(_) | State::Closed => unreachable!("the plaintext stream after Close"),
        },
        Request::Close => close(state, buffers, above, below),
    };
}

/// What the client is doing.
#[derive(Debug)]
enum State {
    /// Made: the handshake not asked for yet.
    Fresh(Fresh),
    /// The handshake asked for, and the connection after it.
    Open(Open),
    /// The connection failed, and the side above was told: waiting for its
    /// close, with the demand outstanding below, if any, which the close
    /// withdraws.
    Failed(Option<Demand>),
    /// Closed by the side above: what TLS owes, `close_notify` last, goes
    /// below, then `Closed`.
    Closing(Closing),
    /// Closed: terminal, and the placeholder of every transition.
    Closed,
}

/// What a client is made with, until its handshake, and the stream below as
/// it knows it meanwhile.
#[derive(Debug)]
struct Fresh {
    config: Config,
    name: Name,
    line: Line,
}

/// The stream below, before the handshake demanded anything of it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Line {
    Open,
    /// It ended: the handshake can never be answered.
    Ended,
    Failed(Fault),
}

/// A connection, from its handshake on.
#[derive(Debug)]
struct Open {
    tls: Connection,
    handshake: Handshake,
    /// What the server's records came to.
    peer: Peer,
    /// The stream written: `close_notify`, then the stream below finished.
    writing: Writing,
    below: Below,
    above: Above,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Handshake {
    Running,
    /// Done, and `Ready` went up.
    Done,
}

/// How the server's side of the connection stands.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Peer {
    Open,
    /// Its `close_notify` was read: what the intake holds, then `End`.
    Closed,
    /// The stream below ended without it: what the intake holds, then the
    /// connection fails, `Truncated`.
    Cut,
}

/// How the side above's writing stands.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Writing {
    Open,
    /// The side above finished: `close_notify` is owed once the handshake is
    /// done.
    Finishing,
    /// `close_notify` is written, and owed below with the rest.
    Flushing,
    /// It went, and the stream below was finished.
    Finished,
}

/// The stream below, as the client knows it.
#[derive(Debug)]
struct Below {
    /// The demand outstanding, if any: stated only when none is, and
    /// answered at most once (lib.md, 7).
    demand: Option<Demand>,
    /// Room granted for the side above, which it has not sent within yet;
    /// or, once it withdrew or finished, which TLS may send within.
    granted: u32,
    /// The part of the next record to read.
    record: Record,
    /// The stream ended: it reads nothing more.
    ended: bool,
}

/// A demand stated below: what it reads, and what its room is for.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Demand {
    read: Read,
    room: Room,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Room {
    None,
    /// For TLS's own output: this much of what it owes.
    Owed(u32),
    /// For the side above's demand of `plaintext` bytes of room: what they
    /// take in records.
    Above {
        ciphertext: u32,
        plaintext: u32,
    },
}

impl Room {
    fn bytes(self) -> u32 {
        match self {
            Room::None => 0,
            Room::Owed(bytes) | Room::Above { ciphertext: bytes, .. } => bytes,
        }
    }
}

/// The part of a record read next.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Record {
    Header,
    /// Its body, of this many bytes, at least one.
    Body(u32),
}

/// The side above's side of the plaintext stream.
#[derive(Debug)]
struct Above {
    /// Its demand outstanding, if any.
    demand: Option<Wanted>,
    /// Room told, which it may send within, once.
    grant: Option<u32>,
    reading: Reading,
}

/// A demand of the side above's.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Wanted {
    read: Read,
    room: u32,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Reading {
    Open,
    /// It withdrew its demand: it reads no more, and is closing.
    Withdrawn,
    /// It was told `End`.
    Ended,
}

/// A close sending what TLS owes.
#[derive(Debug)]
struct Closing {
    /// The demand outstanding below, if any: one for room is kept, as an
    /// answer of room may be on its way; one that only reads is withdrawn.
    demand: Option<Demand>,
}

/// `Handshake`: rustls's connection, with its `ClientHello` owed.
fn start(
    fresh: Fresh,
    buffers: &mut Buffers,
    env: &Env<Limits>,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    let connected = match fresh.line {
        Line::Open => session::connect(&fresh.config, fresh.name, env.wall),
        Line::Ended => Err(Error::Truncated),
        Line::Failed(fault) => Err(Error::Stream(fault)),
    };
    let tls = match connected {
        Ok(tls) => tls,
        Err(error) => {
            above.push(Event::Stream(Up::Failed(error.fault())));
            above.push(Event::Failed(error));
            return State::Failed(None);
        }
    };
    let open = Open {
        tls,
        handshake: Handshake::Running,
        peer: Peer::Open,
        writing: Writing::Open,
        below: Below { demand: None, granted: 0, record: Record::Header, ended: false },
        above: Above { demand: None, grant: None, reading: Reading::Open },
    };
    processed(open, buffers, above, below)
}

/// An event below before the handshake: only the stream's end or failure,
/// as nothing is demanded. A failure after the end says only that the
/// stream cannot send (lib.md, 7), which fails the handshake all the same.
fn fresh_up(mut fresh: Fresh, ev: Up) -> Fresh {
    fresh.line = match ev {
        Up::End => match fresh.line {
            Line::Open => Line::Ended,
            Line::Ended | Line::Failed(_) => fresh.line,
        },
        Up::Failed(fault) => match fresh.line {
            Line::Open | Line::Ended => Line::Failed(fault),
            Line::Failed(_) => fresh.line,
        },
        Up::Bytes(_) | Up::Room => unreachable!("an answer before the handshake demanded anything"),
    };
    fresh
}

/// An event below while open.
fn open_up(mut open: Open, buffers: &mut Buffers, ev: Up, above: &mut Queue<Event>, below: &mut Queue<Down>) -> State {
    match ev {
        Up::Bytes(bytes) => {
            let demand = open.below.demand.take().expect("bytes answer a demand");
            assert!(demand.read == Read::Fill(fill(&bytes)), "bytes answer a fill, exactly");
            buffers.records.append(&bytes).expect("a piece of a record is read only when the records have room");
            open.below.record = match open.below.record {
                Record::Header => match length(&bytes) {
                    0 => Record::Header,
                    length => Record::Body(length),
                },
                Record::Body(length) => {
                    // A record's plaintext is no longer than its body.
                    let plaintext = length.min(MAX_PLAINTEXT);
                    assert!(buffers.intake.room() >= plaintext, "a record is read with room for its plaintext");
                    Record::Header
                }
            };
            // Copied into the records, it is dropped before rustls deciphers
            // them and copies the plaintext out.
            drop(bytes);
            // rustls judges a header as it comes: its type, its version, and
            // a length within what TLS allows.
            processed(open, buffers, above, below)
        }
        Up::Room => {
            let demand = open.below.demand.take().expect("room answers a demand");
            match demand.room {
                Room::Owed(room) => below.push(Down::Send(buffers.owed.take(room))),
                Room::Above { ciphertext, plaintext } => {
                    open.below.granted = ciphertext;
                    // Unless the side above withdrew meanwhile: the room is
                    // then TLS's, for close_notify.
                    if let Some(wanted) = open.above.demand.take() {
                        assert!(wanted.room == plaintext, "room answers the side above's demand");
                        open.above.grant = Some(plaintext);
                        above.push(Event::Stream(Up::Room));
                    }
                }
                Room::None => unreachable!("room answers a demand for room"),
            }
            settle(open, buffers, above, below)
        }
        Up::End => {
            open.below.ended = true;
            open.below.demand = ended(open.below.demand, below);
            match open.handshake {
                Handshake::Running => return fail(open, Error::Truncated, above),
                Handshake::Done => {}
            }
            if open.peer == Peer::Open {
                open.peer = Peer::Cut;
            }
            settle(open, buffers, above, below)
        }
        // Nothing follows a failure: what was outstanding is over.
        Up::Failed(fault) => {
            open.below.demand = None;
            fail(open, Error::Stream(fault), above)
        }
    }
}

/// The plaintext stream, from the side above.
fn stream(
    mut open: Open,
    down: Down,
    buffers: &mut Buffers,
    limits: &Limits,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    match down {
        // A withdrawal: the side above reads no more (lib.md, 7), and closes
        // next. What the client reads below for it may still come, and is
        // dropped with the intake. After the end, it gives up the read that
        // crossed it, and may write on.
        Down::Demand { read: Read::Nothing, room: 0 } => {
            open.above.demand = None;
            if open.above.reading == Reading::Open {
                open.above.reading = Reading::Withdrawn;
            }
        }
        Down::Demand { read, room } => {
            assert!(
                open.above.demand.is_none(),
                "one demand at a time, stated after the last was answered (lib.md, 7)"
            );
            let wanted = match read {
                Read::Nothing => 0,
                Read::Fill(n) => n,
                Read::Scan { max, .. } | Read::Line { max } => max,
            };
            assert!(wanted <= limits.read, "no read past Limits::read");
            assert!(
                read == Read::Nothing || open.above.reading != Reading::Withdrawn,
                "no read after a withdrawal: the side above reads no more"
            );
            assert!(room <= limits.send, "no room past Limits::send");
            assert!(room == 0 || open.above.grant.is_none(), "room is demanded once the last grant was sent within");
            assert!(room == 0 || open.writing == Writing::Open, "no room after Finish");
            open.above.demand = Some(Wanted { read, room });
        }
        Down::Send(bytes) => return sent(open, &bytes, buffers, above, below),
        Down::Finish => {
            assert!(open.writing == Writing::Open, "one Finish");
            let wanted = match open.above.demand {
                Some(wanted) => wanted,
                None => Wanted { read: Read::Nothing, room: 0 },
            };
            assert!(wanted.room == 0, "Finish with no room demanded");
            // A read the client is still reading below for would hold
            // close_notify behind it, while the server may wait for the end
            // of what it is sent before it answers.
            assert!(
                wanted.read == Read::Nothing || open.above.reading == Reading::Ended,
                "Finish with no read outstanding, but one that crossed the end"
            );
            // Nothing more to send: a grant not sent within is TLS's now.
            open.above.grant = None;
            open.writing = Writing::Finishing;
        }
    }
    settle(open, buffers, above, below)
}

/// A `Send` from the side above: encrypted, after what TLS owes, within the
/// room granted below for it.
fn sent(
    mut open: Open,
    bytes: &[u8],
    buffers: &mut Buffers,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    let grant = open.above.grant.take().expect("a Send within the room granted (lib.md, 7)");
    assert!(bytes.len() <= usize::try_from(grant).expect("a u32 fits in a usize"), "a Send within the room granted");
    let room = mem::replace(&mut open.below.granted, 0);
    match session::encrypt(&mut open.tls, &mut buffers.records, bytes, &mut buffers.owed) {
        Ok(Some(sealed)) => {
            assert!(
                sealed.len() <= usize::try_from(room).expect("a u32 fits in a usize"),
                "the records within the room granted below: OVERHEAD and SLACK"
            );
            below.push(Down::Send(sealed));
        }
        Ok(None) => {}
        Err(error) => return fail(open, error, above),
    }
    settle(open, buffers, above, below)
}

/// rustls handed what the records hold; then the handshake's end, if this
/// was it, and where the connection goes.
fn processed(mut open: Open, buffers: &mut Buffers, above: &mut Queue<Event>, below: &mut Queue<Down>) -> State {
    match session::process(&mut open.tls, &mut buffers.records, &mut buffers.intake, &mut buffers.owed) {
        Ok(true) => open.peer = Peer::Closed,
        Ok(false) => {}
        Err(error) => return fail(open, error, above),
    }
    if open.handshake == Handshake::Running && !session::handshaking(&open.tls) {
        open.handshake = Handshake::Done;
        above.push(Event::Ready(session::agreed(&open.tls)));
    }
    settle(open, buffers, above, below)
}

/// Where an open connection goes after every transition, in one place
/// (programming-model.md, 5.4): `close_notify` written once it is owed; the
/// side above's demand met from the intake, or the end or truncation told;
/// what TLS owes sent within room it holds; the stream below finished once
/// `close_notify` went; and the next demand below, if none is outstanding
/// and the state wants one.
fn settle(mut open: Open, buffers: &mut Buffers, above: &mut Queue<Event>, below: &mut Queue<Down>) -> State {
    if open.writing == Writing::Finishing && open.handshake == Handshake::Done {
        if let Err(error) = session::close_notify(&mut open.tls, &mut buffers.records, &mut buffers.owed) {
            return fail(open, error, above);
        }
        open.writing = Writing::Flushing;
    }
    if open.handshake == Handshake::Done && !answer(&mut open, &mut buffers.intake, above) {
        return fail(open, Error::Truncated, above);
    }
    // Room the side above gave up, or held when it withdrew: what TLS owes
    // goes within it. While the side above holds it, what TLS owes goes
    // with the side above's `Send`.
    if !buffers.owed.is_empty() && open.below.granted > 0 && open.above.grant.is_none() {
        let room = mem::replace(&mut open.below.granted, 0);
        below.push(Down::Send(buffers.owed.take(room)));
    }
    if open.writing == Writing::Flushing && buffers.owed.is_empty() {
        below.push(Down::Finish);
        open.writing = Writing::Finished;
    }
    if open.below.demand.is_none() {
        match wanted(&open, buffers) {
            Ok(Some(demand)) => {
                below.push(Down::Demand { read: demand.read, room: demand.room.bytes() });
                open.below.demand = Some(demand);
            }
            Ok(None) => {}
            Err(error) => return fail(open, error, above),
        }
    }
    State::Open(open)
}

/// The side above's demand met from the intake, or the end of what the
/// server sent told: `End` once its `close_notify` was read, and nothing held
/// meets a demand; with none outstanding, once nothing is held (lib.md, 7).
/// `false` where a truncation is due instead.
fn answer(open: &mut Open, intake: &mut Intake, above: &mut Queue<Event>) -> bool {
    if open.above.reading != Reading::Open {
        return true;
    }
    let reads = match open.above.demand {
        Some(wanted) => wanted.read,
        None => Read::Nothing,
    };
    if reads != Read::Nothing {
        if let Some(bytes) = intake.meet(reads) {
            open.above.demand = None;
            above.push(Event::Stream(Up::Bytes(bytes)));
            return true;
        }
    } else if !intake.is_empty() {
        return true;
    }
    match open.peer {
        Peer::Open => true,
        Peer::Cut => false,
        Peer::Closed => {
            // The end answers nothing: a read outstanding stays so, never
            // met, until the side above withdraws it, as io keeps it
            // (lib.md, 7); room may still be granted.
            open.above.reading = Reading::Ended;
            above.push(Event::Stream(Up::End));
            true
        }
    }
}

/// What the client demands below, when nothing is outstanding: the next
/// piece of a record, while the handshake runs, or for the side above's
/// read the intake does not meet; with room for what TLS owes, or else for
/// what the side above demands, once the handshake is done. No room while
/// room granted is not sent within, nor once the stream below is finished.
fn wanted(open: &Open, buffers: &Buffers) -> Result<Option<Demand>, Error> {
    let reads = !open.below.ended
        && open.peer == Peer::Open
        && match open.handshake {
            Handshake::Running => true,
            Handshake::Done => {
                open.above.reading == Reading::Open
                    && match open.above.demand {
                        Some(wanted) => wanted.read != Read::Nothing,
                        None => false,
                    }
            }
        };
    let read = if reads {
        let piece = match open.below.record {
            Record::Header => HEADER,
            Record::Body(length) => length,
        };
        // A handshake message the records cannot hold whole.
        if buffers.records.room() < piece {
            return Err(Error::TooLong);
        }
        Read::Fill(piece)
    } else {
        Read::Nothing
    };
    let room = if open.below.granted > 0 || open.above.grant.is_some() || open.writing == Writing::Finished {
        Room::None
    } else if !buffers.owed.is_empty() {
        Room::Owed(buffers.owed.len())
    } else if open.handshake == Handshake::Done && open.writing == Writing::Open {
        match open.above.demand {
            Some(Wanted { room: 0, .. }) | None => Room::None,
            Some(Wanted { room, .. }) => {
                Room::Above { ciphertext: room_for(room).expect("checked by worst_case"), plaintext: room }
            }
        }
    } else {
        Room::None
    };
    if read == Read::Nothing && room == Room::None {
        return Ok(None);
    }
    Ok(Some(Demand { read, room }))
}

/// The connection failed: the plaintext stream is told, unless the side
/// above withdrew its demand, then `Failed`; the client waits for its close.
/// rustls's connection is dropped, and nothing more is sent.
fn fail(open: Open, error: Error, above: &mut Queue<Event>) -> State {
    match open.above.reading {
        Reading::Withdrawn => {}
        Reading::Open | Reading::Ended => above.push(Event::Stream(Up::Failed(error.fault()))),
    }
    above.push(Event::Failed(error));
    State::Failed(open.below.demand)
}

/// The demand outstanding below once the stream ended. A read outstanding
/// crosses the end and is never met, but stays outstanding, as io keeps it
/// (lib.md, 7; io.md, 3.3): TLS reads no more after the end, so it
/// withdraws a demand that only reads, which leaves it free to demand room.
/// One with room keeps it, as room may still be granted.
fn ended(demand: Option<Demand>, below: &mut Queue<Down>) -> Option<Demand> {
    match demand {
        Some(Demand { room: Room::None, .. }) => {
            below.push(Down::Demand { read: Read::Nothing, room: 0 });
            None
        }
        Some(Demand { room, .. }) => Some(Demand { read: Read::Nothing, room }),
        None => None,
    }
}

/// An event below after the connection failed: an answer to the demand
/// outstanding, which ends it, or the stream's end or failure. A read that
/// crosses the end stays outstanding, for the close to withdraw.
fn failed_up(demand: Option<Demand>, ev: Up) -> Option<Demand> {
    match ev {
        Up::Bytes(_) | Up::Room | Up::Failed(_) => None,
        Up::End => demand,
    }
}

/// `Close`, in any state.
fn close(state: State, buffers: &mut Buffers, above: &mut Queue<Event>, below: &mut Queue<Down>) -> State {
    match state {
        State::Fresh(_) => closed(None, above, below),
        State::Failed(demand) => closed(demand, above, below),
        State::Open(open) => close_open(open, buffers, above, below),
        State::Closing(_) | State::Closed => unreachable!("a Close after Close"),
    }
}

/// `Close` of an open connection: `close_notify`, unless the handshake is not
/// done or it was written already, then what TLS owes goes below.
fn close_open(mut open: Open, buffers: &mut Buffers, above: &mut Queue<Event>, below: &mut Queue<Down>) -> State {
    if open.handshake == Handshake::Running {
        return closed(open.below.demand, above, below);
    }
    match open.writing {
        Writing::Open | Writing::Finishing => {
            let written = session::close_notify(&mut open.tls, &mut buffers.records, &mut buffers.owed);
            if written.is_err() {
                return closed(open.below.demand, above, below);
            }
        }
        Writing::Flushing => {}
        Writing::Finished => return closed(open.below.demand, above, below),
    }
    // A demand that only reads is withdrawn; one for room is kept, as its
    // answer may be on its way, and what TLS owes goes within it.
    let demand = match open.below.demand {
        Some(Demand { room: Room::None, .. }) => {
            below.push(Down::Demand { read: Read::Nothing, room: 0 });
            None
        }
        Some(Demand { room: room @ (Room::Owed(_) | Room::Above { .. }), read }) => Some(Demand { read, room }),
        None => None,
    };
    // Room the side above held and did not send within is TLS's now.
    if open.below.granted > 0 {
        below.push(Down::Send(buffers.owed.take(open.below.granted)));
    }
    closing_settle(Closing { demand }, &mut buffers.owed, above, below)
}

/// Closed at once: the demand outstanding below withdrawn, if any.
fn closed(demand: Option<Demand>, above: &mut Queue<Event>, below: &mut Queue<Down>) -> State {
    if demand.is_some() {
        below.push(Down::Demand { read: Read::Nothing, room: 0 });
    }
    above.push(Event::Closed);
    State::Closed
}

/// An event below while closing.
fn closing_up(
    mut closing: Closing,
    owed: &mut Held,
    ev: Up,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) -> State {
    match ev {
        // An answer to the demand outstanding, if it reads; otherwise one on
        // its way to the demand withdrawn. Either is dropped.
        Up::Bytes(_) => {
            closing.demand = match closing.demand {
                Some(Demand { read: Read::Nothing, room }) => Some(Demand { read: Read::Nothing, room }),
                Some(Demand { read: Read::Fill(_) | Read::Scan { .. } | Read::Line { .. }, .. }) | None => None,
            };
        }
        Up::Room => {
            let demand = closing.demand.take().expect("room answers the demand outstanding");
            let room = demand.room.bytes();
            assert!(room > 0, "room answers a demand for room");
            below.push(Down::Send(owed.take(room)));
        }
        Up::End => closing.demand = ended(closing.demand, below),
        // Nothing more can be sent.
        Up::Failed(_) => {
            above.push(Event::Closed);
            return State::Closed;
        }
    }
    closing_settle(closing, owed, above, below)
}

/// Where a close goes: `Closed` once nothing is owed; otherwise room
/// demanded for it, if no demand is outstanding.
fn closing_settle(mut closing: Closing, owed: &mut Held, above: &mut Queue<Event>, below: &mut Queue<Down>) -> State {
    if owed.is_empty() {
        return closed(closing.demand, above, below);
    }
    if closing.demand.is_none() {
        let demand = Demand { read: Read::Nothing, room: Room::Owed(owed.len()) };
        below.push(Down::Demand { read: demand.read, room: demand.room.bytes() });
        closing.demand = Some(demand);
    }
    State::Closing(closing)
}

/// The length a record's header gives.
fn length(header: &[u8]) -> u32 {
    let [_, _, _, high, low] = *header else { unreachable!("a header is five bytes: a fill of HEADER") };
    u32::from(u16::from_be_bytes([high, low]))
}

/// The fill `bytes` answer.
fn fill(bytes: &[u8]) -> u32 {
    u32::try_from(bytes.len()).expect("a delivery within LARGEST_READ")
}
