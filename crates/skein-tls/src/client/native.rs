//! Native TLS output beside independent plaintext and ciphertext reads.
//! The caller selects this face at construction; it shares the concrete rustls
//! engine and buffers with the classic client (tls.md, 3.6; lib.md, 7.1).
//!
//! It keeps one upper read, one upper output cell, one exact ciphertext read
//! and one lower output cell, plus one exact withdrawn Fill winner size,
//! bounded intake/record/owed buffers and a
//! checked never-reused lower token generator. A held upper grant retains its
//! actual lower byte and Send-slot backing while real control records grow the
//! owed prefix; encryption copies that prefix before the plaintext ciphertext.
//! It never knows socket state, kernel progress, timers, current lower credit
//! or framing. The resource owner supplies actual named terminals and Closed.
//!
//! `down` accepts one Handshake, read-only classic Stream and native output;
//! native Finish requires Ready, no live upper read and no pending output.
//! A ready withdrawn/finished read drops its one actual queued Fill winner;
//! handshake-internal reads keep progressing through genuine Ready.
//! `up` accepts actual Fill bytes/read faults, native terminals and physical
//! Closed. Identity is checked before a matching output's size. A local Close
//! waits for physical Closed and all actual lower settlements. Unsolicited
//! lower cancellation reports `TransportClosing`; physical Closed settles any
//! still-Wanted upper output before `TransportClosed`, including before Ready.
//! A coordinated native Close delivered after `TransportClosed` is inert.
//! Each caller reserves the documented per-entry queue maxima and drains them;
//! construction checks concrete configured read/byte/queued-Send caps before
//! allocating. Dynamic unavailable credit leaves an honest unanswered right.

use alloc::boxed::Box;
use core::mem;

use skein_lib::stream::{Down, OutputDown, OutputOutcome, OutputUp, Read, Up};
use skein_lib::{Env, Intake, Queue, Token};

use super::{
    Buffers, Error, FLIGHT, Fresh, HEADER, Handshake, LARGEST_READ, Limits, Line, MAX_PLAINTEXT, Peer, Reading, Record,
    Waiting, Writing,
};
use crate::held::Held;
use crate::session::{self, Connection};
use crate::{Config, MaxOut, Name};

/// Upper read/handshake/close requests or named plaintext output from the owner.
/// Output never answers a read (tls.md, 3.6; lib.md, 7.1).
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Request {
    /// Handshake once, read-only Stream or Close. Positive classic room and
    /// classic Send violate receiving rules before effects (tls.md, 3.6).
    Client(
        /// Owner's moved handshake/read/close request, preserving the contained
        /// receiving caps and native Finish rules (tls.md, 3.6; lib.md, 7.1).
        super::Request,
    ),

    /// An independent plaintext right, within `Limits.send`; one Settled answers
    /// admitted Room, and one Send/Release spends a grant (tls.md, 3.6; lib.md, 7.1).
    Output(
        /// Owner's moved named plaintext request/box within `Limits.send`; Room
        /// receives one terminal, Send/Release spends backing (tls.md, 3.6; lib.md, 7.1).
        OutputDown,
    ),
}

/// Actual native TLS observations to its owner (tls.md, 3.6).
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Event {
    /// Ready, plaintext read/End/Failed, TLS Failed, or a local Close's Closed.
    /// Native Handshake may instead end by `TransportClosed`; Failed(Other) also
    /// names checked local lower-token exhaustion (tls.md, 3.6).
    Client(
        /// TLS's moved actual handshake/read/local-close observation to its owner;
        /// it never settles an output right (tls.md, 3.6; lib.md, 7.1).
        super::Event,
    ),

    /// Exactly one terminal for admitted plaintext output; an emitted winner
    /// remains authoritative across cancellation/closing (tls.md, 3.6; lib.md, 7.1).
    Output(
        /// TLS's actual named plaintext terminal moved to its owner; Granted is
        /// authoritative and consumes no read (tls.md, 3.6; lib.md, 7.1).
        OutputUp,
    ),

    /// Actual unsolicited lower output cancellation. Owner closes the physical
    /// resource and requests native Close; TLS owns no timer (tls.md, 3.6).
    TransportClosing,

    /// Actual physical lower Closed, without a local Close. Ends Handshake and
    /// read lifecycle after settling any Wanted output; no invented EOF/fault
    /// or second `Client::Closed` follows (tls.md, 3.6).
    TransportClosed,
}

/// Requests to the real ciphertext endpoint (tls.md, 3.6; lib.md, 7.1).
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum LowerRequest {
    /// Read-only classic Fill/withdrawal or Finish; never combined output room.
    /// The physical owner remains responsible for Close (tls.md, 3.6).
    Stream(
        /// TLS's moved exact Fill/withdrawal/Finish request to the resource owner;
        /// lower read caps apply independently of output (tls.md, 3.6; lib.md, 7.1).
        Down,
    ),

    /// Named ciphertext bytes and one actual Send slot, independently of the
    /// lower read. TLS generates every token without reuse (tls.md, 3.6; lib.md, 7.1).
    Output(
        /// TLS's moved named ciphertext reservation or owned Send box; configured
        /// bytes and one Send slot back its terminal (tls.md, 3.6; lib.md, 7.1).
        OutputDown,
    ),
}

/// Actual observations from the exact ciphertext resource (tls.md, 3.6).
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum LowerEvent {
    /// Actual read Bytes/End/Failed. Classic Room is a receiving violation;
    /// none of these impersonates an output terminal (tls.md, 3.6).
    Stream(
        /// Resource owner's moved real read observation, including a queued Fill
        /// winner after withdrawal; no output settlement (tls.md, 3.6; lib.md, 7.1).
        Up,
    ),

    /// The real named lower terminal, observed even after local failure or Close
    /// and before physical Closed (tls.md, 3.6; lib.md, 7.1).
    Output(
        /// Resource owner's actual named terminal moved to TLS; a queued winner
        /// settles its exact reservation once (tls.md, 3.6; lib.md, 7.1).
        OutputUp,
    ),

    /// Actual physical IO Closed. All admitted lower output requests must have
    /// settled; pending upper output is cancelled before lifecycle termination.
    /// The bridge checks resource generation (tls.md, 3.6).
    Closed,
}

/// Maximum genuine Ready plus output Failed, plaintext Failed and TLS Failed;
/// below at most a withdrawal/Send/next independent Room (tls.md, 3.6).
pub const UP_MAX_OUT: MaxOut = MaxOut { above: 4, below: 3 };

/// Pending output terminal plus plaintext/TLS failure; below at most read
/// withdrawal, actual `close_notify` Send and Finish (tls.md, 3.6).
pub const DOWN_MAX_OUT: MaxOut = MaxOut { above: 3, below: 3 };

/// Ciphertext reservation includes the whole bounded owed-control prefix and
/// the original encryption/key-update allowance (tls.md, 3.6; lib.md, 7.1).
#[must_use]
pub fn room_for(plaintext: u32) -> Option<u32> {
    super::room_for(plaintext)?.checked_add(FLIGHT)
}

/// Checked startup output cap, with one actual Send slot, required below this
/// native client. Classic `largest_room` is unchanged (tls.md, 3.6).
#[must_use]
pub fn largest_room(limits: &Limits) -> Option<u32> {
    Some(room_for(limits.send)?.max(FLIGHT))
}

/// Native client value, buffers, rustls and transient work, including complete
/// owed-plus-encrypted allocation before emission or an error. Incoming Bytes
/// is dropped before process/encrypt, so work is their checked maximum. Caller
/// input and emitted boxes/queues remain separately owned (tls.md, 3.6).
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    let work = u64::from(largest_room(limits)?.max(LARGEST_READ));
    super::worst_case(limits)?
        .checked_sub(u64::from(LARGEST_READ))?
        .checked_add(work)?
        .checked_add(u64::try_from(size_of::<Client>()).ok()?)
}

#[derive(Debug)]
enum State {
    Fresh(Fresh),
    Open(Open),
    Failed,
    Closing,
    TransportClosing,
    Closed,
    TransportClosed,
}

#[derive(Debug)]
struct Open {
    tls: Connection,
    handshake: Handshake,
    peer: Peer,
    writing: Writing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpperOutput {
    Idle,
    Wanted { right: Token, bytes: u32 },
    Granted { right: Token, bytes: u32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Purpose {
    Owed,
    Upper { right: Token, bytes: u32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cancellation {
    None,
    Requested,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LowerOutput {
    Idle,
    Wanted { right: Token, bytes: u32, purpose: Purpose, cancellation: Cancellation },
    Granted { right: Token, bytes: u32 },
}

/// Configured receiving bounds of the concrete ciphertext endpoint. The owner
/// supplies these before construction; dynamic available credit is independent
/// (tls.md, 3.6; lib.md, 7.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LowerLimits {
    /// Maximum classic Fill accepted below, at least `LARGEST_READ` (tls.md, 3.6).
    pub read: u32,

    /// Total native output byte cap, at least `largest_room`(limits); actual
    /// flight and queued boxes both consume it (tls.md, 3.6; lib.md, 7.1).
    pub output: u32,

    /// Genuine queued Send slots, at least one, beside one IO flight/stalled
    /// Send. This is the configured N, not current free slots (tls.md, 3.6).
    pub sends: u32,
}

/// Explicitly selected native connection. Four independent bounded obligations
/// share the same session/held engine; no output history or hidden IO exists.
/// Its value is included in `worst_case` (tls.md, 3.6; lib.md, 7.1).
#[derive(Debug)]
pub struct Client {
    state: State,
    buffers: Buffers,
    upper_read: Read,
    reading: Reading,
    lower_read: Option<Read>,
    // One possible already-emitted Fill winner; withdrawal itself needs no acknowledgement.
    withdrawn_read: Option<Read>,
    record: Record,
    lower_ended: bool,
    upper_output: UpperOutput,
    lower_output: LowerOutput,
    next_lower: Option<u64>,
}

impl Client {
    /// Refuse incompatible configured read, output byte or queued-Send caps
    /// before any allocation/effect. At least one real queued Send slot and
    /// `LARGEST_READ` intake are checked independently of the byte envelope;
    /// dynamic unavailable credit remains an admitted waiting right, not startup
    /// refusal (tls.md, 3.6; lib.md, 7.1).
    #[must_use]
    pub fn new(config: &Config, name: Name, limits: &Limits, lower: &LowerLimits) -> Option<Client> {
        worst_case(limits)?;
        if lower.read < LARGEST_READ || lower.output < largest_room(limits)? || lower.sends == 0 {
            return None;
        }
        Some(Client {
            state: State::Fresh(Fresh { config: config.clone(), name, line: Line::Open }),
            buffers: Buffers {
                intake: Intake::with_capacity(super::intake_cap(limits)?),
                records: Held::with_capacity(limits.records),
                owed: Held::with_capacity(FLIGHT),
            },
            upper_read: Read::Nothing,
            reading: Reading::Open,
            lower_read: None,
            withdrawn_read: None,
            record: Record::Header,
            lower_ended: false,
            upper_output: UpperOutput::Idle,
            lower_output: LowerOutput::Idle,
            next_lower: Some(0),
        })
    }

    /// State-only deadline category; read and output can remain unanswered
    /// independently. The owner arms timers (tls.md, 3.6).
    #[must_use]
    pub fn waiting(&self) -> Waiting {
        match &self.state {
            State::Fresh(_) => Waiting::Handshake,
            State::Open(open) => match open.handshake {
                Handshake::Running => Waiting::Handshaking,
                Handshake::Done => match self.lower_output {
                    LowerOutput::Wanted { .. } => Waiting::Room,
                    LowerOutput::Idle | LowerOutput::Granted { .. } => {
                        if self.lower_read.is_some() {
                            Waiting::Bytes
                        } else {
                            Waiting::Above
                        }
                    }
                },
            },
            State::Closing => match self.lower_output {
                LowerOutput::Wanted { .. } | LowerOutput::Granted { .. } => Waiting::Room,
                LowerOutput::Idle => Waiting::Close,
            },
            State::Failed | State::TransportClosing => Waiting::Close,
            State::Closed | State::TransportClosed => Waiting::Nothing,
        }
    }
}

/// Consume one actual ciphertext observation. Caller reserves `UP_MAX_OUT` and
/// drains every real lower right even after failure/closing (tls.md, 3.6).
pub fn up(
    client: &mut Client,
    _env: &Env<Limits>,
    event: LowerEvent,
    above: &mut Queue<Event>,
    below: &mut Queue<LowerRequest>,
) {
    match event {
        LowerEvent::Stream(stream) => stream_up(client, stream, above, below),
        LowerEvent::Output(OutputUp::Settled { right, outcome }) => output_up(client, right, outcome, above, below),
        LowerEvent::Closed => transport_closed(client, above),
    }
    settle(client, above, below);
}

/// Consume an upper request with injected wall time. Caller reserves `DOWN_MAX_OUT`;
/// identity precedes payload checks, and native Close after `TransportClosed` is
/// inert before classic dispatch (tls.md, 3.6; lib.md, 7.1).
pub fn down(
    client: &mut Client,
    env: &Env<Limits>,
    request: Request,
    above: &mut Queue<Event>,
    below: &mut Queue<LowerRequest>,
) {
    match request {
        Request::Client(super::Request::Handshake) => start(client, env, above, below),
        Request::Client(super::Request::Stream(stream)) => stream_down(client, stream, &env.limits, above, below),
        Request::Client(super::Request::Close) => close(client, above, below),
        Request::Output(output) => output_down(client, output, &env.limits, above, below),
    }
    settle(client, above, below);
}

fn start(client: &mut Client, env: &Env<Limits>, above: &mut Queue<Event>, below: &mut Queue<LowerRequest>) {
    let state = mem::replace(&mut client.state, State::Closed);
    let fresh = match state {
        State::Fresh(fresh) => fresh,
        State::Open(_)
        | State::Failed
        | State::Closing
        | State::TransportClosing
        | State::Closed
        | State::TransportClosed => unreachable!("one native Handshake before closing"),
    };
    let connected = match fresh.line {
        Line::Open => session::connect(&fresh.config, fresh.name, env.wall),
        Line::Ended => Err(Error::Truncated),
        Line::Failed(fault) => Err(Error::Stream(fault)),
    };
    match connected {
        Ok(tls) => {
            client.state =
                State::Open(Open { tls, handshake: Handshake::Running, peer: Peer::Open, writing: Writing::Open });
            process(client, above, below);
        }
        Err(error) => fail(client, error, above, below),
    }
}

fn process(client: &mut Client, above: &mut Queue<Event>, below: &mut Queue<LowerRequest>) {
    let open = match &mut client.state {
        State::Open(open) => open,
        State::Fresh(_)
        | State::Failed
        | State::Closing
        | State::TransportClosing
        | State::Closed
        | State::TransportClosed => unreachable!("only open rustls processes records"),
    };
    match session::process(
        &mut open.tls,
        &mut client.buffers.records,
        &mut client.buffers.intake,
        &mut client.buffers.owed,
    ) {
        Ok(true) => open.peer = Peer::Closed,
        Ok(false) => {}
        Err(error) => {
            fail(client, error, above, below);
            return;
        }
    }
    if open.handshake == Handshake::Running && !session::handshaking(&open.tls) {
        open.handshake = Handshake::Done;
        above.push(Event::Client(super::Event::Ready(session::agreed(&open.tls))));
    }
}

fn stream_up(client: &mut Client, event: Up, above: &mut Queue<Event>, below: &mut Queue<LowerRequest>) {
    match event {
        Up::Room => unreachable!("native lower stream reads only"),
        Up::Bytes(bytes) => received(client, bytes, above, below),
        Up::End => {
            client.lower_ended = true;
            withdraw_read(client, below);
            match &mut client.state {
                State::Fresh(fresh) => fresh.line = Line::Ended,
                State::Open(open) => {
                    if open.handshake == Handshake::Running {
                        fail(client, Error::Truncated, above, below);
                    } else if open.peer == Peer::Open {
                        open.peer = Peer::Cut;
                    }
                }
                State::Failed | State::Closing | State::TransportClosing | State::Closed | State::TransportClosed => {}
            }
        }
        Up::Failed(fault) => {
            client.lower_read = None;
            client.lower_ended = true;
            match &mut client.state {
                State::Fresh(fresh) => fresh.line = Line::Failed(fault),
                State::Open(_) => fail(client, Error::Stream(fault), above, below),
                State::Closing => {
                    client.buffers.owed.clear();
                    cancel_lower(client, below);
                }
                State::Failed | State::TransportClosing | State::Closed | State::TransportClosed => {}
            }
        }
    }
}

fn received(client: &mut Client, bytes: Box<[u8]>, above: &mut Queue<Event>, below: &mut Queue<LowerRequest>) {
    match &client.state {
        State::Open(open) if open.handshake == Handshake::Done && client.reading == Reading::Withdrawn => {
            assert!(client.lower_read.is_none(), "ready withdrawal leaves no active ciphertext Fill");
            let read = client.withdrawn_read.take().expect("queued Bytes answers an actual withdrawn Fill");
            assert!(read == Read::Fill(super::fill(&bytes)), "withdrawn ciphertext Fill is answered exactly");
            return;
        }
        State::Open(_) => {}
        State::Fresh(_) => unreachable!("no native ciphertext read before Handshake"),
        State::Failed | State::Closing | State::TransportClosing | State::Closed | State::TransportClosed => {
            client.lower_read = None;
            return;
        }
    }
    let read = client.lower_read.take().expect("actual Bytes answers native ciphertext Fill");
    assert!(read == Read::Fill(super::fill(&bytes)), "ciphertext Fill is answered exactly");
    client.buffers.records.append(&bytes).expect("native read fits retained records");
    client.record = match client.record {
        Record::Header => match super::length(&bytes) {
            0 => Record::Header,
            length => Record::Body(length),
        },
        Record::Body(length) => {
            assert!(
                client.buffers.intake.room() >= length.min(MAX_PLAINTEXT),
                "plaintext room before native record read"
            );
            Record::Header
        }
    };
    drop(bytes);
    process(client, above, below);
}

fn stream_down(
    client: &mut Client,
    stream: Down,
    limits: &Limits,
    above: &mut Queue<Event>,
    below: &mut Queue<LowerRequest>,
) {
    match stream {
        Down::Send(_) => unreachable!("native plaintext uses named Output Send"),
        Down::Demand { read, room } => {
            assert!(room == 0, "native plaintext Demand is read-only");
            match &client.state {
                State::Open(open) => {
                    assert!(open.writing == Writing::Open, "no native read after Finish withdrawal");
                }
                State::Failed => return,
                State::Fresh(_) | State::Closing | State::TransportClosing | State::Closed | State::TransportClosed => {
                    unreachable!("native read in live admitted Handshake only")
                }
            }
            assert!(read_maximum(read) <= limits.read, "native plaintext read within cap");
            if read == Read::Nothing {
                assert!(client.upper_read != Read::Nothing, "only outstanding plaintext read is withdrawn");
                client.upper_read = Read::Nothing;
                client.reading = Reading::Withdrawn;
                match &client.state {
                    State::Open(open) if open.handshake == Handshake::Running => {}
                    State::Open(_) => withdraw_read(client, below),
                    State::Fresh(_)
                    | State::Failed
                    | State::Closing
                    | State::TransportClosing
                    | State::Closed
                    | State::TransportClosed => unreachable!("live native read withdrawal"),
                }
            } else {
                assert!(
                    client.reading != Reading::Withdrawn && client.upper_read == Read::Nothing,
                    "one upper native read before withdrawal"
                );
                client.upper_read = read;
            }
        }
        Down::Finish => finish(client, above, below),
    }
}

fn upper_terminal(client: &mut Client, outcome: OutputOutcome, above: &mut Queue<Event>) {
    let output = mem::replace(&mut client.upper_output, UpperOutput::Idle);
    match output {
        UpperOutput::Wanted { right, .. } => above.push(Event::Output(OutputUp::Settled { right, outcome })),
        UpperOutput::Idle | UpperOutput::Granted { .. } => {}
    }
}

fn output_down(
    client: &mut Client,
    output: OutputDown,
    limits: &Limits,
    above: &mut Queue<Event>,
    below: &mut Queue<LowerRequest>,
) {
    match output {
        OutputDown::Room { right, bytes } => {
            match &client.state {
                State::Open(open) => assert!(open.writing == Writing::Open, "native output before Finish"),
                State::Fresh(_)
                | State::Failed
                | State::Closing
                | State::TransportClosing
                | State::Closed
                | State::TransportClosed => unreachable!("Room requires live native writing"),
            }
            assert!(bytes > 0 && bytes <= limits.send, "positive plaintext reservation within cap");
            assert!(client.upper_output == UpperOutput::Idle, "one upper native output right");
            client.upper_output = UpperOutput::Wanted { right, bytes };
        }
        OutputDown::Cancel { right } => match client.upper_output {
            UpperOutput::Wanted { right: pending, .. } if right == pending => {
                upper_terminal(client, OutputOutcome::Cancelled, above);
                match client.lower_output {
                    LowerOutput::Wanted { purpose: Purpose::Upper { right: upper, .. }, .. } if upper == right => {
                        cancel_lower(client, below);
                    }
                    LowerOutput::Idle | LowerOutput::Wanted { .. } | LowerOutput::Granted { .. } => {}
                }
            }
            UpperOutput::Idle | UpperOutput::Wanted { .. } | UpperOutput::Granted { .. } => {}
        },
        OutputDown::Send { right, bytes } => match client.upper_output {
            UpperOutput::Granted { right: granted, bytes: cap } if granted == right => {
                assert!(
                    bytes.len() <= usize::try_from(cap).expect("plaintext cap fits"),
                    "matching native Send within grant"
                );
                client.upper_output = UpperOutput::Idle;
                send_plain(client, bytes, above, below);
            }
            UpperOutput::Idle | UpperOutput::Wanted { .. } | UpperOutput::Granted { .. } => {}
        },
        OutputDown::Release { right } => match client.upper_output {
            UpperOutput::Granted { right: granted, .. } if granted == right => {
                client.upper_output = UpperOutput::Idle;
            }
            UpperOutput::Idle | UpperOutput::Wanted { .. } | UpperOutput::Granted { .. } => {}
        },
    }
}

fn send_plain(client: &mut Client, plain: Box<[u8]>, above: &mut Queue<Event>, below: &mut Queue<LowerRequest>) {
    let lower = mem::replace(&mut client.lower_output, LowerOutput::Idle);
    let (right, bytes) = match lower {
        LowerOutput::Granted { right, bytes } => (right, bytes),
        LowerOutput::Idle | LowerOutput::Wanted { .. } => unreachable!("upper grant retains actual lower backing"),
    };
    let open = match &mut client.state {
        State::Open(open) => open,
        State::Fresh(_)
        | State::Failed
        | State::Closing
        | State::TransportClosing
        | State::Closed
        | State::TransportClosed => unreachable!("matching live native Send"),
    };
    match session::encrypt(&mut open.tls, &mut client.buffers.records, &plain, &mut client.buffers.owed) {
        Ok(Some(sealed)) => {
            assert!(
                sealed.len() <= usize::try_from(bytes).expect("ciphertext cap fits"),
                "complete native ciphertext within full owed envelope"
            );
            below.push(LowerRequest::Output(OutputDown::Send { right, bytes: sealed }));
        }
        Ok(None) => below.push(LowerRequest::Output(OutputDown::Release { right })),
        Err(error) => {
            below.push(LowerRequest::Output(OutputDown::Release { right }));
            fail(client, error, above, below);
        }
    }
}

fn cancel_lower(client: &mut Client, below: &mut Queue<LowerRequest>) {
    client.lower_output = match client.lower_output {
        LowerOutput::Wanted { right, bytes, purpose, cancellation: Cancellation::None } => {
            below.push(LowerRequest::Output(OutputDown::Cancel { right }));
            LowerOutput::Wanted { right, bytes, purpose, cancellation: Cancellation::Requested }
        }
        LowerOutput::Granted { right, .. } => {
            below.push(LowerRequest::Output(OutputDown::Release { right }));
            LowerOutput::Idle
        }
        LowerOutput::Idle => LowerOutput::Idle,
        wanted @ LowerOutput::Wanted { cancellation: Cancellation::Requested, .. } => wanted,
    };
}

fn output_up(
    client: &mut Client,
    right: Token,
    outcome: OutputOutcome,
    above: &mut Queue<Event>,
    below: &mut Queue<LowerRequest>,
) {
    let (expected, bytes, purpose, cancellation) = match client.lower_output {
        LowerOutput::Wanted { right, bytes, purpose, cancellation } => (right, bytes, purpose, cancellation),
        LowerOutput::Idle | LowerOutput::Granted { .. } => unreachable!("one actual terminal for admitted lower right"),
    };
    assert!(right == expected, "actual lower token matches the admitted obligation");
    client.lower_output = LowerOutput::Idle;
    match outcome {
        OutputOutcome::Granted => {
            client.lower_output = LowerOutput::Granted { right, bytes };
            if cancellation == Cancellation::None {
                match purpose {
                    Purpose::Upper { right: upper, bytes: plaintext } => match client.upper_output {
                        UpperOutput::Wanted { right: wanted, bytes: asked }
                            if wanted == upper && asked == plaintext =>
                        {
                            client.upper_output = UpperOutput::Granted { right: upper, bytes: plaintext };
                            above.push(Event::Output(OutputUp::Settled {
                                right: upper,
                                outcome: OutputOutcome::Granted,
                            }));
                        }
                        UpperOutput::Idle | UpperOutput::Wanted { .. } | UpperOutput::Granted { .. } => {}
                    },
                    Purpose::Owed => {}
                }
            }
        }
        OutputOutcome::Cancelled => {
            if cancellation == Cancellation::None {
                upper_terminal(client, OutputOutcome::Cancelled, above);
                match client.state {
                    State::Closing => client.buffers.owed.clear(),
                    State::Fresh(_) | State::Open(_) | State::Failed => {
                        client.state = State::TransportClosing;
                        client.buffers.owed.clear();
                        above.push(Event::TransportClosing);
                    }
                    State::TransportClosing | State::Closed | State::TransportClosed => {}
                }
                withdraw_read(client, below);
            }
        }
        OutputOutcome::Failed(fault) => match client.state {
            State::Open(_) | State::Fresh(_) => fail(client, Error::Stream(fault), above, below),
            State::Closing => client.buffers.owed.clear(),
            State::Failed | State::TransportClosing | State::Closed | State::TransportClosed => {}
        },
    }
}

fn withdraw_read(client: &mut Client, below: &mut Queue<LowerRequest>) {
    if let Some(read) = client.lower_read.take() {
        assert!(client.withdrawn_read.replace(read).is_none(), "one withdrawn ciphertext Fill winner");
        below.push(LowerRequest::Stream(Down::Demand { read: Read::Nothing, room: 0 }));
    }
}

fn fail(client: &mut Client, error: Error, above: &mut Queue<Event>, below: &mut Queue<LowerRequest>) {
    upper_terminal(client, OutputOutcome::Failed(error.fault()), above);
    if client.reading != Reading::Withdrawn {
        above.push(Event::Client(super::Event::Stream(Up::Failed(error.fault()))));
    }
    above.push(Event::Client(super::Event::Failed(error)));
    client.state = State::Failed;
    client.buffers.owed.clear();
    cancel_lower(client, below);
    withdraw_read(client, below);
    client.upper_read = Read::Nothing;
}

fn transport_closed(client: &mut Client, above: &mut Queue<Event>) {
    assert!(!lower_wanted(client.lower_output), "actual lower terminal precedes physical Closed");
    client.lower_output = LowerOutput::Idle;
    client.lower_read = None;
    client.withdrawn_read = None;
    client.upper_read = Read::Nothing;
    client.buffers.owed.clear();
    upper_terminal(client, OutputOutcome::Cancelled, above);
    match client.state {
        State::Closing => {
            above.push(Event::Client(super::Event::Closed));
            client.state = State::Closed;
        }
        State::Fresh(_) | State::Open(_) | State::Failed | State::TransportClosing => {
            above.push(Event::TransportClosed);
            client.state = State::TransportClosed;
        }
        State::Closed | State::TransportClosed => {}
    }
}

fn close(client: &mut Client, above: &mut Queue<Event>, below: &mut Queue<LowerRequest>) {
    match client.state {
        State::TransportClosed => return,
        State::Closing | State::Closed => unreachable!("one native local Close"),
        State::Fresh(_) | State::Open(_) | State::Failed | State::TransportClosing => {}
    }
    upper_terminal(client, OutputOutcome::Cancelled, above);
    client.upper_read = Read::Nothing;
    client.reading = Reading::Withdrawn;
    withdraw_read(client, below);
    let written = match &mut client.state {
        State::Open(open) if open.handshake == Handshake::Done && open.writing != Writing::Finished => {
            if open.writing == Writing::Open || open.writing == Writing::Finishing {
                session::close_notify(&mut open.tls, &mut client.buffers.records, &mut client.buffers.owed).is_ok()
            } else {
                true
            }
        }
        State::Fresh(_) | State::Open(_) | State::Failed | State::TransportClosing => false,
        State::Closing | State::Closed | State::TransportClosed => unreachable!("checked before local Close"),
    };
    if !written {
        client.buffers.owed.clear();
    }
    client.state = State::Closing;
    if client.buffers.owed.is_empty() {
        cancel_lower(client, below);
    }
}

fn finish(client: &mut Client, above: &mut Queue<Event>, below: &mut Queue<LowerRequest>) {
    let open = match &mut client.state {
        State::Open(open) => open,
        State::Fresh(_)
        | State::Failed
        | State::Closing
        | State::TransportClosing
        | State::Closed
        | State::TransportClosed => unreachable!("native Finish after Ready only"),
    };
    assert!(open.handshake == Handshake::Done && open.writing == Writing::Open, "native Finish after Ready once");
    assert!(
        client.upper_read == Read::Nothing || client.reading == Reading::Ended,
        "native Finish with no live upper read"
    );
    assert!(!upper_wanted(client.upper_output), "native Finish with no pending output");
    client.upper_output = UpperOutput::Idle;
    client.upper_read = Read::Nothing;
    client.reading = Reading::Withdrawn;
    open.writing = Writing::Flushing;
    match session::close_notify(&mut open.tls, &mut client.buffers.records, &mut client.buffers.owed) {
        Ok(()) => withdraw_read(client, below),
        Err(error) => fail(client, error, above, below),
    }
}

fn answer(client: &mut Client, above: &mut Queue<Event>) -> bool {
    let peer = match &client.state {
        State::Open(open) if open.handshake == Handshake::Done => open.peer,
        State::Fresh(_)
        | State::Open(_)
        | State::Failed
        | State::Closing
        | State::TransportClosing
        | State::Closed
        | State::TransportClosed => return true,
    };
    if client.reading != Reading::Open {
        return true;
    }
    if client.upper_read != Read::Nothing {
        if let Some(bytes) = client.buffers.intake.meet(client.upper_read) {
            client.upper_read = Read::Nothing;
            above.push(Event::Client(super::Event::Stream(Up::Bytes(bytes))));
            return true;
        }
    } else if !client.buffers.intake.is_empty() {
        return true;
    }
    match peer {
        Peer::Open => true,
        Peer::Cut => false,
        Peer::Closed => {
            client.reading = Reading::Ended;
            above.push(Event::Client(super::Event::Stream(Up::End)));
            true
        }
    }
}

fn request_room(
    client: &mut Client,
    bytes: u32,
    purpose: Purpose,
    above: &mut Queue<Event>,
    below: &mut Queue<LowerRequest>,
) {
    let Some(sequence) = client.next_lower else {
        match client.state {
            State::Closing => client.buffers.owed.clear(),
            State::Open(_) => fail(client, Error::Other, above, below),
            State::Fresh(_) | State::Failed | State::TransportClosing | State::Closed | State::TransportClosed => {
                unreachable!("only usable output needs a fresh lower token")
            }
        }
        return;
    };
    client.next_lower = sequence.checked_add(1);
    let right = Token::new(sequence);
    client.lower_output = LowerOutput::Wanted { right, bytes, purpose, cancellation: Cancellation::None };
    below.push(LowerRequest::Output(OutputDown::Room { right, bytes }));
}

fn settle(client: &mut Client, above: &mut Queue<Event>, below: &mut Queue<LowerRequest>) {
    match client.state {
        State::Open(_) => {
            if !answer(client, above) {
                fail(client, Error::Truncated, above, below);
                return;
            }
        }
        State::Closing => {}
        State::Fresh(_) | State::Failed | State::TransportClosing | State::Closed | State::TransportClosed => {
            if lower_granted(client.lower_output) {
                cancel_lower(client, below);
            }
            return;
        }
    }
    settle_owed(client, above, below);
    settle_upper(client, above, below);
    match &mut client.state {
        State::Open(open) => {
            if open.writing == Writing::Flushing
                && client.buffers.owed.is_empty()
                && client.lower_output == LowerOutput::Idle
            {
                below.push(LowerRequest::Stream(Down::Finish));
                open.writing = Writing::Finished;
            }
        }
        State::Fresh(_)
        | State::Failed
        | State::Closing
        | State::TransportClosing
        | State::Closed
        | State::TransportClosed => {}
    }
    next_read(client, above, below);
}

fn settle_upper(client: &mut Client, above: &mut Queue<Event>, below: &mut Queue<LowerRequest>) {
    let ready = match &client.state {
        State::Open(open) => open.handshake == Handshake::Done && open.writing == Writing::Open,
        State::Fresh(_)
        | State::Failed
        | State::Closing
        | State::TransportClosing
        | State::Closed
        | State::TransportClosed => false,
    };
    if !ready || !client.buffers.owed.is_empty() {
        return;
    }
    match client.upper_output {
        UpperOutput::Wanted { right, bytes } => match client.lower_output {
            LowerOutput::Idle => request_room(
                client,
                room_for(bytes).expect("checked native cap"),
                Purpose::Upper { right, bytes },
                above,
                below,
            ),
            LowerOutput::Wanted { .. } | LowerOutput::Granted { .. } => {}
        },
        UpperOutput::Idle | UpperOutput::Granted { .. } => {}
    }
}

fn settle_owed(client: &mut Client, above: &mut Queue<Event>, below: &mut Queue<LowerRequest>) {
    if upper_granted(client.upper_output) {
        return;
    }
    match client.lower_output {
        LowerOutput::Granted { right, bytes } => {
            client.lower_output = LowerOutput::Idle;
            if client.buffers.owed.is_empty() {
                below.push(LowerRequest::Output(OutputDown::Release { right }));
            } else {
                below.push(LowerRequest::Output(OutputDown::Send { right, bytes: client.buffers.owed.take(bytes) }));
            }
        }
        LowerOutput::Idle | LowerOutput::Wanted { .. } => {}
    }
    if !client.buffers.owed.is_empty() && client.lower_output == LowerOutput::Idle {
        match client.state {
            State::Open(_) | State::Closing => {
                request_room(client, client.buffers.owed.len(), Purpose::Owed, above, below);
            }
            State::Fresh(_) | State::Failed | State::TransportClosing | State::Closed | State::TransportClosed => {}
        }
    }
}

fn next_read(client: &mut Client, above: &mut Queue<Event>, below: &mut Queue<LowerRequest>) {
    if client.lower_ended || client.lower_read.is_some() {
        return;
    }
    let reads = match &client.state {
        State::Open(open) => {
            open.writing == Writing::Open
                && open.peer == Peer::Open
                && match open.handshake {
                    Handshake::Running => true,
                    Handshake::Done => client.reading == Reading::Open && client.upper_read != Read::Nothing,
                }
        }
        State::Fresh(_)
        | State::Failed
        | State::Closing
        | State::TransportClosing
        | State::Closed
        | State::TransportClosed => false,
    };
    if !reads {
        return;
    }
    let piece = match client.record {
        Record::Header => HEADER,
        Record::Body(bytes) => bytes,
    };
    if client.buffers.records.room() < piece {
        fail(client, Error::TooLong, above, below);
        return;
    }
    let read = Read::Fill(piece);
    client.lower_read = Some(read);
    below.push(LowerRequest::Stream(Down::Demand { read, room: 0 }));
}

fn upper_wanted(output: UpperOutput) -> bool {
    match output {
        UpperOutput::Wanted { .. } => true,
        UpperOutput::Idle | UpperOutput::Granted { .. } => false,
    }
}

fn upper_granted(output: UpperOutput) -> bool {
    match output {
        UpperOutput::Granted { .. } => true,
        UpperOutput::Idle | UpperOutput::Wanted { .. } => false,
    }
}

fn lower_wanted(output: LowerOutput) -> bool {
    match output {
        LowerOutput::Wanted { .. } => true,
        LowerOutput::Idle | LowerOutput::Granted { .. } => false,
    }
}

fn lower_granted(output: LowerOutput) -> bool {
    match output {
        LowerOutput::Granted { .. } => true,
        LowerOutput::Idle | LowerOutput::Wanted { .. } => false,
    }
}

fn read_maximum(read: Read) -> u32 {
    match read {
        Read::Nothing => 0,
        Read::Fill(bytes) | Read::Scan { max: bytes, .. } | Read::Line { max: bytes } => bytes,
    }
}

#[cfg(test)]
mod tests;
