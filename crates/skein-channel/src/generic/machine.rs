//! Generic channel's opening machine (channel.md, sections 5, 6 and 9).
//! It keeps the checked schema, opening phase, one partly filled frame, peer
//! terms, a bounded output queue and one independent output right. It never
//! knows credential policy, body meaning, deadlines or transport type.
//! `new`, `down`, `up` and `poll` are its entry points; the owner reserves
//! `MAX_UP` and `MAX_DOWN` slots first.
//!
//! | State | Event | Next state, emits |
//! |---|---|---|
//! | WaitOpen | Open frame | WaitOwner, Opening; invalid offer: Refusing |
//! | WaitOwner | Accept/Refuse | WaitTerms/Refusing, Accept and Terms/Refuse |
//! | Idle | Open request | WaitAccept, Open |
//! | WaitAccept | Accept/Refuse | WaitTerms/Refusing, Terms/Refused |
//! | WaitTerms | Terms | Ready when own Terms handed down; Ready |
//! | any active | Ping | same, Ping |
//! | any active | malformed frame | Refusing, Refuse |
//! | Refusing | output drained | Finished, Finish |
//! | any active | Close | Closed, Closed after output right settles |
use alloc::boxed::Box;
use core::mem;
use skein_lib::{List, Queue, Token, bytes, stream};

use super::boundary::{Closed, Event, Lower, LowerEvent, ReadWait, Request, Waiting, WriteWait};
use super::frame::{Control, Frame, Header, Term, control_frame, decode_control, parse_header};
use super::opening::{accept_open, check_terms, local_terms, offers_version};
use super::schema::{Limits, Role, Schema, SchemaError};

#[derive(Debug, PartialEq, Eq)]
enum Phase {
    Idle,
    WaitOpen,
    WaitOwner { lowest: u16, highest: u16 },
    WaitAccept,
    WaitTerms { version: u16 },
    Ready { version: u16 },
    Refusing { reason: u16 },
    Finished,
    Closed,
}

impl Phase {
    fn may_ping(&self) -> bool {
        match self {
            Phase::WaitOwner { .. } | Phase::WaitAccept | Phase::WaitTerms { .. } | Phase::Ready { .. } => true,
            Phase::Idle | Phase::WaitOpen | Phase::Refusing { .. } | Phase::Finished | Phase::Closed => false,
        }
    }

    fn may_end_input(&self) -> bool {
        match self {
            Phase::Ready { .. } | Phase::Finished | Phase::Closed => true,
            Phase::Idle
            | Phase::WaitOpen
            | Phase::WaitOwner { .. }
            | Phase::WaitAccept
            | Phase::WaitTerms { .. }
            | Phase::Refusing { .. } => false,
        }
    }

    fn refusing(&self) -> bool {
        match self {
            Phase::Refusing { .. } => true,
            Phase::Idle
            | Phase::WaitOpen
            | Phase::WaitOwner { .. }
            | Phase::WaitAccept
            | Phase::WaitTerms { .. }
            | Phase::Ready { .. }
            | Phase::Finished
            | Phase::Closed => false,
        }
    }
}

#[derive(Debug)]
enum ReadState {
    Header,
    Body { header: Header, body: Box<[u8]>, filled: u32 },
    Idle,
}

#[derive(Debug)]
struct Queued {
    frame: Frame,
    terms: bool,
}

/// One channel machine over a read stream and independent output stream.
#[derive(Debug)]
#[expect(clippy::struct_excessive_bools, reason = "each boolean tracks a separate lower obligation or one-time event")]
pub struct Machine {
    schema: Schema,
    role: Role,
    limits: Limits,
    phase: Phase,
    read: ReadState,
    read_outstanding: bool,
    output: Queue<Queued>,
    output_right: Option<Token>,
    output_granted: bool,
    next_right: u64,
    local_terms_sent: bool,
    peer_terms: Option<List<Term>>,
    finish_sent: bool,
    closed_sent: bool,
}

impl Machine {
    /// Checks the schema before keeping its bounded tables.
    pub fn new(schema: Schema, role: Role, limits: Limits) -> Result<Machine, SchemaError> {
        schema.check(&limits)?;
        let output_capacity = limits.output_frames.checked_add(3).ok_or(SchemaError::InvalidLimit)?;
        let (phase, read) = match role {
            Role::Initiator => (Phase::Idle, ReadState::Idle),
            Role::Responder => (Phase::WaitOpen, ReadState::Header),
        };
        Ok(Machine {
            schema,
            role,
            limits,
            phase,
            read,
            read_outstanding: false,
            output: Queue::with_capacity(output_capacity),
            output_right: None,
            output_granted: false,
            next_right: 1,
            local_terms_sent: false,
            peer_terms: None,
            finish_sent: false,
            closed_sent: false,
        })
    }

    /// Handles one owner request; `poll` afterwards emits the next demands.
    pub fn down(&mut self, request: Request, up: &mut Queue<Event>, below: &mut Queue<Lower>) {
        match request {
            Request::Open { credential } => self.open(credential),
            Request::Accept { version } => self.accept(version),
            Request::Refuse { reason, text } => self.refuse(reason, text),
            Request::Read => {}
            Request::Send { token, frame } => {
                drop(frame);
                up.push(Event::Unsent { token });
            }
            Request::Ping => self.ping(),
            Request::Finish => self.finish(),
            Request::Close => self.close(up, below),
        }
    }

    /// Handles exactly one lower event; `poll` afterwards progresses both sides.
    pub fn up(&mut self, event: LowerEvent, above: &mut Queue<Event>, _below: &mut Queue<Lower>) {
        match event {
            LowerEvent::Read(event) => self.read_event(event, above),
            LowerEvent::Write(event) => self.write_event(event, above),
        }
    }

    /// Emits the current read demand and a separate output reservation or send.
    pub fn poll(&mut self, above: &mut Queue<Event>, below: &mut Queue<Lower>) {
        self.emit_read(below);
        self.emit_output(above, below);
        self.maybe_ready(above);
        self.maybe_finish(above, below);
    }

    /// The owner-visible waits for deadlines.
    #[must_use]
    pub fn waiting(&self) -> Waiting {
        let read = match self.phase {
            Phase::WaitOpen | Phase::WaitAccept | Phase::WaitTerms { .. } => ReadWait::Opening,
            Phase::Idle
            | Phase::WaitOwner { .. }
            | Phase::Finished
            | Phase::Closed
            | Phase::Ready { .. }
            | Phase::Refusing { .. } => ReadWait::Nothing,
        };
        let write =
            if self.output.is_empty() && self.output_right.is_none() { WriteWait::Nothing } else { WriteWait::Frames };
        Waiting { read, write }
    }

    /// The selected version after the opening, if ready.
    #[must_use]
    pub fn version(&self) -> Option<u16> {
        match self.phase {
            Phase::Ready { version } => Some(version),
            Phase::Idle
            | Phase::WaitOpen
            | Phase::WaitOwner { .. }
            | Phase::WaitAccept
            | Phase::WaitTerms { .. }
            | Phase::Refusing { .. }
            | Phase::Finished
            | Phase::Closed => None,
        }
    }

    fn open(&mut self, credential: Box<[u8]>) {
        if self.role != Role::Initiator || self.phase != Phase::Idle {
            return;
        }
        let (lowest, highest) = self.schema.range().expect("validated versions");
        let control = Control::Open { magic: self.schema.magic, lowest, highest, features: 0, credential };
        match control_frame(&control, &self.limits) {
            Ok(frame) => {
                self.queue(frame, false);
                self.phase = Phase::WaitAccept;
                self.read = ReadState::Header;
            }
            Err(_) => self.phase = Phase::Closed,
        }
    }

    fn accept(&mut self, version: u16) {
        let allowed = match self.phase {
            Phase::WaitOwner { lowest, highest } => version >= lowest && version <= highest,
            Phase::Idle
            | Phase::WaitOpen
            | Phase::WaitAccept
            | Phase::WaitTerms { .. }
            | Phase::Ready { .. }
            | Phase::Refusing { .. }
            | Phase::Finished
            | Phase::Closed => false,
        };
        if !allowed {
            return;
        }
        let answer = Control::Accept { version, features: 0 };
        let frame = control_frame(&answer, &self.limits).expect("fixed Accept fits");
        self.queue(frame, false);
        let terms = local_terms(&self.schema, self.role, version, &self.limits);
        let frame = control_frame(&Control::Terms { entries: terms }, &self.limits).expect("checked Terms fit");
        self.queue(frame, true);
        self.phase = Phase::WaitTerms { version };
        self.read = ReadState::Header;
    }

    fn refuse(&mut self, reason: u16, text: Box<[u8]>) {
        if self.phase == Phase::Closed || self.phase.refusing() {
            return;
        }
        assert!(text.len() <= 256, "refusal text is at most 256 bytes");
        self.output = Queue::with_capacity(self.output.capacity());
        let control = Control::Refuse { reason, text };
        let frame = control_frame(&control, &self.limits).expect("bounded Refuse fits");
        self.queue(frame, false);
        self.phase = Phase::Refusing { reason };
        self.read = ReadState::Idle;
    }

    fn ping(&mut self) {
        if self.phase.may_ping() {
            let frame = control_frame(&Control::Ping, &self.limits).expect("fixed Ping fits");
            self.queue(frame, false);
        }
    }

    fn finish(&mut self) {
        if self.version().is_some() {
            self.phase = Phase::Finished;
        }
    }

    fn close(&mut self, above: &mut Queue<Event>, below: &mut Queue<Lower>) {
        if self.phase == Phase::Closed {
            return;
        }
        self.phase = Phase::Closed;
        self.read = ReadState::Idle;
        self.output = Queue::with_capacity(self.output.capacity());
        if self.read_outstanding {
            below.push(Lower::Read(stream::Down::Demand { read: stream::Read::Nothing, room: 0 }));
            self.read_outstanding = false;
        }
        if let Some(right) = self.output_right {
            below.push(Lower::Write(stream::OutputDown::Cancel { right }));
        } else {
            self.closed_sent = true;
            above.push(Event::Closed { why: Closed::Owner });
        }
    }

    fn queue(&mut self, frame: Frame, terms: bool) {
        self.output.try_push(Queued { frame, terms }).expect("control reserve has room");
    }

    fn emit_read(&mut self, below: &mut Queue<Lower>) {
        if self.read_outstanding {
            return;
        }
        let demand = match &self.read {
            ReadState::Header => Some(8),
            ReadState::Body { header, filled, .. } => {
                let left = header.body_len.checked_sub(*filled).expect("filled body does not pass length");
                Some(left.min(self.limits.chunk))
            }
            ReadState::Idle => None,
        };
        if let Some(bytes) = demand
            && bytes > 0
        {
            below.push(Lower::Read(stream::Down::Demand { read: stream::Read::Fill(bytes), room: 0 }));
            self.read_outstanding = true;
        }
    }

    fn emit_output(&mut self, above: &mut Queue<Event>, below: &mut Queue<Lower>) {
        if self.output_granted {
            let right = self.output_right.take().expect("grant has named right");
            let item = self.output.pop().expect("grant has a queued frame");
            below.push(Lower::Write(stream::OutputDown::Send { right, bytes: item.frame.into_bytes() }));
            self.output_granted = false;
            if item.terms {
                self.local_terms_sent = true;
            }
            above.push(Event::Drained);
            return;
        }
        if self.output.is_empty() || self.output_right.is_some() {
            return;
        }
        let right = Token::new(self.next_right);
        self.next_right = self.next_right.checked_add(1).expect("bounded output rights do not exhaust");
        let item = self.output.iter().next().expect("nonempty output");
        below.push(Lower::Write(stream::OutputDown::Room { right, bytes: item.frame.wire_len() }));
        self.output_right = Some(right);
    }

    fn write_event(&mut self, event: stream::OutputUp, above: &mut Queue<Event>) {
        match event {
            stream::OutputUp::Settled { right, outcome } => {
                if self.output_right != Some(right) {
                    return;
                }
                match outcome {
                    stream::OutputOutcome::Granted => {
                        self.output_granted = true;
                    }
                    stream::OutputOutcome::Cancelled | stream::OutputOutcome::Failed(_) => {
                        self.output_right = None;
                        self.output = Queue::with_capacity(self.output.capacity());
                        if self.phase == Phase::Closed && !self.closed_sent {
                            self.closed_sent = true;
                            above.push(Event::Closed { why: Closed::Owner });
                        } else {
                            above.push(Event::OutputFailed);
                        }
                    }
                }
            }
        }
    }

    fn read_event(&mut self, event: stream::Up, above: &mut Queue<Event>) {
        match event {
            stream::Up::Bytes(bytes) => {
                self.read_outstanding = false;
                self.read_bytes(bytes, above);
            }
            stream::Up::End => {
                if !self.phase.may_end_input() {
                    self.phase = Phase::Closed;
                    self.read = ReadState::Idle;
                    above.push(Event::Closed { why: Closed::Truncated });
                }
            }
            stream::Up::Failed(_) => {
                self.phase = Phase::Closed;
                self.read = ReadState::Idle;
                above.push(Event::Closed { why: Closed::Stream });
            }
            stream::Up::Room => unreachable!("the channel only sends read-only demands"),
        }
    }

    #[expect(clippy::manual_let_else, reason = "step code uses exhaustive matches instead of let-else")]
    fn read_bytes(&mut self, bytes: Box<[u8]>, above: &mut Queue<Event>) {
        let state = mem::replace(&mut self.read, ReadState::Idle);
        match state {
            ReadState::Header => {
                if bytes.len() != 8 {
                    self.framing_error();
                    return;
                }
                let header = match parse_header(&bytes) {
                    Ok(header) => header,
                    Err(_) => {
                        self.framing_error();
                        return;
                    }
                };
                if !self.header_allowed(header) {
                    self.framing_error();
                    return;
                }
                if header.body_len == 0 {
                    self.received(header.kind, &[], above);
                } else {
                    let body = bytes::zeroed(usize::try_from(header.body_len).expect("header length bounded"));
                    self.read = ReadState::Body { header, body, filled: 0 };
                }
            }
            ReadState::Body { header, mut body, filled } => {
                let count = u32::try_from(bytes.len()).expect("bounded read chunk");
                let end = filled.checked_add(count).expect("body length bounded");
                let start = usize::try_from(filled).expect("bounded offset");
                let end_index = usize::try_from(end).expect("bounded offset");
                let target = body.get_mut(start..end_index).expect("chunk fits body");
                for (destination, source) in target.iter_mut().zip(bytes.iter()) {
                    *destination = *source;
                }
                if end == header.body_len {
                    self.received(header.kind, &body, above);
                } else {
                    self.read = ReadState::Body { header, body, filled: end };
                }
            }
            ReadState::Idle => {}
        }
    }

    fn header_allowed(&self, header: Header) -> bool {
        let maximum = match header.kind {
            1 => 14_u32.checked_add(self.limits.credential),
            2 => Some(4),
            3 => Some(262),
            4 => match self.limits.kinds.checked_mul(6) {
                Some(count) => count.checked_add(4),
                None => None,
            },
            5 => Some(0),
            6 => Some(2),
            _ => None,
        };
        match maximum {
            Some(maximum) => header.body_len <= maximum,
            None => false,
        }
    }

    #[expect(clippy::manual_let_else, reason = "step code uses exhaustive matches instead of let-else")]
    fn received(&mut self, kind: u16, body: &[u8], above: &mut Queue<Event>) {
        let control = match decode_control(kind, body, &self.limits) {
            Ok(control) => control,
            Err(_) => {
                self.framing_error();
                return;
            }
        };
        match control {
            Control::Open { magic, lowest, highest, features, credential } => match self.phase {
                Phase::WaitOpen => match accept_open(&self.schema, magic, lowest, highest, features) {
                    Ok((lowest, highest)) => {
                        self.phase = Phase::WaitOwner { lowest, highest };
                        above.push(Event::Opening { credential, lowest, highest });
                    }
                    Err(reason) => self.auto_refuse(reason),
                },
                Phase::Idle
                | Phase::WaitOwner { .. }
                | Phase::WaitAccept
                | Phase::WaitTerms { .. }
                | Phase::Ready { .. }
                | Phase::Refusing { .. }
                | Phase::Finished
                | Phase::Closed => {
                    self.framing_error();
                }
            },
            Control::Accept { version, features } => {
                if self.phase == Phase::WaitAccept && features == 0 && offers_version(&self.schema, version) {
                    self.phase = Phase::WaitTerms { version };
                    self.read = ReadState::Header;
                } else {
                    self.framing_error();
                }
            }
            Control::Refuse { reason, text } => {
                above.push(Event::Refused { reason, text });
                self.phase = Phase::Closed;
                self.read = ReadState::Idle;
                self.output = Queue::with_capacity(self.output.capacity());
                if self.output_right.is_none() && !self.closed_sent {
                    above.push(Event::Closed { why: Closed::RefusedPeer(reason) });
                    self.closed_sent = true;
                }
            }
            Control::Terms { entries } => {
                let version = match self.phase {
                    Phase::WaitTerms { version } => version,
                    Phase::Idle
                    | Phase::WaitOpen
                    | Phase::WaitOwner { .. }
                    | Phase::WaitAccept
                    | Phase::Ready { .. }
                    | Phase::Refusing { .. }
                    | Phase::Finished
                    | Phase::Closed => {
                        self.framing_error();
                        return;
                    }
                };
                if !check_terms(&self.schema, self.role, version, &entries) {
                    self.framing_error();
                    return;
                }
                self.peer_terms = Some(entries);
                if self.role == Role::Initiator {
                    let terms = local_terms(&self.schema, self.role, version, &self.limits);
                    let frame =
                        control_frame(&Control::Terms { entries: terms }, &self.limits).expect("checked Terms fit");
                    self.queue(frame, true);
                }
            }
            Control::Ping => {
                if self.phase.may_ping() {
                    above.push(Event::Ping);
                    self.read = ReadState::Header;
                } else {
                    self.framing_error();
                }
            }
            Control::Unsupported { .. } => self.framing_error(),
        }
    }

    fn auto_refuse(&mut self, reason: u16) {
        self.refuse(reason, Box::from([]));
    }

    fn framing_error(&mut self) {
        self.auto_refuse(3);
    }

    fn maybe_ready(&mut self, above: &mut Queue<Event>) {
        if !self.local_terms_sent {
            return;
        }
        let version = match self.phase {
            Phase::WaitTerms { version } => version,
            Phase::Idle
            | Phase::WaitOpen
            | Phase::WaitOwner { .. }
            | Phase::WaitAccept
            | Phase::Ready { .. }
            | Phase::Refusing { .. }
            | Phase::Finished
            | Phase::Closed => return,
        };
        if let Some(terms) = self.peer_terms.take() {
            self.phase = Phase::Ready { version };
            self.read = ReadState::Idle;
            above.push(Event::Ready { version, terms });
        }
    }

    fn maybe_finish(&mut self, above: &mut Queue<Event>, below: &mut Queue<Lower>) {
        if self.finish_sent || !self.output.is_empty() || self.output_right.is_some() {
            return;
        }
        match self.phase {
            Phase::Refusing { reason } => {
                if self.read_outstanding {
                    below.push(Lower::Read(stream::Down::Demand { read: stream::Read::Nothing, room: 0 }));
                    self.read_outstanding = false;
                }
                below.push(Lower::Read(stream::Down::Finish));
                self.finish_sent = true;
                if !self.closed_sent {
                    above.push(Event::Closed { why: Closed::RefusedHere(reason) });
                    self.closed_sent = true;
                }
            }
            Phase::Finished => {
                if !self.read_outstanding {
                    below.push(Lower::Read(stream::Down::Finish));
                    self.finish_sent = true;
                }
            }
            Phase::Idle
            | Phase::WaitOpen
            | Phase::WaitOwner { .. }
            | Phase::WaitAccept
            | Phase::WaitTerms { .. }
            | Phase::Ready { .. }
            | Phase::Closed => {}
        }
    }
}
