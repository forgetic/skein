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

use super::boundary::{Closed, Event, Lower, LowerEvent, ReadWait, Request, Room, Unsent, Waiting, WriteWait};
use super::frame::{Control, Frame, Header, Term, control_frame, decode_control, parse_header};
use super::opening::{accept_open, check_terms, local_terms, offers_version};
use super::read::{ReadKind, classify};
use super::schema::{Limits, Role, Schema, SchemaError};
use super::write::admit;

#[derive(Debug, PartialEq, Eq)]
enum Phase {
    Idle,
    WaitOpen,
    WaitOwner { lowest: u16, highest: u16 },
    WaitAccept,
    WaitTerms { version: u16 },
    Ready { version: u16 },
    Refusing { reason: u16, framing: bool },
    Finished { version: u16 },
    Closed,
}

impl Phase {
    fn may_ping(&self) -> bool {
        match self {
            Phase::WaitOwner { .. }
            | Phase::WaitAccept
            | Phase::WaitTerms { .. }
            | Phase::Ready { .. }
            | Phase::Finished { .. } => true,
            Phase::Idle | Phase::WaitOpen | Phase::Refusing { .. } | Phase::Closed => false,
        }
    }

    fn may_end_input(&self) -> bool {
        match self {
            Phase::Ready { .. } | Phase::Finished { .. } | Phase::Closed => true,
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
            | Phase::Finished { .. }
            | Phase::Closed => false,
        }
    }
}

#[derive(Debug)]
enum ReadState {
    Header { first: Option<u8> },
    Body { header: Header, body: Box<[u8]>, filled: u32 },
    Skip { kind: u16, remaining: u32 },
    Ended,
    Idle,
}

#[derive(Debug)]
struct Queued {
    frame: Frame,
    tag: QueueTag,
}

#[derive(Debug)]
enum QueueTag {
    Control,
    Terms,
    Ping,
    Unsupported,
    Application,
}

/// Whether read and write share one stream or use a pipe in each direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamMode {
    /// Both directions use one socket-like stream.
    One,
    /// The machine reads one pipe and writes the other.
    Two,
}

/// One channel machine over a read stream and independent output stream.
#[derive(Debug)]
#[expect(clippy::struct_excessive_bools, reason = "each boolean tracks a separate lower obligation or one-time event")]
pub struct Machine {
    schema: Schema,
    role: Role,
    mode: StreamMode,
    limits: Limits,
    phase: Phase,
    read: ReadState,
    read_outstanding: bool,
    read_credit: bool,
    pending_unsupported: Option<u16>,
    output: Queue<Queued>,
    output_right: Option<Token>,
    output_granted: bool,
    cancel_requested: bool,
    cancel_sent: bool,
    next_right: u64,
    used_bytes: u32,
    used_frames: u32,
    ping_queued: bool,
    unsupported_queued: bool,
    local_terms_sent: bool,
    peer_terms: Option<List<Term>>,
    finish_sent: bool,
    closed_sent: bool,
    terminal_why: Option<Closed>,
    write_failed: bool,
}

impl Machine {
    /// Checks the schema before keeping its bounded tables.
    pub fn new(schema: Schema, role: Role, limits: Limits) -> Result<Machine, SchemaError> {
        Self::with_mode(schema, role, limits, StreamMode::One)
    }

    /// Checks a channel with an explicit one-stream or two-pipe topology.
    pub fn with_mode(schema: Schema, role: Role, limits: Limits, mode: StreamMode) -> Result<Machine, SchemaError> {
        schema.check(&limits)?;
        let output_capacity = limits.output_frames.checked_add(3).ok_or(SchemaError::InvalidLimit)?;
        let (phase, read) = match role {
            Role::Initiator => (Phase::Idle, ReadState::Idle),
            Role::Responder => (Phase::WaitOpen, ReadState::Header { first: None }),
        };
        Ok(Machine {
            schema,
            role,
            mode,
            limits,
            phase,
            read,
            read_outstanding: false,
            read_credit: false,
            pending_unsupported: None,
            output: Queue::with_capacity(output_capacity),
            output_right: None,
            output_granted: false,
            cancel_requested: false,
            cancel_sent: false,
            next_right: 1,
            used_bytes: 0,
            used_frames: 0,
            ping_queued: false,
            unsupported_queued: false,
            local_terms_sent: false,
            peer_terms: None,
            finish_sent: false,
            closed_sent: false,
            terminal_why: None,
            write_failed: false,
        })
    }

    /// Handles one owner request; `poll` afterwards emits the next demands.
    pub fn down(&mut self, request: Request, up: &mut Queue<Event>, below: &mut Queue<Lower>) {
        match request {
            Request::Open { credential } => self.open(credential),
            Request::Accept { version } => self.accept(version),
            Request::Refuse { reason, text } => self.refuse(reason, text),
            Request::Read => self.read(up),
            Request::Send { token, frame } => self.send(token, frame, up),
            Request::Ping => self.ping(),
            Request::Finish => self.finish(),
            Request::Close => self.close(up, below),
        }
        self.maybe_terminal(up);
    }

    /// Handles exactly one lower event; `poll` afterwards progresses both sides.
    pub fn up(&mut self, event: LowerEvent, above: &mut Queue<Event>, _below: &mut Queue<Lower>) {
        match event {
            LowerEvent::Read(event) => self.read_event(event, above),
            LowerEvent::Write(event) => self.write_event(event, above),
            LowerEvent::WriteFailed(_) => self.output_failure(above),
        }
        self.maybe_terminal(above);
    }

    /// Emits the current read demand and a separate output reservation or send.
    pub fn poll(&mut self, above: &mut Queue<Event>, below: &mut Queue<Lower>) {
        self.flush_unsupported();
        self.cancel_output(below);
        if self.read_outstanding {
            match self.read {
                ReadState::Ended => {
                    below.push(Lower::Read(stream::Down::Demand { read: stream::Read::Nothing, room: 0 }));
                    self.read_outstanding = false;
                }
                ReadState::Header { .. } | ReadState::Body { .. } | ReadState::Skip { .. } | ReadState::Idle => {}
            }
        }
        self.emit_read(below);
        self.emit_output(above, below);
        self.maybe_ready(above);
        self.maybe_finish(below);
        self.maybe_terminal(above);
    }

    /// The owner-visible waits for deadlines.
    #[must_use]
    pub fn waiting(&self) -> Waiting {
        let read = match self.phase {
            Phase::WaitOpen | Phase::WaitAccept | Phase::WaitTerms { .. } => ReadWait::Opening,
            Phase::Ready { .. } | Phase::Finished { .. } if self.read_credit => ReadWait::Frame,
            Phase::Idle
            | Phase::WaitOwner { .. }
            | Phase::Finished { .. }
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
            Phase::Ready { version } | Phase::Finished { version } => Some(version),
            Phase::Idle
            | Phase::WaitOpen
            | Phase::WaitOwner { .. }
            | Phase::WaitAccept
            | Phase::WaitTerms { .. }
            | Phase::Refusing { .. }
            | Phase::Closed => None,
        }
    }

    /// Application output room after all admitted frames are counted.
    #[must_use]
    pub fn room(&self) -> Room {
        Room {
            bytes: self.limits.output_bytes.checked_sub(self.used_bytes).expect("queued bytes fit the cap"),
            frames: self.limits.output_frames.checked_sub(self.used_frames).expect("queued frames fit the cap"),
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
                self.queue(frame, QueueTag::Control);
                self.phase = Phase::WaitAccept;
                self.read = ReadState::Header { first: None };
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
            | Phase::Finished { .. }
            | Phase::Closed => false,
        };
        if !allowed {
            return;
        }
        let answer = Control::Accept { version, features: 0 };
        let frame = control_frame(&answer, &self.limits).expect("fixed Accept fits");
        self.queue(frame, QueueTag::Control);
        let terms = local_terms(&self.schema, self.role, version, &self.limits);
        let frame = control_frame(&Control::Terms { entries: terms }, &self.limits).expect("checked Terms fit");
        self.queue(frame, QueueTag::Terms);
        self.phase = Phase::WaitTerms { version };
        self.read = ReadState::Header { first: None };
    }

    fn refuse(&mut self, reason: u16, text: Box<[u8]>) {
        if self.phase == Phase::Closed || self.phase.refusing() {
            return;
        }
        if self.write_failed {
            self.phase = Phase::Closed;
            self.read = ReadState::Idle;
            self.terminal_why = Some(Closed::Stream);
            return;
        }
        assert!(text.len() <= 256, "refusal text is at most 256 bytes");
        self.clear_output();
        let control = Control::Refuse { reason, text };
        let frame = control_frame(&control, &self.limits).expect("bounded Refuse fits");
        self.queue(frame, QueueTag::Control);
        self.phase = Phase::Refusing { reason, framing: false };
        self.read = ReadState::Idle;
        self.cancel_requested = self.output_right.is_some();
    }

    fn read(&mut self, above: &mut Queue<Event>) {
        let may_read = match self.phase {
            Phase::Ready { .. } => true,
            Phase::Finished { .. } => self.mode == StreamMode::Two,
            Phase::Idle
            | Phase::WaitOpen
            | Phase::WaitOwner { .. }
            | Phase::WaitAccept
            | Phase::WaitTerms { .. }
            | Phase::Refusing { .. }
            | Phase::Closed => false,
        };
        if !may_read || self.read_credit {
            return;
        }
        match self.read {
            ReadState::Ended => above.push(Event::Ended),
            ReadState::Idle => {
                self.read_credit = true;
                self.read = ReadState::Header { first: None };
            }
            ReadState::Header { .. } | ReadState::Body { .. } | ReadState::Skip { .. } => {
                unreachable!("one owner Read installs only one frame demand")
            }
        }
    }

    fn ping(&mut self) {
        let may_send = match self.phase {
            Phase::WaitOwner { .. } | Phase::WaitAccept | Phase::WaitTerms { .. } | Phase::Ready { .. } => true,
            Phase::Idle | Phase::WaitOpen | Phase::Refusing { .. } | Phase::Finished { .. } | Phase::Closed => false,
        };
        if may_send && !self.ping_queued && !self.write_failed {
            let frame = control_frame(&Control::Ping, &self.limits).expect("fixed Ping fits");
            self.queue(frame, QueueTag::Ping);
            self.ping_queued = true;
        }
    }

    fn send(&mut self, token: Token, frame: Frame, above: &mut Queue<Event>) {
        let version = match self.phase {
            Phase::Ready { version } if !self.write_failed => version,
            Phase::Idle
            | Phase::WaitOpen
            | Phase::WaitOwner { .. }
            | Phase::WaitAccept
            | Phase::WaitTerms { .. }
            | Phase::Refusing { .. }
            | Phase::Ready { .. }
            | Phase::Finished { .. }
            | Phase::Closed => {
                above.push(Event::Unsent { token, why: Unsent::Closed });
                return;
            }
        };
        let terms = self.peer_terms.as_ref().expect("Ready holds peer terms");
        match admit(&self.schema, self.role, version, terms, self.room(), &frame) {
            Ok(()) => {
                self.used_bytes = self.used_bytes.checked_add(frame.wire_len()).expect("admission checked byte room");
                self.used_frames = self.used_frames.checked_add(1).expect("admission checked frame room");
                self.queue(frame, QueueTag::Application);
                above.push(Event::Sent { token });
            }
            Err(why) => above.push(Event::Unsent { token, why }),
        }
    }

    fn finish(&mut self) {
        match self.phase {
            Phase::Ready { version } => self.phase = Phase::Finished { version },
            Phase::Idle
            | Phase::WaitOpen
            | Phase::WaitOwner { .. }
            | Phase::WaitAccept
            | Phase::WaitTerms { .. }
            | Phase::Refusing { .. }
            | Phase::Finished { .. }
            | Phase::Closed => {}
        }
    }

    fn close(&mut self, above: &mut Queue<Event>, below: &mut Queue<Lower>) {
        if self.closed_sent || self.phase == Phase::Closed {
            return;
        }
        self.phase = Phase::Closed;
        self.read = ReadState::Idle;
        self.clear_output();
        self.terminal_why = Some(Closed::Owner);
        self.cancel_requested = self.output_right.is_some();
        if self.read_outstanding {
            below.push(Lower::Read(stream::Down::Demand { read: stream::Read::Nothing, room: 0 }));
            self.read_outstanding = false;
        }
        self.cancel_output(below);
        self.maybe_terminal(above);
    }

    fn clear_output(&mut self) {
        for _ in 0..self.output.capacity() {
            match self.output.pop() {
                Some(item) => drop(item),
                None => break,
            }
        }
        self.used_bytes = 0;
        self.used_frames = 0;
        self.ping_queued = false;
        self.unsupported_queued = false;
        self.pending_unsupported = None;
    }

    fn cancel_output(&mut self, below: &mut Queue<Lower>) {
        if !self.cancel_requested {
            return;
        }
        match self.output_right {
            Some(right) if self.output_granted => {
                below.push(Lower::Write(stream::OutputDown::Release { right }));
                self.output_right = None;
                self.output_granted = false;
                self.cancel_requested = false;
                self.cancel_sent = false;
            }
            Some(right) if !self.cancel_sent => {
                below.push(Lower::Write(stream::OutputDown::Cancel { right }));
                self.cancel_sent = true;
            }
            Some(_) => {}
            None => {
                self.cancel_requested = false;
                self.cancel_sent = false;
            }
        }
    }

    fn maybe_terminal(&mut self, above: &mut Queue<Event>) {
        if self.closed_sent || !self.output.is_empty() || self.output_right.is_some() {
            return;
        }
        if let Some(why) = self.terminal_why {
            self.phase = Phase::Closed;
            self.closed_sent = true;
            above.push(Event::Closed { why });
        }
    }

    fn queue(&mut self, frame: Frame, tag: QueueTag) {
        self.output.try_push(Queued { frame, tag }).expect("control reserve has room");
    }

    fn emit_read(&mut self, below: &mut Queue<Lower>) {
        if self.read_outstanding {
            return;
        }
        let demand = match &self.read {
            ReadState::Header { first } => match first {
                Some(_) => Some(7),
                None => Some(1),
            },
            ReadState::Body { header, filled, .. } => {
                let left = header.body_len.checked_sub(*filled).expect("filled body does not pass length");
                Some(left.min(self.limits.chunk))
            }
            ReadState::Skip { remaining, .. } => Some((*remaining).min(self.limits.chunk)),
            ReadState::Ended | ReadState::Idle => None,
        };
        if let Some(bytes) = demand
            && bytes > 0
        {
            below.push(Lower::Read(stream::Down::Demand { read: stream::Read::Fill(bytes), room: 0 }));
            self.read_outstanding = true;
        }
    }

    fn emit_output(&mut self, above: &mut Queue<Event>, below: &mut Queue<Lower>) {
        if self.write_failed || self.cancel_requested {
            return;
        }
        if self.output_granted {
            let right = self.output_right.take().expect("grant has named right");
            let item = self.output.pop().expect("grant has a queued frame");
            match item.tag {
                QueueTag::Control => {}
                QueueTag::Terms => self.local_terms_sent = true,
                QueueTag::Ping => self.ping_queued = false,
                QueueTag::Unsupported => self.unsupported_queued = false,
                QueueTag::Application => {
                    self.used_bytes =
                        self.used_bytes.checked_sub(item.frame.wire_len()).expect("queued bytes were counted");
                    self.used_frames = self.used_frames.checked_sub(1).expect("queued frame was counted");
                }
            }
            below.push(Lower::Write(stream::OutputDown::Send { right, bytes: item.frame.into_bytes() }));
            self.output_granted = false;
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
                    stream::OutputOutcome::Cancelled => {
                        self.output_right = None;
                        if self.cancel_requested {
                            self.cancel_requested = false;
                            self.cancel_sent = false;
                        } else {
                            self.output_failure(above);
                        }
                    }
                    stream::OutputOutcome::Failed(_) => {
                        self.output_right = None;
                        self.cancel_requested = false;
                        self.cancel_sent = false;
                        self.output_failure(above);
                    }
                }
            }
        }
    }

    fn output_failure(&mut self, above: &mut Queue<Event>) {
        if self.write_failed || self.closed_sent {
            return;
        }
        self.write_failed = true;
        self.clear_output();
        if self.terminal_why.is_some() {
            return;
        }
        if self.phase.refusing() {
            self.phase = Phase::Closed;
            self.read = ReadState::Idle;
            self.terminal_why = Some(Closed::Stream);
        }
        match self.mode {
            StreamMode::One => {
                self.phase = Phase::Closed;
                self.read = ReadState::Idle;
                self.terminal_why = Some(Closed::Stream);
            }
            StreamMode::Two => above.push(Event::OutputFailed),
        }
    }

    fn read_event(&mut self, event: stream::Up, above: &mut Queue<Event>) {
        match event {
            stream::Up::Bytes(bytes) => {
                self.read_outstanding = false;
                if self.phase != Phase::Closed {
                    self.read_bytes(bytes, above);
                }
            }
            stream::Up::End => {
                if self.phase == Phase::Closed {
                    return;
                }
                let between_frames = match &self.read {
                    ReadState::Header { first: None } | ReadState::Idle | ReadState::Ended => true,
                    ReadState::Header { first: Some(_) } | ReadState::Body { .. } | ReadState::Skip { .. } => false,
                };
                if self.phase.may_end_input() && between_frames {
                    self.read = ReadState::Ended;
                    if self.read_credit {
                        self.read_credit = false;
                        above.push(Event::Ended);
                    }
                    if self.write_failed {
                        self.phase = Phase::Closed;
                        self.terminal_why = Some(Closed::Stream);
                    }
                } else {
                    self.phase = Phase::Closed;
                    self.read = ReadState::Idle;
                    self.terminal_why = Some(Closed::Truncated);
                    if self.mode == StreamMode::One {
                        self.clear_output();
                        self.cancel_requested = self.output_right.is_some();
                    }
                }
            }
            stream::Up::Failed(_) => {
                if self.phase == Phase::Closed {
                    return;
                }
                self.phase = Phase::Closed;
                self.read = ReadState::Idle;
                self.read_outstanding = false;
                self.terminal_why = Some(Closed::Stream);
                if self.mode == StreamMode::One {
                    self.clear_output();
                    self.cancel_requested = self.output_right.is_some();
                }
            }
            stream::Up::Room => unreachable!("the channel only sends read-only demands"),
        }
    }

    #[expect(clippy::manual_let_else, reason = "step code uses exhaustive matches instead of let-else")]
    #[expect(clippy::boxed_local, reason = "application bodies move from this handler into Event::Body")]
    fn read_bytes(&mut self, bytes: Box<[u8]>, above: &mut Queue<Event>) {
        let state = mem::replace(&mut self.read, ReadState::Idle);
        match state {
            ReadState::Header { first: None } => {
                if bytes.len() == 1 {
                    let first = *bytes.first().expect("one delivered byte");
                    self.read = ReadState::Header { first: Some(first) };
                } else {
                    self.framing_error();
                }
            }
            ReadState::Header { first: Some(first) } => {
                if bytes.len() != 7 {
                    self.framing_error();
                    return;
                }
                let mut head = [0_u8; 8];
                *head.get_mut(0).expect("first header cell") = first;
                let rest = head.get_mut(1..).expect("seven header cells");
                for (destination, source) in rest.iter_mut().zip(bytes.iter()) {
                    *destination = *source;
                }
                let header = match parse_header(&head) {
                    Ok(header) => header,
                    Err(_) => {
                        self.framing_error();
                        return;
                    }
                };
                self.begin_body(header, above);
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
                    self.received(header.kind, body, above);
                } else {
                    self.read = ReadState::Body { header, body, filled: end };
                }
            }
            ReadState::Skip { kind, remaining } => {
                let delivered = u32::try_from(bytes.len()).expect("bounded skip chunk");
                let left = remaining.checked_sub(delivered).expect("skip demand fits remaining body");
                if left == 0 {
                    self.pending_unsupported = Some(kind);
                } else {
                    self.read = ReadState::Skip { kind, remaining: left };
                }
            }
            ReadState::Ended | ReadState::Idle => {}
        }
    }

    fn begin_body(&mut self, header: Header, above: &mut Queue<Event>) {
        let kind = match self.phase {
            Phase::Ready { version } | Phase::Finished { version } => {
                classify(&self.schema, self.role, &self.limits, version, header)
            }
            Phase::Idle
            | Phase::WaitOpen
            | Phase::WaitOwner { .. }
            | Phase::WaitAccept
            | Phase::WaitTerms { .. }
            | Phase::Refusing { .. }
            | Phase::Closed => {
                if self.header_allowed(header) {
                    Some(ReadKind::Control)
                } else {
                    None
                }
            }
        };
        match kind {
            Some(ReadKind::Control | ReadKind::Body) => {
                if header.body_len == 0 {
                    self.received(header.kind, Box::from([]), above);
                } else {
                    let body = bytes::zeroed(usize::try_from(header.body_len).expect("header length bounded"));
                    self.read = ReadState::Body { header, body, filled: 0 };
                }
            }
            Some(ReadKind::Skip) => {
                if header.body_len == 0 {
                    self.pending_unsupported = Some(header.kind);
                } else {
                    self.read = ReadState::Skip { kind: header.kind, remaining: header.body_len };
                }
            }
            None => self.framing_error(),
        }
    }

    #[expect(clippy::manual_let_else, reason = "step code uses exhaustive matches instead of let-else")]
    fn flush_unsupported(&mut self) {
        let kind = match self.pending_unsupported {
            Some(kind) => kind,
            None => return,
        };
        if self.output.room() == 0 || self.unsupported_queued {
            return;
        }
        let frame = control_frame(&Control::Unsupported { kind }, &self.limits).expect("fixed Unsupported fits");
        self.queue(frame, QueueTag::Unsupported);
        self.unsupported_queued = true;
        self.pending_unsupported = None;
        self.read = ReadState::Header { first: None };
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
    #[expect(
        clippy::too_many_lines,
        reason = "the control transition table is kept together until the machine migration completes"
    )]
    fn received(&mut self, kind: u16, body: Box<[u8]>, above: &mut Queue<Event>) {
        if kind >= 0x0100 {
            self.read_credit = false;
            above.push(Event::Body { kind, body });
            return;
        }
        let control = match decode_control(kind, &body, &self.limits) {
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
                | Phase::Finished { .. }
                | Phase::Closed => {
                    self.framing_error();
                }
            },
            Control::Accept { version, features } => {
                if self.phase == Phase::WaitAccept && features == 0 && offers_version(&self.schema, version) {
                    self.phase = Phase::WaitTerms { version };
                    self.read = ReadState::Header { first: None };
                } else {
                    self.framing_error();
                }
            }
            Control::Refuse { reason, text } => {
                self.read_credit = false;
                above.push(Event::Refused { reason, text });
                self.phase = Phase::Closed;
                self.read = ReadState::Idle;
                self.clear_output();
                self.cancel_requested = self.output_right.is_some();
                self.terminal_why = Some(Closed::RefusedPeer(reason));
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
                    | Phase::Finished { .. }
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
                    self.queue(frame, QueueTag::Terms);
                }
            }
            Control::Ping => {
                if self.phase.may_ping() {
                    above.push(Event::Ping);
                    match self.phase {
                        Phase::Ready { .. } | Phase::Finished { .. } => self.read_credit = false,
                        Phase::WaitOwner { .. } | Phase::WaitAccept | Phase::WaitTerms { .. } => {
                            self.read = ReadState::Header { first: None };
                        }
                        Phase::Idle | Phase::WaitOpen | Phase::Refusing { .. } | Phase::Closed => {
                            unreachable!("may_ping checked the phase")
                        }
                    }
                } else {
                    self.framing_error();
                }
            }
            Control::Unsupported { kind } => match self.phase {
                Phase::Ready { .. } | Phase::Finished { .. } => {
                    self.read_credit = false;
                    above.push(Event::Unsupported { kind });
                }
                Phase::Idle
                | Phase::WaitOpen
                | Phase::WaitOwner { .. }
                | Phase::WaitAccept
                | Phase::WaitTerms { .. }
                | Phase::Refusing { .. }
                | Phase::Closed => self.framing_error(),
            },
        }
    }

    fn auto_refuse(&mut self, reason: u16) {
        self.refuse(reason, Box::from([]));
        if reason == 3 {
            self.phase = Phase::Refusing { reason, framing: true };
        }
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
            | Phase::Finished { .. }
            | Phase::Closed => return,
        };
        if let Some(terms) = self.peer_terms.as_ref() {
            self.phase = Phase::Ready { version };
            self.read = ReadState::Idle;
            above.push(Event::Ready { version, terms: terms.clone() });
        }
    }

    fn maybe_finish(&mut self, below: &mut Queue<Lower>) {
        if self.finish_sent || !self.output.is_empty() || self.output_right.is_some() {
            return;
        }
        match self.phase {
            Phase::Refusing { reason, framing } => {
                if self.read_outstanding {
                    below.push(Lower::Read(stream::Down::Demand { read: stream::Read::Nothing, room: 0 }));
                    self.read_outstanding = false;
                }
                match self.mode {
                    StreamMode::One => below.push(Lower::Read(stream::Down::Finish)),
                    StreamMode::Two => below.push(Lower::FinishWrite),
                }
                self.finish_sent = true;
                self.terminal_why = Some(if framing { Closed::Framing } else { Closed::RefusedHere(reason) });
                self.phase = Phase::Closed;
            }
            Phase::Finished { .. } => {
                if self.mode == StreamMode::Two || (!self.read_outstanding && !self.read_credit) {
                    match self.mode {
                        StreamMode::One => below.push(Lower::Read(stream::Down::Finish)),
                        StreamMode::Two => below.push(Lower::FinishWrite),
                    }
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
