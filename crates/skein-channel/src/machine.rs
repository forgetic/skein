//! Explicit consume/resolve/single-poll machine (channel.md §§3–6).
//! Native output obligations survive logical closure until their actual winner.
use crate::{Disposition, Encoded, Event, Fault, First, LowerEvent, LowerRequest, Phase, Request, Schema, Step};
use skein_lib::{Queue, Token, Writer, stream};

mod input;
mod output;
mod requests;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Reading {
    Active,
    Paused,
    Ended,
}

#[derive(Debug)]
enum Input {
    Header,
    Body { kind: u16, length: u32, receipt: Option<Token>, writer: Writer },
    AwaitDecode { kind: u16, receipt: Token },
    Skip { kind: u16, remaining: u32 },
    Skipped { kind: u16 },
    Stopped,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Right {
    Idle,
    Waiting { right: Token },
    Granted { right: Token },
    Retiring { right: Token },
}

#[derive(Debug)]
struct Ledger {
    pending: u32,
    spent: u32,
    initialized: bool,
}

#[derive(Clone, Copy, Debug)]
struct Offer {
    lowest: u16,
    highest: u16,
}

#[derive(Clone, Copy, Debug)]
struct Gate {
    kind: Option<u16>,
    done: bool,
}

/// One bounded owner of schema, raw input, frame queue and native right (§§3–7).
/// Inline cells are priced by the enclosing slab/host, not duplicated in heap.
#[derive(Debug)]
pub struct Machine {
    schema: Schema,
    phase: Phase,
    selected: u16,
    offer: Option<Offer>,
    input: Input,
    reading: Reading,
    demand: Option<u32>,
    output: Queue<Encoded>,
    ledger: Ledger,
    right: Right,
    next_receipt: Option<u64>,
    next_output: Option<u64>,
    received: u64,
    first_receive: Gate,
    first_send: Gate,
    finish_sent: bool,
    logical_fault: Option<Fault>,
    resource_closed: bool,
}

impl Machine {
    /// Takes the checked frozen Schema; fixed identity origin is never reused.
    /// Construction allocates only the fixed Q-frame record queue (§7).
    #[must_use]
    pub fn new(schema: Schema) -> Machine {
        let output = Queue::with_capacity(schema.limits().queued_frames);
        Machine {
            schema,
            phase: Phase::Opening,
            selected: 0,
            offer: None,
            input: Input::Header,
            reading: Reading::Active,
            demand: None,
            output,
            ledger: Ledger { pending: 0, spent: 0, initialized: false },
            right: Right::Idle,
            next_receipt: Some(1),
            next_output: Some(1),
            received: 0,
            first_receive: Gate { kind: None, done: false },
            first_send: Gate { kind: None, done: false },
            finish_sent: false,
            logical_fault: None,
            resource_closed: false,
        }
    }

    /// Mechanical phase only; authenticated/service phase remains above (§2).
    #[must_use]
    pub const fn phase(&self) -> Phase {
        self.phase
    }

    /// Selected wire version, zero until negotiation (§2).
    #[must_use]
    pub const fn version(&self) -> u16 {
        self.selected
    }

    /// Immutable schema observation, no way to mutate first/version rules (§2).
    #[must_use]
    pub const fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Fully validated frame count, including Ping and admitted skip status (§3).
    #[must_use]
    pub const fn received_frames(&self) -> u64 {
        self.received
    }

    /// Owned queued frame bytes P, distinct from native grant/debt (§4).
    #[must_use]
    pub const fn pending_bytes(&self) -> u32 {
        self.ledger.pending
    }

    /// Conservative admission P+S; only a matching Granted clears S (§4).
    #[must_use]
    pub fn queued_bytes(&self) -> u32 {
        self.ledger.pending.checked_add(self.ledger.spent).expect("ledger stays within C")
    }

    /// Admission room C-P-S after initialization; before it, zero (§4).
    #[must_use]
    pub fn room(&self) -> u32 {
        if !self.ledger.initialized || !self.live() {
            return 0;
        }
        self.schema.limits().queued_bytes.checked_sub(self.queued_bytes()).expect("ledger stays within C")
    }

    /// Read-only producer preflight before exact encoder allocation (§3).
    /// Common opening/control reserve can still fit before first grant.
    #[must_use]
    pub fn admits(&self, frame_bytes: u32) -> bool {
        self.live() && self.fits(frame_bytes, 1)
    }

    /// Actual settled lifecycle AND no pending named output terminal (§6).
    /// Anonymous resource Closed alone cannot retire an output identity.
    #[must_use]
    pub fn is_retired(&self) -> bool {
        self.resource_closed
            && match self.right {
                Right::Idle => true,
                Right::Waiting { .. } | Right::Granted { .. } | Right::Retiring { .. } => false,
            }
    }

    /// Runnable local work; retained grant or pending Room alone never spins (§4).
    #[must_use]
    pub fn is_ready(&self) -> bool {
        if !self.live() {
            return false;
        }
        let skipped = match self.input {
            Input::Skipped { .. } => self.fits(10, 1),
            Input::Header | Input::Body { .. } | Input::AwaitDecode { .. } | Input::Skip { .. } | Input::Stopped => {
                false
            }
        };
        let output = match self.right {
            Right::Granted { .. } => !self.output.is_empty(),
            Right::Idle | Right::Waiting { .. } | Right::Retiring { .. } => false,
        };
        skipped
            || output
            || (self.demand.is_none() && self.read_count().is_some())
            || self.needs_room()
            || (self.phase == Phase::Finished && self.output.is_empty())
    }

    /// Wrapper-approved actual End after exhaustive concrete phase policy (§5).
    /// Drops partial input, withdraws its actual read, emits `ReadEnded` once and
    /// returns Halt: no same-call poll, output right/physical lifecycle untouched.
    pub fn read_end(&mut self, events: &mut Queue<Event>, lower: &mut Queue<LowerRequest>) -> Step {
        if !self.live() || self.reading == Reading::Ended {
            return Step::Halt;
        }
        self.reading = Reading::Ended;
        self.input = Input::Stopped;
        self.withdraw(lower);
        events.push(Event::ReadEnded);
        Step::Halt
    }

    fn live(&self) -> bool {
        match self.phase {
            Phase::Opening | Phase::Authorizing | Phase::Terms | Phase::Ready | Phase::Finished => true,
            Phase::Closing | Phase::Closed => false,
        }
    }

    fn fits(&self, bytes: u32, slots: u32) -> bool {
        if self.output.room() < slots {
            return false;
        }
        match self.queued_bytes().checked_add(bytes) {
            Some(total) => total <= self.schema.limits().queued_bytes,
            None => false,
        }
    }

    fn queue(&mut self, encoded: Encoded) -> Option<()> {
        let length = u32::try_from(encoded.bytes.len()).ok()?;
        if !self.fits(length, 1) {
            return None;
        }
        self.output.try_push(encoded).ok()?;
        self.ledger.pending = self.ledger.pending.checked_add(length)?;
        Some(())
    }

    fn withdraw(&mut self, lower: &mut Queue<LowerRequest>) {
        if self.demand.take().is_some() {
            lower.push(LowerRequest::Stream(stream::Down::Demand { read: stream::Read::Nothing, room: 0 }));
        }
    }

    fn install(&mut self, version: u16) -> Option<()> {
        let First { receive, send, initial_read, ping_before_receive: _ } = self.schema.version(version)?.first;
        self.selected = version;
        self.first_receive = Gate { kind: receive, done: false };
        self.first_send = Gate { kind: send, done: false };
        self.reading = if receive.is_some() || initial_read { Reading::Active } else { Reading::Paused };
        Some(())
    }

    fn read_count(&self) -> Option<u32> {
        if self.reading != Reading::Active {
            return None;
        }
        match &self.input {
            Input::Header => Some(8),
            Input::Body { length, writer, .. } => {
                Some(length.checked_sub(u32::try_from(writer.written()).ok()?)?.min(self.schema.limits().chunk_bytes))
            }
            Input::Skip { remaining, .. } => Some((*remaining).min(self.schema.limits().chunk_bytes)),
            Input::AwaitDecode { .. } | Input::Skipped { .. } | Input::Stopped => None,
        }
    }

    fn needs_room(&self) -> bool {
        if self.phase == Phase::Finished {
            return !self.output.is_empty() && self.right == Right::Idle;
        }
        self.right == Right::Idle && (!self.ledger.initialized || self.ledger.spent != 0 || !self.output.is_empty())
    }
}

/// Consume one lower fact without an ordinary poll; resolve `NeedDecode` first (§5).
pub fn up(
    machine: &mut Machine,
    event: LowerEvent,
    events: &mut Queue<Event>,
    lower: &mut Queue<LowerRequest>,
) -> Step {
    match event {
        LowerEvent::Output(event) => output::settled(machine, event, events, lower),
        LowerEvent::Closing => {
            if machine.logical_fault.is_none() {
                output::stop(machine, Fault::Closed, events, lower);
            }
            Step::Halt
        }
        LowerEvent::Closed => {
            machine.resource_closed = true;
            if machine.logical_fault.is_none() {
                output::stop(machine, Fault::Closed, events, lower);
            }
            machine.phase = Phase::Closed;
            Step::Halt
        }
        LowerEvent::Stream(event) => input::stream(machine, event, events, lower),
    }
}

/// Ordered parent admission without an ordinary poll (channel.md §§3,5).
pub fn down(
    machine: &mut Machine,
    request: Request,
    events: &mut Queue<Event>,
    lower: &mut Queue<LowerRequest>,
) -> Step {
    requests::request(machine, request, events, lower)
}

/// State-only exact body resolution; invalid/reject performs bounded stop (§3).
pub fn resolve(
    machine: &mut Machine,
    receipt: Token,
    disposition: Disposition,
    events: &mut Queue<Event>,
    lower: &mut Queue<LowerRequest>,
) -> Step {
    if !machine.live() {
        return Step::Halt;
    }
    let kind = match machine.input {
        Input::AwaitDecode { kind, receipt: current } => {
            if receipt != current {
                return Step::Halt;
            }
            kind
        }
        Input::Header | Input::Body { .. } | Input::Skip { .. } | Input::Skipped { .. } | Input::Stopped => {
            return Step::Halt;
        }
    };
    match disposition {
        Disposition::InvalidBody => {
            output::stop(machine, Fault::Framing, events, lower);
            Step::Halt
        }
        Disposition::DecodedReject => {
            machine.received = machine.received.saturating_add(1);
            output::stop(machine, Fault::Framing, events, lower);
            Step::Halt
        }
        Disposition::DecodedPause | Disposition::DecodedContinue => {
            machine.received = machine.received.saturating_add(1);
            if machine.first_receive.kind == Some(kind) {
                machine.first_receive.done = true;
            }
            machine.input = Input::Header;
            machine.reading = match disposition {
                Disposition::DecodedPause => Reading::Paused,
                Disposition::DecodedContinue => Reading::Active,
                Disposition::InvalidBody | Disposition::DecodedReject => unreachable!("fatal dispositions returned"),
            };
            Step::NeedPoll
        }
    }
}

/// Exactly one normal poll, only where consume/resolve returned `NeedPoll` (§5).
/// Sends at most one frame, one read-only Demand and one whole-cap Room.
pub fn poll(machine: &mut Machine, events: &mut Queue<Event>, lower: &mut Queue<LowerRequest>) {
    output::poll(machine, events, lower);
}

fn identity(next: &mut Option<u64>) -> Option<Token> {
    let raw = (*next)?;
    *next = raw.checked_add(1);
    Some(Token::new(raw))
}

#[cfg(test)]
mod tests;
