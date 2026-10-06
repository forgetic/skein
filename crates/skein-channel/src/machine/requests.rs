//! Ordered sender entrance and internal control admission (channel.md §§3,5).
use super::{Input, Machine, Offer, Reading, output};
use crate::{Encoded, Event, Fault, LowerRequest, Opening, Phase, Refusal, Request, Role, Step, codec};
use skein_lib::{Queue, Token};

pub(super) fn request(
    machine: &mut Machine,
    request: Request,
    events: &mut Queue<Event>,
    lower: &mut Queue<LowerRequest>,
) -> Step {
    match request {
        Request::Close => {
            output::stop(machine, Fault::Closed, events, lower);
            Step::Halt
        }
        Request::Resolve { receipt, disposition } => {
            super::super::resolve(machine, receipt, disposition, events, lower)
        }
        Request::Send { owner, encoded } => send(machine, owner, encoded, events, lower),
        Request::Open { owner, opening } => open(machine, owner, opening, events, lower),
        Request::Ping { owner } => ping(machine, owner, events, lower),
        Request::Read => {
            if !machine.live() || machine.phase == Phase::Finished || machine.reading == Reading::Ended {
                return Step::Halt;
            }
            match machine.input {
                Input::AwaitDecode { .. } => {}
                Input::Header | Input::Body { .. } | Input::Skip { .. } | Input::Skipped { .. } | Input::Stopped => {
                    machine.reading = Reading::Active;
                }
            }
            Step::NeedPoll
        }
        Request::Accept { version } => {
            if !machine.live() || machine.phase == Phase::Finished {
                return Step::Halt;
            }
            if machine.phase != Phase::Authorizing || accept(machine, version, true).is_none() {
                output::stop(machine, Fault::Framing, events, lower);
                Step::Halt
            } else {
                Step::NeedPoll
            }
        }
        Request::Refuse { refusal } => refuse(machine, refusal, events, lower),
        Request::Finish => {
            if !machine.live() || machine.phase == Phase::Finished {
                return Step::Halt;
            }
            machine.phase = Phase::Finished;
            machine.reading = Reading::Ended;
            machine.input = Input::Stopped;
            // Withdrawal belongs to the one final poll; no hidden second pump.
            Step::NeedPoll
        }
    }
}

fn send(
    machine: &mut Machine,
    owner: Token,
    encoded: Option<Encoded>,
    events: &mut Queue<Event>,
    lower: &mut Queue<LowerRequest>,
) -> Step {
    if !machine.live() || machine.phase == Phase::Finished {
        events.push(Event::Unsent { owner });
        return Step::Halt;
    }
    let Some(encoded) = encoded else {
        events.push(Event::Unsent { owner });
        output::stop(machine, Fault::OutputFull, events, lower);
        return Step::Halt;
    };
    if encoded.version != machine.selected {
        events.push(Event::Unsent { owner });
        return Step::Halt;
    }
    if !sender_allowed(machine, &encoded) {
        output::stop(machine, Fault::Framing, events, lower);
        return Step::Halt;
    }
    let kind = encoded.kind;
    if machine.queue(encoded).is_none() {
        events.push(Event::Unsent { owner });
        output::stop(machine, Fault::OutputFull, events, lower);
        return Step::Halt;
    }
    if machine.first_send.kind == Some(kind) {
        machine.first_send.done = true;
    }
    events.push(Event::Sent { owner });
    Step::NeedPoll
}

fn sender_allowed(machine: &Machine, encoded: &Encoded) -> bool {
    if machine.phase != Phase::Ready || !machine.schema.flows(encoded.kind, false) {
        return false;
    }
    match machine.schema.rule(encoded.version, encoded.kind) {
        Some(rule) => {
            if encoded.body_bytes > rule.body_bytes
                || encoded.body_bytes.checked_add(8) != u32::try_from(encoded.bytes.len()).ok()
            {
                return false;
            }
        }
        None => return false,
    }
    if let Some(first) = machine.first_send.kind {
        if !machine.first_send.done && encoded.kind != first {
            return false;
        }
        if machine.first_send.done && encoded.kind == first {
            return false;
        }
    }
    true
}

fn open(
    machine: &mut Machine,
    owner: Token,
    opening: Opening,
    events: &mut Queue<Event>,
    lower: &mut Queue<LowerRequest>,
) -> Step {
    if !machine.live() || machine.phase == Phase::Finished {
        events.push(Event::Unsent { owner });
        return Step::Halt;
    }
    let (lowest, highest) = machine.schema.supported();
    if machine.schema.role != Role::Initiator
        || machine.phase != Phase::Opening
        || machine.offer.is_some()
        || opening.channel != machine.schema.profile().channel
        || opening.lowest < lowest
        || opening.highest > highest
        || opening.lowest > opening.highest
    {
        output::stop(machine, Fault::Framing, events, lower);
        return Step::Halt;
    }
    let offer = Offer { lowest: opening.lowest, highest: opening.highest };
    let name = u32::try_from(opening.name.len()).ok();
    let secret = u32::try_from(opening.secret.len()).ok();
    let length = match name {
        Some(name) => match secret {
            Some(secret) => match 25_u32.checked_add(name) {
                Some(length) => length.checked_add(secret),
                None => None,
            },
            None => None,
        },
        None => None,
    };
    let encoded = if match length {
        Some(length) => machine.fits(length, 1),
        None => false,
    } {
        codec::open(&machine.schema, &opening)
    } else {
        None
    };
    if let Some(encoded) = encoded {
        machine.queue(encoded).expect("opening preflight reserves bytes/slot");
        machine.offer = Some(offer);
        events.push(Event::Sent { owner });
        Step::NeedPoll
    } else {
        events.push(Event::Unsent { owner });
        output::stop(machine, Fault::OutputFull, events, lower);
        Step::Halt
    }
}

fn ping(machine: &mut Machine, owner: Token, events: &mut Queue<Event>, lower: &mut Queue<LowerRequest>) -> Step {
    if !machine.live() || machine.phase == Phase::Finished {
        events.push(Event::Unsent { owner });
        return Step::Halt;
    }
    if machine.phase != Phase::Ready || !machine.schema.profile().ping_allowed {
        output::stop(machine, Fault::Framing, events, lower);
        return Step::Halt;
    }
    if !machine.fits(8, 1) {
        events.push(Event::Unsent { owner });
        output::stop(machine, Fault::OutputFull, events, lower);
        return Step::Halt;
    }
    let writer = codec::FrameWriter::common(4, 0, 0, machine.schema.limits().queued_bytes).expect("frozen ping fits C");
    let frame = writer.finish().expect("zero-body ping consumes exact allocation");
    machine.queue(frame).expect("ping preflight reserves bytes/slot");
    events.push(Event::Sent { owner });
    Step::NeedPoll
}

fn refuse(machine: &mut Machine, refusal: Refusal, events: &mut Queue<Event>, lower: &mut Queue<LowerRequest>) -> Step {
    if !machine.live() || machine.phase == Phase::Finished {
        return Step::Halt;
    }
    let length = match u32::try_from(refusal.text.len()) {
        Ok(text) => text.checked_add(14),
        Err(_) => None,
    };
    let frame = if match length {
        Some(length) => machine.fits(length, 1),
        None => false,
    } {
        codec::refusal(&machine.schema, &refusal)
    } else {
        None
    };
    if let Some(frame) = frame {
        machine.queue(frame).expect("refusal preflight reserves bytes/slot");
        machine.phase = Phase::Finished;
        machine.reading = Reading::Ended;
        machine.input = Input::Stopped;
        Step::NeedPoll
    } else {
        output::stop(machine, Fault::OutputFull, events, lower);
        Step::Halt
    }
}

pub(super) fn check_version(machine: &Machine, version: u16) -> Option<()> {
    let (lowest, highest) = machine.schema.supported();
    let offer = machine.offer?;
    if version < lowest || version > highest || version < offer.lowest || version > offer.highest {
        return None;
    }
    Some(())
}

pub(super) fn accept(machine: &mut Machine, version: u16, respond: bool) -> Option<()> {
    check_version(machine, version)?;
    let terms_length = codec::terms_length(&machine.schema, version)?.checked_add(8)?;
    let length = if respond { terms_length.checked_add(10)? } else { terms_length };
    let slots = if respond { 2 } else { 1 };
    if !machine.fits(length, slots) {
        return None;
    }
    machine.install(version)?;
    if respond {
        let frame = codec::short(&machine.schema, 2, version)?;
        machine.queue(frame)?;
    }
    let terms = codec::terms(&machine.schema, version)?;
    machine.queue(terms)?;
    machine.phase = Phase::Terms;
    machine.reading = Reading::Active;
    Some(())
}
