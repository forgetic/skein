//! One exact raw read receipt, common decode and allocation-free skip (§§1–3).
use super::{Input, Machine, Offer, Reading, identity, output, requests};
use crate::{Event, Fault, LowerRequest, OpeningMode, Phase, Role, Step, codec};
use alloc::boxed::Box;
use skein_lib::{Queue, Writer, stream};

pub(super) fn stream(
    machine: &mut Machine,
    event: stream::Up,
    events: &mut Queue<Event>,
    lower: &mut Queue<LowerRequest>,
) -> Step {
    if !machine.live() {
        match event {
            stream::Up::Failed(fault) => {
                if machine.logical_fault.is_none() {
                    output::stop(machine, Fault::Stream(fault), events, lower);
                }
            }
            stream::Up::Bytes(_) | stream::Up::Room | stream::Up::End => {}
        }
        return Step::Halt;
    }
    match event {
        stream::Up::Failed(fault) => {
            output::stop(machine, Fault::Stream(fault), events, lower);
            Step::Halt
        }
        stream::Up::End => {
            if machine.phase == Phase::Finished || machine.reading == Reading::Ended {
                machine.withdraw(lower);
                Step::NeedPoll
            } else {
                output::stop(machine, Fault::End, events, lower);
                Step::Halt
            }
        }
        stream::Up::Room => {
            output::stop(machine, Fault::Framing, events, lower);
            Step::Halt
        }
        stream::Up::Bytes(bytes) => {
            if machine.phase == Phase::Finished || machine.reading == Reading::Ended {
                machine.demand = None;
                return Step::NeedPoll;
            }
            match receive(machine, bytes, events, lower) {
                Ok(step) => step,
                Err(fault) => {
                    output::stop(machine, fault, events, lower);
                    Step::Halt
                }
            }
        }
    }
}

fn receive(
    machine: &mut Machine,
    bytes: Box<[u8]>,
    events: &mut Queue<Event>,
    lower: &mut Queue<LowerRequest>,
) -> Result<Step, Fault> {
    let count = machine.demand.take().ok_or(Fault::Framing)?;
    if u32::try_from(bytes.len()).ok() != Some(count) {
        return Err(Fault::Framing);
    }
    let source = core::mem::replace(&mut machine.input, Input::Stopped);
    match source {
        Input::Header => header(machine, bytes, events, lower),
        Input::Body { kind, length, receipt, mut writer } => {
            if writer.put(&bytes).is_err() {
                return Err(Fault::Framing);
            }
            if writer.room() == 0 {
                let body = writer.finish();
                deliver(machine, kind, receipt, body, events, lower)
            } else {
                machine.input = Input::Body { kind, length, receipt, writer };
                Ok(Step::NeedPoll)
            }
        }
        Input::Skip { kind, remaining } => {
            let remaining = remaining.checked_sub(count).ok_or(Fault::Framing)?;
            machine.input = if remaining == 0 { Input::Skipped { kind } } else { Input::Skip { kind, remaining } };
            Ok(Step::NeedPoll)
        }
        Input::AwaitDecode { .. } | Input::Skipped { .. } | Input::Stopped => Err(Fault::Framing),
    }
}

fn header(
    machine: &mut Machine,
    bytes: Box<[u8]>,
    events: &mut Queue<Event>,
    lower: &mut Queue<LowerRequest>,
) -> Result<Step, Fault> {
    let header = codec::framing(&bytes).ok_or(Fault::Framing)?;
    let bound = body_bound(machine, header.kind)?;
    if let Some(bound) = bound {
        if header.body_bytes > bound {
            return Err(Fault::Framing);
        }
        let receipt =
            if header.kind > 17 { Some(identity(&mut machine.next_receipt).ok_or(Fault::Limits)?) } else { None };
        if header.body_bytes == 0 {
            return deliver(machine, header.kind, receipt, Box::from([]), events, lower);
        }
        machine.input = Input::Body {
            kind: header.kind,
            length: header.body_bytes,
            receipt,
            writer: Writer::new(usize::try_from(header.body_bytes).ok().ok_or(Fault::Limits)?),
        };
    } else {
        if machine.phase != Phase::Ready
            || (!machine.first_receive.done && machine.first_receive.kind.is_some())
            || header.body_bytes > machine.schema.version(machine.selected).ok_or(Fault::Framing)?.unknown_body_bytes
        {
            return Err(Fault::Framing);
        }
        machine.input = if header.body_bytes == 0 {
            Input::Skipped { kind: header.kind }
        } else {
            Input::Skip { kind: header.kind, remaining: header.body_bytes }
        };
    }
    Ok(Step::NeedPoll)
}

fn body_bound(machine: &Machine, kind: u16) -> Result<Option<u32>, Fault> {
    match kind {
        1 => {
            if machine.phase != Phase::Opening || machine.schema.role != Role::Responder {
                return Err(Fault::Framing);
            }
            Ok(Some(256))
        }
        2 => {
            if machine.phase != Phase::Opening || machine.schema.role != Role::Initiator || machine.offer.is_none() {
                return Err(Fault::Framing);
            }
            Ok(Some(2))
        }
        3 => Ok(Some(512)),
        4 => {
            if !machine.schema.profile().ping_allowed || machine.phase != Phase::Ready {
                return Err(Fault::Framing);
            }
            if machine.first_receive.kind.is_some()
                && !machine.first_receive.done
                && !machine.schema.version(machine.selected).ok_or(Fault::Framing)?.first.ping_before_receive
            {
                return Err(Fault::Framing);
            }
            Ok(Some(0))
        }
        16 => {
            if machine.phase != Phase::Terms {
                return Err(Fault::Framing);
            }
            Ok(Some(196))
        }
        17 => {
            if machine.phase != Phase::Ready || (!machine.first_receive.done && machine.first_receive.kind.is_some()) {
                return Err(Fault::Framing);
            }
            Ok(Some(2))
        }
        _ => {
            if let Some(known) = machine.schema.known(kind)
                && (known.channel != machine.schema.profile().channel || !machine.schema.flows(kind, true))
            {
                return Err(Fault::Framing);
            }
            if let Some(rule) = machine.schema.rule(machine.selected, kind) {
                if machine.phase != Phase::Ready
                    || (machine.first_receive.kind.is_some()
                        && !machine.first_receive.done
                        && machine.first_receive.kind != Some(kind))
                {
                    return Err(Fault::Framing);
                }
                Ok(Some(rule.body_bytes))
            } else {
                Ok(None)
            }
        }
    }
}

fn deliver(
    machine: &mut Machine,
    kind: u16,
    receipt: Option<skein_lib::Token>,
    bytes: Box<[u8]>,
    events: &mut Queue<Event>,
    lower: &mut Queue<LowerRequest>,
) -> Result<Step, Fault> {
    if kind > 17 {
        let receipt = receipt.ok_or(Fault::Limits)?;
        machine.input = Input::AwaitDecode { kind, receipt };
        events.push(Event::Body { receipt, version: machine.selected, kind, bytes });
        return Ok(Step::NeedDecode { receipt });
    }
    machine.input = Input::Header;
    match kind {
        1 => opening(machine, &bytes, events),
        2 => accepted(machine, &bytes),
        3 => refused(machine, &bytes, events, lower),
        4 => {
            if !bytes.is_empty() {
                return Err(Fault::Framing);
            }
            machine.received = machine.received.saturating_add(1);
            Ok(Step::NeedPoll)
        }
        16 => terms(machine, &bytes, events),
        17 => {
            let kind = codec::read_short(&bytes).ok_or(Fault::Framing)?;
            machine.received = machine.received.saturating_add(1);
            machine.reading = Reading::Paused;
            events.push(Event::Unsupported { kind });
            Ok(Step::NeedPoll)
        }
        _ => Err(Fault::Framing),
    }
}

fn opening(machine: &mut Machine, bytes: &[u8], events: &mut Queue<Event>) -> Result<Step, Fault> {
    let opening = codec::read_open(&machine.schema, bytes).ok_or(Fault::Framing)?;
    machine.received = machine.received.saturating_add(1);
    let (lowest, highest) = machine.schema.supported();
    if opening.lowest > highest || opening.highest < lowest {
        return Err(Fault::Version);
    }
    machine.offer = Some(Offer { lowest: opening.lowest, highest: opening.highest });
    match machine.schema.profile().mode {
        OpeningMode::AskParent => {
            machine.phase = Phase::Authorizing;
            machine.reading = Reading::Paused;
            events.push(Event::Opening { opening });
        }
        OpeningMode::AcceptHighest => {
            requests::accept(machine, opening.highest.min(highest), true).ok_or(Fault::OutputFull)?;
        }
    }
    Ok(Step::NeedPoll)
}

fn accepted(machine: &mut Machine, bytes: &[u8]) -> Result<Step, Fault> {
    let version = codec::read_short(bytes).ok_or(Fault::Framing)?;
    machine.received = machine.received.saturating_add(1);
    requests::check_version(machine, version).ok_or(Fault::Version)?;
    requests::accept(machine, version, false).ok_or(Fault::OutputFull)?;
    Ok(Step::NeedPoll)
}

fn terms(machine: &mut Machine, bytes: &[u8], events: &mut Queue<Event>) -> Result<Step, Fault> {
    // Structural body failure never increments; valid Terms with bad semantics
    // is a fully decoded frame and retains the original counter stage.
    let mut reader = skein_lib::Reader::new(bytes);
    let count = reader.u32().ok_or(Fault::Framing)?;
    if count > machine.schema.limits().terms || count.checked_mul(6) != Some(reader.remaining()) {
        return Err(Fault::Framing);
    }
    machine.received = machine.received.saturating_add(1);
    codec::check_terms(&machine.schema, machine.selected, bytes).ok_or(Fault::Limits)?;
    machine.phase = Phase::Ready;
    let first = machine.schema.version(machine.selected).ok_or(Fault::Limits)?.first;
    machine.reading = if first.receive.is_some() || first.initial_read { Reading::Active } else { Reading::Paused };
    events.push(Event::Ready { version: machine.selected });
    Ok(Step::NeedPoll)
}

fn refused(
    machine: &mut Machine,
    bytes: &[u8],
    events: &mut Queue<Event>,
    lower: &mut Queue<LowerRequest>,
) -> Result<Step, Fault> {
    let refusal = codec::read_refusal(&machine.schema, bytes).ok_or(Fault::Framing)?;
    machine.received = machine.received.saturating_add(1);
    events.push(Event::Refused { reason: refusal.reason, text: refusal.text });
    output::stop(machine, Fault::Closed, events, lower);
    Ok(Step::Halt)
}
