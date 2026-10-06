//! C/P/S admission and whole-cap affine lower rights (channel.md §§4–6).
use super::{Input, Machine, Reading, Right, identity};
use crate::{Event, Fault, LowerRequest, Phase, Refusal, Step, codec};
use alloc::boxed::Box;
use skein_lib::{Queue, Token, stream};

pub(super) fn stop(machine: &mut Machine, fault: Fault, events: &mut Queue<Event>, lower: &mut Queue<LowerRequest>) {
    if machine.logical_fault.is_some() {
        return;
    }
    machine.logical_fault = Some(fault);
    machine.phase = Phase::Closing;
    machine.input = Input::Stopped;
    machine.reading = Reading::Ended;
    machine.withdraw(lower);
    for _ in 0..machine.output.len() {
        let frame = machine.output.pop().expect("drain bounded by original queue length");
        drop(frame);
    }
    machine.ledger.pending = 0;
    retire(machine, refusal_reason(fault), lower);
    events.push(Event::Closed { fault });
}

fn refusal_reason(fault: Fault) -> Option<u16> {
    match fault {
        Fault::Framing => Some(6),
        Fault::Limits => Some(3),
        Fault::Version => Some(1),
        Fault::End | Fault::Stream(_) | Fault::Closed | Fault::OutputFull => None,
    }
}

fn finish(machine: &mut Machine, lower: &mut Queue<LowerRequest>) {
    if !machine.finish_sent && machine.right == Right::Idle {
        machine.finish_sent = true;
        if !machine.resource_closed {
            lower.push(LowerRequest::Stream(stream::Down::Finish));
        }
    }
}

fn retire(machine: &mut Machine, reason: Option<u16>, lower: &mut Queue<LowerRequest>) {
    match machine.right {
        Right::Waiting { right } => {
            machine.right = Right::Retiring { right };
            lower.push(LowerRequest::Output(stream::OutputDown::Cancel { right }));
        }
        Right::Granted { right } => {
            machine.right = Right::Idle;
            if let Some(reason) = reason {
                let refusal = Refusal { reason, text: Box::from([]) };
                let frame = codec::refusal(&machine.schema, &refusal).expect("frozen empty refusal fits C");
                lower.push(LowerRequest::Output(stream::OutputDown::Send { right, bytes: frame.bytes }));
            } else {
                lower.push(LowerRequest::Output(stream::OutputDown::Release { right }));
            }
            finish(machine, lower);
        }
        Right::Idle => finish(machine, lower),
        Right::Retiring { .. } => {}
    }
}

pub(super) fn settled(
    machine: &mut Machine,
    event: stream::OutputUp,
    events: &mut Queue<Event>,
    lower: &mut Queue<LowerRequest>,
) -> Step {
    let stream::OutputUp::Settled { right, outcome } = event;
    match machine.right {
        Right::Waiting { right: current } => {
            if current != right {
                return Step::Halt;
            }
            active_terminal(machine, right, outcome, events, lower)
        }
        Right::Retiring { right: current } => {
            if current != right {
                return Step::Halt;
            }
            machine.right = Right::Idle;
            match outcome {
                stream::OutputOutcome::Granted => {
                    lower.push(LowerRequest::Output(stream::OutputDown::Release { right }));
                }
                stream::OutputOutcome::Cancelled | stream::OutputOutcome::Failed(_) => {}
            }
            finish(machine, lower);
            Step::Halt
        }
        Right::Idle | Right::Granted { .. } => Step::Halt,
    }
}

fn active_terminal(
    machine: &mut Machine,
    right: Token,
    outcome: stream::OutputOutcome,
    events: &mut Queue<Event>,
    lower: &mut Queue<LowerRequest>,
) -> Step {
    match outcome {
        stream::OutputOutcome::Granted => {
            machine.right = Right::Granted { right };
            machine.ledger.spent = 0;
            machine.ledger.initialized = true;
            Step::NeedPoll
        }
        stream::OutputOutcome::Cancelled => {
            machine.right = Right::Idle;
            stop(machine, Fault::Closed, events, lower);
            Step::Halt
        }
        stream::OutputOutcome::Failed(fault) => {
            machine.right = Right::Idle;
            stop(machine, Fault::Stream(fault), events, lower);
            Step::Halt
        }
    }
}

fn move_front(machine: &mut Machine, lower: &mut Queue<LowerRequest>) {
    match machine.right {
        Right::Granted { right } => {
            if let Some(frame) = machine.output.pop() {
                let length = u32::try_from(frame.bytes.len()).expect("admitted frame has u32 length");
                machine.ledger.pending = machine.ledger.pending.checked_sub(length).expect("P owns this frame");
                machine.ledger.spent = machine.ledger.spent.checked_add(length).expect("P+S stays within C");
                machine.right = Right::Idle;
                lower.push(LowerRequest::Output(stream::OutputDown::Send { right, bytes: frame.bytes }));
            }
        }
        Right::Idle | Right::Waiting { .. } | Right::Retiring { .. } => {}
    }
}

fn queue_status(machine: &mut Machine) {
    let kind = match machine.input {
        Input::Skipped { kind } => kind,
        Input::Header | Input::Body { .. } | Input::AwaitDecode { .. } | Input::Skip { .. } | Input::Stopped => return,
    };
    if !machine.fits(10, 1) {
        return;
    }
    let frame = codec::short(&machine.schema, 17, kind).expect("status preflight fits C");
    machine.queue(frame).expect("status preflight reserves bytes and slot");
    machine.received = machine.received.saturating_add(1);
    machine.input = Input::Header;
}

fn room_after_move(machine: &Machine) -> bool {
    let sending = match machine.right {
        Right::Granted { .. } => !machine.output.is_empty(),
        Right::Idle | Right::Waiting { .. } | Right::Retiring { .. } => false,
    };
    if machine.phase == Phase::Finished && machine.output.len() <= 1 {
        return false;
    }
    machine.needs_room() || sending
}

pub(super) fn poll(machine: &mut Machine, events: &mut Queue<Event>, lower: &mut Queue<LowerRequest>) {
    if !machine.live() {
        return;
    }
    queue_status(machine);
    // Reserve identity before ordinary poll effects; exhaustion uses bounded
    // fatal retirement instead, never stacks a fatal and an ordinary pump.
    let room = if room_after_move(machine) {
        match identity(&mut machine.next_output) {
            Some(right) => Some(right),
            None => {
                stop(machine, Fault::Limits, events, lower);
                return;
            }
        }
    } else {
        None
    };
    move_front(machine, lower);
    if machine.phase == Phase::Finished && machine.output.is_empty() {
        machine.reading = Reading::Ended;
        machine.input = Input::Stopped;
        machine.withdraw(lower);
        machine.phase = Phase::Closing;
        retire(machine, None, lower);
        return;
    }
    if machine.demand.is_none()
        && let Some(count) = machine.read_count()
    {
        machine.demand = Some(count);
        lower.push(LowerRequest::Stream(stream::Down::Demand { read: stream::Read::Fill(count), room: 0 }));
    }
    if let Some(right) = room
        && machine.needs_room()
    {
        machine.right = Right::Waiting { right };
        lower.push(LowerRequest::Output(stream::OutputDown::Room {
            right,
            bytes: machine.schema.limits().queued_bytes,
        }));
    }
}
