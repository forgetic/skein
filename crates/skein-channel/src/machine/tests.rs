//! Checked exhaustion exercised through genuine production entrances.
use super::{Input, Machine, Right};
use crate::{Disposition, Event, Fault, LowerEvent, LowerRequest, Phase, Request, Step, tests_support};
use skein_lib::{Queue, Token, stream};

fn ready() -> Machine {
    let mut machine = Machine::new(tests_support::schema());
    machine.install(1).expect("supported version");
    machine.phase = Phase::Ready;
    machine
}

fn receipt(event: Event) -> Option<Token> {
    match event {
        Event::Body { receipt, .. } => Some(receipt),
        Event::Opening { .. }
        | Event::Ready { .. }
        | Event::Unsupported { .. }
        | Event::Refused { .. }
        | Event::Sent { .. }
        | Event::Unsent { .. }
        | Event::ReadEnded
        | Event::Closed { .. } => None,
    }
}

fn closed(event: Event) -> Option<Fault> {
    match event {
        Event::Closed { fault } => Some(fault),
        Event::Opening { .. }
        | Event::Ready { .. }
        | Event::Body { .. }
        | Event::Unsupported { .. }
        | Event::Refused { .. }
        | Event::Sent { .. }
        | Event::Unsent { .. }
        | Event::ReadEnded => None,
    }
}

fn observed(request: LowerRequest) -> (u8, Token) {
    match request {
        LowerRequest::Output(output) => match output {
            stream::OutputDown::Room { right, .. } => (1, right),
            stream::OutputDown::Cancel { right } => (2, right),
            stream::OutputDown::Send { right, .. } => (3, right),
            stream::OutputDown::Release { right } => (4, right),
        },
        LowerRequest::Stream(stream) => match stream {
            stream::Down::Demand { .. } => (5, Token::new(0)),
            stream::Down::Finish => (6, Token::new(0)),
            stream::Down::Send(_) => (7, Token::new(0)),
        },
    }
}

#[test]
fn last_body_identity_is_delivered_once_then_exhaustion_precedes_next_allocation() {
    let mut machine = ready();
    machine.next_receipt = Some(u64::MAX);
    let mut events = Queue::with_capacity(2);
    let mut lower = Queue::with_capacity(3);
    machine.demand = Some(8);
    let step = super::up(
        &mut machine,
        LowerEvent::Stream(stream::Up::Bytes(Box::from([1, 1, 0, 0, 0, 0, 0, 0]))),
        &mut events,
        &mut lower,
    );
    assert_eq!(step, Step::NeedDecode { receipt: Token::new(u64::MAX) }, "last identity is delivered exactly once");
    assert_eq!(receipt(events.pop().expect("one Body")), Some(Token::new(u64::MAX)), "last exact receipt");
    assert_eq!(
        super::resolve(&mut machine, Token::new(u64::MAX), Disposition::DecodedContinue, &mut events, &mut lower),
        Step::NeedPoll,
        "state-only resolve"
    );
    machine.demand = Some(8);
    let step = super::up(
        &mut machine,
        LowerEvent::Stream(stream::Up::Bytes(Box::from([1, 1, 0, 0, 0, 0, 0, 1]))),
        &mut events,
        &mut lower,
    );
    assert_eq!(step, Step::Halt, "exhaustion forbids ordinary final poll");
    assert_eq!(closed(events.pop().expect("logical Closed")), Some(Fault::Limits), "exact checked identity failure");
    let stopped = match machine.input {
        Input::Stopped => true,
        Input::Header | Input::Body { .. } | Input::AwaitDecode { .. } | Input::Skip { .. } | Input::Skipped { .. } => {
            false
        }
    };
    assert!(stopped, "no body allocation after exhausted identity");
}

#[test]
fn exhausted_receipt_keeps_actual_waiting_output_until_its_late_winner() {
    let mut machine = ready();
    machine.next_receipt = None;
    machine.right = Right::Waiting { right: Token::new(44) };
    machine.demand = Some(8);
    let mut events = Queue::with_capacity(2);
    let mut lower = Queue::with_capacity(3);
    super::up(
        &mut machine,
        LowerEvent::Stream(stream::Up::Bytes(Box::from([1, 1, 0, 0, 0, 0, 0, 1]))),
        &mut events,
        &mut lower,
    );
    assert_eq!(observed(lower.pop().expect("Cancel pending real right")), (2, Token::new(44)), "no invented terminal");
    assert_eq!(closed(events.pop().expect("logical Closed")), Some(Fault::Limits), "exact receipt exhaustion");
    super::up(&mut machine, LowerEvent::Closed, &mut events, &mut lower);
    assert!(!machine.is_retired(), "anonymous Closed cannot consume pending name");
    super::up(
        &mut machine,
        LowerEvent::Output(stream::OutputUp::Settled {
            right: Token::new(44),
            outcome: stream::OutputOutcome::Granted,
        }),
        &mut events,
        &mut lower,
    );
    assert_eq!(
        observed(lower.pop().expect("Release real late grant")),
        (4, Token::new(44)),
        "actual late winner consumed once"
    );
    assert!(machine.is_retired(), "physical resource and actual named winner both settled");
    assert!(events.is_empty(), "no duplicate logical Closed");
}

#[test]
fn last_output_identity_issued_once_and_exhaustion_stops_without_another_room() {
    let mut machine = ready();
    machine.next_output = Some(u64::MAX);
    let mut events = Queue::with_capacity(2);
    let mut lower = Queue::with_capacity(3);
    super::poll(&mut machine, &mut events, &mut lower);
    assert_eq!(observed(lower.pop().expect("read Demand")), (5, Token::new(0)), "one read demand");
    assert_eq!(observed(lower.pop().expect("last Room")), (1, Token::new(u64::MAX)), "last output identity once");
    super::up(
        &mut machine,
        LowerEvent::Output(stream::OutputUp::Settled {
            right: Token::new(u64::MAX),
            outcome: stream::OutputOutcome::Granted,
        }),
        &mut events,
        &mut lower,
    );
    super::down(&mut machine, Request::Ping { owner: Token::new(9) }, &mut events, &mut lower);
    events.pop().expect("Ping admitted");
    super::poll(&mut machine, &mut events, &mut lower);
    assert_eq!(closed(events.pop().expect("logical Closed")), Some(Fault::Limits), "no identity reuse");
    assert_eq!(lower.len(), 3, "withdraw, true-grant refusal, Finish, no stacked normal pump");
    assert_eq!(observed(lower.pop().expect("withdraw")), (5, Token::new(0)), "actual read withdrawal");
    assert_eq!(
        observed(lower.pop().expect("best effort refusal")),
        (3, Token::new(u64::MAX)),
        "only real retained grant permits refusal"
    );
    assert_eq!(observed(lower.pop().expect("Finish")), (6, Token::new(0)), "bounded final retirement");
}
