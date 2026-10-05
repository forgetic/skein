//! Independent checks on the browser's observable contract.

use std::collections::{BTreeMap, BTreeSet};

use skein_browser::boundary::{Event, Expect};
use skein_lib::Token;

/// Checks terminals, closure, press refusal, and `Await` truth at the time
/// the result is emitted. The fake supplies its visible match count.
#[derive(Debug, Default)]
pub struct Referee {
    terminal: BTreeSet<u64>,
    expectations: BTreeMap<u64, Expect>,
    presses: BTreeMap<u64, usize>,
    mouse_events: usize,
    closed: bool,
}

impl Referee {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn await_op(&mut self, op: Token, expect: Expect) {
        assert!(self.expectations.insert(op.raw(), expect).is_none(), "operation registered once");
    }

    pub fn press(&mut self, op: Token) {
        assert!(self.presses.insert(op.raw(), self.mouse_events).is_none(), "press registered once");
    }

    pub fn command(&mut self, bytes: &[u8]) {
        if bytes.windows(b"Input.dispatchMouseEvent".len()).any(|part| part == b"Input.dispatchMouseEvent") {
            self.mouse_events += 1;
        }
    }

    pub fn event(&mut self, event: &Event, visible_matches: u32) {
        assert!(!self.closed, "nothing is emitted after Closed");
        let op = match event {
            Event::Found { op, .. }
            | Event::Met { op, .. }
            | Event::Missed { op, .. }
            | Event::Done { op }
            | Event::Refused { op, .. }
            | Event::Snapshot { op, .. }
            | Event::Screenshot { op, .. } => Some(*op),
            Event::Ready { .. } | Event::Opened { .. } | Event::Trouble { .. } | Event::Closed { .. } => None,
        };
        if let Some(op) = op {
            assert!(self.terminal.insert(op.raw()), "one terminal per operation: {op:?}");
        }
        if let Event::Met { op, .. } = event {
            let expected = self.expectations.get(&op.raw()).expect("Met belongs to a registered Await");
            let true_now = match expected {
                Expect::Present => visible_matches > 0,
                Expect::Count(count) => visible_matches == *count,
                Expect::Absent => visible_matches == 0,
            };
            assert!(true_now, "Met requires the fake page to satisfy its expectation");
        }
        if let Event::Refused { op, .. } = event
            && let Some(at) = self.presses.get(&op.raw())
        {
            assert_eq!(self.mouse_events, *at, "refused press dispatched a mouse event");
        }
        if matches!(event, Event::Closed { owner } if *owner == Token::new(1)) {
            self.closed = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Referee;
    use skein_browser::boundary::{Event, Expect, Refusal};
    use skein_lib::{List, Token};

    #[test]
    #[should_panic(expected = "one terminal per operation")]
    fn rejects_two_terminals() {
        let mut referee = Referee::new();
        referee.event(&Event::Done { op: Token::new(4) }, 0);
        referee.event(&Event::Done { op: Token::new(4) }, 0);
    }

    #[test]
    #[should_panic(expected = "nothing is emitted after Closed")]
    fn rejects_event_after_browser_closed() {
        let mut referee = Referee::new();
        referee.event(&Event::Closed { owner: Token::new(1) }, 0);
        referee.event(&Event::Ready { version: b"late".to_vec().into_boxed_slice() }, 0);
    }

    #[test]
    #[should_panic(expected = "refused press dispatched a mouse event")]
    fn rejects_mouse_before_refused_press() {
        let mut referee = Referee::new();
        referee.press(Token::new(4));
        referee.command(br#"{"method":"Input.dispatchMouseEvent"}"#);
        referee.event(&Event::Refused { op: Token::new(4), why: Refusal::Covered }, 0);
    }

    #[test]
    #[should_panic(expected = "Met requires the fake page")]
    fn rejects_false_met() {
        let mut referee = Referee::new();
        referee.await_op(Token::new(4), Expect::Present);
        referee.event(&Event::Met { op: Token::new(4), seen: List::with_capacity(0) }, 0);
    }
}
