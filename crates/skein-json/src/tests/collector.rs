//! Selective collection: dispositions, counts, duplicate admission and lifetime.
use super::{boxed, env, key, number, string};
use crate::collector::{self, Collector, Counts, Error, Event, Filter, Keep, Key, Limits, Node, Request, Waiting};
use crate::document;
use crate::tokenizer;
use crate::{Document, Kind, Token};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, Intake, Queue};

const LIMITS: Limits = Limits {
    tokenizer: tokenizer::Limits { depth: 8, string: 16, number: 8, chunk: 4, length: 4096 },
    tokens: 32,
    text: 64,
    skip: 4096,
};
const FIELDS: &[Node] = &[
    Node { key: Key::Field(b"keep"), keep: Keep::Value },
    Node { key: Key::Field(b"text"), keep: Keep::Text(3) },
    Node { key: Key::Field(b"array"), keep: Keep::Into(&[Node { key: Key::Each, keep: Keep::Text(2) }]) },
];
const FILTER: Filter = Filter { root: Keep::Into(FIELDS) };

struct Machine {
    collector: Collector,
    env: Env<Limits>,
    above: Queue<Event>,
    below: Queue<Down>,
}
impl Machine {
    fn new(filter: Filter, limits: Limits) -> Machine {
        let basic = env(limits.tokenizer);
        Machine {
            collector: Collector::new(filter, &limits),
            env: Env { now: basic.now, wall: basic.wall, limits },
            above: Queue::with_capacity(4),
            below: Queue::with_capacity(4),
        }
    }
    fn down(&mut self, request: Request) {
        collector::down(&mut self.collector, &self.env, request, &mut self.above, &mut self.below);
        assert!(self.above.len() <= 1 && self.below.len() <= 1);
    }
    fn up(&mut self, event: Up) {
        collector::up(&mut self.collector, &self.env, event, &mut self.above, &mut self.below);
        assert!(self.above.len() <= 1 && self.below.len() <= 1);
    }
    fn read(&mut self, bytes: &[u8]) -> Event {
        let mut intake = Intake::with_capacity(
            u32::try_from(bytes.len()).unwrap().max(tokenizer::largest_demand(&self.env.limits.tokenizer)),
        );
        intake.append(bytes).unwrap();
        self.down(Request::Collect);
        for _ in 0..bytes.len().checked_mul(4).unwrap().checked_add(16).unwrap() {
            if let Some(event) = self.above.pop() {
                drop(self.below.pop());
                return event;
            }
            match self.below.pop().expect("an unfinished collect demands bytes") {
                Down::Demand { read, room: 0 } => match intake.meet(read) {
                    Some(bytes) => self.up(Up::Bytes(bytes)),
                    None => self.up(Up::End),
                },
                other @ (Down::Demand { .. } | Down::Send(_) | Down::Finish) => panic!("unexpected {other:?}"),
            }
        }
        panic!("bounded progress");
    }
}
fn collect(bytes: &[u8], filter: Filter, limits: Limits) -> (Event, Counts) {
    let mut machine = Machine::new(filter, limits);
    let event = machine.read(bytes);
    (event, machine.collector.counts())
}
fn expected(tokens: &[Token]) -> Event {
    Event::Collected(Document::from_tokens(tokens, &document::Limits { tokens: 32, text: 64 }).unwrap())
}

#[test]
fn named_and_unnamed_fields_and_each_disposition() {
    let (event, counts) =
        collect(br#"{"absent":{"x":[1,false]},"keep":{"x":7},"array":["a","ab"],"text":"abc"}"#, FILTER, LIMITS);
    assert_eq!(
        event,
        expected(&[
            Token::ObjectStart,
            key(b"keep"),
            Token::ObjectStart,
            key(b"x"),
            number(b"7"),
            Token::ObjectEnd,
            key(b"array"),
            Token::ArrayStart,
            string(b"a"),
            string(b"ab"),
            Token::ArrayEnd,
            key(b"text"),
            string(b"abc"),
            Token::ObjectEnd
        ])
    );
    assert_eq!(counts.skipped, 15);
    let empty = Filter { root: Keep::Into(&[]) };
    assert_eq!(collect(b"[1,false,{},[],null]", empty, LIMITS).0, expected(&[Token::ArrayStart, Token::ArrayEnd]));
    assert_eq!(collect(b"17", empty, LIMITS).0, expected(&[number(b"17")]));
}
#[test]
fn long_values_keep_only_their_decoded_length_and_unknown_long_keys_skip() {
    let (event, counts) = collect(br#"{"text":"abcd","a_very_long_unknown_name":"huge"}"#, FILTER, LIMITS);
    let Event::Collected(document) = event else { panic!("collected") };
    assert_eq!(document.token(2).unwrap().kind, Kind::Long);
    assert_eq!(document.token(2).unwrap().len, 4);
    assert_eq!(document.text_len(), 4);
    assert_eq!(counts.skipped, 6);
    assert_eq!(
        collect(br#"{"text":"abc"}"#, FILTER, LIMITS).0,
        expected(&[Token::ObjectStart, key(b"text"), string(b"abc"), Token::ObjectEnd])
    );
}
#[test]
fn only_a_named_duplicate_is_rejected() {
    assert_eq!(collect(br#"{"keep":1,"keep":2}"#, FILTER, LIMITS).0, Event::Failed(Error::Duplicate));
    assert_eq!(
        collect(br#"{"other":1,"other":2}"#, FILTER, LIMITS).0,
        expected(&[Token::ObjectStart, Token::ObjectEnd])
    );
    assert_eq!(
        collect(br#"{"keep":{"x":1,"x":2}}"#, FILTER, LIMITS).0,
        expected(&[
            Token::ObjectStart,
            key(b"keep"),
            Token::ObjectStart,
            key(b"x"),
            number(b"1"),
            key(b"x"),
            number(b"2"),
            Token::ObjectEnd,
            Token::ObjectEnd
        ])
    );
}
#[test]
fn each_retained_count_and_skip_count_accepts_its_edge_then_fails() {
    let limits = Limits { tokens: 4, text: 5, skip: 3, ..LIMITS };
    let bytes = br#"{"keep":1,"omit":123}"#;
    let (_, counts) = collect(bytes, FILTER, limits);
    assert_eq!(counts, Counts { tokens: 4, text: 5, skipped: 3 });
    assert_eq!(
        collect(bytes, FILTER, limits).0,
        expected(&[Token::ObjectStart, key(b"keep"), number(b"1"), Token::ObjectEnd])
    );
    assert_eq!(collect(bytes, FILTER, Limits { tokens: 3, ..limits }).0, Event::Failed(Error::TooManyTokens));
    assert_eq!(collect(bytes, FILTER, Limits { text: 4, ..limits }).0, Event::Failed(Error::TooMuchText));
    assert_eq!(collect(bytes, FILTER, Limits { skip: 2, ..limits }).0, Event::Failed(Error::SkippedTooLong));
}
#[test]
fn dropped_text_is_still_validated() {
    assert_eq!(
        collect(br#"{"omit":"bad\q"}"#, FILTER, LIMITS).0,
        Event::Failed(Error::Tokenizer(tokenizer::Error::Escape))
    );
    assert_eq!(
        collect(br#"{"text":"long\q"}"#, FILTER, LIMITS).0,
        Event::Failed(Error::Tokenizer(tokenizer::Error::Escape))
    );
}
#[test]
fn close_in_idle_reading_and_over_and_restart_after_each_outcome() {
    for state in 0_u32..3 {
        let mut machine = Machine::new(FILTER, LIMITS);
        if state == 1 {
            machine.down(Request::Collect);
            drop(machine.below.pop());
        }
        if state == 2 {
            drop(machine.read(b"{}"));
        }
        machine.down(Request::Close);
        assert_eq!(machine.above.pop(), Some(Event::Closed));
        assert_eq!(machine.collector.waiting(), Waiting::Nothing);
        if state == 1 {
            assert_eq!(machine.below.pop(), Some(Down::Demand { read: Read::Nothing, room: 0 }));
        }
        machine.up(Up::Bytes(boxed(b"{")));
        machine.up(Up::End);
        assert!(machine.above.is_empty());
    }
    let mut machine = Machine::new(FILTER, LIMITS);
    for bytes in [b"{}".as_slice(), br#"{"keep":1,"keep":2}"#, br#"{"omit":"bad\q"}"#] {
        drop(machine.read(bytes));
        machine.collector.restart(Filter { root: Keep::Value });
        assert_eq!(machine.collector.waiting(), Waiting::Collect);
        assert_eq!(machine.collector.counts(), Counts { tokens: 0, text: 0, skipped: 0 });
        assert_eq!(machine.read(b"7"), expected(&[number(b"7")]));
        machine.collector.restart(FILTER);
    }
}

#[test]
fn skip_count_failure_stops_before_a_large_values_end() {
    let limits = Limits { skip: 3, ..LIMITS };
    assert_eq!(collect(br#"{"omit":"abcdefgh"#, FILTER, limits).0, Event::Failed(Error::SkippedTooLong));
}

#[test]
#[should_panic(expected = "a Close after Closed")]
fn a_second_close_is_an_owner_bug() {
    let mut machine = Machine::new(FILTER, LIMITS);
    machine.down(Request::Close);
    drop(machine.above.pop());
    machine.down(Request::Close);
}
