//! skein-json's step tests (testing-strategy.md, 2.1): the tokenizer fed
//! by hand, one event at a time, and read through a stream that meets its
//! demands from a buffer; the writer's two passes; and the checks of UTF-8
//! and of a number's text. The machine worlds are in tests/json.

#![expect(clippy::disallowed_types, reason = "a test collects what it reads in a Vec")]

mod text;
mod tokenizer;
mod writer;

use alloc::boxed::Box;
use alloc::vec::Vec;

use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, Intake, Queue, Time, Wall};

use crate::Token;
use crate::tokenizer::{self as json, Event, Limits, Request, Tokenizer};

/// Small limits, so that a test reaches each of them.
const LIMITS: Limits = Limits { depth: 4, string: 16, number: 8, chunk: 4, length: 1024 };

fn env(limits: Limits) -> Env<Limits> {
    Env { now: Time::ZERO, wall: Wall::EPOCH, limits }
}

fn boxed(bytes: &[u8]) -> Box<[u8]> {
    Box::from(bytes)
}

fn key(text: &[u8]) -> Token {
    Token::Key(boxed(text))
}

fn string(text: &[u8]) -> Token {
    Token::String(boxed(text))
}

fn number(text: &[u8]) -> Token {
    Token::Number(boxed(text))
}

/// A tokenizer and its two queues, with room for what one call emits.
struct Machine {
    tokenizer: Tokenizer,
    env: Env<Limits>,
    above: Queue<Event>,
    below: Queue<Down>,
}

impl Machine {
    fn new(limits: Limits) -> Machine {
        Machine {
            tokenizer: Tokenizer::new(&limits),
            env: env(limits),
            above: Queue::with_capacity(json::UP_MAX_OUT.above.max(json::DOWN_MAX_OUT.above)),
            below: Queue::with_capacity(json::UP_MAX_OUT.below.max(json::DOWN_MAX_OUT.below)),
        }
    }

    /// Sends `rq` down; what came of it, above and below.
    fn down(&mut self, rq: Request) -> (Option<Event>, Option<Down>) {
        json::down(&mut self.tokenizer, &self.env, rq, &mut self.above, &mut self.below);
        self.take()
    }

    /// Sends `ev` up; what came of it, above and below.
    fn up(&mut self, ev: Up) -> (Option<Event>, Option<Down>) {
        json::up(&mut self.tokenizer, &self.env, ev, &mut self.above, &mut self.below);
        self.take()
    }

    /// Delivers `bytes`.
    fn bytes(&mut self, bytes: &[u8]) -> (Option<Event>, Option<Down>) {
        self.up(Up::Bytes(boxed(bytes)))
    }

    fn take(&mut self) -> (Option<Event>, Option<Down>) {
        let taken = (self.above.pop(), self.below.pop());
        assert!(self.above.is_empty() && self.below.is_empty(), "one of each at most");
        taken
    }
}

#[expect(clippy::unnecessary_wraps, reason = "compared with what a call sent below, an Option")]
fn demand(read: Read) -> Option<Down> {
    Some(Down::Demand { read, room: 0 })
}

/// What a document comes to, read to its outcome through a stream that
/// holds all of it and meets each demand from it: the events, the last its
/// outcome.
fn read(document: &[u8], limits: Limits) -> Vec<Event> {
    let mut machine = Machine::new(limits);
    let mut intake = Intake::with_capacity(u32::try_from(document.len()).unwrap().max(json::largest_demand(&limits)));
    intake.append(document).unwrap();
    let mut events = Vec::new();
    let mut demanded = None;
    for _ in 0..document.len().checked_mul(4).unwrap().checked_add(8).unwrap() {
        let (event, down) = match demanded.take() {
            None => machine.down(Request::Next),
            Some(read) => match intake.meet(read) {
                Some(bytes) => machine.up(Up::Bytes(bytes)),
                None => machine.up(Up::End),
            },
        };
        match down {
            Some(Down::Demand { read, room: 0 }) => demanded = Some(read),
            None => {}
            Some(other) => panic!("the tokenizer sent {other:?} down"),
        }
        let Some(event) = event else { continue };
        assert!(demanded.is_none(), "an answer leaves nothing demanded");
        let over = match &event {
            Event::Token(_) => false,
            Event::Done | Event::Failed(_) => true,
            Event::Closed => panic!("closed unasked"),
        };
        events.push(event);
        if over {
            return events;
        }
    }
    panic!("a document is read in a few steps per byte");
}

/// The tokens of `document`, which must be read whole.
fn tokens(document: &[u8]) -> Vec<Token> {
    let mut events = read(document, LIMITS);
    assert_eq!(events.pop(), Some(Event::Done), "{}", document.escape_ascii());
    let mut tokens = Vec::new();
    for event in events {
        let Event::Token(token) = event else { panic!("tokens before the outcome") };
        tokens.push(token);
    }
    tokens
}

/// Why `document` fails, read with `limits`.
fn failure(document: &[u8], limits: Limits) -> json::Error {
    match read(document, limits).pop() {
        Some(Event::Failed(error)) => error,
        other => panic!("{} ends with {other:?}", document.escape_ascii()),
    }
}
