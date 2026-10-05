//! The browser's vocabulary above and below its step machine.

use alloc::boxed::Box;

use skein_lib::stream;
use skein_lib::{Duration, List, Token};

/// A backend node id from Chromium's accessibility tree.
pub type Node = u64;

/// An event from one of the child's pipes.
#[derive(PartialEq, Eq, Debug)]
pub enum Below {
    Commands(stream::Up),
    Replies(stream::Up),
    Errors(stream::Up),
}

/// A request to one of the child's pipes.
#[derive(PartialEq, Eq, Debug)]
pub enum Down {
    Commands(stream::Down),
    Replies(stream::Down),
    Errors(stream::Down),
}

/// What the test asks a browser to do.
#[derive(PartialEq, Eq, Debug)]
pub enum Request {
    Person { person: Token },
    Page { person: Token, page: Token, url: Box<[u8]> },
    Go { page: Token, op: Token, to: Go },
    Find { page: Token, op: Token, query: Query },
    Await { page: Token, op: Token, query: Query, expect: Expect, within: Duration },
    Press { page: Token, op: Token, node: Node },
    Type { page: Token, op: Token, node: Node, text: Box<[u8]> },
    Compose { page: Token, op: Token, node: Node, text: Box<[u8]> },
    Key { page: Token, op: Token, key: Key },
    Snapshot { page: Token, op: Token },
    Screenshot { page: Token, op: Token },
    Close { entity: Token },
}

/// What the browser reports to the test.
#[derive(Debug)]
pub enum Event {
    Ready { version: Box<[u8]> },
    Opened { page: Token },
    Found { op: Token, seen: List<Seen>, more: u32 },
    Met { op: Token, seen: List<Seen> },
    Missed { op: Token, seen: List<Seen>, more: u32 },
    Done { op: Token },
    Refused { op: Token, why: Refusal },
    Snapshot { op: Token, text: Box<[u8]> },
    Screenshot { op: Token, png: Box<[u8]> },
    Trouble { page: Token, trouble: Trouble, text: Box<[u8]> },
    Closed { owner: Token },
}

/// Where to navigate.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Go {
    Address(Box<[u8]>),
    Reload,
    Back,
    Forward,
}

/// A key action, independent of a keyboard layout.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Key {
    Enter,
    Escape,
    Tab,
    Backspace,
    ArrowUp,
    ArrowDown,
    ArrowLeft,
    ArrowRight,
}

/// An exact accessible role and name, optionally scoped beneath a node.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Query {
    pub role: Box<[u8]>,
    pub name: Box<[u8]>,
    pub within: Option<Node>,
    pub boxes: bool,
}

/// What an expectation asks of the query's result.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Expect {
    Present,
    Count(u32),
    Absent,
}

/// An accessible node that matched a query.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Seen {
    pub node: Node,
    pub role: Box<[u8]>,
    pub name: Box<[u8]>,
    pub value: Box<[u8]>,
    pub states: States,
    pub rect: Option<Rect>,
}

/// The states exposed in an accessibility node.
#[expect(clippy::struct_excessive_bools, reason = "each state is an independent accessibility property")]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct States {
    pub focused: bool,
    pub disabled: bool,
    pub checked: bool,
    pub expanded: bool,
    pub selected: bool,
}

/// A box in whole CSS pixels.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Rect {
    pub x: i64,
    pub y: i64,
    pub width: u64,
    pub height: u64,
}

/// Why an action could not be performed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Refusal {
    Gone,
    Hidden,
    Covered,
    Disabled,
    Crashed,
    Limit,
    Timeout,
    Protocol,
}

/// An error observed in the page.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Trouble {
    Exception,
    Console,
    Log,
    Crash,
}
