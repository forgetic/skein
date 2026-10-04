//! The echo example's domain (examples.md, 3): who is served, and what each
//! line is answered with. It knows nothing of connections or bytes on a
//! wire (programming-model.md, 4.4): a peer arrives as a call to `Open`,
//! each of its lines as a call carrying the line's text, and both are
//! answered through their `ReplyTo`.
//!
//! - **It refuses at the entrance:** an `Open` past the slab of sessions, or
//!   after `Shutdown`, is answered `Busy`, and nothing is made.
//! - **A session lasts from its admission to `Gone`,** which the protocol
//!   layer tells once no call of the session is out, so that the domain never
//!   answers for a session it has retired.
//! - **`Shutdown` is the domain's to interpret:** it admits no one more, and
//!   asks once that no one more be let in (`Stop`). The sessions it has run
//!   to their end.
//!
//! [`step`] takes one event and emits at most [`MAX_OUT`] requests.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

#[cfg(test)]
mod tests;

use alloc::boxed::Box;

use skein_lib::{Env, Id, Queue, ReplyTo, Slab, Token};

/// The domain's limits.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// Sessions at once: an `Open` past it is answered `Busy`.
    pub sessions: u32,
}

/// What the protocol layer tells the domain: calls, which the domain
/// answers with exactly one [`Reply`], and events, which it does not.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Event {
    /// A peer asks to be served: answered `Admitted`, with its session, or
    /// `Busy`.
    Open { reply_to: ReplyTo },
    /// A line from the peer of `session`: its text, without the end of line,
    /// answered `Echo`.
    Line { session: Token, reply_to: ReplyTo, text: Box<[u8]> },
    /// The peer of `session` is gone, and no call of its is out: the session
    /// ends.
    Gone { session: Token },
    /// The service is shutting down. It stands in for io's `Shutdown` event,
    /// which is not built (io.md, 7).
    Shutdown,
}

/// What the domain asks of the protocol layer.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Request {
    /// The answer to a call, which consumes its `ReplyTo`.
    Reply { to: ReplyTo, reply: Reply },
    /// Let no one more in: the listener stops.
    Stop,
}

/// The answers to calls.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Reply {
    /// To `Open`: served, as `session`.
    Admitted { session: Token },
    /// To `Open`: not served. Nothing was made.
    Busy,
    /// To `Line`: the text to send back.
    Echo(Box<[u8]>),
}

/// The domain's state: the sessions, and whether it admits more.
#[derive(Debug)]
pub struct Domain {
    sessions: Slab<Session>,
    admission: Admission,
}

/// A peer being served.
#[derive(Debug)]
struct Session {
    /// The lines it was answered.
    lines: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Admission {
    Open,
    /// `Shutdown` came: every `Open` is answered `Busy`.
    Stopped,
}

/// The most requests one call of [`step`] emits.
pub const MAX_OUT: u32 = 1;

/// The most heap the domain holds under `limits`, or `None` past a `u64`
/// (programming-model.md, 6.3): its slab of sessions. The texts it answers
/// with are moved through it, and counted by the protocol layer, whose line
/// in flight they are.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    Slab::<Session>::worst_case(limits.sessions)
}

impl Domain {
    #[must_use]
    pub fn new(limits: &Limits) -> Domain {
        Domain { sessions: Slab::with_capacity(limits.sessions), admission: Admission::Open }
    }

    /// Sessions present, those that ended in this iteration included until
    /// the reclaim point.
    #[must_use]
    pub const fn sessions(&self) -> u32 {
        self.sessions.len()
    }

    /// Whether `Shutdown` came.
    #[must_use]
    pub const fn is_stopped(&self) -> bool {
        match self.admission {
            Admission::Open => false,
            Admission::Stopped => true,
        }
    }

    /// The lines `session` was answered, while it lasts.
    #[must_use]
    pub fn lines(&self, session: Token) -> Option<u64> {
        let session = self.sessions.get(Id::<Session>::from_token(session))?;
        Some(session.lines)
    }

    /// Whether the domain holds no session.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// The reclaim point: frees the sessions that ended.
    pub fn reclaim(&mut self) {
        self.sessions.reclaim();
    }
}

/// Takes one event, emitting at most [`MAX_OUT`] requests. The domain reads
/// nothing from its environment yet; the argument keeps the shape every step
/// has (programming-model.md, section 3).
pub fn step(domain: &mut Domain, _env: &Env<Limits>, event: Event, out: &mut Queue<Request>) {
    match event {
        Event::Open { reply_to } => open(domain, reply_to, out),
        Event::Line { session, reply_to, text } => line(domain, session, reply_to, text, out),
        Event::Gone { session } => gone(domain, session),
        Event::Shutdown => shutdown(domain, out),
    }
}

/// Admitted while the domain admits and a session is free; `Busy` otherwise.
fn open(domain: &mut Domain, reply_to: ReplyTo, out: &mut Queue<Request>) {
    let reply = match domain.admission {
        Admission::Open => match domain.sessions.insert(Session { lines: 0 }) {
            Ok(session) => Reply::Admitted { session: session.token() },
            Err(_refused) => Reply::Busy,
        },
        Admission::Stopped => Reply::Busy,
    };
    out.push(Request::Reply { to: reply_to, reply });
}

/// The text goes back as it came, and the line is counted.
fn line(domain: &mut Domain, session: Token, reply_to: ReplyTo, text: Box<[u8]>, out: &mut Queue<Request>) {
    // A token travelling up is never stale (programming-model.md, 5.2): a
    // session is told gone only once no call of its is out.
    let session =
        domain.sessions.get_mut(Id::<Session>::from_token(session)).expect("a line names a session not yet gone");
    session.lines = session.lines.checked_add(1).expect("fewer than 2^64 lines");
    out.push(Request::Reply { to: reply_to, reply: Reply::Echo(text) });
}

fn gone(domain: &mut Domain, session: Token) {
    let id = Id::<Session>::from_token(session);
    assert!(domain.sessions.get(id).is_some(), "a session is told gone once, while it lasts");
    domain.sessions.retire(id);
}

/// The first `Shutdown` stops admission and asks for the listener to stop;
/// another changes nothing.
fn shutdown(domain: &mut Domain, out: &mut Queue<Request>) {
    match domain.admission {
        Admission::Open => {
            domain.admission = Admission::Stopped;
            out.push(Request::Stop);
        }
        Admission::Stopped => {}
    }
}
