//! The fake provider's state and its entry points.
//!
//! Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, sections 4.4 and 6.3.

use alloc::boxed::Box;
use core::mem;

use skein_lib::{Deadlines, Duration, Env, Id, Queue, ReplyTo, Rng, Slab, Time};

use crate::api::{Answer, Error, Query, Script};
use crate::{cache, limits, respond};

/// The most requests an entry point emits per call.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
pub const MAX_OUT: u32 = 1;

/// How the fake behaves, handed to every step read-only.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Config {
    /// Calls held at once. A call beyond them is refused as overloaded.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub calls: u32,
    /// Owned query bytes, including message and part arrays.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub query_bytes: u32,
    /// Joint owned script and caller-menu bytes, including their arrays, held at startup.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub script_bytes: u32,
    /// Owned bytes in each answer held until its timer fires.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub answer_bytes: u32,
    /// The time to answer is drawn from `latency_min..=latency_max`.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub latency_min: Duration,
    /// Inclusive upper bound for the fake's seeded response latency.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub latency_max: Duration,
    /// The chance, per mille, that a call fails as overloaded.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub overloaded: u32,
    /// The chance, per mille, that a call fails as rate-limited.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub rate_limited: u32,
    /// What a rate-limit failure asks the client to wait.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub retry_after: Duration,
    /// The chances, per mille, that a call fails as unavailable, as too long
    /// for the context window, or as unauthorised.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub unavailable: u32,
    /// Chance per mille that the fake refuses the context window.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub too_long: u32,
    /// Chance per mille that the fake refuses credentials.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub unauthorized: u32,
    /// The chance, per mille, that an answer is refused by the content filter.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub refused: u32,
    /// The chance, per mille, that an answer says it calls tools and calls
    /// none.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub no_calls: u32,
    /// The most tokens a final answer takes: each takes between one and this
    /// many, and is cut short at the query's `max_tokens`.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub answer_tokens: u32,
    /// The most tool calls an answer makes: each that makes some makes
    /// between one and this many.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub calls_per_answer: u32,
    /// The chance, per mille, that a tool call uses an entry from the caller's
    /// configured invalid input/name menu. An empty invalid menu leaves the
    /// offered name and configured ordinary body unchanged.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub malformed: u32,
    /// Chance per mille of choosing an offered tool outside the query's choice.
    pub outside_choice: u32,
    /// Rounds of tool calls after each of the client's messages before the
    /// fake answers it.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub tool_rounds: u32,
    /// Lifetime from a prefix's write at injected monotonic time.
    pub cache_lifetime: Duration,
    /// Maximum written prefixes retained in the cache table.
    pub cache_entries: u32,
    /// Chance per mille that a request with no scope reads existing prefixes.
    pub unscoped_reads: u32,
}

/// protocol -> domain
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(PartialEq, Eq, Debug)]
pub enum Event {
    /// A call: answer `query`.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Call {
        /// Single-use right to answer this call; exactly one terminal consumes it.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        reply_to: ReplyTo,
        /// Owned bounded neutral provider request, independent of agent vocabulary.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        query: Query,
    },
}

/// domain -> protocol
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(PartialEq, Eq, Debug)]
pub enum Request {
    /// The answer to a `Call`: exactly one per call.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Reply {
        /// Single-use reply right returned to the layer that issued it.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        to: ReplyTo,
        /// The one terminal value for the enclosing call; ownership passes to its receiver.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        result: Result<Answer, Error>,
    },
}

/// The fake provider's state.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Debug)]
pub struct Domain {
    calls: Slab<Call>,
    /// When each call is answered.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    timers: Deadlines<Id<Call>>,
    rng: Rng,
    /// Tool call ids issued.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    minted: u64,
    /// The conversations it plays from a script.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    scripts: Box<[Script]>,
    menu: crate::api::Menu,
    cache: cache::Cache,
}

/// A call being answered.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Debug)]
pub(crate) struct Call {
    state: State,
}

#[derive(Debug)]
enum State {
    /// The answer is decided, and goes out when the call's timer fires.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Thinking { reply_to: ReplyTo, result: Result<Answer, Error> },
    /// Terminal: holds nothing.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Closed,
}

impl Domain {
    /// Constructs empty bounded state under the supplied immutable limits; no IO or clocks are consulted.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    #[must_use]
    pub fn new(config: &Config, seed: u64) -> Domain {
        Domain::scripted(config, seed, Box::new([]))
    }

    /// A provider that plays the conversations `scripts` cue from them, and
    /// the others at random.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    #[must_use]
    pub fn scripted(config: &Config, seed: u64, scripts: Box<[Script]>) -> Domain {
        Domain::try_scripted(config, seed, scripts).expect("scripts fit the provider limits")
    }

    /// Refuses a script collection exceeding the configured owned-byte cap.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub fn try_scripted(config: &Config, seed: u64, scripts: Box<[Script]>) -> Result<Domain, Error> {
        Self::configured(config, seed, scripts, crate::api::Menu { arguments: Box::new([]), invalid: Box::new([]) })
    }

    /// Admits caller-owned scripts and random input menus under one aggregate byte cap.
    /// No tool path, command, body or invalid name is supplied by this generic fake.
    pub fn configured(
        config: &Config,
        seed: u64,
        scripts: Box<[Script]>,
        menu: crate::api::Menu,
    ) -> Result<Domain, Error> {
        let held = match limits::scripts(&scripts) {
            Some(scripts) => match limits::menu(&menu) {
                Some(menu) => scripts.checked_add(menu),
                None => None,
            },
            None => None,
        };
        if !limits::fits(held, config.script_bytes) {
            return Err(Error::ContextTooLong);
        }
        Ok(Domain {
            calls: Slab::with_capacity(config.calls),
            timers: Deadlines::with_capacity(config.calls),
            rng: Rng::new(seed),
            minted: 0,
            scripts,
            menu,
            cache: cache::Cache::new(config.cache_entries, config.cache_lifetime),
        })
    }

    /// Calls present, answered ones included until they are reclaimed.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    #[must_use]
    pub fn calls(&self) -> u32 {
        self.calls.len()
    }

    /// Earliest injected deadline still pending, or `None` when no timed work remains.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        self.timers.next()
    }

    /// Whether injected now reaches the earliest pending deadline; no live clock is read.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    #[must_use]
    pub fn is_due(&self, now: Time) -> bool {
        match self.timers.next() {
            Some(at) => at <= now,
            None => false,
        }
    }

    /// Iteration-end reclamation of retired entries; it must follow delivery of owned outputs.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub fn reclaim(&mut self) {
        self.calls.reclaim();
    }
}

/// Handles one event, emitting at most [`MAX_OUT`] requests.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
pub fn step(domain: &mut Domain, env: &Env<Config>, event: Event, out: &mut Queue<Request>) {
    match event {
        Event::Call { reply_to, query } => call(domain, env, reply_to, &query, out),
    }
}

/// Answers the earliest call due at `env.now`, if there is one.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
pub fn fire(domain: &mut Domain, env: &Env<Config>, out: &mut Queue<Request>) {
    let Some(id) = domain.timers.expire(env.now) else {
        return;
    };
    let call = domain.calls.get_mut(id).expect("a call lives until its timer fires");
    let state = mem::replace(&mut call.state, State::Closed);
    call.state = match state {
        State::Thinking { reply_to, result } => answer(reply_to, result, out),
        State::Closed => unreachable!("a closed call has no timer"),
    };
    domain.calls.retire(id);
}

fn call(domain: &mut Domain, env: &Env<Config>, reply_to: ReplyTo, query: &Query, out: &mut Queue<Request>) {
    if domain.calls.is_full() {
        out.push(Request::Reply { to: reply_to, result: Err(Error::Overloaded) });
        return;
    }
    let config = &env.limits;
    let mut result = if !cache::valid(query) {
        Err(Error::InvalidRequest)
    } else if limits::fits(limits::query(query), config.query_bytes) {
        match respond::respond(&mut domain.rng, &mut domain.minted, config, &domain.scripts, &domain.menu, query) {
            Ok(answer) if limits::fits(limits::answer(&answer), config.answer_bytes) => Ok(answer),
            Ok(_) => Err(Error::ContextTooLong),
            Err(error) => Err(error),
        }
    } else {
        Err(Error::ContextTooLong)
    };
    if let Ok(answer) = &mut result {
        let allowed = match query.caching {
            crate::api::Caching::Unscoped => domain.rng.chance(config.unscoped_reads),
            crate::api::Caching::Scope(_) | crate::api::Caching::Marks(_) => true,
        };
        let read = if allowed { cache::read(&domain.cache, query, env.now) } else { 0 };
        let written = cache::write(&mut domain.cache, query, env.now, read);
        answer.usage.input = Some(
            cache::tokens(query)
                .checked_sub(read)
                .expect("read within prompt")
                .checked_sub(written)
                .expect("write within uncached prompt"),
        );
        answer.usage.cache_read = Some(read);
        answer.usage.cache_write = Some(written);
    }
    let latency = domain.rng.between(config.latency_min.as_nanos(), config.latency_max.as_nanos());
    let call = Call { state: State::Thinking { reply_to, result } };
    let id = domain.calls.insert(call).expect("checked for room above");
    let at = env.now.saturating_add(Duration::from_nanos(latency));
    domain.timers.arm(id, at).expect("one timer per call fits");
}

/// Thinking, timer: the answer goes out.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
fn answer(reply_to: ReplyTo, result: Result<Answer, Error>, out: &mut Queue<Request>) -> State {
    out.push(Request::Reply { to: reply_to, result });
    State::Closed
}
