//! skein-tls's step tests (testing-strategy.md, 2.1): its limits, its
//! configuration, the bytes it holds for rustls, and the machine fed by
//! hand, one event at a time, with records written by hand in place of a
//! server's. The handshakes and exchanges with a server are its machine
//! worlds, in tests/tls.

#![expect(clippy::disallowed_types, reason = "a test collects what a step emitted in a Vec")]

mod config;
mod held;
mod machine;

use alloc::boxed::Box;
use alloc::vec::Vec;

use rustls::RootCertStore;
use rustls::pki_types::{Der, TrustAnchor};
use skein_lib::stream::{Down, Up};
use skein_lib::{Env, Queue, Time, Wall};

use crate::client::{self, Client, Event, Limits, Request};
use crate::{Config, Name};

/// Original fixed receiving limits, shared with native controls (tls.md, 3.6).
pub(crate) const LIMITS: Limits = Limits { read: 16, send: 16, records: client::MAX_RECORD };

/// Roots of one anchor, which nothing a test sends is signed by: a
/// configuration needs a root, and these handshakes never reach one.
fn roots() -> RootCertStore {
    let mut roots = RootCertStore::empty();
    roots.roots.push(TrustAnchor {
        subject: Der::from_slice(b"a test root"),
        subject_public_key_info: Der::from_slice(b"its key"),
        name_constraints: None,
    });
    roots
}

/// Original synthetic-root configuration, also used by native step controls.
pub(crate) fn config() -> Config {
    Config::new(roots(), &[]).unwrap()
}

/// A client and its two queues, with room for what one call emits.
struct Machine {
    client: Client,
    env: Env<Limits>,
    above: Queue<Event>,
    below: Queue<Down>,
}

impl Machine {
    fn new(limits: Limits) -> Machine {
        Machine {
            client: Client::new(&config(), Name::new("skein.test").unwrap(), &limits),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            above: Queue::with_capacity(client::UP_MAX_OUT.above.max(client::DOWN_MAX_OUT.above)),
            below: Queue::with_capacity(client::UP_MAX_OUT.below.max(client::DOWN_MAX_OUT.below)),
        }
    }

    /// Sends `rq` down; what came of it, above and below.
    fn down(&mut self, rq: Request) -> (Vec<Event>, Vec<Down>) {
        client::down(&mut self.client, &self.env, rq, &mut self.above, &mut self.below);
        assert!(self.above.len() <= client::DOWN_MAX_OUT.above, "MAX_OUT above");
        assert!(self.below.len() <= client::DOWN_MAX_OUT.below, "MAX_OUT below");
        self.take()
    }

    /// Sends `ev` up; what came of it, above and below.
    fn up(&mut self, ev: Up) -> (Vec<Event>, Vec<Down>) {
        client::up(&mut self.client, &self.env, ev, &mut self.above, &mut self.below);
        assert!(self.above.len() <= client::UP_MAX_OUT.above, "MAX_OUT above");
        assert!(self.below.len() <= client::UP_MAX_OUT.below, "MAX_OUT below");
        self.take()
    }

    fn bytes(&mut self, bytes: &[u8]) -> (Vec<Event>, Vec<Down>) {
        self.up(Up::Bytes(Box::from(bytes)))
    }

    fn take(&mut self) -> (Vec<Event>, Vec<Down>) {
        let mut events = Vec::new();
        while let Some(event) = self.above.pop() {
            events.push(event);
        }
        let mut requests = Vec::new();
        while let Some(request) = self.below.pop() {
            requests.push(request);
        }
        (events, requests)
    }
}
