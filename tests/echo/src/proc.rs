//! The processes of the echo's worlds: the echo, as its shell runs it, and
//! the fake clients, as theirs would; one type, so that a world hosts both.

use skein_echo_client::{self as client, Client, Plan};
use skein_echo_service::{self as service, Service};
use skein_io::kernel::{Addr, Complete, Submit};
use skein_lib::{Queue, Time, Wall};
use skein_world::Host;

/// A process of an echo world.
#[derive(Debug)]
pub enum Proc {
    Echo { svc: Box<Service>, limits: service::Limits },
    Client { client: Box<Client>, limits: client::Limits },
}

impl Proc {
    /// The echo under `limits`, to listen at `addr`.
    #[must_use]
    pub fn echo(limits: service::Limits, addr: Addr, seed: u64) -> Proc {
        Proc::Echo { svc: Box::new(Service::new(&limits, addr, seed)), limits }
    }

    /// A fake client under `limits`, its connections following `plans`.
    #[must_use]
    pub fn client(limits: client::Limits, plans: &[Plan]) -> Proc {
        Proc::Client { client: Box::new(Client::new(&limits, plans)), limits }
    }

    /// The echo, if this is it.
    #[must_use]
    pub fn as_echo(&self) -> Option<&Service> {
        match self {
            Proc::Echo { svc, .. } => Some(svc),
            Proc::Client { .. } => None,
        }
    }

    /// The fake client, if this is one.
    #[must_use]
    pub fn as_client(&self) -> Option<&Client> {
        match self {
            Proc::Client { client, .. } => Some(client),
            Proc::Echo { .. } => None,
        }
    }
}

impl Host for Proc {
    fn iterate(&mut self, now: Time, wall: Wall) {
        match self {
            Proc::Echo { svc, .. } => service::iterate(svc, now, wall),
            Proc::Client { client, .. } => client::iterate(client, now, wall),
        }
    }

    fn completions(&mut self) -> &mut Queue<Complete> {
        match self {
            Proc::Echo { svc, .. } => svc.completions(),
            Proc::Client { client, .. } => client.completions(),
        }
    }

    fn submissions(&mut self) -> &mut Queue<Submit> {
        match self {
            Proc::Echo { svc, .. } => svc.submissions(),
            Proc::Client { client, .. } => client.submissions(),
        }
    }

    fn work_pending(&self, now: Time) -> bool {
        match self {
            Proc::Echo { svc, .. } => svc.work_pending(now),
            Proc::Client { client, .. } => client.work_pending(now),
        }
    }

    fn next_deadline(&self) -> Option<Time> {
        match self {
            Proc::Echo { svc, .. } => svc.next_deadline(),
            Proc::Client { client, .. } => client.next_deadline(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Proc::Echo { svc, .. } => svc.is_empty(),
            Proc::Client { client, .. } => client.is_empty(),
        }
    }

    /// Each one's worst case, and the box this process keeps its state in,
    /// which the harness's choice and not the process's.
    fn worst_case(&self) -> u64 {
        let (worst, boxed) = match self {
            Proc::Echo { limits, .. } => (service::worst_case(limits), size_of::<Service>()),
            Proc::Client { client, limits } => (client::worst_case(limits, client.conns()), size_of::<Client>()),
        };
        let boxed = u64::try_from(boxed).expect("a struct's size fits a u64");
        worst.and_then(|worst| worst.checked_add(boxed)).expect("a process's limits are priced")
    }

    fn operations(&self) -> u32 {
        match self {
            Proc::Echo { limits, .. } => service::operations(limits).expect("a small ring"),
            Proc::Client { limits, .. } => client::operations(limits).expect("a small ring"),
        }
    }
}
