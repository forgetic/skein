//! The shipped echo adapter (examples.md, sections 4 and 6; shell.md, 12).
//! It keeps the service, limits and which diagnostics were said, and knows
//! no peer state beyond the service's public observations.
use skein_echo_service::{self as service, Limits, Service};
use skein_io::kernel::{Addr, Complete, Exit, Submit};
use skein_lib::{Duration, Queue, Time, Wall};
use skein_shell::Host;

/// The echo's shell adapter, driven by main and hosted by its worlds.
#[derive(Debug)]
pub struct Echo {
    pub svc: Service,
    limits: Limits,
    told: bool,
    short: bool,
}

impl Echo {
    #[must_use]
    pub fn new(limits: Limits, addr: Addr, seed: u64) -> Echo {
        Echo { svc: Service::new(&limits, addr, seed), limits, told: false, short: false }
    }
}

impl Host for Echo {
    fn iterate(&mut self, now: Time, wall: Wall) {
        service::iterate(&mut self.svc, now, wall);
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        self.svc.completions()
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        self.svc.submissions()
    }
    fn work_pending(&self, now: Time) -> bool {
        self.svc.work_pending(now)
    }
    fn next_deadline(&self) -> Option<Time> {
        self.svc.next_deadline()
    }

    fn next_policy_deadline(&self) -> Option<Time> {
        self.svc.next_policy_deadline()
    }

    fn is_empty(&self) -> bool {
        self.svc.is_empty()
    }
    fn exit(&self) -> Option<Exit> {
        if !self.is_empty() {
            return None;
        }
        Some(Exit::Code(u8::from(self.svc.failure().is_some())))
    }
    fn worst_case(&self) -> u64 {
        service::worst_case(&self.limits).expect("startup priced the limits")
    }
    fn operations(&self) -> u32 {
        service::operations(&self.limits).expect("startup priced the ring")
    }
    fn drain(&mut self) {
        if !self.told
            && let Some(addr) = self.svc.listening()
        {
            eprintln!("skein-echo: listening at {addr}");
            self.told = true;
        }
        if !self.short
            && let Some(error) = self.svc.retrying()
        {
            eprintln!(
                "skein-echo: the listen was refused for want of resources ({error:?}); trying again until it is not"
            );
        }
        self.short = self.svc.retrying().is_some() || (self.short && !self.told);
    }
}

/// The limits of every layer: a thousand connections of lines up to 4 KiB,
/// with fewer sessions than connections and fewer connections than
/// sockets, so that each layer refuses at its own entrance first
/// (programming-model.md, 7). io accepts only while a socket slot is free,
/// so past the listener and the protocol layer's connections it keeps an
/// accept batch of slots more: the sockets the protocol layer rejects, each
/// holding its slot while io closes it.
#[must_use]
pub const fn limits() -> Limits {
    Limits {
        io: skein_io::Limits {
            sockets: 1024 + 1 + 64 + 1,
            refusals: 1,
            intake: 4096,
            receive: 4096,
            output: 8192,
            sends: 8,
            accepts: 64,
            backlog: 1024,
            close_timeout: Duration::from_secs(5),
            retry: Duration::from_millis(50),
        },
        protocol: service::protocol::Limits {
            conns: 1024,
            line: 4096,
            idle: Duration::from_secs(60),
            spread: Duration::from_secs(6),
            retry: Duration::from_millis(100),
        },
        domain: service::domain::Limits { sessions: 1000 },
        queue: 256,
    }
}
