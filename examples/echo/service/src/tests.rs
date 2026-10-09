//! The service's step tests (testing-strategy.md, 2.1): `iterate` over a
//! kernel played by hand, with queues of the smallest size its `MAX_OUT`s
//! allow, so that a stage that reserved too little fails the test; the
//! startup checks of the limits; and the sum of the worst cases. The
//! service's worlds, over the simulator and the real ring, are tests/echo.

#![expect(clippy::disallowed_types, reason = "the hand-played kernel keeps what it holds in Vecs")]

use alloc::vec::Vec;
use core::net::{Ipv4Addr, SocketAddr};

use crate::{domain, protocol};
use skein_io::kernel::{Addr, Complete, Done, Error, Fd, Op, Submit};
use skein_lib::{Duration, Time, Wall};

use crate::{Limits, Service, Unusable, iterate, operations, worst_case};

const fn limits() -> Limits {
    Limits {
        io: skein_io::Limits {
            sockets: 4,
            refusals: 1,
            intake: 24,
            receive: 8,
            output: 32,
            sends: 2,
            accepts: 1,
            backlog: 2,
            close_timeout: Duration::from_secs(1),
            retry: Duration::from_millis(10),
        },
        protocol: protocol::Limits {
            conns: 2,
            line: 16,
            idle: Duration::from_secs(2),
            spread: Duration::ZERO,
            retry: Duration::from_millis(10),
        },
        domain: domain::Limits { sessions: 1 },
        queue: Limits::LEAST_QUEUE,
    }
}

fn local(port: u16) -> Addr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

/// The service, over a kernel played by hand: what can complete at once
/// does, in the next turn; accepts and receives wait for the test.
struct Rig {
    svc: Service,
    now: Time,
    /// Completions made, not yet reaped.
    done: Vec<Complete>,
    /// Accepts and receives in flight, waiting for the test.
    held: Vec<Submit>,
    /// What was sent, by descriptor, in order.
    sent: Vec<(Fd, u8)>,
    /// Descriptors closed.
    closed: Vec<Fd>,
    next_fd: i32,
}

impl Rig {
    fn new() -> Rig {
        let limits = limits();
        Rig {
            svc: Service::new(&limits, local(0), 7),
            now: Time::ZERO,
            done: Vec::new(),
            held: Vec::new(),
            sent: Vec::new(),
            closed: Vec::new(),
            next_fd: 3,
        }
    }

    /// One turn of the loop: reap what fits, iterate, then answer what was
    /// submitted.
    fn turn(&mut self) {
        while self.svc.completions().room() > 0 && !self.done.is_empty() {
            let complete = self.done.remove(0);
            self.svc.completions().push(complete);
        }
        iterate(&mut self.svc, self.now, Wall::EPOCH);
        while let Some(submit) = self.svc.submissions().pop() {
            self.answer(submit);
        }
    }

    /// Turns until the loop has nothing left to do without the test.
    fn settle(&mut self) {
        for _ in 0..100_u32 {
            self.turn();
            if self.done.is_empty() && !self.svc.work_pending(self.now) {
                return;
            }
        }
        panic!("the service settles");
    }

    fn fd(&mut self) -> Fd {
        let fd = Fd::new(self.next_fd);
        self.next_fd = self.next_fd.checked_add(1).expect("few descriptors");
        fd
    }

    fn answer(&mut self, submit: Submit) {
        let Submit { op, kind } = submit;
        let result = match &kind {
            Op::Socket { .. } => Ok(Done::Fd(self.fd())),
            Op::Bind { .. } => Ok(Done::Bound(local(4000))),
            Op::Listen { .. } | Op::Shutdown { .. } => Ok(Done::Nothing),
            Op::Close { fd } => {
                self.closed.push(*fd);
                Ok(Done::Nothing)
            }
            Op::Send { fd, bytes, from } => {
                let from = usize::try_from(*from).expect("small");
                for byte in &bytes[from..] {
                    self.sent.push((*fd, *byte));
                }
                let left = bytes.len().checked_sub(from).expect("a send from within its bytes");
                Ok(Done::Count(u32::try_from(left).expect("small")))
            }
            Op::Cancel { target } => {
                let mut found = None;
                for (at, held) in self.held.iter().enumerate() {
                    if held.op == *target {
                        found = Some(at);
                    }
                }
                if let Some(at) = found {
                    let held = self.held.remove(at);
                    self.done.push(Complete { op: held.op, kind: held.kind, result: Err(Error::Cancelled) });
                    Ok(Done::Nothing)
                } else {
                    Err(Error::TooLate)
                }
            }
            Op::Accept { .. } | Op::Recv { .. } => {
                self.held.push(Submit { op, kind });
                return;
            }
            Op::Connect { .. } => panic!("the echo connects to no one"),
            Op::Open { .. }
            | Op::Read { .. }
            | Op::Write { .. }
            | Op::Append { .. }
            | Op::Sync { .. }
            | Op::Stat { .. }
            | Op::Rename { .. }
            | Op::Remove { .. }
            | Op::MakeDirectory { .. }
            | Op::List { .. }
            | Op::Spawn { .. }
            | Op::Wait { .. }
            | Op::Signal { .. }
            | Op::Usage
            | Op::ReadSignal { .. }
            | Op::PipeRead { .. }
            | Op::PipeWrite { .. } => panic!("the echo uses only socket operations"),
        };
        self.done.push(Complete { op, kind, result });
    }

    /// A peer connects: the held accept completes.
    fn connect(&mut self) -> Fd {
        let mut found = None;
        for (at, held) in self.held.iter().enumerate() {
            if let Op::Accept { .. } = held.kind {
                found = Some(at);
            }
        }
        let held = self.held.remove(found.expect("an accept armed"));
        let fd = self.fd();
        self.done.push(Complete { op: held.op, kind: held.kind, result: Ok(Done::Accepted { fd, peer: local(5000) }) });
        fd
    }

    /// The peer on `fd` sends `bytes`, which fit a receive: the held receive
    /// completes; or it ends its stream, with none.
    fn deliver(&mut self, fd: Fd, bytes: &[u8]) {
        let mut found = None;
        for (at, held) in self.held.iter().enumerate() {
            if let Op::Recv { fd: on, .. } = held.kind
                && on == fd
            {
                found = Some(at);
            }
        }
        let Submit { op, kind } = self.held.remove(found.expect("a receive in flight"));
        let Op::Recv { fd, mut buf } = kind else { unreachable!("a receive") };
        for (into, byte) in buf.iter_mut().zip(bytes) {
            *into = *byte;
        }
        let n = u32::try_from(bytes.len()).expect("small");
        self.done.push(Complete { op, kind: Op::Recv { fd, buf }, result: Ok(Done::Count(n)) });
    }

    fn sent_on(&self, fd: Fd) -> Vec<u8> {
        let mut bytes = Vec::new();
        for (on, byte) in &self.sent {
            if *on == fd {
                bytes.push(*byte);
            }
        }
        bytes
    }
}

#[test]
fn the_service_listens_and_echoes_a_line_through_every_stage() {
    let mut rig = Rig::new();
    rig.settle();
    assert_eq!(rig.svc.listening(), Some(local(4000)), "listening, at the port the kernel chose");
    let peer = rig.connect();
    rig.settle();
    rig.deliver(peer, b"hel");
    rig.settle();
    rig.deliver(peer, b"lo\n");
    rig.settle();
    assert_eq!(rig.sent_on(peer), b"hello\n", "the line comes back, though it came in two pieces");
}

#[test]
fn a_peer_past_the_sessions_is_told_busy_and_closed() {
    let mut rig = Rig::new();
    rig.settle();
    let first = rig.connect();
    rig.settle();
    let second = rig.connect();
    rig.settle();
    assert_eq!(rig.sent_on(second), b"busy\n", "one session: the second is refused at the domain's entrance");
    assert!(rig.sent_on(first).is_empty(), "the first is served, and has said nothing");
    // The close is graceful: io drains until the peer ends.
    rig.deliver(second, b"");
    rig.settle();
    assert!(rig.closed.contains(&second));
    assert!(!rig.closed.contains(&first));
}

#[test]
fn an_idle_connection_is_closed_at_its_deadline() {
    let mut rig = Rig::new();
    rig.settle();
    let peer = rig.connect();
    rig.settle();
    let deadline = rig.svc.next_deadline().expect("the idle deadline runs while reading");
    assert_eq!(deadline, Time::ZERO.saturating_add(limits().protocol.idle));
    assert!(!rig.svc.work_pending(rig.now), "nothing to do before it");
    rig.now = deadline;
    assert!(rig.svc.work_pending(rig.now), "a deadline is due");
    rig.settle();
    rig.deliver(peer, b"");
    rig.settle();
    assert!(rig.closed.contains(&peer), "closed, once the peer ended its side");
}

#[test]
fn shutdown_stops_the_listener_and_the_service_empties_once_its_peers_leave() {
    let mut rig = Rig::new();
    rig.settle();
    let peer = rig.connect();
    rig.settle();
    rig.svc.shutdown();
    assert!(rig.svc.work_pending(rig.now), "the shutdown waits on the ready list");
    rig.settle();
    assert_eq!(rig.svc.listening(), None, "the listener stopped");
    assert!(!rig.svc.is_empty(), "a peer is still served");
    rig.deliver(peer, b"bye\n");
    rig.settle();
    assert_eq!(rig.sent_on(peer), b"bye\n", "served after the shutdown");
    // The peer ends: the server closes, and io's drain has nothing more to
    // read.
    rig.deliver(peer, b"");
    rig.settle();
    assert!(rig.svc.is_empty(), "nothing left: {:?}", rig.svc);
    assert_eq!(rig.svc.failure(), None, "stopped, not failed");
}

#[test]
fn limits_are_checked_layer_by_layer_and_against_each_other() {
    let good = limits();
    assert_eq!(good.check(), Ok(()));
    assert_eq!(Limits::LEAST_QUEUE, 3, "IO can emit Bytes, End and an independent terminal together");
    assert_eq!(Limits { queue: 3, ..good }.check(), Ok(()), "the actual minimum admits every stage");
    let io = skein_io::Limits { output: 0, ..good.io };
    assert_eq!(Limits { io, ..good }.check(), Err(Unusable::Io));
    let protocol = protocol::Limits { conns: 0, ..good.protocol };
    assert_eq!(Limits { protocol, ..good }.check(), Err(Unusable::Protocol));
    assert_eq!(Limits { queue: 1, ..good }.check(), Err(Unusable::Queue));
    assert_eq!(
        Limits { queue: 2, ..good }.check(),
        Err(Unusable::Queue),
        "the former minimum cannot reserve IO's actual three-event bound"
    );
    let protocol = protocol::Limits { line: 25, ..good.protocol };
    assert_eq!(Limits { protocol, ..good }.check(), Err(Unusable::Read { largest: 25, intake: 24 }));
    let io = skein_io::Limits { intake: 64, output: 20, ..good.io };
    let protocol = protocol::Limits { line: 24, ..good.protocol };
    assert_eq!(Limits { io, protocol, ..good }.check(), Err(Unusable::Room { largest: 24, output: 20 }));
}

#[test]
fn the_worst_case_is_the_sum_of_the_layers_and_the_queues() {
    let limits = limits();
    let total = worst_case(&limits).expect("priced");
    let layers = skein_io::worst_case(&limits.io).expect("priced")
        + protocol::worst_case(&limits.protocol).expect("priced")
        + domain::worst_case(&limits.domain).expect("priced");
    assert!(total > layers, "the queues cost something besides");
    let wider = worst_case(&Limits { queue: 8, ..limits }).expect("priced");
    assert!(wider > total, "and grow with their capacity");
    let huge = skein_io::Limits { sockets: u32::MAX, ..limits.io };
    assert_eq!(worst_case(&Limits { io: huge, ..limits }), None, "past a u64");
    assert_eq!(operations(&limits), Some(16), "four operations a socket");
}
