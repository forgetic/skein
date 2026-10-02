//! A small harness over the simulator: one token counter per world, and calls
//! that submit one record and reap what came of it.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::net::{Ipv4Addr, SocketAddr};

use skein_io::kernel::{Addr, Complete, Done, Error, Family, Fd, Op, Submit};
use skein_lib::{Queue, Token};
use skein_sim::{Config, Pid, Sim};

/// Room for every completion a test can have outstanding.
const ROOM: u32 = 64;

pub struct World {
    pub sim: Sim,
    pub next: u64,
}

impl World {
    pub fn new(seed: u64, config: Config) -> World {
        World { sim: Sim::new(seed, config), next: 1 }
    }

    pub fn calm() -> World {
        World::new(1, Config::calm())
    }

    pub fn spawn(&mut self) -> Pid {
        self.sim.spawn_process()
    }

    /// A fresh token.
    pub fn token(&mut self) -> Token {
        let token = Token::new(self.next);
        self.next = self.next.checked_add(1).unwrap();
        token
    }

    /// Submits `op` under `token`.
    pub fn submit_as(&mut self, pid: Pid, token: Token, op: Op) {
        let mut queue = Queue::with_capacity(1);
        queue.push(Submit { op: token, kind: op });
        self.sim.submit(pid, &mut queue);
    }

    /// Submits `op` under a fresh token.
    pub fn submit(&mut self, pid: Pid, op: Op) -> Token {
        let token = self.token();
        self.submit_as(pid, token, op);
        token
    }

    /// Every completion delivered to `pid`.
    pub fn reap(&mut self, pid: Pid) -> Vec<Complete> {
        let mut queue = Queue::with_capacity(ROOM);
        self.sim.reap(pid, &mut queue);
        let mut out = Vec::new();
        while let Some(complete) = queue.pop() {
            assert!(complete.is_valid(), "the simulator keeps the contract");
            out.push(complete);
        }
        out
    }

    /// The completion of `token`, which must be the only one delivered.
    pub fn reap_one(&mut self, pid: Pid, token: Token) -> Complete {
        let mut got = self.reap(pid);
        assert_eq!(got.len(), 1, "one completion, of the operation just submitted");
        let complete = got.pop().unwrap();
        assert_eq!(complete.op, token, "the completion carries its own token");
        complete
    }

    /// Submits `op` and reaps its completion, which a calm world delivers at
    /// once when the operation need not wait.
    pub fn call(&mut self, pid: Pid, op: Op) -> Complete {
        let token = self.submit(pid, op);
        self.reap_one(pid, token)
    }

    pub fn socket(&mut self, pid: Pid) -> Fd {
        match self.call(pid, Op::Socket { family: Family::Ipv4 }).result {
            Ok(Done::Fd(fd)) => fd,
            other => panic!("a socket: {other:?}"),
        }
    }

    pub fn bind(&mut self, pid: Pid, fd: Fd, addr: Addr) -> Result<Addr, Error> {
        match self.call(pid, Op::Bind { fd, addr }).result {
            Ok(Done::Bound(bound)) => Ok(bound),
            Ok(other) => panic!("a bind answers with the address: {other:?}"),
            Err(error) => Err(error),
        }
    }

    pub fn listen(&mut self, pid: Pid, fd: Fd) -> Result<Done, Error> {
        self.call(pid, Op::Listen { fd, backlog: 16 }).result
    }

    /// A listener on `127.0.0.1`, on a port of the simulator's choice.
    pub fn listener(&mut self, pid: Pid) -> (Fd, Addr) {
        let fd = self.socket(pid);
        let addr = self.bind(pid, fd, local(0)).unwrap();
        assert_eq!(self.listen(pid, fd), Ok(Done::Nothing));
        (fd, addr)
    }

    pub fn connect(&mut self, pid: Pid, fd: Fd, addr: Addr) -> Result<Done, Error> {
        self.call(pid, Op::Connect { fd, addr }).result
    }

    pub fn accept(&mut self, pid: Pid, fd: Fd) -> (Fd, Addr) {
        match self.call(pid, Op::Accept { fd }).result {
            Ok(Done::Accepted { fd, peer }) => (fd, peer),
            other => panic!("an accept: {other:?}"),
        }
    }

    /// A connection from `client` to `server` in a calm world, the listener
    /// closed: the client's descriptor, then the server's.
    pub fn pair(&mut self, client: Pid, server: Pid) -> (Fd, Fd) {
        let (listener, addr) = self.listener(server);
        let fd = self.socket(client);
        assert_eq!(self.connect(client, fd, addr), Ok(Done::Nothing));
        let (accepted, _) = self.accept(server, listener);
        self.close(server, listener);
        (fd, accepted)
    }

    pub fn send(&mut self, pid: Pid, fd: Fd, bytes: &[u8]) -> Result<Done, Error> {
        self.call(pid, Op::Send { fd, bytes: Box::from(bytes), from: 0 }).result
    }

    /// A receive of up to `len` bytes that completes at once: the bytes, or
    /// the error.
    pub fn recv(&mut self, pid: Pid, fd: Fd, len: usize) -> Result<Vec<u8>, Error> {
        let complete = self.call(pid, Op::Recv { fd, buf: vec![0; len].into_boxed_slice() });
        received(complete)
    }

    pub fn shutdown(&mut self, pid: Pid, fd: Fd) -> Result<Done, Error> {
        self.call(pid, Op::Shutdown { fd }).result
    }

    pub fn close(&mut self, pid: Pid, fd: Fd) {
        assert_eq!(self.call(pid, Op::Close { fd }).result, Ok(Done::Nothing), "a close releases the descriptor");
    }

    /// The process has nothing in flight and no descriptor open.
    pub fn settled(&self, pid: Pid) {
        self.sim.assert_quiescent(pid);
        self.sim.assert_no_open_fds(pid);
    }
}

/// The bytes a `Recv` completion received, or its error.
pub fn received(complete: Complete) -> Result<Vec<u8>, Error> {
    match (complete.kind, complete.result) {
        (Op::Recv { buf, .. }, Ok(Done::Count(n))) => Ok(buf[..usize::try_from(n).unwrap()].to_vec()),
        (_, Err(error)) => Err(error),
        (kind, result) => panic!("not a receive: {kind:?}, {result:?}"),
    }
}

/// `127.0.0.1:port`.
pub fn local(port: u16) -> Addr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

pub fn recv_op(fd: Fd, len: usize) -> Op {
    Op::Recv { fd, buf: vec![0; len].into_boxed_slice() }
}
