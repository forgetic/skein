//! A small harness over the simulator: one token counter per world, the
//! minimal fake machine, and calls that submit one record and reap what came
//! of it.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};

use skein_fake_machine::{How, Item, Machine, Opened, serve};
use skein_io::kernel::{Addr, Complete, Done, Entry, Error, Family, Fd, Kind, Op, OpenHow, Stat, Submit};
use skein_lib::{Queue, Token};
use skein_sim::{Config, Handle, Pid, Sim};

/// Room for every completion a test can have outstanding.
const ROOM: u32 = 64;

pub struct World {
    pub sim: Sim,
    pub next: u64,
    /// What the simulator's operations on files go to.
    pub machine: Machine,
    /// Whether a submit is followed by the machine answering what it was
    /// asked, as a world does; off, the calls wait.
    pub serving: bool,
    roots: BTreeMap<(Pid, Fd), Opened>,
}

impl World {
    #[must_use]
    pub fn new(seed: u64, config: Config) -> World {
        World { sim: Sim::new(seed, config), next: 1, machine: Machine::new(), serving: true, roots: BTreeMap::new() }
    }

    #[must_use]
    pub fn calm() -> World {
        World::new(1, Config::calm())
    }

    pub fn spawn(&mut self) -> Pid {
        self.sim.spawn_process()
    }

    /// A fresh token.
    pub fn token(&mut self) -> Token {
        let token = Token::new(self.next);
        self.next = self.next.checked_add(1).expect("tokens never run out");
        token
    }

    /// Submits `op` under `token`, and has the machine answer what it was
    /// asked, if serving.
    pub fn submit_as(&mut self, pid: Pid, token: Token, op: Op) {
        let mut queue = Queue::with_capacity(1);
        queue.push(Submit { op: token, kind: op });
        self.sim.submit(pid, &mut queue);
        if self.serving {
            self.serve();
        }
    }

    /// The machine answers every call waiting.
    pub fn serve(&mut self) {
        serve(&mut self.machine, &mut self.sim);
    }

    /// Submits `op` under a fresh token.
    pub fn submit(&mut self, pid: Pid, op: Op) -> Token {
        let token = self.token();
        self.submit_as(pid, token, op);
        token
    }

    /// `pid` enters the kernel without submitting or reaping, as a loop's
    /// empty submit does: its waiting operations that can proceed are
    /// decided.
    pub fn enter(&mut self, pid: Pid) {
        self.sim.submit(pid, &mut Queue::with_capacity(0));
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
        let complete = got.pop().expect("the one completion");
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
        let addr = self.bind(pid, fd, local(0)).expect("port 0 on loopback binds");
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

    /// A root laid out as `items` say, opened for `pid` as the shell opens
    /// one at startup.
    pub fn root(&mut self, pid: Pid, items: &[Item]) -> Fd {
        let opened = self.machine.lay(items);
        let fd = self.sim.root(pid, Handle::new(opened.raw()));
        self.roots.insert((pid, fd), opened);
        fd
    }

    /// Opens a file to append in the startup machine and adopts its fresh handle.
    pub fn append(&mut self, pid: Pid, root: Fd, path: &[u8], mode: u32) -> Fd {
        let root = *self.roots.get(&(pid, root)).expect("a startup root");
        let opened = self.machine.open(root, path, How::Append { mode }).expect("a writable append file");
        self.sim.append(pid, Handle::new(opened.raw()))
    }

    pub fn open(&mut self, pid: Pid, root: Fd, path: &[u8], how: OpenHow) -> Result<Fd, Error> {
        match self.call(pid, Op::Open { root, path: Box::from(path), how }).result {
            Ok(Done::Fd(fd)) => Ok(fd),
            Ok(other) => panic!("an open answers with a descriptor: {other:?}"),
            Err(error) => Err(error),
        }
    }

    /// One `Read` of up to `len` bytes at `at`: the bytes, or the error.
    pub fn read(&mut self, pid: Pid, fd: Fd, at: u64, len: usize) -> Result<Vec<u8>, Error> {
        let complete = self.call(pid, Op::read(fd, vec![0; len].into(), at).expect("room to read"));
        bytes_read(complete)
    }

    /// One `Write` of all of `bytes` at `at`: what it answered.
    pub fn write(&mut self, pid: Pid, fd: Fd, at: u64, bytes: &[u8]) -> Result<Done, Error> {
        self.call(pid, Op::write(fd, Box::from(bytes), 0, at).expect("bytes to write")).result
    }

    pub fn stat(&mut self, pid: Pid, fd: Fd) -> Result<Stat, Error> {
        match self.call(pid, Op::Stat { fd }).result {
            Ok(Done::Stat(stat)) => Ok(stat),
            Ok(other) => panic!("a stat answers with what it found: {other:?}"),
            Err(error) => Err(error),
        }
    }

    /// One `List` with room for `entries`, and names of 255 bytes: each
    /// name and kind.
    pub fn list(&mut self, pid: Pid, fd: Fd, entries: usize) -> Result<Vec<(Vec<u8>, Kind)>, Error> {
        let op = Op::List { fd, entries: vec![Entry::BLANK; entries].into(), names: vec![0; 255].into() };
        let complete = self.call(pid, op);
        match (complete.kind, complete.result) {
            (Op::List { entries, names, .. }, Ok(Done::Count(n))) => {
                let mut listed = Vec::new();
                for entry in &entries[..usize::try_from(n).expect("a u32 fits a usize")] {
                    listed.push((entry.name(&names).expect("a name within names").to_vec(), entry.kind));
                }
                Ok(listed)
            }
            (_, Err(error)) => Err(error),
            (kind, result) => panic!("not a list: {kind:?}, {result:?}"),
        }
    }
}

/// The bytes a `Read` completion read, or its error.
pub fn bytes_read(complete: Complete) -> Result<Vec<u8>, Error> {
    match (complete.kind, complete.result) {
        (Op::Read { buf, .. }, Ok(Done::Count(n))) => {
            Ok(buf[..usize::try_from(n).expect("a u32 fits a usize")].to_vec())
        }
        (_, Err(error)) => Err(error),
        (kind, result) => panic!("not a read: {kind:?}, {result:?}"),
    }
}

/// The bytes a `Recv` completion received, or its error.
pub fn received(complete: Complete) -> Result<Vec<u8>, Error> {
    match (complete.kind, complete.result) {
        (Op::Recv { buf, .. }, Ok(Done::Count(n))) => {
            Ok(buf[..usize::try_from(n).expect("a u32 fits a usize")].to_vec())
        }
        (_, Err(error)) => Err(error),
        (kind, result) => panic!("not a receive: {kind:?}, {result:?}"),
    }
}

/// `127.0.0.1:port`.
#[must_use]
pub fn local(port: u16) -> Addr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

#[must_use]
pub fn recv_op(fd: Fd, len: usize) -> Op {
    Op::Recv { fd, buf: vec![0; len].into_boxed_slice() }
}
