//! Files: the operations on files a process submits, each passed to the
//! machine as a [`Call`] once the checks and the faults are done with it,
//! and each [`Answer`] made into its completion (simulator.md, 3).

use alloc::boxed::Box;
use alloc::format;

use skein_io::kernel::{Done, Entry, Error, Fd, Op, OpenHow};
use skein_lib::{Queue, Token};

use super::{Pid, Sim, cut};
use crate::machine::{Answer, Ask, Call, Handle, Reply, Ticket};
use crate::trace::Fault;

/// The code of an I/O error the `io_error` fault answers with: `EIO`.
const IO_ERROR: i32 = 5;

/// A descriptor of a file or a directory: what the machine calls it, and
/// how it was opened.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct File {
    pub(super) handle: Handle,
    pub(super) how: OpenHow,
}

/// A call the machine has yet to answer, and whose operation it is.
#[derive(Clone, Copy, Debug)]
pub(super) struct Asked {
    pid: Pid,
    token: Token,
    /// The bytes a `Read` asked for, or a `Write` gave; 0 otherwise.
    len: u32,
}

impl Sim {
    /// A root the shell opened at startup (shell.md, 6), for `pid`: the
    /// machine's `handle` of an open directory, which it issued to the
    /// world, given a descriptor as if opened with `OpenHow::Directory`.
    pub fn root(&mut self, pid: Pid, handle: Handle) -> Fd {
        if self.fds_full(pid) {
            self.fail(pid, "a root past the descriptor limit");
        }
        self.issued(pid, handle);
        self.open_file(pid, File { handle, how: OpenHow::Directory })
    }

    /// Moves the calls waiting for the machine into `calls`, oldest first, as
    /// many as it has room for. A world hands each to its machine as soon as
    /// a process has submitted, and gives the answers back with
    /// [`Sim::answer`]; an operation on files is decided then.
    pub fn calls(&mut self, calls: &mut Queue<Call>) {
        while calls.room() > 0 {
            let Some(call) = self.calls.pop_front() else {
                return;
            };
            calls.push(call);
        }
    }

    /// Calls that wait for the world to take them.
    #[must_use]
    pub fn calls_waiting(&self) -> u32 {
        u32::try_from(self.calls.len()).expect("fewer than 2^32 calls")
    }

    /// Takes every answer of the machine, in order, each completing the
    /// operation its call came from. An answer to no call taken, or of
    /// another shape than its call's, fails the world.
    pub fn answer(&mut self, answers: &mut Queue<Answer>) {
        while let Some(answer) = answers.pop() {
            self.answered(answer);
        }
    }

    /// An operation on files, or a `Close` of a file's descriptor, checked:
    /// a fault drawn instead of it, or its call to the machine.
    pub(super) fn file(&mut self, pid: Pid, token: Token, serial: u64, op: Op) {
        // The descriptor is the first thing an Open takes, as on Linux.
        if let Op::Open { .. } = op
            && self.fds_full(pid)
        {
            self.complete(pid, token, op, Err(Error::TooManyOpenFiles));
            return;
        }
        if let Some(error) = self.failure(pid, &op) {
            self.complete(pid, token, op, Err(error));
            return;
        }
        if let Op::Open { .. } = op {
            let process = self.process_mut(pid);
            process.opening = process.opening.checked_add(1).expect("fewer than 2^32 opens");
        }
        let hangs = match op {
            Op::Open { .. } | Op::Read { .. } | Op::Write { .. } | Op::Sync { .. } => true,
            Op::Stat { .. }
            | Op::Rename { .. }
            | Op::Remove { .. }
            | Op::MakeDirectory { .. }
            | Op::List { .. }
            | Op::Close { .. }
            | Op::Socket { .. }
            | Op::Bind { .. }
            | Op::Listen { .. }
            | Op::Accept { .. }
            | Op::Connect { .. }
            | Op::Recv { .. }
            | Op::Send { .. }
            | Op::Shutdown { .. }
            | Op::Cancel { .. } => false,
        };
        // A hung operation asks the machine nothing: only a Cancel ends it.
        if hangs && self.fault(pid, self.config.faults.hung, Fault::Hung) {
            self.park(pid, token, op);
            return;
        }
        let (ask, len) = self.ask(pid, &op);
        let ticket = Ticket(serial);
        self.asked.insert(ticket, Asked { pid, token, len });
        self.park(pid, token, op);
        self.calls.push_back(Call { ticket, ask });
    }

    /// The failure drawn for `op` instead of it, if any: the faults of a
    /// disk and a kernel beyond a healthy scratch directory (simulator.md,
    /// 4), each for the operations that may answer it.
    fn failure(&mut self, pid: Pid, op: &Op) -> Option<Error> {
        let (space, read_only, io) = match op {
            Op::Open { how: OpenHow::Create, .. } | Op::Rename { .. } | Op::MakeDirectory { .. } => (true, true, false),
            Op::Open { how: OpenHow::Read | OpenHow::Directory, .. } | Op::Stat { .. } | Op::List { .. } => {
                (false, false, false)
            }
            Op::Read { .. } => (false, false, true),
            Op::Write { .. } => (true, true, true),
            Op::Sync { .. } => (true, false, true),
            Op::Remove { .. } => (false, true, false),
            Op::Close { .. } => return None,
            Op::Socket { .. }
            | Op::Bind { .. }
            | Op::Listen { .. }
            | Op::Accept { .. }
            | Op::Connect { .. }
            | Op::Recv { .. }
            | Op::Send { .. }
            | Op::Shutdown { .. }
            | Op::Cancel { .. } => self.bug("only an operation on files can fail as a file's"),
        };
        let faults = self.config.faults;
        if self.fault(pid, faults.no_buffer, Fault::NoBuffer) {
            return Some(Error::NoBufferSpace);
        }
        if read_only && self.fault(pid, faults.read_only, Fault::ReadOnly) {
            return Some(Error::ReadOnly);
        }
        if space && self.fault(pid, faults.no_space, Fault::NoSpace) {
            return Some(Error::NoSpace);
        }
        if io && self.fault(pid, faults.io_error, Fault::IoError) {
            return Some(Error::Other(IO_ERROR));
        }
        None
    }

    /// What the machine is asked for `op`, in its handles, and the bytes a
    /// `Read` asks for or a `Write` gives, the latter cut short when the
    /// fault falls. A `Close` lets its descriptor go at once.
    fn ask(&mut self, pid: Pid, op: &Op) -> (Ask, u32) {
        match op {
            Op::Open { root, path, how } => {
                (Ask::Open { root: self.handle(pid, *root), path: path.clone(), how: *how }, 0)
            }
            Op::Read { fd, buf, at } => {
                let len = u32::try_from(buf.len()).expect("a valid Read's length is a count");
                (Ask::Read { file: self.handle(pid, *fd), at: *at, len }, len)
            }
            Op::Write { fd, bytes, from, at } => {
                let left = bytes.get(usize_of(*from)..).expect("a valid Write has bytes left");
                let (len, cut_short) = cut(&mut self.rng, self.config.faults.short_write, left.len());
                if cut_short {
                    self.record(pid, crate::trace::Event::Fault(Fault::ShortWrite));
                }
                let given = Box::from(left.get(..len).expect("cut within what was left"));
                let len = u32::try_from(len).expect("a valid Write's length is a count");
                (Ask::Write { file: self.handle(pid, *fd), at: *at, bytes: given }, len)
            }
            Op::Sync { fd } => (Ask::Sync { file: self.handle(pid, *fd) }, 0),
            Op::Stat { fd } => (Ask::Stat { file: self.handle(pid, *fd) }, 0),
            Op::Rename { from_dir, from, to_dir, to } => {
                let (from_dir, to_dir) = (self.handle(pid, *from_dir), self.handle(pid, *to_dir));
                (Ask::Rename { from_dir, from: from.clone(), to_dir, to: to.clone() }, 0)
            }
            Op::Remove { dir, name, directory } => {
                (Ask::Remove { dir: self.handle(pid, *dir), name: name.clone(), directory: *directory }, 0)
            }
            Op::MakeDirectory { dir, name } => {
                (Ask::MakeDirectory { dir: self.handle(pid, *dir), name: name.clone() }, 0)
            }
            Op::List { fd, entries, names } => {
                let most = u32::try_from(entries.len()).expect("a valid List's entries are a count");
                let room = u32::try_from(names.len()).expect("a valid List's names are a count");
                (Ask::List { dir: self.handle(pid, *fd), most, room }, 0)
            }
            Op::Close { fd } => {
                let file = self.process_mut(pid).files.remove(fd).expect("checked a file's descriptor");
                (Ask::Close { file: file.handle }, 0)
            }
            Op::Socket { .. }
            | Op::Bind { .. }
            | Op::Listen { .. }
            | Op::Accept { .. }
            | Op::Connect { .. }
            | Op::Recv { .. }
            | Op::Send { .. }
            | Op::Shutdown { .. }
            | Op::Cancel { .. } => self.bug("only an operation on files is asked of the machine"),
        }
    }

    /// The machine's answer, made into its operation's completion.
    fn answered(&mut self, answer: Answer) {
        let Answer { ticket, result } = answer;
        let Some(Asked { pid, token, len }) = self.asked.remove(&ticket) else {
            self.machine(&format!("an answer to {ticket:?}, which is no call waiting"));
        };
        let mut op = self.unpark(pid, token);
        if let Op::Open { .. } = op {
            let process = self.process_mut(pid);
            process.opening = process.opening.checked_sub(1).expect("an Open waiting holds a place");
        }
        let reply = match result {
            Ok(reply) => reply,
            Err(error) => {
                self.complete(pid, token, op, Err(error));
                return;
            }
        };
        let done = match (&mut op, reply) {
            (Op::Open { how, .. }, Reply::Opened(handle)) => {
                let how = *how;
                self.issued(pid, handle);
                Done::Fd(self.open_file(pid, File { handle, how }))
            }
            (Op::Read { buf, .. }, Reply::Read(bytes)) => {
                let (Some(slots), true) = (buf.get_mut(..bytes.len()), bytes.len() <= usize_of(len)) else {
                    self.machine(&format!("{} bytes read, of {len} asked", bytes.len()));
                };
                // Short of what is there, not only of what was asked.
                let (n, cut_short) = cut(&mut self.rng, self.config.faults.short_read, bytes.len());
                if cut_short {
                    self.record(pid, crate::trace::Event::Fault(Fault::ShortRead));
                }
                let (Some(slots), Some(read)) = (slots.get_mut(..n), bytes.get(..n)) else {
                    self.bug("a cut within what was read");
                };
                slots.copy_from_slice(read);
                Done::Count(u32::try_from(n).expect("no more than asked"))
            }
            (Op::Write { .. }, Reply::Done) => Done::Count(len),
            (Op::Stat { .. }, Reply::Stat(stat)) => Done::Stat(stat),
            (Op::List { entries, names, .. }, Reply::Listed(listed)) => {
                let n = fill(entries, names, &listed);
                let Some(n) = n else {
                    self.machine(&format!("{} entries listed, past the room asked: {listed:?}", listed.len()));
                };
                Done::Count(n)
            }
            (
                Op::Sync { .. } | Op::Rename { .. } | Op::Remove { .. } | Op::MakeDirectory { .. } | Op::Close { .. },
                Reply::Done,
            ) => Done::Nothing,
            (op, reply) => {
                let what = format!("{reply:?}, to {:?}", crate::trace::Summary::of(op));
                self.machine(&what);
            }
        };
        self.complete(pid, token, op, Ok(done));
    }

    /// The handle behind a descriptor the checks found a file's.
    fn handle(&self, pid: Pid, fd: Fd) -> Handle {
        self.process(pid).files.get(&fd).expect("checked a file's descriptor").handle
    }

    /// Fails the world unless `handle` is new: no process holds it.
    fn issued(&self, pid: Pid, handle: Handle) {
        for process in &self.processes {
            for file in process.files.values() {
                if file.handle == handle {
                    self.fail(pid, &format!("the machine issued {handle:?}, which is held already"));
                }
            }
        }
    }

    fn open_file(&mut self, pid: Pid, file: File) -> Fd {
        let process = self.process_mut(pid);
        let fd = Fd::new(process.next_fd);
        process.next_fd = process.next_fd.checked_add(1).expect("fewer than 2^31 descriptors");
        process.files.insert(fd, file);
        fd
    }

    /// Fails the world on an answer the machine should not have given.
    fn machine(&self, what: &str) -> ! {
        self.die(&format!("the machine broke the seam: {what}"));
    }
}

/// Writes `listed` into a `List`'s `entries` and `names`, one name after
/// the other: how many, or `None` when they do not fit.
fn fill(entries: &mut [Entry], names: &mut [u8], listed: &[(skein_io::kernel::Kind, Box<[u8]>)]) -> Option<u32> {
    let mut used = 0_usize;
    for ((kind, name), entry) in listed.iter().zip(entries.iter_mut()) {
        let end = used.checked_add(name.len())?;
        names.get_mut(used..end)?.copy_from_slice(name);
        let start = u32::try_from(used).ok()?;
        *entry = Entry { kind: *kind, start, len: u32::try_from(name.len()).ok()? };
        used = end;
    }
    if listed.len() > entries.len() {
        return None;
    }
    u32::try_from(listed.len()).ok()
}

fn usize_of(n: u32) -> usize {
    usize::try_from(n).expect("a u32 fits a usize")
}
