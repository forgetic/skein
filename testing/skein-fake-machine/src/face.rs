//! The face the machine shows behind the simulator (testing-strategy.md,
//! 4.3): the seam's calls translated into the machine's own operations,
//! and its answers and refusals back into the simulator's vocabulary.

use alloc::vec::Vec;

use skein_io::kernel::{Error, Kind, OpenHow, Stat};
use skein_lib::Queue;
use skein_sim::{Answer, Ask, Call, Handle, Reply, Sim};

use crate::fs::{Facts, How, Is, Machine, Opened, Refusal};

/// The most calls a world takes from the simulator at once.
const ROOM: u32 = 64;

/// Answers one call of the seam: a step machine's shape, the machine's
/// state, one call in, its one answer out.
pub fn step(machine: &mut Machine, call: Call, answers: &mut Queue<Answer>) {
    let Call { ticket, ask } = call;
    let result = match ask {
        Ask::Open { root, path, how } => match machine.open(opened(root), &path, how_of(how)) {
            Ok(file) => Ok(Reply::Opened(Handle::new(file.raw()))),
            Err(refusal) => Err(error(refusal)),
        },
        Ask::Read { file, at, len } => match machine.read(opened(file), at, len) {
            Ok(bytes) => Ok(Reply::Read(bytes.into_boxed_slice())),
            Err(refusal) => Err(error(refusal)),
        },
        Ask::Write { file, at, bytes } => match machine.write(opened(file), at, &bytes) {
            Ok(()) => Ok(Reply::Done),
            Err(refusal) => Err(error(refusal)),
        },
        // Nothing is lost in a fake that never crashes.
        Ask::Sync { file } => {
            let _how: How = machine.how(opened(file));
            Ok(Reply::Done)
        }
        Ask::Stat { file } => Ok(Reply::Stat(stat(machine.stat(opened(file))))),
        Ask::Rename { from_dir, from, to_dir, to } => {
            done(machine.rename(opened(from_dir), &from, opened(to_dir), &to))
        }
        Ask::Remove { dir, name, directory } => done(machine.remove(opened(dir), &name, directory)),
        Ask::MakeDirectory { dir, name } => done(machine.make_directory(opened(dir), &name)),
        Ask::List { dir, most, room } => match machine.list(opened(dir), most, room) {
            Ok(listed) => {
                let mut entries = Vec::with_capacity(listed.len());
                for (is, name) in listed {
                    entries.push((kind(is), name));
                }
                Ok(Reply::Listed(entries))
            }
            Err(refusal) => Err(error(refusal)),
        },
        Ask::Close { file } => {
            machine.close(opened(file));
            Ok(Reply::Done)
        }
    };
    answers.push(Answer { ticket, result });
}

/// What a world does once a process has submitted: every call the
/// simulator has for the machine, answered, and the answers handed back,
/// until none is left.
pub fn serve(machine: &mut Machine, sim: &mut Sim) {
    let mut calls = Queue::with_capacity(ROOM);
    let mut answers = Queue::with_capacity(ROOM);
    loop {
        sim.calls(&mut calls);
        if calls.is_empty() {
            return;
        }
        while let Some(call) = calls.pop() {
            step(machine, call, &mut answers);
        }
        sim.answer(&mut answers);
    }
}

fn opened(handle: Handle) -> Opened {
    Opened::new(handle.raw())
}

fn done(result: Result<(), Refusal>) -> Result<Reply, Error> {
    match result {
        Ok(()) => Ok(Reply::Done),
        Err(refusal) => Err(error(refusal)),
    }
}

const fn how_of(how: OpenHow) -> How {
    match how {
        OpenHow::Read => How::Read,
        OpenHow::Directory => How::Directory,
        OpenHow::Create => How::Create,
    }
}

const fn kind(is: Is) -> Kind {
    match is {
        Is::File => Kind::File,
        Is::Directory => Kind::Directory,
        Is::Link => Kind::Symlink,
        Is::Fifo | Is::Device => Kind::Other,
    }
}

const fn stat(facts: Facts) -> Stat {
    Stat { kind: kind(facts.is), size: facts.size }
}

/// A refusal as the kernel names it (`skein_io::kernel::Error`).
const fn error(refusal: Refusal) -> Error {
    match refusal {
        Refusal::NotFound => Error::NotFound,
        Refusal::Exists => Error::Exists,
        Refusal::NotADirectory => Error::NotADirectory,
        Refusal::IsADirectory => Error::IsADirectory,
        Refusal::NotEmpty => Error::NotEmpty,
        Refusal::Permission => Error::Permission,
        Refusal::NoSpace => Error::NoSpace,
        Refusal::Loop => Error::TooManyLinks,
        Refusal::NameTooLong => Error::NameTooLong,
        Refusal::Escape => Error::Escape,
        Refusal::NotAFile => Error::NotAFile,
        // EINVAL, as renameat2 answers a directory moved beneath itself.
        Refusal::Beneath => Error::InvalidArgument,
    }
}
