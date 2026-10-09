//! Files under chaos, over many seeds (simulator.md, 6): a workload of
//! every operation on files through the machine seam, every completion
//! checked by the simulator, each process settled at the end, and every
//! fault of files fell.

use std::collections::BTreeSet;

use skein_fake_machine::Item;
use skein_io::kernel::{Complete, Done, Entry, Error, Fd, Op, OpenHow};
use skein_sim::{Config, Event, Fault, Pid, Summary};
use skein_sim_tests::World;

/// Submits `op` and lets time pass until it completes; one that hangs, with
/// nothing due, is cancelled, as io's deadline would.
fn settle(world: &mut World, pid: Pid, op: Op) -> Complete {
    let token = world.submit(pid, op);
    loop {
        if let Some(complete) = world.reap(pid).into_iter().find(|complete| complete.op == token) {
            return complete;
        }
        if !world.sim.advance() {
            world.submit(pid, Op::Cancel { target: token });
        }
    }
}

fn name(prefix: u8, n: u8) -> Box<[u8]> {
    Box::from(&[prefix, b'0' + n][..])
}

/// Writes `bytes` whole, short writes continued, until done or an error.
fn write_all(world: &mut World, pid: Pid, file: Fd, bytes: &[u8]) -> Result<(), Error> {
    let mut from = 0_u32;
    while usize::try_from(from).expect("a u32 fits a usize") < bytes.len() {
        let op = Op::write(file, Box::from(bytes), from, u64::from(from)).expect("bytes left to write");
        match settle(world, pid, op).result {
            Ok(Done::Count(n)) => from += n,
            Ok(other) => panic!("a write counts: {other:?}"),
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Continues short appends with the original buffer and returns the bytes that landed before any failure.
fn append_all(world: &mut World, pid: Pid, file: Fd, bytes: &[u8]) -> Vec<u8> {
    let mut from = 0_u32;
    while usize::try_from(from).expect("a u32 fits") < bytes.len() {
        let op = Op::append(file, Box::from(bytes), from).expect("bytes left to append");
        match settle(world, pid, op).result {
            Ok(Done::Count(n)) => from = from.checked_add(n).expect("within the original bytes"),
            Err(_) => break,
            Ok(other) => panic!("an append counts: {other:?}"),
        }
    }
    bytes[..usize::try_from(from).expect("a count fits")].to_vec()
}

/// Reads to the end, short reads continued: the bytes, or an error.
fn read_all(world: &mut World, pid: Pid, file: Fd) -> Result<Vec<u8>, Error> {
    let mut got = Vec::new();
    loop {
        let at = u64::try_from(got.len()).expect("a usize fits a u64");
        let op = Op::read(file, vec![0; 16].into(), at).expect("room to read");
        let complete = settle(world, pid, op);
        match (complete.kind, complete.result) {
            (_, Ok(Done::Count(0))) => return Ok(got),
            (Op::Read { buf, .. }, Ok(Done::Count(n))) => {
                got.extend_from_slice(&buf[..usize::try_from(n).expect("a u32 fits a usize")]);
            }
            (_, Err(error)) => return Err(error),
            (kind, result) => panic!("a read: {kind:?}, {result:?}"),
        }
    }
}

/// One seed's workload; each operation may fail as chaos draws it.
fn workload(seed: u64) -> World {
    let mut world = World::new(seed, Config::chaos());
    let pid = world.spawn();
    let root = world.root(pid, &[Item::file(b"seed", b"planted"), Item::directory(b"sub")]);
    let a = world.append(pid, root, b"seed", 0o600);
    let b = world.append(pid, root, b"seed", 0o600);
    let mut expected = b"planted".to_vec();
    for (file, bytes) in [(a, b"first".as_slice()), (b, b"/between/".as_slice()), (a, b"last".as_slice())] {
        expected.extend(append_all(&mut world, pid, file, bytes));
    }
    let _ = settle(&mut world, pid, Op::Close { fd: a });
    let _ = settle(&mut world, pid, Op::Close { fd: b });
    let open = Op::Open { root, path: Box::from(&b"seed"[..]), how: OpenHow::Read };
    if let Ok(Done::Fd(file)) = settle(&mut world, pid, open).result {
        if let Ok(read) = read_all(&mut world, pid, file) {
            assert_eq!(read, expected, "seed {seed}: the prefix and each completed append land once");
        }
        let _ = settle(&mut world, pid, Op::Close { fd: file });
    }
    let bytes: Vec<u8> = (0..40_u8).collect();
    for n in 0..4_u8 {
        let open = Op::Open { root, path: name(b'f', n), how: OpenHow::Create { mode: None } };
        let Ok(Done::Fd(file)) = settle(&mut world, pid, open).result else { continue };
        let written = write_all(&mut world, pid, file, &bytes);
        let _ = settle(&mut world, pid, Op::Sync { fd: file });
        let _ = settle(&mut world, pid, Op::Close { fd: file });
        let open = Op::Open { root, path: name(b'f', n), how: OpenHow::Read };
        if let Ok(Done::Fd(file)) = settle(&mut world, pid, open).result {
            if let (Ok(read), Ok(())) = (read_all(&mut world, pid, file), written) {
                assert_eq!(read, bytes, "seed {seed}: what was written is read back");
            }
            let _ = settle(&mut world, pid, Op::Stat { fd: file });
            let _ = settle(&mut world, pid, Op::Close { fd: file });
        }
        let rename = Op::Rename { from_dir: root, from: name(b'f', n), to_dir: root, to: name(b'g', n) };
        let _ = settle(&mut world, pid, rename);
        let _ = settle(&mut world, pid, Op::MakeDirectory { dir: root, name: name(b'd', n), mode: 0o777 });
    }
    let mut listed = BTreeSet::new();
    loop {
        let op = Op::List { fd: root, entries: vec![Entry::BLANK; 3].into(), names: vec![0; 255].into() };
        let complete = settle(&mut world, pid, op);
        let (Op::List { entries, names, .. }, Ok(Done::Count(n))) = (complete.kind, complete.result) else { break };
        if n == 0 {
            break;
        }
        for entry in &entries[..usize::try_from(n).expect("a u32 fits a usize")] {
            assert!(
                listed.insert(entry.name(&names).expect("a name within names").to_vec()),
                "seed {seed}: each entry once"
            );
        }
    }
    for name in listed {
        let directory = name.first() == Some(&b'd') || name == b"sub";
        let remove = Op::Remove { dir: root, name: name.into(), directory };
        let _ = settle(&mut world, pid, remove);
    }
    let _ = settle(&mut world, pid, Op::Close { fd: root });
    world.settled(pid);
    assert_eq!(world.machine.open_handles(), 0, "seed {seed}: every handle closed");
    world
}

#[test]
fn files_under_chaos_keep_the_contract_and_every_fault_falls() {
    let mut faults = BTreeSet::new();
    let mut errors = BTreeSet::new();
    let mut append_faults = BTreeSet::new();
    for seed in 0..200_u64 {
        let world = workload(seed);
        let mut appending = false;
        for entry in world.sim.trace() {
            match entry.event {
                Event::Fault(fault) => {
                    faults.insert(fault);
                    if appending {
                        append_faults.insert(fault);
                    }
                }
                Event::Complete { result: Err(error), .. } => {
                    errors.insert(error);
                }
                Event::Submit { kind, .. } => appending = matches!(kind, Summary::Append { .. }),
                Event::Complete { .. } => {}
            }
        }
    }
    for fault in [
        Fault::Latency,
        Fault::ShortRead,
        Fault::ShortWrite,
        Fault::NoBuffer,
        Fault::NoSpace,
        Fault::ReadOnly,
        Fault::IoError,
        Fault::Hung,
    ] {
        assert!(faults.contains(&fault), "{fault:?} fell in some seed: {faults:?}");
    }
    for fault in [Fault::ShortWrite, Fault::Hung, Fault::NoBuffer, Fault::NoSpace, Fault::ReadOnly, Fault::IoError] {
        assert!(append_faults.contains(&fault), "{fault:?} fell on an Append: {append_faults:?}");
    }
    for error in [Error::NoBufferSpace, Error::NoSpace, Error::ReadOnly, Error::Other(5), Error::Cancelled] {
        assert!(errors.contains(&error), "some operation failed with {error:?}: {errors:?}");
    }
}
