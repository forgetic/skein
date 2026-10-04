//! Files through the machine seam (simulator.md, 3): the calls the
//! simulator passes to the minimal fake machine and the completions it
//! makes of the answers, each broken invariant of the operations on files,
//! the seam's own checks, and each fault the simulator draws for files.
//! What the records answer, rule by rule, is the conformance suite's.

use skein_fake_machine::Item;
use skein_io::kernel::{Done, Entry, Error, Fd, Kind, Op, OpenHow, Stat};
use skein_lib::{Duration, Queue, Token};
use skein_sim::{Answer, Ask, Call, Config, Event, Fault, Faults, Handle, Pid, Reply, Ticket};
use skein_sim_tests::World;

fn tree() -> [Item; 3] {
    [Item::file(b"a.txt", b"hello"), Item::directory(b"d"), Item::file(b"d/inner", b"inside")]
}

/// A calm world with a process and its root.
fn rooted() -> (World, Pid, Fd) {
    let mut world = World::calm();
    let pid = world.spawn();
    let root = world.root(pid, &tree());
    (world, pid, root)
}

fn faulty(faults: Faults) -> (World, Pid, Fd) {
    let mut world = World::new(7, Config { faults, ..Config::calm() });
    let pid = world.spawn();
    let root = world.root(pid, &tree());
    (world, pid, root)
}

fn close_all(world: &mut World, pid: Pid, fds: &[Fd]) {
    for fd in fds {
        world.close(pid, *fd);
    }
    world.settled(pid);
    assert_eq!(world.machine.open_handles(), 0, "every handle closed");
}

#[test]
fn a_file_is_made_written_read_listed_renamed_and_removed_through_the_machine() {
    let (mut world, pid, root) = rooted();
    let file = world.open(pid, root, b"new", OpenHow::Create).unwrap();
    assert_eq!(world.write(pid, file, 0, b"abcdef"), Ok(Done::Count(6)));
    assert_eq!(world.write(pid, file, 8, b"Z"), Ok(Done::Count(1)), "past the end: zeros fill the gap");
    assert_eq!(world.call(pid, Op::Sync { fd: file }).result, Ok(Done::Nothing));
    assert_eq!(world.stat(pid, file), Ok(Stat { kind: Kind::File, size: 9 }));
    world.close(pid, file);
    let file = world.open(pid, root, b"new", OpenHow::Read).unwrap();
    assert_eq!(world.read(pid, file, 2, 64), Ok(b"cdef\0\0Z".to_vec()));
    assert_eq!(world.read(pid, file, 9, 64), Ok(Vec::new()), "the end of the file");
    let mut listed = world.list(pid, root, 8).unwrap();
    listed.sort();
    let expected = [(b"a.txt".to_vec(), Kind::File), (b"d".to_vec(), Kind::Directory), (b"new".to_vec(), Kind::File)];
    assert_eq!(listed, expected);
    assert_eq!(world.list(pid, root, 8), Ok(Vec::new()), "then the end of the directory");
    let rename =
        Op::Rename { from_dir: root, from: Box::from(&b"new"[..]), to_dir: root, to: Box::from(&b"a.txt"[..]) };
    assert_eq!(world.call(pid, rename).result, Ok(Done::Nothing));
    assert_eq!(world.open(pid, root, b"new", OpenHow::Read), Err(Error::NotFound));
    let remove = Op::Remove { dir: root, name: Box::from(&b"a.txt"[..]), directory: false };
    assert_eq!(world.call(pid, remove).result, Ok(Done::Nothing));
    assert_eq!(world.read(pid, file, 0, 3), Ok(b"abc".to_vec()), "a file removed while open stays readable");
    close_all(&mut world, pid, &[file, root]);
}

#[test]
fn a_call_waits_for_the_world_and_its_operation_is_decided_by_the_answer() {
    let (mut world, pid, root) = rooted();
    world.serving = false;
    let open = world.submit(pid, Op::Open { root, path: Box::from(&b"a.txt"[..]), how: OpenHow::Read });
    assert_eq!(world.sim.calls_waiting(), 1);
    assert!(world.reap(pid).is_empty(), "nothing completes before the machine answers");
    assert_eq!(world.sim.in_flight(pid), 1);
    let mut calls = Queue::with_capacity(4);
    world.sim.calls(&mut calls);
    let call = calls.pop().expect("the open's call");
    let Ask::Open { path, how: OpenHow::Read, .. } = &call.ask else { panic!("an open: {call:?}") };
    assert_eq!(&**path, b"a.txt");
    assert_eq!(world.sim.calls_waiting(), 0, "taken");
    let mut answers = Queue::with_capacity(4);
    skein_fake_machine::step(&mut world.machine, call, &mut answers);
    world.sim.answer(&mut answers);
    let Ok(Done::Fd(file)) = world.reap_one(pid, open).result else { panic!("an open file") };
    world.serving = true;
    close_all(&mut world, pid, &[file, root]);
}

#[test]
fn opens_in_flight_hold_their_descriptors_against_the_limit() {
    let mut world = World::new(1, Config { max_fds: 3, ..Config::calm() });
    let pid = world.spawn();
    let root = world.root(pid, &tree());
    world.serving = false;
    let open = |path: &[u8]| Op::Open { root, path: Box::from(path), how: OpenHow::Read };
    let first = world.submit(pid, open(b"a.txt"));
    let second = world.submit(pid, open(b"d"));
    let third = world.submit(pid, open(b"d/inner"));
    assert_eq!(world.reap_one(pid, third).result, Err(Error::TooManyOpenFiles), "two held, of three");
    world.serve();
    let mut files = Vec::new();
    for complete in world.reap(pid) {
        let Ok(Done::Fd(fd)) = complete.result else { panic!("an open: {complete:?}") };
        assert!(complete.op == first || complete.op == second);
        files.push(fd);
    }
    world.serving = true;
    files.push(root);
    close_all(&mut world, pid, &files);
}

/// Each fault the simulator draws for files falls where the contract allows
/// it, the operation doing nothing (simulator.md, 4).
#[test]
fn each_failure_falls_on_the_operations_that_may_answer_it() {
    let every = Faults { no_space: 1000, ..Faults::NONE };
    let (mut world, pid, root) = faulty(every);
    assert_eq!(world.open(pid, root, b"new", OpenHow::Create), Err(Error::NoSpace));
    assert_eq!(world.open(pid, root, b"a.txt", OpenHow::Read).map(|_| ()).err(), None, "not on a read");
    let make = Op::MakeDirectory { dir: root, name: Box::from(&b"m"[..]) };
    assert_eq!(world.call(pid, make).result, Err(Error::NoSpace));
    let fds = world.sim.open_fds(pid);
    assert_eq!(fds, 2, "the root and the file read");

    let (mut world, pid, root) = faulty(Faults { read_only: 1000, ..Faults::NONE });
    let remove = Op::Remove { dir: root, name: Box::from(&b"a.txt"[..]), directory: false };
    assert_eq!(world.call(pid, remove).result, Err(Error::ReadOnly));
    assert!(world.open(pid, root, b"a.txt", OpenHow::Read).is_ok(), "the file is still there");

    let (mut world, pid, root) = faulty(Faults { io_error: 1000, ..Faults::NONE });
    let file = world.open(pid, root, b"a.txt", OpenHow::Read).unwrap();
    assert_eq!(world.read(pid, file, 0, 8), Err(Error::Other(5)), "EIO");
    assert_eq!(world.call(pid, Op::Sync { fd: file }).result, Err(Error::Other(5)));
    assert!(world.stat(pid, file).is_ok(), "not on a stat");

    let (mut world, pid, root) = faulty(Faults { no_buffer: 1000, ..Faults::NONE });
    assert_eq!(world.open(pid, root, b"a.txt", OpenHow::Read), Err(Error::NoBufferSpace));
    world.close(pid, root);
    world.settled(pid);
}

#[test]
fn a_short_read_and_a_short_write_count_at_least_one_byte() {
    let (mut world, pid, root) = faulty(Faults { short_read: 1000, short_write: 1000, ..Faults::NONE });
    let file = world.open(pid, root, b"new", OpenHow::Create).unwrap();
    let Ok(Done::Count(wrote)) = world.write(pid, file, 0, b"abcdef") else { panic!("a write") };
    assert!((1..6).contains(&wrote), "short: {wrote}");
    assert_eq!(world.write(pid, file, 0, b"x"), Ok(Done::Count(1)), "one byte cannot be cut");
    world.close(pid, file);
    let file = world.open(pid, root, b"a.txt", OpenHow::Read).unwrap();
    let read = world.read(pid, file, 0, 64).unwrap();
    assert!(!read.is_empty() && read.len() < 5 && b"hello".starts_with(&read), "short: {read:?}");
    let faults: Vec<_> = world.sim.trace().iter().filter(|entry| matches!(entry.event, Event::Fault(_))).collect();
    assert!(faults.iter().any(|entry| entry.event == Event::Fault(Fault::ShortWrite)));
    assert!(faults.iter().any(|entry| entry.event == Event::Fault(Fault::ShortRead)));
    close_all(&mut world, pid, &[file, root]);
}

#[test]
fn latency_delays_a_files_completion_too() {
    let faults = Faults { latency: 1000, latency_max: Duration::from_millis(3), ..Faults::NONE };
    let (mut world, pid, root) = faulty(faults);
    let open = world.submit(pid, Op::Open { root, path: Box::from(&b"a.txt"[..]), how: OpenHow::Read });
    assert!(world.reap(pid).is_empty(), "not delivered yet");
    assert!(world.sim.advance());
    let Ok(Done::Fd(file)) = world.reap_one(pid, open).result else { panic!("an open") };
    let close = world.submit(pid, Op::Close { fd: file });
    assert!(world.sim.advance());
    assert_eq!(world.reap_one(pid, close).result, Ok(Done::Nothing));
    let close = world.submit(pid, Op::Close { fd: root });
    assert!(world.sim.advance());
    assert_eq!(world.reap_one(pid, close).result, Ok(Done::Nothing));
    world.settled(pid);
}

/// Submits `op` and lets time pass until it completes, whatever chaos
/// delays it by; one that hangs, with nothing due, is cancelled, as io's
/// deadline would.
fn settle(world: &mut World, pid: Pid, op: Op) -> skein_io::kernel::Complete {
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

/// A seed replays files to the same trace, the machine's answers included
/// in what they made.
#[test]
fn a_seed_replays_a_world_of_files() {
    let run = |seed: u64| {
        let mut world = World::new(seed, Config::chaos());
        let pid = world.spawn();
        let root = world.root(pid, &tree());
        for n in 0..8_u8 {
            let open = Op::Open { root, path: Box::from(&[b'f', b'0' + n][..]), how: OpenHow::Create };
            if let Ok(Done::Fd(file)) = settle(&mut world, pid, open).result {
                settle(&mut world, pid, Op::write(file, Box::from(&b"some bytes"[..]), 0, u64::from(n)).unwrap());
                settle(&mut world, pid, Op::Sync { fd: file });
                settle(&mut world, pid, Op::Close { fd: file });
            }
        }
        settle(&mut world, pid, Op::Close { fd: root });
        world.settled(pid);
        world.sim.render_trace()
    };
    for seed in [3, 4] {
        assert_eq!(run(seed), run(seed), "seed {seed}");
    }
    assert_ne!(run(3), run(4), "seeds differ");
}

/// An `Open`, a `Read`, a `Write` and a `Sync` that hang, as on a network
/// filesystem whose server went away, wait for nothing time brings; a
/// `Cancel` stops each, and the `Open`'s place against the limit is given
/// back.
#[test]
fn a_hung_operation_on_files_waits_until_a_cancel_stops_it() {
    let (mut world, pid, root) = rooted();
    let read = world.open(pid, root, b"a.txt", OpenHow::Read).unwrap();
    let write = world.open(pid, root, b"new", OpenHow::Create).unwrap();
    world.sim.set_faults(Faults { hung: 1000, ..Faults::NONE });
    let ops = [
        Op::Open { root, path: Box::from(&b"a.txt"[..]), how: OpenHow::Read },
        Op::read(read, Box::from([0; 4]), 0).unwrap(),
        Op::write(write, Box::from(&b"x"[..]), 0, 0).unwrap(),
        Op::Sync { fd: write },
    ];
    for op in ops {
        let target = world.submit(pid, op);
        assert!(world.reap(pid).is_empty() && world.sim.next_due().is_none(), "hung, with nothing due");
        let cancel = world.submit(pid, Op::Cancel { target });
        let mut got = world.reap(pid);
        got.sort_by_key(|complete| complete.op);
        let results: Vec<_> = got.into_iter().map(|complete| (complete.op, complete.result)).collect();
        assert_eq!(results, [(target, Err(Error::Cancelled)), (cancel, Ok(Done::Nothing))]);
    }
    assert_eq!(world.sim.open_fds(pid), 3, "the hung Open made no descriptor and holds no place");
    world.sim.set_faults(Faults::NONE);
    close_all(&mut world, pid, &[read, write, root]);
}

/// A `Close` of a file and a `Cancel` of it in one batch: the `Close` is
/// with the machine when the `Cancel` comes, as a ring's close is with the
/// kernel, so the `Cancel` is too late.
#[test]
fn a_cancel_of_a_close_waiting_on_the_machine_is_too_late() {
    let (mut world, pid, root) = rooted();
    let file = world.open(pid, root, b"a.txt", OpenHow::Read).unwrap();
    let (close, cancel) = (world.token(), world.token());
    let mut batch = Queue::with_capacity(2);
    batch.push(skein_io::kernel::Submit { op: close, kind: Op::Close { fd: file } });
    batch.push(skein_io::kernel::Submit { op: cancel, kind: Op::Cancel { target: close } });
    world.sim.submit(pid, &mut batch);
    world.serve();
    let mut got = world.reap(pid);
    got.sort_by_key(|complete| complete.op);
    let results: Vec<_> = got.into_iter().map(|complete| (complete.op, complete.result)).collect();
    assert_eq!(results, [(close, Ok(Done::Nothing)), (cancel, Err(Error::TooLate))]);
    close_all(&mut world, pid, &[root]);
}

// Broken invariants.

#[test]
#[should_panic(expected = "an operation on files on")]
fn an_operation_on_files_on_a_sockets_descriptor() {
    let mut world = World::calm();
    let pid = world.spawn();
    let socket = world.socket(pid);
    world.submit(pid, Op::Stat { fd: socket });
}

#[test]
#[should_panic(expected = "an operation on sockets on a file's descriptor")]
fn an_operation_on_sockets_on_a_files_descriptor() {
    let (mut world, pid, root) = rooted();
    world.submit(pid, Op::Shutdown { fd: root });
}

#[test]
#[should_panic(expected = "a Read on a descriptor not opened to read")]
fn a_read_of_a_file_opened_to_create() {
    let (mut world, pid, root) = rooted();
    let file = world.open(pid, root, b"new", OpenHow::Create).unwrap();
    world.submit(pid, Op::Read { fd: file, buf: Box::from([0; 4]), at: 0 });
}

#[test]
#[should_panic(expected = "a Read on a descriptor not opened to read")]
fn a_read_of_a_directory_opened_as_one() {
    let (mut world, pid, root) = rooted();
    world.submit(pid, Op::Read { fd: root, buf: Box::from([0; 4]), at: 0 });
}

#[test]
#[should_panic(expected = "a Write on a descriptor not opened to create")]
fn a_write_of_a_file_opened_to_read() {
    let (mut world, pid, root) = rooted();
    let file = world.open(pid, root, b"a.txt", OpenHow::Read).unwrap();
    world.submit(pid, Op::Write { fd: file, bytes: Box::from(&b"x"[..]), from: 0, at: 0 });
}

#[test]
#[should_panic(expected = "a List on a descriptor opened to create")]
fn a_list_of_a_file_opened_to_create() {
    let (mut world, pid, root) = rooted();
    let file = world.open(pid, root, b"new", OpenHow::Create).unwrap();
    world.submit(pid, Op::List { fd: file, entries: Box::from([Entry::BLANK]), names: vec![0; 255].into() });
}

#[test]
#[should_panic(expected = "which io never cancels")]
fn a_cancel_of_an_operation_on_files_that_completes_promptly() {
    let (mut world, pid, root) = rooted();
    world.serving = false;
    let rename = Op::Rename { from_dir: root, from: Box::from(&b"a.txt"[..]), to_dir: root, to: Box::from(&b"b"[..]) };
    let rename = world.submit(pid, rename);
    world.submit(pid, Op::Cancel { target: rename });
}

#[test]
#[should_panic(expected = "a Close while")]
fn a_close_of_a_root_with_an_open_beneath_it_in_flight() {
    let (mut world, pid, root) = rooted();
    world.serving = false;
    world.submit(pid, Op::Open { root, path: Box::from(&b"a.txt"[..]), how: OpenHow::Read });
    world.submit(pid, Op::Close { fd: root });
}

#[test]
#[should_panic(expected = "a Close while")]
fn a_close_of_either_directory_of_a_rename_in_flight() {
    let (mut world, pid, root) = rooted();
    let dir = world.open(pid, root, b"d", OpenHow::Directory).unwrap();
    world.serving = false;
    world.submit(
        pid,
        Op::Rename { from_dir: root, from: Box::from(&b"a.txt"[..]), to_dir: dir, to: Box::from(&b"b"[..]) },
    );
    world.submit(pid, Op::Close { fd: dir });
}

#[test]
#[should_panic(expected = "an invalid record")]
fn a_name_that_could_leave_its_directory() {
    let (mut world, pid, root) = rooted();
    world.submit(pid, Op::MakeDirectory { dir: root, name: Box::from(&b"../out"[..]) });
}

#[test]
#[should_panic(expected = "which is not open in this process")]
fn an_operation_on_a_closed_files_descriptor() {
    let (mut world, pid, root) = rooted();
    let file = world.open(pid, root, b"a.txt", OpenHow::Read).unwrap();
    world.close(pid, file);
    world.submit(pid, Op::Stat { fd: file });
}

// The seam's own checks: an answer the machine should not give fails the
// world.

/// A world whose one Read waits for its answer, and the call it made.
fn reading() -> (World, Pid, Call) {
    let (mut world, pid, root) = rooted();
    let file = world.open(pid, root, b"a.txt", OpenHow::Read).expect("a file to read");
    world.serving = false;
    world.submit(pid, Op::Read { fd: file, buf: Box::from([0; 4]), at: 0 });
    let mut calls = Queue::with_capacity(1);
    world.sim.calls(&mut calls);
    (world, pid, calls.pop().expect("the read's call"))
}

fn answer(world: &mut World, answer: Answer) {
    let mut answers = Queue::with_capacity(1);
    answers.push(answer);
    world.sim.answer(&mut answers);
}

#[test]
#[should_panic(expected = "which is no call waiting")]
fn an_answer_to_no_call() {
    let (mut world, _pid, call) = reading();
    answer(&mut world, Answer { ticket: call.ticket, result: Ok(Reply::Read(Box::from(&b"hell"[..]))) });
    answer(&mut world, Answer { ticket: call.ticket, result: Ok(Reply::Read(Box::from(&b"hell"[..]))) });
}

#[test]
#[should_panic(expected = "the machine broke the seam")]
fn an_answer_of_another_shape() {
    let (mut world, _pid, call) = reading();
    answer(&mut world, Answer { ticket: call.ticket, result: Ok(Reply::Done) });
}

#[test]
#[should_panic(expected = "5 bytes read, of 4 asked")]
fn a_read_answered_with_more_than_it_asked() {
    let (mut world, _pid, call) = reading();
    answer(&mut world, Answer { ticket: call.ticket, result: Ok(Reply::Read(Box::from(&b"hello"[..]))) });
}

#[test]
#[should_panic(expected = "the simulator broke the contract")]
fn an_answer_of_an_error_the_operation_never_names() {
    let (mut world, _pid, call) = reading();
    answer(&mut world, Answer { ticket: call.ticket, result: Err(Error::Escape) });
}

#[test]
#[should_panic(expected = "which is held already")]
fn a_handle_issued_twice() {
    let (mut world, pid, root) = rooted();
    world.serving = false;
    world.submit(pid, Op::Open { root, path: Box::from(&b"a.txt"[..]), how: OpenHow::Read });
    let mut calls = Queue::with_capacity(1);
    world.sim.calls(&mut calls);
    let call = calls.pop().expect("the open's call");
    let Ask::Open { root: held, .. } = call.ask else { panic!("an open") };
    answer(&mut world, Answer { ticket: call.ticket, result: Ok(Reply::Opened(held)) });
}

#[test]
fn a_ticket_and_a_handle_are_plain_values() {
    assert_eq!(Handle::new(7).raw(), 7);
    let _ = Token::new(1);
    let _: Option<Ticket> = None;
}
