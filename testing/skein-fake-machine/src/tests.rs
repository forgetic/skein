//! The machine's own bookkeeping, which the conformance suite cannot see:
//! what it keeps while open and frees once closed, handles issued once,
//! the most links one resolution follows, where a listing resumes, and a
//! moved directory's `..`. What it answers, case by case, the suite holds
//! to the real kernel (kernel.md, 8).

use alloc::vec::Vec;

use crate::fs::{How, Is, Item, Machine, Refusal};

fn machine(items: &[Item]) -> (Machine, crate::Opened) {
    let mut machine = Machine::new();
    let root = machine.lay(items);
    (machine, root)
}

#[test]
fn a_file_removed_lives_while_open_and_goes_once_closed() {
    let (mut machine, root) = machine(&[Item::file(b"f", b"bytes")]);
    let laid = machine.nodes.len();
    let file = machine.open(root, b"f", How::Read).unwrap();
    machine.remove(root, b"f", false).unwrap();
    assert_eq!(machine.nodes.len(), laid, "kept while open");
    assert_eq!(machine.read(file, 0, 16).unwrap(), b"bytes");
    machine.close(file);
    assert_eq!(machine.nodes.len(), laid - 1, "freed once closed");
    machine.close(root);
    assert_eq!(machine.open_handles(), 0);
}

#[test]
fn a_file_renamed_over_goes_unless_open() {
    let (mut machine, root) = machine(&[Item::file(b"a", b"new"), Item::file(b"b", b"old")]);
    let laid = machine.nodes.len();
    let old = machine.open(root, b"b", How::Read).unwrap();
    machine.rename(root, b"a", root, b"b").unwrap();
    assert_eq!(machine.nodes.len(), laid, "the old file kept while open");
    assert_eq!(machine.read(old, 0, 8).unwrap(), b"old");
    machine.close(old);
    assert_eq!(machine.nodes.len(), laid - 1);
}

#[test]
fn handles_are_issued_once() {
    let (mut machine, root) = machine(&[Item::file(b"f", b"")]);
    let first = machine.open(root, b"f", How::Read).unwrap();
    machine.close(first);
    let second = machine.open(root, b"f", How::Read).unwrap();
    assert_ne!(first, second);
}

#[test]
#[should_panic(expected = "a handle the machine never issued, or closed")]
fn a_closed_handle_is_its_clients_bug() {
    let (mut machine, root) = machine(&[Item::file(b"f", b"")]);
    let file = machine.open(root, b"f", How::Read).unwrap();
    machine.close(file);
    let stat = machine.stat(file);
    panic!("stated a closed handle: {stat:?}");
}

/// Forty links followed in one resolution, and no more.
#[test]
fn a_resolution_follows_forty_links() {
    let mut items = vec![Item::file(b"l0", b"end")];
    for n in 1..=41_u32 {
        items.push(Item::link(format!("l{n}").as_bytes(), format!("l{}", n - 1).as_bytes()));
    }
    let (mut machine, root) = machine(&items);
    let file = machine.open(root, b"l40", How::Read).unwrap();
    assert_eq!(machine.read(file, 0, 8).unwrap(), b"end");
    assert_eq!(machine.open(root, b"l41", How::Read), Err(Refusal::Loop));
}

#[test]
fn no_follow_refuses_links_in_every_path_part() {
    let (mut machine, root) = machine(&[
        Item::directory(b"dir"),
        Item::file(b"dir/file", b"data"),
        Item::link(b"parent", b"dir"),
        Item::link(b"leaf", b"dir/file"),
    ]);
    assert_eq!(machine.open(root, b"parent/file", How::ReadNoFollow), Err(Refusal::Loop));
    assert_eq!(machine.open(root, b"leaf", How::ReadNoFollow), Err(Refusal::Loop));
    assert_eq!(machine.open(root, b"parent/new", How::CreateNoFollow { mode: 0o644 }), Err(Refusal::Loop));
    assert_eq!(machine.open(root, b"parent", How::DirectoryNoFollow), Err(Refusal::Loop));
    let file = machine.open(root, b"dir/file", How::ReadNoFollow).unwrap();
    assert_eq!(machine.read(file, 0, 4).unwrap(), b"data");
    machine.close(file);
}

#[test]
fn a_listing_resumes_after_the_last_name_it_handed_back() {
    let (mut machine, root) = machine(&[Item::file(b"b", b""), Item::file(b"d", b"")]);
    let first = machine.list(root, 1, 255).unwrap();
    assert_eq!(first, [(Is::File, b"b".to_vec().into_boxed_slice())]);
    let a = machine.open(root, b"a", How::Create { mode: 0o644 }).unwrap();
    let c = machine.open(root, b"c", How::Create { mode: 0o644 }).unwrap();
    let rest: Vec<_> = machine.list(root, 8, 255).unwrap().into_iter().map(|(_, name)| name.to_vec()).collect();
    assert_eq!(rest, [b"c".to_vec(), b"d".to_vec()], "a name made before the cursor is not seen");
    assert!(machine.list(root, 8, 255).unwrap().is_empty());
    machine.close(a);
    machine.close(c);
}

#[test]
fn a_moved_directory_finds_its_new_parent_by_dot_dot() {
    let (mut machine, root) =
        machine(&[Item::directory(b"from"), Item::directory(b"to"), Item::file(b"to/mark", b"here")]);
    machine.rename(root, b"from", root, b"moved").unwrap();
    let to = machine.open(root, b"to", How::Directory).unwrap();
    machine.rename(root, b"moved", to, b"inner").unwrap();
    let mark = machine.open(root, b"to/inner/../mark", How::Read).unwrap();
    assert_eq!(machine.read(mark, 0, 8).unwrap(), b"here");
    assert_eq!(machine.rename(root, b"to", to, b"x"), Err(Refusal::Beneath));
}

/// A device opens as no file, as on a filesystem not mounted `nodev`, and
/// lists as itself; a FIFO likewise.
#[test]
fn a_device_and_a_fifo_open_as_no_file() {
    let (mut machine, root) = machine(&[Item::device(b"null"), Item::fifo(b"pipe")]);
    assert_eq!(machine.open(root, b"null", How::Read), Err(Refusal::NotAFile));
    assert_eq!(machine.open(root, b"pipe", How::Read), Err(Refusal::NotAFile));
    assert_eq!(machine.open(root, b"pipe", How::Directory), Err(Refusal::NotADirectory));
    let listed = machine.list(root, 8, 255).unwrap();
    let kinds: Vec<_> = listed.into_iter().map(|(is, _)| is).collect();
    assert_eq!(kinds, [Is::Device, Is::Fifo]);
    machine.remove(root, b"null", false).unwrap();
    machine.close(root);
}

#[test]
fn child_pipes_echo_at_chosen_descriptors_and_exit() {
    use skein_io::kernel::{Complete, Done, Exit, Op, Pipe, Spawn, Submit, Way};
    use skein_lib::{Queue, Token};
    use skein_sim::{Config, Sim};

    fn run(machine: &mut Machine, sim: &mut Sim, pid: skein_sim::Pid, number: u64, kind: Op) -> Complete {
        let mut submissions = Queue::with_capacity(1);
        submissions.push(Submit { op: Token::new(number), kind });
        sim.submit(pid, &mut submissions);
        crate::serve(machine, sim);
        let mut completions = Queue::with_capacity(1);
        sim.reap(pid, &mut completions);
        completions.pop().expect("one completion")
    }

    let mut machine = Machine::new();
    let laid = machine.lay(&[]);
    let mut sim = Sim::new(1, Config::calm());
    let pid = sim.spawn_process();
    let root = sim.root(pid, skein_sim::Handle::new(laid.raw()));
    let spawn = Spawn {
        program: Box::from(&b"echo"[..]),
        args: Box::new([]),
        env: Box::new([]),
        root,
        dir: Box::from(&b"."[..]),
        pipes: Box::new([
            Pipe { child: 7, way: Way::In, parent: None },
            Pipe { child: 9, way: Way::Out, parent: None },
        ]),
    };
    let complete = run(&mut machine, &mut sim, pid, 1, Op::Spawn { spawn: Box::new(spawn) });
    let Op::Spawn { spawn } = complete.kind else { panic!("spawn record") };
    let pipes: Vec<_> = spawn.pipes.iter().map(|pipe| pipe.parent.expect("parent end")).collect();
    let Done::Spawned { pidfd } = complete.result.unwrap() else { panic!("spawned child") };
    assert_eq!(pipes.len(), 2);
    assert_eq!(
        run(&mut machine, &mut sim, pid, 2, Op::PipeWrite { fd: pipes[0], bytes: Box::from(&b"hello"[..]), from: 0 })
            .result,
        Ok(Done::Count(5))
    );
    let read = run(&mut machine, &mut sim, pid, 3, Op::PipeRead { fd: pipes[1], buf: Box::new([0; 8]) });
    assert_eq!(read.result, Ok(Done::Count(5)));
    let Op::PipeRead { buf, .. } = read.kind else { panic!("pipe read") };
    assert_eq!(&buf[..5], b"hello");
    assert_eq!(run(&mut machine, &mut sim, pid, 4, Op::Close { fd: pipes[0] }).result, Ok(Done::Nothing));
    assert_eq!(
        run(&mut machine, &mut sim, pid, 5, Op::Wait { pidfd, reap: false }).result,
        Ok(Done::Exit(Exit::Code(0)))
    );
    assert_eq!(
        run(&mut machine, &mut sim, pid, 6, Op::PipeRead { fd: pipes[1], buf: Box::new([0; 8]) }).result,
        Ok(Done::Count(0))
    );
    run(&mut machine, &mut sim, pid, 7, Op::Close { fd: pipes[1] });
    run(&mut machine, &mut sim, pid, 8, Op::Close { fd: pidfd });
    run(&mut machine, &mut sim, pid, 9, Op::Close { fd: root });
    sim.assert_quiescent(pid);
    sim.assert_no_open_fds(pid);
    assert_eq!(machine.open_handles(), 0);
}

#[test]
fn never_child_waits_until_killed() {
    use skein_io::kernel::{Done, Exit, Op, Signal, Spawn, Submit};
    use skein_lib::{Queue, Token};
    use skein_sim::{Config, Sim};
    let mut machine = Machine::new();
    let laid = machine.lay(&[]);
    let mut sim = Sim::new(2, Config::calm());
    let pid = sim.spawn_process();
    let root = sim.root(pid, skein_sim::Handle::new(laid.raw()));
    let mut q = Queue::with_capacity(2);
    q.push(Submit {
        op: Token::new(1),
        kind: Op::Spawn {
            spawn: Box::new(Spawn {
                program: Box::from(&b"never"[..]),
                args: Box::new([]),
                env: Box::new([]),
                root,
                dir: Box::new([]),
                pipes: Box::new([]),
            }),
        },
    });
    sim.submit(pid, &mut q);
    crate::serve(&mut machine, &mut sim);
    let mut out = Queue::with_capacity(1);
    sim.reap(pid, &mut out);
    let Done::Spawned { pidfd, .. } = out.pop().unwrap().result.unwrap() else { panic!("spawn") };
    q.push(Submit { op: Token::new(2), kind: Op::Wait { pidfd, reap: false } });
    sim.submit(pid, &mut q);
    assert_eq!(sim.ready(pid), 0);
    q.push(Submit {
        op: Token::new(3),
        kind: Op::Signal { pidfd, signal: Signal::Kill, to: skein_io::kernel::Target::Child },
    });
    sim.submit(pid, &mut q);
    let mut out = Queue::with_capacity(2);
    sim.reap(pid, &mut out);
    let completions = [out.pop().unwrap(), out.pop().unwrap()];
    assert!(completions.iter().any(|done| done.op == Token::new(2) && done.result == Ok(Done::Exit(Exit::Signal(9)))));
    assert!(completions.iter().any(|done| done.op == Token::new(3) && done.result == Ok(Done::Nothing)));
    q.push(Submit { op: Token::new(4), kind: Op::Close { fd: pidfd } });
    q.push(Submit { op: Token::new(5), kind: Op::Close { fd: root } });
    sim.submit(pid, &mut q);
    crate::serve(&mut machine, &mut sim);
    sim.reap(pid, &mut out);
    sim.assert_quiescent(pid);
    sim.assert_no_open_fds(pid);
    assert_eq!(machine.open_handles(), 0);
}

#[test]
fn hard_link_names_share_bytes_and_owner_and_keep_the_node_until_all_names_and_handles_end() {
    let (mut machine, root) = machine(&[Item::file(b"file", b"shared"), Item::hard_link(b"alias", b"file")]);
    let original = machine.open(root, b"file", How::Read).unwrap();
    let alias = machine.open(root, b"alias", How::Read).unwrap();
    let laid = machine.nodes.len();
    assert_eq!(machine.stat(original), machine.stat(alias));
    assert_eq!(machine.stat(original).links, 2);
    assert_eq!(machine.stat(original).owner, 1000);
    assert_eq!(machine.stat(root).owner, 1000);
    machine.remove(root, b"file", false).unwrap();
    assert_eq!(machine.stat(original).links, 1);
    machine.close(original);
    assert_eq!(machine.nodes.len(), laid, "the other name and handle retain the node");
    assert_eq!(machine.read(alias, 0, 16).unwrap(), b"shared");
    machine.remove(root, b"alias", false).unwrap();
    assert_eq!(machine.stat(alias).links, 0);
    assert_eq!(machine.nodes.len(), laid, "the final handle retains the unlinked node");
    machine.close(alias);
    assert_eq!(machine.nodes.len(), laid - 1);
    machine.close(root);
    assert_eq!(machine.open_handles(), 0);
}

#[test]
fn replacing_one_hard_link_leaves_the_other_name_and_open_descriptor_intact() {
    let (mut machine, root) = machine(&[
        Item::file(b"old", b"old bytes"),
        Item::hard_link(b"alias", b"old"),
        Item::file(b"new", b"new bytes"),
    ]);
    let old = machine.open(root, b"old", How::Read).unwrap();
    machine.rename(root, b"new", root, b"old").unwrap();
    assert_eq!(machine.stat(old).links, 1);
    let alias = machine.open(root, b"alias", How::Read).unwrap();
    assert_eq!(machine.read(alias, 0, 16).unwrap(), b"old bytes");
    let new = machine.open(root, b"old", How::Read).unwrap();
    assert_eq!(machine.read(new, 0, 16).unwrap(), b"new bytes");
    assert_eq!(machine.stat(new).links, 1);
    for file in [old, alias, new, root] {
        machine.close(file);
    }
}

#[test]
fn recovered_hard_link_counts_follow_the_synced_namespace() {
    for seed in 0..16 {
        let (mut machine, root) = machine(&[Item::file(b"file", b"shared"), Item::hard_link(b"alias", b"file")]);
        machine.remove(root, b"file", false).unwrap();
        machine.sync(root);
        machine.crash(seed);
        let root = machine.reopen_root(0);
        let file = machine.open(root, b"alias", How::Read).unwrap();
        assert_eq!(machine.stat(file).links, 1);
        assert_eq!(machine.read(file, 0, 16).unwrap(), b"shared");
        assert_eq!(machine.open(root, b"file", How::Read), Err(Refusal::NotFound));
        machine.close(file);
        machine.close(root);
    }
}

#[test]
fn directory_links_count_children_across_moves_and_removal() {
    let (mut machine, root) = machine(&[Item::directory(b"d"), Item::directory(b"d/child")]);
    let dir = machine.open(root, b"d", How::Directory).unwrap();
    let child = machine.open(root, b"d/child", How::Directory).unwrap();
    assert_eq!(machine.stat(root).links, 3);
    assert_eq!(machine.stat(dir).links, 3);
    assert_eq!(machine.stat(child).links, 2);
    machine.rename(dir, b"child", root, b"child").unwrap();
    assert_eq!(machine.stat(root).links, 4);
    assert_eq!(machine.stat(dir).links, 2);
    machine.remove(root, b"child", true).unwrap();
    assert_eq!(machine.stat(root).links, 3);
    assert_eq!(machine.stat(child).links, 0);
    machine.remove(root, b"d", true).unwrap();
    assert_eq!(machine.stat(root).links, 2);
    for file in [child, dir, root] {
        machine.close(file);
    }
}
