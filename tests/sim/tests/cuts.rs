//! Simulator cuts discard original records and preserve the machine's
//! completed/durable state (simulator.md, 3.3).

use skein_fake_machine::{Item, Opened};
use skein_io::kernel::{Done, Op, OpenHow};
use skein_lib::{Duration, Time};
use skein_sim::{Faults, Handle};
use skein_sim_tests::World;

const OLD: &[u8] = b"old contents whole";
const NEW: &[u8] = b"replacement whole";

fn close_handles(world: &mut World, handles: &[Handle]) {
    for handle in handles {
        world.machine.close(Opened::new(handle.raw()));
    }
}

#[test]
fn a_cut_before_each_durable_replace_operation_recovers_old_or_new_whole() {
    for seed in [0, 1, 17] {
        for power_loss in [false, true] {
            for before in 0..=9 {
                let mut world = World::calm();
                let pid = world.spawn();
                let root = world.root(pid, &[Item::file(b"record", OLD)]);
                let mut file = None;
                for step in 0..before {
                    match step {
                        0 => {
                            file = Some(
                                world
                                    .open(pid, root, b"temporary", OpenHow::CreateNoFollow { mode: Some(0o600) })
                                    .expect("temporary"),
                            );
                        }
                        1 => assert_eq!(
                            world.write(pid, file.expect("open temporary"), 0, &NEW[..8]),
                            Ok(Done::Count(8))
                        ),
                        2 => assert_eq!(
                            world.write(pid, file.expect("open temporary"), 8, &NEW[8..]),
                            Ok(Done::Count(9))
                        ),
                        3 => assert_eq!(
                            world.call(pid, Op::Sync { fd: file.expect("open temporary") }).result,
                            Ok(Done::Nothing)
                        ),
                        4 => world.close(pid, file.take().expect("open temporary")),
                        5 | 7 => assert_eq!(world.call(pid, Op::Sync { fd: root }).result, Ok(Done::Nothing)),
                        6 => assert_eq!(
                            world
                                .call(
                                    pid,
                                    Op::Rename {
                                        from_dir: root,
                                        from: Box::from(b"temporary".as_slice()),
                                        to_dir: root,
                                        to: Box::from(b"record".as_slice())
                                    }
                                )
                                .result,
                            Ok(Done::Nothing)
                        ),
                        8 => world.close(pid, root),
                        _ => panic!("nine sequence operations"),
                    }
                }
                let handles = world.sim.cut(pid);
                close_handles(&mut world, &handles);
                if power_loss {
                    world.machine.crash(seed);
                }
                world.settled(pid);
                assert!(world.reap(pid).is_empty(), "the cut never completes outstanding records");
                let opened = world.machine.reopen_root(0);
                let restarted = world.spawn();
                let root = world.sim.root(restarted, Handle::new(opened.raw()));
                let file = world.open(restarted, root, b"record", OpenHow::Read).expect("whole target survives");
                let recovered = world.read(restarted, file, 0, 64).expect("recovered bytes");
                assert!(
                    recovered == OLD || recovered == NEW,
                    "seed {seed}, power={power_loss}, before={before}: {recovered:?}"
                );
                if !power_loss {
                    assert_eq!(recovered, if before >= 7 { NEW } else { OLD }, "kill keeps the completed rename");
                }
                world.close(restarted, file);
                world.close(restarted, root);
                world.settled(restarted);
                assert_eq!(world.machine.open_handles(), 0, "all cut and recovered handles close");
            }
        }
    }
}

#[test]
fn cuts_discard_unanswered_ready_and_scheduled_records_and_pending_close_handles() {
    for stage in 0..4 {
        let mut world = World::calm();
        let pid = world.spawn();
        let root = world.root(pid, &[Item::file(b"file", OLD)]);
        let file = world.open(pid, root, b"file", OpenHow::Read).expect("readable file");
        if stage == 2 {
            world.sim.set_faults(Faults { latency: 1000, latency_max: Duration::from_millis(1), ..Faults::NONE });
        }
        world.serving = stage != 0 && stage != 3;
        if stage == 3 {
            world.submit(pid, Op::Close { fd: file });
        } else {
            world.submit(pid, Op::Read { fd: file, at: 0, buf: vec![0; 64].into_boxed_slice() });
        }
        let handles = world.sim.cut(pid);
        assert_eq!(handles.len(), 2, "root and file, including a pending close");
        close_handles(&mut world, &handles);
        assert_eq!(world.sim.calls_waiting(), 0, "unanswered calls are discarded");
        world.sim.advance_to(Time::from_nanos(2_000_000));
        assert!(world.reap(pid).is_empty(), "ready and delayed completions never arrive");
        world.settled(pid);
        assert_eq!(world.machine.open_handles(), 0, "no lost handle at stage {stage}");
    }
}

#[test]
fn killing_one_process_leaves_the_other_process_and_its_file_undisturbed() {
    let mut world = World::calm();
    let cut = world.spawn();
    let peer = world.spawn();
    let root = world.root(cut, &[Item::file(b"file", OLD)]);
    let other = world.root(peer, &[Item::file(b"file", NEW)]);
    let file = world.open(cut, root, b"file", OpenHow::Read).expect("read file");
    world.serving = false;
    world.submit(cut, Op::Read { fd: file, at: 0, buf: vec![0; 64].into_boxed_slice() });
    let held = world.sim.cut(cut);
    close_handles(&mut world, &held);
    world.serving = true;
    let file = world.open(peer, other, b"file", OpenHow::Read).expect("peer remains readable");
    assert_eq!(world.read(peer, file, 0, 64).expect("peer bytes"), NEW);
    world.close(peer, file);
    world.close(peer, other);
    world.settled(cut);
    world.settled(peer);
}

#[test]
fn cutting_a_waiting_receive_closes_its_socket_and_wakes_the_peer() {
    let mut world = World::calm();
    let cut = world.spawn();
    let peer = world.spawn();
    let (ours, theirs) = world.pair(cut, peer);
    world.submit(cut, Op::Recv { fd: ours, buf: vec![0; 8].into_boxed_slice() });
    assert!(world.sim.cut(cut).is_empty(), "sockets have no machine handles");
    assert!(world.reap(cut).is_empty(), "waiting receive was discarded");
    assert_eq!(world.recv(peer, theirs, 8), Ok(Vec::new()), "peer sees closure");
    world.close(peer, theirs);
    world.settled(cut);
    world.settled(peer);
}
