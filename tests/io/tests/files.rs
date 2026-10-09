//! Durable replacement checks actual namespace and bytes after terminals.

use skein_io::file::Event;
use skein_io::kernel::{Error, Op};
use skein_io_world::files::{FileSystem, NEW, OLD, OTHER, Story, world, world_with_files};
use skein_sim::{Config, Faults};

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

#[test]
fn a_store_failed_at_each_step_has_whole_contents_and_no_temporary() {
    for step in 0..17 {
        let mut outcome = world(7, Config::calm(), Story::Replace, Some(step), 3).run();
        assert!(outcome.machine.failed, "step {step} reached");
        let committed = step >= 15;
        assert!(
            matches!(outcome.procs[0].events[0], Event::Failed { error: Error::Other(5), committed: actual, .. } if actual == committed)
        );
        assert_eq!(outcome.machine.bytes(b"record"), if committed { NEW } else { OLD }, "step {step}");
        assert_eq!(outcome.machine.names(), vec![b"record".to_vec()]);
        outcome.machine.finish();
    }
}

#[test]
fn a_conflict_leaves_the_other_writers_file() {
    let mut outcome = world(11, Config::calm(), Story::Conflict, None, 3).run();
    assert!(matches!(outcome.procs[0].events[0], Event::Conflict { .. }));
    assert_eq!(outcome.machine.bytes(b"record"), OTHER);
    assert_eq!(outcome.machine.names(), vec![b"record".to_vec()]);
    outcome.machine.finish();
}

#[test]
fn a_hung_sync_is_given_up_at_the_owners_deadline() {
    let mut hung = false;
    let mut outcome = world(13, Config::calm(), Story::Replace, None, 3).run_with_faults(|_, _, submissions| {
        let stall = !hung && submissions.iter().any(|submit| matches!(submit.kind, Op::Sync { .. }));
        hung |= stall;
        Some(Faults { hung: if stall { 1000 } else { 0 }, ..Faults::NONE })
    });
    assert!(hung);
    assert!(matches!(outcome.procs[0].events[0], Event::Failed { error: Error::TimedOut, committed: false, .. }));
    assert_eq!(outcome.machine.bytes(b"record"), OLD);
    assert_eq!(outcome.machine.names(), vec![b"record".to_vec()]);
    outcome.machine.finish();
}

#[test]
fn a_hung_directory_sync_reports_committed_new_bytes_and_no_temporary() {
    let mut syncs = 0;
    let mut outcome = world(17, Config::calm(), Story::Replace, None, 3).run_with_faults(|_, _, submissions| {
        let sync = submissions.iter().any(|submit| matches!(submit.kind, Op::Sync { .. }));
        syncs += usize::from(sync);
        Some(Faults { hung: if sync && syncs == 2 { 1000 } else { 0 }, ..Faults::NONE })
    });
    assert_eq!(syncs, 2);
    assert!(matches!(outcome.procs[0].events[0], Event::Failed { error: Error::TimedOut, committed: true, .. }));
    assert_eq!(outcome.machine.bytes(b"record"), NEW);
    assert_eq!(outcome.machine.names(), vec![b"record".to_vec()]);
    outcome.machine.finish();
}

#[test]
fn a_target_link_is_replaced_without_writing_its_target() {
    let mut outcome = world(19, Config::calm(), Story::Link, None, 3).run();
    assert!(matches!(outcome.procs[0].events[0], Event::Stored { .. }));
    assert_eq!(outcome.machine.bytes(b"record"), NEW);
    assert_eq!(outcome.machine.bytes(b"target"), OLD);
    assert_eq!(outcome.machine.names(), vec![b"record".to_vec(), b"target".to_vec()]);
    outcome.machine.finish();
}

#[test]
fn the_largest_admitted_store_stays_within_file_ios_worst_case() {
    let mut outcome = world(23, Config::calm(), Story::Replace, None, 65_536).run();
    assert!(matches!(outcome.procs[0].events[0], Event::Stored { .. }));
    assert_eq!(outcome.machine.bytes(b"record"), vec![b'n'; 65_536]);
    assert!(outcome.heap.as_ref().expect("checked memory").iter().all(|(peak, bound)| peak <= bound));
    outcome.machine.finish();
}

#[test]
fn an_unreadable_target_is_replaced_with_default_mode() {
    let mut outcome = world(29, Config::calm(), Story::Unreadable, None, 3).run();
    assert!(matches!(outcome.procs[0].events[0], Event::Stored { .. }));
    assert_eq!(outcome.machine.bytes(b"record"), NEW);
    assert_eq!(outcome.machine.mode(b"record"), 0o644);
    assert_eq!(outcome.machine.names(), vec![b"record".to_vec()]);
    outcome.machine.finish();
}

#[test]
fn absent_conflicts_with_an_unreadable_target_and_digest_reports_its_read_fault() {
    for story in [Story::AbsentUnreadable, Story::DigestUnreadable] {
        let mut outcome = world(31, Config::calm(), story, None, 3).run();
        match story {
            Story::AbsentUnreadable => assert!(matches!(outcome.procs[0].events[0], Event::Conflict { .. })),
            Story::DigestUnreadable => assert!(matches!(
                outcome.procs[0].events[0],
                Event::Failed { error: Error::Permission, committed: false, residue: None, .. }
            )),
            Story::Replace
            | Story::Conflict
            | Story::Link
            | Story::Unreadable
            | Story::AbsentDangling
            | Story::Dangling => unreachable!("selected unreadable stories"),
        }
        assert_eq!(outcome.machine.names(), vec![b"record".to_vec()]);
        outcome.machine.finish();
    }
}

#[test]
fn absent_conflicts_with_a_dangling_link_while_any_replaces_its_name() {
    for story in [Story::AbsentDangling, Story::Dangling] {
        let mut outcome = world(37, Config::calm(), story, None, 3).run();
        match story {
            Story::AbsentDangling => assert!(matches!(outcome.procs[0].events[0], Event::Conflict { .. })),
            Story::Dangling => {
                assert!(matches!(outcome.procs[0].events[0], Event::Stored { .. }));
                assert_eq!(outcome.machine.bytes(b"record"), NEW);
            }
            Story::Replace
            | Story::Conflict
            | Story::Link
            | Story::Unreadable
            | Story::AbsentUnreadable
            | Story::DigestUnreadable => unreachable!("selected link stories"),
        }
        assert_eq!(outcome.machine.names(), vec![b"record".to_vec()]);
        outcome.machine.finish();
    }
}

#[test]
fn refused_cleanup_names_its_residue_and_the_next_replace_is_independent() {
    let mut first = world(41, Config::calm(), Story::Replace, Some(5), 3).run_with_faults(|_, _, submissions| {
        Some(Faults {
            read_only: if submissions.iter().any(|submit| matches!(submit.kind, Op::Remove { .. })) { 1000 } else { 0 },
            ..Faults::NONE
        })
    });
    let Event::Failed { error: Error::ReadOnly, committed: false, residue: Some(residue), .. } =
        &first.procs[0].events[0]
    else {
        panic!("cleanup refusal terminal: {:?}", first.procs[0].events[0]);
    };
    assert_eq!(residue.error, Error::ReadOnly);
    let residue = residue.name.to_vec();
    assert_eq!(first.machine.bytes(b"record"), OLD);
    assert_eq!(first.machine.names(), vec![b"record".to_vec(), residue.clone()]);
    let files = std::mem::replace(&mut first.machine, FileSystem::new(Story::Replace, None));
    first.machine.finish();
    drop(first);
    let mut second = world_with_files(43, Config::calm(), Story::Replace, 3, files).run();
    assert!(matches!(second.procs[0].events[0], Event::Stored { .. }));
    assert_eq!(second.machine.bytes(b"record"), NEW);
    assert_eq!(second.machine.names(), vec![b"record".to_vec(), residue]);
    second.machine.finish();
}

#[test]
fn private_roots_and_files_refuse_planted_metadata_unread_and_make_secret_modes() {
    use skein_io_world::private::{Story as Private, USER, world as private_world};
    for config in [Config::calm(), Config::chaos()] {
        for seed in [7, 11, 19] {
            for story in [
                Private::Create,
                Private::Replace,
                Private::RootMode,
                Private::RootOwner,
                Private::RootLink,
                Private::RootKind,
                Private::FileMode,
                Private::FileOwner,
                Private::FileLink,
                Private::FileKind,
                Private::FileLinks,
                Private::RootDeniedSafe,
                Private::FileDeniedSafe,
            ] {
                let mut outcome = private_world(seed, config, story).run();
                outcome.procs[0].check(USER);
                if matches!(story, Private::Create | Private::Replace) {
                    assert_eq!(outcome.machine.mode(b"secret"), 0o700);
                    assert_eq!(outcome.machine.mode(b"secret/record"), 0o600);
                } else {
                    assert_eq!(outcome.machine.reads, 0, "unsafe metadata is refused before any content read");
                }
                assert!(outcome.heap.as_ref().unwrap().iter().all(|(peak, bound)| peak <= bound));
                outcome.machine.finish();
            }
        }
    }
}
