use skein_oauth_accounts::{Failure, Place, Unloaded};
use skein_oauth_accounts_world::private::{self, Story};
use skein_oauth_accounts_world::world::Fact;

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

#[test]
fn a_refreshed_record_is_stored_privately_before_it_is_lent() {
    let mut outcome = private::run(37, Story::Refresh);
    assert_eq!(private::facts(&outcome.procs), &[Fact::Keep(1), Fact::Granted(1), Fact::Closed]);
    assert_eq!(outcome.machine.record().generation, 1);
    assert!(outcome.machine.syncs >= 2 && outcome.machine.renames == 1);
    outcome.machine.finish();
}

#[test]
fn a_record_kept_in_a_private_file_is_loaded_at_the_start() {
    let mut outcome = private::run(39, Story::Load);
    assert_eq!(private::facts(&outcome.procs), &[Fact::Granted(0), Fact::Closed]);
    assert_eq!(outcome.machine.writes, 0);
    outcome.machine.finish();
}

#[test]
fn unsafe_private_metadata_is_refused_without_reading() {
    for story in [Story::RootMode, Story::FileMode, Story::FileOwner, Story::FileLink, Story::FileLinks] {
        let mut outcome = private::run(41, story);
        assert_eq!(outcome.machine.reads, 0);
        let at = if story == Story::RootMode { Place::Directory } else { Place::File };
        assert!(matches!(private::facts(&outcome.procs), [Fact::Failed(Failure::Unloaded {
            at: found, why: Unloaded::Refused(_),
        }), Fact::Closed] if *found == at));
        outcome.machine.finish();
    }
}

#[test]
fn a_missing_or_unreadable_record_ends_its_waiting_grant_once() {
    for (story, failure) in [
        (Story::Missing, Failure::Expired),
        (Story::Unreadable, Failure::Unloaded { at: Place::File, why: Unloaded::Unreadable }),
    ] {
        let mut outcome = private::run(43, story);
        assert_eq!(private::facts(&outcome.procs), &[Fact::Failed(failure), Fact::Closed]);
        outcome.machine.finish();
    }
}

#[test]
fn a_filesystem_that_fails_or_stalls_a_keep_lends_nothing_new() {
    for story in [Story::KeepFailed, Story::KeepStalled] {
        let mut outcome = private::run(47, story);
        assert!(outcome.machine.faulted);
        assert_eq!(private::facts(&outcome.procs), &[Fact::Keep(1), Fact::Granted(0), Fact::Closed]);
        assert_eq!(outcome.machine.record().generation, 0);
        outcome.machine.finish();
    }
}

#[test]
fn a_close_waits_for_a_private_keep_and_abort_settles_its_cancelled_file_work() {
    let mut closed = private::run(53, Story::CloseKeeping);
    assert_eq!(private::facts(&closed.procs), &[Fact::Keep(1), Fact::Granted(1), Fact::Closed]);
    closed.machine.finish();
    let mut aborted = private::run(53, Story::AbortKeeping);
    assert_eq!(
        private::facts(&aborted.procs),
        &[Fact::Keep(1), Fact::Failed(Failure::Exchange(skein_oauth::Failure::Cancelled)), Fact::Closed,]
    );
    aborted.machine.finish();
}

#[test]
fn private_file_socket_and_terminal_order_replay() {
    let mut first = private::run(59, Story::Refresh);
    let mut second = private::run(59, Story::Refresh);
    assert_eq!(first.trace, second.trace);
    assert_eq!(private::facts(&first.procs), private::facts(&second.procs));
    first.machine.finish();
    second.machine.finish();
}

#[test]
fn a_sign_in_replaces_missing_and_unreadable_private_records() {
    for story in [Story::Missing, Story::Unreadable] {
        let mut outcome = skein_oauth_accounts_world::sign_in::run_private(61, story);
        assert_eq!(outcome.machine.record().generation, 1);
        assert_eq!(outcome.machine.renames, 1);
        outcome.machine.finish();
    }
}

#[test]
fn unsafe_loading_fails_a_sign_in_before_any_visit() {
    for story in [Story::RootMode, Story::FileMode, Story::FileOwner, Story::FileLink, Story::FileLinks] {
        let mut outcome = skein_oauth_accounts_world::sign_in::run_private(67, story);
        assert_eq!(outcome.machine.reads, 0);
        assert_eq!(outcome.machine.writes, 0);
        outcome.machine.finish();
    }
}

#[test]
fn a_refresh_cut_at_each_private_sync_and_rename_recovers_the_whole_old_or_new_record() {
    use skein_oauth_accounts_world::private_cuts;
    use skein_world::Cut;
    let mut old = false;
    let mut new = false;
    for seed in [0, 1, 17] {
        let mut uncut = private_cuts::world(seed).run();
        let points: Vec<u32> = uncut
            .trace
            .iter()
            .filter(|entry| entry.pid.raw() == 1 && matches!(entry.event, skein_sim::Event::Submit { .. }))
            .enumerate()
            .filter(|(_, entry)| {
                matches!(
                    entry.event,
                    skein_sim::Event::Submit {
                        kind: skein_sim::Summary::Sync { .. } | skein_sim::Summary::Rename { .. },
                        ..
                    }
                )
            })
            .flat_map(|(index, _)| {
                [
                    u32::try_from(index).expect("bounded submissions"),
                    u32::try_from(index + 1).expect("bounded submissions"),
                ]
            })
            .collect();
        assert_eq!(points.len(), 6, "the temporary sync, rename and directory sync");
        uncut.machine.finish();
        for point in points {
            for cut in [Cut::Kill, Cut::PowerLoss] {
                let mut world = private_cuts::world(seed);
                world.cut(1, point, cut);
                let mut outcome = world.run();
                let generation = private_cuts::recovered(&outcome.procs);
                assert!(generation <= 1);
                assert_eq!(outcome.machine.cuts, 1);
                assert_eq!(outcome.machine.record().generation, generation);
                old |= generation == 0;
                new |= generation == 1;
                outcome.machine.finish();
            }
        }
    }
    assert!(old && new, "cuts cover both whole generations");
}

#[test]
fn failed_and_stalled_loads_end_their_grants_and_a_persons_fix_is_loaded_without_restarting() {
    for (story, why) in [
        (Story::LoadFailed, Unloaded::Failed(skein_io::kernel::Error::Other(5))),
        (Story::LoadStalled, Unloaded::Stalled),
    ] {
        let mut outcome = private::run(71, story);
        assert_eq!(
            private::facts(&outcome.procs),
            &[Fact::Failed(Failure::Unloaded { at: Place::File, why }), Fact::Granted(0), Fact::Closed]
        );
        assert!(outcome.machine.faulted);
        outcome.machine.finish();
    }
    let mut fixed = private::run(73, Story::Fixed);
    assert_eq!(
        private::facts(&fixed.procs),
        &[
            Fact::Failed(Failure::Unloaded {
                at: Place::File,
                why: Unloaded::Failed(skein_io::kernel::Error::Other(5))
            }),
            Fact::Granted(0),
            Fact::Closed
        ]
    );
    assert!(fixed.machine.reads >= 2);
    assert_eq!(fixed.machine.writes, 0);
    fixed.machine.finish();
}

#[test]
fn a_private_keep_conflict_preserves_the_other_writers_bytes_and_lends_nothing_new() {
    let mut outcome = private::run(79, Story::Conflict);
    assert!(outcome.machine.faulted);
    assert_eq!(private::facts(&outcome.procs), &[Fact::Keep(1), Fact::Granted(0), Fact::Closed]);
    assert!(outcome.machine.contents().starts_with(b"another writer"));
    assert_eq!(outcome.machine.renames, 0);
    outcome.machine.finish();
}

#[test]
fn failed_stalled_and_oversized_loading_fail_a_sign_in_before_its_visit() {
    for story in [Story::LoadFailed, Story::LoadStalled, Story::TooLarge] {
        let mut outcome = skein_oauth_accounts_world::sign_in::run_private(83, story);
        assert_eq!(outcome.machine.writes, 0);
        if story == Story::TooLarge {
            assert_eq!(outcome.machine.reads, 0);
        }
        outcome.machine.finish();
    }
}

#[test]
fn close_and_abort_during_loading_settle_the_waiting_grant_before_closed() {
    let mut closed = private::run(89, Story::CloseLoading);
    assert_eq!(private::facts(&closed.procs), &[Fact::Granted(0), Fact::Closed]);
    closed.machine.finish();
    let mut aborted = private::run(89, Story::AbortLoading);
    assert_eq!(
        private::facts(&aborted.procs),
        &[Fact::Failed(Failure::Exchange(skein_oauth::Failure::Cancelled)), Fact::Closed]
    );
    aborted.machine.finish();
}

#[test]
fn a_queued_descriptor_close_is_retained_after_its_deadline_and_cancel() {
    use skein_io::kernel::{Complete, Done, Fd, Op};
    use skein_lib::{Duration, Time, Token};
    use skein_world::files::Driver;
    let mut files = Driver::new(Fd::new(7), 1, 1024, Duration::from_secs(1), 1234);
    files.close_root(Time::ZERO);
    files.cancel(Token::new(u64::MAX));
    files.progress(Time::from_nanos(Duration::from_secs(2).as_nanos()));
    let submitted = files.take_submit().expect("close remains admitted");
    assert!(matches!(submitted.kind, Op::Close { fd } if fd == Fd::new(7)));
    files.up(Complete { op: submitted.op, kind: submitted.kind, result: Ok(Done::Nothing) });
    assert!(matches!(files.take_event(), Some(skein_io::file::Event::Closed { .. })));
    assert!(files.is_empty());
}
