//! The event stream writer's machine world (testing-strategy.md, 2.4),
//! focused: a few seeds of each neighbour's behaviour, what was written
//! read back by the reference reader and the reader's own world. The
//! sweeps over many seeds are in `fuzzy_writer.rs`.

use std::collections::BTreeSet;

use skein_http::sse::writer::{Event, Limits, Outgoing, Waiting};
use skein_http_world::writer_world::{self, Item, Settings};
use skein_lib::Rng;
use skein_lib::stream::Fault;

const LIMITS: Limits = Limits { event: 2048, chunk: 16 };

fn items(seed: u64) -> Vec<Item> {
    writer_world::items(&mut Rng::new(seed))
}

#[test]
fn events_written_in_pieces_within_the_room_granted_read_back_as_written() {
    let mut pieces = 0;
    for seed in 0..60 {
        let items = items(seed);
        let mut rng = Rng::new(seed);
        let settings = Settings::calm(&mut rng, LIMITS);
        let run = writer_world::check(&items, &settings, seed);
        assert!(run.finished, "seed {seed}");
        pieces += usize::from(run.fell.pieces);
    }
    assert!(pieces > 30, "events went down in pieces in {pieces} runs");
}

#[test]
fn neighbours_that_fail_or_close_at_any_moment_leave_a_consistent_run() {
    for seed in 0..80 {
        let items = items(seed);
        let mut rng = Rng::new(seed);
        let limits = writer_world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits, 400);
        let _ = writer_world::check(&items, &settings, seed);
    }
}

#[test]
fn the_stream_s_end_changes_nothing_and_room_still_comes_after_it() {
    let mut after = 0;
    for seed in 0..40 {
        let items = items(seed);
        let mut rng = Rng::new(seed);
        let settings = Settings { end: 200, ..Settings::calm(&mut rng, LIMITS) };
        let run = writer_world::check(&items, &settings, seed);
        assert!(run.finished, "seed {seed}");
        after += usize::from(run.fell.room_after_end);
    }
    assert!(after > 10, "room came after the end in {after} runs");
}

#[test]
fn the_stream_failing_fails_the_item_being_written_or_the_next() {
    let items: Vec<Item> = (0..6)
        .map(|n| Item::Event(Outgoing { name: Box::new([]), data: vec![b'a' + n; 40].into(), id: None, retry: None }))
        .collect();
    let mut failed = 0;
    for at in 0..200 {
        let mut rng = Rng::new(at);
        let settings = Settings { failure: Some((at % 80, Fault::Reset)), ..Settings::calm(&mut rng, LIMITS) };
        let run = writer_world::check(&items, &settings, at);
        failed += usize::from(run.answered.iter().any(|(_, answer)| *answer == Some(Event::Failed(Fault::Reset))));
    }
    assert!(failed > 50, "most failures land on an item: {failed}");
}

#[test]
fn the_side_above_closes_in_every_state() {
    let mut seen = BTreeSet::new();
    for at in 0..200 {
        let items = items(at % 9);
        let mut rng = Rng::new(at);
        let settings = Settings { close: Some(at % 60), ..Settings::calm(&mut rng, LIMITS) };
        let run = writer_world::check(&items, &settings, at);
        seen.insert(format!("{:?}", run.closed_while));
    }
    let every: BTreeSet<String> =
        [Waiting::Above, Waiting::Room, Waiting::Close].iter().map(|waiting| format!("{waiting:?}")).collect();
    assert_eq!(seen, every, "closed while waiting for each thing");
}

#[test]
fn a_seed_replays_to_the_same_run() {
    for seed in 0..20 {
        let items = items(seed);
        let mut rng = Rng::new(seed);
        let limits = writer_world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits, 400);
        assert_eq!(
            writer_world::run(&items, &settings, seed),
            writer_world::run(&items, &settings, seed),
            "seed {seed}"
        );
    }
}
