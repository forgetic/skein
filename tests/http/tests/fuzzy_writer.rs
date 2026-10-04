//! The event stream writer's machine world, swept (testing-strategy.md, 8;
//! http.md, 6): many runs of generated events and comments, now and then
//! flawed, under limits and neighbours drawn from each seed, what was
//! written read back by the reference reader and the reader's own world.
//! This stands in for the fuzz target, which waits for a nightly
//! toolchain.
//!
//! A sweep asserts that what it injects fell (testing-strategy.md, 3):
//! each answer and each refusal, an event in pieces, the stream's end and
//! room after it, a failure idle and while writing, and a close in each
//! state.

use std::collections::BTreeMap;

use skein_http_world::writer_world::{self, Run, Settings};
use skein_lib::Rng;

const ROUNDS: u64 = 5_000;

#[derive(Default)]
struct Seen(BTreeMap<String, u32>);

impl Seen {
    fn note(&mut self, what: String) {
        *self.0.entry(what).or_default() += 1;
    }

    fn record(&mut self, run: &Run) {
        for (_, answer) in &run.answered {
            match answer {
                Some(answer) => self.note(format!("{answer:?}")),
                None => self.note("closed before the answer".into()),
            }
        }
        if run.finished {
            self.note("finished".into());
        }
        self.note(format!("closed while waiting for {:?}", run.closed_while));
        let fell = run.fell;
        for (flag, what) in [
            (fell.pieces, "an event in pieces"),
            (fell.ended, "the stream ended"),
            (fell.room_after_end, "room after the end"),
            (fell.failed_idle, "a failure while idle"),
        ] {
            if flag {
                self.note(what.into());
            }
        }
        if let Some(waiting) = fell.failed_while {
            self.note(format!("failed while waiting for {waiting:?}"));
        }
    }
}

#[test]
fn generated_events_are_written_and_read_back_as_written_whatever_the_neighbours() {
    let mut tally = Seen::default();
    for round in 0..ROUNDS {
        let seed = 0x00a0_0000 + round;
        let mut rng = Rng::new(seed);
        let items = writer_world::items(&mut rng);
        let limits = writer_world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits, 400);
        let run = writer_world::check(&items, &settings, seed);
        tally.record(&run);
    }
    for what in [
        "Sent",
        "Refused(Name)",
        "Refused(Id)",
        "Refused(Comment)",
        "Refused(TooLong)",
        "Failed(Reset)",
        "Failed(Invalid)",
        "Failed(Other)",
        "closed before the answer",
        "finished",
        "closed while waiting for Above",
        "closed while waiting for Room",
        "closed while waiting for Close",
        "an event in pieces",
        "the stream ended",
        "room after the end",
        "a failure while idle",
        "failed while waiting for Above",
        "failed while waiting for Room",
        "failed while waiting for Close",
        "failed while waiting for Nothing",
    ] {
        assert!(tally.0.contains_key(what), "{what} fell: {:#?}", tally.0);
    }
}
