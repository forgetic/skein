//! The protocol worlds, swept (testing-strategy.md, 8): many seeds of each
//! scenario, under caps between the ends drawn down to the least the
//! stacks allow and a wire of every pace, now and then reset, each run
//! held by the referee to what the other end's top sent.
//!
//! The sweep asserts that what it aims at fell (testing-strategy.md, 3): a
//! writer held back by a slow reader, an upload stopped by a response that
//! came first, a writer that heard its stream fail, a reset, and each
//! outcome at each end.

use std::collections::BTreeMap;

use skein_lib::Rng;
use skein_protocol_world::scenario;
use skein_protocol_world::world::{self, Expect, Run, Settings};

const ROUNDS: u64 = 400;

#[derive(Default)]
struct Seen(BTreeMap<String, u32>);

impl Seen {
    fn note(&mut self, what: String) {
        *self.0.entry(what).or_default() += 1;
    }

    fn record(&mut self, run: &Run) {
        let fell = run.fell;
        for (flag, what) in [
            (fell.writer_held, "a writer held back by a slow reader"),
            (fell.upload_stopped, "an upload stopped by a response first"),
            (fell.writer_failed, "a writer that heard its stream fail"),
            (fell.reset, "a reset"),
        ] {
            if flag {
                self.note(what.into());
            }
        }
        match run.client.outcome {
            Some(outcome) => self.note(format!("client {outcome:?}")),
            None => self.note("client closed first".into()),
        }
        for outcome in &run.server.outcomes {
            self.note(format!("server {outcome:?}"));
        }
    }
}

#[test]
fn every_scenario_holds_whatever_the_caps_and_the_wire() {
    let mut tally = Seen::default();
    for round in 0..ROUNDS {
        let seed = 0x00b0_0000 + round;
        let mut rng = Rng::new(seed);
        // The slow reader, ten times as long a run, a tenth of them.
        let mut scenario = match rng.below(10) {
            0 => scenario::slow_reader(&mut rng),
            1 | 2 => scenario::early(&mut rng),
            3..=6 => scenario::partway(&mut rng),
            _ => scenario::stream(&mut rng),
        };
        let mut settings = Settings::drawn(&mut rng);
        if rng.chance(500) {
            // The least each stack allows below it.
            settings.client_intake = skein_http::client::largest_read(&settings.client.client);
            settings.client_output = skein_http::client::largest_room(&settings.client.client);
            settings.server_intake = skein_http::server::largest_read(&settings.server.server);
            settings.server_output = skein_http::server::largest_room(&settings.server.server);
        }
        if rng.chance(100) {
            settings.reset = Some(rng.below(1_000));
            scenario.expect = Expect::Partway;
        }
        let run = world::run(&scenario, &settings, seed);
        tally.record(&run);
    }
    for what in [
        "a writer held back by a slow reader",
        "an upload stopped by a response first",
        "a writer that heard its stream fail",
        "a reset",
        "client Done(Keep)",
        "client Done(Close)",
        "client closed first",
        "server Done(Keep)",
        "server Done(Close)",
        "server Ended",
        "server Failed(Truncated)",
        "server Failed(Stream(Reset))",
    ] {
        assert!(tally.0.contains_key(what), "{what} fell: {:#?}", tally.0);
    }
}
