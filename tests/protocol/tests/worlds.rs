//! The protocol worlds (testing-strategy.md, 2.5), focused: each thing
//! stacking adds over a few seeds, the referee holding each to what the
//! other end's top sent. The sweeps over many seeds are in
//! `fuzzy_worlds.rs`.

use skein_http::client::Reuse;
use skein_lib::Rng;
use skein_protocol_world::client_end::Outcome;
use skein_protocol_world::scenario;
use skein_protocol_world::world::{self, Settings};

#[test]
fn an_llm_s_stream_goes_from_the_server_s_top_to_the_client_s_top() {
    let mut ids = 0;
    for seed in 0..12 {
        let mut rng = Rng::new(seed);
        let scenario = scenario::stream(&mut rng);
        let settings = Settings::drawn(&mut rng);
        let run = world::run(&scenario, &settings, seed);
        assert_eq!(run.client.events.len(), scenario.answers[0].len(), "seed {seed}");
        ids += run.fell.ids;
    }
    assert!(ids > 0, "events with an id or a reconnection time, checked against the reader's face");
}

#[test]
fn a_connection_carries_two_or_three_calls_through_both_stacks() {
    for seed in 0..8 {
        let mut rng = Rng::new(600 + seed);
        let scenario = scenario::several(&mut rng);
        let settings = Settings::drawn(&mut rng);
        let run = world::run(&scenario, &settings, seed);
        let calls = scenario.requests.len();
        assert!((2..=3).contains(&calls), "seed {seed}");
        assert_eq!(run.client.outcomes, vec![Outcome::Done(Reuse::Keep); calls], "seed {seed}");
        assert_eq!(run.server.requests, scenario.requests, "seed {seed}");
        assert_eq!(run.fell.calls, calls, "seed {seed}");
    }
}

#[test]
fn a_slow_reader_at_the_client_s_top_stops_the_writer_at_the_server_s() {
    for seed in 0..3 {
        let mut rng = Rng::new(100 + seed);
        let scenario = scenario::slow_reader(&mut rng);
        let settings = Settings::drawn(&mut rng);
        let run = world::run(&scenario, &settings, seed);
        assert!(run.fell.writer_held, "seed {seed}: an event waited while the reader stopped");
        assert!(run.fell.most_in_flight <= settings.in_flight(), "seed {seed}: held to the caps between the ends");
        assert_eq!(run.client.events.len(), scenario.answers[0].len(), "seed {seed}: and all came once it read again");
    }
}

#[test]
fn a_slow_consumer_at_the_server_s_top_stops_the_upload_at_the_client_s() {
    for seed in 0..3 {
        let mut rng = Rng::new(700 + seed);
        let scenario = scenario::slow_consumer(&mut rng);
        let settings = Settings::drawn(&mut rng);
        let run = world::run(&scenario, &settings, seed);
        assert!(run.fell.upload_held, "seed {seed}: the upload waited while the server's user stopped");
        assert_eq!(run.server.requests, scenario.requests, "seed {seed}: and all came once it read again");
    }
}

#[test]
fn a_response_that_arrives_mid_upload_stops_the_upload_and_is_read() {
    for seed in 0..6 {
        let mut rng = Rng::new(200 + seed);
        let scenario = scenario::early(&mut rng);
        let settings = Settings::drawn(&mut rng);
        let run = world::run(&scenario, &settings, seed);
        assert!(run.fell.upload_stopped, "seed {seed}: the response came before the body was sent");
        assert!(run.client.uploaded < scenario.client.bodies[0].len(), "seed {seed}");
    }
}

#[test]
fn one_end_closing_while_the_other_sends_leaves_both_settled() {
    let mut reset = 0;
    for seed in 0..30 {
        let mut rng = Rng::new(300 + seed);
        let scenario = scenario::partway(&mut rng);
        let settings = Settings::drawn(&mut rng);
        let run = world::run(&scenario, &settings, seed);
        reset += usize::from(run.fell.reset);
    }
    assert!(reset > 3, "a closed end's peer met a reset in {reset} runs");
}

#[test]
fn the_wire_resetting_at_any_moment_leaves_both_settled() {
    for seed in 0..20 {
        let mut rng = Rng::new(400 + seed);
        let scenario = scenario::stream(&mut rng);
        let mut settings = Settings::drawn(&mut rng);
        settings.reset = Some(rng.below(400));
        let _ = world::run(&scenario, &settings, seed);
    }
}

#[test]
fn a_seed_replays_to_the_same_run() {
    for seed in 0..4 {
        let mut rng = Rng::new(500 + seed);
        let scenario = scenario::partway(&mut rng);
        let settings = Settings::drawn(&mut rng);
        assert_eq!(world::run(&scenario, &settings, seed), world::run(&scenario, &settings, seed), "seed {seed}");
    }
}
