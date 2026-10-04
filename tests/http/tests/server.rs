//! The server's machine world (testing-strategy.md, 2.4), focused: a few
//! seeds of each neighbour's behaviour, each exchange checked against the
//! reference reader and the test's own writer. The sweeps over many seeds
//! are in `fuzzy_server.rs`.

use std::collections::BTreeSet;

use skein_http::server::{Body, Error, Limits, Response, Reuse, Waiting};
use skein_http_world::reference::{self, RequestEnding};
use skein_http_world::requests;
use skein_http_world::server_world::{self, Outcome, Plan, Reads, Settings, When};
use skein_lib::Rng;
use skein_lib::stream::Fault;

const LIMITS: Limits = Limits { head: 2048, headers: 32, body: 4096, read: 64, response: 1024, send: 32 };

/// `count` requests and the side above's plans for them, from `seed`.
fn scenario(seed: u64, count: usize) -> (Vec<u8>, Vec<Plan>) {
    let mut rng = Rng::new(seed);
    let mut client = Vec::new();
    let mut plans = Vec::new();
    for _ in 0..count {
        client.extend(requests::request(&mut rng));
        plans.push(server_world::plan(&mut rng));
    }
    (client, plans)
}

fn post(body: &[u8], fields: &str) -> Vec<u8> {
    let mut request =
        format!("POST /v1/messages HTTP/1.1\r\nHost: h\r\n{fields}Content-Length: {}\r\n\r\n", body.len()).into_bytes();
    request.extend_from_slice(body);
    request
}

fn plan(when: When, status: u16, reply: &[u8]) -> Plan {
    let response = Response { status, headers: Box::new([]), body: Body::Length(reply.len() as u64), close: false };
    Plan { response, reply: reply.to_vec(), when }
}

#[test]
fn requests_cut_and_delivered_at_random_are_served_whole() {
    for seed in 0..40 {
        let (client, plans) = scenario(seed, 3);
        let mut rng = Rng::new(seed);
        let settings = Settings::calm(&mut rng, LIMITS);
        let run = server_world::check(&client, &plans, &settings, seed);
        assert!(run.seen.iter().all(|seen| seen.outcome.is_some()), "seed {seed}: {run:?}");
    }
}

#[test]
fn neighbours_that_end_early_fail_or_close_at_any_moment_leave_a_consistent_run() {
    for seed in 0..60 {
        let (mut client, plans) = scenario(seed, 2);
        let mut rng = Rng::new(seed);
        if rng.chance(300) {
            client = skein_http_world::generate::mutate(&mut rng, &client);
        } else if rng.chance(400) {
            client = requests::corrupt(&mut rng, &client);
        }
        let limits = server_world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits, client.len());
        let _ = server_world::check(&client, &plans, &settings, seed);
    }
}

#[test]
fn a_connection_carries_one_request_after_another_pipelined_or_not() {
    let mut reused = 0;
    let mut pipelined = 0;
    for seed in 0..30 {
        let (client, plans) = scenario(2000 + seed, 4);
        let mut rng = Rng::new(seed);
        let settings = Settings::calm(&mut rng, LIMITS);
        let run = server_world::check(&client, &plans, &settings, seed);
        reused += usize::from(run.fell.reused);
        pipelined += usize::from(run.fell.pipelined);
    }
    assert!(reused > 10, "connections carried several requests in {reused} runs");
    assert!(pipelined > 3, "requests came pipelined in {pipelined} runs");
}

#[test]
fn a_client_that_waits_for_a_continue_gets_one_once_the_body_is_read() {
    let body = vec![b'b'; 300];
    let client = post(&body, "Expect: 100-continue\r\n");
    let mut continued = 0;
    for seed in 0..30 {
        let mut rng = Rng::new(seed);
        let settings = Settings {
            patience: 1_000_000,
            idle_end: 0,
            withdraw: 0,
            reads: Reads::Bytes,
            ..Settings::calm(&mut rng, LIMITS)
        };
        let run = server_world::check(&client, &[plan(When::AfterBody, 200, b"ok")], &settings, seed);
        let seen = &run.seen[0];
        assert_eq!(seen.outcome, Some(Outcome::Done(Reuse::Keep)), "seed {seed}: {seen:?}");
        assert_eq!(seen.body, body, "seed {seed}");
        assert!(seen.sent.starts_with(b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\n"), "seed {seed}");
        continued += 1;
    }
    assert_eq!(continued, 30);
}

#[test]
fn a_response_given_first_to_a_client_that_waits_for_a_continue_is_its_answer() {
    let client = post(&[b'b'; 300], "Expect: 100-continue\r\n");
    for seed in 0..20 {
        let mut rng = Rng::new(seed);
        let settings = Settings { patience: 1_000_000, ..Settings::calm(&mut rng, LIMITS) };
        let run = server_world::check(&client, &[plan(When::First, 417, b"")], &settings, seed);
        let seen = &run.seen[0];
        assert_eq!(seen.outcome, Some(Outcome::Done(Reuse::Close)), "seed {seed}: {seen:?}");
        assert!(seen.body.is_empty(), "the body never came");
        assert!(seen.sent.starts_with(b"HTTP/1.1 417 Expectation Failed\r\n"), "seed {seed}");
    }
}

#[test]
fn a_response_given_before_the_body_is_read_gives_it_up_unless_it_is_discarded() {
    let client = [post(&[b'x'; 200], ""), post(b"second", "")].concat();
    let mut gave_up = 0;
    for seed in 0..30 {
        let mut rng = Rng::new(seed);
        let settings = Settings { withdraw: 0, idle_end: 0, reads: Reads::Bytes, ..Settings::calm(&mut rng, LIMITS) };
        let plans = [plan(When::Partway(1), 413, b"too big"), plan(When::AfterBody, 200, b"")];
        let run = server_world::check(&client, &plans, &settings, seed);
        let seen = &run.seen[0];
        if seen.outcome == Some(Outcome::Done(Reuse::Close)) {
            gave_up += 1;
            assert_eq!(run.seen.len(), 1, "seed {seed}: the connection is not used again");
        }
        let plans = [plan(When::Discarding, 413, b"too big"), plan(When::AfterBody, 200, b"")];
        let run = server_world::check(&client, &plans, &settings, seed);
        assert_eq!(run.seen[0].outcome, Some(Outcome::Done(Reuse::Keep)), "seed {seed}: the head waited: {run:?}");
        assert_eq!(run.seen[1].body, b"second", "seed {seed}: {run:?}");
    }
    assert!(gave_up > 20, "the body was given up in {gave_up} runs");
}

#[test]
fn hostile_requests_are_rejected_with_the_server_s_own_answer() {
    let mut rejected = BTreeSet::new();
    for seed in 0..400 {
        let (client, plans) = scenario(seed, 1);
        let mut rng = Rng::new(seed);
        let client = requests::corrupt(&mut rng, &client);
        let settings = Settings::calm(&mut rng, LIMITS);
        let run = server_world::check(&client, &plans, &settings, seed);
        if let Some(Outcome::Failed(Error::Rejected(rejection))) = run.seen[0].outcome {
            rejected.insert(format!("{rejection:?}"));
        }
    }
    let expected: BTreeSet<String> = [
        "RequestLine",
        "Version",
        "Method",
        "Header",
        "HeadTooLong",
        "TooManyHeaders",
        "Host",
        "Framing",
        "Coding",
        "TargetTooLong",
        "BodyTooLong",
    ]
    .iter()
    .map(|name| (*name).to_string())
    .collect();
    assert_eq!(rejected, expected);
}

#[test]
fn the_stream_ending_after_any_byte_ends_or_fails_the_request_as_the_reference_reads_it() {
    let (client, plans) = scenario(78, 2);
    for cut in 0..=client.len() {
        let mut rng = Rng::new(cut as u64);
        let settings = Settings { cut: Some(cut), ..Settings::calm(&mut rng, LIMITS) };
        let _ = server_world::check(&client, &plans, &settings, cut as u64);
    }
}

#[test]
fn the_stream_failing_at_any_moment_fails_the_request_in_progress() {
    let (client, plans) = scenario(5, 2);
    let mut failed = 0;
    for at in 0..300 {
        let mut rng = Rng::new(at);
        let settings = Settings { failure: Some((at % 60, Fault::Reset)), ..Settings::calm(&mut rng, LIMITS) };
        let run = server_world::check(&client, &plans, &settings, at);
        if run.seen.iter().any(|seen| seen.outcome == Some(Outcome::Failed(Error::Stream(Fault::Reset)))) {
            failed += 1;
        }
    }
    assert!(failed > 50, "most failures land in a request: {failed}");
}

#[test]
fn the_side_above_closes_in_every_state() {
    let mut seen = BTreeSet::new();
    for at in 0..400 {
        let (client, plans) = scenario(at % 7, 2);
        let mut rng = Rng::new(at);
        let settings = Settings { close: Some(at % 150), ..Settings::calm(&mut rng, LIMITS) };
        let run = server_world::check(&client, &plans, &settings, at);
        seen.insert(format!("{:?}", run.closed_while));
    }
    let every: BTreeSet<String> =
        [Waiting::Next, Waiting::Room, Waiting::Request, Waiting::Body, Waiting::Above, Waiting::Close]
            .iter()
            .map(|waiting| format!("{waiting:?}"))
            .collect();
    assert_eq!(seen, every, "closed while waiting for each thing");
}

#[test]
fn a_seed_replays_to_the_same_run() {
    for seed in 0..20 {
        let (client, plans) = scenario(seed, 2);
        let mut rng = Rng::new(seed);
        let client = skein_http_world::generate::mutate(&mut rng, &client);
        let limits = server_world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits, client.len());
        assert_eq!(
            server_world::run(&client, &plans, &settings, seed),
            server_world::run(&client, &plans, &settings, seed),
            "seed {seed}"
        );
    }
}

#[test]
fn the_reference_reader_reads_what_the_generator_wrote() {
    for seed in 0..300 {
        let mut rng = Rng::new(seed);
        let request = requests::request(&mut rng);
        let read = reference::request(&request, &LIMITS);
        assert_eq!(read.ending, RequestEnding::Whole, "seed {seed}: {}", request.escape_ascii());
        assert_eq!(read.used, request.len(), "seed {seed}");
    }
}
