//! The client's machine world (testing-strategy.md, 2.4), focused: a few
//! seeds of each neighbour's behaviour, each exchange checked against the
//! reference reader. The sweeps over many seeds are in `fuzzy_client.rs`.

use std::collections::BTreeSet;

use skein_http::client::{Body, Call, Error, Limits, Method, Reuse, Waiting};
use skein_http_world::client_world::{self, Exchange, Reads, Settings};
use skein_http_world::generate;
use skein_http_world::reference::Outcome;
use skein_lib::Rng;
use skein_lib::stream::Fault;

const LIMITS: Limits = Limits { request: 1024, head: 2048, headers: 32, read: 64, send: 32 };

/// `count` calls and the server's responses to them, from `seed`.
fn scenario(seed: u64, count: usize) -> (Vec<Exchange>, Vec<u8>) {
    let mut rng = Rng::new(seed);
    let mut exchanges = Vec::new();
    let mut server = Vec::new();
    for index in 0..count {
        let (call, upload) = generate::call(&mut rng);
        let body = generate::body(&mut rng);
        server.extend(generate::response(&mut rng, call.method, &body, index + 1 == count));
        exchanges.push(Exchange { call, upload });
    }
    (exchanges, server)
}

#[test]
fn exchanges_cut_and_delivered_at_random_decode_whole() {
    for seed in 0..40 {
        let (exchanges, server) = scenario(seed, 3);
        let mut rng = Rng::new(seed);
        let settings = Settings::calm(&mut rng, LIMITS);
        let run = client_world::check(&exchanges, &server, &settings, seed);
        let last = run.exchanges.last().expect("an exchange");
        assert!(last.outcome.is_some(), "seed {seed}: {run:?}");
    }
}

#[test]
fn neighbours_that_end_early_fail_or_close_at_any_moment_leave_a_consistent_run() {
    for seed in 0..60 {
        let (exchanges, mut server) = scenario(seed, 2);
        let mut rng = Rng::new(seed);
        if rng.chance(400) {
            server = generate::mutate(&mut rng, &server);
        }
        let limits = client_world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits, server.len());
        let _ = client_world::check(&exchanges, &server, &settings, seed);
    }
}

#[test]
fn a_connection_carries_one_exchange_after_another() {
    let mut reused = 0;
    for seed in 0..20 {
        let (exchanges, server) = scenario(1000 + seed, 4);
        let mut rng = Rng::new(seed);
        let settings = Settings::calm(&mut rng, LIMITS);
        let run = client_world::check(&exchanges, &server, &settings, seed);
        if run.fell.reused {
            reused += 1;
        }
    }
    assert!(reused > 5, "connections carried several exchanges in {reused} runs");
}

#[test]
fn a_response_that_comes_mid_upload_stops_it_and_the_exchange_ends_with_it() {
    let mut stopped = 0;
    for seed in 0..40 {
        let call = Call {
            method: Method::Post,
            target: b"/upload".to_vec().into(),
            headers: vec![skein_http::Header { name: b"Host".to_vec().into(), value: b"h".to_vec().into() }].into(),
            body: Body::Length(400),
            close: false,
        };
        let exchanges = [Exchange { call, upload: vec![b'u'; 400] }];
        let server = b"HTTP/1.1 413 Content Too Large\r\nContent-Length: 5\r\nConnection: close\r\n\r\nlarge".to_vec();
        let mut rng = Rng::new(seed);
        let settings = Settings {
            grant: 50,
            arrival: 1000,
            reads: Reads::Bytes,
            discard: 0,
            withdraw: 0,
            ..Settings::calm(&mut rng, LIMITS)
        };
        let run = client_world::check(&exchanges, &server, &settings, seed);
        let seen = &run.exchanges[0];
        assert_eq!(seen.outcome, Some(Outcome::Done(Reuse::Close)), "seed {seed}");
        assert_eq!(seen.body, b"large");
        if seen.upload_failed == Some(Fault::Other) {
            stopped += 1;
            assert!(seen.sent.len() < 400, "seed {seed}: the upload stopped");
        }
    }
    assert!(stopped > 20, "the response came first in {stopped} runs");
}

#[test]
fn a_server_that_answers_only_once_it_has_the_whole_request_is_answered() {
    for seed in 0..20 {
        let mut rng = Rng::new(seed);
        let mut exchanges = Vec::new();
        let mut server = Vec::new();
        for index in 0..3 {
            let body = generate::body(&mut rng);
            let call = Call {
                method: Method::Post,
                target: b"/v1/messages".to_vec().into(),
                headers: vec![skein_http::Header { name: b"Host".to_vec().into(), value: b"h".to_vec().into() }].into(),
                body: Body::Length(200),
                close: false,
            };
            server.extend(generate::response(&mut rng, Method::Post, &body, index == 2));
            exchanges.push(Exchange { call, upload: vec![b'u'; 200] });
        }
        let settings = Settings { patient: true, ..Settings::calm(&mut rng, LIMITS) };
        let run = client_world::check(&exchanges, &server, &settings, seed);
        for seen in &run.exchanges {
            assert!(seen.upload_failed.is_none(), "seed {seed}: no response came before the request was whole");
            assert!(matches!(seen.outcome, Some(Outcome::Done(_))), "seed {seed}: {:?}", seen.outcome);
        }
    }
}

#[test]
fn the_stream_ending_after_any_byte_fails_or_ends_the_exchange_as_the_reference_reads_it() {
    let (exchanges, server) = scenario(77, 2);
    for cut in 0..=server.len() {
        let mut rng = Rng::new(cut as u64);
        let settings = Settings { cut: Some(cut), ..Settings::calm(&mut rng, LIMITS) };
        let _ = client_world::check(&exchanges, &server, &settings, cut as u64);
    }
}

#[test]
fn the_stream_failing_at_any_moment_fails_the_exchange_in_progress() {
    let (exchanges, server) = scenario(5, 2);
    let mut failed = 0;
    for at in 0..300 {
        let mut rng = Rng::new(at);
        let settings = Settings { failure: Some((at, Fault::Reset)), ..Settings::calm(&mut rng, LIMITS) };
        let run = client_world::check(&exchanges, &server, &settings, at);
        if run.exchanges.iter().any(|seen| seen.outcome == Some(Outcome::Failed(Error::Stream(Fault::Reset)))) {
            failed += 1;
        }
    }
    assert!(failed > 50, "most failures land in an exchange: {failed}");
}

#[test]
fn the_side_above_closes_in_every_state() {
    let mut seen = BTreeSet::new();
    for at in 0..400 {
        let (exchanges, server) = scenario(at % 7, 2);
        let mut rng = Rng::new(at);
        let settings = Settings { close: Some(at % 120), ..Settings::calm(&mut rng, LIMITS) };
        let run = client_world::check(&exchanges, &server, &settings, at);
        seen.insert(format!("{:?}", run.closed_while));
    }
    let every: BTreeSet<String> =
        [Waiting::Call, Waiting::Room, Waiting::Response, Waiting::Body, Waiting::Above, Waiting::Close]
            .iter()
            .map(|waiting| format!("{waiting:?}"))
            .collect();
    assert_eq!(seen, every, "closed while waiting for each thing");
}

#[test]
fn a_seed_replays_to_the_same_run() {
    for seed in 0..20 {
        let (exchanges, server) = scenario(seed, 2);
        let mut rng = Rng::new(seed);
        let server = generate::mutate(&mut rng, &server);
        let limits = client_world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits, server.len());
        assert_eq!(
            client_world::run(&exchanges, &server, &settings, seed),
            client_world::run(&exchanges, &server, &settings, seed),
            "seed {seed}"
        );
    }
}

#[test]
fn the_reference_reader_reads_what_the_generator_wrote() {
    for seed in 0..200 {
        let mut rng = Rng::new(seed);
        let method = [Method::Get, Method::Head, Method::Post][usize::try_from(rng.below(3)).unwrap()];
        let body = generate::body(&mut rng);
        let response = generate::response(&mut rng, method, &body, true);
        let read = skein_http_world::reference::response(&response, method, false, &LIMITS);
        assert!(matches!(read.outcome, Outcome::Done(_)), "seed {seed}: {read:?} of {}", response.escape_ascii());
        let head = read.head.expect("a head");
        let bodyless = method == Method::Head || head.status == 204 || head.status == 304;
        assert_eq!(read.body, if bodyless { &[][..] } else { &body[..] }, "seed {seed}");
    }
}
