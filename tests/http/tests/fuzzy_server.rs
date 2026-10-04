//! The server's machine world, swept (testing-strategy.md, 8; http.md, 6):
//! many connections of generated requests, valid, mutated, and corrupted
//! where a random edit seldom lands, and the request transcripts cut and
//! mutated, under limits and neighbours drawn from each seed, each exchange checked against the reference reader and
//! the test's own writer. This stands in for the fuzz target, which waits
//! for a nightly toolchain.
//!
//! A sweep asserts that what it injects fell (testing-strategy.md, 3): each
//! way a request can end, each rejection and each refusal, each way a
//! stream can end or fail below, each fault failing the exchange and each
//! body's stream, a close in each state, a 100 (Continue), a client tired
//! of waiting for one, a final response before a 100 that was due, a
//! response to `HEAD`, a reply in chunks to HTTP/1.0 run to the end of the
//! stream, a body given up, a head that waited for a discard, pipelining,
//! and reuse.

use std::collections::BTreeMap;

use skein_http::server::{Body, Error};
use skein_http::{Method, Version};
use skein_http_world::server_world::{self, Outcome, Plan, Run, Seen as Served, Settings};
use skein_http_world::{generate, request_transcript, requests};
use skein_lib::Rng;

const ROUNDS: u64 = 10_000;

/// The transcripts, longer, are swept fewer times.
const TRANSCRIPT_ROUNDS: u64 = 1_000;

/// How often each thing a sweep injects or reaches fell.
#[derive(Default)]
struct Seen(BTreeMap<String, u32>);

impl Seen {
    fn note(&mut self, what: String) {
        *self.0.entry(what).or_default() += 1;
    }

    fn record(&mut self, run: &Run, settings: &Settings) {
        for seen in &run.seen {
            match seen.outcome {
                Some(Outcome::Failed(Error::Stream(fault))) => {
                    self.note("failed Stream".into());
                    self.note(format!("failed Stream({fault:?})"));
                }
                Some(outcome) => self.note(format!("{outcome:?}")),
                None => self.note("closed before the outcome".into()),
            }
            self.note_reply(seen);
            for (_, refusal) in &seen.refused {
                self.note(format!("refused {refusal:?}"));
            }
            if seen.discarded {
                self.note("a body discarded".into());
            }
            if seen.reply_failed.is_some() {
                self.note("the reply failed".into());
            }
            if seen.body_failed.is_some() {
                self.note(format!("the body failed with {:?}", seen.body_failed));
            }
            if seen.outcome == Some(Outcome::Failed(Error::BodyTooLong)) {
                let answered = seen.sent.starts_with(b"HTTP/1.1 413");
                self.note(format!("a chunked body past the limit, answered: {answered}"));
            }
        }
        self.note(format!("closed while waiting for {:?}", run.closed_while));
        let fell = run.fell;
        for (flag, what) in [
            (fell.idle_end, "an end with nothing demanded"),
            (fell.crossed_end, "a demand crossed the end"),
            (fell.withdrew, "a body demand withdrawn"),
            (fell.gave_up, "a body given up"),
            (fell.late_answer, "an answer after the withdrawal"),
            (fell.failed_after_end, "a failure after the end"),
            (fell.reused, "a connection reused"),
            (fell.pipelined, "requests pipelined"),
            (fell.continued, "a 100 (Continue)"),
            (fell.tired, "a client tired of waiting for a 100"),
            (fell.head_waited, "a head waited for a discard"),
            (fell.room_after_end, "room after the end"),
            (fell.reply_withdrawn, "a reply's demand withdrawn"),
            (fell.crossed_withdrawal, "a withdrawal crossed a piece"),
            (fell.untouched, "a body untouched, discarded late"),
            (fell.finished_over_grant, "a Finish in place of a grant's Send"),
        ] {
            if flag {
                self.note(what.into());
            }
        }
        if let Some(waiting) = fell.failed_while {
            self.note(format!("failed while waiting for {waiting:?}"));
        }
        if settings.cut.is_some() {
            self.note("a stream ended early".into());
        }
    }

    /// What the reply came to: one to `HEAD`, one in chunks to HTTP/1.0 run
    /// to the end of the stream, or a final response that went before a 100
    /// (Continue) the client waited for.
    fn note_reply(&mut self, seen: &Served) {
        let (Some(call), Some((response, _)), Some(Outcome::Done(_))) = (&seen.call, &seen.response, seen.outcome)
        else {
            return;
        };
        if call.method == Method::Head {
            self.note("a response to HEAD".into());
        }
        if call.version == Version::Http10
            && response.body == Body::Chunked
            && requests::sends_body(call.method, response)
            && seen.finished
        {
            self.note("a reply in chunks to HTTP/1.0, to the end of the stream".into());
        }
        let expects = call.version == Version::Http11
            && call.body != Body::None
            && call.body != Body::Length(0)
            && call.headers.iter().any(|header| {
                header.is(b"expect") && header.value.to_ascii_lowercase().windows(12).any(|w| w == b"100-continue")
            });
        if expects && !seen.sent.starts_with(b"HTTP/1.1 100") {
            self.note("a final response before the 100 that was due".into());
        }
    }

    fn assert_fell(&self, expected: &[String]) {
        for what in expected {
            assert!(self.0.contains_key(what), "{what} fell: {:#?}", self.0);
        }
    }
}

/// `count` requests and the side above's plans, from `rng`.
fn scenario(rng: &mut Rng, count: usize) -> (Vec<u8>, Vec<Plan>) {
    let mut client = Vec::new();
    let mut plans = Vec::new();
    for _ in 0..count {
        client.extend(requests::request(rng));
        plans.push(server_world::plan(rng));
    }
    (client, plans)
}

#[test]
fn generated_mutated_and_corrupted_requests_are_served_as_the_reference_reads_them_whatever_the_neighbours() {
    let mut tally = Seen::default();
    for round in 0..ROUNDS {
        let seed = 0x0090_0000 + round;
        let mut rng = Rng::new(seed);
        let count = usize::try_from(rng.between(1, 4)).unwrap();
        let (mut client, plans) = scenario(&mut rng, count);
        if rng.chance(300) {
            client = generate::mutate(&mut rng, &client);
        } else if rng.chance(400) {
            client = requests::corrupt(&mut rng, &client);
        }
        let limits = server_world::limits(&mut rng);
        let settings = Settings::chaotic(&mut rng, limits, client.len());
        let run = server_world::check(&client, &plans, &settings, seed);
        tally.record(&run, &settings);
    }
    let mut every: Vec<String> = [
        "Done(Keep)",
        "Done(Close)",
        "Ended",
        "Failed(Truncated)",
        "Failed(ChunkSize)",
        "Failed(Chunk)",
        "Failed(Trailer)",
        "Failed(Extensions)",
        "Failed(BodyTooLong)",
        "a chunked body past the limit, answered: true",
        "a chunked body past the limit, answered: false",
        "failed Stream",
        "failed Stream(Reset)",
        "failed Stream(Invalid)",
        "failed Stream(Other)",
        "closed before the outcome",
        "a response to HEAD",
        "a reply in chunks to HTTP/1.0, to the end of the stream",
        "a final response before the 100 that was due",
        "refused Status",
        "refused Name",
        "refused Value",
        "refused Reserved",
        "refused Body",
        "refused TooLong",
        "a body discarded",
        "the reply failed",
        "the body failed with Some(Other)",
        "the body failed with Some(Invalid)",
        "the body failed with Some(Reset)",
        "closed while waiting for Next",
        "closed while waiting for Room",
        "closed while waiting for Request",
        "closed while waiting for Body",
        "closed while waiting for Above",
        "closed while waiting for Close",
        "an end with nothing demanded",
        "a demand crossed the end",
        "a body demand withdrawn",
        "a body given up",
        "an answer after the withdrawal",
        "a failure after the end",
        "a connection reused",
        "requests pipelined",
        "a 100 (Continue)",
        "a client tired of waiting for a 100",
        "a head waited for a discard",
        "room after the end",
        "a reply's demand withdrawn",
        "a withdrawal crossed a piece",
        "a body untouched, discarded late",
        "a Finish in place of a grant's Send",
        "failed while waiting for Next",
        "failed while waiting for Room",
        "failed while waiting for Request",
        "failed while waiting for Body",
        "failed while waiting for Above",
        "failed while waiting for Close",
        "failed while waiting for Nothing",
        "a stream ended early",
    ]
    .iter()
    .map(|what| (*what).to_string())
    .collect();
    for rejection in server_world::rejections() {
        every.push(format!("Failed(Rejected({rejection:?}))"));
    }
    tally.assert_fell(&every);
}

#[test]
fn request_transcripts_cut_mutated_and_failed_are_served_as_the_reference_reads_them() {
    let transcripts = request_transcript::all();
    let mut tally = Seen::default();
    for round in 0..TRANSCRIPT_ROUNDS {
        let seed = 0x0091_0000 + round;
        let mut rng = Rng::new(seed);
        let transcript = &transcripts[usize::try_from(rng.below(transcripts.len() as u64)).unwrap()];
        let mut client = transcript.bytes.clone();
        if rng.chance(500) {
            client = generate::mutate(&mut rng, &client);
        }
        let plans: Vec<Plan> = (0..4).map(|_| server_world::plan(&mut rng)).collect();
        let settings = Settings::chaotic(&mut rng, transcript.limits, client.len());
        let run = server_world::check(&client, &plans, &settings, seed);
        tally.record(&run, &settings);
    }
    let every: Vec<String> =
        ["Done(Keep)", "Done(Close)", "Ended", "Failed(Truncated)", "failed Stream", "closed before the outcome"]
            .iter()
            .map(|what| (*what).to_string())
            .collect();
    tally.assert_fell(&every);
}
