//! Scenarios (testing-strategy.md, 7), in the ends' own terms, from a
//! seed: the requests an LLM client writes and the events a provider
//! answers each with, all JSON the writer writes, with text of every kind
//! in them, now and then an event with an id or a reconnection time; and
//! what each end's user does, for each thing a protocol world aims at.

use skein_json::Token;
use skein_lib::Rng;

use crate::client_end;
use crate::server_end::{self, Item};
use crate::world::{Expect, Hold, Scenario, json_limits};

fn key(name: &str) -> Token {
    Token::Key(name.as_bytes().into())
}

fn string(text: &[u8]) -> Token {
    Token::String(text.into())
}

fn number(n: u64) -> Token {
    Token::Number(n.to_string().into_bytes().into())
}

/// Text a string is made of: words, and what JSON escapes or keeps as it
/// is: quotes, backslashes, line endings, a control character, and UTF-8
/// of two, three and four bytes.
fn text(rng: &mut Rng, most: u64) -> Vec<u8> {
    const PIECES: &[&str] = &[
        "the ", "sky ", "is ", "blue ", "because ", "light ", "scatters", ".", "\"", "\\", "\n", "\t", "\u{1}", "é",
        "€", "😀", "/", " ",
    ];
    let mut out = Vec::new();
    for _ in 0..rng.between(0, most) {
        out.extend_from_slice(PIECES[usize::try_from(rng.below(PIECES.len() as u64)).expect("fits")].as_bytes());
    }
    out
}

/// The request an LLM client writes: its model, its budget, and a
/// conversation of `turns` messages.
#[must_use]
pub fn request(rng: &mut Rng, turns: u64, most: u64) -> Vec<Token> {
    let mut tokens = vec![
        Token::ObjectStart,
        key("model"),
        string(b"claude-sonnet-4-5"),
        key("max_tokens"),
        number(rng.between(1, 8192)),
        key("stream"),
        Token::True,
        key("messages"),
        Token::ArrayStart,
    ];
    for turn in 0..turns {
        tokens.extend([Token::ObjectStart, key("role"), string(if turn % 2 == 0 { b"user" } else { b"assistant" })]);
        tokens.extend([key("content"), string(&text(rng, most)), Token::ObjectEnd]);
    }
    tokens.extend([Token::ArrayEnd, Token::ObjectEnd]);
    tokens
}

/// The events a provider answers with: the message's start, `deltas`
/// pieces of text, and its end.
#[must_use]
pub fn events(rng: &mut Rng, deltas: u64, most: u64) -> Vec<(Vec<u8>, Vec<Token>)> {
    let mut events = vec![(
        b"message_start".to_vec(),
        vec![
            Token::ObjectStart,
            key("type"),
            string(b"message_start"),
            key("message"),
            Token::ObjectStart,
            key("id"),
            string(b"msg_01"),
            key("usage"),
            Token::ObjectStart,
            key("input_tokens"),
            number(rng.below(1000)),
            Token::ObjectEnd,
            Token::ObjectEnd,
            Token::ObjectEnd,
        ],
    )];
    for _ in 0..deltas {
        events.push((
            b"content_block_delta".to_vec(),
            vec![
                Token::ObjectStart,
                key("type"),
                string(b"content_block_delta"),
                key("index"),
                number(0),
                key("delta"),
                Token::ObjectStart,
                key("type"),
                string(b"text_delta"),
                key("text"),
                string(&text(rng, most)),
                Token::ObjectEnd,
                Token::ObjectEnd,
            ],
        ));
    }
    events.push((
        b"message_stop".to_vec(),
        vec![Token::ObjectStart, key("type"), string(b"message_stop"), Token::ObjectEnd],
    ));
    events
}

/// `tokens`, written by the JSON writer.
fn written(tokens: &[Token]) -> Box<[u8]> {
    skein_json_world::write(tokens, &json_limits()).expect("a document the writer writes")
}

/// A scenario of calls, each `requests`' document answered by the
/// `answers`' of the same index, neither user stalling nor closing early.
/// Now and then an event carries an id, which the reader's last event ID
/// becomes, or a reconnection time.
fn plain(requests: Vec<Vec<Token>>, answers: Vec<Vec<(Vec<u8>, Vec<Token>)>>, rng: &mut Rng) -> Scenario {
    let mut items = Vec::new();
    let mut count = 0;
    for events in &answers {
        let mut answer = Vec::new();
        for (name, tokens) in events {
            count += 1;
            let id = if rng.chance(300) { Some(format!("evt_{count}").into_bytes().into()) } else { None };
            let retry = if rng.chance(100) { Some(rng.below(60_000)) } else { None };
            answer.push(Item { name: name.as_slice().into(), data: written(tokens), id, retry });
        }
        items.push(answer);
    }
    let ping = if rng.chance(300) { Some(usize::try_from(rng.between(1, 5)).expect("fits")) } else { None };
    Scenario {
        client: client_end::Script {
            bodies: requests.iter().map(|request| written(request)).collect(),
            stall: None,
            close: None,
        },
        server: server_end::Script { answers: items, ping, early: None, stall: None, close: None },
        requests,
        answers,
        expect: Expect::Stream,
        holds: None,
        linger: 0,
    }
}

/// `calls` streamed answers, of up to `turns` turns and `deltas` deltas.
fn calls(rng: &mut Rng, calls: u64, turns: u64, deltas: u64) -> Scenario {
    let mut requests = Vec::new();
    let mut answers = Vec::new();
    for _ in 0..calls {
        let turns = rng.between(1, turns);
        requests.push(request(rng, turns, 40));
        let deltas = rng.between(0, deltas);
        answers.push(events(rng, deltas, 30));
    }
    plain(requests, answers, rng)
}

/// A streamed answer, whole: the request goes up the server's stack and
/// every event up the client's.
#[must_use]
pub fn stream(rng: &mut Rng) -> Scenario {
    calls(rng, 1, 6, 30)
}

/// Two or three calls, one after another on the connection the last one
/// kept, each streamed whole through both stacks.
#[must_use]
pub fn several(rng: &mut Rng) -> Scenario {
    let count = rng.between(2, 3);
    calls(rng, count, 4, 12)
}

/// A slow reader: the client's user stops reading for a long while, far
/// more events in the answer than the caps between the ends hold; the
/// server's writer must stop until it reads again.
#[must_use]
pub fn slow_reader(rng: &mut Rng) -> Scenario {
    let request = request(rng, 1, 20);
    let events = events(rng, 120, 40);
    let mut scenario = plain(vec![request], vec![events], rng);
    let after = usize::try_from(rng.between(0, 20)).expect("fits a usize");
    scenario.client.stall = Some((after, rng.between(600, 2_000)));
    scenario.holds = Some(Hold::Writer);
    scenario
}

/// A slow consumer: the server's user stops reading the request a while,
/// partway through a body far larger than the caps between the ends hold;
/// the client's upload must stop until it reads again.
#[must_use]
pub fn slow_consumer(rng: &mut Rng) -> Scenario {
    let request = request(rng, 40, 400);
    let events = events(rng, 4, 20);
    let mut scenario = plain(vec![request], vec![events], rng);
    let after = usize::try_from(rng.between(1, 30)).expect("fits a usize");
    scenario.server.stall = Some((after, rng.between(600, 2_000)));
    scenario.holds = Some(Hold::Upload);
    scenario
}

/// A response that comes mid-upload: the client uploads a body larger than
/// everything between the ends holds, and the server answers at once with
/// an error, before it reads any of it.
#[must_use]
pub fn early(rng: &mut Rng) -> Scenario {
    let request = request(rng, 40, 400);
    let mut scenario = plain(vec![request], vec![Vec::new()], rng);
    let error = vec![
        Token::ObjectStart,
        key("type"),
        string(b"error"),
        key("error"),
        Token::ObjectStart,
        key("type"),
        string(b"request_too_large"),
        key("message"),
        string(&text(rng, 20)),
        Token::ObjectEnd,
        Token::ObjectEnd,
    ];
    scenario.server.early = Some((413, written(&error)));
    scenario.expect = Expect::Early;
    scenario.linger = 100_000;
    scenario
}

/// One end closes partway, the client while the server writes or the
/// server while the client uploads or reads, in one of up to three calls,
/// at a moment drawn.
#[must_use]
pub fn partway(rng: &mut Rng) -> Scenario {
    let count = rng.between(1, 3);
    let mut requests = Vec::new();
    let mut answers = Vec::new();
    let mut deltas = 0;
    for _ in 0..count {
        let turns = rng.between(1, 20);
        requests.push(request(rng, turns, 200));
        let these = rng.between(0, 60 / count);
        deltas += these;
        answers.push(events(rng, these, 40));
    }
    let mut scenario = plain(requests, answers, rng);
    // Within the span a run takes, about a dozen iterations an event.
    let at = rng.between(0, 100 * count + 12 * deltas);
    if rng.chance(500) {
        scenario.client.close = Some(at);
    } else {
        scenario.server.close = Some(at);
    }
    scenario.expect = Expect::Partway;
    scenario
}
