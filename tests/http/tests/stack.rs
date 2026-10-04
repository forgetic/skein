//! The client, the event stream reader and the JSON tokenizer stacked
//! (http.md, 2 and 6): an LLM client's stack over a transcript, as a
//! connection routes between the machines, a small step towards the
//! protocol worlds (testing-strategy.md, 2.5).
//!
//! The request body is a JSON document, written by the JSON writer and
//! uploaded in pieces within the room granted. The response goes up the
//! stack: the client's body to the reader, and each event's data to a
//! tokenizer made for it, through `sse::Data`; a response that does not
//! stream events goes to one tokenizer whole. The stream below meets each
//! demand from the transcript's bytes, arriving in pieces cut at random.
//! Every event must be the transcript's, and every document the JSON
//! reference parser's reading of its data.

use skein_http::Header;
use skein_http::client::{self, Body, Call, Client, Method, Reuse};
use skein_http::sse::{self, Data, Reader};
use skein_http_world::transcript::{self, Transcript};
use skein_json::Token;
use skein_json::tokenizer::{self as json, Tokenizer};
use skein_json::writer::{self, Encoder};
use skein_json_world::{Decoded, Outcome, reference};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, Intake, Queue, Rng, Time, Wall};

const JSON: json::Limits = json::Limits { depth: 32, string: 4096, number: 64, chunk: 64, length: 1 << 16 };

/// The request body an LLM client sends: written by the JSON writer.
fn request_body() -> Box<[u8]> {
    fn encode(json: &mut Encoder) {
        json.object_start();
        json.key(b"model");
        json.string(b"claude-sonnet-4-5");
        json.key(b"max_tokens");
        json.unsigned(1024);
        json.key(b"stream");
        json.boolean(true);
        json.key(b"messages");
        json.array_start();
        json.object_start();
        json.key(b"role");
        json.string(b"user");
        json.key(b"content");
        json.string("Why is the sky blue? \u{2600}".as_bytes());
        json.object_end();
        json.array_end();
        json.object_end();
    }
    let limits = writer::Limits { depth: 8, length: 4096 };
    let mut measure = Encoder::measure(&limits);
    encode(&mut measure);
    let len = measure.measured().expect("the request is written");
    let mut write = Encoder::write(len, &limits);
    encode(&mut write);
    write.finish()
}

/// What the stack made of a response: the events (their type and their
/// data), the documents decoded from their data, and how the exchange
/// ended.
#[derive(Debug, Default)]
struct Stacked {
    events: Vec<(Vec<u8>, Vec<u8>)>,
    documents: Vec<Decoded>,
    sse_outcome: Option<sse::Event>,
    outcome: Option<client::Event>,
    sent: Vec<u8>,
}

/// The machines of one connection's stack, and the queues between them.
struct Stack {
    env: Env<client::Limits>,
    client: Client,
    sse_env: Env<sse::Limits>,
    reader: Option<Reader>,
    json_env: Env<json::Limits>,
    /// The tokenizer of the document being read, and the data it reads
    /// from, or the client's body when the response is not events.
    tokenizer: Option<(Tokenizer, Option<Data>)>,
    tokens: Vec<Token>,
    client_up: Queue<client::Event>,
    client_down: Queue<Down>,
    sse_up: Queue<sse::Event>,
    sse_down: Queue<Down>,
    json_up: Queue<json::Event>,
    json_down: Queue<Down>,
}

/// Runs `transcript` up the stack, with the stream below cutting its bytes
/// at random from `seed`.
#[expect(clippy::too_many_lines, reason = "one loop that routes between the machines, as a connection does")]
fn run(transcript: &Transcript, seed: u64) -> Stacked {
    let mut rng = Rng::new(seed);
    let limits = transcript.limits;
    let mut stack = Stack {
        env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
        client: Client::new(&limits),
        sse_env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: transcript.sse },
        reader: None,
        json_env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: JSON },
        tokenizer: None,
        tokens: Vec::new(),
        client_up: Queue::with_capacity(8),
        client_down: Queue::with_capacity(8),
        sse_up: Queue::with_capacity(8),
        sse_down: Queue::with_capacity(8),
        json_up: Queue::with_capacity(8),
        json_down: Queue::with_capacity(8),
    };
    // The startup checks: each machine's largest demand within its side
    // below's.
    assert!(sse::largest_demand(&transcript.sse) <= limits.read, "the reader's scans within the client's reads");
    assert!(json::largest_demand(&JSON) <= limits.read, "the tokenizer's demands within the client's reads");

    let body = request_body();
    let call = Call {
        method: Method::Post,
        target: b"/v1/messages".to_vec().into(),
        headers: Box::new([Header { name: b"Host".to_vec().into(), value: b"api.example.com".to_vec().into() }]),
        body: Body::Length(body.len() as u64),
        close: false,
    };
    let call = if transcript.method == Method::Get {
        call
    } else {
        Call { method: transcript.method, body: Body::None, ..call }
    };
    let uploads = call.body != Body::None;
    let mut out = Stacked::default();
    let mut unsent = &transcript.bytes[..];
    let mut intake = Intake::with_capacity(client::largest_read(&limits).max(64));
    let mut below: Option<(Read, u32)> = None;
    let mut uploaded = 0;
    let mut room = 0;
    stack.client_call(client::Request::Call(call));
    if uploads {
        room = limits.send.min(u32::try_from(body.len()).expect("fits a u32"));
        stack.client_call(client::Request::Upload(Down::Demand { read: Read::Nothing, room }));
    }
    for _ in 0..1_000_000 {
        // Route what each machine emitted, the side above first.
        if let Some(event) = stack.json_up.pop() {
            stack.json_event(event, &mut out);
            continue;
        }
        if let Some(request) = stack.json_down.pop() {
            stack.json_request(request);
            continue;
        }
        if let Some(event) = stack.sse_up.pop() {
            stack.sse_event(event, &mut out);
            continue;
        }
        if let Some(request) = stack.sse_down.pop() {
            stack.client_call(client::Request::Body(request));
            continue;
        }
        if let Some(event) = stack.client_up.pop() {
            match event {
                client::Event::Upload(Up::Room) => {
                    let end = (uploaded + usize::try_from(room).expect("fits a usize")).min(body.len());
                    stack.client_call(client::Request::Upload(Down::Send(body[uploaded..end].into())));
                    uploaded = end;
                    if uploaded == body.len() {
                        stack.client_call(client::Request::Upload(Down::Finish));
                    } else {
                        room = limits.send.min(u32::try_from(body.len() - uploaded).expect("fits a u32"));
                        stack.client_call(client::Request::Upload(Down::Demand { read: Read::Nothing, room }));
                    }
                }
                event @ (client::Event::Done(_) | client::Event::Failed(_)) => {
                    out.outcome = Some(event);
                    return out;
                }
                event @ (client::Event::Response(_)
                | client::Event::Body(_)
                | client::Event::Upload(_)
                | client::Event::Closed) => stack.client_event(event),
            }
            continue;
        }
        if let Some(request) = stack.client_down.pop() {
            match request {
                Down::Demand { read: Read::Nothing, room: 0 } => below = None,
                Down::Demand { read, room } => below = Some((read, room)),
                Down::Send(bytes) => out.sent.extend_from_slice(&bytes),
                Down::Finish => panic!("the client never finishes"),
            }
            continue;
        }
        // Nothing to route: the stream below receives, and answers.
        if !unsent.is_empty() && intake.room() > 0 {
            let piece = usize::try_from(rng.between(1, 200))
                .expect("fits a usize")
                .min(unsent.len())
                .min(intake.room() as usize);
            intake.append(&unsent[..piece]).expect("the stream holds it");
            unsent = &unsent[piece..];
        }
        let Some((read, room)) = below else { panic!("the stack stalled: {out:?}") };
        let answer = if room > 0 {
            Some(Up::Room)
        } else if let Some(bytes) = intake.meet(read) {
            Some(Up::Bytes(bytes))
        } else if unsent.is_empty() {
            Some(Up::End)
        } else {
            None
        };
        if let Some(answer) = answer {
            below = None;
            client::up(&mut stack.client, &stack.env, answer, &mut stack.client_up, &mut stack.client_down);
        }
    }
    panic!("a response is read in a few steps a byte");
}

impl Stack {
    fn client_call(&mut self, rq: client::Request) {
        client::down(&mut self.client, &self.env, rq, &mut self.client_up, &mut self.client_down);
    }

    /// An event from the client for the machines above it.
    fn client_event(&mut self, event: client::Event) {
        match event {
            client::Event::Response(response) => {
                let streams = match response.header(b"content-type") {
                    Some(value) => value.to_ascii_lowercase().starts_with(b"text/event-stream"),
                    None => false,
                };
                if streams {
                    self.reader = Some(Reader::new(&self.sse_env.limits));
                    self.sse_call(sse::Request::Next);
                } else {
                    self.tokenizer = Some((Tokenizer::new(&JSON), None));
                    self.json_call(json::Request::Next);
                }
            }
            client::Event::Body(up) => match (&mut self.reader, &mut self.tokenizer) {
                (Some(reader), _) => sse::up(reader, &self.sse_env, up, &mut self.sse_up, &mut self.sse_down),
                (None, Some((tokenizer, None))) => {
                    json::up(tokenizer, &self.json_env, up, &mut self.json_up, &mut self.json_down);
                }
                (None, Some((_, Some(_))) | None) => panic!("the body has a machine above it"),
            },
            other @ (client::Event::Upload(_)
            | client::Event::Done(_)
            | client::Event::Failed(_)
            | client::Event::Closed) => panic!("the client told {other:?}"),
        }
    }

    fn sse_call(&mut self, rq: sse::Request) {
        let reader = self.reader.as_mut().expect("a reader");
        sse::down(reader, &self.sse_env, rq, &mut self.sse_up, &mut self.sse_down);
    }

    /// An event from the reader: a document to tokenize, or the end.
    fn sse_event(&mut self, event: sse::Event, out: &mut Stacked) {
        match event {
            sse::Event::Message(message) => {
                out.events.push((message.name.to_vec(), message.data.to_vec()));
                if &*message.data == b"[DONE]" {
                    // Not JSON: the end of an OpenAI stream.
                    self.sse_call(sse::Request::Next);
                    return;
                }
                self.tokenizer = Some((Tokenizer::new(&JSON), Some(Data::new(message.data))));
                self.json_call(json::Request::Next);
            }
            sse::Event::Ended | sse::Event::Failed(_) => {
                out.sse_outcome = Some(event);
                self.sse_call(sse::Request::Close);
            }
            sse::Event::Closed => self.reader = None,
        }
    }

    fn json_call(&mut self, rq: json::Request) {
        let (tokenizer, _) = self.tokenizer.as_mut().expect("a tokenizer");
        json::down(tokenizer, &self.json_env, rq, &mut self.json_up, &mut self.json_down);
    }

    /// A demand from the tokenizer: met by the event's data at once, or
    /// passed to the client.
    fn json_request(&mut self, request: Down) {
        let (tokenizer, data) = self.tokenizer.as_mut().expect("a tokenizer");
        match data {
            Some(data) => {
                let Down::Demand { read, room: 0 } = request else { panic!("the tokenizer reads") };
                if let Some(answer) = data.answer(read) {
                    json::up(tokenizer, &self.json_env, answer, &mut self.json_up, &mut self.json_down);
                }
            }
            None => self.client_call(client::Request::Body(request)),
        }
    }

    /// An event from the tokenizer: a token, or the document's outcome.
    fn json_event(&mut self, event: json::Event, out: &mut Stacked) {
        match event {
            json::Event::Token(token) => {
                self.tokens.push(token);
                self.json_call(json::Request::Next);
            }
            json::Event::Done | json::Event::Failed(_) => {
                let outcome = match event {
                    json::Event::Failed(error) => Outcome::Failed(error),
                    json::Event::Done | json::Event::Token(_) | json::Event::Closed => Outcome::Done,
                };
                out.documents.push(Decoded { tokens: std::mem::take(&mut self.tokens), outcome });
                self.json_call(json::Request::Close);
            }
            json::Event::Closed => {
                let (_, data) = self.tokenizer.take().expect("a tokenizer");
                if data.is_some() {
                    self.sse_call(sse::Request::Next);
                }
            }
        }
    }
}

#[test]
fn an_llm_s_stream_goes_up_the_stack_event_by_event_and_document_by_document() {
    let mut streams = 0;
    for transcript in transcript::all() {
        if !transcript.name.starts_with("anthropic-") && !transcript.name.starts_with("openai-") {
            continue;
        }
        let expected = transcript.expected.clone().expect("an expectation");
        for seed in 0..6 {
            let stacked = run(&transcript, seed);
            let what = format!("{}, seed {seed}", transcript.name);
            assert_eq!(stacked.outcome, Some(client::Event::Done(Reuse::Keep)), "{what}");
            let body = request_body();
            assert!(stacked.sent.ends_with(&body), "{what}: the request body, written and uploaded");
            if let Some(events) = &expected.events {
                streams += 1;
                let names: Vec<(Vec<u8>, Vec<u8>)> =
                    events.events.iter().map(|event| (event.name.clone(), event.data.clone())).collect();
                assert_eq!(stacked.events, names, "{what}: the transcript's events");
                assert_eq!(stacked.sse_outcome, Some(sse::Event::Ended), "{what}");
                let mut documents = stacked.documents.iter();
                for (_, data) in &stacked.events {
                    if data == b"[DONE]" {
                        continue;
                    }
                    let decoded = documents.next().expect("a document for each event's data");
                    assert_eq!(decoded, &reference::parse(data, &JSON), "{what}: the reference's reading");
                    assert_eq!(decoded.outcome, Outcome::Done, "{what}: one JSON document per event");
                }
                assert!(documents.next().is_none(), "{what}");
            } else {
                // An error document, read whole by one tokenizer.
                let body = expected.body.as_ref().expect("a body");
                assert_eq!(stacked.documents, [reference::parse(body, &JSON)], "{what}");
            }
        }
    }
    assert!(streams >= 4 * 6, "every stream of both providers, chunked and by length");
}
