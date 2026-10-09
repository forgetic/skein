//! Deterministic, demand-checking plaintext stream world. Wire fixtures are
//! handwritten independently of the product encoders.
use skein_http::{Header, client as http, sse};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, Intake, Queue, Rng, Time, Token, Wall};
use skein_llm::{Block, Call, Credential, Endpoint, Message, Prompt, Role, client};

#[must_use]
pub fn limits() -> client::Limits {
    client::Limits {
        http: http::Limits { request: 4096, head: 4096, headers: 32, read: 256, send: 31 },
        sse: sse::Limits { line: 4096, event: 8192, field: 128, chunk: 128 },

        request: 8192,
        retained: 8192,
        strings: 4096,
        depth: 32,
        tokens: 1024,
        output_items: 16,
        input: 2048,
        reasoning: 2048,
        answer: 8192,
        detail_bytes: 256,

        tools: 16,
        history_items: 16,
        metadata: 2048,
        receiving: 1_048_576,
        skip: 8192,

        error_bytes: 4096,
        drop_reasoning: false,
        declared_output_tokens: 4096,
    }
}

#[must_use]
pub fn call(owner: u64) -> Call {
    let mut endpoint = Endpoint::codex();
    endpoint.headers =
        Box::new([Header { name: b"originator".to_vec().into(), value: b"skein-world".to_vec().into() }]);
    Call {
        owner: Token::new(owner),
        endpoint,
        credential: Credential {
            access_token: b"secret-test-token".to_vec().into(),
            account_id: b"account-test".to_vec().into(),
        },
        prompt: Prompt {
            model: b"fixture-model".to_vec().into(),
            instructions: b"Be brief.".to_vec().into(),
            tools: Box::new([]),
            messages: Box::new([Message {
                role: Role::User,
                content: Box::new([Block::Text { text: b"Hello".to_vec().into(), replay: None }]),
            }]),
            reasoning_effort: None,
            affinity: Some(skein_llm::Affinity { key: [0x42; 16], thread: 0 }),
            choice: skein_llm::ToolChoice::Auto,
            max_output_tokens: None,
        },
    }
}

#[must_use]
pub fn events(documents: &[&str]) -> Vec<u8> {
    documents.iter().flat_map(|document| format!("data: {document}\n\n").into_bytes()).collect()
}

#[must_use]
pub fn response(status: u16, headers: &str, body: &[u8], chunked: bool) -> Vec<u8> {
    let framing = if chunked {
        "Transfer-Encoding: chunked\r\n".to_owned()
    } else {
        format!("Content-Length: {}\r\n", body.len())
    };
    let mut bytes = format!("HTTP/1.1 {status} Test\r\n{headers}{framing}\r\n").into_bytes();
    if chunked {
        for piece in body.chunks(37) {
            bytes.extend_from_slice(format!("{:x}\r\n", piece.len()).as_bytes());
            bytes.extend_from_slice(piece);
            bytes.extend_from_slice(b"\r\n");
        }
        bytes.extend_from_slice(b"0\r\n\r\n");
    } else {
        bytes.extend_from_slice(body);
    }
    bytes
}

pub const TERMINAL: &str = r#"{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":12,"output_tokens":3,"input_tokens_details":{"cached_tokens":4}}}}"#;
pub const TEXT_ADDED: &str =
    r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_1","type":"message"}}"#;
pub const TEXT_DELTA: &str =
    r#"{"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"Hello"}"#;
pub const TEXT_DONE: &str = r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_1","type":"message","phase":"final_answer","content":[{"type":"output_text","text":"Hello"}]}}"#;

#[must_use]
pub fn text_response(chunked: bool) -> Vec<u8> {
    response(
        200,
        "Content-Type: text/event-stream\r\n",
        &events(&[TEXT_ADDED, TEXT_DELTA, TEXT_DONE, TERMINAL]),
        chunked,
    )
}

/// Queues are deliberately only as large as the public entry-point bound.
#[expect(missing_debug_implementations, reason = "Client intentionally hides credential-bearing state")]
pub struct World {
    pub machine: client::Client,
    pub env: Env<client::Limits>,
    pub seen: Vec<client::Event>,
    pub sent: Vec<u8>,
    pub demand: Option<(Read, u32)>,
    pub grants: u32,
    pub source_at: usize,
    source: Vec<u8>,
    intake: Intake,
    above: Queue<client::Event>,
    below: Queue<Down>,
    rng: Rng,
    fragment: u32,
    room_delay: u32,
    ticks: u32,
    ended: bool,
    early_reads: bool,
}
impl World {
    #[must_use]
    pub fn new(call: Call, limits: client::Limits, source: Vec<u8>, seed: u64) -> Self {
        let machine = client::Client::prepare(call, &limits).expect("fixture request is admitted");
        Self::prepared(machine, limits, source, seed)
    }

    /// Adopts the caller's one freshly prepared Client and independent literal
    /// response bytes without preparing another Client or entering either side.
    /// The caller supplies the unchanged limits used at admission; the original
    /// callback owner and request bytes remain owned by this Client. Start, Next,
    /// Cancel, Close and actual lower settlement use the existing entrances.
    /// Queue and intake capacities are identical to [`Self::new`]. Price the
    /// Client once, with source bytes, sent/seen observations and world buffers
    /// separately; this constructor adds no state or memory allowance.
    /// Contract: docs/design/fake-llm.md, section 5; programming-model.md,
    /// sections 4.4, 5.2 and 6.3; testing-strategy.md, sections 2.4 and 6.
    #[must_use]
    pub fn prepared(machine: client::Client, limits: client::Limits, source: Vec<u8>, seed: u64) -> Self {
        Self {
            machine,
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            seen: Vec::new(),
            sent: Vec::new(),
            demand: None,
            grants: 0,
            source_at: 0,
            source,
            intake: Intake::with_capacity(http::largest_read(&limits.http)),
            above: Queue::with_capacity(client::MAX_OUT.above),
            below: Queue::with_capacity(client::MAX_OUT.below),
            rng: Rng::new(seed),
            fragment: 257,
            room_delay: 0,
            ticks: 0,
            ended: false,
            early_reads: false,
        }
    }
    pub fn fragmentation(&mut self, max: u32, room_delay: u32) {
        self.fragment = max;
        self.room_delay = room_delay;
    }
    pub fn allow_early_response(&mut self) {
        self.early_reads = true;
    }
    pub fn eof(&mut self) {
        self.ended = true;
        client::up(&mut self.machine, &self.env, Up::End, &mut self.above, &mut self.below);
        self.take();
    }
    pub fn transport_failed(&mut self) {
        self.demand = None;
        self.grants = 0;
        client::up(
            &mut self.machine,
            &self.env,
            Up::Failed(skein_lib::stream::Fault::Reset),
            &mut self.above,
            &mut self.below,
        );
        self.take();
    }
    pub fn request(&mut self, request: client::Request) {
        client::down(&mut self.machine, &self.env, request, &mut self.above, &mut self.below);
        self.take();
    }
    pub fn settle(&mut self) {
        client::closed(&mut self.machine, &self.env, &mut self.above, &mut self.below);
        self.take();
    }
    pub fn abort(&mut self, failure: skein_llm::Failure) {
        client::abort(&mut self.machine, &self.env, failure, &mut self.above, &mut self.below);
        self.take();
    }
    fn take(&mut self) {
        while let Some(event) = self.above.pop() {
            self.seen.push(event);
        }
        while let Some(request) = self.below.pop() {
            match request {
                Down::Demand { read: Read::Nothing, room: 0 } => self.demand = None,
                Down::Demand { read, room } => {
                    assert!(self.demand.is_none(), "a demand is answered before its replacement");
                    assert_eq!(self.grants, 0, "a send grant is spent before another demand");
                    self.demand = Some((read, room));
                }
                Down::Send(data) => {
                    assert!(self.grants > 0, "each send follows a room grant");
                    assert!(data.len() <= usize::try_from(self.grants).expect("u32 fits usize"), "send fits its grant");
                    self.grants = 0;
                    self.sent.extend_from_slice(&data);
                }
                Down::Finish => panic!("HTTP client does not finish a reusable lower stream"),
            }
        }
    }
    /// One deterministic turn; source fragments may arrive without satisfying
    /// the outstanding demand. Reads are delivered only through `Intake::meet`.
    pub fn tick(&mut self, auto_next: bool) -> bool {
        self.ticks += 1;
        if self.machine.has_work() {
            client::resume(&mut self.machine, &self.env, &mut self.above, &mut self.below);
            self.take();
            return true;
        }
        match self.machine.waiting() {
            client::Waiting::Next if auto_next => {
                self.request(client::Request::Next);
                return true;
            }
            client::Waiting::Closing | client::Waiting::Nothing | client::Waiting::Idle => return false,
            client::Waiting::Start
            | client::Waiting::Ready
            | client::Waiting::Http(_)
            | client::Waiting::Next
            | client::Waiting::Response
            | client::Waiting::Draining => {}
        }
        if self.source_at < self.source.len() && self.intake.room() > 0 {
            let n = usize::try_from(self.rng.between(1, u64::from(self.fragment)))
                .expect("u32 fits usize")
                .min(self.source.len() - self.source_at)
                .min(usize::try_from(self.intake.room()).expect("u32 fits usize"));
            self.intake.append(&self.source[self.source_at..self.source_at + n]).expect("fragment fits intake");
            self.source_at += n;
        }
        let Some((read, room)) = self.demand else {
            return false;
        };
        let answer = if room > 0 && self.ticks.is_multiple_of(self.room_delay + 1) {
            self.grants = room;
            Some(Up::Room)
        } else if room > 0 && !self.early_reads {
            None
        } else if let Some(data) = self.intake.meet(read) {
            Some(Up::Bytes(data))
        } else if read != Read::Nothing && self.source_at == self.source.len() && !self.ended {
            self.ended = true;
            Some(Up::End)
        } else {
            None
        };
        if let Some(answer) = answer {
            if !matches!(answer, Up::End) {
                self.demand = None;
            }
            client::up(&mut self.machine, &self.env, answer, &mut self.above, &mut self.below);
            self.take();
        }
        true
    }
    pub fn run(&mut self) {
        for _ in 0..100_000 {
            if !self.tick(true) {
                return;
            }
        }
        panic!("bounded wire world stalled in {:?}, demand {:?}", self.machine.waiting(), self.demand);
    }
    #[must_use]
    pub fn terminals(&self) -> usize {
        self.seen
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    client::Event::Completed { .. } | client::Event::Failed { .. } | client::Event::Cancelled { .. }
                )
            })
            .count()
    }
    pub fn assert_once(&self) {
        assert_eq!(self.terminals(), 1, "one terminal per accepted call");
        assert!(self.seen.iter().filter(|event| matches!(event, client::Event::Close)).count() <= 1, "Close is unique");
        assert!(
            self.seen.iter().filter(|event| matches!(event, client::Event::Closed)).count() <= 1,
            "Closed is unique"
        );
    }
}

/// Shared actual-client/byte-peer world; applications supply only schemas and scripts.
pub mod fake;
