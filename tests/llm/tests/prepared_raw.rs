//! One caller-prepared Client adopts independent native response bytes.
//! Literal expectations observe request, original callback and real lower
//! settlement; these synthetic fixtures establish no live admission policy.
//! Contract: docs/design/fake-llm.md, section 5; testing-strategy.md,
//! sections 2.4, 4.1 and 6.

use skein_lib::Token;
use skein_llm::{Block, Completion, Credential, Endpoint, Stop, client};
use skein_llm_world::{World, call, events, limits, response};

const CODEX: &[&str] = &[
    r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"adopted_message","type":"message"}}"#,
    r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"adopted_message","type":"message","phase":"final_answer","content":[{"type":"output_text","text":"Original owner."}]}}"#,
    r#"{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":12,"output_tokens":3,"input_tokens_details":{"cached_tokens":4}}}}"#,
];

const ANTHROPIC: &[u8] = br#"event: message_start
data: {"type":"message_start","message":{"id":"adopted_message","type":"message","role":"assistant","model":"fixture-model","content":[],"stop_reason":null,"usage":{"input_tokens":7,"cache_read_input_tokens":11,"cache_creation_input_tokens":13,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Original owner."}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":9}}

event: message_stop
data: {"type":"message_stop"}

"#;

fn request_evidence(bytes: &[u8], path: &[u8]) -> bool {
    let Some(at) = bytes.windows(4).position(|part| part == b"\r\n\r\n") else { return false };
    let (head, body) = bytes.split_at(at + 4);
    head.starts_with(path)
        && body.windows(b"\"model\":\"fixture-model\"".len()).any(|part| part == b"\"model\":\"fixture-model\"")
        && body.windows(b"Original question.".len()).any(|part| part == b"Original question.")
}

fn completion_evidence(completion: &Completion, usage: [u64; 4], replay: bool) -> bool {
    let [Block::Text { text, replay: actual_replay }] = completion.content.as_ref() else { return false };
    text.as_ref() == b"Original owner."
        && actual_replay.is_some() == replay
        && completion.stop == Stop::EndTurn
        && [
            completion.usage.input_tokens,
            completion.usage.output_tokens,
            completion.usage.cache_read_tokens,
            completion.usage.cache_write_tokens,
        ] == usage
}

fn observe(mut world: World, owner: Token, path: &[u8], usage: [u64; 4], replay: bool) {
    assert_eq!(world.machine.owner(), owner);
    assert_eq!(world.machine.waiting(), client::Waiting::Start);
    assert!(world.seen.is_empty());
    assert!(world.sent.is_empty());
    assert_eq!(world.source_at, 0);
    assert_eq!(world.demand, None);
    assert_eq!(world.grants, 0);
    world.fragmentation(1, 2);
    world.request(client::Request::Next);
    assert!(world.seen.is_empty(), "Next before Start spends no terminal right");
    assert!(world.sent.is_empty());
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert!(request_evidence(&world.sent, path), "actual original request: {:?}", world.sent);
    let mut changed = world.sent.clone();
    changed[0] = b'X';
    assert!(!request_evidence(&changed, path));
    assert!(!request_evidence(b"POST / HTTP/1.1\r\n\r\n{}", path));
    let completed: Vec<_> = world
        .seen
        .iter()
        .filter_map(|event| match event {
            client::Event::Completed { owner: actual, completion } => {
                assert_eq!(*actual, owner);
                Some(completion)
            }
            client::Event::Failed { .. } | client::Event::Cancelled { .. } => panic!("actual native success"),
            client::Event::Delta { .. }
            | client::Event::Block { .. }
            | client::Event::Reusable
            | client::Event::Close
            | client::Event::Closed => None,
        })
        .collect();
    let [completion] = completed.as_slice() else { panic!("one original owner's Completed") };
    assert!(completion_evidence(completion, usage, replay));
    let mut changed = (*completion).clone();
    changed.usage.cache_read_tokens += 1;
    assert!(!completion_evidence(&changed, usage, replay));
    changed = (*completion).clone();
    changed.content = Box::new([]);
    assert!(!completion_evidence(&changed, usage, replay));
    assert_eq!(world.machine.waiting(), client::Waiting::Idle);
    assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Reusable)).count(), 1);
    world.request(client::Request::Close);
    assert_eq!(world.machine.waiting(), client::Waiting::Closing);
    assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Close)).count(), 1);
    assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Closed)).count(), 0);
    world.settle();
    assert_eq!(world.machine.waiting(), client::Waiting::Nothing);
    assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Closed)).count(), 1);
    let settled_events = world.seen.len();
    world.settle();
    assert_eq!(world.seen.len(), settled_events);
    world.assert_once();
}

#[test]
fn actual_prepared_codex_owner_and_request_survive_raw_world_adoption() {
    let bounds = limits();
    let mut input = call(0);
    input.prompt.messages[0].content =
        Box::new([Block::Text { text: b"Original question.".as_slice().into(), replay: None }]);
    let machine = client::Client::prepare(input, &bounds).expect("one caller-owned prepared Client");
    let source = response(200, "Content-Type: text/event-stream\r\n", &events(CODEX), false);
    let world = World::prepared(machine, bounds, source, 17);
    observe(world, Token::new(0), b"POST /backend-api/codex/responses HTTP/1.1\r\n", [8, 3, 4, 0], true);
}

#[test]
fn actual_prepared_anthropic_owner_and_request_survive_raw_world_adoption() {
    let bounds = limits();
    let mut input = call(99);
    input.endpoint = Endpoint::anthropic();
    input.credential = Credential::anthropic(b"fixture-token".as_slice().into());
    input.prompt.cache_key = None;
    input.prompt.max_output_tokens = Some(128);
    input.prompt.messages[0].content =
        Box::new([Block::Text { text: b"Original question.".as_slice().into(), replay: None }]);
    let machine = client::Client::prepare(input, &bounds).expect("one caller-owned prepared Client");
    let source = response(200, "Content-Type: text/event-stream\r\n", ANTHROPIC, true);
    let world = World::prepared(machine, bounds, source, 29);
    observe(world, Token::new(99), b"POST /v1/messages HTTP/1.1\r\n", [7, 9, 11, 13], false);
}
