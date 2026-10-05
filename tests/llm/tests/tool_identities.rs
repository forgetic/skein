//! Actual wire completions retain separate opaque call and item identities.
//! The expected IDs and native response are independent handwritten evidence.
use skein_heap::{Counting, Meter};
use skein_lib::Token;
use skein_llm::{Block, Completion, Failure, Message, Replay, Role, client, openai};
use skein_llm_world::{TERMINAL, World, call, events, limits, response, text_response};

#[global_allocator]
static HEAP: Counting = Counting;

const CALL_ID: &[u8] = b"call|\"id\\tail";
const ITEM_ID: &[u8] = b"item|\"id\\tail";

fn source() -> Vec<u8> {
    response(
        200,
        "Content-Type: text/event-stream\r\n",
        &events(&[
            r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"item|\"id\\tail","type":"function_call"}}"#,
            r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"item|\"id\\tail","type":"function_call","call_id":"call|\"id\\tail","name":"read","arguments":"{ \"path\" : \"a\" }"}}"#,
            TERMINAL,
        ]),
        false,
    )
}

fn completed(world: &World) -> &Completion {
    let terminals: Vec<_> = world
        .seen
        .iter()
        .filter_map(|event| match event {
            client::Event::Completed { owner, completion } => {
                assert_eq!(*owner, Token::new(51));
                Some(completion)
            }
            client::Event::Failed { .. } | client::Event::Cancelled { .. } => panic!("valid opaque IDs must complete"),
            client::Event::Delta { .. }
            | client::Event::Block { .. }
            | client::Event::Reusable
            | client::Event::Close
            | client::Event::Closed => None,
        })
        .collect();
    assert_eq!(terminals.len(), 1);
    terminals[0]
}

fn restored(block: &Block) -> Block {
    let Block::ToolCall { id, name, arguments, replay } = block else {
        panic!("actual function call");
    };
    assert_eq!(id.as_ref(), CALL_ID);
    assert_eq!(name.as_ref(), b"read");
    assert_eq!(arguments.as_ref(), br#"{ "path" : "a" }"#);
    let envelope = replay
        .as_ref()
        .expect("actual item identity")
        .to_bytes(&limits().dialect)
        .expect("admitted metadata fits durable envelope");
    let replay = Replay::from_bytes(&envelope, &limits().dialect).expect("durable transcript restored");
    assert_eq!(
        replay.value.to_bytes(&limits().dialect).expect("exact raw metadata").as_ref(),
        br#"{"item_id":"item|\"id\\tail"}"#
    );
    Block::ToolCall { id: id.clone(), name: name.clone(), arguments: arguments.clone(), replay: Some(replay) }
}

fn continuation(block: Block, result_id: &[u8]) -> openai::Request {
    let mut next = call(52);
    next.prompt.messages = Box::new([
        Message { role: Role::Assistant, content: Box::new([block]) },
        Message {
            role: Role::User,
            content: Box::new([Block::ToolResult {
                id: result_id.into(),
                text: b"actual file contents".as_slice().into(),
                is_error: false,
            }]),
        },
    ]);
    let mut world = World::new(next, limits(), text_response(false), 7);
    world.fragmentation(1, 1);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    assert_eq!(world.machine.waiting(), client::Waiting::Idle);
    let at = world.sent.windows(4).position(|bytes| bytes == b"\r\n\r\n").expect("actual HTTP request head") + 4;
    let json = skein_llm::Json::from_bytes(&world.sent[at..], &limits().dialect).expect("actual request document");
    let request = openai::decode_request(&json, &limits().dialect).expect("actual native continuation");
    world.request(client::Request::Close);
    world.settle();
    world.assert_once();
    request
}

fn paired(request: &openai::Request) -> bool {
    match request.input.as_ref() {
        [
            openai::Input::FunctionCall { call_id, item_id, name, arguments },
            openai::Input::FunctionOutput { call_id: result_id, output },
        ] => {
            call_id.as_ref() == CALL_ID
                && item_id.as_deref() == Some(ITEM_ID)
                && result_id.as_ref() == CALL_ID
                && name.as_ref() == b"read"
                && arguments.as_ref() == br#"{ "path" : "a" }"#
                && output.as_ref() == b"actual file contents"
        }
        _ => false,
    }
}

#[test]
fn pipe_and_escaped_ids_complete_restore_and_pair_exactly_on_actual_continuation() {
    let mut world = World::new(call(51), limits(), source(), 9);
    world.fragmentation(1, 1);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    let [block] = completed(&world).content.as_ref() else {
        panic!("one complete function call");
    };
    let restored = restored(block);
    assert_eq!(world.machine.waiting(), client::Waiting::Idle);
    world.request(client::Request::Close);
    world.settle();
    world.settle();
    world.assert_once();
    assert!(paired(&continuation(restored.clone(), CALL_ID)), "positive actual replay and result evidence");
    assert!(
        !paired(&continuation(restored.clone(), b"changed|call")),
        "altered actual result ID cannot satisfy pairing"
    );
    let Block::ToolCall { replay, .. } = restored.clone() else {
        panic!("restored function call");
    };
    let changed_call = Block::ToolCall {
        id: b"changed|call".as_slice().into(),
        name: b"read".as_slice().into(),
        arguments: br#"{ "path" : "a" }"#.as_slice().into(),
        replay,
    };
    assert!(!paired(&continuation(changed_call, CALL_ID)), "altered call identity cannot satisfy history");
    let Block::ToolCall { id, name, arguments, .. } = restored else {
        panic!("restored function call");
    };
    let changed_item = Block::ToolCall {
        id,
        name,
        arguments,
        replay: Some(Replay {
            provider: skein_llm::Provider::OpenAiCodex,
            value: skein_llm::Json::from_bytes(br#"{"item_id":"changed|item"}"#, &limits().dialect)
                .expect("bounded corruption"),
        }),
    };
    assert!(!paired(&continuation(changed_item, CALL_ID)), "altered item identity cannot satisfy history");
}

#[test]
fn actual_both_maximum_id_payloads_and_new_wrapper_fit_counted_client_bound() {
    let mut bounds = limits();
    bounds.dialect.document_bytes = 32768;
    bounds.dialect.request_bytes = 32768;
    bounds.dialect.answer_bytes = 32768;
    bounds.dialect.opaque_bytes = 8192;
    bounds.sse.line = 32768;
    bounds.sse.event = 32768;
    let bound = client::worst_case(&bounds).expect("checked enlarged wrapper and simultaneous payload bound");
    let meter = Meter::new();
    let id = "|".repeat(usize::try_from(bounds.dialect.string_bytes).expect("bounded maximum ID"));
    let added = format!(
        r#"{{"type":"response.output_item.added","output_index":0,"item":{{"id":"{id}","type":"function_call"}}}}"#
    );
    let done = format!(
        r#"{{"type":"response.output_item.done","output_index":0,"item":{{"id":"{id}","type":"function_call","call_id":"{id}","name":"read","arguments":"{{}}"}}}}"#
    );
    let wire = response(200, "Content-Type: text/event-stream\r\n", &events(&[&added, &done, TERMINAL]), false);
    let mut world = World::new(call(51), bounds, wire, 19);
    world.request(client::Request::Start);
    for _ in 0..100_000 {
        meter.start();
        let progress = world.tick(true);
        assert!(meter.end().peak() <= bound, "both actual maximum ID buffers and all wrappers fit {bound}");
        assert!(meter.held() <= bound);
        if !progress {
            break;
        }
    }
    let [Block::ToolCall { id: actual, replay, .. }] = completed(&world).content.as_ref() else {
        panic!("actual maximum IDs complete");
    };
    assert_eq!(actual.as_ref(), id.as_bytes());
    assert_eq!(
        replay
            .as_ref()
            .expect("maximum item ID replay")
            .value
            .to_bytes(&bounds.dialect)
            .expect("bounded raw item ID")
            .len(),
        id.len() + 14
    );
    world.request(client::Request::Close);
    world.settle();
    world.settle();
    world.assert_once();
    drop(world);
    drop(id);
    drop(added);
    drop(done);
    assert_eq!(meter.held(), 0, "actual settlement releases both separate IDs and enlarged wrappers");
}

#[test]
fn actual_escaped_item_identity_obeys_raw_metadata_cap_and_retains_lower_settlement() {
    let raw = br#"{"item_id":"item|\"id\\tail"}"#;
    for exact in [true, false] {
        let mut bounds = limits();
        bounds.dialect.opaque_bytes = u32::try_from(raw.len() - usize::from(!exact)).expect("tiny metadata cap");
        let mut world = World::new(call(51), bounds, source(), 13);
        world.request(client::Request::Start);
        world.run();
        world.assert_once();
        if exact {
            let [Block::ToolCall { replay, .. }] = completed(&world).content.as_ref() else {
                panic!("one admitted function call");
            };
            let envelope = replay
                .as_ref()
                .expect("exact item metadata")
                .to_bytes(&bounds.dialect)
                .expect("exact raw cap plus envelope header");
            assert_eq!(
                envelope.len(),
                raw.len() + usize::try_from(skein_llm::REPLAY_HEADER_BYTES).expect("small header")
            );
            assert_eq!(world.machine.waiting(), client::Waiting::Idle);
            world.request(client::Request::Close);
        } else {
            let terminals: Vec<_> = world
                .seen
                .iter()
                .filter(|event| {
                    matches!(
                        event,
                        client::Event::Completed { .. }
                            | client::Event::Failed { .. }
                            | client::Event::Cancelled { .. }
                    )
                })
                .collect();
            let [client::Event::Failed { failure: Failure::Limit, .. }] = terminals.as_slice() else {
                panic!("one-over item metadata fails before completion");
            };
            assert!(!world.seen.iter().any(|event| matches!(event, client::Event::Block { .. })));
            assert_eq!(world.machine.waiting(), client::Waiting::Closing);
            assert!(!world.seen.iter().any(|event| matches!(event, client::Event::Closed)));
        }
        world.settle();
        world.settle();
        world.assert_once();
        assert_eq!(world.machine.waiting(), client::Waiting::Nothing);
    }
}
