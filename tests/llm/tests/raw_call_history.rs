//! Actual malformed Codex calls remain exact correction history, not effects.
//! Handwritten native responses and full outside predicates are independent evidence.
use skein_lib::Token;
use skein_llm::{Block, Call, Completion, Error, Json, Message, Replay, Role, client, openai};
use skein_llm_world::{TERMINAL, World, call, events, limits, response, text_response};

const RAW: &[u8] = b"{broken\n\"\\";
const FIXED: &[u8] = br#"{ "path" : "corrected" }"#;
const FEEDBACK: &[u8] = b"Invalid: arguments must be a JSON object";
const NATIVE_FEEDBACK: &[u8] = b"Error: Invalid: arguments must be a JSON object";

fn source(corrected: bool) -> Vec<u8> {
    let documents = if corrected {
        [
            r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"fixed_item","type":"function_call"}}"#,
            r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"fixed_item","type":"function_call","call_id":"fixed_call","name":"host_action","arguments":"{ \"path\" : \"corrected\" }"}}"#,
            TERMINAL,
        ]
    } else {
        [
            r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"raw_item","type":"function_call"}}"#,
            r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"raw_item","type":"function_call","call_id":"raw_call","name":"host_action","arguments":"{broken\n\"\\"}}"#,
            TERMINAL,
        ]
    };
    response(200, "Content-Type: text/event-stream\r\n", &events(&documents), false)
}

fn completed(world: &World, owner: u64) -> &Completion {
    world.assert_once();
    let terminals: Vec<_> = world
        .seen
        .iter()
        .filter_map(|event| match event {
            client::Event::Completed { owner: actual, completion } => {
                assert_eq!(*actual, Token::new(owner));
                Some(completion)
            }
            client::Event::Failed { .. } | client::Event::Cancelled { .. } => panic!("actual call must complete"),
            client::Event::Delta { .. }
            | client::Event::Block { .. }
            | client::Event::Reusable
            | client::Event::Close
            | client::Event::Closed => None,
        })
        .collect();
    let [completion] = terminals.as_slice() else {
        panic!("one actual Completed");
    };
    completion
}

fn close(world: &mut World) {
    assert_eq!(world.machine.waiting(), client::Waiting::Idle);
    assert!(world.seen.iter().any(|event| matches!(event, client::Event::Reusable)));
    world.request(client::Request::Close);
    assert_eq!(world.machine.waiting(), client::Waiting::Closing);
    assert!(!world.seen.iter().any(|event| matches!(event, client::Event::Closed)));
    world.settle();
    world.settle();
    assert_eq!(world.machine.waiting(), client::Waiting::Nothing);
    assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Close)).count(), 1);
    assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Closed)).count(), 1);
    world.assert_once();
}

fn restore(block: &Block) -> Block {
    let Block::ToolCall { id, name, arguments, replay } = block else {
        panic!("actual raw call");
    };
    assert_eq!(id.as_ref(), b"raw_call");
    assert_eq!(name.as_ref(), b"host_action");
    assert_eq!(arguments.as_ref(), RAW);
    let durable =
        replay.as_ref().expect("actual item metadata").to_bytes(&limits().dialect).expect("bounded durable record");
    let replay = Replay::from_bytes(&durable, &limits().dialect).expect("restore actual metadata");
    assert_eq!(replay.value.to_bytes(&limits().dialect).expect("raw metadata").as_ref(), br#"{"item_id":"raw_item"}"#);
    Block::ToolCall { id: id.clone(), name: name.clone(), arguments: arguments.clone(), replay: Some(replay) }
}

fn next(block: Block, error: bool) -> Call {
    let mut input = call(62);
    input.prompt.messages = Box::new([
        Message { role: Role::Assistant, content: Box::new([block]) },
        Message {
            role: Role::User,
            content: Box::new([Block::ToolResult {
                id: b"raw_call".as_slice().into(),
                text: FEEDBACK.into(),
                is_error: error,
            }]),
        },
    ]);
    input
}

fn native(world: &World) -> openai::Request {
    let at = world.sent.windows(4).position(|bytes| bytes == b"\r\n\r\n").expect("actual request head") + 4;
    let document = Json::from_bytes(&world.sent[at..], &world.env.limits.dialect).expect("actual native request");
    openai::decode_request(&document, &world.env.limits.dialect).expect("raw argument string is representable")
}

fn evidence(request: &openai::Request) -> bool {
    match request.input.as_ref() {
        [
            openai::Input::FunctionCall { call_id, item_id, name, arguments },
            openai::Input::FunctionOutput { call_id: answer_id, output },
        ] => {
            call_id.as_ref() == b"raw_call"
                && item_id.as_deref() == Some(b"raw_item".as_slice())
                && name.as_ref() == b"host_action"
                && arguments.as_ref() == RAW
                && answer_id.as_ref() == b"raw_call"
                && output.as_ref() == NATIVE_FEEDBACK
        }
        _ => false,
    }
}

fn corruptions(actual: &openai::Request) {
    assert!(evidence(actual), "closed positive history before any corruption");
    for field in 0_u32..7 {
        let mut changed = actual.clone();
        match changed.input.as_mut() {
            [
                openai::Input::FunctionCall { call_id, item_id, name: _, arguments },
                openai::Input::FunctionOutput { call_id: answer_id, output },
            ] => match field {
                0 => *arguments = b"{}".as_slice().into(),
                1 => *arguments = Box::new([]),
                2 => *call_id = b"different".as_slice().into(),
                3 => *item_id = None,
                4 => *answer_id = b"different".as_slice().into(),
                5 => *output = Box::new([]),
                6 => *output = b"Invalid: rewritten".as_slice().into(),
                _ => panic!("bounded corruption index"),
            },
            _ => panic!("positive native shape"),
        }
        assert!(!evidence(&changed), "corruption {field} cannot satisfy exact outside evidence");
    }
    let mut absent = actual.clone();
    absent.input = Box::new([actual.input[0].clone()]);
    assert!(!evidence(&absent), "missing feedback cannot satisfy continuation");
}

#[test]
fn actual_malformed_call_replays_with_exact_error_feedback_and_corrects() {
    let mut first = World::new(call(61), limits(), source(false), 17);
    first.fragmentation(1, 1);
    first.request(client::Request::Start);
    first.run();
    let [block] = completed(&first, 61).content.as_ref() else {
        panic!("one actual malformed call");
    };
    assert!(Json::from_bytes(RAW, &limits().dialect).is_err(), "application must not execute this body");
    let restored = restore(block);
    close(&mut first);
    let mut second = World::new(next(restored.clone(), true), limits(), source(true), 29);
    second.fragmentation(1, 1);
    second.request(client::Request::Start);
    second.run();
    let [Block::ToolCall { id, name, arguments, .. }] = completed(&second, 62).content.as_ref() else {
        panic!("actual corrected call");
    };
    assert_eq!(id.as_ref(), b"fixed_call");
    assert_eq!(name.as_ref(), b"host_action");
    assert_eq!(arguments.as_ref(), FIXED);
    assert!(Json::from_bytes(arguments, &limits().dialect).is_ok(), "corrected object is representable");
    let observed = native(&second);
    corruptions(&observed);
    close(&mut second);
    let mut wrong_error = World::new(next(restored, false), limits(), text_response(false), 31);
    wrong_error.request(client::Request::Start);
    wrong_error.run();
    completed(&wrong_error, 62);
    assert!(!evidence(&native(&wrong_error)), "success-classified feedback cannot satisfy error history");
    close(&mut wrong_error);
}

fn raw_request(arguments: &[u8]) -> openai::Request {
    openai::Request {
        model: b"m".as_slice().into(),
        instructions: Box::new([]),
        tools: Box::new([]),
        input: Box::new([openai::Input::FunctionCall {
            call_id: b"c".as_slice().into(),
            item_id: None,
            name: b"n".as_slice().into(),
            arguments: arguments.into(),
        }]),
        effort: None,
        prompt_cache_key: None,
    }
}

fn history(arguments: &[u8]) -> Call {
    let mut input = call(71);
    input.prompt.model = b"m".as_slice().into();
    input.prompt.instructions = Box::new([]);
    input.prompt.cache_key = None;
    input.prompt.messages = Box::new([Message {
        role: Role::Assistant,
        content: Box::new([Block::ToolCall {
            id: b"c".as_slice().into(),
            name: b"n".as_slice().into(),
            arguments: arguments.into(),
            replay: None,
        }]),
    }]);
    input
}

#[test]
fn native_and_client_admission_bound_raw_strings_and_utf8_without_inner_parsing() {
    let mut bounds = limits();
    bounds.dialect.string_bytes = 8;
    for raw in [b"".as_slice(), b"broken", b"123", b"[]", b"\"\\\n{bad!"] {
        let request = raw_request(raw);
        let wire = openai::encode_request(&request, &bounds.dialect).expect("bounded native argument string");
        let value = Json::from_bytes(&wire, &limits().dialect).expect("complete actual outer document");
        assert_eq!(openai::decode_request(&value, &bounds.dialect).expect("native raw string roundtrip"), request);
        assert!(client::Client::prepare(history(raw), &bounds).is_ok(), "generic admission accepts bounded raw text");
    }
    let exact = b"\"\\\n{bad!";
    assert_eq!(exact.len(), 8, "attained raw cap");
    assert!(client::Client::prepare(history(exact), &bounds).is_ok());
    let over = b"\"\\\n{bad!!";
    assert_eq!(
        openai::encode_request(&raw_request(over), &bounds.dialect),
        Err(openai::DecodeError::TooLarge { which: skein_llm::Cap::String, bound: 8 })
    );
    assert!(matches!(client::Client::prepare(history(over), &bounds), Err(Error::Limit { .. })));
    assert_eq!(openai::encode_request(&raw_request(&[0xff]), &bounds.dialect), Err(openai::DecodeError::Malformed));
    assert!(matches!(client::Client::prepare(history(&[0xff]), &bounds), Err(Error::Invalid)));
}

#[test]
fn native_and_actual_client_request_bounds_charge_complete_escaped_history() {
    let request = raw_request(RAW);
    let mut bounds = limits();
    let wire = openai::encode_request(&request, &bounds.dialect).expect("escaped malformed string fits");
    bounds.dialect.request_bytes = u32::try_from(wire.len()).expect("small actual native document");
    assert_eq!(openai::encode_request(&request, &bounds.dialect).expect("exact escaped cap"), wire);
    bounds.dialect.request_bytes -= 1;
    assert_eq!(
        openai::encode_request(&request, &bounds.dialect),
        Err(openai::DecodeError::TooLarge {
            which: skein_llm::Cap::Request,
            bound: u64::from(bounds.dialect.request_bytes)
        })
    );

    let mut world = World::new(history(RAW), limits(), text_response(false), 37);
    world.request(client::Request::Start);
    world.run();
    completed(&world, 71);
    let at = world.sent.windows(4).position(|bytes| bytes == b"\r\n\r\n").expect("actual native body") + 4;
    let length = world.sent.len() - at;
    close(&mut world);
    bounds = limits();
    bounds.dialect.request_bytes = u32::try_from(length).expect("small escaped request");
    let mut exact = World::new(history(RAW), bounds, text_response(false), 41);
    exact.request(client::Request::Start);
    exact.run();
    completed(&exact, 71);
    assert_eq!(exact.sent.len() - at, length, "whole actual escaped body reaches receiving cap");
    close(&mut exact);
    bounds.dialect.request_bytes -= 1;
    assert!(
        matches!(client::Client::prepare(history(RAW), &bounds), Err(Error::Limit { .. })),
        "refusal occurs before Start/stream/terminal"
    );
}

fn anthropic_history(arguments: &[u8]) -> Call {
    let mut input = history(arguments);
    input.endpoint = skein_llm::Endpoint::anthropic();
    input.credential = skein_llm::Credential::anthropic(b"caller-token".as_slice().into());
    input.prompt.max_output_tokens = Some(32);
    input
}

#[test]
fn anthropic_embedded_object_history_remains_honest() {
    assert!(
        client::Client::prepare(anthropic_history(br#"{"path":"wrong-for-application"}"#), &limits()).is_ok(),
        "representable application-invalid object is legal history"
    );
    assert!(
        matches!(client::Client::prepare(anthropic_history(RAW), &limits()), Err(Error::Invalid)),
        "unrepresentable native object cannot be rewritten or dropped"
    );
}
