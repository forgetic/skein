//! An admitted Client completion always fits the durable replay envelope.
//! Native item metadata is hand-written independently of the product encoder.

use skein_lib::Token;
use skein_llm::{Block, Failure, client};
use skein_llm_world::{TERMINAL, World, call, events, limits, response};

#[derive(Clone, Copy)]
enum Kind {
    Text,
    Refusal,
    Tool,
}

fn source(kind: Kind) -> Vec<u8> {
    let documents: &[&str] = match kind {
        Kind::Text => &[
            r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"message\"item\\tail","type":"message"}}"#,
            r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"message\"item\\tail","type":"message","phase":"final_answer","content":[{"type":"output_text","text":"actual text"}]}}"#,
            TERMINAL,
        ],
        Kind::Refusal => &[
            r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"message\"item\\tail","type":"message"}}"#,
            r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"message\"item\\tail","type":"message","phase":"final_answer","content":[{"type":"refusal","refusal":"actual refusal"}]}}"#,
            TERMINAL,
        ],
        Kind::Tool => &[
            r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"function\"item\\tail","type":"function_call"}}"#,
            r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"function\"item\\tail","type":"function_call","call_id":"call_id","name":"caller_tool","arguments":"{}"}}"#,
            TERMINAL,
        ],
    };
    response(200, "Content-Type: text/event-stream\r\n", &events(documents), false)
}

fn raw(kind: Kind) -> &'static [u8] {
    match kind {
        Kind::Text | Kind::Refusal => br#"{"id":"message\"item\\tail","phase":"final_answer"}"#,
        Kind::Tool => br#"{"item_id":"function\"item\\tail"}"#,
    }
}

fn admitted(kind: Kind, owner: u64) {
    let mut bounds = limits();
    bounds.dialect.opaque_bytes = u32::try_from(raw(kind).len()).expect("tiny expected metadata");
    let mut world = World::new(call(owner), bounds, source(kind), 1);
    world.fragmentation(1, 1);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    let completion = world
        .seen
        .iter()
        .find_map(|event| match event {
            client::Event::Completed { owner: actual, completion } => {
                assert_eq!(*actual, Token::new(owner));
                Some(completion)
            }
            client::Event::Delta { .. }
            | client::Event::Block { .. }
            | client::Event::Failed { .. }
            | client::Event::Cancelled { .. }
            | client::Event::Reusable
            | client::Event::Close
            | client::Event::Closed => None,
        })
        .expect("exact raw metadata cap admits actual completion");
    let [block] = completion.content.as_ref() else {
        panic!("one actual native output block");
    };
    let replay = match block {
        Block::Text { text, replay } => {
            assert_eq!(text.as_ref(), b"actual text");
            replay.as_ref().expect("complete text replay")
        }
        Block::Refusal { text, replay } => {
            assert_eq!(text.as_ref(), b"actual refusal");
            replay.as_ref().expect("complete refusal replay")
        }
        Block::ToolCall { id, name, arguments, replay } => {
            assert_eq!(id.as_ref(), b"call_id");
            assert_eq!(name.as_ref(), b"caller_tool");
            assert_eq!(arguments.as_ref(), b"{}");
            replay.as_ref().expect("complete actual item ID")
        }
        Block::ToolResult { .. }
        | Block::Reasoning { .. }
        | Block::Oversize { .. }
        | Block::Cut { .. }
        | Block::Dropped { .. } => {
            panic!("expected native item metadata")
        }
    };
    assert_eq!(replay.value.to_bytes(&bounds.dialect).expect("actual replay serialization").as_ref(), raw(kind));
    let envelope = replay.to_bytes(&bounds.dialect).expect("every admitted actual replay fits its envelope");
    assert_eq!(envelope.len(), raw(kind).len() + usize::try_from(skein_llm::REPLAY_HEADER_BYTES).expect("tiny header"));
    assert_eq!(world.machine.waiting(), client::Waiting::Idle);
    assert!(world.seen.iter().any(|event| matches!(event, client::Event::Reusable)));
    world.request(client::Request::Close);
    world.settle();
    world.settle();
    world.assert_once();
    assert_eq!(world.machine.waiting(), client::Waiting::Nothing);
}

fn refused(kind: Kind, owner: u64) {
    let mut bounds = limits();
    bounds.dialect.opaque_bytes = u32::try_from(raw(kind).len() - 1).expect("positive tiny cap");
    let mut world = World::new(call(owner), bounds, source(kind), 1);
    world.fragmentation(1, 1);
    world.request(client::Request::Start);
    world.run();
    world.assert_once();
    let terminals: Vec<_> = world
        .seen
        .iter()
        .filter(|event| {
            matches!(
                event,
                client::Event::Completed { .. } | client::Event::Failed { .. } | client::Event::Cancelled { .. }
            )
        })
        .collect();
    let [client::Event::Failed { owner: actual, failure, evidence, .. }] = terminals.as_slice() else {
        panic!("one-over raw metadata produces actual Limit, never a completed value");
    };
    assert_eq!(*actual, Token::new(owner));
    assert_eq!(
        *failure,
        Failure::Limit { which: skein_llm::Cap::Opaque, bound: u64::from(bounds.dialect.opaque_bytes) }
    );
    assert_eq!(*evidence, client::Evidence::Response { status: 200 });
    assert!(
        !world.seen.iter().any(|event| matches!(event, client::Event::Block { .. })),
        "over-cap metadata cannot escape in a completed block"
    );
    assert_eq!(world.machine.waiting(), client::Waiting::Closing, "actual lower right remains outstanding");
    assert!(!world.seen.iter().any(|event| matches!(event, client::Event::Closed)));
    world.settle();
    world.settle();
    world.assert_once();
    assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Closed)).count(), 1);
    assert_eq!(world.machine.waiting(), client::Waiting::Nothing);
}

#[test]
fn actual_text_refusal_and_tool_metadata_fit_the_exact_raw_cap_or_fail_before_completion() {
    for kind in [Kind::Text, Kind::Refusal, Kind::Tool] {
        admitted(kind, 41);
        refused(kind, 42);
    }
}
