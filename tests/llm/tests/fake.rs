//! Positive controls and malformed histories for the shared byte peer.

use skein_fake_llm_domain::api::{Finish, Line, Script, Turn};
use skein_llm::{Block, Completion, Message, Role, Tool, client};
use skein_llm_world::{call, fake::Exchange, limits};

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

fn input(provider: skein_llm::Provider, owner: u64) -> skein_llm::Call {
    let mut input = call(owner);
    match provider {
        skein_llm::Provider::OpenAiCodex => {}
        skein_llm::Provider::Anthropic => {
            input.endpoint = skein_llm::Endpoint::anthropic();
            input.credential = skein_llm::Credential::anthropic(b"fake-token".as_slice().into());
            input.prompt.cache_key = None;
        }
    }
    input.prompt.output_ceiling(provider, 4096).expect("shared provider configuration");
    input.prompt.instructions = b"caller-script".as_slice().into();
    input.prompt.tools = Box::new([Tool {
        name: b"caller_tool".as_slice().into(),
        description: b"Caller description".as_slice().into(),
        schema: skein_llm::Json::from_bytes(
            br#"{"type":"object","properties":{"opaque":{"type":"string"}},"x-caller-extension":[1,null,true]}"#,
            &limits().dialect,
        )
        .expect("whole caller schema"),
    }]);
    input
}

fn scripts() -> Box<[Script]> {
    Box::new([Script {
        cue: b"caller-script".as_slice().into(),
        turns: Box::new([
            Turn {
                lines: Box::new([Line::Call {
                    name: b"caller_tool".as_slice().into(),
                    arguments: br#"{ "opaque" : "whole body" }"#.as_slice().into(),
                }]),
                finish: Finish::ToolCalls,
                tokens: 8,
            },
            Turn {
                lines: Box::new([Line::Text { text: b"actual continuation".as_slice().into() }]),
                finish: Finish::Stop,
                tokens: 2,
            },
        ]),
    }])
}

fn completion(world: &Exchange) -> &Completion {
    let values: Vec<_> = world
        .seen
        .iter()
        .filter_map(|event| match event {
            client::Event::Completed { completion, .. } => Some(completion),
            client::Event::Delta { .. }
            | client::Event::Block { .. }
            | client::Event::Failed { .. }
            | client::Event::Cancelled { .. }
            | client::Event::Reusable
            | client::Event::Close
            | client::Event::Closed => None,
        })
        .collect();
    let [value] = values.as_slice() else {
        panic!("one actual completed terminal required: {:?}", world.seen);
    };
    value
}

fn check_feedback(provider: skein_llm::Provider, text: &[u8], error: bool) {
    match provider {
        skein_llm::Provider::Anthropic => {
            assert_eq!(text, b"actual feedback");
            assert!(error, "native error flag reaches peer");
        }
        skein_llm::Provider::OpenAiCodex => {
            assert_eq!(text, b"Error: actual feedback");
            assert!(!error, "Codex encodes error in exact text");
        }
    }
}

#[test]
fn actual_client_and_independent_byte_peer_relay_whole_schema_call_and_feedback() {
    for provider in [skein_llm::Provider::OpenAiCodex, skein_llm::Provider::Anthropic] {
        let mut world = Exchange::new(input(provider, 1), limits(), scripts());
        world.start();
        world.run();
        let [query] = world.queries.as_slice() else {
            panic!("one actual provider request");
        };
        assert_eq!(query.tools[0].name.as_ref(), b"caller_tool");
        assert_eq!(query.tools[0].description.as_ref(), b"Caller description");
        assert_eq!(
            skein_llm::Json::from_bytes(&query.tools[0].parameters, &limits().dialect).expect("actual schema"),
            input(provider, 9).prompt.tools[0].schema,
            "whole semantic schema survives shared client and peer"
        );
        let answer = completion(&world).clone();
        let [Block::ToolCall { id, name, arguments, .. }] = &*answer.content else {
            panic!("one real tool call");
        };
        assert_eq!(name.as_ref(), b"caller_tool");
        assert_eq!(arguments.as_ref(), br#"{ "opaque" : "whole body" }"#);
        assert!(
            world.seen.iter().any(|event| matches!(event, client::Event::Reusable)),
            "actual HTTP drainage permits reuse"
        );
        let mut next = input(provider, 2);
        next.prompt.messages = Box::new([
            Message {
                role: Role::User,
                content: Box::new([Block::Text { text: b"start".as_slice().into(), replay: None }]),
            },
            Message { role: Role::Assistant, content: answer.content.clone() },
            Message {
                role: Role::User,
                content: Box::new([Block::ToolResult {
                    id: id.clone(),
                    text: b"actual feedback".as_slice().into(),
                    is_error: true,
                }]),
            },
        ]);
        let prepared = client::Client::prepare(next, &world.env.limits)
            .expect("same immutable schema and exact actual answer replay");
        assert!(world.machine.next_call(prepared).is_ok(), "actual reusable binding accepts next call");
        world.seen.clear();
        world.queries.clear();
        world.request(client::Request::Start);
        world.run();
        let [query] = world.queries.as_slice() else {
            panic!("one continuation request");
        };
        let outputs: Vec<_> = query
            .messages
            .iter()
            .flat_map(|message| message.parts.iter())
            .filter_map(|part| match part {
                skein_fake_llm_domain::api::Part::ToolOutput { id, output, is_error } => Some((id, output, is_error)),
                skein_fake_llm_domain::api::Part::Text { .. }
                | skein_fake_llm_domain::api::Part::Opaque { .. }
                | skein_fake_llm_domain::api::Part::ToolCall { .. } => None,
            })
            .collect();
        let [(actual_id, text, error)] = outputs.as_slice() else {
            panic!("exact paired feedback reached independent peer");
        };
        assert_eq!(*actual_id, id);
        check_feedback(provider, text, **error);
        assert!(
            completion(&world)
                .content
                .iter()
                .any(|block| matches!(block, Block::Text { text, .. } if text.as_ref() == b"actual continuation"))
        );

        // A corrupted provider-ID history goes through actual Client and is refused by the outside peer.
        let mut bad = input(provider, 3);
        bad.prompt.messages = Box::new([
            Message { role: Role::Assistant, content: answer.content.clone() },
            Message {
                role: Role::User,
                content: Box::new([Block::ToolResult {
                    id: b"wrong-id".as_slice().into(),
                    text: b"actual feedback".as_slice().into(),
                    is_error: true,
                }]),
            },
        ]);
        let mut bad = Exchange::new(bad, limits(), scripts());
        bad.start();
        bad.run();
        assert!(
            bad.seen
                .iter()
                .any(|event| matches!(event, client::Event::Failed { failure: skein_llm::Failure::Invalid, .. })),
            "outside peer refuses corrupted feedback identity"
        );
    }
}

#[test]
fn cancelling_an_actual_peer_exchange_retains_the_terminal_until_lower_settlement() {
    let mut world = Exchange::new(input(skein_llm::Provider::OpenAiCodex, 1), limits(), scripts());
    world.start();
    world.request(client::Request::Cancel);
    assert!(
        !world.seen.iter().any(|event| matches!(event, client::Event::Cancelled { .. })),
        "requesting lower close is not actual settlement"
    );
    world.settle();
    assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Cancelled { .. })).count(), 1);
    assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Closed)).count(), 1);
    world.settle();
    assert_eq!(world.seen.iter().filter(|event| matches!(event, client::Event::Cancelled { .. })).count(), 1);
}

fn awaiting_domain(world: &mut Exchange) {
    for _ in 0..100_000_u32 {
        if !world.tick(true) {
            assert_eq!(world.pending.len(), 1, "actual HTTP request issued one domain right");
            return;
        }
    }
    panic!("bounded routing entrance stalled");
}

fn routing_scripts() -> Box<[Script]> {
    Box::new([Script {
        cue: b"caller-script".as_slice().into(),
        turns: Box::new([Turn {
            lines: Box::new([Line::Text { text: b"routed exact".as_slice().into() }]),
            finish: Finish::Stop,
            tokens: 1,
        }]),
    }])
}

fn routing_bound() -> u64 {
    let peer = skein_llm_world::fake::limits(&limits());
    let one = client::worst_case(&limits()).expect("client bound")
        + skein_fake_llm_protocol::provider::worst_case(&peer).expect("one Server and Service bound")
        + skein_fake_llm_domain::worst_case(&skein_llm_world::fake::config()).expect("internal script domain bound");
    2 * one
        + skein_fake_llm_domain::worst_case(&skein_llm_world::fake::config())
            .expect("external actual script domain bound")
        + 8 * 32768
        + 16 * u64::from(limits().dialect.document_bytes)
        + 128 * 1024
}

#[test]
fn actual_two_connection_routing_preserves_each_terminal_and_returns_a_closed_route() {
    use skein_fake_llm_domain::{Domain, fire, step};
    use skein_fake_llm_protocol::provider;
    use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
    let bound = routing_bound();
    // Each connection owns its Client, Server, spare Service, unused internal
    // domain, two intakes, queues and observation tapes. The shared delayed
    // domain is additional; no native connection storage is charged as inline routing slots.
    let span = skein_heap::Span::start();
    let dialect = skein_llm::Provider::OpenAiCodex;
    let mut first = Exchange::new(input(dialect, 1), limits(), scripts());
    let mut second = Exchange::new(input(dialect, 2), limits(), scripts());
    first.manual_replies = true;
    second.manual_replies = true;
    second.server = provider::Server::new(Token::new(3), &skein_llm_world::fake::limits(&limits()))
        .expect("second actual connection");
    first.start();
    awaiting_domain(&mut first);
    std::mem::swap(&mut first.service, &mut second.service);
    second.start();
    awaiting_domain(&mut second);
    std::mem::swap(&mut first.service, &mut second.service);
    assert_eq!(first.service.count(), 2, "both actual HTTP requests share one service");
    let config = skein_fake_llm_domain::Config {
        latency_min: Duration::from_secs(1),
        latency_max: Duration::from_secs(1),
        ..skein_llm_world::fake::config()
    };
    let mut domain = Domain::try_scripted(&config, 7, routing_scripts()).expect("shared actual script domain");
    let mut domain_env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: config };
    let mut replies = Queue::with_capacity(skein_fake_llm_domain::MAX_OUT);
    step(&mut domain, &domain_env, first.pending.pop().expect("first actual right"), &mut replies);
    step(&mut domain, &domain_env, second.pending.pop().expect("second actual right"), &mut replies);
    assert!(replies.pop().is_none(), "both actual decisions await injected deadlines");
    assert_eq!(domain.calls(), 2);
    domain_env.now = Time::from_nanos(1_000_000_000);
    fire(&mut domain, &domain_env, &mut replies);
    let reply = replies.pop().expect("first actual delayed terminal");
    let (target, reply) = first.service.target(reply).expect("first route retains terminal");
    assert_eq!(target, Token::new(2));
    first.reply(reply);
    first.run();
    let [Block::Text { text, .. }] = &*completion(&first).content else {
        panic!("first actual terminal");
    };
    assert_eq!(text.as_ref(), b"routed exact");
    fire(&mut domain, &domain_env, &mut replies);
    let reply = replies.pop().expect("second actual delayed terminal");
    let (target, reply) = first.service.target(reply).expect("second route retains terminal");
    assert_eq!(target, Token::new(3));
    std::mem::swap(&mut first.service, &mut second.service);
    second.reply(reply);
    second.run();
    std::mem::swap(&mut first.service, &mut second.service);
    let [Block::Text { text, .. }] = &*completion(&second).content else {
        panic!("second actual terminal");
    };
    assert_eq!(text.as_ref(), b"routed exact");
    domain.reclaim();
    assert_eq!(domain.calls(), 0, "both emitted terminals retire their domain slots");

    let next = client::Client::prepare(input(dialect, 3), &limits()).expect("reused actual client call");
    assert!(second.machine.next_call(next).is_ok());
    second.seen.clear();
    std::mem::swap(&mut first.service, &mut second.service);
    second.request(client::Request::Start);
    awaiting_domain(&mut second);
    step(&mut domain, &domain_env, second.pending.pop().expect("third actual issued right"), &mut replies);
    assert!(replies.pop().is_none(), "third actual decision also waits");
    second.request(client::Request::Cancel);
    second.settle();
    std::mem::swap(&mut first.service, &mut second.service);
    domain_env.now = Time::from_nanos(2_000_000_000);
    fire(&mut domain, &domain_env, &mut replies);
    let reply = replies.pop().expect("third actual terminal retained past stream close");
    let Err(returned) = first.service.target(reply) else {
        panic!("closed actual connection cannot consume a delayed terminal");
    };
    let skein_fake_llm_domain::Request::Reply { result, .. } = returned;
    let answer = result.expect("same owned terminal is returned to caller");
    let [skein_fake_llm_domain::api::Part::Text { text }] = &*answer.parts else {
        panic!("same terminal part");
    };
    assert_eq!(text.as_ref(), b"routed exact");
    assert_eq!(first.service.count(), 3, "no duplicate wire request was fabricated");
    assert!(
        !second.seen.iter().any(|event| matches!(event, client::Event::Completed { .. })),
        "closed route cannot deliver a second decision"
    );
    let grown = span.end();
    assert!(
        grown.peak <= i64::try_from(bound).expect("checked counted comparison"),
        "two native connections peak {} exceeds {bound}",
        grown.peak
    );
}

fn opaque_script(opaque: &[u8]) -> Box<[Script]> {
    Box::new([Script {
        cue: b"caller-script".as_slice().into(),
        turns: Box::new([
            Turn {
                lines: Box::new([
                    Line::Opaque { bytes: opaque.into() },
                    Line::Text { text: b"actual refusal".as_slice().into() },
                ]),
                finish: Finish::ContentFilter,
                tokens: 17,
            },
            Turn {
                lines: Box::new([Line::Text { text: b"resumed exact".as_slice().into() }]),
                finish: Finish::Stop,
                tokens: 3,
            },
        ]),
    }])
}

#[test]
fn actual_byte_peer_preserves_opaque_extensions_refusal_stop_and_continuation_replay() {
    for (provider, opaque) in [
        (skein_llm::Provider::OpenAiCodex, br#"{"type":"reasoning","id":"opaque_1","encrypted_content":"signed","summary":[],"extension":{"signed":true}}"#.as_slice()),
        (skein_llm::Provider::Anthropic, br#"{"type":"thinking","extension":{"signed":true},"thinking":"visible","signature":"signed"}"#.as_slice()),
        (skein_llm::Provider::Anthropic, br#"{"type":"future_block","proof":{"a":[1,2]},"data":"opaque"}"#.as_slice()),
    ] {
        let mut world = Exchange::new(input(provider, 1), limits(), opaque_script(opaque));
        world.start();
        world.run();
        let answer = completion(&world).clone();
        assert_eq!(answer.stop, skein_llm::Stop::Refusal, "actual native refusal terminal");
        let [Block::Reasoning { replay }, Block::Text { text, .. }] = &*answer.content else {
            panic!("actual opaque and visible content");
        };
        assert_eq!(text.as_ref(), b"actual refusal");
        let restored = skein_llm::Replay::from_bytes(
            &replay.to_bytes(&limits().dialect).expect("durable envelope"),
            &limits().dialect,
        )
        .expect("restore actual replay");
        assert_eq!(
            restored.value,
            skein_llm::Json::from_bytes(opaque, &limits().dialect).expect("whole caller opaque value")
        );
        let [query] = world.queries.as_slice() else {
            panic!("one outside request");
        };
        let fresh = u64::try_from(query.system.len() + 5).expect("Hello and actual system bytes") / 4;
        assert_eq!(answer.usage.input_tokens, fresh);
        assert_eq!(answer.usage.output_tokens, 17);
        assert_eq!(answer.usage.cache_read_tokens, 0);
        assert_eq!(
            answer.usage.cache_write_tokens,
            match provider {
                skein_llm::Provider::Anthropic => fresh,
                skein_llm::Provider::OpenAiCodex => 0,
            }
        );
        let mut next = input(provider, 2);
        next.prompt.messages = Box::new([
            Message { role: Role::Assistant, content: answer.content },
            Message {
                role: Role::User,
                content: Box::new([Block::Text { text: b"continue".as_slice().into(), replay: None }]),
            },
        ]);
        let next = client::Client::prepare(next, &limits()).expect("actual retained opaque replay admitted");
        assert!(world.machine.next_call(next).is_ok(), "actual HTTP reusable boundary");
        world.seen.clear();
        world.queries.clear();
        world.request(client::Request::Start);
        world.run();
        let [query] = world.queries.as_slice() else {
            panic!("one outside continuation request");
        };
        let [skein_fake_llm_domain::api::Part::Opaque { bytes }, skein_fake_llm_domain::api::Part::Text { text }] =
            &*query.messages[0].parts
        else {
            panic!("opaque and visible prior completion reached native request peer");
        };
        assert_eq!(
            skein_llm::Json::from_bytes(bytes, &limits().dialect).expect("actual continuation opaque document"),
            restored.value
        );
        assert_eq!(text.as_ref(), b"actual refusal");
        let terminal = completion(&world);
        assert_eq!(terminal.stop, skein_llm::Stop::EndTurn);
        let [Block::Text { text, .. }] = &*terminal.content else {
            panic!("actual ordinary continuation");
        };
        assert_eq!(text.as_ref(), b"resumed exact");
        assert_eq!(terminal.usage.input_tokens, 2, "eight continuation text bytes read fresh");
        assert_eq!(terminal.usage.output_tokens, 3);
        assert_eq!(
            terminal.usage.cache_read_tokens,
            u64::try_from(query.system.len() + bytes.len() + b"actual refusal".len())
                .expect("exact prior content bytes")
                / 4
        );
        assert_eq!(
            terminal.usage.cache_write_tokens,
            match provider {
                skein_llm::Provider::Anthropic => 2,
                skein_llm::Provider::OpenAiCodex => 0,
            }
        );
    }
}
