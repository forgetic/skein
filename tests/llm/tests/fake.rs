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
            input.prompt.affinity = None;
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
fn prepared_client_is_adopted_and_drives_the_actual_scripted_peer() {
    for provider in [skein_llm::Provider::OpenAiCodex, skein_llm::Provider::Anthropic] {
        let input = input(provider, 17);
        let endpoint = input.endpoint.clone();
        let credential = skein_llm::Credential {
            access_token: input.credential.access_token.clone(),
            account_id: input.credential.account_id.clone(),
        };
        let machine = client::Client::prepare(input, &limits()).expect("caller prepares its one Client");
        let mut world = Exchange::prepared(machine, endpoint, credential, limits(), scripts());
        world.start();
        world.run();
        let [query] = world.queries.as_slice() else {
            panic!("the adopted Client makes one actual peer request");
        };
        assert_eq!(query.system.as_ref(), b"caller-script");
        let [tool] = query.tools.as_ref() else {
            panic!("the prepared application tool survives adoption");
        };
        assert_eq!(tool.name.as_ref(), b"caller_tool");
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
        let [client::Event::Completed { owner, completion }] = terminals.as_slice() else {
            panic!("the adopted call has one actual successful terminal");
        };
        assert_eq!(*owner, skein_lib::Token::new(17), "adoption preserves the original callback owner");
        let [Block::ToolCall { name, arguments, .. }] = completion.content.as_ref() else {
            panic!("the real script domain returns the configured tool call");
        };
        assert_eq!(name.as_ref(), b"caller_tool");
        assert_eq!(arguments.as_ref(), br#"{ "opaque" : "whole body" }"#);
        assert!(
            world.seen.iter().any(|event| matches!(event, client::Event::Reusable)),
            "adopted actual HTTP drainage completes before reuse"
        );
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
        assert_eq!(answer.usage.input, Some(0));
        assert_eq!(answer.usage.output, Some(17));
        assert_eq!(answer.usage.cache_read, Some(0));
        assert_eq!(
            answer.usage.cache_write,
Some(fresh)
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
        assert_eq!(terminal.usage.input, Some(0), "continuation text is written to the cache");
        assert_eq!(terminal.usage.output, Some(3));
        assert_eq!(
            terminal.usage.cache_read,
Some(            u64::try_from(query.system.len() + bytes.len() + b"actual refusal".len())
                .expect("exact prior content bytes")
                / 4)
        );
        assert_eq!(
            terminal.usage.cache_write,
Some(2)
        );
    }
}

#[test]
fn tool_choice_none_and_an_outside_call_cross_both_actual_wires() {
    use skein_fake_llm_domain::{Domain, api, fire, step};
    use skein_lib::{Duration, Env, Queue, Time, Wall};
    for provider in [skein_llm::Provider::OpenAiCodex, skein_llm::Provider::Anthropic] {
        for outside_choice in [0, 1000] {
            let mut input = input(provider, 1);
            input.prompt.instructions = Box::new([]);
            input.prompt.choice = skein_llm::ToolChoice::None;
            let mut world = Exchange::new(input, limits(), Box::new([]));
            world.manual_replies = true;
            world.start();
            awaiting_domain(&mut world);
            let config = skein_fake_llm_domain::Config {
                tool_rounds: 1,
                outside_choice,
                latency_min: Duration::ZERO,
                latency_max: Duration::ZERO,
                ..skein_llm_world::fake::config()
            };
            let mut domain = Domain::configured(
                &config,
                3,
                Box::new([]),
                api::Menu { arguments: Box::new([b"{}".as_slice().into()]), invalid: Box::new([]) },
            )
            .expect("bounded actual domain");
            let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: config };
            let mut out = Queue::with_capacity(skein_fake_llm_domain::MAX_OUT);
            let [query] = world.queries.as_slice() else { panic!("actual decoded query") };
            assert_eq!(query.choice, api::ToolChoice::None);
            assert_eq!(query.tools.len(), 1, "offered tools are never filtered");
            step(&mut domain, &env, world.pending.pop().expect("actual call right"), &mut out);
            fire(&mut domain, &env, &mut out);
            world.reply(out.pop().expect("one actual domain terminal"));
            world.run();
            let answer = completion(&world);
            if outside_choice == 0 {
                assert_eq!(answer.stop, skein_llm::Stop::EndTurn);
                assert!(
                    answer.content.iter().all(|block| matches!(block, Block::Text { .. })),
                    "None gives text without a call"
                );
            } else {
                let [Block::ToolCall { name, .. }] = answer.content.as_ref() else {
                    panic!("outside call reaches caller")
                };
                assert_eq!(name.as_ref(), b"caller_tool");
            }
        }
    }
}

#[test]
fn scripted_calls_are_delivered_even_when_tool_choice_is_none() {
    for provider in [skein_llm::Provider::OpenAiCodex, skein_llm::Provider::Anthropic] {
        let mut input = input(provider, 1);
        input.prompt.choice = skein_llm::ToolChoice::None;
        let mut world = Exchange::new(input, limits(), scripts());
        world.start();
        world.run();
        let [Block::ToolCall { name, .. }] = completion(&world).content.as_ref() else {
            panic!("scripts call what they name")
        };
        assert_eq!(name.as_ref(), b"caller_tool");
    }
}

#[test]
fn escaped_calls_at_the_input_edge_and_one_over_complete_beside_a_normal_call() {
    let arguments = br#"{"x":"\u0000\u0000"}"#;
    for provider in [skein_llm::Provider::OpenAiCodex, skein_llm::Provider::Anthropic] {
        for below in [false, true] {
            let mut bounds = limits();
            bounds.dialect.input_bytes = u32::try_from(arguments.len()).expect("tiny argument cap") - u32::from(below);
            let scripts = Box::new([Script {
                cue: b"caller-script".as_slice().into(),
                turns: Box::new([Turn {
                    lines: Box::new([
                        Line::Call { name: b"caller_tool".as_slice().into(), arguments: arguments.as_slice().into() },
                        Line::Call { name: b"caller_tool".as_slice().into(), arguments: b"{}".as_slice().into() },
                    ]),
                    finish: Finish::ToolCalls,
                    tokens: 8,
                }]),
            }]);
            let mut world = Exchange::new(input(provider, 1), bounds, scripts);
            world.start();
            world.run();
            let answer = completion(&world);
            assert_eq!(answer.stop, skein_llm::Stop::ToolUse);
            assert_eq!(answer.content.len(), 2);
            if below {
                let Block::Oversize { id, name, bytes } = &answer.content[0] else {
                    panic!("one over is an oversize block")
                };
                assert_eq!(id.as_ref(), b"call_0000000000000001");
                assert_eq!(name.as_ref(), b"caller_tool");
                assert_eq!(*bytes, u64::try_from(arguments.len()).expect("bounded bytes"));
                let mut next = input(provider, 2);
                next.prompt.messages = Box::new([Message { role: Role::Assistant, content: answer.content.clone() }]);
                assert_eq!(client::Client::prepare(next, &limits()).err(), Some(skein_llm::Error::Invalid));
            } else {
                let Block::ToolCall { arguments: actual, .. } = &answer.content[0] else {
                    panic!("exact edge is a call")
                };
                assert_eq!(actual.as_ref(), arguments);
            }
            assert!(matches!(&answer.content[1], Block::ToolCall { arguments, .. } if arguments.as_ref() == b"{}"));
        }
    }
}

#[test]
fn scripted_provider_output_cuts_complete_and_are_refused_in_history() {
    for provider in [skein_llm::Provider::OpenAiCodex, skein_llm::Provider::Anthropic] {
        let mut call_input = input(provider, 1);
        call_input.prompt.output_ceiling(provider, 1).expect("caller output ceiling");
        let mut world = Exchange::new(call_input, limits(), scripts());
        world.model_ceiling(1);
        world.start();
        world.run();
        let answer = completion(&world);
        assert_eq!(answer.stop, skein_llm::Stop::MaxTokens);
        let [Block::Cut { id, name, arguments }] = answer.content.as_ref() else {
            panic!("one cut outcome: {answer:?}")
        };
        assert_eq!(id.as_ref(), b"call_0000000000000001");
        assert_eq!(name.as_ref(), b"caller_tool");
        assert_eq!(arguments.as_ref(), br#"{ ""#);
        let mut next = input(provider, 2);
        next.prompt.messages = Box::new([Message { role: Role::Assistant, content: answer.content.clone() }]);
        assert_eq!(client::Client::prepare(next, &limits()).err(), Some(skein_llm::Error::Invalid));
    }
}

#[test]
fn oversized_arguments_charge_only_the_call_id_and_name() {
    for provider in [skein_llm::Provider::OpenAiCodex, skein_llm::Provider::Anthropic] {
        let mut bounds = limits();
        bounds.dialect.input_bytes = 3;
        bounds.dialect.answer_bytes = 32; // 21-byte fake call ID plus 11-byte caller name.
        let scripts = Box::new([Script {
            cue: b"caller-script".as_slice().into(),
            turns: Box::new([Turn {
                lines: Box::new([Line::Call {
                    name: b"caller_tool".as_slice().into(),
                    arguments: vec![b'x'; 128].into(),
                }]),
                finish: Finish::ToolCalls,
                tokens: 8,
            }]),
        }]);
        let mut world = Exchange::new(input(provider, 1), bounds, scripts);
        world.start();
        world.run();
        let [Block::Oversize { id, name, bytes }] = completion(&world).content.as_ref() else {
            panic!("one oversize completion")
        };
        assert_eq!(id.len() + name.len(), 32);
        assert_eq!(*bytes, 128);
    }
}

#[test]
fn scripted_codex_reasoning_drops_only_by_owner_opt_in_and_can_remain_in_history() {
    let opaque = br#"{"type":"reasoning","id":"opaque_1","encrypted_content":"signed","summary":[]}"#;
    for enabled in [false, true] {
        let mut bounds = limits();
        bounds.dialect.opaque_bytes = u32::try_from(opaque.len() - 1).unwrap();
        bounds.drop_reasoning = enabled;
        let mut world = Exchange::new(input(skein_llm::Provider::OpenAiCodex, 1), bounds, opaque_script(opaque));
        world.start();
        world.run();
        if enabled {
            let answer = completion(&world).clone();
            assert!(
                matches!(answer.content.as_ref(), [Block::Dropped { bytes }, Block::Text { .. }] if *bytes == u64::try_from(opaque.len()).unwrap())
            );
            let mut next = input(skein_llm::Provider::OpenAiCodex, 2);
            next.prompt.messages = Box::new([Message { role: Role::Assistant, content: answer.content }]);
            let next = client::Client::prepare(next, &bounds).unwrap();
            assert!(world.machine.next_call(next).is_ok());
            world.start();
            world.run();
            assert!(matches!(
                world.queries[1].messages[0].parts.as_ref(),
                [skein_fake_llm_domain::api::Part::Text { .. }]
            ));
        } else {
            assert!(world.seen.iter().any(|event| matches!(event, client::Event::Failed { failure: skein_llm::Failure::Limit { which: skein_llm::Cap::Opaque, bound }, .. } if *bound == u64::from(bounds.dialect.opaque_bytes))));
        }
    }
}

fn echo_input(owner: u64) -> skein_llm::Call {
    let mut input = input(skein_llm::Provider::OpenAiCodex, owner);
    input.prompt.messages = (0..20)
        .map(|_| Message {
            role: Role::User,
            content: Box::new([Block::Text { text: b"history".as_slice().into(), replay: None }]),
        })
        .collect();
    input
}

fn echoed_completion_json(world: &Exchange, bounds: &client::Limits) -> skein_llm::Json {
    let response =
        skein_http_world::reference::response(&world.responses, skein_http::Method::Post, false, &bounds.http);
    let events = skein_http_world::reference::events(&response.body, &bounds.sse);
    let terminal =
        events.events.iter().find(|event| event.name == b"response.completed").expect("actual terminal wire echo");
    skein_llm::Json::from_bytes(&terminal.data, &bounds.dialect)
        .expect("reference framing and bounded terminal document")
}

#[test]
fn configured_codex_echoes_preserve_the_completion_and_retain_the_token_cliff() {
    let mut bounds = limits();
    bounds.dialect.parts = 32;
    let echo = skein_llm::openai::Echo { instructions: true, tools: true, attribution_bytes: 24 };
    let mut plain = Exchange::new(echo_input(1), bounds, scripts());
    plain.start();
    plain.run();
    let expected = completion(&plain).clone();
    let plain_document = echoed_completion_json(&plain, &bounds);
    assert!(
        [b"instructions".as_slice(), b"tools", b"attribution"]
            .iter()
            .all(|name| key_count(plain_document.document(), name) == 0),
        "echoes are off by default"
    );
    let mut echoed = Exchange::new_with_codex_echo(echo_input(1), bounds, scripts(), echo);
    echoed.start();
    echoed.run();
    assert_eq!(completion(&echoed), &expected);
    let document = echoed_completion_json(&echoed, &bounds);
    let tokens = document.document();
    assert!(key_count(tokens, b"instructions") > 0);
    assert!(key_count(tokens, b"tools") > 0);
    assert_eq!(key_count(tokens, b"input_index"), 20);
    let edge = tokens.len();
    let request = skein_llm::Json::from_bytes(
        &skein_http_world::reference::request(&echoed.requests, &skein_llm_world::fake::limits(&bounds).http).body,
        &bounds.dialect,
    )
    .unwrap();
    assert!(request.document().len() < tokens.len(), "attribution cliff exceeds the admitted request");
    for cap in [edge, edge - 1] {
        bounds.dialect.tokens = cap;
        let mut world = Exchange::new_with_codex_echo(echo_input(1), bounds, scripts(), echo);
        world.start();
        world.run();
        if cap == edge {
            assert_eq!(completion(&world), &expected);
        } else {
            assert!(world.seen.iter().any(|event| matches!(event, client::Event::Failed { failure: skein_llm::Failure::Limit { which: skein_llm::Cap::Tokens, bound }, .. } if *bound == u64::from(cap))));
        }
    }
}

#[test]
fn byte_peers_can_omit_each_usage_report_without_changing_completion() {
    use skein_fake_llm_protocol::documents::{Options, UsageField, UsageFields};
    for provider in [skein_llm::Provider::OpenAiCodex, skein_llm::Provider::Anthropic] {
        for selected in [
            None,
            Some(UsageField::Input),
            Some(UsageField::CacheRead),
            Some(UsageField::CacheWrite),
            Some(UsageField::Output),
            Some(UsageField::Reasoning),
        ] {
            let fields = selected.map_or(UsageFields::NONE, |field| UsageFields::NONE.with(field));
            let mut world = Exchange::new_configured(
                input(provider, 99),
                limits(),
                scripts(),
                Options { echo: skein_llm::openai::Echo::NONE, usage_fields: fields },
            );
            world.start();
            world.run();
            let answer = completion(&world);
            assert_eq!(answer.stop, skein_llm::Stop::ToolUse);
            assert!(matches!(&*answer.content, [Block::ToolCall { name, .. }] if name.as_ref() == b"caller_tool"));
            assert_eq!(answer.usage.input, (selected == Some(UsageField::Input)).then_some(0));
            assert_eq!(answer.usage.cache_read, (selected == Some(UsageField::CacheRead)).then_some(0));
            assert_eq!(answer.usage.cache_write.is_some(), selected == Some(UsageField::CacheWrite));
            assert_eq!(answer.usage.output, (selected == Some(UsageField::Output)).then_some(8));
            assert_eq!(
                answer.usage.reasoning,
                (provider == skein_llm::Provider::OpenAiCodex && selected == Some(UsageField::Reasoning)).then_some(0)
            );
        }
    }
}

#[test]
fn actual_request_heads_match_the_independent_http_reader_on_both_dialects() {
    for provider in [skein_llm::Provider::OpenAiCodex, skein_llm::Provider::Anthropic] {
        let mut input = input(provider, 101);
        input.endpoint.headers = Box::new([skein_http::Header {
            name: b"x-caller-proof".as_slice().into(),
            value: b"actual-field-value".as_slice().into(),
        }]);
        let mut world = Exchange::new(input, limits(), scripts());
        world.start();
        world.run();
        assert_eq!(world.queries.len(), 1);
        let [actual] = world.heads.as_slice() else { panic!("one actual request head") };
        let parsed =
            skein_http_world::reference::request(&world.requests, &skein_llm_world::fake::limits(&limits()).http);
        let expected = parsed.head.expect("independent whole HTTP head");
        let actual: Vec<_> = actual.iter().map(|header| (header.name.to_vec(), header.value.to_vec())).collect();
        assert_eq!(actual, expected.headers);
        assert!(
            actual
                .iter()
                .any(|(name, value)| name.eq_ignore_ascii_case(b"x-caller-proof") && value == b"actual-field-value")
        );
        assert_eq!(completion(&world).stop, skein_llm::Stop::ToolUse);
    }
}

#[test]
fn explicit_call_reasoning_policy_overrides_the_default_without_changing_wire_bytes() {
    let provider = skein_llm::Provider::OpenAiCodex;
    let opaque = br#"{"type":"reasoning","id":"r","encrypted_content":"signed","summary":[]}"#;
    let mut request_bytes = None;
    for enabled in [false, true] {
        let mut bounds = limits();
        bounds.drop_reasoning = !enabled;
        bounds.dialect.opaque_bytes = u32::try_from(opaque.len() - 1).expect("bounded fixture");
        let input = input(provider, 71);
        let endpoint = input.endpoint.clone();
        let credential = skein_llm::Credential {
            access_token: input.credential.access_token.clone(),
            account_id: input.credential.account_id.clone(),
        };
        let machine =
            client::Client::prepare_with_reasoning_drop(input, &bounds, enabled).expect("explicit policy admission");
        let mut world = Exchange::prepared(machine, endpoint, credential, bounds, opaque_script(opaque));
        world.start();
        world.run();
        match &request_bytes {
            Some(expected) => assert_eq!(&world.requests, expected, "policy never changes provider request bytes"),
            None => request_bytes = Some(world.requests.clone()),
        }
        if enabled {
            assert!(matches!(completion(&world).content.as_ref(), [Block::Dropped { .. }, Block::Text { .. }]));
        } else {
            assert!(world.seen.iter().any(|event| matches!(
                event,
                client::Event::Failed { failure: skein_llm::Failure::Limit { which: skein_llm::Cap::Opaque, .. }, .. }
            )));
            assert!(!world.seen.iter().any(|event| matches!(event, client::Event::Completed { .. })));
        }
    }
}

fn key_count(document: &skein_json::Document, name: &[u8]) -> usize {
    (0..document.len())
        .filter(|index| {
            let record = document.token(*index).expect("record index within document");
            record.kind == skein_json::Kind::Key && document.text(record) == Some(name)
        })
        .count()
}

fn affinity_head<'a>(head: &'a [skein_http::Header], name: &[u8]) -> Option<&'a [u8]> {
    head.iter().find(|header| header.is(name)).map(|header| header.value.as_ref())
}

#[test]
fn affinity_is_fixed_across_calls_and_threads_are_distinct_on_the_actual_wire() {
    for thread in [0, 0x0102_0304] {
        let mut first = input(skein_llm::Provider::OpenAiCodex, 151);
        first.prompt.affinity.as_mut().expect("valid bounded affinity control").thread = thread;
        let mut world = Exchange::new(first, limits(), scripts());
        world.start();
        world.run();
        let mut next = input(skein_llm::Provider::OpenAiCodex, 152);
        next.prompt.affinity.as_mut().expect("valid bounded affinity control").thread = thread;
        let prepared = client::Client::prepare(next, &limits()).expect("valid bounded affinity control");
        assert!(world.machine.next_call(prepared).is_ok());
        world.seen.clear();
        world.request(client::Request::Start);
        world.run();
        assert_eq!(world.heads.len(), 2);
        assert_eq!(world.queries.len(), 2);
        for head in &world.heads {
            assert_eq!(affinity_head(head, b"session-id"), Some(b"42424242-4242-4242-4242-424242424242".as_slice()));
            let expected: &[u8] = if thread == 0 {
                b"42424242-4242-4242-4242-424242424242"
            } else {
                b"42424242-4242-4242-4242-424243404146"
            };
            assert_eq!(affinity_head(head, b"thread-id"), Some(expected));
        }
        assert!(world.queries.iter().all(|query| query.cache_scope == Some([0x42; 16])));
        assert_eq!(completion(&world).stop, skein_llm::Stop::ToolUse);
    }
}

#[test]
fn absent_affinity_sends_none_and_anthropic_ignores_it_on_the_actual_wire() {
    for provider in [skein_llm::Provider::OpenAiCodex, skein_llm::Provider::Anthropic] {
        let mut without = input(provider, 161);
        without.prompt.affinity = None;
        let mut world = Exchange::new(without, limits(), scripts());
        world.start();
        world.run();
        let [head] = world.heads.as_slice() else { panic!("one actual head") };
        assert_eq!(affinity_head(head, b"session-id"), None);
        assert_eq!(affinity_head(head, b"thread-id"), None);
        assert!(!String::from_utf8_lossy(&world.requests).contains("prompt_cache_key"));
        assert_eq!(world.queries[0].cache_scope, None);
        if provider == skein_llm::Provider::Anthropic {
            let mut with = input(provider, 162);
            with.prompt.affinity = Some(skein_llm::Affinity { key: [0x17; 16], thread: 31 });
            let mut selected = Exchange::new(with, limits(), scripts());
            selected.start();
            selected.run();
            assert_eq!(selected.requests, world.requests);
            assert_eq!(selected.heads, world.heads);
            assert_eq!(completion(&selected), completion(&world));
        }
    }
}

#[test]
fn endpoint_affinity_headers_are_reserved_for_both_dialects() {
    for provider in [skein_llm::Provider::OpenAiCodex, skein_llm::Provider::Anthropic] {
        for name in [b"Session-ID".as_slice(), b"THREAD-id"] {
            let mut input = input(provider, 171);
            input.endpoint.headers =
                Box::new([skein_http::Header { name: name.into(), value: b"override".as_slice().into() }]);
            assert!(matches!(client::Client::prepare(input, &limits()), Err(skein_llm::Error::Invalid)));
        }
    }
}

// Sends independently mutated actual HTTP bytes into the real peer. Its
// transport grants each send and answers each read; no Client API can forge
// these reserved fields. Returning the actual domain query distinguishes
// admission from a refusal before the domain, and the HTTP tape proves it.
fn peer_affinity_request(wire: &[u8]) -> (Option<skein_fake_llm_domain::api::Query>, Vec<u8>) {
    use skein_fake_llm_protocol::{documents, provider};
    use skein_lib::stream::{Down, Read, Up};
    use skein_lib::{Env, Intake, Queue, Time, Token, Wall};
    let input = input(skein_llm::Provider::OpenAiCodex, 181);
    let bounds = skein_llm_world::fake::limits(&limits());
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: bounds };
    let mut service = provider::Service::new(
        provider::Config {
            echo: skein_llm::openai::Echo::NONE,
            usage_fields: documents::UsageFields::ALL,
            provider: documents::Provider::OpenAi,
            path: input.endpoint.target,
            headers: Box::new([]),
        },
        &bounds,
    )
    .expect("valid bounded affinity control");
    let mut server = provider::Server::new(Token::new(2), &bounds).expect("valid bounded affinity control");
    let mut above = Queue::with_capacity(provider::MAX_UP);
    let mut below = Queue::with_capacity(provider::MAX_DOWN);
    let mut intake = Intake::with_capacity(32768);
    intake.append(wire).expect("valid bounded affinity control");
    let mut response = Vec::new();
    let mut demand = None;
    let mut grant = 0;
    provider::start(&mut server, &mut service, &input.credential, &env, &mut above, &mut below);
    for _ in 0..10000 {
        while let Some(event) = above.pop() {
            match event {
                provider::Event::Domain(skein_fake_llm_domain::Event::Call { query, .. }) => {
                    return (Some(query), response);
                }
                provider::Event::Head { .. } | provider::Event::Close | provider::Event::Closed => {}
            }
        }
        while let Some(down) = below.pop() {
            match down {
                Down::Demand { read: Read::Nothing, room: 0 } => demand = None,
                Down::Demand { read, room } => {
                    assert!(demand.is_none());
                    demand = Some((read, room));
                }
                Down::Send(data) => {
                    assert!(usize::try_from(grant).expect("valid bounded affinity control") >= data.len());
                    grant = 0;
                    response.extend_from_slice(&data);
                }
                Down::Finish => panic!("HTTP response uses framing"),
            }
        }
        if server.has_work() {
            provider::resume(&mut server, &mut service, &input.credential, &env, &mut above, &mut below);
            continue;
        }
        if let Some((read, room)) = demand {
            let event = if room > 0 {
                grant = room;
                Some(Up::Room)
            } else {
                intake.meet(read).map(Up::Bytes)
            };
            if let Some(event) = event {
                demand = None;
                provider::up(&mut server, &mut service, &input.credential, &env, event, &mut above, &mut below);
                continue;
            }
        }
        return (None, response);
    }
    panic!("finite peer story settles");
}

#[test]
fn independent_peer_refuses_mismatched_affinity_and_malformed_thread_before_dispatch() {
    let mut world = Exchange::new(input(skein_llm::Provider::OpenAiCodex, 181), limits(), scripts());
    world.start();
    world.run();
    let wire = String::from_utf8(world.requests).expect("valid bounded affinity control");
    let (accepted, response) = peer_affinity_request(wire.as_bytes());
    assert_eq!(accepted.expect("valid bounded affinity control").cache_scope, Some([0x42; 16]));
    assert!(response.is_empty());
    for (from, to) in [
        ("session-id: 42424242", "session-id: 43434343"),
        ("thread-id: 42424242", "thread-id: z2424242"),
        ("session-id: 42424242-4242-4242-4242-424242424242\r\n", ""),
        ("thread-id: 42424242-4242-4242-4242-424242424242\r\n", ""),
    ] {
        let changed = wire.replacen(from, to, 1);
        assert_ne!(changed, wire, "control really mutates actual header bytes");
        let (query, response) = peer_affinity_request(changed.as_bytes());
        assert!(query.is_none(), "invalid affinity never enters the neutral domain");
        assert!(response.starts_with(b"HTTP/1.1 400 "), "{:?}", String::from_utf8_lossy(&response));
    }
}
