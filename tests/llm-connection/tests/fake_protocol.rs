//! The component's owner routes io and a TLS stream to the independent fake
//! LLM byte peer in both dialects (llm-connection.md, section 8).

use core::net::Ipv4Addr;

use skein_fake_llm_domain::{self as fake, api};
use skein_fake_llm_protocol::{documents, provider};
use skein_io::kernel::Addr;
use skein_io::{Event as IoEvent, Request as IoRequest};
use skein_lib::Wall;
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Duration, Env, Intake, List, Queue, Time, Token};
use skein_llm::{Credential, Provider};
use skein_llm_connection::{Component, Deadlines, Endpoint, Event, Limits, MAX_OUT, Request};
use skein_llm_connection_world::plaintext::Wire;
use skein_world::domain::assert_replays;

fn limits() -> Limits {
    Limits {
        endpoints: 1,
        connections: 1,
        calls: 1,
        per_endpoint: 1,
        idle_keep: Duration::from_secs(10),
        io: skein_io::Limits {
            sockets: 1,
            refusals: 1,
            intake: 19_000,
            receive: 1024,
            output: 19_000,
            sends: 2,
            accepts: 1,
            backlog: 1,
            close_timeout: Duration::from_secs(1),
            retry: Duration::from_millis(10),
        },
        tls: skein_tls::client::Limits { read: 4096, send: 4096, records: skein_tls::client::MAX_RECORD },
    }
}

#[expect(clippy::too_many_lines, reason = "the protocol story drives the component, io and fake peer")]
fn run(dialect: Provider, seed: u64) -> (Vec<String>, (u32, u32)) {
    let mut call = skein_llm_world::call(7);
    match dialect {
        Provider::OpenAiCodex => {}
        Provider::Anthropic => {
            call.endpoint = skein_llm::Endpoint::anthropic();
            call.credential = Credential::anthropic(b"fake-token".as_slice().into());
            call.prompt.cache_key = None;
        }
    }
    call.prompt.output_ceiling(dialect, 4096).expect("provider output ceiling");
    call.prompt.instructions = b"connection-world".as_slice().into();
    let peer_credential = Credential {
        access_token: call.credential.access_token.clone(),
        account_id: call.credential.account_id.clone(),
    };
    let peer_limits = skein_llm_world::fake::limits(&skein_llm_world::limits());
    let provider_kind = match dialect {
        Provider::OpenAiCodex => documents::Provider::OpenAi,
        Provider::Anthropic => documents::Provider::Anthropic,
    };
    let mut service = provider::Service::new(
        provider::Config {
            usage_fields: skein_fake_llm_protocol::documents::UsageFields::ALL,
            echo: skein_llm::openai::Echo::NONE,
            provider: provider_kind,
            path: call.endpoint.target.clone(),
            headers: Box::new([]),
        },
        &peer_limits,
    )
    .expect("fake service");
    let mut peer = provider::Server::new(Token::new(2), &peer_limits).expect("fake server");
    let fake_limits = skein_llm_world::fake::config();
    let scripts = Box::new([api::Script {
        cue: b"connection-world".as_slice().into(),
        turns: Box::new([api::Turn {
            lines: Box::new([api::Line::Text { text: b"scripted answer".as_slice().into() }]),
            finish: api::Finish::Stop,
            tokens: 2,
        }]),
    }]);
    let mut domain = fake::Domain::try_scripted(&fake_limits, 3, scripts).expect("scripted domain");
    let mut endpoints = List::with_capacity(1);
    endpoints
        .push(Endpoint {
            address: Addr::from((Ipv4Addr::LOCALHOST, 443)),
            transport: skein_llm_connection::Transport::Plaintext,
            llm: call.endpoint,
            limits: skein_llm_world::limits(),
            credential: skein_llm::client::CredentialLimits { access_token: 2048, account_id: 128 },
        })
        .expect("one endpoint");
    let mut component = Component::new(endpoints, &limits()).expect("component");
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits() };
    let peer_env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: peer_limits };
    let fake_env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: fake_limits };
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    let mut peer_up = Queue::with_capacity(provider::MAX_UP);
    let mut peer_down = Queue::with_capacity(provider::MAX_DOWN);
    let mut replies = Queue::with_capacity(fake::MAX_OUT);
    let mut to_peer = Intake::with_capacity(32768);
    let mut peer_demand: Option<(Read, u32)> = None;
    let mut grant: Option<u32> = None;
    let mut wire = Wire::new(seed);
    let mut received = 0_usize;
    let mut owner = None;
    let mut completed = 0_u32;
    let mut fragments = 0_u32;
    provider::start(&mut peer, &mut service, &peer_credential, &peer_env, &mut peer_up, &mut peer_down);
    component.down(
        &env,
        Request::Start {
            call: Token::new(7),
            endpoint: 0,
            prompt: call.prompt,
            credential: call.credential,
            deadlines: Deadlines::none(),
        },
        &mut up,
        &mut io,
    );
    component.down(&env, Request::Next { call: Token::new(7) }, &mut up, &mut io);
    for _ in 0_u32..40_000 {
        if let Some(event) = up.pop() {
            wire.trace.push(format!("owner {event:?}"));
            match event {
                Event::Delta { call, .. } | Event::Block { call, .. } => {
                    fragments += 1;
                    component.down(&env, Request::Next { call }, &mut up, &mut io);
                }
                Event::Completed { call, completion } => {
                    assert_eq!(call, Token::new(7));
                    assert!(!completion.content.is_empty());
                    completed += 1;
                }
                Event::Closed | Event::Refused { .. } | Event::Failed { .. } | Event::Cancelled { .. } => {
                    panic!("unexpected component event: {event:?}");
                }
            }
        }
        if let Some(request) = io.pop() {
            match request {
                IoRequest::Connect { owner: token, .. } => {
                    assert!(owner.replace(token).is_none());
                    component.up(&env, IoEvent::Connecting { owner: token, socket: Token::new(100) }, &mut up, &mut io);
                    component.up(&env, IoEvent::Connected { owner: token }, &mut up, &mut io);
                }
                IoRequest::Stream { stream, down } => {
                    assert_eq!(stream, Token::new(100));
                    wire.take(down);
                }
                IoRequest::Close { entity } | IoRequest::Abort { entity } => {
                    assert_eq!(entity, Token::new(100));
                    component.up(&env, IoEvent::Closed { owner: owner.expect("connected owner") }, &mut up, &mut io);
                }
                other @ (IoRequest::Listen { .. }
                | IoRequest::Bind { .. }
                | IoRequest::Reject { .. }
                | IoRequest::Output { .. }
                | IoRequest::Spawn { .. }
                | IoRequest::Signal { .. }) => panic!("unexpected io request: {other:?}"),
            }
        }
        if wire.received.len() > received {
            to_peer.append(&wire.received[received..]).expect("bounded request intake");
            received = wire.received.len();
        }
        if let Some(answer) = wire.answer() {
            component.up(
                &env,
                IoEvent::Stream { owner: owner.expect("connected owner"), up: answer },
                &mut up,
                &mut io,
            );
        }
        if let Some(event) = peer_up.pop() {
            match event {
                provider::Event::Domain(input) => fake::step(&mut domain, &fake_env, input, &mut replies),
                provider::Event::Close | provider::Event::Closed => {}
            }
        }
        if let Some(reply) = replies.pop() {
            provider::down(&mut peer, &mut service, &peer_credential, &peer_env, reply, &mut peer_up, &mut peer_down);
            domain.reclaim();
            service.reclaim();
        }
        if let Some(down) = peer_down.pop() {
            match down {
                Down::Demand { read: Read::Nothing, room: 0 } => peer_demand = None,
                Down::Demand { read, room } => {
                    assert!(peer_demand.is_none(), "one demand at a time");
                    peer_demand = Some((read, room));
                }
                Down::Send(bytes) => {
                    assert!(bytes.len() <= usize::try_from(grant.take().expect("room grant")).expect("room fits"));
                    wire.write(&bytes);
                }
                Down::Finish => wire.eof = true,
            }
        }
        if let Some((read, room)) = peer_demand {
            let answer = match to_peer.meet(read) {
                Some(bytes) => Some(Up::Bytes(bytes)),
                None if room > 0 => {
                    grant = Some(room);
                    Some(Up::Room)
                }
                None => None,
            };
            if let Some(answer) = answer {
                peer_demand = None;
                provider::up(
                    &mut peer,
                    &mut service,
                    &peer_credential,
                    &peer_env,
                    answer,
                    &mut peer_up,
                    &mut peer_down,
                );
            }
        }
        if peer.has_work() {
            provider::resume(&mut peer, &mut service, &peer_credential, &peer_env, &mut peer_up, &mut peer_down);
        }
        if domain.is_due(fake_env.now) {
            fake::fire(&mut domain, &fake_env, &mut replies);
        }
        if component.has_work() {
            component.fire(&env, &mut up, &mut io);
        }
        if completed == 1 && !component.has_work() && io.is_empty() && up.is_empty() && !peer.has_work() {
            break;
        }
    }
    assert_eq!(service.count(), 1, "the independent fake decoded the call");
    assert_eq!(completed, 1);
    assert!(fragments > 0);
    let later = env;
    component.down(&later, Request::Close, &mut up, &mut io);
    match io.pop() {
        Some(IoRequest::Close { entity }) => {
            assert_eq!(entity, Token::new(100));
            component.up(&later, IoEvent::Closed { owner: owner.expect("owner") }, &mut up, &mut io);
            component.reclaim();
        }
        other => panic!("expected idle close, got {other:?}"),
    }
    provider::closed(&mut peer, &mut service, &peer_env, &mut peer_up, &mut peer_down);
    service.reclaim();
    domain.reclaim();
    assert!(matches!(up.pop(), Some(Event::Closed)));
    assert!(up.is_empty() && io.is_empty() && !component.has_work());
    (wire.trace, (completed, fragments))
}

#[test]
fn codex_fake_peer_over_plaintext_replays() {
    assert_replays(7, 8, |seed| run(Provider::OpenAiCodex, seed));
}

#[test]
fn anthropic_fake_peer_over_plaintext_replays() {
    assert_replays(7, 8, |seed| run(Provider::Anthropic, seed));
}
