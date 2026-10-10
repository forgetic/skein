//! Actual Client and independent bounded peer ownership through physical close.
//! Public payloads and literal observations supply the fixture oracle; the
//! peer price excludes the one separately priced Client. Contract:
//! docs/design/fake-llm.md, sections 2–5; programming-model.md, section 6.3;
//! testing-strategy.md, sections 2.5 and 6.

use skein_fake_llm_domain::api::{Finish, Line, Script, Turn};
use skein_fake_llm_protocol::provider;
use skein_heap::{Counting, Meter};
use skein_http_world::reference;
use skein_lib::Token;
use skein_llm::{Block, Call, Completion, Credential, Endpoint, Provider, Stop, client};
use skein_llm_world::fake::{Exchange, ObservationLimits, extra_worst_case};

#[global_allocator]
static HEAP: Counting = Counting;

const CUE: &[u8] = b"bounded-peer-memory";
const ANSWER: &[u8] = b"actual bounded native answer";
const LARGE: usize = 8192;

fn size<T>() -> u64 {
    u64::try_from(size_of::<T>()).expect("fixture wrapper fits")
}

fn bytes(value: &[u8]) -> u64 {
    u64::try_from(value.len()).expect("fixture bytes fit")
}

fn observations(bounds: &client::Limits) -> ObservationLimits {
    ObservationLimits {
        heads: 1,
        head_bytes: 8192,
        events: client::MAX_OUT.above,
        event_bytes: 32768,
        queries: 1,
        query_bytes: 32768,
        pending: 0,
        request: bounds.http.request.checked_add(bounds.request).expect("whole HTTP request"),
        response_bytes: 4096,
    }
}

fn limits() -> client::Limits {
    let mut bounds = skein_llm_world::limits();
    bounds.http.request = 32768;
    bounds.request = 32768;
    bounds.retained = 32768;
    bounds.strings = 16384;
    bounds.output_items = 32;
    bounds.tokens = 4096;
    bounds.answer = 1024;
    bounds
}

fn input(owner: u64, provider: Provider) -> Call {
    let mut input = skein_llm_world::call(owner);
    if provider == Provider::Anthropic {
        input.endpoint = Endpoint::anthropic();
        input.credential = Credential::anthropic(b"fixture-token".as_slice().into());
        input.prompt.affinity = None;
    }
    input.prompt.output_ceiling(provider, 4096).expect("caller output configuration");
    input.prompt.instructions = CUE.into();
    let [message] = &mut *input.prompt.messages else { panic!("one fixture message") };
    message.content = Box::new([Block::Text { text: vec![b'x'; LARGE].into(), replay: None }]);
    input
}

fn scripts() -> Box<[Script]> {
    Box::new([Script {
        cue: CUE.into(),
        turns: Box::new([Turn {
            lines: Box::new([Line::Text { text: ANSWER.into() }]),
            finish: Finish::Stop,
            tokens: 1,
        }]),
    }])
}

// Counts the observed fixture's public owning fields, not any source pricing
// helper or provider grammar. Unexpected vocabulary fails the fixture.
fn endpoint_bytes(endpoint: &Endpoint) -> u64 {
    bytes(&endpoint.authority)
        + bytes(&endpoint.target)
        + u64::try_from(endpoint.headers.len()).expect("bounded headers") * size::<skein_http::Header>()
        + endpoint.headers.iter().map(|header| bytes(&header.name) + bytes(&header.value)).sum::<u64>()
}

fn input_bytes(input: &Call) -> u64 {
    assert!(input.prompt.tools.is_empty());
    assert!(input.prompt.reasoning_effort.is_none());
    let mut owned = endpoint_bytes(&input.endpoint)
        + bytes(&input.credential.access_token)
        + bytes(&input.credential.account_id)
        + bytes(&input.prompt.model)
        + bytes(&input.prompt.instructions)
        + u64::try_from(input.prompt.messages.len()).expect("bounded messages") * size::<skein_llm::Message>();
    for message in &input.prompt.messages {
        let [Block::Text { text, replay: None }] = message.content.as_ref() else { panic!("literal fixture text") };
        owned += size::<Block>() + bytes(text);
    }
    owned
}

fn terminal_bytes(completion: &Completion) -> u64 {
    let [Block::Text { text, replay }] = completion.content.as_ref() else { panic!("one real native text") };
    let mut owned = size::<Block>() + bytes(text);
    if let Some(replay) = replay {
        let document = replay.value.document();
        owned += u64::from(document.len()) * size::<skein_json::Compact>() + u64::from(document.text_len());
    }
    owned
}

#[derive(Default)]
struct Seen {
    terminal: Option<Completion>,
    reusable: u32,
    close: u32,
    closed: u32,
}

impl Seen {
    fn take(&mut self, world: &mut Exchange, owner: u64) {
        for event in core::mem::take(&mut world.seen) {
            match event {
                client::Event::Completed { owner: got, completion } => {
                    assert_eq!(got, Token::new(owner));
                    assert!(self.terminal.replace(completion).is_none(), "one actual terminal");
                }
                client::Event::Reusable => self.reusable += 1,
                client::Event::Close => self.close += 1,
                client::Event::Closed => self.closed += 1,
                client::Event::Delta { .. } | client::Event::Block { .. } => {}
                client::Event::Failed { .. } | client::Event::Cancelled { .. } => panic!("positive actual completion"),
            }
        }
    }
}

fn quiesce_closed(world: &mut Exchange, seen: &mut Seen, owner: u64) {
    for _ in 0..128 {
        let progress = world.tick(true);
        seen.take(world, owner);
        if !progress {
            return;
        }
    }
    panic!("actual closed peer drains its bounded remaining work");
}

// Passive validation runs after the physical entrance's Meter step ends. The
// shared independent reference reader's temporary allocations are outside the
// runtime Client/peer peak claim and drop here, before physical progress resumes
// and before the final held-zero check. No framing reader is copied here.
fn assert_reference_response(response: &[u8], bounds: &client::Limits) {
    assert!(response.len() <= 4096, "whole fixture response fits its declared tape");
    let parsed = reference::response(response, skein_http::Method::Post, false, &bounds.http);
    assert_eq!(parsed.outcome, reference::Outcome::Done(skein_http::client::Reuse::Keep));
    assert_eq!(parsed.used, response.len(), "reference consumes the whole actual raw response");
    let head = parsed.head.as_ref().expect("complete actual native response head");
    assert_eq!(head.status, 200);
    assert_eq!(head.framing, skein_http::client::Framing::Chunked);
    assert!(
        head.headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case(b"content-type") && value == b"text/event-stream")
    );
    assert!(
        parsed.body.windows(ANSWER.len()).any(|bytes| bytes == ANSWER),
        "whole literal native answer in actual chunk payloads: {response:?}"
    );
}

fn assert_native_payloads(world: &Exchange, seen: &Seen, caps: &ObservationLimits, bounds: &client::Limits) {
    let [query] = world.queries.as_slice() else { panic!("one actual decoded query") };
    assert_eq!(query.system.as_ref(), CUE);
    let [message] = query.messages.as_ref() else { panic!("one native user message") };
    let [skein_fake_llm_domain::api::Part::Text { text }] = message.parts.as_ref() else {
        panic!("whole actual native text")
    };
    assert_eq!(text.len(), LARGE);
    assert!(text.iter().all(|byte| *byte == b'x'));
    assert!(world.requests.windows(LARGE).any(|text| text.iter().all(|byte| *byte == b'x')));
    assert_reference_response(&world.responses, bounds);
    assert!(world.requests.len() <= usize::try_from(caps.request).expect("declared trace cap"));
    assert!(world.responses.len() <= 4096);
    let completion = seen.terminal.as_ref().expect("actual native terminal");
    assert_eq!(completion.stop, Stop::EndTurn);
    let [Block::Text { text, .. }] = completion.content.as_ref() else { panic!("literal native output") };
    assert_eq!(text.as_ref(), ANSWER);
    assert!(terminal_bytes(completion) <= caps.event_bytes, "caller independently counts full native replay ownership");
}

#[test]
fn actual_native_peer_and_client_fit_checked_prices_until_every_owner_drops() {
    for provider in [Provider::OpenAiCodex, Provider::Anthropic] {
        let bounds = limits();
        let caps = observations(&bounds);
        let meter = Meter::new();
        meter.start();
        let input = input(19, provider);
        // Client preparation consumes input. The peer retains a second grant
        // and target; its other cloned endpoint fields exist during adoption.
        let caller = input_bytes(&input) + endpoint_bytes(&input.endpoint);
        let extra = extra_worst_case(&bounds, &caps, &input.endpoint, &input.credential).expect("checked peer price");
        let client_price = client::call_worst_case(&bounds).expect("one Client price");
        let bound = client_price
            .checked_add(extra)
            .expect("composition")
            .checked_add(caller)
            .expect("caller construction")
            .checked_add(caps.event_bytes)
            .expect("caller-held native completion");
        let mut world = Exchange::new(input, bounds, scripts());
        world.observe(caps);
        assert_eq!(world.seen.capacity(), usize::try_from(caps.events).expect("event reservation"));
        assert_eq!(world.queries.capacity(), 1);
        assert_eq!(world.requests.capacity(), usize::try_from(caps.request).expect("request reservation"));
        assert_eq!(world.responses.capacity(), 4096);
        assert_eq!(world.service.calls(), 0);
        let constructed = meter.end();
        assert!(constructed.peak() <= bound, "construction {} exceeds {bound}", constructed.peak());

        let mut seen = Seen::default();
        meter.start();
        world.start();
        seen.take(&mut world, 19);
        let started = meter.end();
        assert!(started.peak() <= bound);
        let mut quiet = false;
        for _ in 0..100_000 {
            meter.start();
            let progress = world.tick(true);
            seen.take(&mut world, 19);
            let measured = meter.end();
            assert!(measured.peak() <= bound, "native step {} exceeds {bound}", measured.peak());
            if !progress {
                quiet = true;
                break;
            }
        }
        assert!(quiet, "actual bounded native drainage");
        assert_eq!((seen.reusable, seen.close, seen.closed), (1, 0, 0));
        assert_eq!(world.service.count(), 1);
        assert_eq!(world.service.calls(), 0, "actual route was reclaimed while Service remains live");
        assert_native_payloads(&world, &seen, &caps, &bounds);

        meter.start();
        world.request(client::Request::Close);
        seen.take(&mut world, 19);
        assert_eq!((seen.reusable, seen.close, seen.closed), (1, 1, 0));
        world.settle();
        seen.take(&mut world, 19);
        world.settle();
        seen.take(&mut world, 19);
        assert_eq!((seen.reusable, seen.close, seen.closed), (1, 1, 1));
        assert_eq!(world.machine.waiting(), client::Waiting::Nothing);
        quiesce_closed(&mut world, &mut seen, 19);
        assert_eq!((seen.reusable, seen.close, seen.closed), (1, 1, 1));
        let settled = meter.end();
        assert!(settled.peak() <= bound);
        assert_eq!(world.service.calls(), 0);
        meter.start();
        drop(world);
        let released = meter.end();
        assert!(released.peak() <= bound);
        assert_eq!(
            meter.held(),
            terminal_bytes(seen.terminal.as_ref().expect("caller still owns terminal")),
            "peer and Client released exactly their ownership"
        );
        drop(seen);
        assert_eq!(meter.held(), 0, "every caller, Client, peer and script allocation released");
    }
}

#[test]
fn actual_connections_reuse_reclaimed_service_routes_past_its_capacity() {
    let bounds = limits();
    let caps = observations(&bounds);
    let meter = Meter::new();
    let mut world = Exchange::new(input(1, Provider::Anthropic), bounds, scripts());
    world.observe(caps);
    // One retained Service has four slots. Five actual physical connections
    // cannot all complete if retired route slots remain unreclaimed.
    for owner in 1..=5 {
        if owner > 1 {
            world.machine =
                client::Client::prepare(input(owner, Provider::Anthropic), &bounds).expect("next actual Client");
            world.server = provider::Server::new(Token::new(2), &skein_llm_world::fake::limits(&bounds))
                .expect("next actual physical peer");
            world.queries.clear();
            world.heads.clear();
            world.requests.clear();
            world.responses.clear();
        }
        let mut seen = Seen::default();
        world.start();
        seen.take(&mut world, owner);
        let mut quiet = false;
        for _ in 0..100_000 {
            let progress = world.tick(true);
            seen.take(&mut world, owner);
            if !progress {
                quiet = true;
                break;
            }
        }
        assert!(quiet);
        assert!(seen.terminal.is_some());
        assert_eq!(seen.reusable, 1);
        assert_eq!(world.service.count(), owner);
        assert_eq!(world.service.calls(), 0);
        world.request(client::Request::Close);
        seen.take(&mut world, owner);
        world.settle();
        seen.take(&mut world, owner);
        assert_eq!((seen.close, seen.closed), (1, 1));
        assert_eq!(world.machine.waiting(), client::Waiting::Nothing);
        quiesce_closed(&mut world, &mut seen, owner);
        assert_eq!((seen.reusable, seen.close, seen.closed), (1, 1, 1));
    }
    drop(world);
    assert_eq!(meter.held(), 0, "repeated actual physical connections release every owner");
}

#[test]
fn impossible_observation_products_refuse_without_allocating_the_ceiling() {
    let bounds = limits();
    let input = input(1, Provider::Anthropic);
    let mut caps = observations(&bounds);
    assert!(extra_worst_case(&bounds, &caps, &input.endpoint, &input.credential).is_some());
    caps.event_bytes = u64::MAX;
    assert!(extra_worst_case(&bounds, &caps, &input.endpoint, &input.credential).is_none());
    caps = observations(&bounds);
    caps.queries = 2;
    caps.query_bytes = u64::MAX;
    assert!(extra_worst_case(&bounds, &caps, &input.endpoint, &input.credential).is_none());
}

#[test]
fn exact_query_observation_capacity_copies_the_whole_query_and_one_less_copies_nothing() {
    let bounds = limits();
    // This literal fixture offers no tools and carries one user/text pair.
    // Count the public owned arrays independently, including every byte.
    let exact = bytes(b"fixture-model")
        + bytes(CUE)
        + u64::try_from(LARGE).expect("bounded text")
        + size::<skein_fake_llm_domain::api::Message>()
        + size::<skein_fake_llm_domain::api::Part>()
        + 2 * size::<skein_fake_llm_domain::api::Mark>();
    let mut caps = observations(&bounds);
    caps.query_bytes = exact;
    let mut positive = Exchange::new(input(1, Provider::Anthropic), bounds, scripts());
    positive.observe(caps);
    positive.start();
    let mut seen = Seen::default();
    seen.take(&mut positive, 1);
    let mut quiet = false;
    for _ in 0..100_000 {
        let progress = positive.tick(true);
        seen.take(&mut positive, 1);
        if !progress {
            quiet = true;
            break;
        }
    }
    assert!(quiet);
    assert!(seen.terminal.is_some());
    let [query] = positive.queries.as_slice() else { panic!("exact query observation fits") };
    assert_eq!(query.model.as_ref(), b"fixture-model");
    assert_eq!(query.system.as_ref(), CUE);
    assert!(query.tools.is_empty());
    assert_eq!(query.messages.len(), 1);
    positive.request(client::Request::Close);
    seen.take(&mut positive, 1);
    positive.settle();
    seen.take(&mut positive, 1);
    assert_eq!((seen.reusable, seen.close, seen.closed), (1, 1, 1));
    assert_eq!(positive.service.calls(), 0);
    drop(positive);
    drop(seen);

    caps.query_bytes = exact - 1;
    let mut negative = Exchange::new(input(1, Provider::Anthropic), bounds, scripts());
    negative.observe(caps);
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        negative.start();
        for _ in 0..100_000 {
            if !negative.tick(true) {
                break;
            }
        }
    }));
    assert!(refused.is_err(), "one less observation byte reaches the fixture ceiling");
    assert!(negative.queries.is_empty(), "guard precedes the owning query clone");
    assert!(negative.responses.is_empty(), "no scripted response was started after the ceiling");
    negative.request(client::Request::Close);
    assert!(matches!(negative.seen.as_slice(), [client::Event::Close]));
    negative.seen.clear();
    negative.settle();
    assert!(
        matches!(negative.seen.as_slice(), [client::Event::Cancelled { owner }, client::Event::Closed] if *owner == Token::new(1))
    );
    negative.seen.clear();
    negative.settle();
    assert!(negative.seen.is_empty());
    assert_eq!(negative.service.calls(), 0, "refused observation still settles its real route");
    assert_eq!(negative.machine.waiting(), client::Waiting::Nothing);
}

#[test]
fn exact_head_observation_capacity_retains_actual_fields_and_one_less_retains_nothing() {
    for provider in [Provider::OpenAiCodex, Provider::Anthropic] {
        let bounds = limits();
        let mut baseline = Exchange::new(input(1, provider), bounds, scripts());
        baseline.start();
        baseline.run();
        let [head] = baseline.heads.as_slice() else { panic!("one baseline actual head") };
        let exact = u64::try_from(head.len()).expect("bounded wrappers") * size::<skein_http::Header>()
            + head.iter().map(|field| bytes(&field.name) + bytes(&field.value)).sum::<u64>();
        drop(baseline);
        let mut caps = observations(&bounds);
        caps.events = 64;
        caps.head_bytes = exact;
        let meter = Meter::new();
        meter.start();
        let call_input = input(1, provider);
        let caller = input_bytes(&call_input) + endpoint_bytes(&call_input.endpoint);
        let bound = client::call_worst_case(&bounds).expect("client price")
            + extra_worst_case(&bounds, &caps, &call_input.endpoint, &call_input.credential).expect("peer price")
            + caller;
        let mut positive = Exchange::new(call_input, bounds, scripts());
        positive.observe(caps);
        positive.start();
        positive.run();
        let measured = meter.end();
        assert!(measured.peak() <= bound, "actual head ownership {} exceeds {bound}", measured.peak());
        let [head] = positive.heads.as_slice() else { panic!("exact head observation fits") };
        let actual = u64::try_from(head.len()).expect("bounded wrappers") * size::<skein_http::Header>()
            + head.iter().map(|field| bytes(&field.name) + bytes(&field.value)).sum::<u64>();
        assert_eq!(actual, exact);
        assert_eq!(positive.queries.len(), 1);
        drop(positive);
        assert_eq!(meter.held(), 0, "all exact-head owners released");

        caps.head_bytes = exact - 1;
        let mut negative = Exchange::new(input(1, provider), bounds, scripts());
        negative.observe(caps);
        let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            negative.start();
            negative.run();
        }));
        assert!(refused.is_err(), "one byte below actual head ownership refuses retention");
        assert!(negative.heads.is_empty());
        assert!(negative.queries.is_empty(), "the script never received the request");
        assert!(negative.responses.is_empty());
        negative.request(client::Request::Close);
        negative.seen.clear();
        negative.settle();
        assert_eq!(negative.machine.waiting(), client::Waiting::Nothing);
        assert_eq!(negative.service.calls(), 0);
    }
}
