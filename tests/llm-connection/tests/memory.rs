//! A full connection pool stays within the component's checked heap bound.

use core::net::Ipv4Addr;

use skein_heap::{Counting, Meter};
use skein_io::kernel::Addr;
use skein_lib::{Duration, Env, List, Queue, Time, Token};
use skein_llm_connection::{Component, Deadlines, Endpoint, Event, Limits, MAX_OUT, Request};
use skein_tls_world::pki;

#[global_allocator]
static HEAP: Counting = Counting;

fn full_pool_limits() -> Limits {
    Limits {
        endpoints: 1,
        connections: 2,
        calls: 2,
        per_endpoint: 2,
        memory: 16 * 1024 * 1024,
        idle_keep: Duration::from_secs(10),
        io: skein_io::Limits {
            sockets: 2,
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

#[test]
fn full_pool_stays_within_its_checked_bound() {
    let mut limits = full_pool_limits();
    let meter = Meter::new();
    meter.start();
    let mut endpoints = List::with_capacity(1);
    endpoints
        .push(Endpoint {
            address: Addr::from((Ipv4Addr::LOCALHOST, 443)),
            transport: skein_llm_connection::Transport::Tls {
                server_name: skein_tls::Name::new("skein.test").expect("name"),
                trust: pki::client(&[]),
            },
            llm: skein_llm::Endpoint::codex(),
            limits: skein_llm_world::limits(),
            credential: skein_llm::client::CredentialLimits { access_token: 2048, account_id: 128 },
        })
        .expect("one endpoint");
    let first = skein_llm_world::call(7);
    let destination = endpoints.get(0).unwrap();
    let mut client = destination.limits;
    client.http.request = skein_llm::client::request_head(&destination.llm, &destination.credential, &client).unwrap();
    let measured = skein_llm::client::measure(&first.prompt, &first.credential, &destination.llm, &client).unwrap();
    limits.memory = 2 * skein_llm::client::reservation(measured, &client).unwrap();
    let second = skein_llm_world::call(8);
    let third = skein_llm_world::call(9);
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    let env = Env { now: Time::ZERO, wall: pki::VALID, limits };
    let mut component = Component::new(endpoints, &limits).expect("component");
    let bound = component.worst_case().expect("representable pool bound");
    component.down(
        &env,
        Request::Start {
            drop_reasoning: false,
            call: Token::new(7),
            endpoint: 0,
            prompt: first.prompt,
            credential: first.credential,
            deadlines: Deadlines::none(),
        },
        &mut up,
        &mut io,
    );
    component.down(
        &env,
        Request::Start {
            drop_reasoning: false,
            call: Token::new(8),
            endpoint: 0,
            prompt: second.prompt,
            credential: second.credential,
            deadlines: Deadlines::none(),
        },
        &mut up,
        &mut io,
    );
    assert_eq!(io.len(), 2, "both physical slots are occupied");
    assert_eq!(component.reserved(), limits.memory, "both calls fill the memory pool exactly");
    component.down(
        &env,
        Request::Start {
            drop_reasoning: false,
            call: Token::new(9),
            endpoint: 0,
            prompt: third.prompt,
            credential: third.credential,
            deadlines: Deadlines::none(),
        },
        &mut up,
        &mut io,
    );
    match up.pop() {
        Some(Event::Refused { call, why }) => {
            assert_eq!(call, Token::new(9));
            assert_eq!(why, skein_llm_connection::Refusal::Calls { bound: 2 });
        }
        other => panic!("expected pool refusal, got {other:?}"),
    }
    let measured = meter.end();
    assert!(measured.peak() <= bound, "full pool peak {} exceeds {bound}", measured.peak());
    assert!(meter.held() <= bound, "full pool retained {} exceeds {bound}", meter.held());
    drop(component);
    drop(up);
    drop(io);
    drop(first.endpoint);
    drop(second.endpoint);
    drop(third.endpoint);
    assert_eq!(meter.held(), 0, "all pool allocations released");
}

#[test]
fn full_fake_process_and_connection_pool_fit_and_free_their_separate_heaps() {
    for transport in [skein_fake_peers::Transport::Plaintext, skein_fake_peers::Transport::Tls] {
        let first =
            skein_llm_connection_world::simulated::run(37, false, transport, true, skein_world::Memory::Checked);
        let heap = first.heap.as_ref().unwrap();
        assert_eq!(heap.len(), 2);
        assert!(heap.iter().all(|(peak, bound)| *peak > 0 && peak <= bound));
        if transport == skein_fake_peers::Transport::Plaintext {
            let second =
                skein_llm_connection_world::simulated::run(37, false, transport, true, skein_world::Memory::Checked);
            skein_llm_connection_world::simulated::assert_replay(&first, &second);
            assert_eq!(first.heap, second.heap);
        }
    }
}

#[test]
fn bounded_request_batches_preserve_exact_upload_latency_replay_and_separate_heaps() {
    use skein_fake_peers::Transport;
    use skein_llm_connection_world::simulated::{assert_replay, run_with_input};
    use skein_world::Memory;

    let first = run_with_input(23, false, Transport::Plaintext, false, Memory::Checked, 1500);
    let second = run_with_input(23, false, Transport::Plaintext, false, Memory::Checked, 1500);
    assert_replay(&first, &second);
    assert_eq!(first.heap, second.heap);
    assert!(first.iterations < 256, "bounded upload settled in {} iterations", first.iterations);
    assert_eq!(first.procs.len(), 2, "client and fake remain separately owned processes");
    let heap = first.heap.as_ref().unwrap();
    assert!(heap.iter().all(|(peak, bound)| *peak > 0 && peak <= bound));
}

#[test]
fn an_owner_close_in_each_phase_fits_the_checked_pool_bound() {
    use skein_llm_connection_world::world::{Point, World};
    for point in
        [Point::Waiting, Point::Connecting, Point::Head, Point::Streaming, Point::Draining, Point::Idle, Point::Closing]
    {
        let calls = if point == Point::Waiting { 2 } else { 1 };
        let meter = Meter::new();
        meter.start();
        let mut world = World::configured(53, calls, 1, 1);
        let bound = world.component.worst_case().expect("pool bound");
        if point == Point::Closing {
            world.until(Point::Idle);
            world.delay_close = true;
            world.request(Request::Close);
            world.until(Point::Closing);
        } else {
            world.until(point);
            world.request(Request::Close);
        }
        world.finish();
        let sample = meter.end();
        assert!(sample.peak() <= bound, "{point:?} peak {} exceeds {bound}", sample.peak());
        drop(world);
        assert_eq!(meter.held(), 0);
    }
}

#[test]
fn every_declared_waiting_start_fits_the_checked_bound() {
    use skein_llm_connection_world::world::{Point, World};
    let meter = Meter::new();
    meter.start();
    let mut input = skein_llm_world::call(7);
    input.endpoint = skein_llm::Endpoint::codex();
    input.prompt.instructions = b"close-world".as_slice().into();
    let mut bounds = skein_llm_world::limits();
    bounds.http.request = skein_llm::client::request_head(
        &input.endpoint,
        &skein_llm::client::CredentialLimits { access_token: 2048, account_id: 128 },
        &bounds,
    )
    .unwrap();
    let measured = skein_llm::client::measure(&input.prompt, &input.credential, &input.endpoint, &bounds).unwrap();
    let memory = 2 * skein_llm::client::reservation(measured, &bounds).unwrap();
    drop(input);
    let mut world = World::empty_with_memory(59, 8, 1, 1, memory);
    for token in 7..15 {
        world.start(token);
    }
    assert_eq!(world.component.reserved(), memory);
    let bound = world.component.worst_case().expect("pool bound");
    world.until(Point::Head);
    assert_eq!(world.connections(), 1, "one running call, one connection wait, six memory waits");
    world.request(Request::Close);
    world.finish();
    assert_eq!(world.judge.completed, 8);
    assert_eq!(world.component.reserved(), 0);
    let sample = meter.end();
    assert!(sample.peak() <= bound, "waiting peak {} exceeds {bound}", sample.peak());
    drop(world);
    assert_eq!(meter.held(), 0);
}

#[test]
fn two_endpoints_with_different_limits_fit_their_measured_component_bound() {
    use skein_llm_connection_world::world::World;
    let small = World::configured(83, 3, 1, 1);
    let small_bound = small.component.worst_case().expect("small endpoint bound");
    drop(small);
    let meter = Meter::new();
    meter.start();
    let mut world = World::configured(89, 3, 1, 2);
    let bound = world.component.worst_case().expect("two endpoint bound");
    assert!(bound > small_bound, "the larger endpoint contributes to the checked bound");
    world.request(Request::Cancel { call: Token::new(9) });
    world.start_at(10, 1, Deadlines::none());
    world.request(Request::Close);
    world.finish();
    assert_eq!(world.judge.completed, 3);
    let sample = meter.end();
    assert!(sample.peak() <= bound, "two endpoint peak {} exceeds {bound}", sample.peak());
    drop(world);
    assert_eq!(meter.held(), 0);
}
