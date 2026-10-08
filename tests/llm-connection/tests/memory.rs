//! A full connection pool stays within the component's checked heap bound.

use core::net::Ipv4Addr;

use skein_heap::{Counting, Meter};
use skein_io::kernel::Addr;
use skein_lib::{Duration, Env, List, Queue, Time, Token};
use skein_llm_connection::{Component, Deadlines, Endpoint, Event, Limits, MAX_OUT, Request, worst_case};
use skein_tls_world::pki;

#[global_allocator]
static HEAP: Counting = Counting;

#[test]
fn full_pool_stays_within_its_checked_bound() {
    let limits = Limits {
        endpoints: 1,
        connections: 2,
        per_endpoint: 2,
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
        llm: skein_llm_world::limits(),
    };
    let bound = worst_case(&limits).expect("representable pool bound");
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
        })
        .expect("one endpoint");
    let first = skein_llm_world::call(7);
    let second = skein_llm_world::call(8);
    let third = skein_llm_world::call(9);
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    let env = Env { now: Time::ZERO, wall: pki::VALID, limits };
    let mut component = Component::new(endpoints, &limits).expect("component");
    component.down(
        &env,
        Request::Start {
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
    component.down(
        &env,
        Request::Start {
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
            assert_eq!(why, skein_llm_connection::Refusal::Pool);
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
