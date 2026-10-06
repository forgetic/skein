use skein_fake_oauth as fake;
use skein_heap::{Counting, Meter};
use skein_lib::{Duration, Queue, Time, Wall, bytes};
use skein_oauth as oauth;

#[global_allocator]
static HEAP: Counting = Counting;

fn boxed(input: &[u8]) -> Box<[u8]> {
    bytes::copy_of(input)
}
fn documents() -> oauth::Limits {
    oauth::Limits {
        document_bytes: 1024,
        string_bytes: 256,
        token_bytes: 256,
        client_bytes: 64,
        detail_bytes: 64,
        record_bytes: 1024,
        depth: 8,
        tokens: 64,
    }
}

#[test]
fn client_and_rotating_issuer_steps_fit_declared_heap_bounds() {
    let document = documents();
    let client_limits = oauth::ClientLimits {
        document,
        uri_bytes: 256,
        scope_bytes: 64,
        state_bytes: 64,
        code_bytes: 64,
        url_bytes: 1024,
        request_bytes: 1024,
        sign_in_time: Duration::from_secs(120),
        request_time: Duration::from_secs(10),
        backoff_base: Duration::from_secs(2),
        backoff_ceiling: Duration::from_secs(8),
        max_attempts: 3,
    };
    let bound = oauth::client_worst_case(&client_limits).expect("bound");
    let meter = Meter::new();
    let mut client = oauth::Client::new(client_limits).expect("client");
    let mut out = Queue::with_capacity(1);
    let event = oauth::Event::Refresh {
        registration: oauth::Registration {
            authorization_url: boxed(b"https://issuer.example/authorize"),
            token_endpoint: boxed(b"https://issuer.example/token"),
            client_id: boxed(b"client"),
            redirect_uri: boxed(b"http://127.0.0.1:1234/callback"),
            scope: boxed(b""),
            wire: oauth::WireFormat::Json,
            client_secret: None,
            pkce_for_confidential: false,
            metadata_claim: None,
        },
        prior: oauth::RefreshState { key: 1, generation: 0, refresh_token: boxed(b"seed") },
        now: Time::ZERO,
    };
    meter.start();
    client.step(event, &mut out);
    let measured = meter.end();
    drop(out.pop());
    meter.check(measured, bound, &"OAuth client refresh");

    let fake_limits = fake::Limits { document, uri_bytes: 256, request_bytes: 1024, codes: 8, rotations: 8, plans: 8 };
    let bound = fake::worst_case(&fake_limits).expect("bound");
    let meter = Meter::new();
    let mut issuer = fake::Issuer::new(
        fake::Config {
            authorization_url: boxed(b"https://issuer.example/authorize"),
            token_endpoint: boxed(b"https://issuer.example/token"),
            client_id: boxed(b"client"),
            client_secret: None,
            redirect_uri: boxed(b"http://127.0.0.1:1234/callback"),
            refresh_token: boxed(b"seed"),
        },
        fake_limits,
    )
    .expect("issuer");
    issuer
        .queue(fake::Plan {
            status: 200,
            body: fake::Body::Token(oauth::TokenResponse {
                access_token: boxed(b"access"),
                refresh_token: Some(boxed(b"next")),
                expires_in: 30,
            }),
            delay: Duration::ZERO,
            retry_after: Duration::ZERO,
        })
        .expect("plan");
    let request = oauth::HttpRequest {
        id: 1,
        endpoint: boxed(b"https://issuer.example/token"),
        content_type: b"application/json",
        body: boxed(br#"{"grant_type":"refresh_token","client_id":"client","refresh_token":"seed"}"#),
        deadline: Time::from_nanos(10_000_000_000),
    };
    let mut out = Queue::with_capacity(1);
    meter.start();
    issuer.step(fake::Event::Post { request, now: Time::ZERO, wall: Wall::EPOCH }, &mut out);
    let measured = meter.end();
    drop(out.pop());
    meter.check(measured, bound, &"OAuth issuer rotation");
}
