use skein_fake_oauth as fake;
use skein_lib::{Duration, Queue, Time, Wall, bytes};
use skein_oauth as oauth;

fn box_of(input: &[u8]) -> Box<[u8]> {
    bytes::copy_of(input)
}
fn at(seconds: u64) -> Time {
    Time::from_nanos(seconds.saturating_mul(1_000_000_000))
}
fn wall(seconds: u64) -> Wall {
    Wall::from_nanos(seconds.saturating_mul(1_000_000_000))
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
fn client_step(client: &mut oauth::Client, event: oauth::Event) -> Option<oauth::Request> {
    let mut out = Queue::with_capacity(1);
    client.step(event, &mut out);
    out.pop()
}
fn fake_step(issuer: &mut fake::Issuer, event: fake::Event) -> Option<fake::Request> {
    let mut out = Queue::with_capacity(1);
    issuer.step(event, &mut out);
    out.pop()
}

fn registration(seed: u64) -> oauth::Registration {
    oauth::Registration {
        authorization_url: box_of(b"https://issuer.example/authorize"),
        token_endpoint: box_of(b"https://issuer.example/token"),
        client_id: box_of(b"client"),
        redirect_uri: box_of(b"http://127.0.0.1:1234/callback"),
        scope: box_of(b""),
        wire: if seed.is_multiple_of(2) { oauth::WireFormat::Json } else { oauth::WireFormat::Form },
        client_secret: None,
        pkce_for_confidential: false,
        metadata_claim: None,
    }
}

fn scenario(seed: u64) {
    let document = documents();
    let mut client = oauth::Client::new(oauth::ClientLimits {
        document,
        uri_bytes: 256,
        scope_bytes: 32,
        state_bytes: 32,
        code_bytes: 32,
        url_bytes: 512,
        request_bytes: 512,
        sign_in_time: Duration::from_secs(60),
        request_time: Duration::from_secs(10),
        backoff_base: Duration::from_secs(1),
        backoff_ceiling: Duration::from_secs(4),
        max_attempts: 2,
    })
    .expect("client");
    let mut issuer = fake::Issuer::new(
        fake::Config {
            authorization_url: box_of(b"https://issuer.example/authorize"),
            token_endpoint: box_of(b"https://issuer.example/token"),
            client_id: box_of(b"client"),
            client_secret: None,
            redirect_uri: box_of(b"http://127.0.0.1:1234/callback"),
            refresh_token: box_of(b"seed"),
        },
        fake::Limits { document, uri_bytes: 256, request_bytes: 512, codes: 2, rotations: 2, plans: 2 },
    )
    .expect("issuer");
    let delay = seed % 5;
    let mode = seed % 3;
    let plan = match mode {
        0 => fake::Plan {
            status: 200,
            body: fake::Body::Token(oauth::TokenResponse {
                access_token: box_of(b"access"),
                refresh_token: Some(box_of(b"next")),
                expires_in: 30,
            }),
            delay: Duration::from_secs(delay),
            retry_after: Duration::ZERO,
        },
        1 => fake::Plan {
            status: 200,
            body: fake::Body::Token(oauth::TokenResponse {
                access_token: box_of(b"access"),
                refresh_token: None,
                expires_in: 30,
            }),
            delay: Duration::from_secs(delay),
            retry_after: Duration::ZERO,
        },
        _ => fake::Plan {
            status: 400,
            body: fake::Body::Error(oauth::OAuthError { code: box_of(b"invalid_grant"), detail: box_of(b"revoked") }),
            delay: Duration::from_secs(delay),
            retry_after: Duration::ZERO,
        },
    };
    issuer.queue(plan).expect("plan");
    let Some(oauth::Request::Http(request)) = client_step(
        &mut client,
        oauth::Event::Refresh {
            registration: registration(seed),
            prior: oauth::RefreshState { key: 1, generation: 3, refresh_token: box_of(b"seed") },
            now: at(1),
        },
    ) else {
        panic!("request at seed {seed}")
    };
    let first = fake_step(&mut issuer, fake::Event::Post { request, now: at(2), wall: wall(100) });
    let answer = if delay == 0 {
        first
    } else {
        assert!(first.is_none(), "issuer waits at seed {seed}");
        fake_step(&mut issuer, fake::Event::Tick { now: at(2 + delay), wall: wall(100 + delay) })
    };
    let Some(fake::Request::Http(answer)) = answer else { panic!("answer at seed {seed}") };
    let terminal = client_step(&mut client, oauth::Event::Http(answer));
    match (mode, terminal) {
        (0, Some(oauth::Request::Tokens { record })) => {
            assert_eq!(record.refresh_token.as_deref().expect("refresh token"), b"next", "seed {seed}");
            assert_eq!(issuer.generation(), 1, "seed {seed}");
        }
        (1, Some(oauth::Request::Tokens { record })) => {
            assert_eq!(record.refresh_token.as_deref().expect("refresh token"), b"seed", "seed {seed}");
            assert_eq!(issuer.generation(), 0, "seed {seed}");
        }
        (2, Some(oauth::Request::Failed { failure: oauth::Failure::Refused })) => {}
        _ => panic!("unexpected terminal at seed {seed}"),
    }
    assert!(client.is_done(), "one terminal at seed {seed}");
}

#[test]
fn varied_delay_rotation_and_error_body_keep_one_terminal_per_refresh() {
    for seed in 0_u64..192 {
        scenario(seed);
    }
}
