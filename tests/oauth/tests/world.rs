use skein_fake_oauth as fake;
use skein_lib::{Duration, Queue, Time, Wall, bytes};
use skein_oauth as oauth;

fn boxed(input: &[u8]) -> Box<[u8]> {
    bytes::copy_of(input)
}
fn time(seconds: u64) -> Time {
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
fn client_limits() -> oauth::ClientLimits {
    oauth::ClientLimits {
        document: documents(),
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
    }
}
fn fake_limits() -> fake::Limits {
    fake::Limits { document: documents(), uri_bytes: 256, request_bytes: 1024, codes: 8, rotations: 8, plans: 8 }
}
fn registration(public: bool) -> oauth::Registration {
    oauth::Registration {
        authorization_url: boxed(b"https://issuer.example/authorize"),
        token_endpoint: boxed(b"https://issuer.example/token"),
        client_id: boxed(b"client"),
        redirect_uri: if public {
            boxed(b"http://127.0.0.1:1234/callback")
        } else {
            boxed(b"https://web.example/callback")
        },
        scope: boxed(b"read profile"),
        wire: if public { oauth::WireFormat::Form } else { oauth::WireFormat::Json },
        client_secret: if public { None } else { Some(boxed(b"secret")) },
        pkce_for_confidential: false,
        metadata_claim: None,
    }
}
fn issuer(public: bool) -> fake::Issuer {
    fake::Issuer::new(
        fake::Config {
            authorization_url: boxed(b"https://issuer.example/authorize"),
            token_endpoint: boxed(b"https://issuer.example/token"),
            client_id: boxed(b"client"),
            client_secret: if public { None } else { Some(boxed(b"secret")) },
            redirect_uri: if public {
                boxed(b"http://127.0.0.1:1234/callback")
            } else {
                boxed(b"https://web.example/callback")
            },
            refresh_token: boxed(b"seed"),
        },
        fake_limits(),
    )
    .expect("issuer")
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
fn http(request: Option<oauth::Request>) -> oauth::HttpRequest {
    match request {
        Some(oauth::Request::Http(value)) => value,
        _ => panic!("HTTP request expected"),
    }
}
fn response(request: Option<fake::Request>) -> oauth::HttpResponse {
    match request {
        Some(fake::Request::Http(value)) => value,
        _ => panic!("HTTP response expected"),
    }
}
fn record(request: Option<oauth::Request>) -> oauth::SavedToken {
    match request {
        Some(oauth::Request::Tokens { record }) => record,
        _ => panic!("token record expected"),
    }
}
fn failure(request: Option<&oauth::Request>) -> oauth::Failure {
    match request {
        Some(oauth::Request::Failed { failure }) => *failure,
        _ => panic!("failure expected"),
    }
}
fn success(access: &[u8], refresh: Option<&[u8]>, delay: u64) -> fake::Plan {
    fake::Plan {
        status: 200,
        body: fake::Body::Token(oauth::TokenResponse {
            access_token: boxed(access),
            refresh_token: refresh.map(boxed),
            expires_in: 30,
        }),
        delay: Duration::from_secs(delay),
        retry_after: Duration::ZERO,
    }
}
fn error(status: u16, code: &[u8], retry_after: u64) -> fake::Plan {
    fake::Plan {
        status,
        body: fake::Body::Error(oauth::OAuthError { code: boxed(code), detail: boxed(b"scripted") }),
        delay: Duration::ZERO,
        retry_after: Duration::from_secs(retry_after),
    }
}
fn sign_in(client: &mut oauth::Client, issuer: &mut fake::Issuer, public: bool) -> oauth::HttpRequest {
    let visit = client_step(
        client,
        oauth::Event::SignIn {
            registration: registration(public),
            key: 7,
            generation: 0,
            state: boxed(b"0123456789abcdef"),
            verifier: if public { Some(boxed(b"dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk")) } else { None },
            now: time(1),
        },
    );
    let Some(oauth::Request::Visit { url }) = visit else { panic!("visit expected") };
    if public {
        assert!(bytes::find(&url, b"code_challenge=E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM").is_some());
    }
    let redirect = fake_step(issuer, fake::Event::Authorize { url, now: time(1) });
    let Some(fake::Request::Redirect { uri, state, code }) = redirect else { panic!("redirect expected") };
    http(client_step(client, oauth::Event::Redirected { uri, state, code: Some(code), error: None, now: time(2) }))
}

#[test]
fn a_localhost_registration_signs_in_through_its_exact_redirect() {
    let redirect_uri = b"http://localhost:1234/callback";
    let mut registration = registration(true);
    registration.redirect_uri = boxed(redirect_uri);
    let mut issuer = fake::Issuer::new(
        fake::Config {
            authorization_url: registration.authorization_url.clone(),
            token_endpoint: registration.token_endpoint.clone(),
            client_id: registration.client_id.clone(),
            client_secret: None,
            redirect_uri: boxed(redirect_uri),
            refresh_token: boxed(b"seed"),
        },
        fake_limits(),
    )
    .expect("issuer");
    issuer.queue(success(b"access", Some(b"refresh"), 0)).expect("plan");
    let mut client = oauth::Client::new(client_limits()).expect("client");
    let Some(oauth::Request::Visit { url }) = client_step(
        &mut client,
        oauth::Event::SignIn {
            registration,
            key: 7,
            generation: 0,
            state: boxed(b"0123456789abcdef"),
            verifier: Some(boxed(b"dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk")),
            now: time(1),
        },
    ) else {
        panic!("visit expected")
    };
    let Some(fake::Request::Redirect { uri, state, code }) =
        fake_step(&mut issuer, fake::Event::Authorize { url, now: time(1) })
    else {
        panic!("redirect expected")
    };
    assert_eq!(uri.as_ref(), redirect_uri);
    let request = http(client_step(
        &mut client,
        oauth::Event::Redirected { uri, state, code: Some(code), error: None, now: time(2) },
    ));
    assert!(bytes::find(&request.body, b"redirect_uri=http%3A%2F%2Flocalhost%3A1234%2Fcallback").is_some());
    let answer = response(fake_step(&mut issuer, fake::Event::Post { request, now: time(2), wall: wall(100) }));
    let saved = record(client_step(&mut client, oauth::Event::Http(answer)));
    assert_eq!(saved.key, 7);
    assert_eq!(saved.generation, 1);
    assert!(client.is_done());
}

#[test]
fn public_sign_in_refresh_rotation_and_old_refresh_replay() {
    let mut client = oauth::Client::new(client_limits()).expect("client");
    let mut issuer = issuer(true);
    issuer.queue(success(b"access-1", Some(b"refresh-1"), 0)).expect("plan");
    let request = sign_in(&mut client, &mut issuer, true);
    let answer = response(fake_step(&mut issuer, fake::Event::Post { request, now: time(2), wall: wall(100) }));
    let first = record(client_step(&mut client, oauth::Event::Http(answer)));
    assert_eq!(first.generation, 1);
    assert_eq!(first.refresh_token.as_deref().expect("refresh token"), b"refresh-1");
    assert_eq!(issuer.generation(), 1);

    issuer.queue(success(b"access-2", Some(b"refresh-2"), 0)).expect("plan");
    assert!(client_step(&mut client, oauth::Event::Reset).is_none());
    let request = http(client_step(
        &mut client,
        oauth::Event::Refresh {
            registration: registration(true),
            prior: first.refresh_state().expect("record has refresh state"),
            now: time(3),
        },
    ));
    let answer = response(fake_step(&mut issuer, fake::Event::Post { request, now: time(3), wall: wall(101) }));
    let second = record(client_step(&mut client, oauth::Event::Http(answer)));
    assert_eq!(second.generation, 2);
    assert_eq!(second.refresh_token.as_deref().expect("refresh token"), b"refresh-2");
    assert_eq!(issuer.generation(), 2);
    assert_eq!(second.remaining(wall(126)), Duration::from_secs(5));

    assert!(client_step(&mut client, oauth::Event::Reset).is_none());
    let replay = http(client_step(
        &mut client,
        oauth::Event::Refresh {
            registration: registration(true),
            prior: first.refresh_state().expect("record has refresh state"),
            now: time(4),
        },
    ));
    let refusal =
        response(fake_step(&mut issuer, fake::Event::Post { request: replay, now: time(4), wall: wall(102) }));
    assert_eq!(failure(client_step(&mut client, oauth::Event::Http(refusal)).as_ref()), oauth::Failure::Refused);
}

#[test]
fn confidential_sign_in_and_stable_refresh_use_the_same_machine() {
    let mut client = oauth::Client::new(client_limits()).expect("client");
    let mut issuer = issuer(false);
    issuer.queue(success(b"access-1", Some(b"refresh-1"), 0)).expect("plan");
    let request = sign_in(&mut client, &mut issuer, false);
    assert!(bytes::find(&request.body, b"\"client_secret\":\"secret\"").is_some());
    let answer = response(fake_step(&mut issuer, fake::Event::Post { request, now: time(2), wall: wall(100) }));
    let first = record(client_step(&mut client, oauth::Event::Http(answer)));
    issuer.queue(success(b"access-2", None, 0)).expect("plan");
    assert!(client_step(&mut client, oauth::Event::Reset).is_none());
    let request = http(client_step(
        &mut client,
        oauth::Event::Refresh {
            registration: registration(false),
            prior: first.refresh_state().expect("record has refresh state"),
            now: time(3),
        },
    ));
    let answer = response(fake_step(&mut issuer, fake::Event::Post { request, now: time(3), wall: wall(101) }));
    let second = record(client_step(&mut client, oauth::Event::Http(answer)));
    assert_eq!(second.refresh_token, first.refresh_token);
    assert_eq!(issuer.generation(), 1, "omission preserves the issuer's refresh token");
}

#[test]
fn issuer_revocation_error_and_malformed_document_are_typed() {
    let mut client = oauth::Client::new(client_limits()).expect("client");
    let mut issuer = issuer(true);
    issuer.queue(error(400, b"invalid_grant", 0)).expect("plan");
    let request = http(client_step(
        &mut client,
        oauth::Event::Refresh {
            registration: registration(true),
            prior: oauth::RefreshState { key: 1, generation: 0, refresh_token: boxed(b"seed") },
            now: time(1),
        },
    ));
    let answer = response(fake_step(&mut issuer, fake::Event::Post { request, now: time(2), wall: wall(100) }));
    assert_eq!(failure(client_step(&mut client, oauth::Event::Http(answer)).as_ref()), oauth::Failure::Refused);

    issuer
        .queue(fake::Plan {
            status: 200,
            body: fake::Body::Raw(boxed(b"{not-json")),
            delay: Duration::ZERO,
            retry_after: Duration::ZERO,
        })
        .expect("plan");
    assert!(client_step(&mut client, oauth::Event::Reset).is_none());
    let request = http(client_step(
        &mut client,
        oauth::Event::Refresh {
            registration: registration(true),
            prior: oauth::RefreshState { key: 1, generation: 0, refresh_token: boxed(b"seed") },
            now: time(3),
        },
    ));
    let answer = response(fake_step(&mut issuer, fake::Event::Post { request, now: time(3), wall: wall(101) }));
    assert_eq!(failure(client_step(&mut client, oauth::Event::Http(answer)).as_ref()), oauth::Failure::Malformed);
}

#[test]
fn delayed_rotated_answer_lost_before_delivery_makes_old_grant_revoked() {
    let mut client = oauth::Client::new(client_limits()).expect("client");
    let mut issuer = issuer(true);
    issuer.queue(success(b"access", Some(b"new"), 20)).expect("plan");
    let request = http(client_step(
        &mut client,
        oauth::Event::Refresh {
            registration: registration(true),
            prior: oauth::RefreshState { key: 1, generation: 0, refresh_token: boxed(b"seed") },
            now: time(1),
        },
    ));
    assert!(fake_step(&mut issuer, fake::Event::Post { request, now: time(2), wall: wall(100) }).is_none());
    assert_eq!(issuer.generation(), 1, "rotation precedes response delivery");
    assert_eq!(
        failure(client_step(&mut client, oauth::Event::Tick { now: time(11) }).as_ref()),
        oauth::Failure::TimedOut
    );
    assert!(fake_step(&mut issuer, fake::Event::LoseResponse).is_none());
    assert!(client_step(&mut client, oauth::Event::Reset).is_none());
    let retry = http(client_step(
        &mut client,
        oauth::Event::Refresh {
            registration: registration(true),
            prior: oauth::RefreshState { key: 1, generation: 0, refresh_token: boxed(b"seed") },
            now: time(12),
        },
    ));
    let answer = response(fake_step(&mut issuer, fake::Event::Post { request: retry, now: time(12), wall: wall(110) }));
    assert_eq!(failure(client_step(&mut client, oauth::Event::Http(answer)).as_ref()), oauth::Failure::Refused);
}

#[test]
fn issuer_rate_limit_then_client_backoff_reaches_success() {
    let mut client = oauth::Client::new(client_limits()).expect("client");
    let mut issuer = issuer(true);
    issuer.queue(error(429, b"slow_down", 2)).expect("rate limit");
    issuer.queue(success(b"access", Some(b"next"), 0)).expect("success");
    let request = http(client_step(
        &mut client,
        oauth::Event::Refresh {
            registration: registration(true),
            prior: oauth::RefreshState { key: 1, generation: 0, refresh_token: boxed(b"seed") },
            now: time(1),
        },
    ));
    let answer = response(fake_step(&mut issuer, fake::Event::Post { request, now: time(2), wall: wall(100) }));
    assert!(client_step(&mut client, oauth::Event::Http(answer)).is_none());
    assert_eq!(client.next_deadline(), Some(time(4)));
    let retry = http(client_step(&mut client, oauth::Event::Tick { now: time(4) }));
    let answer = response(fake_step(&mut issuer, fake::Event::Post { request: retry, now: time(4), wall: wall(102) }));
    let record = record(client_step(&mut client, oauth::Event::Http(answer)));
    assert_eq!(record.refresh_token.as_deref().expect("refresh token"), b"next");
}

#[test]
fn authorization_code_cannot_be_redeemed_twice() {
    let mut client = oauth::Client::new(client_limits()).expect("client");
    let mut issuer = issuer(true);
    issuer.queue(success(b"access", Some(b"refresh"), 0)).expect("plan");
    let request = sign_in(&mut client, &mut issuer, true);
    let replay = oauth::HttpRequest {
        id: request.id.saturating_add(1),
        endpoint: request.endpoint.clone(),
        content_type: request.content_type,
        body: request.body.clone(),
        deadline: request.deadline,
    };
    let answer = response(fake_step(&mut issuer, fake::Event::Post { request, now: time(2), wall: wall(100) }));
    let _record = record(client_step(&mut client, oauth::Event::Http(answer)));
    let refusal =
        response(fake_step(&mut issuer, fake::Event::Post { request: replay, now: time(3), wall: wall(101) }));
    assert_eq!(refusal.status, 400);
    assert!(bytes::find(&refusal.body, b"invalid_grant").is_some());
}
