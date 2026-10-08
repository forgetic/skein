use crate::*;
use alloc::boxed::Box;
use skein_lib::{Duration, Queue, Time, Wall, bytes};

fn boxed(input: &[u8]) -> Box<[u8]> {
    bytes::copy_of(input)
}
fn time(seconds: u64) -> Time {
    Time::from_nanos(seconds.saturating_mul(1_000_000_000))
}
fn wall(seconds: u64) -> Wall {
    Wall::from_nanos(seconds.saturating_mul(1_000_000_000))
}
fn limits() -> ClientLimits {
    ClientLimits {
        document: Limits {
            document_bytes: 1024,
            string_bytes: 512,
            token_bytes: 256,
            client_bytes: 64,
            detail_bytes: 64,
            record_bytes: 1024,
            depth: 8,
            tokens: 64,
        },
        uri_bytes: 256,
        scope_bytes: 64,
        state_bytes: 64,
        code_bytes: 128,
        url_bytes: 1024,
        request_bytes: 1024,
        sign_in_time: Duration::from_secs(120),
        request_time: Duration::from_secs(10),
        backoff_base: Duration::from_secs(2),
        backoff_ceiling: Duration::from_secs(8),
        max_attempts: 3,
    }
}
fn registration(public: bool, wire: WireFormat) -> Registration {
    Registration {
        authorization_url: boxed(b"https://issuer.example/authorize"),
        token_endpoint: boxed(b"https://issuer.example/token"),
        client_id: boxed(b"client"),
        redirect_uri: if public {
            boxed(b"http://127.0.0.1:2345/callback")
        } else {
            boxed(b"https://web.example/callback")
        },
        scope: boxed(b"read profile"),
        wire,
        client_secret: if public { None } else { Some(boxed(b"secret")) },
        pkce_for_confidential: false,
        metadata_claim: None,
    }
}
fn state() -> Box<[u8]> {
    boxed(b"0123456789abcdef")
}
fn verifier() -> Box<[u8]> {
    boxed(b"dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk")
}
fn step(client: &mut Client, event: Event) -> Option<Request> {
    let mut out = Queue::with_capacity(1);
    client.step(event, &mut out);
    out.pop()
}
fn request(result: Option<Request>) -> HttpRequest {
    match result {
        Some(Request::Http(request)) => request,
        _ => panic!("HTTP demand expected"),
    }
}
fn failure(result: Option<Request>) -> Failure {
    match result {
        Some(Request::Failed { failure }) => failure,
        _ => panic!("failure expected"),
    }
}
fn token_body() -> Box<[u8]> {
    encode_response(
        &TokenResponse { access_token: boxed(b"access"), refresh_token: Some(boxed(b"refresh-new")), expires_in: 30 },
        &limits().document,
    )
    .expect("response")
}
fn answered(id: u64, status: u16, body: &[u8], now: u64) -> Event {
    Event::Http(HttpResponse {
        id,
        status,
        body: boxed(body),
        retry_after: Duration::ZERO,
        evidence: HttpEvidence::Response,
        now: time(now),
        wall: wall(now),
    })
}

#[test]
fn public_sign_in_uses_s256_state_and_one_code_exchange() {
    let mut client = Client::new(limits()).expect("limits");
    let visit = step(
        &mut client,
        Event::SignIn {
            registration: registration(true, WireFormat::Form),
            key: 7,
            generation: 0,
            state: state(),
            verifier: Some(verifier()),
            now: time(1),
        },
    );
    let Some(Request::Visit { url }) = visit else { panic!("visit expected") };
    assert!(bytes::find(&url, b"code_challenge=E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM").is_some());
    assert!(bytes::find(&url, b"state=0123456789abcdef").is_some());
    let uri = boxed(b"http://127.0.0.1:2345/callback");
    let http = request(step(
        &mut client,
        Event::Redirected { uri, state: state(), code: Some(boxed(b"one-use")), error: None, now: time(2) },
    ));
    assert!(bytes::find(&http.body, b"code_verifier=").is_some());
    assert!(bytes::find(&http.body, b"code=one-use").is_some());
    let Some(Request::Tokens { record }) = step(&mut client, answered(http.id, 200, &token_body(), 3)) else {
        panic!("tokens expected")
    };
    assert_eq!(record.key, 7);
    assert_eq!(record.generation, 1);
    assert_eq!(record.refresh_token.as_ref(), b"refresh-new");
    assert!(client.is_done());
    assert!(step(&mut client, answered(http.id, 200, &token_body(), 4)).is_none(), "late answer ignored");
}

#[test]
fn confidential_sign_in_sends_secret_in_json_only_to_token_endpoint() {
    let mut client = Client::new(limits()).expect("limits");
    let visit = step(
        &mut client,
        Event::SignIn {
            registration: registration(false, WireFormat::Json),
            key: 1,
            generation: 8,
            state: state(),
            verifier: None,
            now: time(1),
        },
    );
    let Some(Request::Visit { url }) = visit else { panic!("visit expected") };
    assert!(bytes::find(&url, b"secret").is_none());
    let http = request(step(
        &mut client,
        Event::Redirected {
            uri: boxed(b"https://web.example/callback"),
            state: state(),
            code: Some(boxed(b"grant")),
            error: None,
            now: time(2),
        },
    ));
    assert!(bytes::find(&http.body, b"\"client_secret\":\"secret\"").is_some());
    assert!(bytes::find(&http.body, b"\"grant_type\":\"authorization_code\"").is_some());
    let Some(Request::Tokens { record }) = step(&mut client, answered(http.id, 200, &token_body(), 3)) else {
        panic!("tokens expected")
    };
    assert_eq!(record.generation, 9);
}

#[test]
fn confidential_registration_can_require_pkce_too() {
    let mut client = Client::new(limits()).expect("limits");
    let mut registration = registration(false, WireFormat::Form);
    registration.pkce_for_confidential = true;
    let Some(Request::Visit { url }) = step(
        &mut client,
        Event::SignIn { registration, key: 1, generation: 0, state: state(), verifier: Some(verifier()), now: time(1) },
    ) else {
        panic!("visit expected")
    };
    assert!(bytes::find(&url, b"code_challenge_method=S256").is_some());
    let http = request(step(
        &mut client,
        Event::Redirected {
            uri: boxed(b"https://web.example/callback"),
            state: state(),
            code: Some(boxed(b"grant")),
            error: None,
            now: time(2),
        },
    ));
    assert!(bytes::find(&http.body, b"code_verifier=").is_some());
    assert!(bytes::find(&http.body, b"client_secret=secret").is_some());
}

#[test]
fn public_redirect_must_have_a_numeric_loopback_port() {
    for uri in [
        b"http://127.0.0.1:evil/callback".as_slice(),
        b"http://127.0.0.1:0/callback",
        b"http://127.0.0.1:65536/callback",
        b"http://127.0.0.1:3456.evil/callback",
    ] {
        let mut client = Client::new(limits()).expect("limits");
        let mut registration = registration(true, WireFormat::Form);
        registration.redirect_uri = boxed(uri);
        assert_eq!(
            failure(step(
                &mut client,
                Event::SignIn {
                    registration,
                    key: 1,
                    generation: 0,
                    state: state(),
                    verifier: Some(verifier()),
                    now: time(1)
                }
            )),
            Failure::InvalidRedirect
        );
    }
}

#[test]
fn wrong_state_and_redirect_are_terminal_without_exchange() {
    for wrong_uri in [false, true] {
        let mut client = Client::new(limits()).expect("limits");
        let _visit = step(
            &mut client,
            Event::SignIn {
                registration: registration(true, WireFormat::Form),
                key: 1,
                generation: 0,
                state: state(),
                verifier: Some(verifier()),
                now: time(1),
            },
        );
        let uri =
            if wrong_uri { boxed(b"http://127.0.0.1:2345/other") } else { boxed(b"http://127.0.0.1:2345/callback") };
        let returned_state = if wrong_uri { state() } else { boxed(b"unrelated-state!!") };
        assert_eq!(
            failure(step(
                &mut client,
                Event::Redirected { uri, state: returned_state, code: Some(boxed(b"code")), error: None, now: time(2) }
            )),
            Failure::InvalidRedirect
        );
        assert!(
            step(
                &mut client,
                Event::Redirected {
                    uri: boxed(b"http://127.0.0.1:2345/callback"),
                    state: state(),
                    code: Some(boxed(b"code")),
                    error: None,
                    now: time(3)
                }
            )
            .is_none()
        );
    }
}

#[test]
fn refresh_rotation_preserves_old_token_when_answer_omits_one() {
    let mut client = Client::new(limits()).expect("limits");
    let http = request(step(
        &mut client,
        Event::Refresh {
            registration: registration(true, WireFormat::Json),
            prior: RefreshState { key: 3, generation: 4, refresh_token: boxed(b"old") },
            now: time(1),
        },
    ));
    assert!(bytes::find(&http.body, b"\"refresh_token\":\"old\"").is_some());
    let body = encode_response(
        &TokenResponse { access_token: boxed(b"access"), refresh_token: None, expires_in: 30 },
        &limits().document,
    )
    .expect("response");
    let Some(Request::Tokens { record }) = step(&mut client, answered(http.id, 200, &body, 2)) else {
        panic!("tokens expected")
    };
    assert_eq!(record.refresh_token.as_ref(), b"old");
    assert_eq!(record.generation, 5);
}

#[test]
fn rejected_grant_malformed_answer_and_late_answer_are_distinct() {
    for (status, body, expected) in [
        (400, br#"{"error":"invalid_grant"}"#.as_slice(), Failure::Refused),
        (200, br#"{"access_token":1}"#, Failure::Malformed),
    ] {
        let mut client = Client::new(limits()).expect("limits");
        let http = request(step(
            &mut client,
            Event::Refresh {
                registration: registration(true, WireFormat::Form),
                prior: RefreshState { key: 1, generation: 0, refresh_token: boxed(b"old") },
                now: time(1),
            },
        ));
        assert_eq!(failure(step(&mut client, answered(http.id, status, body, 2))), expected);
    }
    let mut client = Client::new(limits()).expect("limits");
    let http = request(step(
        &mut client,
        Event::Refresh {
            registration: registration(true, WireFormat::Form),
            prior: RefreshState { key: 1, generation: 0, refresh_token: boxed(b"old") },
            now: time(1),
        },
    ));
    assert_eq!(failure(step(&mut client, answered(http.id, 200, &token_body(), 12))), Failure::TimedOut);
}

#[test]
fn proved_unsent_attempt_retries_with_bounded_backoff() {
    let mut client = Client::new(limits()).expect("limits");
    let http = request(step(
        &mut client,
        Event::Refresh {
            registration: registration(true, WireFormat::Form),
            prior: RefreshState { key: 1, generation: 0, refresh_token: boxed(b"old") },
            now: time(1),
        },
    ));
    assert!(
        step(
            &mut client,
            Event::Http(HttpResponse {
                id: http.id,
                status: 0,
                body: boxed(b""),
                retry_after: Duration::ZERO,
                evidence: HttpEvidence::Unsent,
                now: time(2),
                wall: wall(2)
            })
        )
        .is_none()
    );
    assert_eq!(client.next_deadline(), Some(time(4)));
    assert!(step(&mut client, Event::Tick { now: time(3) }).is_none());
    let retry = request(step(&mut client, Event::Tick { now: time(4) }));
    assert_ne!(retry.id, http.id);
    assert_eq!(retry.body, http.body);
    assert!(step(&mut client, answered(http.id, 200, &token_body(), 5)).is_none(), "old answer ignored");
    let Some(Request::Tokens { .. }) = step(&mut client, answered(retry.id, 200, &token_body(), 5)) else {
        panic!("tokens expected")
    };
}

#[test]
fn issuer_http_endpoints_are_admitted_only_on_numeric_loopback() {
    for (uri, admitted) in [
        (b"http://127.0.0.1/issuer".as_slice(), true),
        (b"http://127.0.0.0:1/issuer", true),
        (b"http://127.255.255.255:65535/issuer", true),
        (b"http://127.42.1.2/issuer?tenant=one", true),
        (b"http://127.0.0.1", true),
        (b"http://127.0.0.1?tenant=one", true),
        (b"http://[::1]/issuer", true),
        (b"http://[0:0:0:0:0:0:0:1]:80/issuer", true),
        (b"http://[::1]:65535/issuer", true),
        (b"http://126.255.255.255/issuer", false),
        (b"http://128.0.0.0/issuer", false),
        (b"http://0.0.0.0/issuer", false),
        (b"http://192.168.1.1/issuer", false),
        (b"http://[::]/issuer", false),
        (b"http://[::2]/issuer", false),
        (b"http://[::ffff:127.0.0.1]/issuer", false),
        (b"http://issuer.example/issuer", false),
        (b"http://localhost/issuer", false),
        (b"http://127.0.0.1.evil/issuer", false),
        (b"http://127.0.0.1@evil/issuer", false),
        (b"http://user@127.0.0.1/issuer", false),
        (b"http://[::1]@evil/issuer", false),
        (b"http://127.0.0.1:80@evil/issuer", false),
        (b"http://127.0.0.1:0/issuer", false),
        (b"http://127.0.0.1:65536/issuer", false),
        (b"http://127.0.0.1:/issuer", false),
        (b"http://127.0.0.1:evil/issuer", false),
        (b"http://[::1]:0/issuer", false),
        (b"http://[::1]:65536/issuer", false),
        (b"http://[::1]evil/issuer", false),
        (b"http://[::1/issuer", false),
        (b"http://127.1/issuer", false),
        (b"http://2130706433/issuer", false),
        (b"http://0x7f000001/issuer", false),
        (b"http://127.00.0.1/issuer", false),
        (b"ftp://127.0.0.1/issuer", false),
    ] {
        for authorization in [false, true] {
            for public in [false, true] {
                let mut configured = registration(public, WireFormat::Form);
                if authorization {
                    configured.authorization_url = boxed(uri);
                } else {
                    configured.token_endpoint = boxed(uri);
                }
                let expected_token = configured.token_endpoint.clone();
                let mut client = Client::new(limits()).expect("limits");
                let result = step(
                    &mut client,
                    Event::SignIn {
                        registration: configured,
                        key: 1,
                        generation: 0,
                        state: state(),
                        verifier: if public { Some(verifier()) } else { None },
                        now: time(1),
                    },
                );
                if admitted {
                    match result {
                        Some(Request::Visit { url }) => {
                            if authorization {
                                assert!(url.starts_with(uri));
                            }
                        }
                        other => panic!("expected Visit for {uri:?}, got {}", other.is_some()),
                    }
                    let http = request(step(
                        &mut client,
                        Event::Redirected {
                            uri: if public {
                                boxed(b"http://127.0.0.1:2345/callback")
                            } else {
                                boxed(b"https://web.example/callback")
                            },
                            state: state(),
                            code: Some(boxed(b"grant")),
                            error: None,
                            now: time(2),
                        },
                    ));
                    assert_eq!(http.endpoint, expected_token, "the endpoint binding survives the redirect");
                } else {
                    assert_eq!(failure(result), Failure::Malformed, "{uri:?}");
                    assert!(client.is_done());
                    assert!(step(&mut client, Event::Tick { now: time(2) }).is_none());
                }
            }
        }
    }
}

#[test]
fn refresh_keeps_its_admitted_http_loopback_endpoint() {
    for (uri, admitted) in [
        (b"http://127.1.2.3:9876/token".as_slice(), true),
        (b"http://[::1]:9876/token", true),
        (b"http://issuer.example/token", false),
    ] {
        let mut configured = registration(true, WireFormat::Form);
        configured.token_endpoint = boxed(uri);
        let prior = RefreshState { key: 7, generation: 1, refresh_token: boxed(b"refresh-old") };
        let mut client = Client::new(limits()).expect("limits");
        let result = step(&mut client, Event::Refresh { registration: configured, prior, now: time(1) });
        if admitted {
            assert_eq!(request(result).endpoint.as_ref(), uri);
        } else {
            assert_eq!(failure(result), Failure::Malformed);
        }
    }
}
