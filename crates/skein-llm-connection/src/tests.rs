use super::*;
use core::net::Ipv4Addr;
use skein_io::kernel::Addr;
use skein_lib::bytes;
use skein_lib::{Duration, List, Token};
use skein_llm::{Block, Credential, Message, Prompt, Role};

fn limits() -> Limits {
    Limits {
        endpoints: 1,
        connections: 1,
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
        llm: skein_llm::client::Limits {
            http: skein_http::client::Limits { request: 4096, head: 4096, headers: 32, read: 256, send: 31 },
            sse: skein_http::sse::Limits { line: 4096, event: 8192, field: 128, chunk: 128 },
            dialect: skein_llm::DocumentLimits {
                request_bytes: 8192,
                document_bytes: 8192,
                string_bytes: 4096,
                depth: 32,
                tokens: 1024,
                parts: 16,
                input_bytes: 2048,
                opaque_bytes: 2048,
                answer_bytes: 8192,
                detail_bytes: 256,
            },
            error_bytes: 4096,
        },
    }
}

fn endpoint() -> Endpoint {
    let mut roots = skein_tls::RootCertStore::empty();
    roots
        .add(skein_tls::CertificateDer::from(
            std::fs::read("../../tests/tls/fixtures/root.der").expect("fixture bytes"),
        ))
        .expect("fixture root");
    Endpoint {
        address: Addr::from((Ipv4Addr::LOCALHOST, 443)),
        server_name: skein_tls::Name::new("example.test").expect("valid test name"),
        trust: skein_tls::Config::new(roots, &[]).expect("valid test trust"),
        llm: skein_llm::Endpoint::codex(),
    }
}

fn prompt() -> Prompt {
    Prompt {
        model: bytes::copy_of(b"test-model"),
        instructions: Box::new([]),
        tools: Box::new([]),
        messages: Box::new([Message {
            role: Role::User,
            content: Box::new([Block::Text { text: bytes::copy_of(b"hi"), replay: None }]),
        }]),
        reasoning_effort: None,
        cache_key: None,
        max_output_tokens: None,
    }
}

fn credential() -> Credential {
    Credential { access_token: bytes::copy_of(b"secret"), account_id: bytes::copy_of(b"account") }
}

#[test]
fn unknown_endpoint_is_refused() {
    let limits = limits();
    let component = Component::new(List::with_capacity(1), &limits).expect("empty configuration fits");
    assert_eq!(component.admit(0, Token::new(7), 0, prompt(), credential()).err(), Some(Refusal::Endpoint));
}

#[test]
fn full_pool_is_refused() {
    let limits = limits();
    let mut endpoints = List::with_capacity(1);
    endpoints.push(endpoint()).expect("one endpoint fits");
    let component = Component::new(endpoints, &limits).expect("valid endpoints");
    drop(component.admit(0, Token::new(7), 0, prompt(), credential()).expect("valid call admitted"));
    assert_eq!(component.admit(1, Token::new(8), 0, prompt(), credential()).err(), Some(Refusal::Pool));
}

#[test]
fn invalid_and_oversized_requests_are_refused() {
    let limits = limits();
    let mut endpoints = List::with_capacity(1);
    endpoints.push(endpoint()).expect("one endpoint fits");
    let component = Component::new(endpoints, &limits).expect("valid endpoints");
    let mut invalid = prompt();
    invalid.model = Box::new([]);
    assert_eq!(
        component.admit(0, Token::new(7), 0, invalid, credential()).err(),
        Some(Refusal::Client(skein_llm::Error::Invalid))
    );
    let mut oversized = prompt();
    oversized.instructions = Box::new([b'x'; 9000]);
    assert_eq!(
        component.admit(0, Token::new(7), 0, oversized, credential()).err(),
        Some(Refusal::Client(skein_llm::Error::Limit))
    );
}

#[test]
fn startup_rejects_impossible_limits() {
    let mut config = limits();
    config.io.intake = 1;
    assert_eq!(Component::new(List::with_capacity(0), &config).err(), Some(EndpointError::Stream));
    let mut config = limits();
    config.connections = 0;
    assert_eq!(Component::new(List::with_capacity(0), &config).err(), Some(EndpointError::Limits));
    let mut config = limits();
    config.endpoints = 0;
    let mut endpoints = List::with_capacity(1);
    endpoints.push(endpoint()).expect("one endpoint fits");
    assert_eq!(Component::new(endpoints, &config).err(), Some(EndpointError::TooMany));
}
