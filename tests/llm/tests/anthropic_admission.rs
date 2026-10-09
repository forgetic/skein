//! Admission and exact HTTP head bounds for the subscription route.
use skein_http::Header;
use skein_llm::{Credential, Endpoint, Error, anthropic, client};
use skein_llm_world::{World, call, limits, response};

fn anthropic_call() -> skein_llm::Call {
    let mut input = call(1);
    input.endpoint = Endpoint::anthropic();
    input.credential = Credential::anthropic(b"test-token".to_vec().into());
    input.prompt.affinity = None;
    input
}
fn header(name: &[u8], value: &[u8]) -> Header {
    Header { name: name.to_vec().into(), value: value.to_vec().into() }
}
fn rejection(input: skein_llm::Call, bounds: &client::Limits) -> Error {
    match client::Client::prepare(input, bounds) {
        Err(error) => error,
        Ok(_) => panic!("expected admission rejection"),
    }
}
#[test]
fn credentials_and_reserved_headers_cannot_cross_auth_modes() {
    let mut input = anthropic_call();
    input.credential.account_id = b"unexpected-account".to_vec().into();
    assert_eq!(rejection(input, &limits()), Error::Invalid);
    for name in [
        b"Authorization".as_slice(),
        b"x-api-key",
        b"CHATGPT-ACCOUNT-ID",
        b"Content-Length",
        b"SESSION-ID",
        b"Thread-Id",
    ] {
        let mut input = anthropic_call();
        input.endpoint.headers = Box::new([header(name, b"override")]);
        assert_eq!(rejection(input, &limits()), Error::Invalid);
    }
    let mut input = anthropic_call();
    input.credential.access_token = b"secret\r\nInjected: header".to_vec().into();
    assert_eq!(rejection(input, &limits()), Error::Invalid);
    let mut input = call(1);
    input.prompt.max_output_tokens = Some(100);
    assert_eq!(rejection(input, &limits()), Error::Unsupported);
}
#[test]
fn version_and_beta_overrides_are_single_case_insensitive_fields() {
    let mut input = anthropic_call();
    input.endpoint.headers = Box::new([
        header(b"Anthropic-Beta", b"oauth-2025-04-20,interleaved-thinking-2025-05-14"),
        header(b"Anthropic-Version", b"2023-06-01"),
    ]);
    let mut world = World::new(input, limits(), response(401, "", b"{}", false), 1);
    world.request(client::Request::Start);
    world.run();
    let wire = String::from_utf8(world.sent.clone()).unwrap();
    let head = wire.split_once("\r\n\r\n").unwrap().0.to_ascii_lowercase();
    assert_eq!(head.matches("anthropic-beta:").count(), 1);
    assert_eq!(head.matches("anthropic-version:").count(), 1);
    assert!(head.contains("oauth-2025-04-20,interleaved-thinking-2025-05-14"));
    let mut input = anthropic_call();
    input.endpoint.headers = Box::new([header(b"anthropic-beta", b"a"), header(b"Anthropic-Beta", b"b")]);
    assert_eq!(rejection(input, &limits()), Error::Invalid);
}
#[test]
fn measured_http_head_accepts_exact_cap_and_rejects_one_byte_less() {
    for profile in [false, true] {
        let mut input = anthropic_call();
        if profile {
            input.endpoint.headers = anthropic::identity::claude_code_headers();
        }
        let mut bounds = limits();
        bounds.http.headers = 32;
        bounds.http.request = 4096;
        let mut world = World::new(input, bounds, response(401, "", b"{}", false), 1);
        world.request(client::Request::Start);
        world.run();
        let split = world.sent.windows(4).position(|bytes| bytes == b"\r\n\r\n").unwrap() + 4;
        bounds.http.request = u32::try_from(split).unwrap();
        let mut input = anthropic_call();
        if profile {
            input.endpoint.headers = anthropic::identity::claude_code_headers();
        }
        assert!(client::Client::prepare(input, &bounds).is_ok());
        bounds.http.request -= 1;
        let mut input = anthropic_call();
        if profile {
            input.endpoint.headers = anthropic::identity::claude_code_headers();
        }
        assert_eq!(
            rejection(input, &bounds),
            Error::Limit { which: skein_llm::Cap::RequestHead, bound: u64::from(bounds.http.request) }
        );
    }
}
#[test]
fn fixed_header_count_is_validated_before_encoding() {
    let mut bounds = limits();
    bounds.http.headers = 6;
    assert_eq!(rejection(anthropic_call(), &bounds), Error::Limit { which: skein_llm::Cap::ResponseFields, bound: 6 });
    bounds.http.headers = 7;
    assert!(client::Client::prepare(anthropic_call(), &bounds).is_ok());
}
