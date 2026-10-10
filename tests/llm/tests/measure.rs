//! Admission measures the same wire and refusals as preparation in both dialects.

use skein_llm::client::{self, Client};
use skein_llm::{Call, Credential, Endpoint};
use skein_llm_world::{World, call, limits, text_response};

fn request(anthropic: bool) -> Call {
    let mut input = call(1);
    input.prompt.instructions = b"quotes: \" and newline:\n and unicode: \xc3\xa9".as_slice().into();
    input.prompt.affinity = Some(skein_llm::Affinity { key: [0x42; 16], thread: 19 });
    if anthropic {
        input.endpoint = Endpoint::anthropic();
        input.credential = Credential::anthropic(b"synthetic-token".as_slice().into());
    }
    input
}

#[test]
fn measured_head_and_body_equal_the_uploaded_wire() {
    for anthropic in [false, true] {
        let input = request(anthropic);
        let bounds = limits();
        let measured = client::measure(&input.prompt, &input.credential, &input.endpoint, &bounds).unwrap();
        let mut world = World::new(input, bounds, text_response(false), 47);
        world.request(client::Request::Start);
        for _ in 0..100_000 {
            if !world.tick(true) {
                break;
            }
        }
        world.settle();
        let head = world.sent.windows(4).position(|bytes| bytes == b"\r\n\r\n").unwrap() + 4;
        assert_eq!(u32::try_from(head).unwrap(), measured.head);
        assert_eq!(u32::try_from(world.sent.len() - head).unwrap(), measured.body);
        assert_eq!(
            client::reservation(measured, &bounds).unwrap(),
            u64::from(measured.head) + u64::from(measured.body) + u64::from(bounds.receiving)
        );
    }
}

#[test]
fn measurement_and_preparation_make_the_same_entrance_refusals() {
    for anthropic in [false, true] {
        for case in 0..10 {
            let mut input = request(anthropic);
            let mut bounds = limits();
            match case {
                0 => input.prompt.model = Box::new([]),
                1 => input.credential.access_token = b"bad\r\nvalue".as_slice().into(),
                2 => input.endpoint.target = b"bad target".as_slice().into(),
                3 => bounds.request = 1,
                4 => bounds.http.request = 1,
                5 => bounds.http.headers = 1,
                6 => bounds.output_items = 0,
                7 => bounds.tokens = 0,
                8 => bounds.error_bytes = 0,
                9 => bounds.declared_output_tokens = 0,
                _ => unreachable!(),
            }
            let measured = client::measure(&input.prompt, &input.credential, &input.endpoint, &bounds);
            let prepared = Client::prepare(input, &bounds);
            assert_eq!(measured.err(), prepared.err(), "dialect {anthropic}, case {case}");
        }
    }
}

#[test]
fn request_payload_limits_belong_to_the_reservation_not_fixed_client_state() {
    let bounds = limits();
    let fixed = client::worst_case(&bounds).unwrap();
    let largest = client::largest_reservation(&bounds).unwrap();
    let mut larger = bounds;
    larger.http.request += 99;
    larger.request += 1234;
    assert_eq!(client::worst_case(&larger).unwrap(), fixed);
    assert_eq!(client::largest_reservation(&larger).unwrap() - largest, 1333);
}
