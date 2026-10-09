//! The durable opaque seam exercised independently of application domains.

use skein_llm::{Block, Error, Json, Message, Provider, Replay, Role, client};
use skein_llm_world::{call, limits};

#[test]
fn replay_envelopes_keep_tag_fields_and_exact_bounds_before_negative_mutations() {
    let bounds = limits();
    for provider in [Provider::OpenAiCodex, Provider::Anthropic] {
        let replay = Replay {
            provider,
            value: Json::from_bytes(
                br#"{"opaque":{"signed":[true,1,null]},"text":"literal"}"#,
                &bounds.native().document(),
            )
            .expect("whole opaque document"),
        };
        let encoded = replay.to_bytes(&bounds.native().document()).expect("bounded envelope");
        assert_eq!(Replay::from_bytes(&encoded, &bounds.native().document()), Ok(replay.clone()));
        let exact = skein_llm::DocumentLimits {
            bytes: u32::try_from(encoded.len()).expect("bounded bytes") - skein_llm::REPLAY_HEADER_BYTES,
            ..bounds.native().document()
        };
        assert_eq!(replay.to_bytes(&exact), Ok(encoded.clone()));
        assert_eq!(skein_llm::replay_bytes(&exact), Some(u32::try_from(encoded.len()).expect("bounded envelope")));
        let tight = skein_llm::DocumentLimits { bytes: exact.bytes - 1, ..exact };
        assert_eq!(
            replay.to_bytes(&tight),
            Err(Error::Limit { which: skein_llm::Cap::Retained, bound: u64::from(tight.bytes) })
        );
        assert_eq!(
            Replay::from_bytes(&encoded, &tight),
            Err(Error::Limit { which: skein_llm::Cap::Retained, bound: u64::from(tight.bytes) })
        );
        for at in 0..encoded.len() {
            assert!(
                Replay::from_bytes(&encoded[..at], &bounds.native().document()).is_err(),
                "truncation has no complete replay"
            );
        }
        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert_eq!(Replay::from_bytes(&trailing, &bounds.native().document()), Err(Error::Invalid));
        for (offset, replacement) in [(0_usize, 7_u8), (2, 99)] {
            let mut corrupted = encoded.to_vec();
            corrupted[offset] = replacement;
            assert_eq!(Replay::from_bytes(&corrupted, &bounds.native().document()), Err(Error::Unsupported));
        }
    }
}

#[test]
fn provider_mismatch_is_rejected_by_actual_client_after_positive_control() {
    let bounds = limits();
    let mut input = call(1);
    let replay = Replay {
        provider: Provider::OpenAiCodex,
        value: Json::from_bytes(br#"{"id":"message-id","phase":"final_answer"}"#, &bounds.native().document())
            .expect("metadata"),
    };
    let encoded = replay.to_bytes(&bounds.native().document()).expect("opaque seam");
    let replay = Replay::from_bytes(&encoded, &bounds.native().document()).expect("restore envelope");
    input.prompt.messages = Box::new([Message {
        role: Role::Assistant,
        content: Box::new([Block::Text { text: b"prior".as_slice().into(), replay: Some(replay) }]),
    }]);
    assert!(client::Client::prepare(input, &bounds).is_ok(), "matching replay reaches actual Client admission");
    let mut input = call(2);
    input.endpoint = skein_llm::Endpoint::anthropic();
    input.credential = skein_llm::Credential::anthropic(b"fake-token".as_slice().into());
    input.prompt.affinity = None;
    input.prompt.messages = Box::new([Message {
        role: Role::Assistant,
        content: Box::new([Block::Text {
            text: b"prior".as_slice().into(),
            replay: Some(Replay::from_bytes(&encoded, &bounds.native().document()).expect("same immutable replay")),
        }]),
    }]);
    match client::Client::prepare(input, &bounds) {
        Err(error) => assert_eq!(error, Error::Unsupported),
        Ok(_) => panic!("another dialect must not accept a retained provider tag"),
    }
}

#[test]
fn output_ceiling_uses_shared_dialect_configuration() {
    let mut input = call(1);
    input.prompt.output_ceiling(Provider::Anthropic, 1234).expect("bounded ceiling");
    assert_eq!(input.prompt.max_output_tokens, Some(1234));
    input.prompt.output_ceiling(Provider::OpenAiCodex, 1234).expect("unsupported wire option omitted");
    assert_eq!(input.prompt.max_output_tokens, None);
    assert_eq!(input.prompt.output_ceiling(Provider::Anthropic, 0), Err(Error::Invalid));
}
