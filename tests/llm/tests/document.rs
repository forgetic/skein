//! The neutral document surface retains admission evidence from real entrances.
use skein_llm::{DocumentError, DocumentLimits, Error, Json, document_error, openai};

const LIMITS: DocumentLimits = DocumentLimits { bytes: 1024, strings: 256, depth: 8, tokens: 64 };

fn refusal(input: &[u8], retained: u32) -> DocumentError {
    let mut limits = LIMITS;
    limits.bytes = retained;
    match Json::from_bytes(input, &limits) {
        Ok(document) => match openai::decode_request(&document, &skein_llm_world::limits().native()) {
            Ok(_) => panic!("the corrupted request must be refused"),
            Err(error) => error,
        },
        Err(error) => error,
    }
}

#[test]
fn neutral_document_errors_distinguish_receiving_limits_from_invalid_input() {
    let valid = br#"{"stream":true,"store":false,"instructions":"test","model":"caller","input":[]}"#;
    let document = Json::from_bytes(valid, &LIMITS).expect("the complete request is admitted first");
    let request = openai::decode_request(&document, &skein_llm_world::limits().native())
        .expect("the complete request has the correct shape");
    assert_eq!(request.model.as_ref(), b"caller");
    let controls: [(&[u8], u32, DocumentError, Error); 4] = [
        (
            valid,
            1,
            DocumentError::TooLarge { which: skein_llm::Cap::Retained, bound: 1 },
            Error::Limit { which: skein_llm::Cap::Retained, bound: 1 },
        ),
        (b"{", 1024, DocumentError::Malformed, Error::Invalid),
        (b"{}", 1024, DocumentError::Missing, Error::Invalid),
        (b"[]", 1024, DocumentError::WrongType, Error::Invalid),
    ];
    for (input, cap, expected, classification) in controls {
        let error = refusal(input, cap);
        assert_eq!(error, expected, "each actual entrance retains its refusal class");
        assert_eq!(document_error(error), classification, "neutral mapping preserves receiving overflow");
    }
}
