//! Anthropic subscription Messages documents and a bounded streaming decoder.
pub(crate) mod filter;
pub mod identity;
mod peer;
mod request;
mod response;

pub use crate::openai::{DecodeError, Failure, Json, Limits, ProviderError, RateLimit, Stop, Usage, classify};
pub use peer::{Mark, Request, decode_request, encode_event};
pub use request::{encode_request, measure_request};
pub use response::{
    BlockStart, Delta, Event, MAX_OUT, Output, Part, StreamDecoder, UsagePatch, decode_error, decode_event,
    decoder_worst_case,
};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod request_tests;

/// A conservative per-exchange heap bound, including common JSON processing
/// and provider request/response ownership.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    crate::openai::worst_case(limits)?
        .checked_add(decoder_worst_case(limits)?)?
        .checked_add(skein_lib::List::<Mark>::worst_case(4)?.checked_mul(2)?)
}
