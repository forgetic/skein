//! `ChatGPT` Responses documents, both sides, and an ordered answer decoder.
mod common;
pub(crate) use common::{limit_detail, measured};
pub mod identity;
pub(crate) mod json;
mod request;
mod response;
pub(crate) use common::clipped as clip_detail;
pub use common::{DecodeError, Failure, Limits, ProviderError, RateLimit, Stop, Usage, classify, worst_case};
pub use json::Json;
pub use request::{Input, Request, Role, Tool, decode_request, encode_request, measure_request};
pub use response::{
    Echo, Event, Item, MAX_OUT, Output, Part, StreamDecoder, decode_error, decode_event, encode_completion,
    encode_error, encode_event, encode_peer_event,
};

#[cfg(test)]
mod tests;
