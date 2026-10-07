//! Configured wire destinations (llm-connection.md, section 3). The address
//! is resolved before the loop, while the TLS name and trust are checked at
//! configuration construction. The component does not resolve names.

use skein_io::kernel::Addr;
use skein_llm::Endpoint as LlmEndpoint;
use skein_tls::{Config, Name};

/// One configured destination and its TLS and LLM bindings.
#[derive(Debug)]
pub struct Endpoint {
    pub address: Addr,
    pub server_name: Name,
    pub trust: Config,
    pub llm: LlmEndpoint,
}

/// Why the component cannot use its configured endpoints or bounds.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum EndpointError {
    /// The endpoint count exceeds its configured bound.
    TooMany,
    /// A limit is zero, inconsistent or cannot be measured.
    Limits,
    /// A child machine's demand cannot fit the stream below it.
    Stream,
}
