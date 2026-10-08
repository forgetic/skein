//! Configured wire destinations (llm-connection.md, section 3). The address
//! is resolved before the loop, while transport admission is checked at
//! component construction. The component does not resolve names.

use skein_io::kernel::Addr;
use skein_llm::Endpoint as LlmEndpoint;
use skein_tls::{Config, Name};

/// Transport selected by the owner for a configured destination.
#[derive(Debug)]
pub enum Transport {
    /// TLS authenticated with the owner's server name and trust configuration.
    Tls { server_name: Name, trust: Config },
    /// Unencrypted bytes, admitted only for an address on loopback.
    Plaintext,
}

/// One configured destination and its transport and LLM bindings.
#[derive(Debug)]
pub struct Endpoint {
    pub address: Addr,
    pub transport: Transport,
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
    /// Plaintext was configured for an address outside loopback.
    PlaintextAddress,
}
