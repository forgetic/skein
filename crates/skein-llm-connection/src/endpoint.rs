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
    pub limits: skein_llm::client::Limits,
    pub credential: skein_llm::client::CredentialLimits,
}

impl Endpoint {
    /// Apply the transport's native pieces before any test cuts lower them
    /// (llm-connection.md, section 7).
    pub fn pieces(&mut self, limits: &crate::Limits) {
        let read = match self.transport {
            Transport::Tls { .. } => {
                self.limits.http.send = skein_tls::client::MAX_PLAINTEXT;
                limits.tls.read
            }
            Transport::Plaintext => {
                self.limits.http.send = limits.io.output;
                limits.io.intake
            }
        };
        self.limits.http.read = read;
        self.limits.sse.chunk = read;
    }
}

/// Why the component cannot use its configured endpoints or bounds.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum EndpointError {
    /// The endpoint count exceeds its configured bound.
    TooMany,
    /// A limit is zero, inconsistent or cannot be measured.
    Limits,
    /// The physical connection bound is smaller than the declared conversations.
    ConnectionsCalls { connections: u32, calls: u32 },
    /// Endpoint request-head configuration was rejected by the LLM client.
    Client(skein_llm::Error),
    /// An LLM read demand exceeds TLS's delivered plaintext cap.
    HttpReadTlsRead { demand: u32, cap: u32 },
    /// An LLM upload demand exceeds TLS's plaintext room cap.
    HttpSendTlsSend { demand: u32, cap: u32 },
    /// A TLS read demand exceeds io's intake cap.
    TlsReadIoIntake { demand: u32, cap: u32 },
    /// A TLS write demand exceeds io's output cap.
    TlsSendIoOutput { demand: u32, cap: u32 },
    /// A plaintext HTTP read demand exceeds io's intake cap.
    HttpReadIoIntake { demand: u32, cap: u32 },
    /// A plaintext HTTP upload demand exceeds io's output cap.
    HttpSendIoOutput { demand: u32, cap: u32 },
    /// An SSE read demand exceeds the HTTP body's delivery cap.
    SseChunkHttpRead { demand: u32, cap: u32 },
    /// A tokenizer demand exceeds the SSE data face's delivery cap.
    TokenizerDemandSseChunk { demand: u32, cap: u32 },
    /// Plaintext was configured for an address outside loopback.
    PlaintextAddress,
}
