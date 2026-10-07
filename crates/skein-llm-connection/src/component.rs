//! Endpoint configuration and admission (llm-connection.md, sections 3, 4
//! and 7). An admitted client owns its credential and is handed to the pool
//! when the connection machine starts it. No network traffic occurs here.

use skein_lib::List;
use skein_lib::Token;
use skein_llm::{self as llm, Call, Credential, Prompt};

use crate::boundary::Refusal;
use crate::endpoint::{Endpoint, EndpointError};
use crate::limits::{Limits, worst_case};

/// Configured endpoints and bounds, with no live calls or retry policy.
#[derive(Debug)]
pub struct Component {
    endpoints: List<Endpoint>,
    limits: Limits,
}

impl Component {
    /// Validates bounds and stream capacities before the loop starts.
    pub fn new(endpoints: List<Endpoint>, limits: &Limits) -> Result<Component, EndpointError> {
        if endpoints.len() > limits.endpoints {
            return Err(EndpointError::TooMany);
        }
        if worst_case(limits).is_none() || limits.per_endpoint > limits.connections {
            return Err(EndpointError::Limits);
        }
        if llm::client::largest_read(&limits.llm) > limits.tls.read
            || llm::client::largest_room(&limits.llm) > limits.tls.send
            || skein_tls::client::LARGEST_READ > limits.io.largest_read()
            || skein_tls::client::largest_room(&limits.tls) > limits.io.largest_room()
        {
            return Err(EndpointError::Stream);
        }
        Ok(Component { endpoints, limits: *limits })
    }

    /// Admit one call before touching the stream. `occupied` is the pool's
    /// current connection count, including connections still settling.
    /// The connection machine takes the returned prepared client.
    pub fn admit(
        &self,
        occupied: u32,
        call: Token,
        endpoint: u32,
        prompt: Prompt,
        credential: Credential,
    ) -> Result<llm::client::Client, Refusal> {
        let Some(destination) = self.endpoints.get(endpoint) else {
            return Err(Refusal::Endpoint);
        };
        if occupied >= self.limits.connections {
            return Err(Refusal::Pool);
        }
        let input = Call { owner: call, prompt, credential, endpoint: destination.llm.clone() };
        match llm::client::Client::prepare(input, &self.limits.llm) {
            Ok(client) => Ok(client),
            Err(error) => Err(Refusal::Client(error)),
        }
    }
}
