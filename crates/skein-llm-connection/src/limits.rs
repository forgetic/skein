//! Startup limits and checked memory bounds (llm-connection.md, section 7).

use skein_lib::{Duration, Id, List, Queue, Slab};

use crate::call::Connection;

/// Bounded pool and child machine limits, fixed at startup.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    pub endpoints: u32,
    pub connections: u32,
    pub calls: u32,
    pub per_endpoint: u32,
    /// Bytes calls may reserve at once from the owner's LLM memory pool.
    pub memory: u64,
    pub idle_keep: Duration,
    pub io: skein_io::Limits,
    pub tls: skein_tls::client::Limits,
}

/// Conservative bytes for the complete pool and component tables.
#[must_use]
pub fn worst_case(limits: &Limits, endpoints: &List<crate::Endpoint>) -> Option<u64> {
    if limits.endpoints == 0
        || limits.connections == 0
        || limits.calls == 0
        || limits.per_endpoint == 0
        || limits.connections < limits.calls
        || limits.per_endpoint > limits.connections
        || endpoints.len() > limits.endpoints
    {
        return None;
    }
    let routes = Queue::<skein_llm::client::Event>::worst_case(64)?
        .checked_add(Queue::<skein_lib::stream::Down>::worst_case(256)?.checked_mul(2)?)?
        .checked_add(Queue::<skein_tls::client::Event>::worst_case(64)?)?;
    let mut largest = 0_u64;
    let mut scratch = 0_u64;
    for endpoint in endpoints {
        let mut client = endpoint.limits;
        client.http.request = match skein_llm::client::request_head(&endpoint.llm, &endpoint.credential, &client) {
            Ok(head) => head,
            Err(_) => return None,
        };
        if skein_llm::client::largest_reservation(&client)? > limits.memory {
            return None;
        }
        let fixed = skein_llm::client::worst_case(&client)?;
        let transport = match &endpoint.transport {
            crate::Transport::Tls { .. } => skein_tls::client::worst_case(&limits.tls)?,
            crate::Transport::Plaintext => 0,
        };
        let each = transport.checked_add(fixed)?.checked_add(routes)?;
        largest = largest.max(each);
        // One entrance measures or prepares one call, never one per connection.
        // Its borrowed prompt remains the owner's; translated codec tables,
        // validated replay copies and temporary JSON belong to this scratch.
        scratch = scratch.max(
            skein_llm::openai::worst_case(&client.native())?
                .max(skein_llm::anthropic::worst_case(&client.native())?)
                .checked_add(fixed)?,
        );
    }
    let pool = largest.checked_mul(u64::from(limits.connections))?;
    pool.checked_add(limits.memory)?
        .checked_add(scratch)?
        .checked_add(Slab::<Connection>::worst_case(limits.connections)?)?
        .checked_add(List::<Option<Id<Connection>>>::worst_case(limits.connections)?)?
        .checked_add(List::<Option<crate::component::Waiting>>::worst_case(limits.calls)?)?
        .checked_add(List::<crate::Endpoint>::worst_case(limits.endpoints)?)?
        .checked_add(skein_io::worst_case(&limits.io)?)
}
