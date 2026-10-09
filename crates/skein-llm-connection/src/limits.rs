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
    let mut prepared = 0_u64;
    for endpoint in endpoints {
        let mut client = endpoint.limits;
        client.http.request = match skein_llm::client::request_head(&endpoint.llm, &endpoint.credential, &client) {
            Ok(head) => head,
            Err(_) => return None,
        };
        let each = skein_tls::client::worst_case(&limits.tls)?
            .checked_add(skein_llm::client::worst_case(&client)?)?
            .checked_add(routes)?
            .checked_add(u64::from(client.answer).checked_mul(64)?)?
            .checked_add(u64::from(client.http.request.max(client.http.send)).checked_mul(256)?)?
            .checked_add(
                u64::from(skein_tls::client::largest_room(&limits.tls).max(skein_llm::client::largest_room(&client)))
                    .checked_mul(256)?,
            )?;
        largest = largest.max(each);
        prepared = prepared.max(skein_llm::client::worst_case(&client)?);
    }
    let pool = largest.checked_mul(u64::from(limits.connections))?;
    pool.checked_add(Slab::<Connection>::worst_case(limits.connections)?)?
        .checked_add(List::<Option<Id<Connection>>>::worst_case(limits.connections)?)?
        .checked_add(List::<Option<crate::component::Waiting>>::worst_case(limits.calls)?)?
        .checked_add(prepared.checked_mul(u64::from(limits.calls))?)?
        .checked_add(List::<crate::Endpoint>::worst_case(limits.endpoints)?)?
        .checked_add(skein_io::worst_case(&limits.io)?)
}
