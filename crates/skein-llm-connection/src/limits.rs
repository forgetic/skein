//! Startup limits and checked memory bounds (llm-connection.md, section 7).

use skein_lib::Duration;

/// Bounded pool and child machine limits, fixed at startup.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    pub endpoints: u32,
    pub connections: u32,
    pub per_endpoint: u32,
    pub idle_keep: Duration,
    pub io: skein_io::Limits,
    pub tls: skein_tls::client::Limits,
    pub llm: skein_llm::client::Limits,
}

/// Conservative bytes for the complete pool and component tables.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    if limits.endpoints == 0 || limits.connections == 0 || limits.per_endpoint == 0 {
        return None;
    }
    let each = skein_tls::client::worst_case(&limits.tls)?.checked_add(skein_llm::client::worst_case(&limits.llm)?)?;
    let pool = each.checked_mul(u64::from(limits.connections))?;
    pool.checked_add(skein_io::worst_case(&limits.io)?)
}
