//! Account storage, exchange and transport bounds (oauth.md, section 6.7).

use crate::accounts::Source;
use crate::component::State;
use crate::exchange::{Exchange, ROUTES};
use skein_lib::{Duration, Id, List, Queue, Slab};
use skein_oauth::ClientLimits;

/// The component's limits supplied by its owner before the loop.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    pub accounts: u32,
    /// Deadline stated on each private keeper request, including cleanup.
    pub file_stall: Duration,
    pub exchanges: u32,
    /// Public redirect listeners admitted at once; zero permits only confidential sign-ins.
    pub listeners: u32,
    pub server: skein_http::server::Limits,
    /// How long before expiry a held record refreshes or a handed-in one announces Expiring.
    pub refresh_lead: Duration,
    pub client: ClientLimits,
    pub http: skein_http::client::Limits,
    pub tls: skein_tls::client::Limits,
    /// The owner's socket bounds, included conservatively once in this footprint.
    pub io: skein_io::Limits,
}

pub(crate) fn usable(limits: &Limits) -> bool {
    limits.file_stall > Duration::ZERO
        && limits.accounts > 0
        && limits.exchanges > 0
        && limits.exchanges <= u32::MAX.checked_div(3).expect("nonzero divisor")
        && match socket_count(limits) {
            Some(sockets) => limits.io.sockets >= sockets,
            None => false,
        }
        && (limits.listeners == 0
            || (skein_http::server::worst_case(&limits.server).is_some()
                && skein_http::server::largest_read(&limits.server) <= limits.io.intake
                && skein_http::server::largest_room(&limits.server) <= limits.io.output))
        && limits.client.document.token_bytes > 0
        && limits.client.document.record_bytes > 0
        && limits.http.head >= 2
        && limits.http.headers >= 2
        && limits.http.read > 0
        && limits.http.send > 0
        && limits.http.request > 0
        && limits.tls.read >= skein_http::client::largest_read(&limits.http)
        && limits.tls.send >= skein_http::client::largest_room(&limits.http)
        && limits.io.intake >= skein_tls::client::LARGEST_READ
        && limits.io.output >= skein_tls::client::largest_room(&limits.tls)
        && skein_oauth::Client::new(limits.client).is_ok()
}

/// Account generations and registration, each exchange's queues and child machines, and socket buffers.
/// The owner's shared trust roots are counted once by the owner (tls.md, section 3.4).
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    let record = u64::from(limits.client.document.record_bytes);
    let routes = Queue::<skein_oauth::Request>::worst_case(skein_oauth::MAX_OUT)?
        .checked_add(Queue::<skein_http::client::Event>::worst_case(ROUTES)?)?
        .checked_add(Queue::<skein_http::client::Request>::worst_case(ROUTES)?)?
        .checked_add(Queue::<skein_lib::stream::Down>::worst_case(ROUTES)?.checked_mul(2)?)?
        .checked_add(Queue::<skein_tls::client::Event>::worst_case(ROUTES)?)?;
    let payload = u64::from(limits.http.head.max(limits.http.request).max(limits.http.send))
        .checked_mul(u64::from(ROUTES))?
        .checked_add(u64::from(skein_tls::client::largest_room(&limits.tls)).checked_mul(u64::from(ROUTES))?)?;
    let exchange = skein_oauth::client_worst_case(&limits.client)?
        .checked_add(skein_http::client::worst_case(&limits.http)?)?
        .checked_add(skein_tls::client::worst_case(&limits.tls)?)?
        .checked_add(routes)?
        .checked_add(payload)?
        .checked_add(u64::from(limits.client.document.document_bytes).checked_mul(2)?)?;
    let listener = if limits.listeners == 0 {
        0
    } else {
        skein_http::server::worst_case(&limits.server)?.checked_add(crate::listener::worst_case(limits)?)?
    };
    // Directory, filename and one outstanding request path, plus encoded load/store bytes.
    let private = 4095_u64.checked_mul(3)?.checked_add(record.checked_mul(2)?)?;
    List::<State>::worst_case(limits.accounts)?
        .checked_add(List::<Source>::worst_case(limits.accounts)?)?
        .checked_add(List::<crate::keeper::Store>::worst_case(limits.accounts)?)?
        .checked_add(u64::from(limits.accounts).checked_mul(private)?)?
        .checked_add(List::<Option<Id<Exchange>>>::worst_case(limits.accounts)?)?
        .checked_add(
            u64::from(limits.accounts)
                .checked_mul(record.checked_mul(2)?.checked_add(skein_oauth::client_worst_case(&limits.client)?)?)?,
        )?
        .checked_add(Slab::<Exchange>::worst_case(limits.exchanges)?)?
        .checked_add(u64::from(limits.exchanges).checked_mul(exchange)?)?
        .checked_add(u64::from(limits.listeners).checked_mul(listener)?)?
        .checked_add(skein_io::worst_case(&limits.io)?)
}

fn socket_count(limits: &Limits) -> Option<u32> {
    limits.listeners.checked_mul(2)?.checked_add(limits.exchanges)
}
