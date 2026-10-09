//! Account storage and lending bounds (oauth.md, section 6.7).

use crate::component::State;
use skein_lib::{Duration, List};
use skein_oauth::ClientLimits;

/// The component's limits supplied by its owner before the loop.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    pub accounts: u32,
    /// How long before expiry a handed-in record is announced Expiring.
    pub refresh_lead: Duration,
    pub client: ClientLimits,
}

/// Each account's records and one entrance's bounded record validation scratch.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    let record = u64::from(limits.client.document.record_bytes);
    List::<State>::worst_case(limits.accounts)?
        .checked_add(u64::from(limits.accounts).checked_mul(record.checked_mul(2)?)?)?
        .checked_add(skein_oauth::worst_case(&limits.client.document)?)
}
