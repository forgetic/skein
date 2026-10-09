//! Configured account sources (oauth.md, section 6.2), named by index.

/// One configured account, supplied by the owner before the loop.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Account {
    /// Access-only records the owner supplies; never refreshed or kept.
    HandedIn,
}
