//! Configured sources, endpoint bindings and keepers (oauth.md, sections 6.2 and 6.4).

use alloc::boxed::Box;
use skein_io::kernel::Addr;
use skein_lib::Token;
use skein_oauth::{Registration, SavedToken};
use skein_tls::{Config, Name};

/// One configured account, supplied by the owner before the loop.
#[expect(missing_debug_implementations, reason = "configuration may hold credentials")]
#[expect(
    clippy::large_enum_variant,
    reason = "bounded startup configuration follows oauth.md section 6.2 without an extra allocation"
)]
pub enum Account {
    /// Access-only records the owner supplies; never refreshed or kept.
    HandedIn,
    /// Issuer records loaded from its keeper, refreshed while held and kept before lending.
    SignIn { registration: Registration, endpoint: Endpoint, keeper: Keeper },
}

/// The token endpoint's address, resolved by the owner before the loop.
#[derive(Debug)]
pub struct Endpoint {
    pub address: Addr,
    pub transport: Transport,
}

/// How an exchange reaches its configured endpoint.
#[derive(Debug)]
pub enum Transport {
    /// TLS with the owner's server name and trust roots, counted once by the owner.
    Tls { server_name: Name, trust: Config },
    /// Unencrypted bytes, admitted only for an address on loopback.
    Plaintext,
}

/// Who durably keeps this signed-in account's records.
#[expect(missing_debug_implementations, reason = "the keeper holds token records")]
pub enum Keeper {
    /// The owner's store; each candidate goes out as Keep and waits for Kept.
    Owner { kept: Option<SavedToken> },
    /// Private whole-file storage beneath the owner's `FileIo` root; the component closes only its private root.
    /// The owner configures `FileIo`'s effective user before adopting that startup root (io.md, section 5.3).
    Private { root: Token, directory: Box<[u8]>, file: Box<[u8]> },
}

#[expect(clippy::large_enum_variant, reason = "bounded account configuration stores its one source in place")]
pub(crate) enum Source {
    HandedIn,
    SignIn { registration: Registration, endpoint: Endpoint },
}

pub(crate) fn copy_registration(registration: &Registration) -> Registration {
    Registration {
        authorization_url: registration.authorization_url.clone(),
        token_endpoint: registration.token_endpoint.clone(),
        client_id: registration.client_id.clone(),
        redirect_uri: registration.redirect_uri.clone(),
        scope: registration.scope.clone(),
        wire: registration.wire,
        client_secret: registration.client_secret.clone(),
        pkce_for_confidential: registration.pkce_for_confidential,
        metadata_claim: registration.metadata_claim.clone(),
    }
}
