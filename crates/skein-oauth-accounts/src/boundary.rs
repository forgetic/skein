//! The owner's lending boundary (oauth.md, sections 6.3 and 6.6).

use alloc::boxed::Box;
use skein_lib::Duration;
use skein_oauth::SavedToken;

/// What the owner asks of its configured accounts.
#[derive(PartialEq, Eq, Hash)]
#[expect(missing_debug_implementations, reason = "credential values must never occur in traces")]
pub enum Request {
    /// Starts this account's sign-in: Visit once, then `SignedIn`, Failed or Refused.
    SignIn { account: u32 },
    /// The confidential owner's complete redirect URI; refusal leaves a sign-in waiting.
    Redirected { account: u32, uri: Box<[u8]> },
    /// Cancels this account's exchange; its requested or held work hears the terminal.
    Cancel { account: u32 },
    /// An access-only record; a newer generation replaces it, with only refusals answered.
    HandIn { account: u32, record: SavedToken },
    /// Holds the account's token until Release; answered Granted, Failed or Refused.
    Grant { account: u32 },
    /// The provider rejected the held generation; ends the hold if current.
    Rejected { account: u32, generation: u64 },
    /// Ends the owner's hold, with no answer.
    Release { account: u32 },
    /// Answers the owner-store Keep of this generation, with no event of its own.
    Kept { account: u32, generation: u64, keeping: Keeping },
    /// Drains the component; Closed is its terminal.
    Close,
    /// Ends the component immediately; Closed is its terminal.
    Abort,
}

/// What the component tells its owner.
#[derive(PartialEq, Eq, Hash)]
#[expect(missing_debug_implementations, reason = "credential values must never occur in traces")]
pub enum Event {
    /// The authorization URL for the owner to show, once per admitted sign-in.
    Visit { account: u32, url: Box<[u8]> },
    /// Ends `SignIn` once its record is durably kept, with a grant ready to ask for.
    SignedIn { account: u32, generation: u64 },
    /// Answers Grant, then announces each new generation while the owner holds it.
    Granted { account: u32, token: Box<[u8]>, generation: u64, valid: Duration },
    /// An access-only record reached its lead; announced once per record.
    Expiring { account: u32, generation: u64 },
    /// A candidate for the owner's store; nothing new is lent until its Kept terminal.
    Keep { account: u32, record: SavedToken },
    /// Ends the request named by ends, or its held grant.
    Failed { account: u32, ends: Ends, failure: Failure },
    /// Refuses a request before work starts; its one terminal.
    Refused { account: u32, asked: Asked, why: Refusal },
    /// Ends Close or Abort once, after all admitted work has settled.
    Closed,
}

/// The request a failure ends.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Ends {
    /// The owner's admitted sign-in.
    SignIn,
    /// The owner's requested or held grant.
    Grant,
}

/// Why the account cannot serve its grant.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Failure {
    /// A sign-in's candidate could not be durably kept; nothing of it is lent.
    NotKept,
    /// The issuer exchange failed, as named by the OAuth client.
    Exchange(skein_oauth::Failure),
    /// No valid record, or the provider rejected its token; a newer record is needed.
    Expired,
    /// The private record did not load, with its location and unread cause (oauth.md, section 6.4).
    Unloaded { at: Place, why: Unloaded },
}

/// The request a refusal answers.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Asked {
    /// The owner's sign-in request.
    SignIn,
    /// The confidential owner's redirect.
    Redirected,
    /// The owner's access-only record.
    HandIn,
    /// The owner's requested grant.
    Grant,
}

/// Why a request was refused before any work.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Refusal {
    /// This account already has its one exchange.
    Busy,
    /// No sign-in of the account waits for a redirect.
    NotWaiting,
    /// The URI differs from the registered redirect or exceeds its admitted syntax.
    Redirect,
    /// The owner has closed or aborted its component.
    Closed,
    /// The index names no configured account.
    Account,
    /// The requested operation does not belong to this account's source.
    Source,
    /// The exchange or public listener bound admits no more work.
    Full { bound: u32 },
    /// A handed-in record carries a refresh token its source alone may rotate.
    NotAccessOnly,
    /// The account's grant is already held.
    Held,
    /// The handed-in record violates the client's record bounds or encoding.
    Record(skein_oauth::DecodeError),
}

/// The owner's answer to a candidate Keep; its durable-store terminal.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Keeping {
    /// The record was stored durably and can be lent.
    Kept,
    /// The record was not stored; no part of it may be lent.
    NotKept,
}

/// The part of a private keeper whose loading stopped (oauth.md, section 6.4).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Place {
    /// The private directory, opened before any record is served.
    Directory,
    /// The account's whole record file.
    File,
}

/// Why loading left an account without a record (oauth.md, section 6.4).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Unloaded {
    /// Loaded bytes are damaged or belong to another version; a sign-in may replace their digest.
    Unreadable,
    /// io refused unsafe metadata without reading; sign-in fails before Visit.
    Refused(skein_io::file::Unsafe),
    /// io's kernel error; sign-in fails before Visit.
    Failed(skein_io::kernel::Error),
    /// The file operation reached its stall deadline; sign-in fails before Visit.
    Stalled,
    /// io observed more bytes than the record bound; sign-in fails before Visit.
    TooLarge { size: u64 },
}
