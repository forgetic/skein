//! The owner's lending boundary (oauth.md, sections 6.3 and 6.6).

use alloc::boxed::Box;
use skein_lib::Duration;
use skein_oauth::SavedToken;

/// What the owner asks of its configured accounts.
#[derive(PartialEq, Eq, Hash)]
#[expect(missing_debug_implementations, reason = "credential values must never occur in traces")]
pub enum Request {
    /// An access-only record; a newer generation replaces it, with only refusals answered.
    HandIn { account: u32, record: SavedToken },
    /// Holds the account's token until Release; answered Granted, Failed or Refused.
    Grant { account: u32 },
    /// The provider rejected the held generation; ends the hold if current.
    Rejected { account: u32, generation: u64 },
    /// Ends the owner's hold, with no answer.
    Release { account: u32 },
    /// Drains the component; Closed is its terminal.
    Close,
    /// Ends the component immediately; Closed is its terminal.
    Abort,
}

/// What the component tells its owner.
#[derive(PartialEq, Eq, Hash)]
#[expect(missing_debug_implementations, reason = "credential values must never occur in traces")]
pub enum Event {
    /// Answers Grant, then announces each new generation while the owner holds it.
    Granted { account: u32, token: Box<[u8]>, generation: u64, valid: Duration },
    /// An access-only record reached its lead; announced once per record.
    Expiring { account: u32, generation: u64 },
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
    /// The owner's requested or held grant.
    Grant,
}

/// Why the account cannot serve its grant.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Failure {
    /// No valid record, or the provider rejected its token; a newer record is needed.
    Expired,
}

/// The request a refusal answers.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Asked {
    /// The owner's access-only record.
    HandIn,
    /// The owner's requested grant.
    Grant,
}

/// Why a request was refused before any work.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Refusal {
    /// The owner has closed or aborted its component.
    Closed,
    /// The index names no configured account.
    Account,
    /// A handed-in record carries a refresh token its source alone may rotate.
    NotAccessOnly,
    /// The account's grant is already held.
    Held,
    /// The handed-in record violates the client's record bounds or encoding.
    Record(skein_oauth::DecodeError),
}
