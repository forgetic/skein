//! Opening policy that is mechanical rather than the owner's decision
//! (channel.md, sections 5.1–5.3). The machine checks magic, range, phase
//! and terms; its owner alone judges credentials and acceptable limits.
use skein_lib::List;

use super::{Direction, Limits, Role, Schema, Term};

/// Returns the offered intersection or a frozen refusal reason.
pub(super) fn accept_open(
    schema: &Schema,
    magic: [u8; 4],
    lowest: u16,
    highest: u16,
    features: u16,
) -> Result<(u16, u16), u16> {
    if magic != schema.magic || lowest > highest || features != 0 {
        return Err(3);
    }
    let (ours_low, ours_high) = schema.range().expect("validated versions");
    let common_low = lowest.max(ours_low);
    let common_high = highest.min(ours_high);
    if common_low > common_high {
        return Err(1);
    }
    Ok((common_low, common_high))
}

/// Whether an Accept names a version in this side's offer.
pub(super) fn offers_version(schema: &Schema, number: u16) -> bool {
    schema.version(number).is_some()
}

/// Builds the terms for kinds this side receives in one version.
pub(super) fn local_terms(schema: &Schema, role: Role, number: u16, limits: &Limits) -> List<Term> {
    let version = schema.version(number).expect("selected version is supported");
    let mut terms = List::with_capacity(limits.kinds);
    for kind in &version.kinds {
        let receive = match role {
            Role::Initiator => match kind.direction {
                Direction::FromInitiator => false,
                Direction::FromResponder => true,
            },
            Role::Responder => match kind.direction {
                Direction::FromInitiator => true,
                Direction::FromResponder => false,
            },
        };
        if receive {
            terms.push(Term { kind: kind.kind, largest: kind.largest }).expect("schema kinds fit limits");
        }
    }
    terms
}

/// Checks that peer terms name only distinct kinds this side may send.
#[expect(clippy::manual_let_else, reason = "step code uses an exhaustive match instead of let-else")]
pub(super) fn check_terms(schema: &Schema, role: Role, number: u16, terms: &List<Term>) -> bool {
    let version = schema.version(number).expect("selected version is supported");
    for (index, term) in terms.iter().enumerate() {
        let kind = match version.kind(term.kind) {
            Some(kind) => kind,
            None => return false,
        };
        let send = match role {
            Role::Initiator => match kind.direction {
                Direction::FromInitiator => true,
                Direction::FromResponder => false,
            },
            Role::Responder => match kind.direction {
                Direction::FromInitiator => false,
                Direction::FromResponder => true,
            },
        };
        if !send {
            return false;
        }
        for earlier in terms.iter().take(index) {
            if earlier.kind == term.kind {
                return false;
            }
        }
    }
    true
}
