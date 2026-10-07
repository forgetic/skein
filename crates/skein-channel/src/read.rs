//! Header admission for one owner read (channel.md, sections 5.3 and 6).
//! It knows the selected version and this side's receive bounds, but never
//! decodes application bodies or stores stream bytes. `classify` is called
//! before any body allocation or skip demand.
use super::{Direction, Header, Limits, Role, Schema};

/// What a checked header asks the read side to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReadKind {
    /// Decode one of the channel's fixed messages.
    Control,
    /// Allocate one application body and hand it to the owner.
    Body,
    /// Discard an unknown application kind within the skip bound.
    Skip,
}

/// Validates kind, direction and length before any allocation.
pub(super) fn classify(schema: &Schema, role: Role, limits: &Limits, version: u16, header: Header) -> Option<ReadKind> {
    match header.kind {
        0 | 1 | 2 | 4 | 7..=0x00ff => None,
        3 => {
            if header.body_len <= 262 {
                Some(ReadKind::Control)
            } else {
                None
            }
        }
        5 => {
            if header.body_len == 0 {
                Some(ReadKind::Control)
            } else {
                None
            }
        }
        6 => {
            if header.body_len == 2 {
                Some(ReadKind::Control)
            } else {
                None
            }
        }
        0x0100..=0xffff => {
            let table = schema.version(version).expect("selected version was checked");
            match table.kind(header.kind) {
                Some(kind) => {
                    let peer_sends = match role {
                        Role::Initiator => kind.direction == Direction::FromResponder,
                        Role::Responder => kind.direction == Direction::FromInitiator,
                    };
                    if peer_sends && header.body_len <= kind.largest { Some(ReadKind::Body) } else { None }
                }
                None => {
                    if header.body_len <= limits.skip {
                        Some(ReadKind::Skip)
                    } else {
                        None
                    }
                }
            }
        }
    }
}
